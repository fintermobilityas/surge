//! Release artifact graph helpers for restore/reconstruction and pruning.

use std::collections::{BTreeMap, BTreeSet};

use crate::crypto::sha256::sha256_hex;
use crate::diff::wrapper::bspatch_buffers;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex};
use crate::releases::version::compare_versions;
use crate::storage::StorageBackend;

/// Return releases for a RID sorted by semantic version (oldest -> newest).
#[must_use]
pub fn sorted_releases_for_rid<'a>(index: &'a ReleaseIndex, rid: &str) -> Vec<&'a ReleaseEntry> {
    let mut releases: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|release| release.rid == rid || release.rid.is_empty())
        .collect();
    releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
    releases
}

/// Find a specific RID release by exact version.
#[must_use]
pub fn find_release_for_version_rid<'a>(index: &'a ReleaseIndex, rid: &str, version: &str) -> Option<&'a ReleaseEntry> {
    sorted_releases_for_rid(index, rid)
        .into_iter()
        .find(|release| release.version == version)
}

/// Find the most recent release before `version` for a RID.
#[must_use]
pub fn find_previous_release_for_rid<'a>(
    index: &'a ReleaseIndex,
    rid: &str,
    version: &str,
) -> Option<&'a ReleaseEntry> {
    let mut previous: Option<&ReleaseEntry> = None;
    for release in sorted_releases_for_rid(index, rid) {
        if compare_versions(&release.version, version) != std::cmp::Ordering::Less {
            continue;
        }
        previous = Some(release);
    }
    previous
}

/// Restore a release full archive for `version` by RID.
///
/// This first attempts to download the direct full artifact. If missing, it
/// searches for an earlier available full and rebuilds forward via delta chain.
pub async fn restore_full_archive_for_version(
    storage: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
    version: &str,
) -> Result<Vec<u8>> {
    let releases = sorted_releases_for_rid(index, rid);
    let target_idx = releases
        .iter()
        .position(|release| release.version == version)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} ({rid}) not found in index")))?;

    for base_idx in (0..=target_idx).rev() {
        let base_release = releases[base_idx];
        if base_release.full_filename.trim().is_empty() {
            continue;
        }

        let mut candidate = match storage.get_object(&base_release.full_filename).await {
            Ok(bytes) => bytes,
            Err(SurgeError::NotFound(_)) => continue,
            Err(e) => return Err(e),
        };
        if verify_expected_sha256(
            &base_release.full_sha256,
            &candidate,
            &format!("full artifact '{}'", base_release.full_filename),
        )
        .is_err()
        {
            continue;
        }

        let mut chain_valid = true;
        for release in releases.iter().take(target_idx + 1).skip(base_idx + 1) {
            let Some(delta) = release.selected_delta() else {
                chain_valid = false;
                break;
            };

            if !is_supported_delta(&delta) {
                chain_valid = false;
                break;
            }

            let delta_compressed = match storage.get_object(&delta.filename).await {
                Ok(bytes) => bytes,
                Err(SurgeError::NotFound(_)) => {
                    chain_valid = false;
                    break;
                }
                Err(e) => return Err(e),
            };
            if verify_expected_sha256(
                &delta.sha256,
                &delta_compressed,
                &format!("delta artifact '{}'", delta.filename),
            )
            .is_err()
            {
                chain_valid = false;
                break;
            }

            let patch = match decode_delta_patch(delta_compressed.as_slice(), &delta) {
                Ok(data) => data,
                Err(_) => {
                    chain_valid = false;
                    break;
                }
            };
            candidate = match apply_delta_patch(&candidate, &patch, &delta) {
                Ok(bytes) => bytes,
                Err(_) => {
                    chain_valid = false;
                    break;
                }
            };
            if verify_expected_sha256(
                &release.full_sha256,
                &candidate,
                &format!("rebuilt full archive for {}", release.version),
            )
            .is_err()
            {
                chain_valid = false;
                break;
            }
        }

        if chain_valid {
            return Ok(candidate);
        }
    }

    Err(SurgeError::NotFound(format!(
        "No reconstructable full archive found for {version} ({rid})"
    )))
}

/// Compute the required artifact keys for a release index after dependency
/// pruning. This keeps the minimum chain for each RID plus latest full.
#[must_use]
pub fn required_artifacts_for_index(index: &ReleaseIndex) -> BTreeSet<String> {
    let mut by_rid: BTreeMap<&str, Vec<&ReleaseEntry>> = BTreeMap::new();
    for release in &index.releases {
        by_rid.entry(release.rid.as_str()).or_default().push(release);
    }

    let mut required = BTreeSet::new();
    for releases in by_rid.values_mut() {
        releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
        extend_required_artifacts_for_sorted_releases(releases, &mut required);
    }
    required
}

fn extend_required_artifacts_for_sorted_releases(releases: &[&ReleaseEntry], required: &mut BTreeSet<String>) {
    if releases.is_empty() {
        return;
    }

    let mut required_full_indices = Vec::new();
    if let Some(first_full_idx) = releases
        .iter()
        .position(|release| !release.full_filename.trim().is_empty())
    {
        required_full_indices.push(first_full_idx);
    }

    for (idx, release) in releases.iter().enumerate().skip(1) {
        if release.selected_delta().is_none() && !release.full_filename.trim().is_empty() {
            required_full_indices.push(idx);
        }
    }

    if let Some(last_full_idx) = releases
        .iter()
        .rposition(|release| !release.full_filename.trim().is_empty())
        && !required_full_indices.contains(&last_full_idx)
    {
        required_full_indices.push(last_full_idx);
    }

    if required_full_indices.is_empty() {
        return;
    }

    required_full_indices.sort_unstable();
    required_full_indices.dedup();

    for idx in &required_full_indices {
        let full = releases[*idx].full_filename.trim();
        if !full.is_empty() {
            required.insert(full.to_string());
        }
    }

    let first_required_full = required_full_indices[0];
    for release in releases.iter().skip(first_required_full + 1) {
        for delta in release.all_deltas() {
            let key = delta.filename.trim();
            if !key.is_empty() {
                required.insert(key.to_string());
            }
        }
    }
}

fn normalized_or_default<'a>(value: &'a str, default: &'a str) -> &'a str {
    let trimmed = value.trim();
    if trimmed.is_empty() { default } else { trimmed }
}

fn is_supported_delta(delta: &DeltaArtifact) -> bool {
    let algorithm = normalized_or_default(&delta.algorithm, crate::releases::manifest::DIFF_ALGORITHM_BSDIFF);
    let patch_format = normalized_or_default(&delta.patch_format, crate::releases::manifest::PATCH_FORMAT_BSDIFF4);
    let compression = normalized_or_default(&delta.compression, crate::releases::manifest::COMPRESSION_ZSTD);

    algorithm.eq_ignore_ascii_case(crate::releases::manifest::DIFF_ALGORITHM_BSDIFF)
        && patch_format.eq_ignore_ascii_case(crate::releases::manifest::PATCH_FORMAT_BSDIFF4)
        && compression.eq_ignore_ascii_case(crate::releases::manifest::COMPRESSION_ZSTD)
}

fn decode_delta_patch(data: &[u8], delta: &DeltaArtifact) -> Result<Vec<u8>> {
    let compression = normalized_or_default(&delta.compression, crate::releases::manifest::COMPRESSION_ZSTD);
    if compression.eq_ignore_ascii_case(crate::releases::manifest::COMPRESSION_ZSTD) {
        return zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("{e}")));
    }
    Err(SurgeError::Update(format!(
        "Unsupported delta compression '{}'",
        delta.compression
    )))
}

fn apply_delta_patch(older: &[u8], patch: &[u8], delta: &DeltaArtifact) -> Result<Vec<u8>> {
    let algorithm = normalized_or_default(&delta.algorithm, crate::releases::manifest::DIFF_ALGORITHM_BSDIFF);
    let patch_format = normalized_or_default(&delta.patch_format, crate::releases::manifest::PATCH_FORMAT_BSDIFF4);

    if algorithm.eq_ignore_ascii_case(crate::releases::manifest::DIFF_ALGORITHM_BSDIFF)
        && patch_format.eq_ignore_ascii_case(crate::releases::manifest::PATCH_FORMAT_BSDIFF4)
    {
        return bspatch_buffers(older, patch);
    }

    Err(SurgeError::Update(format!(
        "Unsupported delta algorithm/format '{}/{}'",
        delta.algorithm, delta.patch_format
    )))
}

fn verify_expected_sha256(expected: &str, data: &[u8], context: &str) -> Result<()> {
    let expected = expected.trim();
    if expected.is_empty() {
        return Ok(());
    }

    let actual = sha256_hex(data);
    if actual != expected {
        return Err(SurgeError::Storage(format!(
            "SHA-256 mismatch for {context}: expected {expected}, got {actual}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::releases::manifest::ReleaseEntry;
    use crate::storage::filesystem::FilesystemBackend;

    fn make_entry(version: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["stable".to_string()],
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
            is_genesis: false,
            full_filename: format!("demo-{version}-linux-x64-full.tar.zst"),
            full_size: 0,
            full_sha256: String::new(),
            deltas: vec![DeltaArtifact::bsdiff_zstd(
                "primary",
                "",
                &format!("demo-{version}-linux-x64-delta.tar.zst"),
                0,
                "",
            )],
            preferred_delta_id: "primary".to_string(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demo".to_string(),
            install_directory: "demo".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn test_required_artifacts_prunes_redundant_fulls_and_deltas() {
        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);

        let mut v2 = make_entry("1.1.0");
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta",
            0,
            "",
        )));

        let mut v3 = make_entry("1.2.0");
        v3.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-delta",
            0,
            "",
        )));

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2.clone(), v3.clone()],
            ..ReleaseIndex::default()
        };

        let v2_delta = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        let v3_delta = v3.selected_delta().expect("v3 should have delta descriptor").filename;
        let required = required_artifacts_for_index(&index);
        assert!(required.contains(&v1.full_filename));
        assert!(required.contains(&v2_delta));
        assert!(required.contains(&v3.full_filename));
        assert!(required.contains(&v3_delta));
        assert!(!required.contains(&v2.full_filename));
    }

    #[tokio::test]
    async fn test_restore_full_archive_rebuilds_from_deltas_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();
        let full_v3 = b"full-v3".to_vec();

        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let patch_v3 = bsdiff_buffers(&full_v2, &full_v3).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        let mut v3 = make_entry("1.2.0");
        v3.full_sha256 = sha256_hex(&full_v3);
        v3.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-linux-x64-delta.tar.zst",
            delta_v3.len() as i64,
            &sha256_hex(&delta_v3),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let v2_delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        let v3_delta_key = v3.selected_delta().expect("v3 should have delta descriptor").filename;
        backend
            .put_object(&v2_delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v3_delta_key, &delta_v3, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2, v3],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version(&backend, &index, "linux-x64", "1.2.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v3);
    }

    #[tokio::test]
    async fn test_restore_full_archive_prefers_direct_full_when_available() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta",
            13,
            &sha256_hex(b"invalid-delta"),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v2.full_filename, &full_v2, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version(&backend, &index, "linux-x64", "1.1.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v2);
    }
}
