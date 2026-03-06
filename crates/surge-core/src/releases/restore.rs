//! Release artifact graph helpers for restore/reconstruction and pruning.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{cache_path_for_key, fetch_or_reuse_file};
use crate::releases::delta::{apply_delta_patch, decode_delta_patch, is_supported_delta};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex};
use crate::releases::version::compare_versions;
use crate::storage::StorageBackend;
use futures_util::stream::{self, StreamExt};

pub type RestoreProgressCallback<'a> = dyn Fn(RestoreProgress) + Send + Sync + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreProgress {
    pub items_done: i64,
    pub items_total: i64,
    pub bytes_done: i64,
    pub bytes_total: i64,
}

#[derive(Default)]
pub struct RestoreOptions<'a> {
    pub cache_dir: Option<&'a Path>,
    pub progress: Option<&'a RestoreProgressCallback<'a>>,
}

#[derive(Debug, Clone)]
struct ArtifactPrefetchSpec {
    key: String,
    sha256: String,
    size: i64,
}

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
    restore_full_archive_for_version_with_options(storage, index, rid, version, RestoreOptions::default()).await
}

/// Restore a release full archive with optional local-cache and progress reporting.
pub async fn restore_full_archive_for_version_with_options(
    storage: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
    version: &str,
    options: RestoreOptions<'_>,
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

        let chain_releases: Vec<&ReleaseEntry> = releases
            .iter()
            .take(target_idx + 1)
            .skip(base_idx + 1)
            .copied()
            .collect();
        let mut chain_deltas = Vec::with_capacity(chain_releases.len());
        let mut chain_valid = true;
        let mut total_bytes = base_release.full_size.max(0);
        for release in &chain_releases {
            let Some(delta) = release.selected_delta() else {
                chain_valid = false;
                break;
            };
            if !is_supported_delta(&delta) {
                chain_valid = false;
                break;
            }
            total_bytes = total_bytes.saturating_add(delta.size.max(0));
            chain_deltas.push(delta);
        }
        if !chain_valid {
            continue;
        }

        let total_items = i64::try_from(chain_deltas.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .unwrap_or(i64::MAX);
        let mut items_done = 0i64;
        let mut bytes_done = 0i64;
        let report_progress = |items_done: i64, bytes_done: i64| {
            if let Some(callback) = options.progress {
                callback(RestoreProgress {
                    items_done,
                    items_total: total_items,
                    bytes_done,
                    bytes_total: total_bytes,
                });
            }
        };
        let prefetch_specs = {
            let mut specs = Vec::with_capacity(chain_deltas.len().saturating_add(1));
            specs.push(ArtifactPrefetchSpec {
                key: base_release.full_filename.clone(),
                sha256: base_release.full_sha256.clone(),
                size: base_release.full_size,
            });
            for delta in &chain_deltas {
                specs.push(ArtifactPrefetchSpec {
                    key: delta.filename.clone(),
                    sha256: delta.sha256.clone(),
                    size: delta.size,
                });
            }
            specs
        };
        let used_prefetch = if let Some(cache_root) = options.cache_dir {
            match prefetch_artifacts_to_cache(
                storage,
                cache_root,
                &prefetch_specs,
                options.progress,
                total_items,
                total_bytes,
            )
            .await
            {
                Ok(()) => true,
                Err(SurgeError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        } else {
            false
        };

        let mut candidate = match fetch_artifact_bytes(
            storage,
            &base_release.full_filename,
            &base_release.full_sha256,
            options.cache_dir,
        )
        .await
        {
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
        items_done = items_done.saturating_add(1);
        bytes_done = bytes_done.saturating_add(base_release.full_size.max(0));
        if !used_prefetch {
            report_progress(items_done, bytes_done);
        }

        for (release, delta) in chain_releases.iter().zip(chain_deltas.iter()) {
            let delta_compressed =
                match fetch_artifact_bytes(storage, &delta.filename, &delta.sha256, options.cache_dir).await {
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
            items_done = items_done.saturating_add(1);
            bytes_done = bytes_done.saturating_add(delta.size.max(0));
            if !used_prefetch {
                report_progress(items_done, bytes_done);
            }

            let patch = match decode_delta_patch(delta_compressed.as_slice(), delta) {
                Ok(data) => data,
                Err(_) => {
                    chain_valid = false;
                    break;
                }
            };
            candidate = match apply_delta_patch(&candidate, &patch, delta) {
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

async fn fetch_artifact_bytes(
    storage: &dyn StorageBackend,
    key: &str,
    expected_sha256: &str,
    cache_dir: Option<&Path>,
) -> Result<Vec<u8>> {
    if let Some(cache_root) = cache_dir {
        let cache_path = cache_path_for_key(cache_root, key)?;
        fetch_or_reuse_file(storage, key, &cache_path, expected_sha256, None).await?;
        return std::fs::read(cache_path).map_err(SurgeError::Io);
    }
    storage.get_object(key).await
}

async fn prefetch_artifacts_to_cache(
    storage: &dyn StorageBackend,
    cache_root: &Path,
    specs: &[ArtifactPrefetchSpec],
    progress: Option<&RestoreProgressCallback<'_>>,
    items_total: i64,
    bytes_total: i64,
) -> Result<()> {
    const PREFETCH_CONCURRENCY: usize = 4;

    let mut items_done = 0i64;
    let mut bytes_done = 0i64;
    let mut prefetch_stream = stream::iter(specs.iter().cloned())
        .map(|spec| async move {
            let cache_path = cache_path_for_key(cache_root, &spec.key)?;
            fetch_or_reuse_file(storage, &spec.key, &cache_path, &spec.sha256, None).await?;
            Ok::<i64, SurgeError>(spec.size.max(0))
        })
        .buffer_unordered(PREFETCH_CONCURRENCY);

    while let Some(result) = prefetch_stream.next().await {
        let size = result?;
        items_done = items_done.saturating_add(1);
        bytes_done = bytes_done.saturating_add(size);
        if let Some(callback) = progress {
            callback(RestoreProgress {
                items_done,
                items_total,
                bytes_done,
                bytes_total,
            });
        }
    }

    Ok(())
}

/// Compute the required artifact keys for a release index after dependency
/// pruning. This keeps the minimum forward chain for each RID from one base
/// full artifact plus deltas.
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
    use crate::diff::chunked::chunked_bsdiff;
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::releases::manifest::{DeltaArtifact, ReleaseEntry};
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
        assert!(required.contains(&v3_delta));
        assert!(!required.contains(&v2.full_filename));
        assert!(!required.contains(&v3.full_filename));
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
    async fn test_restore_full_archive_rebuilds_from_chunked_deltas_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2-with-extra-data".to_vec();
        let full_v3 = b"full-v3-with-even-more-extra-data".to_vec();

        let patch_v2 = chunked_bsdiff(&full_v1, &full_v2, &Default::default()).unwrap();
        let patch_v3 = chunked_bsdiff(&full_v2, &full_v3, &Default::default()).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        let mut v3 = make_entry("1.2.0");
        v3.full_sha256 = sha256_hex(&full_v3);
        v3.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_zstd(
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

    #[tokio::test]
    async fn test_restore_full_archive_uses_local_cache_when_backend_artifacts_are_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).unwrap();
        let cache_root = tmp.path().join("cache");
        std::fs::create_dir_all(&cache_root).unwrap();
        let backend = FilesystemBackend::new(backend_root.to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();

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

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let v2_delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        backend
            .put_object(&v2_delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2],
            ..ReleaseIndex::default()
        };

        let first = restore_full_archive_for_version_with_options(
            &backend,
            &index,
            "linux-x64",
            "1.1.0",
            RestoreOptions {
                cache_dir: Some(&cache_root),
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(first, full_v2);

        std::fs::remove_dir_all(&backend_root).unwrap();
        std::fs::create_dir_all(&backend_root).unwrap();

        let second = restore_full_archive_for_version_with_options(
            &backend,
            &index,
            "linux-x64",
            "1.1.0",
            RestoreOptions {
                cache_dir: Some(&cache_root),
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(second, full_v2);
    }
}
