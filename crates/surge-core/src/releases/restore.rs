//! Release artifact graph helpers for restore/reconstruction and pruning.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{cache_path_for_key, cached_artifact_matches, fetch_or_reuse_file};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreArtifactSpec {
    pub key: String,
    pub sha256: String,
    pub size: i64,
}

struct RestoreCandidate<'a> {
    base_release: &'a ReleaseEntry,
    chain_releases: Vec<&'a ReleaseEntry>,
    chain_deltas: Vec<crate::releases::manifest::DeltaArtifact>,
    total_items: i64,
    total_bytes: i64,
    missing_items: i64,
    missing_bytes: i64,
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

pub async fn plan_full_archive_restore(
    storage: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
    version: &str,
) -> Result<Vec<RestoreArtifactSpec>> {
    let releases = sorted_releases_for_rid(index, rid);
    let target_idx = releases
        .iter()
        .position(|release| release.version == version)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} ({rid}) not found in index")))?;

    for base_idx in (0..=target_idx).rev() {
        let Some(specs) = build_restore_plan_specs(&releases, target_idx, base_idx) else {
            continue;
        };
        if restore_artifacts_exist(storage, &specs).await? {
            return Ok(specs);
        }
    }

    Err(SurgeError::NotFound(format!(
        "No reconstructable full archive found for {version} ({rid})"
    )))
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
///
/// When `cache_dir` is provided, the restore path prefers any valid cached
/// artifact chain that minimizes additional downloads rather than always
/// fetching the direct target full archive.
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

    let candidate = select_restore_candidate(storage, &releases, target_idx, options.cache_dir).await?;

    let report_progress = |items_done: i64, bytes_done: i64| {
        if let Some(callback) = options.progress {
            callback(RestoreProgress {
                items_done,
                items_total: candidate.total_items,
                bytes_done,
                bytes_total: candidate.total_bytes,
            });
        }
    };
    let prefetch_specs = {
        let mut specs = Vec::with_capacity(candidate.chain_deltas.len().saturating_add(1));
        specs.push(ArtifactPrefetchSpec {
            key: candidate.base_release.full_filename.clone(),
            sha256: candidate.base_release.full_sha256.clone(),
            size: candidate.base_release.full_size,
        });
        for delta in &candidate.chain_deltas {
            specs.push(ArtifactPrefetchSpec {
                key: delta.filename.clone(),
                sha256: delta.sha256.clone(),
                size: delta.size,
            });
        }
        specs
    };
    let used_prefetch = if let Some(cache_root) = options.cache_dir {
        prefetch_artifacts_to_cache(
            storage,
            cache_root,
            &prefetch_specs,
            options.progress,
            candidate.total_items,
            candidate.total_bytes,
        )
        .await?;
        true
    } else {
        false
    };

    let mut restored = fetch_artifact_bytes(
        storage,
        &candidate.base_release.full_filename,
        &candidate.base_release.full_sha256,
        options.cache_dir,
    )
    .await?;
    verify_expected_sha256(
        &candidate.base_release.full_sha256,
        &restored,
        &format!("full artifact '{}'", candidate.base_release.full_filename),
    )?;

    let mut items_done = 1i64;
    let mut bytes_done = candidate.base_release.full_size.max(0);
    if !used_prefetch {
        report_progress(items_done, bytes_done);
    }

    for (release, delta) in candidate.chain_releases.iter().zip(candidate.chain_deltas.iter()) {
        let delta_compressed = fetch_artifact_bytes(storage, &delta.filename, &delta.sha256, options.cache_dir).await?;
        verify_expected_sha256(
            &delta.sha256,
            &delta_compressed,
            &format!("delta artifact '{}'", delta.filename),
        )?;
        items_done = items_done.saturating_add(1);
        bytes_done = bytes_done.saturating_add(delta.size.max(0));
        if !used_prefetch {
            report_progress(items_done, bytes_done);
        }

        let patch = decode_delta_patch(delta_compressed.as_slice(), delta).map_err(|_| {
            SurgeError::NotFound(format!(
                "Failed to decode delta artifact '{}' while restoring {version} ({rid})",
                delta.filename
            ))
        })?;
        restored = apply_delta_patch(&restored, &patch, delta).map_err(|_| {
            SurgeError::NotFound(format!(
                "Failed to apply delta artifact '{}' while restoring {version} ({rid})",
                delta.filename
            ))
        })?;
        verify_expected_sha256(
            &release.full_sha256,
            &restored,
            &format!("rebuilt full archive for {}", release.version),
        )?;
    }

    Ok(restored)
}

async fn select_restore_candidate<'a>(
    storage: &dyn StorageBackend,
    releases: &[&'a ReleaseEntry],
    target_idx: usize,
    cache_dir: Option<&Path>,
) -> Result<RestoreCandidate<'a>> {
    let mut best: Option<RestoreCandidate<'a>> = None;
    let mut cache_state = BTreeMap::new();
    let mut storage_state = BTreeMap::new();

    for base_idx in (0..=target_idx).rev() {
        let Some(mut candidate) = build_restore_candidate(releases, target_idx, base_idx) else {
            continue;
        };
        let Some((missing_items, missing_bytes)) =
            assess_restore_candidate(storage, &candidate, cache_dir, &mut cache_state, &mut storage_state).await?
        else {
            continue;
        };
        candidate.missing_items = missing_items;
        candidate.missing_bytes = missing_bytes;
        if cache_dir.is_none() {
            return Ok(candidate);
        }
        if best
            .as_ref()
            .is_none_or(|current| restore_candidate_is_better(&candidate, current))
        {
            best = Some(candidate);
        }
    }

    best.ok_or_else(|| {
        let version = releases
            .get(target_idx)
            .map_or_else(|| "<unknown>".to_string(), |release| release.version.clone());
        let rid = releases
            .get(target_idx)
            .map_or_else(|| "<unknown>".to_string(), |release| release.rid.clone());
        SurgeError::NotFound(format!("No reconstructable full archive found for {version} ({rid})"))
    })
}

fn build_restore_candidate<'a>(
    releases: &[&'a ReleaseEntry],
    target_idx: usize,
    base_idx: usize,
) -> Option<RestoreCandidate<'a>> {
    let base_release = *releases.get(base_idx)?;
    if base_release.full_filename.trim().is_empty() {
        return None;
    }

    let chain_releases: Vec<&ReleaseEntry> = releases
        .iter()
        .take(target_idx + 1)
        .skip(base_idx + 1)
        .copied()
        .collect();
    let mut chain_deltas = Vec::with_capacity(chain_releases.len());
    let mut total_bytes = base_release.full_size.max(0);
    for release in &chain_releases {
        let delta = release.selected_delta()?;
        if !is_supported_delta(&delta) {
            return None;
        }
        total_bytes = total_bytes.saturating_add(delta.size.max(0));
        chain_deltas.push(delta);
    }

    let total_items = i64::try_from(chain_deltas.len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .unwrap_or(i64::MAX);

    Some(RestoreCandidate {
        base_release,
        chain_releases,
        chain_deltas,
        total_items,
        total_bytes,
        missing_items: 0,
        missing_bytes: 0,
    })
}

async fn assess_restore_candidate(
    storage: &dyn StorageBackend,
    candidate: &RestoreCandidate<'_>,
    cache_dir: Option<&Path>,
    cache_state: &mut BTreeMap<String, bool>,
    storage_state: &mut BTreeMap<String, bool>,
) -> Result<Option<(i64, i64)>> {
    let mut missing_items = 0i64;
    let mut missing_bytes = 0i64;

    let mut specs = Vec::with_capacity(candidate.chain_deltas.len().saturating_add(1));
    specs.push(RestoreArtifactSpec {
        key: candidate.base_release.full_filename.clone(),
        sha256: candidate.base_release.full_sha256.clone(),
        size: candidate.base_release.full_size,
    });
    for delta in &candidate.chain_deltas {
        specs.push(RestoreArtifactSpec {
            key: delta.filename.clone(),
            sha256: delta.sha256.clone(),
            size: delta.size,
        });
    }

    for spec in specs {
        if let Some(cache_root) = cache_dir
            && cached_artifact_available(cache_root, &spec, cache_state)?
        {
            continue;
        }
        if !storage_artifact_available(storage, &spec.key, storage_state).await? {
            return Ok(None);
        }
        missing_items = missing_items.saturating_add(1);
        missing_bytes = missing_bytes.saturating_add(spec.size.max(0));
    }

    Ok(Some((missing_items, missing_bytes)))
}

fn cached_artifact_available(
    cache_root: &Path,
    spec: &RestoreArtifactSpec,
    cache_state: &mut BTreeMap<String, bool>,
) -> Result<bool> {
    if let Some(cached) = cache_state.get(&spec.key) {
        return Ok(*cached);
    }

    let cache_path = cache_path_for_key(cache_root, &spec.key)?;
    let cached = cached_artifact_matches(&cache_path, &spec.sha256)?;
    cache_state.insert(spec.key.clone(), cached);
    Ok(cached)
}

async fn storage_artifact_available(
    storage: &dyn StorageBackend,
    key: &str,
    storage_state: &mut BTreeMap<String, bool>,
) -> Result<bool> {
    if let Some(cached) = storage_state.get(key) {
        return Ok(*cached);
    }

    let available = match storage.head_object(key).await {
        Ok(_) => true,
        Err(SurgeError::NotFound(_)) => false,
        Err(e) => return Err(e),
    };
    storage_state.insert(key.to_string(), available);
    Ok(available)
}

fn restore_candidate_is_better(candidate: &RestoreCandidate<'_>, current: &RestoreCandidate<'_>) -> bool {
    (
        candidate.missing_bytes,
        candidate.missing_items,
        candidate.chain_deltas.len(),
    ) < (current.missing_bytes, current.missing_items, current.chain_deltas.len())
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

fn build_restore_plan_specs(
    releases: &[&ReleaseEntry],
    target_idx: usize,
    base_idx: usize,
) -> Option<Vec<RestoreArtifactSpec>> {
    let base_release = releases.get(base_idx)?;
    let full_key = base_release.full_filename.trim();
    if full_key.is_empty() {
        return None;
    }

    let mut specs = vec![RestoreArtifactSpec {
        key: full_key.to_string(),
        sha256: base_release.full_sha256.clone(),
        size: base_release.full_size,
    }];

    for release in releases.iter().take(target_idx + 1).skip(base_idx + 1) {
        let delta = release.selected_delta()?;
        if !is_supported_delta(&delta) {
            return None;
        }
        specs.push(RestoreArtifactSpec {
            key: delta.filename.clone(),
            sha256: delta.sha256.clone(),
            size: delta.size,
        });
    }

    Some(specs)
}

async fn restore_artifacts_exist(storage: &dyn StorageBackend, specs: &[RestoreArtifactSpec]) -> Result<bool> {
    for spec in specs {
        match storage.head_object(&spec.key).await {
            Ok(_) => {}
            Err(SurgeError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff};
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

        let patch_v2 = chunked_bsdiff(&full_v1, &full_v2, &ChunkedDiffOptions::default()).unwrap();
        let patch_v3 = chunked_bsdiff(&full_v2, &full_v3, &ChunkedDiffOptions::default()).unwrap();
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
    async fn test_restore_full_archive_rebuilds_from_archive_chunked_deltas_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let mut packer_v1 = ArchivePacker::new(7).unwrap();
        packer_v1
            .add_buffer("Program.cs", b"Console.WriteLine(\"v1\");\n", 0o644)
            .unwrap();
        packer_v1
            .add_buffer("payload.bin", &vec![b'A'; 1024 * 1024], 0o644)
            .unwrap();
        let full_v1 = packer_v1.finalize().unwrap();

        let mut packer_v2 = ArchivePacker::new(7).unwrap();
        packer_v2
            .add_buffer("Program.cs", b"Console.WriteLine(\"v2\");\n", 0o644)
            .unwrap();
        packer_v2
            .add_buffer("payload.bin", &vec![b'A'; 1024 * 1024], 0o644)
            .unwrap();
        let full_v2 = packer_v2.finalize().unwrap();

        let mut packer_v3 = ArchivePacker::new(7).unwrap();
        packer_v3
            .add_buffer("Program.cs", b"Console.WriteLine(\"v3\");\n", 0o644)
            .unwrap();
        packer_v3
            .add_buffer("payload.bin", &vec![b'A'; 1024 * 1024], 0o644)
            .unwrap();
        let full_v3 = packer_v3.finalize().unwrap();

        let patch_v2 = crate::releases::delta::build_archive_chunked_patch(
            &full_v1,
            &full_v2,
            7,
            0,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let patch_v3 = crate::releases::delta::build_archive_chunked_patch(
            &full_v2,
            &full_v3,
            7,
            0,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = crate::crypto::sha256::sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = crate::crypto::sha256::sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &crate::crypto::sha256::sha256_hex(&delta_v2),
        )));

        let mut v3 = make_entry("1.2.0");
        v3.full_sha256 = crate::crypto::sha256::sha256_hex(&full_v3);
        v3.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-linux-x64-delta.tar.zst",
            delta_v3.len() as i64,
            &crate::crypto::sha256::sha256_hex(&delta_v3),
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

    #[tokio::test]
    async fn test_restore_full_archive_prefers_cached_graph_over_direct_full_download() {
        let tmp = tempfile::tempdir().unwrap();
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).unwrap();
        let cache_root = tmp.path().join("cache");
        std::fs::create_dir_all(&cache_root).unwrap();
        let backend = FilesystemBackend::new(backend_root.to_str().unwrap(), "");

        let full_v1 = vec![b'a'; 4096];
        let mut full_v2 = full_v1.clone();
        full_v2[0] = b'b';
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_size = i64::try_from(full_v1.len()).unwrap();
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_size = i64::try_from(full_v2.len()).unwrap();
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            i64::try_from(delta_v2.len()).unwrap(),
            &sha256_hex(&delta_v2),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v2.full_filename, &full_v2, "application/octet-stream")
            .await
            .unwrap();
        let delta_key = v2.selected_delta().unwrap().filename.clone();
        backend
            .put_object(&delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();

        let cached_v1 = cache_path_for_key(&cache_root, &v1.full_filename).unwrap();
        let cached_delta = cache_path_for_key(&cache_root, &delta_key).unwrap();
        std::fs::create_dir_all(cached_v1.parent().unwrap()).unwrap();
        std::fs::write(&cached_v1, &full_v1).unwrap();
        std::fs::write(&cached_delta, &delta_v2).unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2.clone()],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version_with_options(
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

        assert_eq!(restored, full_v2);
        assert!(
            !cache_path_for_key(&cache_root, &v2.full_filename).unwrap().exists(),
            "direct full should not be fetched when the cached graph can reconstruct the target"
        );
    }

    #[tokio::test]
    async fn test_plan_full_archive_restore_reports_delta_chain_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

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
        let delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        backend
            .put_object(&delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2],
            ..ReleaseIndex::default()
        };

        let specs = plan_full_archive_restore(&backend, &index, "linux-x64", "1.1.0")
            .await
            .unwrap();

        assert_eq!(
            specs,
            vec![
                RestoreArtifactSpec {
                    key: v1.full_filename,
                    sha256: v1.full_sha256,
                    size: v1.full_size,
                },
                RestoreArtifactSpec {
                    key: delta_key,
                    sha256: sha256_hex(&delta_v2),
                    size: delta_v2.len() as i64,
                },
            ]
        );
    }
}
