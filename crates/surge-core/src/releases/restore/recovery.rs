use std::path::Path;

use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::releases::artifact_cache::{cache_path_for_key, fetch_or_reuse_file};
use crate::releases::delta::{apply_delta_patch, decode_delta_patch};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex};
use crate::storage::StorageBackend;
use futures_util::stream::{self, StreamExt};

use super::candidate::select_restore_candidate;
use super::planning::sorted_releases_for_rid;
use super::{RestoreOptions, RestoreProgress, RestoreProgressCallback};

#[derive(Debug, Clone)]
struct ArtifactPrefetchSpec {
    key: String,
    sha256: String,
    size: i64,
}

pub async fn restore_full_archive_for_version(
    storage: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
    version: &str,
) -> Result<Vec<u8>> {
    restore_full_archive_for_version_with_options(storage, index, rid, version, RestoreOptions::default()).await
}

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
        if let Some(cache_root) = options.cache_dir {
            cache_restored_full_archive(cache_root, release, &restored)?;
        }
    }

    Ok(restored)
}

fn cache_restored_full_archive(cache_root: &Path, release: &ReleaseEntry, restored: &[u8]) -> Result<()> {
    let key = release.full_filename.trim();
    if key.is_empty() {
        return Ok(());
    }

    verify_expected_sha256(
        &release.full_sha256,
        restored,
        &format!("cached rebuilt full archive for {}", release.version),
    )?;

    let cache_path = cache_path_for_key(cache_root, key)?;
    write_file_atomic(&cache_path, restored)
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
