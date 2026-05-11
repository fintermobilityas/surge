use std::path::Path;
use std::sync::{Arc, Mutex};

use futures_util::stream::{self, StreamExt};
use tracing::debug;

use crate::crypto::sha256::sha256_hex_file;
use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{CacheFetchOutcome, cache_path_for_key, fetch_or_reuse_file};
use crate::releases::manifest::ReleaseEntry;

use super::progress::{
    ArtifactDownload, DownloadProgressState, ProgressInfo, average_speed_bytes_per_sec, emit_progress,
};
use super::{ApplyStrategy, UpdateInfo, UpdateManager};

const DOWNLOAD_CONCURRENCY: usize = 4;

pub(super) async fn prepare_update_artifacts<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    progress: Option<&Arc<F>>,
) -> Result<()>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let progress = progress.cloned();
    let artifacts = selected_artifacts(info)?;
    let total_items = i64::try_from(artifacts.len()).unwrap_or(i64::MAX);
    let total_bytes = artifacts
        .iter()
        .fold(0i64, |acc, artifact| acc.saturating_add(artifact.size.max(0)));
    let total_bytes_u64 = u64::try_from(total_bytes).unwrap_or(u64::MAX);

    emit_progress(
        progress.as_ref(),
        ProgressInfo {
            phase: 2,
            total_percent: 10,
            bytes_total: total_bytes,
            items_total: total_items,
            ..ProgressInfo::default()
        },
    );

    let storage = manager.storage.as_ref();
    let download_progress_state = Arc::new(Mutex::new(DownloadProgressState::new()));
    let mut download_stream = stream::iter(artifacts)
        .map(|artifact| {
            let download_progress_state = Arc::clone(&download_progress_state);
            let progress = progress.clone();
            async move {
                let cache_path = cache_path_for_key(artifact_cache_dir, &artifact.key)?;
                let artifact_key_for_progress = artifact.key.clone();
                let progress_callback = move |done: u64, _total: u64| {
                    let snapshot = {
                        let mut state = download_progress_state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.observe_artifact_bytes(&artifact_key_for_progress, done);
                        state.snapshot(total_bytes_u64, total_items)
                    };
                    emit_progress(progress.as_ref(), snapshot);
                };
                let outcome = fetch_or_reuse_file(
                    storage,
                    &artifact.key,
                    &cache_path,
                    &artifact.sha256,
                    Some(&progress_callback),
                )
                .await?;

                let stage_path = staging_dir.join(&artifact.key);
                if let Some(parent) = stage_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::copy(&cache_path, &stage_path).await?;

                Ok::<(ArtifactDownload, CacheFetchOutcome), SurgeError>((artifact, outcome))
            }
        })
        .buffer_unordered(DOWNLOAD_CONCURRENCY);

    while let Some(result) = download_stream.next().await {
        manager.ctx.check_cancelled()?;
        let (artifact, outcome) = result?;

        debug!(key = %artifact.key, ?outcome, "Prepared artifact for update application");

        let snapshot = {
            let mut state = download_progress_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.finish_artifact(&artifact.key, u64::try_from(artifact.size.max(0)).unwrap_or(u64::MAX));
            state.snapshot(total_bytes_u64, total_items)
        };
        emit_progress(progress.as_ref(), snapshot);
    }

    emit_progress(
        progress.as_ref(),
        ProgressInfo {
            phase: 2,
            phase_percent: 100,
            total_percent: 40,
            bytes_done: total_bytes,
            bytes_total: total_bytes,
            items_done: total_items,
            items_total: total_items,
            speed_bytes_per_sec: {
                let state = download_progress_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                average_speed_bytes_per_sec(state.bytes_done(), state.started_at())
            },
            ..ProgressInfo::default()
        },
    );

    verify_update_artifacts(info, staging_dir, total_items, progress.as_ref())
}

fn selected_artifacts(info: &UpdateInfo) -> Result<Vec<ArtifactDownload>> {
    let artifacts = if matches!(info.apply_strategy, ApplyStrategy::Delta) {
        info.apply_releases
            .iter()
            .filter_map(ReleaseEntry::selected_delta)
            .map(|delta| ArtifactDownload {
                key: delta.filename.clone(),
                sha256: delta.sha256.clone(),
                size: delta.size,
            })
            .collect()
    } else {
        let latest = info
            .apply_releases
            .last()
            .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
        vec![ArtifactDownload {
            key: latest.full_filename.clone(),
            sha256: latest.full_sha256.clone(),
            size: latest.full_size,
        }]
    };

    if artifacts.is_empty() {
        return Err(SurgeError::Update("No artifacts selected for download".to_string()));
    }

    Ok(artifacts)
}

fn verify_update_artifacts<F>(
    info: &UpdateInfo,
    staging_dir: &Path,
    total_items: i64,
    progress: Option<&Arc<F>>,
) -> Result<()>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    emit_progress(
        progress,
        ProgressInfo {
            phase: 3,
            total_percent: 45,
            items_total: total_items,
            ..ProgressInfo::default()
        },
    );

    if matches!(info.apply_strategy, ApplyStrategy::Delta) {
        for release in &info.apply_releases {
            let Some(delta) = release.selected_delta() else {
                continue;
            };

            let path = staging_dir.join(&delta.filename);
            let hash = sha256_hex_file(&path)?;
            if !delta.sha256.is_empty() && hash != delta.sha256 {
                return Err(SurgeError::Update(format!(
                    "SHA-256 mismatch for {}: expected {}, got {hash}",
                    delta.filename, delta.sha256
                )));
            }
        }
    } else {
        let latest = info
            .apply_releases
            .last()
            .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
        let path = staging_dir.join(&latest.full_filename);
        let hash = sha256_hex_file(&path)?;
        if !latest.full_sha256.is_empty() && hash != latest.full_sha256 {
            return Err(SurgeError::Update(format!(
                "SHA-256 mismatch for {}: expected {}, got {hash}",
                latest.full_filename, latest.full_sha256
            )));
        }
    }

    emit_progress(
        progress,
        ProgressInfo {
            phase: 3,
            phase_percent: 100,
            total_percent: 55,
            items_done: total_items,
            items_total: total_items,
            ..ProgressInfo::default()
        },
    );

    Ok(())
}
