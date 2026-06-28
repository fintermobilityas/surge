use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::warn;

use crate::archive::extractor::extract_file_to_with_progress;
use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{cache_path_for_key, fetch_or_reuse_file};
use crate::releases::manifest::ReleaseEntry;

use super::progress::{
    ProgressInfo, average_speed_bytes_per_sec, clamp_progress_percent_u64, emit_progress, phase_total_percent,
    saturating_i64_from_u64,
};
use super::progress_substep::{HEARTBEAT_INTERVAL, PhaseProgressEmitter, labels as apply_phase};
use super::{ApplyStrategy, UpdateInfo, UpdateManager};

mod base;
mod delta;
mod installed_app;

use self::base::{BaseFullArchiveSource, restore_base_full_archive, restore_release_graph_base_full_archive};
use self::delta::apply_target_deltas;
pub(super) use self::installed_app::find_previous_app_dir;
#[cfg(test)]
pub(super) use self::installed_app::synthesize_current_full_archive_from_installed_app;

pub(super) async fn materialize_update_payload<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    if matches!(info.apply_strategy, ApplyStrategy::Delta) {
        match materialize_delta_payload(
            manager,
            info,
            staging_dir,
            artifact_cache_dir,
            extract_dir,
            progress,
            progress_emitter,
        )
        .await
        {
            Ok(path) => Ok(path),
            Err(SurgeError::Cancelled) => Err(SurgeError::Cancelled),
            Err(delta_error) => {
                materialize_full_payload_after_delta_failure(
                    manager,
                    info,
                    staging_dir,
                    artifact_cache_dir,
                    extract_dir,
                    progress,
                    delta_error,
                )
                .await
            }
        }
    } else {
        materialize_full_payload(info, staging_dir, extract_dir, progress)
    }
}

fn materialize_full_payload<F>(
    info: &UpdateInfo,
    staging_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    materialize_full_payload_with_progress_range(info, staging_dir, extract_dir, progress, 60, 75, 80, 85)
}

fn materialize_full_payload_with_progress_range<F>(
    info: &UpdateInfo,
    staging_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    extract_total_percent_start: i32,
    extract_total_percent_end: i32,
    apply_total_percent_start: i32,
    apply_total_percent_end: i32,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
    let archive_path = staging_dir.join(&latest.full_filename);
    extract_archive_with_progress(
        &archive_path,
        extract_dir,
        progress,
        extract_total_percent_start,
        extract_total_percent_end,
    )?;

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            total_percent: apply_total_percent_start,
            ..ProgressInfo::default()
        },
    );
    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            phase_percent: 100,
            total_percent: apply_total_percent_end,
            ..ProgressInfo::default()
        },
    );

    Ok(extract_dir.to_path_buf())
}

async fn materialize_full_payload_after_delta_failure<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    delta_error: SurgeError,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;

    warn!(
        version = %latest.version,
        error = %delta_error,
        "Delta materialization failed; falling back to the full package"
    );

    let cache_path = cache_path_for_key(artifact_cache_dir, &latest.full_filename)?;
    let download_started_at = Instant::now();
    let full_size = u64::try_from(latest.full_size.max(0)).unwrap_or(u64::MAX);
    emit_progress(
        progress,
        ProgressInfo {
            phase: 2,
            phase_label: "download full package fallback",
            total_percent: 60,
            bytes_total: saturating_i64_from_u64(full_size),
            items_total: 1,
            ..ProgressInfo::default()
        },
    );
    let progress_for_download = progress.cloned();
    let download_progress = move |done: u64, total: u64| {
        let total = total.max(done).max(full_size);
        let phase_percent = clamp_progress_percent_u64(done, total);
        emit_progress(
            progress_for_download.as_ref(),
            ProgressInfo {
                phase: 2,
                phase_label: "download full package fallback",
                phase_percent,
                total_percent: phase_total_percent(60, 15, phase_percent),
                bytes_done: saturating_i64_from_u64(done),
                bytes_total: saturating_i64_from_u64(total),
                items_done: i64::from(phase_percent == 100),
                items_total: 1,
                speed_bytes_per_sec: average_speed_bytes_per_sec(done, download_started_at),
            },
        );
    };

    fetch_or_reuse_file(
        manager.storage.as_ref(),
        &latest.full_filename,
        &cache_path,
        &latest.full_sha256,
        Some(&download_progress),
    )
    .await
    .map_err(|fallback_error| {
        SurgeError::Update(format!(
            "Delta materialization failed: {delta_error}; full package fallback failed: {fallback_error}"
        ))
    })?;

    let stage_path = staging_dir.join(&latest.full_filename);
    if let Some(parent) = stage_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(&cache_path, &stage_path).await?;
    if extract_dir.exists() {
        tokio::fs::remove_dir_all(extract_dir).await?;
    }
    tokio::fs::create_dir_all(extract_dir).await?;

    materialize_full_payload_with_progress_range(info, staging_dir, extract_dir, progress, 75, 90, 90, 90)
}

async fn materialize_delta_payload<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let apply_delta_total_items = i64::try_from(info.apply_releases.len()).unwrap_or(i64::MAX);
    let apply_delta_total_bytes = info
        .apply_releases
        .iter()
        .filter_map(ReleaseEntry::selected_delta)
        .fold(0i64, |acc, delta| acc.saturating_add(delta.size.max(0)));

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            total_percent: 60,
            bytes_total: apply_delta_total_bytes,
            items_total: apply_delta_total_items,
            ..ProgressInfo::default()
        },
    );

    let base_archive = restore_base_full_archive(manager, info, artifact_cache_dir, progress, progress_emitter).await?;
    let rebuilt_archive = match apply_target_deltas(
        manager,
        info,
        staging_dir,
        base_archive.archive,
        progress,
        progress_emitter,
        apply_delta_total_items,
        apply_delta_total_bytes,
    )
    .await
    {
        Ok(archive) => archive,
        Err(delta_error)
            if base_archive.source == BaseFullArchiveSource::InstalledApp
                && should_retry_delta_with_release_graph(&delta_error) =>
        {
            warn!(
                error = %delta_error,
                "Installed app base did not produce a valid delta result; retrying with release graph base"
            );
            let release_graph_base =
                restore_release_graph_base_full_archive(manager, artifact_cache_dir, progress, progress_emitter)
                    .await?;
            apply_target_deltas(
                manager,
                info,
                staging_dir,
                release_graph_base.archive,
                progress,
                progress_emitter,
                apply_delta_total_items,
                apply_delta_total_bytes,
            )
            .await
            .map_err(|retry_error| {
                SurgeError::Update(format!(
                    "Installed-app delta application failed: {delta_error}; release-graph retry failed: {retry_error}"
                ))
            })?
        }
        Err(delta_error) => return Err(delta_error),
    };

    let rebuilt_archive_path = staging_dir.join("rebuilt-full.tar.zst");
    progress_emitter
        .run_with_heartbeat(
            5,
            apply_phase::WRITING_REBUILT_PACKAGE,
            80,
            HEARTBEAT_INTERVAL,
            tokio::fs::write(&rebuilt_archive_path, &rebuilt_archive),
        )
        .await?;
    progress_emitter.emit_substep(5, apply_phase::EXTRACTING_REBUILT_PACKAGE, 80);
    extract_archive_with_progress(&rebuilt_archive_path, extract_dir, progress, 80, 90)?;

    let source = extract_dir.join(&info.latest_version);
    if source.exists() {
        Ok(source)
    } else {
        Ok(extract_dir.to_path_buf())
    }
}

fn should_retry_delta_with_release_graph(error: &SurgeError) -> bool {
    matches!(
        error,
        SurgeError::Update(_) | SurgeError::Integrity(_) | SurgeError::Diff(_) | SurgeError::Archive(_)
    )
}

fn extract_archive_with_progress<F>(
    archive_path: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    total_percent_start: i32,
    total_percent_end: i32,
) -> Result<()>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    emit_progress(
        progress,
        ProgressInfo {
            phase: 4,
            total_percent: total_percent_start,
            ..ProgressInfo::default()
        },
    );

    let extract_started_at = Instant::now();
    let progress_for_extract = progress.cloned();
    let extract_progress = move |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
        let phase_percent = if bytes_total > 0 {
            clamp_progress_percent_u64(bytes_done, bytes_total)
        } else {
            clamp_progress_percent_u64(items_done, items_total)
        };
        emit_progress(
            progress_for_extract.as_ref(),
            ProgressInfo {
                phase: 4,
                phase_percent,
                total_percent: phase_total_percent(
                    total_percent_start,
                    total_percent_end - total_percent_start,
                    phase_percent,
                ),
                bytes_done: saturating_i64_from_u64(bytes_done),
                bytes_total: saturating_i64_from_u64(bytes_total),
                items_done: saturating_i64_from_u64(items_done),
                items_total: saturating_i64_from_u64(items_total),
                speed_bytes_per_sec: average_speed_bytes_per_sec(bytes_done, extract_started_at),
                ..ProgressInfo::default()
            },
        );
    };

    extract_file_to_with_progress(archive_path, extract_dir, Some(&extract_progress))?;

    emit_progress(
        progress,
        ProgressInfo {
            phase: 4,
            phase_percent: 100,
            total_percent: total_percent_end,
            ..ProgressInfo::default()
        },
    );

    Ok(())
}
