use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tracing::debug;

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::error::{Result, SurgeError};
use crate::platform::detect::current_rid;
use crate::releases::manifest::{
    PATCH_FORMAT_SPARSE_FILE_OPS_V1, ReleaseEntry, ReleaseIndex, decompress_release_index,
};
use crate::releases::restore::{
    RebuiltFullCachePolicy, RestoreOptions, RestoreProgress, find_release_for_version_rid,
    restore_full_archive_for_version_with_options,
};

use super::super::progress::{
    ProgressInfo, average_speed_bytes_per_sec, clamp_progress_percent, emit_progress, phase_total_percent,
};
use super::super::progress_substep::{HEARTBEAT_INTERVAL, PhaseProgressEmitter, labels as apply_phase};
use super::super::{UpdateInfo, UpdateManager};
use super::installed_app::synthesize_current_full_archive_from_installed_app;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BaseFullArchiveSource {
    InstalledApp,
    ReleaseGraph,
}

pub(super) struct BaseFullArchive {
    pub(super) archive: Vec<u8>,
    pub(super) source: BaseFullArchiveSource,
}

pub(super) async fn restore_base_full_archive<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    artifact_cache_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<BaseFullArchive>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let (index, rid, current_release) = load_current_release_context(manager).await?;

    if should_prefer_installed_app_base(info) {
        match restore_base_full_archive_from_installed_app(
            manager,
            &current_release,
            artifact_cache_dir,
            progress_emitter,
        )
        .await
        {
            Ok(archive) => {
                return Ok(BaseFullArchive {
                    archive,
                    source: BaseFullArchiveSource::InstalledApp,
                });
            }
            Err(installed_app_err) => {
                debug!(
                    version = %manager.current_version,
                    error = %installed_app_err,
                    "Installed app current package restoration failed; falling back to release graph"
                );
            }
        }
    }

    restore_base_full_archive_from_release_graph(
        manager,
        &index,
        &rid,
        &current_release,
        artifact_cache_dir,
        progress,
        progress_emitter,
    )
    .await
    .map(|archive| BaseFullArchive {
        archive,
        source: BaseFullArchiveSource::ReleaseGraph,
    })
}

pub(super) async fn restore_release_graph_base_full_archive<F>(
    manager: &UpdateManager,
    artifact_cache_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<BaseFullArchive>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let (index, rid, current_release) = load_current_release_context(manager).await?;
    restore_base_full_archive_from_release_graph(
        manager,
        &index,
        &rid,
        &current_release,
        artifact_cache_dir,
        progress,
        progress_emitter,
    )
    .await
    .map(|archive| BaseFullArchive {
        archive,
        source: BaseFullArchiveSource::ReleaseGraph,
    })
}

async fn load_current_release_context(manager: &UpdateManager) -> Result<(ReleaseIndex, String, ReleaseEntry)> {
    let index = if let Some(cached) = &manager.cached_index {
        cached.clone()
    } else {
        let data = manager.storage.get_object(RELEASES_FILE_COMPRESSED).await?;
        decompress_release_index(&data)?
    };
    let rid = current_rid();
    let current_release = find_release_for_version_rid(&index, &rid, &manager.current_version)
        .cloned()
        .ok_or_else(|| {
            SurgeError::Update(format!(
                "Current release {} ({rid}) was not found in the release index",
                manager.current_version
            ))
        })?;
    Ok((index, rid, current_release))
}

async fn restore_base_full_archive_from_release_graph<F>(
    manager: &UpdateManager,
    index: &ReleaseIndex,
    rid: &str,
    current_release: &ReleaseEntry,
    artifact_cache_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<Vec<u8>>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let restore_started_at = Instant::now();
    let progress_for_restore = progress.cloned();
    let restore_progress = |restore: RestoreProgress| {
        let phase_percent = if restore.bytes_total > 0 {
            clamp_progress_percent(restore.bytes_done, restore.bytes_total)
        } else {
            clamp_progress_percent(restore.items_done, restore.items_total)
        };
        emit_progress(
            progress_for_restore.as_ref(),
            ProgressInfo {
                phase: 5,
                phase_label: apply_phase::RESTORING_CURRENT_PACKAGE_FROM_RELEASE_GRAPH,
                phase_percent,
                total_percent: phase_total_percent(60, 10, phase_percent),
                bytes_done: restore.bytes_done,
                bytes_total: restore.bytes_total,
                items_done: restore.items_done,
                items_total: restore.items_total,
                speed_bytes_per_sec: average_speed_bytes_per_sec(
                    u64::try_from(restore.bytes_done.max(0)).unwrap_or(u64::MAX),
                    restore_started_at,
                ),
            },
        );
        progress_emitter.persist_current_phase(apply_phase::RESTORING_CURRENT_PACKAGE_FROM_RELEASE_GRAPH);
    };

    let restore_future = restore_full_archive_for_version_with_options(
        manager.storage.as_ref(),
        index,
        rid,
        &manager.current_version,
        RestoreOptions {
            cache_dir: Some(artifact_cache_dir),
            progress: Some(&restore_progress),
            rebuilt_full_cache_policy: RebuiltFullCachePolicy::TargetOnly,
        },
    );
    match progress_emitter
        .run_with_heartbeat(
            5,
            apply_phase::RESTORING_CURRENT_PACKAGE_FROM_RELEASE_GRAPH,
            60,
            HEARTBEAT_INTERVAL,
            restore_future,
        )
        .await
    {
        Ok(archive) => Ok(archive),
        Err(restore_err) => synthesize_current_full_archive_from_installed_app(
            &manager.install_dir,
            &manager.current_version,
            current_release,
            artifact_cache_dir,
            &manager.ctx,
        )
        .map_err(|fallback_err| {
            SurgeError::Update(format!(
                "Failed to restore base full archive for {}: {restore_err}; installed-app fallback failed: {fallback_err}",
                manager.current_version
            ))
        }),
    }
}

async fn restore_base_full_archive_from_installed_app<F>(
    manager: &UpdateManager,
    current_release: &ReleaseEntry,
    artifact_cache_dir: &Path,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<Vec<u8>>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let install_dir = manager.install_dir.clone();
    let current_version = manager.current_version.clone();
    let current_release = current_release.clone();
    let artifact_cache_dir = artifact_cache_dir.to_path_buf();
    let ctx = Arc::clone(&manager.ctx);

    progress_emitter
        .run_with_heartbeat(
            5,
            apply_phase::RESTORING_CURRENT_PACKAGE_FROM_INSTALLED_APP,
            60,
            HEARTBEAT_INTERVAL,
            tokio::task::spawn_blocking(move || {
                synthesize_current_full_archive_from_installed_app(
                    &install_dir,
                    &current_version,
                    &current_release,
                    &artifact_cache_dir,
                    &ctx,
                )
            }),
        )
        .await
        .map_err(|e| SurgeError::Update(format!("Failed to join installed app archive synthesis task: {e}")))?
}

fn should_prefer_installed_app_base(info: &UpdateInfo) -> bool {
    info.apply_releases.iter().all(|release| {
        release.selected_delta().is_some_and(|delta| {
            delta
                .patch_format
                .trim()
                .eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1)
        })
    })
}
