//! Phase 6 (finalize) execution for the update pipeline.
//!
//! Owns the substeps that run after the update payload is materialized on
//! disk: stop the supervisor, atomic-swap the active app directory, copy
//! persistent assets, write the runtime manifest, install shortcuts, prune
//! old version snapshots and cached artifacts, run the post-update hook,
//! and (re)start the supervisor. Each substep emits a labelled progress
//! event and persists the same label into the in-progress
//! [`UpdateStatusRecord`] via [`PhaseProgressEmitter`].

use std::path::Path;
use std::time::Duration;

use tracing::{debug, warn};

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::error::{Result, SurgeError};
use crate::install::{
    InstallProfile, RuntimeManifestMetadata, copy_persistent_assets, prune_version_snapshots,
    storage_provider_manifest_name, write_runtime_manifest,
};
use crate::installer_package::prune_install_artifact_cache_dir_with_stats;
use crate::platform::fs::atomic_rename;
use crate::platform::shortcuts::install_shortcuts;
use crate::releases::manifest::decompress_release_index;
use crate::releases::restore::{
    retained_artifacts_for_cache_policy, retained_artifacts_for_cache_policy_without_index,
};
use crate::supervisor::state::supervisor_pid_file;

use super::progress::{ProgressInfo, emit_progress};
use super::progress_substep::{HEARTBEAT_INTERVAL, PhaseProgressEmitter, labels as finalize_phase};
use super::{RELEASE_GRAPH_CHECKPOINT_FULLS, SupervisorRestartOutcome, UpdateInfo, UpdateManager, apply, lifecycle};

/// Bound the post-finalize storage read used to pick which artifacts to keep
/// in the local cache. Pruning is best-effort, so an unreachable storage
/// backend must not stall the rest of finalize indefinitely.
const PRUNE_INDEX_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

#[allow(clippy::too_many_lines)]
pub(super) async fn finalize_update<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    extracted_final_dir: &Path,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<SupervisorRestartOutcome>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    emit_progress(
        progress_emitter.progress,
        ProgressInfo {
            phase: 6,
            total_percent: 90,
            ..ProgressInfo::default()
        },
    );
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
    let active_app_dir = manager.install_dir.join("app");
    let next_app_dir = manager.install_dir.join(".surge-app-next");
    let previous_swap_dir = manager.install_dir.join(".surge-app-prev");
    let supervisor_was_running = !latest.supervisor_id.trim().is_empty()
        && supervisor_pid_file(&manager.install_dir, &latest.supervisor_id).is_file();

    if supervisor_was_running {
        progress_emitter
            .run_with_heartbeat(
                6,
                finalize_phase::STOPPING_SUPERVISOR,
                91,
                HEARTBEAT_INTERVAL,
                lifecycle::request_supervisor_shutdown(&manager.install_dir, &latest.supervisor_id),
            )
            .await?;
    } else {
        lifecycle::request_supervisor_shutdown(&manager.install_dir, &latest.supervisor_id).await?;
    }

    progress_emitter.emit_substep(6, finalize_phase::PREPARING_SWAP, 92);
    if next_app_dir.exists() {
        tokio::fs::remove_dir_all(&next_app_dir).await?;
    }
    if previous_swap_dir.exists() {
        tokio::fs::remove_dir_all(&previous_swap_dir).await?;
    }

    // Legacy installs may still be on `app-{version}` layout.
    let fallback_previous_app_dir = if active_app_dir.is_dir() {
        None
    } else {
        apply::find_previous_app_dir(&manager.install_dir, &manager.current_version)
    };

    progress_emitter.emit_substep(6, finalize_phase::SWAPPING_APP_DIRECTORY, 93);
    atomic_rename(extracted_final_dir, &next_app_dir)?;

    if active_app_dir.is_dir() {
        atomic_rename(&active_app_dir, &previous_swap_dir)?;
    }
    if let Err(err) = atomic_rename(&next_app_dir, &active_app_dir) {
        // Best effort rollback to previous active content.
        if previous_swap_dir.is_dir() && !active_app_dir.exists() {
            let _ = atomic_rename(&previous_swap_dir, &active_app_dir);
        }
        return Err(err);
    }

    let previous_app_dir_for_assets = if previous_swap_dir.is_dir() {
        Some(previous_swap_dir.as_path())
    } else {
        fallback_previous_app_dir.as_deref()
    };

    if !latest.persistent_assets.is_empty() && previous_app_dir_for_assets.is_some() {
        progress_emitter.emit_substep(6, finalize_phase::COPYING_PERSISTENT_ASSETS, 94);
        if let Some(previous) = previous_app_dir_for_assets {
            copy_persistent_assets(previous, &active_app_dir, &latest.persistent_assets)?;
        }
    } else if !latest.persistent_assets.is_empty() {
        debug!(
            version = %latest.version,
            "No previous app directory found; skipping persistent asset carry-over"
        );
    }

    progress_emitter.emit_substep(6, finalize_phase::WRITING_RUNTIME_MANIFEST, 95);
    let storage_cfg = manager.ctx.storage_config();
    let runtime_manifest_profile = InstallProfile::new(
        &manager.app_id,
        latest.display_name(&manager.app_id),
        &latest.main_exe,
        &latest.install_directory,
        &latest.supervisor_id,
        &latest.icon,
        &latest.shortcuts,
        &latest.persistent_assets,
        &latest.environment,
    );
    let runtime_manifest_metadata = RuntimeManifestMetadata::new(
        &latest.version,
        &manager.channel,
        storage_provider_manifest_name(storage_cfg.provider),
        &storage_cfg.bucket,
        &storage_cfg.region,
        &storage_cfg.endpoint,
    );
    write_runtime_manifest(&active_app_dir, &runtime_manifest_profile, &runtime_manifest_metadata)?;

    if !latest.shortcuts.is_empty() {
        progress_emitter.emit_substep(6, finalize_phase::INSTALLING_SHORTCUTS, 96);
        match install_shortcuts(
            &manager.app_id,
            latest.display_name(&manager.app_id),
            &active_app_dir,
            &latest.main_exe,
            &latest.supervisor_id,
            &latest.icon,
            &latest.shortcuts,
            &latest.environment,
        ) {
            Ok(()) => {
                debug!(version = %latest.version, "Installed shortcuts");
            }
            Err(e) => {
                warn!(
                    version = %latest.version,
                    error = %e,
                    "Failed to install shortcuts (continuing)"
                );
            }
        }
    }

    progress_emitter.emit_substep(6, finalize_phase::PRUNING_OLD_VERSIONS, 97);
    if previous_swap_dir.is_dir() {
        let previous_version_dir = manager.install_dir.join(format!("app-{}", manager.current_version));
        if !manager.current_version.trim().is_empty()
            && previous_version_dir != active_app_dir
            && !previous_version_dir.exists()
        {
            if let Err(e) = atomic_rename(&previous_swap_dir, &previous_version_dir) {
                warn!(
                    previous = %previous_swap_dir.display(),
                    target = %previous_version_dir.display(),
                    error = %e,
                    "Failed to preserve previous active directory snapshot"
                );
                let _ = tokio::fs::remove_dir_all(&previous_swap_dir).await;
            }
        } else {
            let _ = tokio::fs::remove_dir_all(&previous_swap_dir).await;
        }
    }
    match prune_version_snapshots(&manager.install_dir, manager.release_retention_limit) {
        Ok(0) => {}
        Ok(pruned) => {
            debug!(
                pruned,
                retained = manager.release_retention_limit,
                "Pruned stale installed app version snapshots"
            );
        }
        Err(e) => {
            warn!(error = %e, "Failed to prune installed app version snapshots");
        }
    }

    // Clean up staging directory
    if staging_dir.exists() {
        let _ = tokio::fs::remove_dir_all(staging_dir).await;
    }

    let prune_index = if let Some(cached) = &manager.cached_index {
        Some(cached.clone())
    } else {
        match tokio::time::timeout(
            PRUNE_INDEX_FETCH_TIMEOUT,
            manager.storage.get_object(RELEASES_FILE_COMPRESSED),
        )
        .await
        {
            Ok(Ok(data)) => Some(decompress_release_index(&data)?),
            Ok(Err(SurgeError::NotFound(_))) => None,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                warn!(
                    timeout_secs = PRUNE_INDEX_FETCH_TIMEOUT.as_secs(),
                    "Timed out fetching release index for artifact pruning; skipping prune step"
                );
                None
            }
        }
    };
    let retained_artifacts = if let Some(index) = prune_index {
        Some(retained_artifacts_for_cache_policy(
            &index,
            manager.artifact_retention_policy,
            &latest.full_filename,
            RELEASE_GRAPH_CHECKPOINT_FULLS,
        ))
    } else {
        retained_artifacts_for_cache_policy_without_index(manager.artifact_retention_policy, &latest.full_filename)
    };
    if let Some(retained_artifacts) = retained_artifacts {
        match prune_install_artifact_cache_dir_with_stats(artifact_cache_dir, &retained_artifacts) {
            Ok(result) if result.pruned_artifact_count == 0 => {}
            Ok(result) => {
                debug!(
                    pruned = result.pruned_artifact_count,
                    retained = result.retained_policy_key_count,
                    "Pruned stale local artifact cache entries"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to prune local artifact cache");
            }
        }
    }

    progress_emitter.emit_substep(6, finalize_phase::POST_UPDATE_HOOK, 98);
    lifecycle::invoke_post_update_hook(&manager.install_dir, &active_app_dir, latest);

    let restart_outcome = if supervisor_was_running {
        progress_emitter.emit_substep(6, finalize_phase::RESTARTING_SUPERVISOR, 99);
        lifecycle::restart_supervisor_after_update(&manager.install_dir, &active_app_dir, latest)
    } else {
        SupervisorRestartOutcome::NotApplicable
    };
    match lifecycle::terminate_superseded_app_processes(&manager.install_dir, &active_app_dir, &latest.main_exe) {
        Ok(0) => {}
        Ok(terminated) => {
            debug!(
                version = %latest.version,
                terminated,
                "Terminated stale app processes from superseded install directories"
            );
        }
        Err(e) => return Err(e),
    }

    emit_progress(
        progress_emitter.progress,
        ProgressInfo {
            phase: 6,
            phase_percent: 100,
            total_percent: 100,
            ..ProgressInfo::default()
        },
    );

    Ok(restart_outcome)
}
