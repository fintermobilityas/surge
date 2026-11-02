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
use crate::releases::manifest::{ReleaseEntry, decompress_release_index};
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
    finalize_update_with_pre_restart(
        manager,
        info,
        extracted_final_dir,
        staging_dir,
        artifact_cache_dir,
        progress_emitter,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_lines)]
pub(in crate::update::manager) async fn finalize_update_with_pre_restart<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    extracted_final_dir: &Path,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
    pre_restart: Option<&(dyn Fn() -> Result<()> + Send + Sync)>,
    restart_args: Option<&[String]>,
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
    let fallback_previous_app_dir = if active_app_dir.is_dir() {
        None
    } else {
        apply::find_previous_app_dir(&manager.install_dir, &manager.current_version)
    };

    // Verify the materialized payload before anything destructive happens: a
    // truncated payload must leave the previous version and its supervisor
    // untouched instead of swapping a broken tree in and stopping the node.
    progress_emitter.emit_substep(6, finalize_phase::VERIFYING_PAYLOAD, 90);
    let payload_survey = crate::install::verify_payload_for_activation(
        extracted_final_dir,
        &latest.main_exe,
        Some(crate::platform::process::supervisor_binary_name()),
    )?;
    debug!(
        version = %latest.version,
        files = payload_survey.files,
        zero_byte_files = payload_survey.zero_byte_files,
        bytes = payload_survey.bytes,
        "Verified update payload before activation"
    );

    let current_app_dir = if active_app_dir.is_dir() {
        Some(active_app_dir.as_path())
    } else {
        fallback_previous_app_dir.as_deref()
    };
    #[cfg(unix)]
    if current_app_dir.is_none() && manager.current_release_identity.is_some() {
        return Err(SurgeError::Update(
            "Installed application directory disappeared after the update check; refusing to swap the active application"
                .to_string(),
        ));
    }
    #[cfg(unix)]
    let (current_main_exe, current_supervisor_id) = if current_app_dir.is_some() {
        let installed_identity = super::current_install::load(manager)?;
        if installed_identity != manager.current_release_identity {
            return Err(SurgeError::Update(
                "Installed application identity changed after the update check; refusing to swap the active application"
                    .to_string(),
            ));
        }
        let current = manager.current_release_identity.as_ref().ok_or_else(|| {
            SurgeError::Update(format!(
                "Current release {} is unavailable; refusing to swap the active application without its locally persisted process identity",
                manager.current_version
            ))
        })?;
        (current.main_exe.as_str(), current.supervisor_id.as_str())
    } else {
        ("", "")
    };
    #[cfg(not(unix))]
    let (current_main_exe, current_supervisor_id) = manager.current_release_identity.as_ref().map_or_else(
        || (latest.main_exe.as_str(), latest.supervisor_id.as_str()),
        |current| (current.main_exe.as_str(), current.supervisor_id.as_str()),
    );
    let supervisor_was_running = !current_supervisor_id.trim().is_empty()
        && supervisor_pid_file(&manager.install_dir, current_supervisor_id).is_file();

    // Supervision may only be restored when the previous version actually stopped:
    // a supervisor started while the old app still runs would watch the updater pid
    // and launch a second instance of the app once the updater exits.
    let mut previous_app_quiesced = false;
    let swap_result: Result<()> = async {
        if supervisor_was_running {
            progress_emitter
                .run_with_heartbeat(
                    6,
                    finalize_phase::STOPPING_SUPERVISOR,
                    91,
                    HEARTBEAT_INTERVAL,
                    lifecycle::request_supervisor_shutdown(&manager.install_dir, current_supervisor_id),
                )
                .await?;
        } else {
            lifecycle::request_supervisor_shutdown(&manager.install_dir, current_supervisor_id).await?;
        }

        progress_emitter.emit_substep(6, finalize_phase::QUIESCING_ACTIVE_APP, 92);
        if let Some(current_app_dir) = current_app_dir {
            lifecycle::terminate_active_app_processes_before_swap(
                current_app_dir,
                current_main_exe,
                manager.allow_in_process_swap,
            )?;
        }
        previous_app_quiesced = true;
        #[cfg(unix)]
        if previous_swap_dir.is_dir() {
            let previous_swap_identity = super::current_install::load_previous_swap(manager, &previous_swap_dir)?
                .ok_or_else(|| {
                    SurgeError::Update(
                        "Previous swap directory has no persisted process identity; refusing to delete or reuse it"
                            .to_string(),
                    )
                })?;
            lifecycle::request_supervisor_shutdown(&manager.install_dir, &previous_swap_identity.supervisor_id).await?;
            lifecycle::terminate_active_app_processes_before_swap(
                &previous_swap_dir,
                &previous_swap_identity.main_exe,
                manager.allow_in_process_swap,
            )?;
        }

        progress_emitter.emit_substep(6, finalize_phase::PREPARING_SWAP, 92);
        if next_app_dir.exists() {
            tokio::fs::remove_dir_all(&next_app_dir).await?;
        }
        if previous_swap_dir.exists() {
            tokio::fs::remove_dir_all(&previous_swap_dir).await?;
        }

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

        Ok(())
    }
    .await;

    if let Err(err) = swap_result {
        let _ = restore_supervision_after_failed_swap(
            manager,
            latest,
            supervisor_was_running,
            previous_app_quiesced,
            &active_app_dir,
            &previous_swap_dir,
            &err,
        );
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

    // Start the replacement watch-supervisor as soon as the swapped app is
    // usable (directory swapped, persistent assets carried over, runtime
    // manifest written), before the best-effort shortcut/prune/hook steps that
    // can block for many seconds. This shrinks the window in which no
    // supervisor is watching the app. The start no longer depends on a
    // supervisor having been running before the update: if the release
    // configures supervision, a fresh watch supervisor is always started so a
    // supervisor that died before the update is recovered here.
    let defer_restart = restart_args.is_some();
    let mut restart_outcome = if latest.supervisor_id.trim().is_empty() || defer_restart {
        SupervisorRestartOutcome::NotApplicable
    } else {
        if let Some(pre_restart) = pre_restart {
            pre_restart()?;
        }
        progress_emitter.emit_substep(6, finalize_phase::RESTARTING_SUPERVISOR, 96);
        restart_args.map_or_else(
            || lifecycle::restart_supervisor_after_update(&manager.install_dir, &active_app_dir, latest),
            |restart_args| {
                lifecycle::restart_supervisor_after_update_with_args(
                    &manager.install_dir,
                    &active_app_dir,
                    latest,
                    crate::platform::process::current_pid(),
                    restart_args,
                    Some(&latest.version),
                )
            },
        )
    };

    if !latest.shortcuts.is_empty() {
        progress_emitter.emit_substep(6, finalize_phase::INSTALLING_SHORTCUTS, 97);
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

    progress_emitter.emit_substep(6, finalize_phase::PRUNING_OLD_VERSIONS, 98);
    if !defer_restart && previous_swap_dir.is_dir() {
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

    if defer_restart {
        let pre_restart = pre_restart.ok_or_else(|| {
            SurgeError::Update("Deferred supervisor restart requires terminal status persistence".to_string())
        })?;
        pre_restart()?;
        emit_progress(
            progress_emitter.progress,
            ProgressInfo {
                phase: 6,
                phase_label: finalize_phase::POST_UPDATE_HOOK,
                total_percent: 99,
                ..ProgressInfo::default()
            },
        );
    } else {
        progress_emitter.emit_substep(6, finalize_phase::POST_UPDATE_HOOK, 99);
    }
    lifecycle::invoke_post_update_hook(&manager.install_dir, &active_app_dir, latest);

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

    if defer_restart {
        emit_progress(
            progress_emitter.progress,
            ProgressInfo {
                phase: 6,
                phase_label: finalize_phase::RESTARTING_SUPERVISOR,
                total_percent: 99,
                ..ProgressInfo::default()
            },
        );
        restart_outcome = lifecycle::restart_supervisor_after_update_with_args(
            &manager.install_dir,
            &active_app_dir,
            latest,
            crate::platform::process::current_pid(),
            restart_args.unwrap_or_default(),
            Some(&latest.version),
        );
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

/// Restart supervision of the currently active app directory after finalize
/// failed once the supervisor had already been stopped.
///
/// Without this, any I/O or rename failure in the swap window leaves a node with
/// no supervisor watching it. The original finalize error is still returned to
/// the caller; a recovery restart that cannot be confirmed is only logged.
/// What [`restore_supervision_after_failed_swap`] did, so the decision can be
/// asserted without spawning a supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisionRecovery {
    /// No supervisor was running before the update, so there is nothing to restore.
    NotNeeded,
    /// The previous version never stopped; starting a supervisor would duplicate it.
    SkippedPreviousAppStillRunning,
    /// A replacement supervisor was started for the previous version.
    Attempted,
}

fn restore_supervision_after_failed_swap(
    manager: &UpdateManager,
    latest: &ReleaseEntry,
    supervisor_was_running: bool,
    previous_app_quiesced: bool,
    active_app_dir: &Path,
    previous_swap_dir: &Path,
    error: &SurgeError,
) -> SupervisionRecovery {
    if !supervisor_was_running {
        return SupervisionRecovery::NotNeeded;
    }

    // A replacement supervisor watches this updater process and starts the app once
    // the updater exits. If the previous version never stopped, that would add a
    // second instance beside the one still running, so leave it alone and say so.
    if !previous_app_quiesced {
        warn!(
            version = %manager.current_version,
            error = %error,
            "Update finalize failed before the previous application stopped; leaving it running unsupervised instead of starting a supervisor that would launch a second instance"
        );
        return SupervisionRecovery::SkippedPreviousAppStillRunning;
    }

    let app_dir = if active_app_dir.is_dir() {
        active_app_dir
    } else {
        previous_swap_dir
    };

    // Every failure path in the swap window leaves the previous version active,
    // never the new payload, so supervision has to be restored with the identity
    // that is actually on disk. Supervising with the target release would also
    // record the target version as the handoff version and hide the version drift.
    let mut recovery_release = latest.clone();
    recovery_release.version.clone_from(&manager.current_version);
    if let Some(current) = manager.current_release_identity.as_ref() {
        recovery_release.main_exe.clone_from(&current.main_exe);
        recovery_release.supervisor_id.clone_from(&current.supervisor_id);
    }

    warn!(
        version = %recovery_release.version,
        app_dir = %app_dir.display(),
        error = %error,
        "Update finalize failed after the supervisor was stopped; restoring supervision of the active application"
    );

    match lifecycle::restart_supervisor_after_update(&manager.install_dir, app_dir, &recovery_release) {
        SupervisorRestartOutcome::PendingRestart { reason, .. } => warn!(
            version = %recovery_release.version,
            app_dir = %app_dir.display(),
            reason,
            "Recovery supervisor restart after a failed finalize was not confirmed; the node may need manual intervention"
        ),
        SupervisorRestartOutcome::NotApplicable | SupervisorRestartOutcome::ExternalFinalizeScheduled => {}
    }

    SupervisionRecovery::Attempted
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::context::{Context, StorageProvider};
    use crate::update::manager::ApplyStrategy;
    use crate::update::status::{self, UpdateStatusRecord};

    fn write_file(root: &Path, relative: &str, contents: &[u8]) -> PathBuf {
        let path = root.join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("parent directory should be created");
        }
        std::fs::write(&path, contents).expect("file should be written");
        path
    }

    fn target_release(app_id: &str, supervisor_id: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: "2.0.0".to_string(),
            channels: vec!["stable".to_string()],
            main_exe: app_id.to_string(),
            install_directory: app_id.to_string(),
            supervisor_id: supervisor_id.to_string(),
            ..ReleaseEntry::default()
        }
    }

    /// When the swap fails before the previous version stopped, a replacement
    /// supervisor must NOT be started: it would watch this updater process and
    /// launch a second copy of the app beside the one still running once the
    /// updater exits. Leaving it unsupervised matches the behaviour before the
    /// recovery path existed.
    #[test]
    fn supervision_is_not_restored_while_the_previous_app_is_still_running() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_dir = tmp.path().join("install");
        let active_app_dir = install_dir.join("app");
        let app_id = "sampleapp";
        let supervisor_id = "sampleapp-supervisor";
        write_file(&active_app_dir, app_id, b"previous release binary");

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            tmp.path().to_str().expect("store root path"),
            "",
            "",
            "",
            "",
        );
        let mut manager = UpdateManager::new(
            ctx,
            app_id,
            "1.0.0",
            "stable",
            install_dir.to_str().expect("install dir path"),
        )
        .expect("update manager should be created");
        manager.current_release_identity = Some(super::super::current_install::ReleaseIdentity {
            version: "1.0.0".to_string(),
            main_exe: app_id.to_string(),
            supervisor_id: supervisor_id.to_string(),
            environment: BTreeMap::default(),
        });
        let latest = target_release(app_id, supervisor_id);
        let error = SurgeError::Update("quiescing the active application failed".to_string());

        let recovery = restore_supervision_after_failed_swap(
            &manager,
            &latest,
            true,
            false,
            &active_app_dir,
            &install_dir.join(".surge-app-prev"),
            &error,
        );

        assert_eq!(recovery, SupervisionRecovery::SkippedPreviousAppStillRunning);
    }

    /// Nothing to restore when no supervisor was watching before the update.
    #[test]
    fn supervision_is_not_restored_when_none_was_running() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_dir = tmp.path().join("install");
        std::fs::create_dir_all(&install_dir).expect("install dir should be created");
        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            tmp.path().to_str().expect("store root path"),
            "",
            "",
            "",
            "",
        );
        let manager = UpdateManager::new(
            ctx,
            "sampleapp",
            "1.0.0",
            "stable",
            install_dir.to_str().expect("install dir path"),
        )
        .expect("update manager should be created");
        let latest = target_release("sampleapp", "sampleapp-supervisor");
        let error = SurgeError::Update("swap failed".to_string());

        let recovery = restore_supervision_after_failed_swap(
            &manager,
            &latest,
            false,
            true,
            &install_dir.join("app"),
            &install_dir.join(".surge-app-prev"),
            &error,
        );

        assert_eq!(recovery, SupervisionRecovery::NotNeeded);
    }

    /// A truncated payload (every entry extracted as 0 bytes, including the main
    /// executable) must be rejected before finalize stops anything, so the
    /// previous release and its supervisor keep running.
    #[tokio::test]
    async fn truncated_payload_is_rejected_before_the_supervisor_is_stopped() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let store_root = tmp.path().join("store");
        let install_dir = tmp.path().join("install");
        let staging_dir = tmp.path().join("staging");
        let extracted_dir = staging_dir.join("extracted");
        let artifact_cache_dir = tmp.path().join("artifact-cache");
        let app_id = "sampleapp";
        let supervisor_id = "sampleapp-supervisor";
        for dir in [
            &store_root,
            &install_dir,
            &staging_dir,
            &extracted_dir,
            &artifact_cache_dir,
        ] {
            std::fs::create_dir_all(dir).expect("fixture directory should be created");
        }

        let active_app_dir = install_dir.join("app");
        write_file(&active_app_dir, app_id, b"previous release binary");
        let pid_file = crate::supervisor::state::supervisor_pid_file(&install_dir, supervisor_id);
        write_file(
            &install_dir,
            pid_file
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .expect("pid file name"),
            b"1",
        );
        let stop_file = crate::supervisor::state::supervisor_stop_file(&install_dir, supervisor_id);

        for entry in [app_id, "lib/Some.Dependency.dll", "lib/Other.Dependency.dll"] {
            write_file(&extracted_dir, entry, b"");
        }

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().expect("store root path"),
            "",
            "",
            "",
            "",
        );
        let mut manager = UpdateManager::new(
            ctx,
            app_id,
            "1.0.0",
            "stable",
            install_dir.to_str().expect("install dir path"),
        )
        .expect("update manager should be created");
        manager.current_release_identity = Some(super::super::current_install::ReleaseIdentity {
            version: "1.0.0".to_string(),
            main_exe: app_id.to_string(),
            supervisor_id: supervisor_id.to_string(),
            environment: BTreeMap::default(),
        });

        let latest = target_release(app_id, supervisor_id);
        let info = UpdateInfo {
            available_releases: vec![latest.clone()],
            latest_version: latest.version.clone(),
            delta_available: false,
            download_size: 0,
            apply_releases: vec![latest],
            apply_strategy: ApplyStrategy::Full,
            fallback_reason: None,
        };

        let observed: Arc<Mutex<Vec<ProgressInfo>>> = Arc::new(Mutex::new(Vec::new()));
        let observed_for_callback = Arc::clone(&observed);
        let progress = Arc::new(move |progress: ProgressInfo| {
            observed_for_callback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(progress);
        });
        let status_record =
            UpdateStatusRecord::in_progress(app_id, "1.0.0", "2.0.0", "stable", status::now_utc_rfc3339());
        let progress_emitter = PhaseProgressEmitter {
            progress: Some(&progress),
            install_dir: &manager.install_dir,
            in_progress_template: &status_record,
        };

        let result = finalize_update_with_pre_restart(
            &manager,
            &info,
            &extracted_dir,
            &staging_dir,
            &artifact_cache_dir,
            &progress_emitter,
            None,
            None,
        )
        .await;

        let err = result.expect_err("a truncated payload must fail finalize");
        let message = err.to_string();
        assert!(
            message.contains("main executable 'sampleapp' is 0 bytes"),
            "unexpected error: {message}"
        );

        assert_eq!(
            std::fs::read(active_app_dir.join(app_id)).expect("previous release should still be active"),
            b"previous release binary"
        );
        assert!(
            !install_dir.join(".surge-app-next").exists(),
            "no staged swap directory should be created"
        );
        assert!(
            !install_dir.join(".surge-app-prev").exists(),
            "the active application must not be moved aside"
        );
        assert!(pid_file.is_file(), "the supervisor pid file must be untouched");
        assert!(
            !stop_file.exists(),
            "finalize must not ask the supervisor to stop for an unverified payload"
        );

        let labels: Vec<&'static str> = observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|progress| progress.phase == 6 && !progress.phase_label.is_empty())
            .map(|progress| progress.phase_label)
            .collect();
        assert!(
            labels.contains(&finalize_phase::VERIFYING_PAYLOAD),
            "expected finalize substep '{}' in {labels:?}",
            finalize_phase::VERIFYING_PAYLOAD
        );
        for destructive_label in [
            finalize_phase::STOPPING_SUPERVISOR,
            finalize_phase::QUIESCING_ACTIVE_APP,
            finalize_phase::PREPARING_SWAP,
            finalize_phase::SWAPPING_APP_DIRECTORY,
        ] {
            assert!(
                !labels.contains(&destructive_label),
                "finalize reached destructive substep '{destructive_label}' for a truncated payload: {labels:?}"
            );
        }
    }
}
