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

#[cfg(unix)]
const PREVIOUS_SWAP_QUARANTINE_DIR: &str = ".surge-app-prev-quiescing";

#[cfg(unix)]
fn restore_interrupted_previous_swap_quarantine(previous_swap_dir: &Path, quarantine_dir: &Path) -> Result<()> {
    let previous_exists = previous_swap_dir.try_exists()?;
    let quarantine_exists = quarantine_dir.try_exists()?;
    if previous_exists && quarantine_exists {
        return Err(SurgeError::Update(
            "Both the previous swap directory and its quiescence quarantine exist; refusing to choose one".to_string(),
        ));
    }
    if quarantine_exists {
        atomic_rename(quarantine_dir, previous_swap_dir).map_err(|error| {
            SurgeError::Update(format!(
                "Failed to restore the interrupted previous swap quarantine: {error}"
            ))
        })?;
    }
    Ok(())
}

#[cfg(unix)]
fn restore_previous_swap_after_error(previous_swap_dir: &Path, quarantine_dir: &Path, error: SurgeError) -> SurgeError {
    match atomic_rename(quarantine_dir, previous_swap_dir) {
        Ok(()) => error,
        Err(restore_error) => SurgeError::Update(format!(
            "{error}; failed to restore the previous swap directory from its quiescence quarantine: {restore_error}"
        )),
    }
}

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
    #[cfg(unix)]
    let previous_swap_quarantine_dir = manager.install_dir.join(PREVIOUS_SWAP_QUARANTINE_DIR);
    #[cfg(unix)]
    restore_interrupted_previous_swap_quarantine(&previous_swap_dir, &previous_swap_quarantine_dir)?;
    let active_app_was_present = active_app_dir.is_dir();
    let fallback_previous_app_dir = if active_app_was_present {
        None
    } else {
        apply::find_previous_app_dir(&manager.install_dir, &manager.current_version)
    };
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
    let (current_version, current_main_exe, current_supervisor_id) = if current_app_dir.is_some() {
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
        (
            current.version.as_str(),
            current.main_exe.as_str(),
            current.supervisor_id.as_str(),
        )
    } else {
        ("", "", "")
    };
    #[cfg(not(unix))]
    let (current_main_exe, current_supervisor_id) = manager.current_release_identity.as_ref().map_or_else(
        || (latest.main_exe.as_str(), latest.supervisor_id.as_str()),
        |current| (current.main_exe.as_str(), current.supervisor_id.as_str()),
    );
    let supervisor_was_running = !current_supervisor_id.trim().is_empty()
        && supervisor_pid_file(&manager.install_dir, current_supervisor_id).is_file();

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
    #[cfg(unix)]
    let current_quiescence = if let Some(current_app_dir) = current_app_dir {
        prepare_and_quiesce_active_app_before_swap(
            &manager.install_dir,
            current_app_dir,
            current_version,
            current_main_exe,
            current_supervisor_id,
            supervisor_was_running,
            manager.allow_in_process_swap,
        )?
    } else {
        None
    };
    #[cfg(not(unix))]
    if let Some(current_app_dir) = current_app_dir {
        lifecycle::terminate_active_app_processes_before_swap(
            current_app_dir,
            current_main_exe,
            manager.allow_in_process_swap,
        )?;
    }
    #[cfg(unix)]
    let previous_swap_quiescence = if previous_swap_dir.is_dir() {
        let previous_swap_identity = super::current_install::load_previous_swap(manager, &previous_swap_dir)?
            .ok_or_else(|| {
                SurgeError::Update(
                    "Previous swap directory has no persisted process identity; refusing to delete or reuse it"
                        .to_string(),
                )
            })?;
        lifecycle::request_supervisor_shutdown(&manager.install_dir, &previous_swap_identity.supervisor_id).await?;
        let prepared = lifecycle::prepare_app_quiescence(
            &previous_swap_dir,
            &previous_swap_identity.main_exe,
            manager.allow_in_process_swap,
        )?;
        if let Some(prepared) = &prepared {
            lifecycle::terminate_prepared_app_processes(prepared)?;
        }
        let prepared = prepared.ok_or_else(|| {
            SurgeError::Update(
                "Previous swap application entrypoint disappeared before it could be quiesced".to_string(),
            )
        })?;
        Some((prepared, previous_swap_identity.main_exe))
    } else {
        None
    };

    progress_emitter.emit_substep(6, finalize_phase::PREPARING_SWAP, 92);
    if next_app_dir.exists() {
        tokio::fs::remove_dir_all(&next_app_dir).await?;
    }
    #[cfg(unix)]
    if let Some((previous_swap_quiescence, previous_main_exe)) = &previous_swap_quiescence {
        atomic_rename(&previous_swap_dir, &previous_swap_quarantine_dir)?;
        let quarantine_result = (|| {
            let quarantined_quiescence = lifecycle::prepare_app_quiescence(
                &previous_swap_quarantine_dir,
                previous_main_exe,
                manager.allow_in_process_swap,
            )?
            .ok_or_else(|| {
                SurgeError::Update(
                    "Previous swap application entrypoint disappeared while entering quiescence quarantine".to_string(),
                )
            })?;
            lifecycle::terminate_prepared_app_processes(&quarantined_quiescence)?;
            lifecycle::terminate_prepared_app_processes(previous_swap_quiescence)?;
            lifecycle::terminate_superseded_app_processes(&manager.install_dir, &active_app_dir, previous_main_exe)?;
            Ok::<(), SurgeError>(())
        })();
        if let Err(error) = quarantine_result {
            let error = restore_previous_swap_after_error(
                &previous_swap_dir,
                &previous_swap_quarantine_dir,
                error,
            );
            return Err(restore_previous_supervisor_after_quiescence_failure(
                &manager.install_dir,
                current_app_dir,
                current_version,
                current_main_exe,
                current_supervisor_id,
                supervisor_was_running,
                error,
            ));
        }
        if let Err(error) = tokio::fs::remove_dir_all(&previous_swap_quarantine_dir).await {
            let error = restore_previous_swap_after_error(
                &previous_swap_dir,
                &previous_swap_quarantine_dir,
                error.into(),
            );
            return Err(restore_previous_supervisor_after_quiescence_failure(
                &manager.install_dir,
                current_app_dir,
                current_version,
                current_main_exe,
                current_supervisor_id,
                supervisor_was_running,
                error,
            ));
        }
    }
    #[cfg(not(unix))]
    if previous_swap_dir.exists() {
        tokio::fs::remove_dir_all(&previous_swap_dir).await?;
    }

    progress_emitter.emit_substep(6, finalize_phase::SWAPPING_APP_DIRECTORY, 93);
    atomic_rename(extracted_final_dir, &next_app_dir)?;

    if active_app_was_present {
        atomic_rename(&active_app_dir, &previous_swap_dir)?;
        #[cfg(unix)]
        if let Err(error) = (|| {
            if let Some(current_quiescence) = &current_quiescence {
                lifecycle::terminate_prepared_app_processes(current_quiescence)?;
            }
            lifecycle::terminate_superseded_app_processes(&manager.install_dir, &active_app_dir, current_main_exe)?;
            Ok::<(), SurgeError>(())
        })() {
            let _ = atomic_rename(&previous_swap_dir, &active_app_dir);
            return Err(restore_previous_supervisor_after_quiescence_failure(
                &manager.install_dir,
                Some(&active_app_dir),
                current_version,
                current_main_exe,
                current_supervisor_id,
                supervisor_was_running,
                error,
            ));
        }
    }
    #[cfg(unix)]
    if !active_app_was_present {
        if let Err(error) = (|| {
            if let Some(current_quiescence) = &current_quiescence {
                lifecycle::terminate_prepared_app_processes(current_quiescence)?;
            }
            lifecycle::terminate_superseded_app_processes(&manager.install_dir, &active_app_dir, current_main_exe)?;
            Ok::<(), SurgeError>(())
        })() {
            return Err(restore_previous_supervisor_after_quiescence_failure(
                &manager.install_dir,
                current_app_dir,
                current_version,
                current_main_exe,
                current_supervisor_id,
                supervisor_was_running,
                error,
            ));
        }
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

    // Start the replacement watch-supervisor as soon as the swapped app is
    // usable (directory swapped, persistent assets carried over, runtime
    // manifest written), before the best-effort shortcut/prune/hook steps that
    // can block for many seconds. This shrinks the window in which no
    // supervisor is watching the app. The start no longer depends on a
    // supervisor having been running before the update: if the release
    // configures supervision, a fresh watch supervisor is always started so a
    // supervisor that died before the update is recovered here.
    let restart_outcome = if latest.supervisor_id.trim().is_empty() {
        SupervisorRestartOutcome::NotApplicable
    } else {
        progress_emitter.emit_substep(6, finalize_phase::RESTARTING_SUPERVISOR, 96);
        lifecycle::restart_supervisor_after_update(&manager.install_dir, &active_app_dir, latest)
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

    progress_emitter.emit_substep(6, finalize_phase::POST_UPDATE_HOOK, 99);
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

#[cfg(unix)]
fn prepare_and_quiesce_active_app_before_swap(
    install_dir: &Path,
    current_app_dir: &Path,
    current_version: &str,
    current_main_exe: &str,
    current_supervisor_id: &str,
    supervisor_was_running: bool,
    allow_in_process_swap: bool,
) -> Result<Option<lifecycle::PreparedAppQuiescence>> {
    let result = (|| {
        let prepared = lifecycle::prepare_app_quiescence(current_app_dir, current_main_exe, allow_in_process_swap)?;
        if let Some(prepared) = &prepared {
            lifecycle::terminate_prepared_app_processes(prepared)?;
        }
        Ok(prepared)
    })();
    let Err(error) = result else {
        return result;
    };

    Err(restore_previous_supervisor_after_quiescence_failure(
        install_dir,
        Some(current_app_dir),
        current_version,
        current_main_exe,
        current_supervisor_id,
        supervisor_was_running,
        error,
    ))
}

#[cfg(unix)]
fn restore_previous_supervisor_after_quiescence_failure(
    install_dir: &Path,
    current_app_dir: Option<&Path>,
    current_version: &str,
    current_main_exe: &str,
    current_supervisor_id: &str,
    supervisor_was_running: bool,
    error: SurgeError,
) -> SurgeError {
    if supervisor_was_running && let Some(current_app_dir) = current_app_dir {
        let restore_outcome = lifecycle::restore_previous_supervisor_after_failed_quiescence(
            install_dir,
            current_app_dir,
            current_version,
            current_main_exe,
            current_supervisor_id,
        );
        match restore_outcome {
            SupervisorRestartOutcome::NotApplicable => {
                warn!(
                    supervisor_id = current_supervisor_id,
                    "Previous supervisor could not be restored after active application quiescence failed"
                );
            }
            SupervisorRestartOutcome::PendingRestart { reason, failure_phase } => {
                warn!(
                    supervisor_id = current_supervisor_id,
                    reason,
                    failure_phase,
                    "Requested previous supervisor restoration after active application quiescence failed"
                );
            }
        }
    }

    error
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(target_os = "linux")]
    use std::path::PathBuf;
    #[cfg(target_os = "linux")]
    use std::process::{Command, Stdio};

    #[cfg(target_os = "linux")]
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn quiescence_failure_restores_previous_supervisor_and_preserves_error() {
        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path();
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();

        let app_path = active_app_dir.join("cwd-changing-demo-script");
        std::fs::write(&app_path, "#!/bin/sh\ncd /\nread _\n").unwrap();
        make_executable(&app_path);

        let supervisor_path = active_app_dir.join(crate::platform::process::supervisor_binary_name());
        std::fs::write(
            &supervisor_path,
            r#"#!/bin/sh
id=""
dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id) id="$2"; shift 2 ;;
    --dir) dir="$2"; shift 2 ;;
    *) shift ;;
  esac
done
echo $$ > "$dir/.surge-supervisor-$id.pid"
"#,
        )
        .unwrap();
        make_executable(&supervisor_path);

        let spawn_deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut child = loop {
            match Command::new("./cwd-changing-demo-script")
                .current_dir(&active_app_dir)
                .stdin(Stdio::piped())
                .spawn()
            {
                Ok(child) => break child,
                Err(error)
                    if error.raw_os_error() == Some(nix::errno::Errno::ETXTBSY as i32)
                        && std::time::Instant::now() < spawn_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("failed to launch changed-cwd app fixture: {error}"),
            }
        };
        let cwd_path = PathBuf::from(format!("/proc/{}/cwd", child.id()));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::fs::read_link(&cwd_path).ok().as_deref() != Some(Path::new("/")) {
            assert!(std::time::Instant::now() < deadline, "test child did not change cwd");
            std::thread::sleep(Duration::from_millis(10));
        }

        let result = prepare_and_quiesce_active_app_before_swap(
            install_dir,
            &active_app_dir,
            "1.0.0",
            "cwd-changing-demo-script",
            "demo-supervisor",
            true,
            false,
        );
        let child_still_running = child.try_wait().unwrap().is_none();
        if child_still_running {
            child.kill().unwrap();
        }
        let _ = child.wait();

        let error = result.err().unwrap();
        assert!(error.to_string().contains("process identity is ambiguous"));
        assert!(child_still_running);
        assert!(supervisor_pid_file(install_dir, "demo-supervisor").is_file());
        assert_eq!(
            crate::supervisor::state::read_supervisor_exe_path(install_dir, "demo-supervisor").as_deref(),
            Some(app_path.as_path())
        );
    }

    #[cfg(target_os = "linux")]
    fn make_executable(path: &Path) {
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }
}
