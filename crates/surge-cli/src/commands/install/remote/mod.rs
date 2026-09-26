#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

mod activation;
mod completion;
mod detached;
mod detached_identity;
mod execution;
mod installer_stage;
mod lock;
mod operation;
mod published_installer;
mod reporting;
mod runtime;
mod stage_manifest;
mod staging;
mod state;
mod types;
mod watchdog;

use self::installer_stage::stage_installer_file_for_tailscale;
use super::{
    ArchiveAcquisition, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader, CacheFetchOutcome, Command,
    InstallBehavior, InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi, Instant,
    Path, PathBuf, RELEASES_FILE_COMPRESSED, ReleaseEntry, ReleaseIndex, Result, Serialize, Stdio, StorageBackend,
    SurgeError, SurgeManifest, cache_path_for_key, compare_versions, core_install, download_release_archive,
    fetch_or_reuse_file, host_can_build_installer_locally, infer_os_from_rid, logline, make_progress_bar, make_spinner,
    release_install_profile, release_runtime_manifest_metadata, shell_single_quote,
};
use crate::commands::pack;
use serde::Deserialize;

pub(crate) use self::execution::{resolve_tailscale_targets, run_tailscale_capture, run_tailscale_streaming};
pub(crate) use self::published_installer::{
    build_installer_for_tailscale, missing_remote_installer_error, plan_remote_published_installer,
    plan_remote_published_installer_without_manifest, try_prepare_published_installer_for_tailscale,
};
use self::runtime::{converge_current_remote_runtime, verify_remote_runtime_after_install};
pub(crate) use self::staging::{
    deploy_remote_app_copy_for_tailscale, run_remote_staged_installer_setup, warn_if_remote_stage_cleanup_fails,
};
pub(crate) use self::state::{
    check_remote_install_state, detect_remote_launch_environment, remote_install_matches,
    remote_staged_installer_matches_release, remote_staged_payload_matches_release, select_remote_installer_mode,
    select_remote_tailscale_transfer_strategy_for_convergence, verify_remote_stage_readiness,
};
pub(crate) use self::types::{
    RemoteConvergenceAction, RemoteConvergencePlan, RemoteHostInstallerAvailability, RemoteInstallerMode,
    RemoteTailscaleCachedState, RemoteTailscaleOperation, RemoteTailscaleTransferInputs,
    RemoteTailscaleTransferStrategy, ensure_supported_tailscale_rid,
};
pub(crate) use self::watchdog::{RemoteSetupWatchdog, read_remote_update_status_file};

#[cfg(test)]
pub(crate) use self::activation::build_remote_app_copy_activation_script;
#[cfg(test)]
pub(crate) use self::published_installer::{build_remote_installer_manifest, published_installer_public_url};
#[cfg(test)]
pub(crate) use self::runtime::{
    REMOTE_PROCESS_VERIFICATION_POLL_INTERVAL, REMOTE_PROCESS_VERIFICATION_TIMEOUT,
    build_remote_process_verification_probe, build_remote_runtime_start_command,
};
#[cfg(test)]
pub(crate) use self::staging::{
    build_remote_paths_exist_probe, build_remote_stage_cleanup_command, build_remote_staged_installer_setup_command,
    build_remote_stop_supervisor_command, select_latest_remote_legacy_app_dir,
};
#[cfg(test)]
pub(crate) use self::state::{
    parse_remote_install_state, parse_remote_launch_environment, parse_remote_staged_payload_identity,
    plan_remote_convergence, remote_launch_environment_probe, remote_staged_payload_identity,
    select_remote_tailscale_transfer_strategy, should_skip_remote_install,
};
#[cfg(test)]
pub(crate) use self::types::{RemoteInstallState, RemoteLaunchEnvironment, RemotePublishedInstallerPlan};

#[allow(clippy::too_many_arguments)]
pub(super) async fn install_release_via_tailscale(
    manifest: Option<&SurgeManifest>,
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    download_dir: &Path,
    ssh_target: &str,
    file_target: &str,
    app_id: &str,
    selected_rid: &str,
    rid_candidates: &[String],
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    full_filename: &str,
    behavior: InstallBehavior,
) -> Result<()> {
    let installer_mode = select_remote_installer_mode(storage_config);
    let install_dir = if release.install_directory.trim().is_empty() {
        app_id
    } else {
        release.install_directory.trim()
    };
    let main_exe_name = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };
    let operation = operation::request_fingerprint(app_id, selected_rid, release, channel, storage_config, behavior)?;
    let install_target = detached::RemoteInstallTarget {
        is_stage: behavior.mode.is_stage(),
        app_id,
        rid: selected_rid,
        release,
        channel,
        storage: storage_config,
    };
    let control = if behavior.plan_only {
        None
    } else {
        let mut lock = lock::RemoteInstallerLock::acquire(ssh_target).await?;
        let probe =
            detached::probe_remote_install_before_transfer(ssh_target, &mut lock, std::time::Duration::from_secs(30))
                .await?;
        if probe.unverified_alive {
            return Err(SurgeError::Platform("A legacy installer PID is alive without verifiable process identity; leave it running and retry after it exits".to_string()));
        }
        if probe.alive {
            if probe.operation.as_deref() != Some(operation.as_str()) {
                return Err(SurgeError::Platform(format!(
                    "Another detached installer is running on '{file_target}'; wait for it to finish before starting a different operation."
                )));
            }
            let remote_home = execution::detect_remote_home_directory(ssh_target).await?;
            let install_root = staging::remote_install_root(&remote_home, app_id, &release.install_directory)?;
            logline::info("Reattaching to the matching detached install operation before inspecting installed state.");
            return completion::finish_detached_install(
                ssh_target,
                file_target,
                &install_root,
                probe.log_size,
                &install_target,
                &operation,
                &mut lock,
                probe.verification_intent()?,
            )
            .await;
        }
        Some(lock)
    };
    let remote_state = check_remote_install_state(ssh_target, install_dir, main_exe_name).await?;
    let convergence_plan = state::plan_remote_convergence(
        remote_state.as_ref(),
        index,
        app_id,
        selected_rid,
        release,
        channel,
        storage_config,
        installer_mode,
        behavior.force,
    )?;
    reporting::log_remote_convergence_plan(file_target, app_id, channel, release, &convergence_plan);

    let Some(mut installer_lock) = control else {
        return Ok(());
    };
    installer_lock.ensure_held()?;

    if !behavior.mode.is_stage() && convergence_plan.action == RemoteConvergenceAction::Skip {
        logline::success(&format!(
            "'{app_id}' v{} ({channel}) is already installed on '{file_target}', skipping.",
            release.version
        ));
        return Ok(());
    }

    if !behavior.mode.is_stage() && convergence_plan.action == RemoteConvergenceAction::ConvergeRuntime {
        if behavior.no_start {
            logline::success(&format!(
                "'{app_id}' v{} ({channel}) is package-current on '{file_target}'; runtime convergence was skipped because --no-start was supplied.",
                release.version
            ));
            return Ok(());
        }
        let launch_env = detect_remote_launch_environment(ssh_target).await;
        converge_current_remote_runtime(ssh_target, file_target, app_id, release, &launch_env).await?;
        return Ok(());
    }

    let install_matches = remote_install_matches(remote_state.as_ref(), app_id, &release.version, channel);
    if install_matches && behavior.force {
        logline::info(&format!(
            "'{app_id}' v{} ({channel}) is already installed on '{file_target}'; reinstalling due to --force.",
            release.version
        ));
    } else if let Some(remote_state) = &remote_state
        && remote_state.version.trim() == release.version
    {
        logline::info(&format!(
            "'{app_id}' v{} is installed on '{file_target}' with channel '{}'; reinstalling to switch to '{channel}'.",
            release.version,
            remote_state.channel.as_deref().unwrap_or("unknown")
        ));
    }

    let prefer_update_setup = matches!(
        convergence_plan.action,
        RemoteConvergenceAction::Update | RemoteConvergenceAction::RepairMetadata
    ) && installer_mode == RemoteInstallerMode::Online
        && !behavior.mode.is_stage();

    let launch_env = detect_remote_launch_environment(ssh_target).await;
    if let Some(display) = launch_env.display.as_deref() {
        logline::info(&format!("Detected remote X11 session for install: DISPLAY={display}"));
    } else if let Some(wayland_display) = launch_env.wayland_display.as_deref() {
        logline::info(&format!(
            "Detected remote Wayland session for install: WAYLAND_DISPLAY={wayland_display}"
        ));
    } else if launch_env.has_graphical_session() {
        logline::info("Detected remote graphical session for install.");
    } else {
        logline::info("No remote graphical session environment detected; install will default to headless startup.");
    }

    let host_can_build_installer = host_can_build_installer_locally(selected_rid);
    let has_matching_pre_staged_app_copy_payload = if !prefer_update_setup
        && host_can_build_installer
        && installer_mode == RemoteInstallerMode::Offline
        && !behavior.mode.is_stage()
    {
        remote_staged_payload_matches_release(ssh_target, app_id, release, channel, storage_config).await?
    } else {
        false
    };
    let has_matching_pre_staged_installer_cache =
        if !prefer_update_setup && installer_mode == RemoteInstallerMode::Online && !behavior.mode.is_stage() {
            remote_staged_installer_matches_release(ssh_target, app_id, release, channel, storage_config).await?
        } else {
            false
        };
    let transfer_strategy = if prefer_update_setup {
        RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
    } else {
        select_remote_tailscale_transfer_strategy_for_convergence(
            RemoteTailscaleTransferInputs {
                host_installer_availability: if host_can_build_installer {
                    RemoteHostInstallerAvailability::Available
                } else {
                    RemoteHostInstallerAvailability::Unavailable
                },
                installer_mode,
                operation: if behavior.mode.is_stage() {
                    RemoteTailscaleOperation::Stage
                } else {
                    RemoteTailscaleOperation::Install
                },
                cached_state: if has_matching_pre_staged_installer_cache {
                    RemoteTailscaleCachedState::InstallerCache
                } else if has_matching_pre_staged_app_copy_payload {
                    RemoteTailscaleCachedState::AppCopyPayload
                } else {
                    RemoteTailscaleCachedState::None
                },
            },
            convergence_plan.action,
        )
    };
    if matches!(transfer_strategy, RemoteTailscaleTransferStrategy::AppCopy) {
        installer_lock.ensure_held()?;
        deploy_remote_app_copy_for_tailscale(
            backend,
            index,
            download_dir,
            ssh_target,
            file_target,
            app_id,
            selected_rid,
            release,
            channel,
            storage_config,
            &launch_env,
            rid_candidates,
            full_filename,
            behavior.no_start,
            behavior.mode.is_stage(),
        )
        .await?;
        if !behavior.mode.is_stage() {
            warn_if_remote_stage_cleanup_fails(ssh_target, app_id, release).await;
            verify_remote_runtime_after_install(
                ssh_target,
                file_target,
                install_dir,
                app_id,
                release,
                channel,
                storage_config,
                !behavior.no_start,
            )
            .await?;
        }
        if behavior.mode.is_stage() {
            logline::success(&format!(
                "Staged '{app_id}' v{} on tailscale node '{file_target}'.",
                release.version
            ));
        } else {
            logline::success(&format!("Installed '{app_id}' on tailscale node '{file_target}'."));
        }
        return Ok(());
    }

    if matches!(transfer_strategy, RemoteTailscaleTransferStrategy::StagedInstallerCache) {
        installer_lock.ensure_held()?;
        run_remote_staged_installer_setup(ssh_target, file_target, app_id, release, behavior.no_start).await?;
        verify_remote_runtime_after_install(
            ssh_target,
            file_target,
            install_dir,
            app_id,
            release,
            channel,
            storage_config,
            !behavior.no_start
                && matches!(
                    convergence_plan.action,
                    RemoteConvergenceAction::CleanInstall | RemoteConvergenceAction::Reinstall
                ),
        )
        .await?;
        logline::success(&format!("Installed '{app_id}' on tailscale node '{file_target}'."));
        return Ok(());
    }

    let published_installer_plan = if let Some(manifest) = manifest {
        plan_remote_published_installer(manifest, app_id, selected_rid, channel, release, installer_mode)?
    } else {
        plan_remote_published_installer_without_manifest(app_id, selected_rid, channel, release, installer_mode)
    };
    let published_installer_path = if matches!(
        transfer_strategy,
        RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
    ) {
        try_prepare_published_installer_for_tailscale(
            backend,
            download_dir,
            &published_installer_plan,
            app_id,
            release,
            channel,
            storage_config,
            &launch_env,
            installer_mode,
        )
        .await?
    } else {
        None
    };
    let installer_path = if let Some(installer_path) = published_installer_path {
        installer_path
    } else if installer_mode == RemoteInstallerMode::Offline {
        if !host_can_build_installer {
            return Err(missing_remote_installer_error(
                selected_rid,
                &published_installer_plan,
                installer_mode,
            ));
        }
        std::fs::create_dir_all(download_dir)?;
        let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
        let acquisition =
            download_release_archive(backend, index, release, rid_candidates, full_filename, &local_package).await?;
        match acquisition {
            ArchiveAcquisition::ReusedLocal => {
                logline::success(&format!(
                    "Using cached package '{}' at '{}'.",
                    Path::new(full_filename).display(),
                    local_package.display()
                ));
            }
            ArchiveAcquisition::Downloaded => {
                logline::success(&format!(
                    "Downloaded '{}' to '{}'.",
                    Path::new(full_filename).display(),
                    local_package.display()
                ));
            }
            ArchiveAcquisition::Reconstructed => {
                logline::warn(&format!(
                    "Direct full package '{}' missing in backend; reconstructed from retained release artifacts.",
                    Path::new(full_filename).display()
                ));
            }
        }
        logline::info("Building offline installer for remote deployment...");
        build_installer_for_tailscale(
            manifest,
            app_id,
            selected_rid,
            release,
            channel,
            storage_config,
            Some(&local_package),
            &launch_env,
            installer_mode,
        )?
    } else {
        if !host_can_build_installer {
            return Err(missing_remote_installer_error(
                selected_rid,
                &published_installer_plan,
                installer_mode,
            ));
        }
        logline::info("Building online installer for remote deployment...");
        build_installer_for_tailscale(
            manifest,
            app_id,
            selected_rid,
            release,
            channel,
            storage_config,
            None,
            &launch_env,
            installer_mode,
        )?
    };
    let installer_size = std::fs::metadata(&installer_path)
        .map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to read installer metadata at '{}': {e}",
                installer_path.display()
            ))
        })?
        .len();
    let installer_sha256 = surge_core::crypto::sha256::sha256_hex_file(&installer_path)?;
    logline::info(&format!(
        "Preparing installer stage on '{file_target}' ({}, sha256 {})...",
        crate::formatters::format_bytes(installer_size),
        &installer_sha256[..installer_sha256.len().min(12)],
    ));
    let no_start_flag = if behavior.no_start { " --no-start" } else { "" };
    let stage_flag = if behavior.mode.is_stage() { " --stage" } else { "" };
    let reinstall_flag = if matches!(convergence_plan.action, RemoteConvergenceAction::Reinstall) || behavior.force {
        " --reinstall"
    } else {
        ""
    };
    let remote_home = execution::detect_remote_home_directory(ssh_target).await?;
    let install_root_for_watchdog = staging::remote_install_root(&remote_home, app_id, &release.install_directory)?;

    let install_flags = format!("{no_start_flag}{stage_flag}{reinstall_flag}");

    let mut watch_log_offset = 0_u64;
    let mut verify_started_process = !behavior.no_start
        && matches!(
            convergence_plan.action,
            RemoteConvergenceAction::CleanInstall | RemoteConvergenceAction::Reinstall
        );
    let mut reattached = false;
    let probe = detached::probe_remote_install_before_transfer(
        ssh_target,
        &mut installer_lock,
        std::time::Duration::from_secs(30),
    )
    .await?;
    if probe.unverified_alive {
        return Err(SurgeError::Platform("A legacy installer PID is alive without verifiable process identity; leave it running and retry after it exits".to_string()));
    }
    if probe.alive {
        if probe.operation.as_deref() == Some(operation.as_str()) {
            logline::info(&format!(
                "Detected a detached remote installer still running on '{file_target}' (pid {}); reattaching instead of starting a new install.",
                probe.pid.as_deref().unwrap_or("unknown")
            ));
            watch_log_offset = probe.log_size;
            verify_started_process = probe.verification_intent()?;
            reattached = true;
        } else {
            return Err(SurgeError::Platform(format!(
                "Another detached installer is running on '{file_target}'; wait for it to finish before starting a different operation."
            )));
        }
    } else if let Some(status) = read_remote_update_status_file(ssh_target, &install_root_for_watchdog).await?
        && status.state == "in_progress"
    {
        logline::warn(&format!(
            "The previous remote installer on '{file_target}' exited without converging; starting a fresh install."
        ));
        if let Some(tail) = detached::read_remote_detached_install_tail(ssh_target).await {
            logline::warn(&format!("Last installer log lines before the failure:\n{tail}"));
        }
    }

    if !reattached {
        reporting::warn_remote_full_download_downtime(
            file_target,
            app_id,
            convergence_plan.action,
            release,
            behavior.mode.is_stage(),
        );
        if behavior.mode.is_stage() {
            logline::info(&format!("Running installer in stage mode on '{file_target}'..."));
        } else {
            logline::info(&format!("Running installer on '{file_target}'..."));
        }
        stage_installer_file_for_tailscale(
            ssh_target,
            file_target,
            &installer_path,
            installer_size,
            &installer_sha256,
        )
        .await?;

        // Launch the installer detached from this SSH session so a local
        // orchestrator death cannot strand the node: the installer keeps
        // running node-locally and this process only watches it.
        installer_lock.ensure_held()?;
        let launch_script =
            detached::build_remote_detached_install_launch_command(&install_flags, &operation, verify_started_process);
        let ssh_command = format!("sh -lc {}", shell_single_quote(&launch_script));
        let launch_output = execution::run_tailscale_capture(&["ssh", ssh_target, ssh_command.as_str()]).await?;
        logline::info(&format!(
            "Remote installer launched on '{file_target}' ({}); the install will continue if this connection drops.",
            launch_output.trim()
        ));
    }

    completion::finish_detached_install(
        ssh_target,
        file_target,
        &install_root_for_watchdog,
        watch_log_offset,
        &install_target,
        &operation,
        &mut installer_lock,
        verify_started_process,
    )
    .await
}
