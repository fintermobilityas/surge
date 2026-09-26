use super::{Path, Result, detached, lock, logline, runtime, staging};

#[allow(clippy::too_many_arguments)]
pub(super) async fn finish_detached_install(
    ssh_target: &str,
    file_target: &str,
    install_root: &Path,
    log_offset: u64,
    target: &detached::RemoteInstallTarget<'_>,
    operation: &str,
    installer_lock: &mut lock::RemoteInstallerLock,
    verify_started_process: bool,
) -> Result<()> {
    detached::watch_remote_detached_install(
        ssh_target,
        file_target,
        install_root,
        log_offset,
        target,
        operation,
        installer_lock,
    )
    .await?;
    installer_lock.ensure_held()?;
    if let Err(error) = detached::cleanup_remote_detached_install(ssh_target, operation).await {
        logline::warn(&format!("Could not remove remote installer transfer helpers: {error}"));
    }
    if !target.is_stage {
        staging::warn_if_remote_stage_cleanup_fails(ssh_target, target.app_id, target.release).await;
        let install_dir = if target.release.install_directory.trim().is_empty() {
            target.app_id
        } else {
            target.release.install_directory.trim()
        };
        runtime::verify_remote_runtime_after_install(
            ssh_target,
            file_target,
            install_dir,
            target.app_id,
            target.release,
            target.channel,
            target.storage,
            verify_started_process,
        )
        .await?;
    }
    let action = if target.is_stage { "Staged" } else { "Installed" };
    logline::success(&format!(
        "{action} '{}' v{} on tailscale node '{file_target}'.",
        target.app_id, target.release.version
    ));
    Ok(())
}
