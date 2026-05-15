use std::path::Path;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::error::{Result, SurgeError};
use crate::platform::process::{ProcessHandle, current_pid, spawn_detached, spawn_process, supervisor_binary_name};
use crate::releases::manifest::ReleaseEntry;
use crate::supervisor::state::{read_restart_args, supervisor_pid_file, supervisor_stop_file};
use crate::update::status::confirm_supervisor_restart;

const SUPERVISOR_RESTART_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(super) enum SupervisorRestartOutcome {
    /// No supervisor was configured for this release (or wasn't running before
    /// the update) so there is no post-update restart to confirm.
    NotApplicable,
    /// Supervisor restart was confirmed by observing the supervisor pid file
    /// within `SUPERVISOR_RESTART_CONFIRM_TIMEOUT` after the spawn.
    Confirmed,
    /// Supervisor restart could not be confirmed within the timeout window.
    /// The reason describes why (spawn failure, missing binary, no pid file).
    Unconfirmed { reason: String },
}

pub(super) async fn request_supervisor_shutdown(install_dir: &Path, supervisor_id: &str) -> Result<()> {
    request_supervisor_shutdown_with_timeout(
        install_dir,
        supervisor_id,
        Duration::from_secs(20),
        Duration::from_millis(100),
    )
    .await
}

pub(super) async fn request_supervisor_shutdown_with_timeout(
    install_dir: &Path,
    supervisor_id: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<()> {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return Ok(());
    }

    let pid_file = supervisor_pid_file(install_dir, supervisor_id);
    if !pid_file.is_file() {
        return Ok(());
    }

    let stop_file = supervisor_stop_file(install_dir, supervisor_id);
    tokio::fs::write(&stop_file, b"surge-update").await?;

    let deadline = tokio::time::Instant::now() + timeout;
    while pid_file.exists() {
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "Timed out waiting for supervisor '{supervisor_id}' to stop before applying update"
            )));
        }
        tokio::time::sleep(poll_interval).await;
    }

    let _ = tokio::fs::remove_file(&stop_file).await;
    Ok(())
}

pub(super) fn invoke_post_update_hook(install_dir: &Path, active_app_dir: &Path, latest: &ReleaseEntry) {
    let main_exe = latest.main_exe.trim();
    if main_exe.is_empty() {
        return;
    }

    let exe_path = active_app_dir.join(main_exe);
    if !exe_path.is_file() {
        warn!(
            exe = %exe_path.display(),
            version = %latest.version,
            "Skipping post-update lifecycle hook because the executable is missing"
        );
        return;
    }

    let lifecycle_args = [String::from("--surge-updated"), latest.version.clone()];
    let lifecycle_args_refs: Vec<&str> = lifecycle_args.iter().map(String::as_str).collect();

    match spawn_process(&exe_path, &lifecycle_args_refs, Some(install_dir), &latest.environment) {
        Ok(mut handle) => wait_for_post_update_hook(&mut handle, &exe_path),
        Err(e) => {
            warn!(
                exe = %exe_path.display(),
                version = %latest.version,
                error = %e,
                "Failed to invoke post-update lifecycle hook (continuing)"
            );
        }
    }
}

fn wait_for_post_update_hook(handle: &mut ProcessHandle, exe_path: &Path) {
    let check_interval = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + Duration::from_secs(15);

    while std::time::Instant::now() < deadline {
        if !handle.poll_running() {
            match handle.wait() {
                Ok(result) if result.exit_code == 0 => {
                    debug!(exe = %exe_path.display(), "Post-update lifecycle hook completed successfully");
                }
                Ok(result) => {
                    warn!(
                        exe = %exe_path.display(),
                        exit_code = result.exit_code,
                        "Post-update lifecycle hook exited non-zero (continuing)"
                    );
                }
                Err(e) => {
                    warn!(
                        exe = %exe_path.display(),
                        error = %e,
                        "Failed waiting for post-update lifecycle hook (continuing)"
                    );
                }
            }
            return;
        }

        std::thread::sleep(check_interval);
    }

    warn!(
        exe = %exe_path.display(),
        "Post-update lifecycle hook exceeded timeout, terminating it (continuing)"
    );
    let _ = handle.kill();
    let _ = handle.wait();
}

pub(super) fn restart_supervisor_after_update(
    install_dir: &Path,
    active_app_dir: &Path,
    latest: &ReleaseEntry,
) -> SupervisorRestartOutcome {
    restart_supervisor_after_update_with_pid(install_dir, active_app_dir, latest, current_pid())
}

pub(super) fn restart_supervisor_after_update_with_pid(
    install_dir: &Path,
    active_app_dir: &Path,
    latest: &ReleaseEntry,
    watched_pid: u32,
) -> SupervisorRestartOutcome {
    let supervisor_id = latest.supervisor_id.trim();
    if supervisor_id.is_empty() {
        return SupervisorRestartOutcome::NotApplicable;
    }

    let supervisor_path = active_app_dir.join(supervisor_binary_name());
    if !supervisor_path.is_file() {
        warn!(
            supervisor = %supervisor_path.display(),
            "Cannot restart supervisor after update because the bundled binary is missing"
        );
        return SupervisorRestartOutcome::Unconfirmed {
            reason: format!("supervisor binary missing at {}", supervisor_path.display()),
        };
    }

    let exe_path = active_app_dir.join(&latest.main_exe);
    if !exe_path.is_file() {
        warn!(
            exe = %exe_path.display(),
            "Cannot restart supervisor after update because the application executable is missing"
        );
        return SupervisorRestartOutcome::Unconfirmed {
            reason: format!("application executable missing at {}", exe_path.display()),
        };
    }

    let restart_args = match read_restart_args(install_dir, supervisor_id) {
        Ok(args) => args,
        Err(e) => {
            warn!(
                supervisor_id,
                error = %e,
                "Failed reading stored supervisor restart arguments; restarting with no extra args"
            );
            Vec::new()
        }
    };

    let install_dir_str = install_dir.to_string_lossy();
    let pid_str = watched_pid.to_string();
    let exe_path_str = exe_path.to_string_lossy();
    let mut args: Vec<&str> = vec![
        "watch",
        "--id",
        supervisor_id,
        "--dir",
        &install_dir_str,
        "--pid",
        &pid_str,
        "--exe",
        &exe_path_str,
    ];
    if !restart_args.is_empty() {
        args.push("--");
        args.extend(restart_args.iter().map(String::as_str));
    }

    match spawn_detached(&supervisor_path, &args, Some(install_dir), &latest.environment) {
        Ok(handle) => {
            info!(pid = handle.pid(), supervisor_id, "Restarted supervisor after update");
        }
        Err(e) => {
            warn!(
                supervisor_id,
                error = %e,
                "Failed to restart supervisor after update (continuing)"
            );
            return SupervisorRestartOutcome::Unconfirmed {
                reason: format!("spawn failed: {e}"),
            };
        }
    }

    if confirm_supervisor_restart(install_dir, supervisor_id, SUPERVISOR_RESTART_CONFIRM_TIMEOUT) {
        SupervisorRestartOutcome::Confirmed
    } else {
        let timeout_ms = u64::try_from(SUPERVISOR_RESTART_CONFIRM_TIMEOUT.as_millis()).unwrap_or(u64::MAX);
        warn!(
            supervisor_id,
            timeout_ms, "Supervisor pid file did not appear after restart within timeout window"
        );
        SupervisorRestartOutcome::Unconfirmed {
            reason: format!("supervisor pid file did not appear within {timeout_ms}ms after restart"),
        }
    }
}
