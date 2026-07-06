use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::error::{Result, SurgeError};
use crate::platform::process::{ProcessHandle, current_pid, spawn_detached, spawn_process, supervisor_binary_name};
use crate::releases::manifest::ReleaseEntry;
use crate::supervisor::state::{
    read_restart_args, supervisor_pid_file, supervisor_stop_file, write_supervisor_exe_path,
};
use crate::update::status::{
    RESTART_HANDOFF_FAILED_PHASE, RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE, confirm_supervisor_restart,
};

const SUPERVISOR_RESTART_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISOR_RESTART_MAX_ATTEMPTS: u32 = 2;
const SUPERVISOR_RESTART_RETRY_DELAY: Duration = Duration::from_millis(500);

#[derive(Debug, Clone)]
pub(super) enum SupervisorRestartOutcome {
    /// No supervisor was configured for this release (or wasn't running before
    /// the update) so there is no post-update restart to confirm.
    NotApplicable,
    /// The target package is installed, but restart handoff is still pending.
    /// The supervisor writes the converged record only after the old child exits
    /// and a replacement target child stays active.
    PendingRestart {
        reason: String,
        failure_phase: &'static str,
    },
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
    restart_supervisor_after_update_with_config(
        install_dir,
        active_app_dir,
        latest,
        watched_pid,
        SUPERVISOR_RESTART_CONFIRM_TIMEOUT,
        SUPERVISOR_RESTART_MAX_ATTEMPTS,
        SUPERVISOR_RESTART_RETRY_DELAY,
    )
}

fn restart_supervisor_after_update_with_config(
    install_dir: &Path,
    active_app_dir: &Path,
    latest: &ReleaseEntry,
    watched_pid: u32,
    confirm_timeout: Duration,
    max_attempts: u32,
    retry_delay: Duration,
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
        return SupervisorRestartOutcome::PendingRestart {
            reason: format!("supervisor binary missing at {}", supervisor_path.display()),
            failure_phase: RESTART_HANDOFF_FAILED_PHASE,
        };
    }

    let exe_path = active_app_dir.join(&latest.main_exe);
    if !exe_path.is_file() {
        warn!(
            exe = %exe_path.display(),
            "Cannot restart supervisor after update because the application executable is missing"
        );
        return SupervisorRestartOutcome::PendingRestart {
            reason: format!("application executable missing at {}", exe_path.display()),
            failure_phase: RESTART_HANDOFF_FAILED_PHASE,
        };
    }

    if let Err(e) = write_supervisor_exe_path(install_dir, supervisor_id, &exe_path) {
        warn!(
            supervisor_id,
            error = %e,
            "Failed to persist supervisor exe state before restart"
        );
        return SupervisorRestartOutcome::PendingRestart {
            reason: format!("failed to persist supervisor exe state: {e}"),
            failure_phase: RESTART_HANDOFF_FAILED_PHASE,
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

    let args = supervisor_watch_args(supervisor_id, install_dir, watched_pid, &latest.version, &restart_args);
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    let mut last_failure: Option<(String, &'static str)> = None;
    for attempt in 1..=max_attempts {
        match spawn_detached(&supervisor_path, &arg_refs, Some(install_dir), &latest.environment) {
            Ok(handle) => {
                info!(
                    pid = handle.pid(),
                    supervisor_id, attempt, "Restarted supervisor after update"
                );
                if confirm_supervisor_restart(install_dir, supervisor_id, confirm_timeout) {
                    return SupervisorRestartOutcome::PendingRestart {
                        reason: format!(
                            "supervisor handoff accepted; waiting for previous child pid {watched_pid} to exit and target version {} to start",
                            latest.version
                        ),
                        failure_phase: RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE,
                    };
                }
                let timeout_ms = u64::try_from(confirm_timeout.as_millis()).unwrap_or(u64::MAX);
                warn!(
                    supervisor_id,
                    timeout_ms, attempt, "Supervisor pid file did not appear after restart within timeout window"
                );
                last_failure = Some((
                    format!("supervisor pid file did not appear within {timeout_ms}ms after restart"),
                    RESTART_HANDOFF_FAILED_PHASE,
                ));
            }
            Err(e) => {
                warn!(
                    supervisor_id,
                    error = %e,
                    attempt,
                    "Failed to restart supervisor after update"
                );
                last_failure = Some((format!("spawn failed: {e}"), RESTART_HANDOFF_FAILED_PHASE));
            }
        }

        if attempt < max_attempts {
            warn!(supervisor_id, attempt, "Retrying supervisor restart after short delay");
            std::thread::sleep(retry_delay);
        }
    }

    let (reason, failure_phase) = last_failure.unwrap_or_else(|| {
        (
            "supervisor restart did not complete".to_string(),
            RESTART_HANDOFF_FAILED_PHASE,
        )
    });
    SupervisorRestartOutcome::PendingRestart { reason, failure_phase }
}

pub(super) fn terminate_superseded_app_processes(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
) -> Result<usize> {
    terminate_superseded_app_processes_except(install_dir, active_app_dir, main_exe, current_pid())
}

#[cfg(unix)]
fn terminate_superseded_app_processes_except(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
) -> Result<usize> {
    use nix::errno::Errno;
    use nix::sys::signal::Signal;

    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return Ok(0);
    }

    let pids = superseded_app_process_pids(install_dir, active_app_dir, main_exe, protected_pid);
    if pids.is_empty() {
        return Ok(0);
    }

    for pid in &pids {
        if let Err(e) = signal_pid(*pid, Signal::SIGTERM) {
            warn!(pid, error = %e, "Failed to request stale app process termination");
        }
    }

    if wait_until_superseded_processes_exit(
        install_dir,
        active_app_dir,
        main_exe,
        protected_pid,
        Duration::from_secs(5),
    ) {
        info!(
            count = pids.len(),
            "Terminated stale app processes from superseded install directories"
        );
        return Ok(pids.len());
    }

    let remaining = superseded_app_process_pids(install_dir, active_app_dir, main_exe, protected_pid);
    for pid in &remaining {
        match signal_pid(*pid, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(e) => {
                warn!(pid, error = %e, "Failed to force-kill stale app process");
            }
        }
    }

    if wait_until_superseded_processes_exit(
        install_dir,
        active_app_dir,
        main_exe,
        protected_pid,
        Duration::from_secs(2),
    ) {
        info!(
            count = pids.len(),
            forced = remaining.len(),
            "Force-killed stale app processes from superseded install directories"
        );
        return Ok(pids.len());
    }

    Err(SurgeError::Platform(format!(
        "Timed out waiting for stale '{main_exe}' processes from superseded install directories to exit"
    )))
}

#[cfg(unix)]
fn signal_pid(pid: u32, signal: nix::sys::signal::Signal) -> std::result::Result<(), nix::errno::Errno> {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(raw_pid) = i32::try_from(pid) else {
        return Ok(());
    };
    kill(Pid::from_raw(raw_pid), signal)
}

#[cfg(not(unix))]
fn terminate_superseded_app_processes_except(
    _install_dir: &Path,
    _active_app_dir: &Path,
    _main_exe: &str,
    _protected_pid: u32,
) -> Result<usize> {
    Ok(0)
}

#[cfg(unix)]
fn wait_until_superseded_processes_exit(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
    timeout: Duration,
) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if superseded_app_process_pids(install_dir, active_app_dir, main_exe, protected_pid).is_empty() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(unix)]
fn superseded_app_process_pids(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
) -> Vec<u32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };

    entries
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<u32>().ok())
        .filter(|pid| *pid != protected_pid)
        .filter(|pid| {
            std::fs::read_link(format!("/proc/{pid}/exe"))
                .map(normalize_proc_exe_path)
                .is_ok_and(|exe| is_superseded_app_exe(install_dir, active_app_dir, main_exe, &exe))
        })
        .collect()
}

#[cfg(unix)]
fn normalize_proc_exe_path(path: PathBuf) -> PathBuf {
    let normalized = {
        let path_text = path.to_string_lossy();
        path_text.strip_suffix(" (deleted)").map(PathBuf::from)
    };
    normalized.unwrap_or(path)
}

#[cfg(unix)]
fn is_superseded_app_exe(install_dir: &Path, active_app_dir: &Path, main_exe: &str, exe: &Path) -> bool {
    if exe == active_app_dir.join(main_exe) || exe.file_name().and_then(|name| name.to_str()) != Some(main_exe) {
        return false;
    }

    let Ok(relative) = exe.strip_prefix(install_dir) else {
        return false;
    };

    let mut components = relative.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return false;
    };
    let first = first.to_string_lossy();

    first == ".surge-app-prev" || first.starts_with("app-") || components.next().is_none()
}

fn supervisor_watch_args(
    supervisor_id: &str,
    install_dir: &Path,
    watched_pid: u32,
    handoff_version: &str,
    restart_args: &[String],
) -> Vec<String> {
    let mut args = vec![
        "watch".to_string(),
        "--id".to_string(),
        supervisor_id.to_string(),
        "--dir".to_string(),
        install_dir.to_string_lossy().into_owned(),
        "--pid".to_string(),
        watched_pid.to_string(),
        "--handoff-version".to_string(),
        handoff_version.to_string(),
    ];
    if !restart_args.is_empty() {
        args.push("--".to_string());
        args.extend(restart_args.iter().cloned());
    }
    args
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn supervisor_watch_args_include_handoff_version_before_child_args() {
        let restart_args = vec!["--app-mode".to_string(), "service".to_string()];

        let args = supervisor_watch_args("demo-supervisor", Path::new("/opt/demo"), 42, "2.0.0", &restart_args);

        assert_eq!(
            args,
            vec![
                "watch",
                "--id",
                "demo-supervisor",
                "--dir",
                "/opt/demo",
                "--pid",
                "42",
                "--handoff-version",
                "2.0.0",
                "--",
                "--app-mode",
                "service",
            ]
        );
        assert!(
            !args.iter().any(|arg| arg == "--exe"),
            "supervisor argv must not carry the app exe path so external pkill -f <app-path> cannot match it"
        );
    }

    #[cfg(unix)]
    #[test]
    fn restart_supervisor_retries_once_when_pid_file_never_appears() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path();
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();

        let attempts_log = install_dir.join("supervisor-attempts.log");
        let supervisor_path = active_app_dir.join(crate::platform::process::supervisor_binary_name());
        std::fs::write(
            &supervisor_path,
            format!("#!/bin/sh\necho attempt >> '{}'\nexit 0\n", attempts_log.display()),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&supervisor_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&supervisor_path, permissions).unwrap();

        let app_path = active_app_dir.join("demo-app");
        std::fs::write(&app_path, "#!/bin/sh\nexit 0\n").unwrap();
        let mut permissions = std::fs::metadata(&app_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&app_path, permissions).unwrap();

        let latest = ReleaseEntry {
            version: "2.0.0".to_string(),
            main_exe: "demo-app".to_string(),
            supervisor_id: "demo-supervisor".to_string(),
            ..ReleaseEntry::default()
        };

        let outcome = restart_supervisor_after_update_with_config(
            install_dir,
            &active_app_dir,
            &latest,
            std::process::id(),
            Duration::from_millis(200),
            2,
            Duration::from_millis(10),
        );

        match outcome {
            SupervisorRestartOutcome::PendingRestart { failure_phase, .. } => {
                assert_eq!(failure_phase, RESTART_HANDOFF_FAILED_PHASE);
            }
            SupervisorRestartOutcome::NotApplicable => panic!("expected PendingRestart failure, got NotApplicable"),
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut attempts = 0;
        while std::time::Instant::now() < deadline {
            attempts = std::fs::read_to_string(&attempts_log).map_or(0, |contents| contents.lines().count());
            if attempts >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            attempts, 2,
            "restart should spawn the supervisor exactly twice (one retry)"
        );
    }

    #[cfg(unix)]
    #[test]
    fn superseded_app_exe_detection_matches_retained_directories_only() {
        let install_dir = Path::new("/opt/demo");
        let active_app_dir = install_dir.join("app");

        assert!(is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/app-1.0.0/demo")
        ));
        assert!(is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/.surge-app-prev/demo")
        ));
        assert!(is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/demo")
        ));
        assert!(!is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/app/demo")
        ));
        assert!(!is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/app-1.0.0/other")
        ));
        assert!(!is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/srv/other/app-1.0.0/demo")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn proc_exe_deleted_suffix_is_ignored_for_matching() {
        assert_eq!(
            normalize_proc_exe_path(PathBuf::from("/opt/demo/app-1.0.0/demo (deleted)")),
            PathBuf::from("/opt/demo/app-1.0.0/demo")
        );
    }
}
