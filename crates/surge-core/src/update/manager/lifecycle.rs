use std::path::Path;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::error::Result;
#[cfg(not(unix))]
use crate::error::SurgeError;
use crate::platform::process::{ProcessHandle, current_pid, spawn_detached, spawn_process, supervisor_binary_name};
use crate::releases::manifest::ReleaseEntry;
use crate::supervisor::state::{read_restart_args, write_supervisor_exe_path};
#[cfg(not(unix))]
use crate::supervisor::state::{supervisor_pid_file, supervisor_stop_file};
use crate::update::status::{
    RESTART_HANDOFF_FAILED_PHASE, RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE, confirm_supervisor_restart,
};

const SUPERVISOR_RESTART_CONFIRM_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISOR_RESTART_MAX_ATTEMPTS: u32 = 2;
const SUPERVISOR_RESTART_RETRY_DELAY: Duration = Duration::from_millis(500);
mod process_quiescence;
#[cfg(unix)]
mod supervisor_takeover;

#[cfg(not(unix))]
pub(super) use self::process_quiescence::terminate_active_app_processes_before_swap;
pub(super) use self::process_quiescence::terminate_superseded_app_processes;
#[cfg(unix)]
pub(super) use self::process_quiescence::{
    PreparedAppQuiescence, prepare_app_quiescence, terminate_prepared_app_processes,
};
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(in crate::update::manager) use self::process_quiescence::{spawn_native_test_app, wait_for_native_test_app};

#[derive(Debug, Clone)]
pub(super) enum SupervisorRestartOutcome {
    /// No supervisor was configured for this release, so there is no
    /// post-update restart to confirm.
    NotApplicable,
    /// The target package is installed, but restart handoff is still pending.
    /// The supervisor writes the converged record only after the old child exits
    /// and a replacement target child stays active.
    PendingRestart {
        reason: String,
        failure_phase: &'static str,
    },
}

pub(super) async fn request_supervisor_shutdown(install_dir: &Path, supervisor_id: &str) -> Result<Option<u32>> {
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
) -> Result<Option<u32>> {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return Ok(None);
    }

    #[cfg(unix)]
    return supervisor_takeover::request_shutdown(install_dir, supervisor_id, timeout, poll_interval).await;

    #[cfg(not(unix))]
    {
        request_legacy_supervisor_shutdown(install_dir, supervisor_id, timeout, poll_interval).await
    }
}

#[cfg(not(unix))]
async fn request_legacy_supervisor_shutdown(
    install_dir: &Path,
    supervisor_id: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<Option<u32>> {
    let pid_file = supervisor_pid_file(install_dir, supervisor_id);
    if !pid_file.is_file() {
        return Ok(None);
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
    Ok(None)
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

#[cfg(unix)]
pub(super) fn restore_previous_supervisor_after_failed_quiescence(
    install_dir: &Path,
    active_app_dir: &Path,
    version: &str,
    main_exe: &str,
    supervisor_id: &str,
    environment: &std::collections::BTreeMap<String, String>,
    watched_pid: u32,
) -> SupervisorRestartOutcome {
    let previous = ReleaseEntry {
        version: version.to_string(),
        main_exe: main_exe.to_string(),
        supervisor_id: supervisor_id.to_string(),
        environment: environment.clone(),
        ..ReleaseEntry::default()
    };
    restart_supervisor_after_update_with_config(
        install_dir,
        active_app_dir,
        &previous,
        Some(watched_pid),
        None,
        SUPERVISOR_RESTART_CONFIRM_TIMEOUT,
        SUPERVISOR_RESTART_MAX_ATTEMPTS,
        SUPERVISOR_RESTART_RETRY_DELAY,
    )
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
        Some(watched_pid),
        Some(&latest.version),
        SUPERVISOR_RESTART_CONFIRM_TIMEOUT,
        SUPERVISOR_RESTART_MAX_ATTEMPTS,
        SUPERVISOR_RESTART_RETRY_DELAY,
    )
}

#[cfg(unix)]
pub(super) fn restart_supervisor_immediately(
    install_dir: &Path,
    active_app_dir: &Path,
    release: &ReleaseEntry,
) -> SupervisorRestartOutcome {
    restart_supervisor_after_update_with_config(
        install_dir,
        active_app_dir,
        release,
        None,
        None,
        SUPERVISOR_RESTART_CONFIRM_TIMEOUT,
        SUPERVISOR_RESTART_MAX_ATTEMPTS,
        SUPERVISOR_RESTART_RETRY_DELAY,
    )
}

fn restart_supervisor_after_update_with_config(
    install_dir: &Path,
    active_app_dir: &Path,
    latest: &ReleaseEntry,
    watched_pid: Option<u32>,
    handoff_version: Option<&str>,
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

    let args = supervisor_restart_args(supervisor_id, install_dir, watched_pid, handoff_version, &restart_args);
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
                    let reason = handoff_version.map_or_else(
                        || {
                            watched_pid.map_or_else(
                                || "previous supervisor restoration accepted".to_string(),
                                |watched_pid| {
                                    format!(
                                        "previous supervisor restoration accepted; waiting for process pid {watched_pid} to exit"
                                    )
                                },
                            )
                        },
                        |version| {
                            watched_pid.map_or_else(
                                || format!("supervisor restart accepted; waiting for target version {version} to start"),
                                |watched_pid| {
                                    format!(
                                        "supervisor handoff accepted; waiting for previous child pid {watched_pid} to exit and target version {version} to start"
                                    )
                                },
                            )
                        },
                    );
                    return SupervisorRestartOutcome::PendingRestart {
                        reason,
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

fn supervisor_restart_args(
    supervisor_id: &str,
    install_dir: &Path,
    watched_pid: Option<u32>,
    handoff_version: Option<&str>,
    restart_args: &[String],
) -> Vec<String> {
    let mut args = vec![
        if watched_pid.is_some() { "watch" } else { "run" }.to_string(),
        "--id".to_string(),
        supervisor_id.to_string(),
        "--dir".to_string(),
        install_dir.to_string_lossy().into_owned(),
    ];
    if let Some(watched_pid) = watched_pid {
        args.push("--pid".to_string());
        args.push(watched_pid.to_string());
    }
    if watched_pid.is_some()
        && let Some(handoff_version) = handoff_version
    {
        args.push("--handoff-version".to_string());
        args.push(handoff_version.to_string());
    }
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
    #[cfg(unix)]
    use crate::platform::process::ProcessIdentity;
    #[cfg(unix)]
    use crate::supervisor::state::{
        SupervisorTakeoverAcknowledgement, SupervisorTakeoverInstance, SupervisorTakeoverRequest,
        accept_supervisor_takeover_request, read_accepted_supervisor_takeover, read_supervisor_takeover_commit,
        read_supervisor_takeover_request, supervisor_pid_file, supervisor_stop_file, supervisor_takeover_request_file,
        write_supervisor_takeover_acknowledgement, write_supervisor_takeover_instance,
    };

    #[cfg(unix)]
    fn wait_for_takeover_request(install_dir: &Path, supervisor_id: &str) -> SupervisorTakeoverRequest {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(request) = read_supervisor_takeover_request(install_dir, supervisor_id).unwrap() {
                return request;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "takeover request did not appear before the test deadline"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(unix)]
    fn prepare_supervisor_instance(
        install_dir: &Path,
        supervisor_id: &str,
        supervisor_pid: u32,
    ) -> SupervisorTakeoverInstance {
        std::fs::write(
            supervisor_pid_file(install_dir, supervisor_id),
            supervisor_pid.to_string(),
        )
        .unwrap();
        let instance = SupervisorTakeoverInstance::new(supervisor_pid);
        write_supervisor_takeover_instance(install_dir, supervisor_id, &instance).unwrap();
        instance
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn acknowledged_supervisor_handoff_returns_the_refreshed_child_pid() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor_id = "demo-supervisor";
        let supervisor_pid = 42;
        prepare_supervisor_instance(dir.path(), supervisor_id, supervisor_pid);
        let install_dir = dir.path().to_path_buf();
        let responder = std::thread::spawn(move || {
            let request = wait_for_takeover_request(&install_dir, supervisor_id);
            let acknowledgement =
                SupervisorTakeoverAcknowledgement::new(&request, Some(ProcessIdentity { pid: 84, generation: 7 }));
            write_supervisor_takeover_acknowledgement(&install_dir, supervisor_id, &acknowledgement).unwrap();

            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                if read_supervisor_takeover_commit(&install_dir, supervisor_id)
                    .unwrap()
                    .is_some_and(|commit| commit.matches_acknowledgement(&acknowledgement))
                {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "takeover commit did not appear before the test deadline"
                );
                std::thread::sleep(Duration::from_millis(5));
            }

            assert!(accept_supervisor_takeover_request(&install_dir, supervisor_id, &request).unwrap());
            std::fs::remove_file(supervisor_pid_file(&install_dir, supervisor_id)).unwrap();
        });

        let child_pid = request_supervisor_shutdown_with_timeout(
            dir.path(),
            supervisor_id,
            Duration::from_secs(1),
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        responder.join().unwrap();

        assert_eq!(child_pid, Some(84));
        assert!(
            read_accepted_supervisor_takeover(dir.path(), supervisor_id)
                .unwrap()
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_supervisor_is_not_stopped_without_acknowledgement_support() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor_id = "demo-supervisor";
        std::fs::write(supervisor_pid_file(dir.path(), supervisor_id), "42").unwrap();

        let error = request_supervisor_shutdown_with_timeout(
            dir.path(),
            supervisor_id,
            Duration::from_millis(50),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("does not advertise acknowledged takeover"));
        assert!(!supervisor_stop_file(dir.path(), supervisor_id).exists());
        assert!(!supervisor_takeover_request_file(dir.path(), supervisor_id).exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_takeover_is_cancelled_before_a_late_acceptance() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor_id = "demo-supervisor";
        prepare_supervisor_instance(dir.path(), supervisor_id, 42);
        let install_dir = dir.path().to_path_buf();
        let late_responder = std::thread::spawn(move || {
            let request = wait_for_takeover_request(&install_dir, supervisor_id);
            std::thread::sleep(Duration::from_millis(150));
            accept_supervisor_takeover_request(&install_dir, supervisor_id, &request).unwrap()
        });

        let error = request_supervisor_shutdown_with_timeout(
            dir.path(),
            supervisor_id,
            Duration::from_millis(50),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("Timed out waiting"));
        assert!(
            !late_responder.join().unwrap(),
            "a cancelled request must not be accepted later"
        );
        assert!(!supervisor_takeover_request_file(dir.path(), supervisor_id).exists());
        assert!(supervisor_pid_file(dir.path(), supervisor_id).exists());
    }

    #[test]
    fn supervisor_watch_args_include_handoff_version_before_child_args() {
        let restart_args = vec!["--app-mode".to_string(), "service".to_string()];

        let args = supervisor_restart_args(
            "demo-supervisor",
            Path::new("/opt/demo"),
            Some(42),
            Some("2.0.0"),
            &restart_args,
        );

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

    #[test]
    fn previous_supervisor_restoration_omits_update_handoff_version() {
        let args = supervisor_restart_args("demo-supervisor", Path::new("/opt/demo"), Some(42), None, &[]);

        assert!(!args.iter().any(|argument| argument == "--handoff-version"));
    }

    #[test]
    fn previous_supervisor_without_surviving_child_starts_immediately() {
        let args = supervisor_restart_args("demo-supervisor", Path::new("/opt/demo"), None, None, &[]);

        assert_eq!(args[0], "run");
        assert!(!args.iter().any(|argument| argument == "--pid"));
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
            Some(std::process::id()),
            Some(&latest.version),
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
}
