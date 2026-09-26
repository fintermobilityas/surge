use std::time::{Duration, Instant};

use super::ReleaseEntry;
use super::detached_identity::{installer_process_probe, process_identity_function};
use super::execution::{REMOTE_INSTALLER_FINAL_PATH, run_tailscale_capture};
use super::watchdog::read_remote_update_status_file;
use super::{Path, Result, SurgeError, logline, shell_single_quote};
use surge_core::context::StorageConfig;

pub(crate) const REMOTE_INSTALLER_LOG_PATH: &str = "/tmp/.surge-installer.log";
pub(crate) const REMOTE_INSTALLER_PID_PATH: &str = "/tmp/.surge-installer.pid";

/// A full package download on a slow tailnet link can take hours; the
/// detached monitor must outlive the interactive 30-minute stream timeout.
pub(crate) const DETACHED_INSTALL_MONITOR_TIMEOUT: Duration = Duration::from_hours(6);
pub(crate) const DETACHED_INSTALL_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Remote log/status checks each require a fresh SSH round trip.
pub(crate) const DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT: Duration = Duration::from_mins(5);
const DETACHED_INSTALL_LOG_TAIL_CAP: u64 = 256 * 1024;

pub(crate) struct RemoteInstallTarget<'a> {
    pub is_stage: bool,
    pub app_id: &'a str,
    pub rid: &'a str,
    pub release: &'a ReleaseEntry,
    pub channel: &'a str,
    pub storage: &'a StorageConfig,
}

pub(crate) struct RemoteDetachedInstallProbe {
    pub pid: Option<String>,
    pub alive: bool,
    pub unverified_alive: bool,
    pub log_size: u64,
    pub operation: Option<String>,
    pub exit_code: Option<i32>,
}

pub(crate) fn build_remote_detached_install_probe_command() -> String {
    format!(
        "set -eu; {}; \
log={REMOTE_INSTALLER_LOG_PATH}; \
logsize=0; \
if [ -f \"$log\" ]; then logsize=\"$(wc -c < \"$log\" | tr -d '[:space:]')\"; fi; \
operation=\"$(cat /tmp/.surge-installer.operation 2>/dev/null || true)\"; \
result=\"$(cat /tmp/.surge-installer.result 2>/dev/null || true)\"; \
printf 'operation=%s\\nresult=%s\\n' \"$operation\" \"$result\"; \
printf 'pid=%s\\nalive=%s\\nlogsize=%s\\nunverified=%s\\n' \"$pid\" \"$alive\" \"$logsize\" \"$unverified\"",
        installer_process_probe()
    )
}

pub(crate) fn parse_remote_detached_install_probe(output: &str) -> Result<RemoteDetachedInstallProbe> {
    let mut pid: Option<String> = None;
    let mut alive = false;
    let mut unverified_alive = false;
    let mut log_size = 0_u64;
    let mut operation = None;
    let mut exit_code = None;
    for line in output.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("pid=") {
            pid = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        } else if let Some(value) = line.strip_prefix("operation=") {
            operation = (!value.is_empty()).then(|| value.to_string());
        } else if let Some(value) = line.strip_prefix("result=") {
            if !value.is_empty() {
                exit_code = Some(
                    value
                        .parse::<i32>()
                        .map_err(|e| SurgeError::Platform(format!("Invalid detached installer result: {e}")))?,
                );
            }
        } else if let Some(value) = line.strip_prefix("alive=") {
            alive = value == "yes";
        } else if let Some(value) = line.strip_prefix("unverified=") {
            unverified_alive = value == "yes";
        } else if let Some(value) = line.strip_prefix("logsize=") {
            log_size = value.trim().parse::<u64>().map_err(|e| {
                SurgeError::Platform(format!("Remote detached install probe returned invalid log size: {e}"))
            })?;
        }
    }
    Ok(RemoteDetachedInstallProbe {
        pid,
        alive,
        unverified_alive,
        log_size,
        operation,
        exit_code,
    })
}

/// Build the command that launches the staged installer as a detached
/// node-local process (new session, SIGHUP-immune, stdout/stderr to the
/// install log) and reports the installer PID.
///
/// `flags` must contain only the fixed CLI flag tokens (`--no-start`,
/// `--stage`, `--reinstall`); it is interpolated unquoted into the inner
/// command on purpose.
pub(crate) fn build_remote_detached_install_launch_command(flags: &str, operation: &str) -> String {
    let flags = flags.trim();
    let inner = format!(
        "{}; identity=\"$(process_identity \"$$\")\" || exit 1; \
         printf '%s\\n' \"$identity\" > /tmp/.surge-installer.identity; \
         echo $$ > {REMOTE_INSTALLER_PID_PATH}.partial; mv {REMOTE_INSTALLER_PID_PATH}.partial {REMOTE_INSTALLER_PID_PATH}; {REMOTE_INSTALLER_FINAL_PATH} {flags}; result=$?; \
         printf '%s\\n' \"$result\" > /tmp/.surge-installer.result.partial; \
         mv /tmp/.surge-installer.result.partial /tmp/.surge-installer.result; exit \"$result\"",
        process_identity_function()
    );
    format!(
        "set -eu; \
if [ ! -x {REMOTE_INSTALLER_FINAL_PATH} ]; then echo 'remote installer binary is missing or not executable' >&2; exit 1; fi; \
rm -f /tmp/.surge-installer.result /tmp/.surge-installer.result.partial {REMOTE_INSTALLER_PID_PATH} {REMOTE_INSTALLER_PID_PATH}.partial; \
printf '%s\\n' {} > /tmp/.surge-installer.operation; \
: > {REMOTE_INSTALLER_LOG_PATH}; \
inner={}; \
if command -v setsid >/dev/null 2>&1; then \
  setsid sh -c \"$inner\" >> {REMOTE_INSTALLER_LOG_PATH} 2>&1 < /dev/null & \
else \
  nohup sh -c \"$inner\" >> {REMOTE_INSTALLER_LOG_PATH} 2>&1 < /dev/null & \
fi; \
attempt=0; \
while [ \"$attempt\" -lt 300 ]; do \
  pid=\"$(cat {REMOTE_INSTALLER_PID_PATH} 2>/dev/null || true)\"; \
  case \"$pid\" in ''|*[!0-9]*) ;; *) echo \"launched $pid\"; exit 0 ;; esac; \
  attempt=$((attempt + 1)); sleep 0.1; \
done; \
echo 'detached installer did not publish its PID within 30 seconds' >&2; exit 1",
        shell_single_quote(operation),
        shell_single_quote(&inner)
    )
}

pub(crate) fn build_remote_detached_install_log_tail_command(offset: u64) -> String {
    format!(
        "if [ -f {REMOTE_INSTALLER_LOG_PATH} ]; then tail -c +$(({offset} + 1)) {REMOTE_INSTALLER_LOG_PATH} | head -c {DETACHED_INSTALL_LOG_TAIL_CAP}; fi"
    )
}

pub(crate) fn build_remote_detached_install_cleanup_command(operation: &str) -> String {
    format!(
        "set -eu; current=\"$(cat /tmp/.surge-installer.operation 2>/dev/null || true)\"; \
         [ \"$current\" = {} ] || {{ echo 'installer operation changed; refusing cleanup' >&2; exit 1; }}; \
         {}; if [ \"$alive\" = yes ] || [ \"$unverified\" = yes ] || {{ [ -z \"$pid\" ] && [ ! -s /tmp/.surge-installer.result ]; }}; then exit 0; fi; \
         rm -f {REMOTE_INSTALLER_FINAL_PATH} {REMOTE_INSTALLER_PID_PATH} {REMOTE_INSTALLER_FINAL_PATH}.partial {REMOTE_INSTALLER_FINAL_PATH}.partial.meta {REMOTE_INSTALLER_PID_PATH}.partial /tmp/.surge-installer.operation /tmp/.surge-installer.result /tmp/.surge-installer.result.partial /tmp/.surge-installer.identity",
        shell_single_quote(operation),
        installer_process_probe()
    )
}

async fn run_remote_detached_install_script(ssh_target: &str, script: &str) -> Result<String> {
    let command = format!("sh -c {}", shell_single_quote(script));
    run_tailscale_capture(&["ssh", ssh_target, command.as_str()]).await
}

pub(crate) async fn probe_remote_detached_install(ssh_target: &str) -> Result<RemoteDetachedInstallProbe> {
    let raw = run_remote_detached_install_script(ssh_target, &build_remote_detached_install_probe_command()).await?;
    parse_remote_detached_install_probe(raw.trim())
}

pub(crate) async fn probe_remote_install_before_transfer(
    ssh_target: &str,
    installer_lock: &mut super::lock::RemoteInstallerLock,
    timeout: Duration,
) -> Result<RemoteDetachedInstallProbe> {
    let started = Instant::now();
    loop {
        installer_lock.ensure_held()?;
        let probe = probe_remote_detached_install(ssh_target).await?;
        if probe.operation.is_none() || probe.pid.is_some() || probe.exit_code.is_some() {
            return Ok(probe);
        }
        if started.elapsed() >= timeout {
            return Err(SurgeError::Platform(
                "A detached installer launch has not published its PID or result; preserving the operation and refusing transfer cleanup. Retry after startup completes or inspect the remote installer log.".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub(crate) async fn cleanup_remote_detached_install(ssh_target: &str, operation: &str) -> Result<()> {
    run_remote_detached_install_script(ssh_target, &build_remote_detached_install_cleanup_command(operation)).await?;
    Ok(())
}

pub(crate) async fn read_remote_detached_install_tail(ssh_target: &str) -> Option<String> {
    read_remote_detached_install_log_tail(ssh_target).await
}

async fn read_remote_detached_install_log_tail(ssh_target: &str) -> Option<String> {
    let command = format!("if [ -f {REMOTE_INSTALLER_LOG_PATH} ]; then tail -n 12 {REMOTE_INSTALLER_LOG_PATH}; fi");
    let raw = run_tailscale_capture(&["ssh", ssh_target, &format!("sh -c {}", shell_single_quote(&command))])
        .await
        .ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum WatchOutcome {
    Converged,
    InProgress,
}

async fn poll_remote_detached_install_once(
    ssh_target: &str,
    file_target: &str,
    install_root: &Path,
    log_offset: &mut u64,
    last_progress: &mut Instant,
    target: &RemoteInstallTarget<'_>,
    operation: &str,
) -> Result<WatchOutcome> {
    // Relay any new installer output first so the local console mirrors the
    // node even when the status file is quiet.
    let tail_raw =
        run_remote_detached_install_script(ssh_target, &build_remote_detached_install_log_tail_command(*log_offset))
            .await?;
    let had_log_output = !tail_raw.is_empty();
    if had_log_output {
        *last_progress = Instant::now();
        *log_offset = log_offset.saturating_add(tail_raw.len() as u64);
        for line in tail_raw.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                logline::subtle(&format!("remote: {trimmed}"));
            }
        }
    }

    let probe = probe_remote_detached_install(ssh_target).await?;
    if probe.unverified_alive {
        return Err(SurgeError::Platform(
            "Installer process identity is missing; refusing to watch an unverified PID".to_string(),
        ));
    }
    if probe.operation.as_deref() != Some(operation) {
        return Err(SurgeError::Platform(
            "Remote installer operation changed while watching".to_string(),
        ));
    }
    if target.is_stage {
        let outcome = stage_process_outcome(&probe, last_progress.elapsed())?;
        if outcome == WatchOutcome::Converged {
            super::state::verify_remote_stage_readiness(
                ssh_target,
                file_target,
                target.app_id,
                target.rid,
                target.release,
                target.channel,
                target.storage,
            )
            .await?;
        }
        return Ok(outcome);
    }
    if probe.exit_code.is_some_and(|code| code != 0) || (!probe.alive && probe.exit_code.is_none()) {
        return Err(SurgeError::Platform(format!(
            "Remote installer exited without success (result {:?})",
            probe.exit_code
        )));
    }
    let installer_finished = !probe.alive && probe.exit_code == Some(0);
    let status = read_remote_update_status_file(ssh_target, install_root)
        .await?
        .filter(|status| status.target_version == target.release.version);

    if let Some(status) = &status {
        if installer_finished && status.state == "failed" {
            return Err(SurgeError::Platform(format!(
                "Remote setup failed on '{file_target}'{}",
                status.format_context()
            )));
        }
        if installer_finished && status.is_terminal_success() && status.installed_version == target.release.version {
            return Ok(WatchOutcome::Converged);
        }
        if status.has_recent_progress(DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT) {
            return Ok(WatchOutcome::InProgress);
        }
    }

    if !probe.alive {
        // The installer can exit right before the final status write; give
        // the status file one grace re-read before declaring failure.
        tokio::time::sleep(DETACHED_INSTALL_POLL_INTERVAL).await;
        if let Some(status) = read_remote_update_status_file(ssh_target, install_root)
            .await?
            .filter(|status| status.target_version == target.release.version)
        {
            if status.state == "failed" {
                return Err(SurgeError::Platform(format!(
                    "Remote setup failed on '{file_target}'{}",
                    status.format_context()
                )));
            }
            if status.is_terminal_success() && status.installed_version == target.release.version {
                return Ok(WatchOutcome::Converged);
            }
            // Restart handoff in progress: the new process owns the status
            // file from here on.
            if status.state == "pending_restart" {
                return Ok(WatchOutcome::InProgress);
            }
        }
        let context = status.map_or_else(String::new, |status| status.format_context());
        return Err(SurgeError::Platform(format!(
            "The detached remote installer on '{file_target}' exited before converging{context}"
        )));
    }

    if last_progress.elapsed() < DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT {
        return Ok(WatchOutcome::InProgress);
    }

    Err(SurgeError::Platform(format!(
        "Timed out after {}s without fresh remote installer progress on '{file_target}'{}",
        DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT.as_secs(),
        status.map_or_else(String::new, |status| format!(": {}", status.format_context()))
    )))
}

fn stage_process_outcome(probe: &RemoteDetachedInstallProbe, quiet_for: Duration) -> Result<WatchOutcome> {
    match probe.exit_code {
        Some(0) if !probe.alive => Ok(WatchOutcome::Converged),
        Some(code) if code != 0 => Err(SurgeError::Platform(format!(
            "Detached staging installer failed with exit code {code}"
        ))),
        _ if !probe.alive => Err(SurgeError::Platform(
            "Detached staging installer exited without a completion result".to_string(),
        )),
        _ if quiet_for < DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT => Ok(WatchOutcome::InProgress),
        _ => Err(SurgeError::Platform(format!(
            "Timed out after {}s without fresh staging progress",
            DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT.as_secs()
        ))),
    }
}

/// Watch a detached node-local installer until it converges or fails.
///
/// Relays installer output and verifies the requested operation: staged cache
/// readiness for staging, application status for installation. Fails when the
/// process exits without converging, when progress goes stale, or when the
/// overall monitor timeout elapses.
pub(crate) async fn watch_remote_detached_install(
    ssh_target: &str,
    file_target: &str,
    install_root: &Path,
    start_log_offset: u64,
    target: &RemoteInstallTarget<'_>,
    operation: &str,
    installer_lock: &mut super::lock::RemoteInstallerLock,
) -> Result<()> {
    let started_at = Instant::now();
    let mut log_offset = start_log_offset;
    let mut last_progress = started_at;

    loop {
        installer_lock.ensure_held()?;
        if started_at.elapsed() >= DETACHED_INSTALL_MONITOR_TIMEOUT {
            return Err(SurgeError::Platform(format!(
                "Timed out after {}s waiting for the detached remote installer on '{file_target}' to converge",
                DETACHED_INSTALL_MONITOR_TIMEOUT.as_secs()
            )));
        }

        match poll_remote_detached_install_once(
            ssh_target,
            file_target,
            install_root,
            &mut log_offset,
            &mut last_progress,
            target,
            operation,
        )
        .await
        {
            Ok(WatchOutcome::Converged) => {
                logline::success(&format!(
                    "Detached remote installer on '{file_target}' converged ({}s).",
                    started_at.elapsed().as_secs()
                ));
                return Ok(());
            }
            Ok(WatchOutcome::InProgress) => {}
            Err(error) => {
                if let Some(tail) = read_remote_detached_install_log_tail(ssh_target).await {
                    return Err(SurgeError::Platform(format!(
                        "{error} — last installer log lines:\n{tail}"
                    )));
                }
                return Err(error);
            }
        }

        tokio::time::sleep(DETACHED_INSTALL_POLL_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn stage_waits_for_its_installer_result() {
        let mut probe = parse_remote_detached_install_probe("pid=1234\nalive=yes\noperation=stage-job\n").unwrap();
        assert_eq!(
            stage_process_outcome(&probe, Duration::from_secs(2)).unwrap(),
            WatchOutcome::InProgress
        );
        probe.exit_code = Some(0);
        assert_eq!(
            stage_process_outcome(&probe, Duration::from_secs(2)).unwrap(),
            WatchOutcome::InProgress
        );
        probe.alive = false;
        assert_eq!(
            stage_process_outcome(&probe, Duration::from_secs(2)).unwrap(),
            WatchOutcome::Converged
        );
    }

    #[test]
    fn stage_rejects_failed_or_missing_installer_result() {
        for result in ["", "1", "127"] {
            let probe = parse_remote_detached_install_probe(&format!("alive=no\nresult={result}\n")).unwrap();
            assert!(stage_process_outcome(&probe, Duration::ZERO).is_err());
        }
    }

    #[test]
    fn quiet_stage_gets_the_full_progress_timeout() {
        let probe = parse_remote_detached_install_probe("alive=yes\n").unwrap();
        assert_eq!(
            stage_process_outcome(
                &probe,
                DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT
                    .checked_sub(Duration::from_secs(1))
                    .unwrap()
            )
            .unwrap(),
            WatchOutcome::InProgress
        );
        assert!(stage_process_outcome(&probe, DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT).is_err());
    }

    #[test]
    fn probe_rejects_invalid_result() {
        assert!(parse_remote_detached_install_probe("result=not-a-code\n").is_err());
    }

    #[test]
    fn parse_remote_detached_install_probe_reads_fields() {
        let probe = parse_remote_detached_install_probe("pid=1234\nalive=yes\nlogsize=42\n").unwrap();
        assert_eq!(probe.pid.as_deref(), Some("1234"));
        assert!(probe.alive);
        assert_eq!(probe.log_size, 42);

        let probe = parse_remote_detached_install_probe("pid=\nalive=no\nlogsize=0\n").unwrap();
        assert_eq!(probe.pid, None);
        assert!(!probe.alive);
    }

    #[test]
    fn launch_command_detaches_and_reports_pid() {
        let command = build_remote_detached_install_launch_command("--no-start --reinstall", "test-operation");
        assert!(command.contains("setsid sh -c \"$inner\""));
        assert!(command.contains("nohup sh -c \"$inner\""));
        assert!(command.contains("echo $$ > /tmp/.surge-installer.pid.partial"));
        assert!(command.contains("/tmp/.surge-installer --no-start --reinstall; result=$?"));
        assert!(command.contains("2>&1 < /dev/null &"));
        assert!(command.contains("echo \"launched $pid\""));
    }

    #[test]
    fn launch_command_without_flags() {
        let command = build_remote_detached_install_launch_command("", "test-operation");
        assert!(command.contains("/tmp/.surge-installer ; result=$?"));
        assert!(!command.contains("--no-start"));
    }

    #[test]
    fn probe_command_reports_pid_aliveness_and_log_size() {
        let command = build_remote_detached_install_probe_command();
        assert!(command.contains("kill -0 \"$pid\""));
        assert!(command.contains("unverified=%s"));
    }

    #[test]
    fn cleanup_command_targets_expected_paths() {
        let cleanup = build_remote_detached_install_cleanup_command("test-operation");
        assert!(cleanup.contains("rm -f /tmp/.surge-installer /tmp/.surge-installer.pid"));
    }

    #[cfg(target_os = "linux")]
    fn script_for_temp_paths(
        script: &str,
        base: &Path,
    ) -> (String, std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let log_path = base.join(".surge-installer.log");
        let pid_path = base.join(".surge-installer.pid");
        let bin_path = base.join(".surge-installer");
        let script = script.replace("/tmp/.surge-installer", &bin_path.to_string_lossy());
        (script, bin_path, log_path, pid_path)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cleanup_preserves_a_different_operation_and_live_installer() {
        let temp = tempfile::tempdir().unwrap();
        let (script, bin, _, pid) =
            script_for_temp_paths(&build_remote_detached_install_cleanup_command("owned"), temp.path());
        let operation = temp.path().join(".surge-installer.operation");
        std::fs::write(&bin, "installer").unwrap();
        std::fs::write(&operation, "different").unwrap();
        let run = || std::process::Command::new("sh").args(["-c", &script]).output().unwrap();
        assert!(!run().status.success());
        assert!(bin.exists());
        assert_eq!(std::fs::read_to_string(&operation).unwrap(), "different");
        std::fs::write(&operation, "owned").unwrap();
        std::fs::write(&pid, std::process::id().to_string()).unwrap();
        assert!(run().status.success());
        assert!(bin.exists());
        std::fs::remove_file(pid).unwrap();
        assert!(run().status.success());
        assert!(bin.exists(), "a launch without a published PID must survive cleanup");
        std::fs::write(temp.path().join(".surge-installer.result"), "0").unwrap();
        assert!(run().status.success());
        assert!(!bin.exists());
        assert!(!operation.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detached_launch_fails_when_the_child_never_publishes_a_pid() {
        let temp = tempfile::tempdir().unwrap();
        let (script, bin, _, pid) = script_for_temp_paths(
            &build_remote_detached_install_launch_command("", "test-operation"),
            temp.path(),
        );
        for (path, body) in [
            (bin, "#!/bin/sh\nexit 0\n"),
            (temp.path().join("setsid"), "#!/bin/sh\nexit 7\n"),
            (temp.path().join("sleep"), "#!/bin/sh\nexit 0\n"),
        ] {
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let output = std::process::Command::new("sh")
            .args(["-c", &script])
            .env("PATH", format!("{}:/usr/bin:/bin", temp.path().display()))
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("did not publish its PID"));
        assert!(!pid.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detached_launch_command_runs_installer_detached_and_reports_pid() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (script, bin_path, log_path, pid_path) = script_for_temp_paths(
            &build_remote_detached_install_launch_command("", "test-operation"),
            temp_dir.path(),
        );

        // Fake installer: reports start/end and stays alive long enough to probe.
        std::fs::write(
            &bin_path,
            "#!/bin/sh\necho installer-started\nsleep 1\necho installer-done\n",
        )
        .unwrap();
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        let shim = temp_dir.path().join("setsid");
        std::fs::write(&shim, "#!/bin/sh\nsleep 1\nexec /usr/bin/setsid \"$@\"\n").unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
        let path = format!("{}:/usr/bin:/bin", temp_dir.path().display());

        // The parent exits after PID publication, like an ending SSH session.
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(&script)
            .env("PATH", path)
            .output()
            .expect("run launch script");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "script failed: {stdout}");
        assert!(stdout.contains("launched "), "expected a launched pid, got: {stdout}");

        let pid = std::fs::read_to_string(&pid_path)
            .expect("pidfile written")
            .trim()
            .to_string();
        assert!(!pid.is_empty());
        let pid_alive = |pid: &str| {
            std::process::Command::new("kill")
                .args(["-0", pid])
                .output()
                .expect("kill probe")
                .status
                .success()
        };
        assert!(pid_alive(&pid), "installer should be alive after the launcher exits");

        // Let the fake installer run to completion, then verify the log.
        std::thread::sleep(std::time::Duration::from_millis(1500));
        assert!(!pid_alive(&pid), "installer should have exited");
        let log = std::fs::read_to_string(&log_path).expect("log written");
        assert!(log.contains("installer-started"), "log was: {log}");
        assert!(log.contains("installer-done"), "log was: {log}");
        let (probe_script, _, _, _) =
            script_for_temp_paths(&build_remote_detached_install_probe_command(), temp_dir.path());
        let output = std::process::Command::new("sh")
            .args(["-c", &probe_script])
            .output()
            .unwrap();
        let probe = parse_remote_detached_install_probe(&String::from_utf8(output.stdout).unwrap()).unwrap();
        assert_eq!(probe.operation.as_deref(), Some("test-operation"));
        assert_eq!(probe.exit_code, Some(0));
        assert!(!probe.alive);
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "detached_stage_tests.rs"]
mod stage_tests;
