use std::time::{Duration, Instant};

use super::execution::{REMOTE_INSTALLER_FINAL_PATH, run_tailscale_capture};
use super::watchdog::read_remote_update_status_file;
use super::{Path, Result, SurgeError, logline, shell_single_quote};

pub(crate) const REMOTE_INSTALLER_LOG_PATH: &str = "/tmp/.surge-installer.log";
pub(crate) const REMOTE_INSTALLER_PID_PATH: &str = "/tmp/.surge-installer.pid";

/// A full package download on a slow tailnet link can take hours; the
/// detached monitor must outlive the interactive 30-minute stream timeout.
pub(crate) const DETACHED_INSTALL_MONITOR_TIMEOUT: Duration = Duration::from_hours(6);
pub(crate) const DETACHED_INSTALL_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// The status file is the authoritative liveness signal; allow more slack
/// than the interactive watchdog because every check is a fresh SSH round
/// trip and slow links slow both down.
pub(crate) const DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT: Duration = Duration::from_mins(5);
const DETACHED_INSTALL_LOG_TAIL_CAP: u64 = 256 * 1024;

pub(crate) struct RemoteDetachedInstallProbe {
    pub pid: Option<String>,
    pub alive: bool,
    pub log_size: u64,
}

pub(crate) fn build_remote_detached_install_probe_command() -> String {
    format!(
        "set -eu; \
pidfile={REMOTE_INSTALLER_PID_PATH}; \
log={REMOTE_INSTALLER_LOG_PATH}; \
pid=''; \
if [ -f \"$pidfile\" ]; then pid=\"$(tr -d '[:space:]' < \"$pidfile\")\"; fi; \
alive=no; \
if [ -n \"$pid\" ]; then \
  case \"$pid\" in \
    ''|*[!0-9]*) : ;; \
    *) if kill -0 \"$pid\" 2>/dev/null; then alive=yes; fi ;; \
  esac; \
fi; \
logsize=0; \
if [ -f \"$log\" ]; then logsize=\"$(wc -c < \"$log\" | tr -d '[:space:]')\"; fi; \
printf 'pid=%s\\nalive=%s\\nlogsize=%s\\n' \"$pid\" \"$alive\" \"$logsize\""
    )
}

pub(crate) fn parse_remote_detached_install_probe(output: &str) -> Result<RemoteDetachedInstallProbe> {
    let mut pid: Option<String> = None;
    let mut alive = false;
    let mut log_size = 0_u64;
    for line in output.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix("pid=") {
            pid = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        } else if let Some(value) = line.strip_prefix("alive=") {
            alive = value == "yes";
        } else if let Some(value) = line.strip_prefix("logsize=") {
            log_size = value.trim().parse::<u64>().map_err(|e| {
                SurgeError::Platform(format!("Remote detached install probe returned invalid log size: {e}"))
            })?;
        }
    }
    Ok(RemoteDetachedInstallProbe { pid, alive, log_size })
}

/// Build the command that launches the staged installer as a detached
/// node-local process (new session, SIGHUP-immune, stdout/stderr to the
/// install log) and reports the installer PID.
///
/// `flags` must contain only the fixed CLI flag tokens (`--no-start`,
/// `--stage`, `--reinstall`); it is interpolated unquoted into the inner
/// command on purpose.
pub(crate) fn build_remote_detached_install_launch_command(flags: &str) -> String {
    let flags = flags.trim();
    let inner = if flags.is_empty() {
        format!("echo $$ > {REMOTE_INSTALLER_PID_PATH}; exec {REMOTE_INSTALLER_FINAL_PATH}")
    } else {
        format!("echo $$ > {REMOTE_INSTALLER_PID_PATH}; exec {REMOTE_INSTALLER_FINAL_PATH} {flags}")
    };
    format!(
        "set -eu; \
if [ ! -x {REMOTE_INSTALLER_FINAL_PATH} ]; then echo 'remote installer binary is missing or not executable' >&2; exit 1; fi; \
: > {REMOTE_INSTALLER_LOG_PATH}; \
inner='{inner}'; \
if command -v setsid >/dev/null 2>&1; then \
  setsid sh -c \"$inner\" >> {REMOTE_INSTALLER_LOG_PATH} 2>&1 < /dev/null & \
else \
  nohup sh -c \"$inner\" >> {REMOTE_INSTALLER_LOG_PATH} 2>&1 < /dev/null & \
fi; \
sleep 0.3; \
if [ -f {REMOTE_INSTALLER_PID_PATH} ]; then \
  echo \"launched $(tr -d '[:space:]' < {REMOTE_INSTALLER_PID_PATH})\"; \
else \
  echo launched; \
fi"
    )
}

pub(crate) fn build_remote_detached_install_log_tail_command(offset: u64) -> String {
    format!(
        "if [ -f {REMOTE_INSTALLER_LOG_PATH} ]; then tail -c +$(({offset} + 1)) {REMOTE_INSTALLER_LOG_PATH} | head -c {DETACHED_INSTALL_LOG_TAIL_CAP}; fi"
    )
}

pub(crate) fn build_remote_detached_install_stop_command() -> String {
    format!(
        "pidfile={REMOTE_INSTALLER_PID_PATH}; \
pid=\"$(tr -d '[:space:]' < \"$pidfile\" 2>/dev/null || true)\"; \
case \"$pid\" in \
  ''|*[!0-9]*) rm -f \"$pidfile\"; echo none ;; \
  *) kill \"$pid\" 2>/dev/null || true; \
    i=0; \
    while kill -0 \"$pid\" 2>/dev/null && [ \"$i\" -lt 50 ]; do sleep 0.1; i=$((i + 1)); done; \
    kill -KILL \"$pid\" 2>/dev/null || true; \
    rm -f \"$pidfile\"; echo stopped ;; \
esac"
    )
}

pub(crate) fn build_remote_detached_install_cleanup_command() -> String {
    format!(
        "rm -f {REMOTE_INSTALLER_FINAL_PATH} {REMOTE_INSTALLER_PID_PATH} {REMOTE_INSTALLER_FINAL_PATH}.partial {REMOTE_INSTALLER_FINAL_PATH}.partial.meta"
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

pub(crate) async fn stop_remote_detached_install(ssh_target: &str) -> Result<()> {
    run_remote_detached_install_script(ssh_target, &build_remote_detached_install_stop_command()).await?;
    Ok(())
}

pub(crate) async fn cleanup_remote_detached_install(ssh_target: &str) -> Result<()> {
    run_remote_detached_install_script(ssh_target, &build_remote_detached_install_cleanup_command()).await?;
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

enum WatchOutcome {
    Converged,
    InProgress,
}

async fn poll_remote_detached_install_once(
    ssh_target: &str,
    file_target: &str,
    install_root: &Path,
    log_offset: &mut u64,
) -> Result<WatchOutcome> {
    // Relay any new installer output first so the local console mirrors the
    // node even when the status file is quiet.
    let tail_raw =
        run_remote_detached_install_script(ssh_target, &build_remote_detached_install_log_tail_command(*log_offset))
            .await?;
    let had_log_output = !tail_raw.is_empty();
    if had_log_output {
        *log_offset = log_offset.saturating_add(tail_raw.len() as u64);
        for line in tail_raw.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                logline::subtle(&format!("remote: {trimmed}"));
            }
        }
    }

    let probe = probe_remote_detached_install(ssh_target).await?;
    let status = read_remote_update_status_file(ssh_target, install_root).await?;

    if let Some(status) = &status {
        if status.state == "failed" {
            return Err(SurgeError::Platform(format!(
                "Remote setup failed on '{file_target}'{}",
                status.format_context()
            )));
        }
        if status.is_terminal_success() {
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
        if let Some(status) = read_remote_update_status_file(ssh_target, install_root).await? {
            if status.state == "failed" {
                return Err(SurgeError::Platform(format!(
                    "Remote setup failed on '{file_target}'{}",
                    status.format_context()
                )));
            }
            if status.is_terminal_success() {
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

    if had_log_output {
        return Ok(WatchOutcome::InProgress);
    }

    Err(SurgeError::Platform(format!(
        "Timed out after {}s without fresh remote installer progress on '{file_target}{}",
        DETACHED_INSTALL_STALE_PROGRESS_TIMEOUT.as_secs(),
        status.map_or_else(String::new, |status| format!(": {}", status.format_context()))
    )))
}

/// Watch a detached node-local installer until it converges or fails.
///
/// Relays new installer log bytes to the local console, treats the node's
/// update status file as the authoritative outcome, and fails when the
/// process exits without converging, when progress goes stale, or when the
/// overall monitor timeout elapses.
pub(crate) async fn watch_remote_detached_install(
    ssh_target: &str,
    file_target: &str,
    install_root: &Path,
    start_log_offset: u64,
) -> Result<()> {
    let started_at = Instant::now();
    let mut log_offset = start_log_offset;

    loop {
        if started_at.elapsed() >= DETACHED_INSTALL_MONITOR_TIMEOUT {
            return Err(SurgeError::Platform(format!(
                "Timed out after {}s waiting for the detached remote installer on '{file_target}' to converge",
                DETACHED_INSTALL_MONITOR_TIMEOUT.as_secs()
            )));
        }

        match poll_remote_detached_install_once(ssh_target, file_target, install_root, &mut log_offset).await {
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
        let command = build_remote_detached_install_launch_command("--no-start --reinstall");
        assert!(command.contains("setsid sh -c \"$inner\""));
        assert!(command.contains("nohup sh -c \"$inner\""));
        assert!(command.contains("echo $$ > /tmp/.surge-installer.pid"));
        assert!(command.contains("exec /tmp/.surge-installer --no-start --reinstall"));
        assert!(command.contains("2>&1 < /dev/null &"));
        assert!(command.contains("echo \"launched $(tr -d '[:space:]' < /tmp/.surge-installer.pid)\""));
    }

    #[test]
    fn launch_command_without_flags() {
        let command = build_remote_detached_install_launch_command("");
        assert!(command.contains("exec /tmp/.surge-installer'"));
        assert!(!command.contains("--no-start"));
    }

    #[test]
    fn probe_command_reports_pid_aliveness_and_log_size() {
        let command = build_remote_detached_install_probe_command();
        assert!(command.contains("kill -0 \"$pid\""));
        assert!(command.contains("printf 'pid=%s\\nalive=%s\\nlogsize=%s\\n'"));
    }

    #[test]
    fn stop_and_cleanup_commands_target_expected_paths() {
        let stop = build_remote_detached_install_stop_command();
        assert!(stop.contains("kill \"$pid\""));
        assert!(stop.contains("kill -KILL \"$pid\""));
        assert!(stop.contains("rm -f \"$pidfile\""));

        let cleanup = build_remote_detached_install_cleanup_command();
        assert!(cleanup.contains("rm -f /tmp/.surge-installer /tmp/.surge-installer.pid"));
    }

    #[cfg(unix)]
    fn script_for_temp_paths(
        script: &str,
        base: &Path,
    ) -> (String, std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let log_path = base.join(".surge-installer.log");
        let pid_path = base.join(".surge-installer.pid");
        let bin_path = base.join(".surge-installer");
        let script = script
            .replace("/tmp/.surge-installer.log", &log_path.to_string_lossy())
            .replace("/tmp/.surge-installer.pid", &pid_path.to_string_lossy())
            .replace("/tmp/.surge-installer", &bin_path.to_string_lossy());
        (script, bin_path, log_path, pid_path)
    }

    #[cfg(unix)]
    #[test]
    fn detached_launch_command_runs_installer_detached_and_reports_pid() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let (script, bin_path, log_path, pid_path) =
            script_for_temp_paths(&build_remote_detached_install_launch_command(""), temp_dir.path());

        // Fake installer: reports start/end and stays alive long enough to probe.
        std::fs::write(
            &bin_path,
            "#!/bin/sh\necho installer-started\nsleep 1\necho installer-done\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755)).unwrap();

        // The parent exits immediately after launch, like an ending SSH session.
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(&script)
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
    }
}
