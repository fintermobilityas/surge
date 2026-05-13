use std::process::ExitStatus;
use std::time::{Duration, Instant};

use serde::Deserialize;
use surge_core::update::status::UPDATE_STATUS_FILE_NAME;

use super::execution::run_tailscale_capture;
use super::{Path, Result, SurgeError, logline, shell_single_quote};

const TAILSCALE_STREAM_COMMAND_TIMEOUT: Duration = Duration::from_mins(30);
const REMOTE_SETUP_STALE_PROGRESS_TIMEOUT: Duration = Duration::from_mins(2);
const REMOTE_SETUP_WATCHDOG_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub(crate) struct RemoteSetupWatchdog {
    ssh_node: String,
    install_root: std::path::PathBuf,
    stale_timeout: Duration,
}

impl RemoteSetupWatchdog {
    pub(crate) fn new(ssh_node: &str, install_root: &Path) -> Self {
        Self {
            ssh_node: ssh_node.to_string(),
            install_root: install_root.to_path_buf(),
            stale_timeout: REMOTE_SETUP_STALE_PROGRESS_TIMEOUT,
        }
    }
}

pub(super) async fn wait_for_tailscale_command_with_status_watchdog(
    child: &mut tokio::process::Child,
    cmd: &str,
    watchdog: &RemoteSetupWatchdog,
    mut progress_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
) -> Result<ExitStatus> {
    let started_at = Instant::now();
    let mut last_progress_at = Instant::now();

    loop {
        if started_at.elapsed() >= TAILSCALE_STREAM_COMMAND_TIMEOUT {
            let _ = child.kill().await;
            return Err(SurgeError::Platform(format!(
                "Timed out after {}s running {cmd}",
                TAILSCALE_STREAM_COMMAND_TIMEOUT.as_secs()
            )));
        }

        if last_progress_at.elapsed() >= watchdog.stale_timeout {
            let status = read_remote_update_status(watchdog).await?;
            if let Some(status) = status.as_ref() {
                if status.is_terminal_failure() {
                    let _ = child.kill().await;
                    return Err(SurgeError::Platform(format!(
                        "Remote setup failed{}",
                        status.format_context()
                    )));
                }
                if status.has_recent_progress(watchdog.stale_timeout) {
                    logline::subtle(&format!("remote: setup still in progress{}", status.format_context()));
                    last_progress_at = Instant::now();
                    continue;
                }
            }

            let _ = child.kill().await;
            let context = status.map_or_else(String::new, |status| status.format_context());
            return Err(SurgeError::Platform(format!(
                "Timed out after {}s without fresh remote setup progress{context}",
                watchdog.stale_timeout.as_secs()
            )));
        }

        tokio::select! {
            status = child.wait() => {
                return status.map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale command: {e}")));
            }
            progress = progress_rx.recv() => {
                if progress.is_some() {
                    last_progress_at = Instant::now();
                }
            }
            () = tokio::time::sleep(REMOTE_SETUP_WATCHDOG_POLL_INTERVAL) => {}
        }
    }
}

async fn read_remote_update_status(watchdog: &RemoteSetupWatchdog) -> Result<Option<RemoteUpdateStatusSnapshot>> {
    let status_path = watchdog.install_root.join(UPDATE_STATUS_FILE_NAME);
    let script = format!(
        "status_path={}; if [ -f \"$status_path\" ]; then cat \"$status_path\"; fi",
        shell_single_quote(&status_path.to_string_lossy())
    );
    let command = format!("sh -c {}", shell_single_quote(&script));
    let raw = run_tailscale_capture(&["ssh", watchdog.ssh_node.as_str(), command.as_str()]).await?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    serde_json::from_str(trimmed)
        .map(Some)
        .map_err(|e| SurgeError::Config(format!("Failed to decode remote update status: {e}")))
}

#[derive(Debug, Deserialize)]
struct RemoteUpdateStatusSnapshot {
    state: String,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    last_progress_at_utc: Option<String>,
    #[serde(default)]
    current_phase: Option<String>,
    #[serde(default)]
    last_completed_phase: Option<String>,
    #[serde(default)]
    failure_phase: Option<String>,
    #[serde(default)]
    retry_safe: Option<bool>,
}

impl RemoteUpdateStatusSnapshot {
    fn has_recent_progress(&self, stale_timeout: Duration) -> bool {
        let Some(last_progress_at_utc) = self.last_progress_at_utc.as_deref() else {
            return false;
        };
        let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(last_progress_at_utc) else {
            return false;
        };
        match chrono::Utc::now()
            .signed_duration_since(parsed.with_timezone(&chrono::Utc))
            .to_std()
        {
            Ok(age) => age <= stale_timeout,
            Err(_) => true,
        }
    }

    fn is_terminal_failure(&self) -> bool {
        self.state == "failed" || self.state == "pending_restart"
    }

    fn format_context(&self) -> String {
        let mut parts = vec![format!("state={}", self.state)];
        if let Some(phase) = self
            .failure_phase
            .as_deref()
            .or(self.current_phase.as_deref())
            .or(self.last_completed_phase.as_deref())
        {
            parts.push(format!("phase={phase}"));
        }
        if let Some(last_progress) = self.last_progress_at_utc.as_deref() {
            parts.push(format!("last_progress_at_utc={last_progress}"));
        }
        if let Some(reason) = self.reason.as_deref() {
            parts.push(format!("reason={reason}"));
        }
        if let Some(retry_safe) = self.retry_safe {
            parts.push(format!("retry_safe={retry_safe}"));
        }
        format!(" ({})", parts.join(", "))
    }
}
