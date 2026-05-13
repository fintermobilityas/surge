//! Explicit update convergence state recorded per install root.
//!
//! After a channel promotion, operators need a reliable signal that distinguishes:
//! - **`Idle`** — the install completed but no update has been attempted since.
//! - **`InProgress`** — an update is currently being applied.
//! - **`Converged`** — the latest update applied to disk and the supervisor restart
//!   (or the install-time auto-start) was confirmed by observing the supervisor pid
//!   file.
//! - **`PendingRestart`** — the latest update applied to disk but the supervisor
//!   restart could not be confirmed within the post-update window. The runtime
//!   process may still be running an older binary even though `installed_version`
//!   already reflects the new release.
//! - **`Failed`** — the most recent attempt failed before the install swap could
//!   complete. The `installed_version` field reflects the pre-attempt state.
//!
//! This record is persisted at `{install_dir}/.surge-update-status.json` so it
//! survives the active app directory swap that happens on every successful update.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::supervisor::state::supervisor_pid_file;

pub const UPDATE_STATUS_FILE_NAME: &str = ".surge-update-status.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateConvergenceState {
    Idle,
    InProgress,
    Converged,
    PendingRestart,
    Failed,
}

impl UpdateConvergenceState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            UpdateConvergenceState::Idle => "idle",
            UpdateConvergenceState::InProgress => "in_progress",
            UpdateConvergenceState::Converged => "converged",
            UpdateConvergenceState::PendingRestart => "pending_restart",
            UpdateConvergenceState::Failed => "failed",
        }
    }
}

impl std::fmt::Display for UpdateConvergenceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A point-in-time snapshot of the install's convergence to a channel release.
///
/// `installed_version` always reflects what is on disk in the active app
/// directory at the time the record was written. `target_version` is the
/// version the most recent update attempt was trying to reach. For `Converged`
/// records the two are equal; for `Failed` records `installed_version` is the
/// pre-attempt version; for `PendingRestart` records `installed_version` is
/// already the new release even though the runtime process may not yet be.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateStatusRecord {
    pub state: UpdateConvergenceState,
    pub installed_version: String,
    pub target_version: String,
    pub channel: String,
    pub app_id: String,
    /// True when a supervisor was configured for this release and its pid file
    /// was observed after the post-update restart, or when no supervisor was
    /// configured (in which case there is nothing to restart and the field
    /// carries no signal).
    pub supervisor_restart_confirmed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempted_at_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Last time the active updater wrote progress for this transaction.
    /// Observers use this as a durable heartbeat for remote setup watchdogs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_progress_at_utc: Option<String>,
    /// Coarse-grained label for the substep currently in progress (for
    /// example "downloading artifacts" or "swapping app directory"). Only
    /// meaningful for `InProgress` records; observers can use it to tell
    /// "stuck in finalize" apart from "stuck in download".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_phase: Option<String>,
    /// Most recent phase that completed before the current or terminal state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_completed_phase: Option<String>,
    /// Phase active when a terminal failure was recorded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_phase: Option<String>,
    /// Whether retrying the same setup/update command is expected to be safe.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_safe: Option<bool>,
}

impl UpdateStatusRecord {
    #[must_use]
    pub fn idle(app_id: &str, installed_version: &str, channel: &str) -> Self {
        Self {
            state: UpdateConvergenceState::Idle,
            installed_version: installed_version.to_string(),
            target_version: installed_version.to_string(),
            channel: channel.to_string(),
            app_id: app_id.to_string(),
            supervisor_restart_confirmed: false,
            attempted_at_utc: None,
            completed_at_utc: None,
            reason: None,
            last_progress_at_utc: None,
            current_phase: None,
            last_completed_phase: None,
            failure_phase: None,
            retry_safe: None,
        }
    }

    #[must_use]
    pub fn in_progress(
        app_id: &str,
        installed_version: &str,
        target_version: &str,
        channel: &str,
        attempted_at_utc: String,
    ) -> Self {
        Self {
            state: UpdateConvergenceState::InProgress,
            installed_version: installed_version.to_string(),
            target_version: target_version.to_string(),
            channel: channel.to_string(),
            app_id: app_id.to_string(),
            supervisor_restart_confirmed: false,
            attempted_at_utc: Some(attempted_at_utc),
            completed_at_utc: None,
            reason: None,
            last_progress_at_utc: None,
            current_phase: None,
            last_completed_phase: None,
            failure_phase: None,
            retry_safe: None,
        }
    }

    /// Set the current substep label on an [`UpdateConvergenceState::InProgress`]
    /// record. No-op for any other state.
    #[must_use]
    pub fn with_current_phase(self, phase: impl Into<String>) -> Self {
        self.with_current_phase_at(phase, now_utc_rfc3339())
    }

    #[must_use]
    pub fn with_current_phase_at(mut self, phase: impl Into<String>, progress_at_utc: String) -> Self {
        let label = phase.into();
        if matches!(self.state, UpdateConvergenceState::InProgress) {
            self.current_phase = Some(label);
            self.last_progress_at_utc = Some(progress_at_utc);
        }
        self
    }

    #[must_use]
    pub fn with_completed_phase(self, phase: impl Into<String>) -> Self {
        self.with_completed_phase_at(phase, now_utc_rfc3339())
    }

    #[must_use]
    pub fn with_completed_phase_at(mut self, phase: impl Into<String>, progress_at_utc: String) -> Self {
        let label = phase.into();
        if matches!(self.state, UpdateConvergenceState::InProgress) {
            self.last_completed_phase = Some(label);
            self.current_phase = None;
            self.last_progress_at_utc = Some(progress_at_utc);
        }
        self
    }

    #[must_use]
    pub fn converged(
        app_id: &str,
        version: &str,
        channel: &str,
        attempted_at_utc: Option<String>,
        completed_at_utc: String,
        supervisor_restart_confirmed: bool,
    ) -> Self {
        Self {
            state: UpdateConvergenceState::Converged,
            installed_version: version.to_string(),
            target_version: version.to_string(),
            channel: channel.to_string(),
            app_id: app_id.to_string(),
            supervisor_restart_confirmed,
            attempted_at_utc,
            completed_at_utc: Some(completed_at_utc),
            reason: None,
            last_progress_at_utc: None,
            current_phase: None,
            last_completed_phase: None,
            failure_phase: None,
            retry_safe: None,
        }
    }

    #[must_use]
    pub fn pending_restart(
        app_id: &str,
        installed_version: &str,
        target_version: &str,
        channel: &str,
        attempted_at_utc: String,
        completed_at_utc: String,
        reason: &str,
    ) -> Self {
        Self {
            state: UpdateConvergenceState::PendingRestart,
            installed_version: installed_version.to_string(),
            target_version: target_version.to_string(),
            channel: channel.to_string(),
            app_id: app_id.to_string(),
            supervisor_restart_confirmed: false,
            attempted_at_utc: Some(attempted_at_utc),
            completed_at_utc: Some(completed_at_utc),
            reason: Some(reason.to_string()),
            last_progress_at_utc: None,
            current_phase: None,
            last_completed_phase: None,
            failure_phase: Some("supervisor restart requested".to_string()),
            retry_safe: Some(true),
        }
    }

    #[must_use]
    pub fn failed(
        app_id: &str,
        installed_version: &str,
        target_version: &str,
        channel: &str,
        attempted_at_utc: String,
        reason: &str,
    ) -> Self {
        Self {
            state: UpdateConvergenceState::Failed,
            installed_version: installed_version.to_string(),
            target_version: target_version.to_string(),
            channel: channel.to_string(),
            app_id: app_id.to_string(),
            supervisor_restart_confirmed: false,
            attempted_at_utc: Some(attempted_at_utc),
            completed_at_utc: None,
            reason: Some(reason.to_string()),
            last_progress_at_utc: None,
            current_phase: None,
            last_completed_phase: None,
            failure_phase: None,
            retry_safe: Some(true),
        }
    }

    #[must_use]
    pub fn failed_with_context(
        app_id: &str,
        installed_version: &str,
        target_version: &str,
        channel: &str,
        attempted_at_utc: String,
        reason: &str,
        context: FailureContext,
    ) -> Self {
        Self {
            state: UpdateConvergenceState::Failed,
            installed_version: installed_version.to_string(),
            target_version: target_version.to_string(),
            channel: channel.to_string(),
            app_id: app_id.to_string(),
            supervisor_restart_confirmed: false,
            attempted_at_utc: Some(attempted_at_utc),
            completed_at_utc: Some(now_utc_rfc3339()),
            reason: Some(reason.to_string()),
            last_progress_at_utc: context.last_progress_at_utc,
            current_phase: None,
            last_completed_phase: context.last_completed_phase,
            failure_phase: context.failure_phase,
            retry_safe: Some(context.retry_safe),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailureContext {
    pub failure_phase: Option<String>,
    pub last_completed_phase: Option<String>,
    pub last_progress_at_utc: Option<String>,
    pub retry_safe: bool,
}

impl FailureContext {
    #[must_use]
    pub fn from_record(record: Option<&UpdateStatusRecord>, retry_safe: bool) -> Self {
        let Some(record) = record else {
            return Self {
                retry_safe,
                ..Self::default()
            };
        };
        Self {
            failure_phase: record.current_phase.clone().or_else(|| record.failure_phase.clone()),
            last_completed_phase: record.last_completed_phase.clone(),
            last_progress_at_utc: record.last_progress_at_utc.clone(),
            retry_safe,
        }
    }
}

#[must_use]
pub fn update_status_path(install_dir: &Path) -> PathBuf {
    install_dir.join(UPDATE_STATUS_FILE_NAME)
}

/// Read the persisted update status record from `install_dir`, if any.
///
/// Returns `Ok(None)` when no record has been written yet (clean install that
/// happened before this signal existed, or never converged through an update
/// flow that writes the file).
pub fn read_update_status(install_dir: &Path) -> Result<Option<UpdateStatusRecord>> {
    let path = update_status_path(install_dir);
    if !path.is_file() {
        return Ok(None);
    }
    let raw = std::fs::read(&path)?;
    serde_json::from_slice(&raw)
        .map(Some)
        .map_err(|e| SurgeError::Config(format!("Failed to decode {}: {e}", path.display())))
}

pub fn write_update_status(install_dir: &Path, record: &UpdateStatusRecord) -> Result<()> {
    let path = update_status_path(install_dir);
    let json = serde_json::to_vec_pretty(record)
        .map_err(|e| SurgeError::Config(format!("Failed to encode update status: {e}")))?;
    write_file_atomic(&path, &json)?;
    Ok(())
}

#[must_use]
pub fn now_utc_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Poll for the supervisor pid file to appear after a restart attempt.
///
/// Returns `true` if the pid file is present and parses as a non-zero PID
/// within the timeout window, `false` otherwise. An empty `supervisor_id`
/// means there is no supervisor to confirm; the caller is responsible for
/// deciding what that implies for the convergence state.
#[must_use]
pub fn confirm_supervisor_restart(install_dir: &Path, supervisor_id: &str, timeout: Duration) -> bool {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return false;
    }

    let pid_file = supervisor_pid_file(install_dir, supervisor_id);
    let deadline = Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);
    loop {
        if let Ok(contents) = std::fs::read_to_string(&pid_file)
            && contents.trim().parse::<u32>().is_ok_and(|pid| pid > 0)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(poll_interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_converged_record() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::converged(
            "demo-app",
            "9999.0.0",
            "stable",
            Some("2026-05-11T14:00:00Z".to_string()),
            "2026-05-11T14:05:00Z".to_string(),
            true,
        );

        write_update_status(dir.path(), &record).unwrap();
        let loaded = read_update_status(dir.path()).unwrap().unwrap();

        assert_eq!(loaded, record);
        assert_eq!(loaded.state, UpdateConvergenceState::Converged);
        assert_eq!(loaded.installed_version, "9999.0.0");
        assert!(loaded.supervisor_restart_confirmed);
    }

    #[test]
    fn round_trip_pending_restart_record() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::pending_restart(
            "demo-app",
            "9999.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "2026-05-11T14:05:00Z".to_string(),
            "supervisor pid file never appeared after restart",
        );

        write_update_status(dir.path(), &record).unwrap();
        let loaded = read_update_status(dir.path()).unwrap().unwrap();

        assert_eq!(loaded.state, UpdateConvergenceState::PendingRestart);
        assert!(!loaded.supervisor_restart_confirmed);
        assert!(loaded.reason.as_deref().unwrap().contains("supervisor pid"));
    }

    #[test]
    fn round_trip_failed_record_preserves_pre_attempt_version() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::failed(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "storage backend returned 503",
        );

        write_update_status(dir.path(), &record).unwrap();
        let loaded = read_update_status(dir.path()).unwrap().unwrap();

        assert_eq!(loaded.state, UpdateConvergenceState::Failed);
        assert_eq!(loaded.installed_version, "9998.0.0");
        assert_eq!(loaded.target_version, "9999.0.0");
        assert!(loaded.completed_at_utc.is_none());
    }

    #[test]
    fn read_returns_none_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_update_status(dir.path()).unwrap().is_none());
    }

    #[test]
    fn with_current_phase_sets_only_for_in_progress_records() {
        let in_progress = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        )
        .with_current_phase("swapping app directory");
        assert_eq!(in_progress.current_phase.as_deref(), Some("swapping app directory"));

        let converged = UpdateStatusRecord::converged(
            "demo-app",
            "9999.0.0",
            "stable",
            Some("2026-05-11T14:00:00Z".to_string()),
            "2026-05-11T14:05:00Z".to_string(),
            true,
        )
        .with_current_phase("ignored for non-in-progress records");
        assert!(converged.current_phase.is_none());
    }

    #[test]
    fn round_trip_in_progress_record_with_current_phase() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        )
        .with_current_phase("stopping supervisor");

        write_update_status(dir.path(), &record).unwrap();
        let loaded = read_update_status(dir.path()).unwrap().unwrap();

        assert_eq!(loaded.state, UpdateConvergenceState::InProgress);
        assert_eq!(loaded.current_phase.as_deref(), Some("stopping supervisor"));
    }

    #[test]
    fn in_progress_record_serializes_current_phase_only_when_set() {
        let dir = tempfile::tempdir().unwrap();
        let without_phase = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        );
        write_update_status(dir.path(), &without_phase).unwrap();
        let raw = std::fs::read_to_string(update_status_path(dir.path())).unwrap();
        assert!(
            !raw.contains("current_phase"),
            "expected current_phase to be skipped when None, got: {raw}"
        );

        let with_phase = without_phase.with_current_phase("swapping app directory");
        write_update_status(dir.path(), &with_phase).unwrap();
        let raw = std::fs::read_to_string(update_status_path(dir.path())).unwrap();
        assert!(raw.contains("\"current_phase\""), "expected current_phase in: {raw}");
        assert!(raw.contains("swapping app directory"), "expected label in: {raw}");
    }

    #[test]
    fn write_overwrites_existing_record() {
        let dir = tempfile::tempdir().unwrap();
        let in_progress = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        );
        write_update_status(dir.path(), &in_progress).unwrap();

        let converged = UpdateStatusRecord::converged(
            "demo-app",
            "9999.0.0",
            "stable",
            Some("2026-05-11T14:00:00Z".to_string()),
            "2026-05-11T14:05:00Z".to_string(),
            true,
        );
        write_update_status(dir.path(), &converged).unwrap();

        let loaded = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.state, UpdateConvergenceState::Converged);
        assert_eq!(loaded.installed_version, "9999.0.0");
    }

    #[test]
    fn confirm_supervisor_restart_detects_fresh_pid_file() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = supervisor_pid_file(dir.path(), "demo-supervisor");
        std::fs::write(&pid_file, "12345").unwrap();

        let confirmed = confirm_supervisor_restart(dir.path(), "demo-supervisor", Duration::from_millis(200));
        assert!(confirmed);
    }

    #[test]
    fn confirm_supervisor_restart_times_out_when_pid_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let confirmed = confirm_supervisor_restart(dir.path(), "demo-supervisor", Duration::from_millis(200));
        assert!(!confirmed);
    }

    #[test]
    fn confirm_supervisor_restart_returns_false_when_supervisor_id_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!confirm_supervisor_restart(dir.path(), "", Duration::from_millis(200)));
        assert!(!confirm_supervisor_restart(
            dir.path(),
            "   ",
            Duration::from_millis(200)
        ));
    }

    #[test]
    fn convergence_state_as_str_round_trips_through_serde() {
        for state in [
            UpdateConvergenceState::Idle,
            UpdateConvergenceState::InProgress,
            UpdateConvergenceState::Converged,
            UpdateConvergenceState::PendingRestart,
            UpdateConvergenceState::Failed,
        ] {
            let encoded = serde_json::to_string(&state).unwrap();
            let decoded: UpdateConvergenceState = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, state);
            assert_eq!(state.to_string(), state.as_str());
        }
    }
}
