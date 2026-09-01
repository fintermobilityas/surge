//! Explicit update convergence state recorded per install root.
//!
//! After a channel promotion, operators need a reliable signal that distinguishes:
//! - **`Idle`** — the install completed but no update has been attempted since.
//! - **`InProgress`** — an update is currently being applied.
//! - **`Converged`** — the latest update applied to disk and the supervisor handoff
//!   (or the install-time auto-start) proved a replacement runtime is active.
//! - **`PendingRestart`** — the latest update applied to disk but the supervisor
//!   restart could not be confirmed within the post-update window. The runtime
//!   process may still be running an older binary even though `installed_version`
//!   already reflects the new release.
//! - **`Failed`** — the most recent attempt failed. The `installed_version`
//!   field reflects the generation retained in the active app directory.
//!
//! This record is persisted at `{install_dir}/.surge-update-status.json` so it
//! survives the active app directory swap that happens on every successful update.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::supervisor::state::supervisor_pid_file;

mod handoff;
mod worker;

pub use handoff::{
    RESTART_HANDOFF_FAILED_PHASE, RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE,
    RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE, mark_restart_handoff_converged, mark_restart_handoff_pending,
};
pub use worker::{UpdateWorkerGuard, fail_abandoned_in_progress_update};

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
/// generation retained after recovery; for `PendingRestart` records
/// `installed_version` is already the new release even though the runtime
/// process may not yet be.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateStatusRecord {
    pub state: UpdateConvergenceState,
    pub installed_version: String,
    pub target_version: String,
    pub channel: String,
    pub app_id: String,
    /// True when a supervisor was configured for this release and the post-update
    /// handoff proved a target-version child is active. When no supervisor was
    /// configured this is false and carries no signal; read `state` for
    /// convergence.
    pub supervisor_restart_confirmed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempted_at_utc: Option<String>,
    /// When the update work (download, verify, apply, restart initiation)
    /// finished. Deliberately not re-stamped when convergence is proven later:
    /// `attempted_at_utc..completed_at_utc` must stay a truthful measure of the
    /// update work even when the restart handoff takes hours to settle.
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
    /// Earliest time a new attempt for this target may start again after a
    /// retry-safe failure. Observers (for example in-app update loops) defer
    /// new attempts until this instant, turning consecutive failures into a
    /// bounded backoff instead of a tight retry loop. Only set on `Failed`
    /// records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_retry_at_utc: Option<String>,
    /// One-based count of consecutive retry-safe failures for this target.
    /// Together with [`Self::next_retry_at_utc`] it makes the backoff ladder
    /// exact instead of re-deriving it from wall-clock gaps. Only set on
    /// `Failed` records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
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
            next_retry_at_utc: None,
            retry_count: None,
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
            next_retry_at_utc: None,
            retry_count: None,
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
            next_retry_at_utc: None,
            retry_count: None,
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
        Self::pending_restart_with_failure_phase(
            app_id,
            installed_version,
            target_version,
            channel,
            attempted_at_utc,
            completed_at_utc,
            reason,
            "supervisor restart requested",
        )
    }

    #[must_use]
    pub fn pending_restart_with_failure_phase(
        app_id: &str,
        installed_version: &str,
        target_version: &str,
        channel: &str,
        attempted_at_utc: String,
        completed_at_utc: String,
        reason: &str,
        failure_phase: &str,
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
            failure_phase: Some(failure_phase.to_string()),
            retry_safe: Some(true),
            next_retry_at_utc: None,
            retry_count: None,
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
            next_retry_at_utc: None,
            retry_count: None,
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
        Self::failed_with_context_at(
            app_id,
            installed_version,
            target_version,
            channel,
            attempted_at_utc,
            now_utc_rfc3339(),
            reason,
            context,
        )
    }

    /// Like [`Self::failed_with_context`], but with an explicit
    /// `completed_at_utc` for failures classified after the fact (for example
    /// an abandoned attempt discovered on the next update check), where "now"
    /// would fold the idle gap into the recorded attempt duration.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn failed_with_context_at(
        app_id: &str,
        installed_version: &str,
        target_version: &str,
        channel: &str,
        attempted_at_utc: String,
        completed_at_utc: String,
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
            completed_at_utc: Some(completed_at_utc),
            reason: Some(reason.to_string()),
            last_progress_at_utc: context.last_progress_at_utc,
            current_phase: None,
            last_completed_phase: context.last_completed_phase,
            failure_phase: context.failure_phase,
            retry_safe: Some(context.retry_safe),
            next_retry_at_utc: None,
            retry_count: None,
        }
    }

    /// Stamp the retry-backoff schedule onto a `Failed` record. No-op for any
    /// other state so converged/pending-restart records never advertise a
    /// retry time.
    #[must_use]
    pub fn with_retry_schedule_at(self, schedule: &RetrySchedule, next_retry_at_utc: String) -> Self {
        if matches!(self.state, UpdateConvergenceState::Failed) {
            Self {
                next_retry_at_utc: Some(next_retry_at_utc),
                retry_count: Some(schedule.retry_count),
                ..self
            }
        } else {
            self
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

/// Base delay before the first retry of a retry-safe update failure.
pub const RETRY_BACKOFF_BASE: Duration = Duration::from_mins(5);
/// Maximum delay between consecutive retries of a failed update attempt.
pub const RETRY_BACKOFF_CAP: Duration = Duration::from_hours(6);

/// A retry-backoff schedule for a new retry-safe failure, derived from the
/// record it replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetrySchedule {
    /// Delay to wait before the next attempt for this target.
    pub backoff: Duration,
    /// One-based count of consecutive retry-safe failures for the target.
    pub retry_count: u32,
}

impl RetrySchedule {
    #[must_use]
    pub fn base() -> Self {
        Self {
            backoff: RETRY_BACKOFF_BASE,
            retry_count: 1,
        }
    }
}

/// Compute the retry schedule for a new failure record.
///
/// The ladder starts at [`RETRY_BACKOFF_BASE`] and doubles per consecutive
/// retry-safe failure for the same target, capped at [`RETRY_BACKOFF_CAP`].
/// Any missing, non-failed, non-retry-safe, or differently-targeted previous
/// record resets the ladder to the base schedule.
#[must_use]
pub fn retry_schedule(previous: Option<&UpdateStatusRecord>, target_version: &str) -> RetrySchedule {
    let Some(previous) = previous else {
        return RetrySchedule::base();
    };
    if !matches!(previous.state, UpdateConvergenceState::Failed)
        || previous.retry_safe != Some(true)
        || previous.target_version != target_version
        || previous.next_retry_at_utc.is_none()
    {
        return RetrySchedule::base();
    }

    let retry_count = previous.retry_count.unwrap_or(1).saturating_add(1);
    let exponent = retry_count.saturating_sub(1).min(63);
    let backoff_secs = RETRY_BACKOFF_BASE
        .as_secs()
        .saturating_mul(1u64 << exponent)
        .min(RETRY_BACKOFF_CAP.as_secs());
    RetrySchedule {
        backoff: Duration::from_secs(backoff_secs),
        retry_count,
    }
}

/// RFC 3339 UTC timestamp for the next retry given `now` and a schedule.
#[must_use]
pub fn next_retry_timestamp(now: chrono::DateTime<chrono::Utc>, schedule: &RetrySchedule) -> String {
    let secs = i64::try_from(schedule.backoff.as_secs()).unwrap_or(i64::MAX);
    (now + chrono::Duration::seconds(secs)).to_rfc3339()
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
    fn pending_restart_can_classify_restart_handoff_failure() {
        let record = UpdateStatusRecord::pending_restart_with_failure_phase(
            "demo-app",
            "9999.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "2026-05-11T14:05:00Z".to_string(),
            "supervisor pid file never appeared after restart",
            RESTART_HANDOFF_FAILED_PHASE,
        );

        assert_eq!(record.state, UpdateConvergenceState::PendingRestart);
        assert_eq!(record.failure_phase.as_deref(), Some(RESTART_HANDOFF_FAILED_PHASE));
        assert_eq!(record.retry_safe, Some(true));
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

    #[test]
    fn retry_schedule_starts_at_base_without_a_previous_failure() {
        assert_eq!(retry_schedule(None, "9999.0.0"), RetrySchedule::base());

        let in_progress = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        );
        assert_eq!(retry_schedule(Some(&in_progress), "9999.0.0"), RetrySchedule::base());
    }

    #[test]
    fn retry_schedule_doubles_per_consecutive_failure_until_cap() {
        let base = RetrySchedule::base();
        let mut previous = failed_record_with_schedule("9999.0.0", base.retry_count, base.backoff);

        for expected_count in 2u32..=7 {
            let expected_backoff = Duration::from_secs(
                RETRY_BACKOFF_BASE
                    .as_secs()
                    .saturating_mul(1u64 << (expected_count - 1))
                    .min(RETRY_BACKOFF_CAP.as_secs()),
            );
            let schedule = retry_schedule(Some(&previous), "9999.0.0");
            assert_eq!(schedule.retry_count, expected_count);
            assert_eq!(schedule.backoff, expected_backoff);
            previous = failed_record_with_schedule("9999.0.0", expected_count, expected_backoff);
        }

        let capped = retry_schedule(Some(&previous), "9999.0.0");
        assert_eq!(capped.backoff, RETRY_BACKOFF_CAP);
        assert_eq!(
            retry_schedule(
                Some(&failed_record_with_schedule("9999.0.0", 9, RETRY_BACKOFF_CAP)),
                "9999.0.0"
            )
            .backoff,
            RETRY_BACKOFF_CAP
        );
    }

    #[test]
    fn retry_schedule_resets_for_other_target_or_non_retry_safe_failure() {
        let previous = failed_record_with_schedule("9999.0.0", 3, RETRY_BACKOFF_CAP);

        assert_eq!(retry_schedule(Some(&previous), "9998.0.0"), RetrySchedule::base());

        let not_retry_safe = UpdateStatusRecord::failed(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "not safe to retry",
        );
        assert_eq!(retry_schedule(Some(&not_retry_safe), "9999.0.0"), RetrySchedule::base());

        let no_schedule = UpdateStatusRecord::failed(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "failure without schedule",
        );
        assert_eq!(retry_schedule(Some(&no_schedule), "9999.0.0"), RetrySchedule::base());
    }

    #[test]
    fn with_retry_schedule_at_only_stamps_failed_records_and_omits_unset_fields() {
        let failed = UpdateStatusRecord::failed(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "storage backend returned 503",
        );
        let raw = serde_json::to_string(&failed).unwrap();
        assert!(
            !raw.contains("next_retry_at_utc"),
            "unset fields must be omitted: {raw}"
        );
        assert!(!raw.contains("retry_count"), "unset fields must be omitted: {raw}");

        let stamped = failed.with_retry_schedule_at(&RetrySchedule::base(), "2026-05-11T14:05:00Z".to_string());
        assert_eq!(stamped.next_retry_at_utc.as_deref(), Some("2026-05-11T14:05:00Z"));
        assert_eq!(stamped.retry_count, Some(1));
        let raw = serde_json::to_string(&stamped).unwrap();
        assert!(raw.contains("\"next_retry_at_utc\":\"2026-05-11T14:05:00Z\""));
        assert!(raw.contains("\"retry_count\":1"));

        let in_progress = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        );
        let untouched = in_progress.with_retry_schedule_at(&RetrySchedule::base(), "2026-05-11T14:05:00Z".to_string());
        assert!(untouched.next_retry_at_utc.is_none());
        assert!(untouched.retry_count.is_none());
    }

    #[test]
    fn legacy_status_json_without_retry_fields_still_deserializes() {
        let json = r#"{
            "state": "failed",
            "installed_version": "9998.0.0",
            "target_version": "9999.0.0",
            "channel": "stable",
            "app_id": "demo-app",
            "supervisor_restart_confirmed": false,
            "attempted_at_utc": "2026-05-11T14:00:00Z",
            "reason": "storage backend returned 503",
            "retry_safe": true
        }"#;
        let record: UpdateStatusRecord = serde_json::from_str(json).unwrap();
        assert_eq!(record.state, UpdateConvergenceState::Failed);
        assert_eq!(record.retry_safe, Some(true));
        assert!(record.next_retry_at_utc.is_none());
        assert!(record.retry_count.is_none());
        assert_eq!(retry_schedule(Some(&record), "9999.0.0"), RetrySchedule::base());
    }

    fn failed_record_with_schedule(target: &str, retry_count: u32, backoff: Duration) -> UpdateStatusRecord {
        UpdateStatusRecord::failed(
            "demo-app",
            "9998.0.0",
            target,
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "test failure",
        )
        .with_retry_schedule_at(
            &RetrySchedule { backoff, retry_count },
            "2026-05-11T14:05:00Z".to_string(),
        )
    }
}
