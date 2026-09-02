//! Update-worker ownership marker and abandoned-attempt classification.
//!
//! A live update attempt records its worker (pid, process start time, app,
//! target version) next to the status file. When a later attempt finds an
//! `InProgress` record it classifies that attempt against the worker marker:
//! - worker identity is this process → the attempt is (re)started by us;
//!   proceed.
//! - worker pid is another *live* process with recent progress → a
//!   concurrent updater is active; never reclassify it.
//! - worker pid is another *live* process whose progress has been silent
//!   past the stalled-work deadline → presumed hung; fail it so the retry
//!   machinery can requeue the work (a hung process will never converge on
//!   its own).
//! - worker pid is *dead* → the attempt can never make progress again; fail
//!   it immediately and schedule a backoff, without waiting out the
//!   progress-staleness window.
//! - the liveness probe is inconclusive (`Unknown`, e.g. the probe utility
//!   failed) or there is no worker marker → fall back to the
//!   progress-staleness window; never fail an attempt on an inconclusive
//!   probe.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::platform::process::{PidLiveness, current_pid, probe_process_identity, process_start_time};

use super::{
    FailureContext, UpdateConvergenceState, UpdateStatusRecord, next_retry_timestamp, now_utc_rfc3339,
    read_update_status, retry_schedule, write_update_status,
};

const UPDATE_WORKER_FILE_NAME: &str = ".surge-update-worker.json";
const UPDATE_WORKER_LOCK_FILE_NAME: &str = ".surge-update-worker.lock";

/// A live worker whose progress has been silent this long is presumed hung
/// (deadlocked or stalled on a frozen connection): active update work emits
/// progress heartbeats, so silence this long means no forward progress is
/// possible. The deadline is deliberately far above the progress-staleness
/// window used without a marker, because a live substep may be quiet for a
/// while without being hung.
const STALLED_LIVE_WORKER_PROGRESS_DEADLINE: Duration = Duration::from_mins(30);
const LEGACY_WORKER_MARKER_GRACE: Duration = Duration::from_mins(5);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct UpdateWorkerRecord {
    pid: u32,
    app_id: String,
    target_version: String,
    started_at_utc: String,
    #[serde(default)]
    process_start_time: Option<u64>,
    #[serde(default)]
    owner_id: String,
}

pub struct UpdateWorkerGuard {
    path: PathBuf,
    lock_path: PathBuf,
    owner_id: String,
}

impl UpdateWorkerGuard {
    pub fn record(install_dir: &Path, app_id: &str, target_version: &str) -> Result<Self> {
        let lock_path = update_worker_lock_path(install_dir);
        let _lock = acquire_worker_lock(&lock_path)?;
        if let Some(existing) = read_update_worker(install_dir)?
            && worker_blocks_new_attempt(install_dir, &existing)?
        {
            return Err(SurgeError::Update(format!(
                "Another update worker (pid {}) already owns this installation",
                existing.pid
            )));
        }

        write_owned_worker(install_dir, &lock_path, app_id, target_version)
    }

    pub(crate) fn take_over(
        install_dir: &Path,
        app_id: &str,
        target_version: &str,
        expected_pid: u32,
        expected_start_time: u64,
    ) -> Result<Self> {
        let lock_path = update_worker_lock_path(install_dir);
        let _lock = acquire_worker_lock(&lock_path)?;
        let existing = read_update_worker(install_dir)?.ok_or_else(|| {
            SurgeError::Update("The external finalizer could not find the updating worker to take over".to_string())
        })?;
        if existing.pid != expected_pid
            || existing.process_start_time != Some(expected_start_time)
            || existing.app_id != app_id
            || existing.target_version != target_version
            || !matches!(probe_worker_liveness(&existing), PidLiveness::Alive)
        {
            return Err(SurgeError::Update(
                "Update worker ownership changed before the external finalizer took over".to_string(),
            ));
        }

        write_owned_worker(install_dir, &lock_path, app_id, target_version)
    }
}

impl Drop for UpdateWorkerGuard {
    fn drop(&mut self) {
        let Ok(_lock) = acquire_worker_lock(&self.lock_path) else {
            return;
        };
        let Ok(raw) = std::fs::read(&self.path) else {
            return;
        };
        let Ok(record) = serde_json::from_slice::<UpdateWorkerRecord>(&raw) else {
            return;
        };
        if record.owner_id == self.owner_id {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn write_owned_worker(
    install_dir: &Path,
    lock_path: &Path,
    app_id: &str,
    target_version: &str,
) -> Result<UpdateWorkerGuard> {
    let pid = current_pid();
    let process_start_time = process_start_time(pid).ok_or_else(|| {
        SurgeError::Platform(format!(
            "Could not read start-time identity for update worker process {pid}"
        ))
    })?;
    let record = UpdateWorkerRecord {
        pid,
        app_id: app_id.to_string(),
        target_version: target_version.to_string(),
        started_at_utc: now_utc_rfc3339(),
        process_start_time: Some(process_start_time),
        owner_id: Uuid::new_v4().to_string(),
    };
    let path = update_worker_path(install_dir);
    let json = serde_json::to_vec_pretty(&record)
        .map_err(|e| SurgeError::Config(format!("Failed to encode update worker marker: {e}")))?;
    write_file_atomic(&path, &json)?;
    Ok(UpdateWorkerGuard {
        path,
        lock_path: lock_path.to_path_buf(),
        owner_id: record.owner_id,
    })
}

fn acquire_worker_lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    FileExt::lock_exclusive(&file)?;
    Ok(file)
}

fn probe_worker_liveness(worker: &UpdateWorkerRecord) -> PidLiveness {
    worker.process_start_time.map_or(PidLiveness::Unknown, |start_time| {
        probe_process_identity(worker.pid, start_time)
    })
}

fn worker_blocks_new_attempt(install_dir: &Path, worker: &UpdateWorkerRecord) -> Result<bool> {
    if worker.process_start_time.is_some() {
        return Ok(!matches!(probe_worker_liveness(worker), PidLiveness::Dead));
    }

    if let Some(status) = read_update_status(install_dir)?
        && status.app_id == worker.app_id
        && status.target_version == worker.target_version
    {
        return Ok(status.state == UpdateConvergenceState::InProgress);
    }

    let started_at = chrono::DateTime::parse_from_rfc3339(&worker.started_at_utc)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
    Ok(started_at.is_some_and(|started_at| {
        let age = chrono::Utc::now().signed_duration_since(started_at);
        chrono::Duration::from_std(LEGACY_WORKER_MARKER_GRACE).is_ok_and(|grace| age < grace)
    }))
}

pub fn fail_abandoned_in_progress_update(
    install_dir: &Path,
    app_id: &str,
    target_version: &str,
    channel: &str,
    stale_after: Duration,
) -> Result<Option<UpdateStatusRecord>> {
    fail_abandoned_in_progress_update_at(
        install_dir,
        app_id,
        target_version,
        channel,
        stale_after,
        chrono::Utc::now(),
    )
}

fn fail_abandoned_in_progress_update_at(
    install_dir: &Path,
    app_id: &str,
    target_version: &str,
    channel: &str,
    stale_after: Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<UpdateStatusRecord>> {
    let Some(record) = read_update_status(install_dir)? else {
        return Ok(None);
    };
    if record.state != UpdateConvergenceState::InProgress
        || record.app_id != app_id
        || record.target_version != target_version
        || record.channel != channel
    {
        return Ok(None);
    }

    // A worker marker that does not describe this record (different app or
    // target) cannot classify it; treat it as absent.
    let worker = read_update_worker(install_dir)?
        .filter(|worker| worker.app_id == record.app_id && worker.target_version == record.target_version);

    let Some(worker) = worker else {
        // No worker marker: fall back to the progress-staleness window.
        let Some(age) = stale_progress_age(&record, now) else {
            return Ok(None);
        };
        if age < stale_after {
            return Ok(None);
        }
        let failed = abandoned_failure(
            &record,
            now,
            &format!(
                "previous update attempt abandoned after {}s without progress at phase '{}'",
                age.as_secs(),
                abandoned_phase(&record)
            ),
        );
        write_update_status(install_dir, &failed)?;
        return Ok(Some(failed));
    };

    let worker_liveness = probe_worker_liveness(&worker);

    if worker.process_start_time.is_some()
        && worker.pid == std::process::id()
        && matches!(worker_liveness, PidLiveness::Alive)
    {
        // The marker belongs to this process: a previous in-process attempt
        // stalled. The caller proceeds with its own attempt, which takes over
        // the record; there is nothing to reclassify.
        return Ok(None);
    }

    match worker_liveness {
        PidLiveness::Dead => {
            // Dead foreign worker: the attempt can never make progress again,
            // so fail it immediately instead of waiting out the staleness
            // window.
            let age_note = stale_progress_age(&record, now)
                .map(|age| format!(" (last progress {}s ago)", age.as_secs()))
                .unwrap_or_default();
            let failed = abandoned_failure(
                &record,
                now,
                &format!(
                    "previous update worker (pid {}) exited without completing{age_note} at phase '{}'",
                    worker.pid,
                    abandoned_phase(&record)
                ),
            );
            write_update_status(install_dir, &failed)?;
            Ok(Some(failed))
        }
        PidLiveness::Unknown => {
            // The probe could not run or exited abnormally; an inconclusive
            // probe must never fail a live attempt. Use the same
            // progress-staleness window as the missing-marker case.
            let Some(age) = stale_progress_age(&record, now) else {
                return Ok(None);
            };
            if age < stale_after {
                return Ok(None);
            }
            let failed = abandoned_failure(
                &record,
                now,
                &format!(
                    "previous update worker (pid {}) could not be probed and has made no progress for {}s at phase '{}'",
                    worker.pid,
                    age.as_secs(),
                    abandoned_phase(&record)
                ),
            );
            write_update_status(install_dir, &failed)?;
            Ok(Some(failed))
        }
        PidLiveness::Alive => {
            // A live concurrent worker inside an active substep is never
            // reclassified. A live worker whose progress has been silent
            // past the stalled-work deadline is presumed hung: failing it
            // lets the retry/backoff machinery requeue the work instead of
            // leaving the status in_progress forever.
            let Some(age) = stale_progress_age(&record, now) else {
                return Ok(None);
            };
            if age < STALLED_LIVE_WORKER_PROGRESS_DEADLINE {
                return Ok(None);
            }
            let failed = abandoned_failure(
                &record,
                now,
                &format!(
                    "previous update worker (pid {}) is alive but has made no progress for {}s at phase '{}'; presumed stalled",
                    worker.pid,
                    age.as_secs(),
                    abandoned_phase(&record)
                ),
            );
            write_update_status(install_dir, &failed)?;
            Ok(Some(failed))
        }
    }
}

fn abandoned_phase(record: &UpdateStatusRecord) -> String {
    record
        .current_phase
        .clone()
        .or_else(|| record.failure_phase.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

fn abandoned_failure(
    record: &UpdateStatusRecord,
    now: chrono::DateTime<chrono::Utc>,
    reason: &str,
) -> UpdateStatusRecord {
    let attempted_at_utc = record
        .attempted_at_utc
        .clone()
        .or_else(|| record.last_progress_at_utc.clone())
        .unwrap_or_else(now_utc_rfc3339);
    let last_activity_at_utc = record
        .last_progress_at_utc
        .clone()
        .unwrap_or_else(|| attempted_at_utc.clone());
    let schedule = retry_schedule(Some(record), &record.target_version);
    UpdateStatusRecord::failed_with_context_at(
        &record.app_id,
        &record.installed_version,
        &record.target_version,
        &record.channel,
        attempted_at_utc,
        last_activity_at_utc,
        reason,
        FailureContext::from_record(Some(record), true),
    )
    .with_retry_schedule_at(&schedule, next_retry_timestamp(now, &schedule))
}

fn stale_progress_age(record: &UpdateStatusRecord, now: chrono::DateTime<chrono::Utc>) -> Option<Duration> {
    let timestamp = record
        .last_progress_at_utc
        .as_deref()
        .or(record.attempted_at_utc.as_deref())?;
    let parsed = chrono::DateTime::parse_from_rfc3339(timestamp).ok()?;
    now.signed_duration_since(parsed.with_timezone(&chrono::Utc))
        .to_std()
        .ok()
}

fn read_update_worker(install_dir: &Path) -> Result<Option<UpdateWorkerRecord>> {
    let path = update_worker_path(install_dir);
    if !path.is_file() {
        return Ok(None);
    }
    let raw = std::fs::read(&path)?;
    serde_json::from_slice(&raw)
        .map(Some)
        .map_err(|e| SurgeError::Config(format!("Failed to decode {}: {e}", path.display())))
}

#[must_use]
fn update_worker_path(install_dir: &Path) -> PathBuf {
    install_dir.join(UPDATE_WORKER_FILE_NAME)
}

#[must_use]
fn update_worker_lock_path(install_dir: &Path) -> PathBuf {
    install_dir.join(UPDATE_WORKER_LOCK_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::process::is_pid_alive;
    use std::time::Instant;

    #[test]
    fn stale_in_progress_package_apply_becomes_retry_safe_failed_when_abandoned() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:01:00Z".to_string());
        write_update_status(dir.path(), &record).unwrap();

        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let failed = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(1),
            now,
        )
        .unwrap()
        .expect("stale record should transition");

        assert_eq!(failed.state, UpdateConvergenceState::Failed);
        assert_eq!(failed.installed_version, "9998.0.0");
        assert_eq!(failed.target_version, "9999.0.0");
        assert_eq!(failed.failure_phase.as_deref(), Some("package apply started"));
        assert_eq!(failed.retry_safe, Some(true));
        assert_eq!(failed.attempted_at_utc.as_deref(), Some("2026-05-15T20:00:00Z"));
        assert_eq!(failed.completed_at_utc.as_deref(), Some("2026-05-15T20:01:00Z"));
        assert!(
            failed
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains("abandoned after 540s without progress")
        );
        assert_eq!(failed.retry_count, Some(1));
        let next_retry = chrono::DateTime::parse_from_rfc3339(failed.next_retry_at_utc.as_deref().unwrap())
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            (next_retry - now).to_std().unwrap(),
            crate::update::status::RETRY_BACKOFF_BASE,
            "first failure should schedule the base backoff"
        );

        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::Failed);
        assert_eq!(persisted.failure_phase.as_deref(), Some("package apply started"));
        assert_eq!(persisted.next_retry_at_utc, failed.next_retry_at_utc);
        assert_eq!(persisted.retry_count, Some(1));
    }

    #[test]
    fn fresh_in_progress_without_worker_marker_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:09:30Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:09:45Z".to_string());
        write_update_status(dir.path(), &record).unwrap();

        let result = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(1),
            now,
        )
        .unwrap();

        assert!(result.is_none());
        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::InProgress);
    }

    #[test]
    fn dead_worker_pid_fails_attempt_immediately_even_with_fresh_progress() {
        let dir = tempfile::tempdir().unwrap();
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:09:30Z".to_string(),
        )
        .with_current_phase_at(
            "restoring current package from release graph",
            "2026-05-15T20:09:45Z".to_string(),
        );
        write_update_status(dir.path(), &record).unwrap();
        let (pid, start_time) = dead_helper_identity();
        write_worker_file_with_start_time(dir.path(), pid, "demo-app", "9999.0.0", Some(start_time));

        let failed = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(5),
            now,
        )
        .unwrap()
        .expect("dead worker should fail the attempt without waiting out the staleness window");

        assert_eq!(failed.state, UpdateConvergenceState::Failed);
        assert_eq!(
            failed.failure_phase.as_deref(),
            Some("restoring current package from release graph")
        );
        assert!(failed.reason.as_deref().unwrap().contains("exited without completing"));
        assert!(failed.reason.as_deref().unwrap().contains("last progress 15s ago"));
        assert_eq!(failed.retry_count, Some(1));
        assert!(failed.next_retry_at_utc.is_some());

        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::Failed);
        assert_eq!(persisted.reason, failed.reason);
    }

    #[test]
    fn live_foreign_worker_is_never_abandoned_within_the_stalled_work_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:01:00Z".to_string());
        write_update_status(dir.path(), &record).unwrap();
        let (pid, mut child) = live_helper_pid();
        write_worker_file(dir.path(), pid, "demo-app", "9999.0.0");

        // Progress is 9 minutes stale: past the markerless staleness window
        // but inside the stalled-work deadline for a live worker.
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let result = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(1),
            now,
        )
        .unwrap();

        assert!(result.is_none(), "a live concurrent worker must never be reclassified");
        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::InProgress);
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn live_foreign_worker_stalled_past_deadline_is_failed_as_retry_safe() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:01:00Z".to_string());
        write_update_status(dir.path(), &record).unwrap();
        let (pid, mut child) = live_helper_pid();
        write_worker_file(dir.path(), pid, "demo-app", "9999.0.0");

        // Progress is 59 minutes stale: past the stalled-work deadline, so a
        // live-but-hung worker must be failed instead of staying in_progress
        // forever.
        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T21:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let result = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(1),
            now,
        )
        .unwrap();

        let failed = result.expect("a stalled live worker must be failed");
        assert_eq!(failed.state, UpdateConvergenceState::Failed);
        assert_eq!(failed.retry_safe, Some(true));
        assert!(
            failed
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("presumed stalled")),
            "reason was: {:?}",
            failed.reason
        );
        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::Failed);
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn worker_marker_for_other_target_does_not_classify_record() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:01:00Z".to_string());
        write_update_status(dir.path(), &record).unwrap();
        // A live worker for a different target does not protect this record;
        // it falls back to the staleness gate (stale here) and fails.
        let (pid, mut child) = live_helper_pid();
        write_worker_file(dir.path(), pid, "demo-app", "9998.0.0");

        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let failed = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(1),
            now,
        )
        .unwrap()
        .expect("unmatched worker marker should fall back to the staleness gate");

        assert_eq!(failed.state, UpdateConvergenceState::Failed);
        let _ = child.kill();
        let _ = child.wait();
    }

    fn write_worker_file(dir: &Path, pid: u32, app_id: &str, target_version: &str) {
        write_worker_file_with_start_time(dir, pid, app_id, target_version, process_start_time(pid));
    }

    fn write_worker_file_with_start_time(
        dir: &Path,
        pid: u32,
        app_id: &str,
        target_version: &str,
        process_start_time: Option<u64>,
    ) {
        let record = UpdateWorkerRecord {
            pid,
            app_id: app_id.to_string(),
            target_version: target_version.to_string(),
            started_at_utc: now_utc_rfc3339(),
            process_start_time,
            owner_id: String::new(),
        };
        let json = serde_json::to_vec_pretty(&record).unwrap();
        write_file_atomic(&update_worker_path(dir), &json).unwrap();
    }

    fn dead_helper_identity() -> (u32, u64) {
        let (pid, mut child) = live_helper_pid();
        let start_time = process_start_time(pid).expect("live helper start time");
        let _ = child.kill();
        let _ = child.wait();
        let deadline = Instant::now() + Duration::from_secs(5);
        while is_pid_alive(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!is_pid_alive(pid), "helper pid {pid} should be dead before the test");
        (pid, start_time)
    }

    fn live_helper_pid() -> (u32, std::process::Child) {
        let child = spawn_helper(true);
        let pid = child.id();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !is_pid_alive(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(is_pid_alive(pid), "helper pid {pid} should be alive during the test");
        (pid, child)
    }

    /// Spawn a helper that either exits immediately (`sleep: false`) or sleeps
    /// ~15s (`sleep: true`).
    fn spawn_helper(sleep: bool) -> std::process::Child {
        #[cfg(windows)]
        {
            // `ping` sleeps without needing a console; 16 pings ~= 15s.
            let script = if sleep {
                "ping 127.0.0.1 -n 16 >nul"
            } else {
                "exit /b 0"
            };
            std::process::Command::new("cmd")
                .args(["/c", script])
                .spawn()
                .expect("spawn helper")
        }
        #[cfg(not(windows))]
        {
            std::process::Command::new("sh")
                .args(["-c", if sleep { "sleep 15" } else { "exit 0" }])
                .spawn()
                .expect("spawn helper")
        }
    }

    #[test]
    fn stale_in_progress_owned_by_current_worker_is_left_in_progress() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:01:00Z".to_string());
        write_update_status(dir.path(), &record).unwrap();
        let _worker = UpdateWorkerGuard::record(dir.path(), "demo-app", "9999.0.0").unwrap();

        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let result = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(1),
            now,
        )
        .unwrap();

        assert!(
            result.is_none(),
            "the current process owns the marker; its new attempt takes over"
        );
        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::InProgress);
        assert_eq!(persisted.current_phase.as_deref(), Some("package apply started"));
    }

    #[test]
    fn reused_current_pid_does_not_own_worker_marker() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-15T20:00:00Z".to_string(),
        )
        .with_current_phase_at("package apply started", "2026-05-15T20:09:45Z".to_string());
        write_update_status(dir.path(), &record).unwrap();

        let pid = std::process::id();
        let actual_start_time = process_start_time(pid).expect("current process start time");
        let worker = UpdateWorkerRecord {
            pid,
            app_id: "demo-app".to_string(),
            target_version: "9999.0.0".to_string(),
            started_at_utc: now_utc_rfc3339(),
            process_start_time: Some(actual_start_time ^ 1),
            owner_id: "stale-owner".to_string(),
        };
        let json = serde_json::to_vec_pretty(&worker).unwrap();
        write_file_atomic(&update_worker_path(dir.path()), &json).unwrap();

        let now = chrono::DateTime::parse_from_rfc3339("2026-05-15T20:10:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let failed = fail_abandoned_in_progress_update_at(
            dir.path(),
            "demo-app",
            "9999.0.0",
            "stable",
            Duration::from_mins(5),
            now,
        )
        .unwrap()
        .expect("a reused pid must not retain worker ownership");

        assert_eq!(failed.state, UpdateConvergenceState::Failed);
        assert!(failed.reason.as_deref().unwrap().contains("exited without completing"));
    }

    #[test]
    fn live_worker_excludes_a_second_update_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let first = UpdateWorkerGuard::record(dir.path(), "demo-app", "9999.0.0").unwrap();

        let Err(error) = UpdateWorkerGuard::record(dir.path(), "demo-app", "9999.0.0") else {
            panic!("a live worker must keep exclusive ownership");
        };

        assert!(error.to_string().contains("already owns"));
        drop(first);
        assert!(UpdateWorkerGuard::record(dir.path(), "demo-app", "9999.0.0").is_ok());
    }

    #[test]
    fn external_helper_takeover_survives_parent_guard_drop() {
        let dir = tempfile::tempdir().unwrap();
        let parent = UpdateWorkerGuard::record(dir.path(), "demo-app", "9999.0.0").unwrap();
        let pid = current_pid();
        let start_time = process_start_time(pid).unwrap();
        let helper = UpdateWorkerGuard::take_over(dir.path(), "demo-app", "9999.0.0", pid, start_time).unwrap();

        drop(parent);

        let persisted = read_update_worker(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.owner_id, helper.owner_id);
        drop(helper);
        assert!(read_update_worker(dir.path()).unwrap().is_none());
    }
}
