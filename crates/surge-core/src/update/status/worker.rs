//! Update-worker ownership marker and abandoned-attempt classification.
//!
//! A live update attempt records its worker (pid, app, target version) next
//! to the status file. When a later attempt finds an `InProgress` record
//! whose worker is gone and whose progress heartbeat is stale, it classifies
//! that attempt as abandoned before starting its own.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;

use super::{
    FailureContext, UpdateConvergenceState, UpdateStatusRecord, now_utc_rfc3339, read_update_status,
    write_update_status,
};

const UPDATE_WORKER_FILE_NAME: &str = ".surge-update-worker.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct UpdateWorkerRecord {
    pid: u32,
    app_id: String,
    target_version: String,
    started_at_utc: String,
}

pub struct UpdateWorkerGuard {
    path: PathBuf,
    pid: u32,
    app_id: String,
    target_version: String,
}

impl UpdateWorkerGuard {
    pub fn record(install_dir: &Path, app_id: &str, target_version: &str) -> Result<Self> {
        let record = UpdateWorkerRecord {
            pid: std::process::id(),
            app_id: app_id.to_string(),
            target_version: target_version.to_string(),
            started_at_utc: now_utc_rfc3339(),
        };
        let path = update_worker_path(install_dir);
        let json = serde_json::to_vec_pretty(&record)
            .map_err(|e| SurgeError::Config(format!("Failed to encode update worker marker: {e}")))?;
        write_file_atomic(&path, &json)?;
        Ok(Self {
            path,
            pid: record.pid,
            app_id: record.app_id,
            target_version: record.target_version,
        })
    }
}

impl Drop for UpdateWorkerGuard {
    fn drop(&mut self) {
        let Ok(raw) = std::fs::read(&self.path) else {
            return;
        };
        let Ok(record) = serde_json::from_slice::<UpdateWorkerRecord>(&raw) else {
            return;
        };
        if record.pid == self.pid && record.app_id == self.app_id && record.target_version == self.target_version {
            let _ = std::fs::remove_file(&self.path);
        }
    }
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
    let Some(age) = stale_progress_age(&record, now) else {
        return Ok(None);
    };
    if age < stale_after || matching_worker_is_current(install_dir, &record) {
        return Ok(None);
    }

    let phase = record
        .current_phase
        .as_deref()
        .or(record.failure_phase.as_deref())
        .unwrap_or("unknown");
    let attempted_at_utc = record.attempted_at_utc.clone().unwrap_or_else(now_utc_rfc3339);
    let last_activity_at_utc = record
        .last_progress_at_utc
        .clone()
        .unwrap_or_else(|| attempted_at_utc.clone());
    let failed = UpdateStatusRecord::failed_with_context_at(
        &record.app_id,
        &record.installed_version,
        &record.target_version,
        &record.channel,
        attempted_at_utc,
        last_activity_at_utc,
        &format!(
            "previous update attempt abandoned after {}s without progress at phase '{phase}'",
            age.as_secs()
        ),
        FailureContext::from_record(Some(&record), true),
    );
    write_update_status(install_dir, &failed)?;
    Ok(Some(failed))
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

fn matching_worker_is_current(install_dir: &Path, record: &UpdateStatusRecord) -> bool {
    let Ok(Some(worker)) = read_update_worker(install_dir) else {
        return false;
    };
    worker.pid == std::process::id() && worker.app_id == record.app_id && worker.target_version == record.target_version
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

#[cfg(test)]
mod tests {
    use super::*;

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

        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::Failed);
        assert_eq!(persisted.failure_phase.as_deref(), Some("package apply started"));
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

        assert!(result.is_none());
        let persisted = read_update_status(dir.path()).unwrap().unwrap();
        assert_eq!(persisted.state, UpdateConvergenceState::InProgress);
        assert_eq!(persisted.current_phase.as_deref(), Some("package apply started"));
    }
}
