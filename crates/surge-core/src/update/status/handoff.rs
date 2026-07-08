use std::path::Path;

use crate::error::Result;

use super::{UpdateConvergenceState, UpdateStatusRecord, now_utc_rfc3339, read_update_status, write_update_status};

pub const RESTART_HANDOFF_FAILED_PHASE: &str = "restart handoff failed";
pub const RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE: &str = "restart handoff waiting for old child";
pub const RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE: &str = "restart handoff target child exited";

pub fn mark_restart_handoff_pending(
    install_dir: &Path,
    target_version: &str,
    reason: &str,
    failure_phase: &str,
) -> Result<Option<UpdateStatusRecord>> {
    let Some(existing) = read_matching_handoff_record(install_dir, target_version)? else {
        return Ok(None);
    };

    let completed_at_utc = update_work_completed_at(&existing);
    let attempted_at_utc = existing.attempted_at_utc.unwrap_or_else(now_utc_rfc3339);
    let record = UpdateStatusRecord::pending_restart_with_failure_phase(
        &existing.app_id,
        &existing.target_version,
        &existing.target_version,
        &existing.channel,
        attempted_at_utc,
        completed_at_utc,
        reason,
        failure_phase,
    );
    write_update_status(install_dir, &record)?;
    Ok(Some(record))
}

pub fn mark_restart_handoff_converged(install_dir: &Path, target_version: &str) -> Result<Option<UpdateStatusRecord>> {
    let Some(existing) = read_matching_handoff_record(install_dir, target_version)? else {
        return Ok(None);
    };

    let completed_at_utc = update_work_completed_at(&existing);
    let record = UpdateStatusRecord::converged(
        &existing.app_id,
        &existing.target_version,
        &existing.channel,
        existing.attempted_at_utc,
        completed_at_utc,
        true,
    );
    write_update_status(install_dir, &record)?;
    Ok(Some(record))
}

/// `attempted_at_utc..completed_at_utc` measures the update work itself, not
/// how long convergence took to prove. Handoff transitions can fire hours
/// after the apply finished (wedged old child, power loss, crash-looping
/// target child), so re-stamping `completed_at_utc` at transition time would
/// fold that outage into the recorded update duration.
fn update_work_completed_at(existing: &UpdateStatusRecord) -> String {
    existing
        .completed_at_utc
        .clone()
        .or_else(|| existing.last_progress_at_utc.clone())
        .unwrap_or_else(now_utc_rfc3339)
}

fn read_matching_handoff_record(install_dir: &Path, target_version: &str) -> Result<Option<UpdateStatusRecord>> {
    let Some(record) = read_update_status(install_dir)? else {
        return Ok(None);
    };
    if record.target_version.trim() != target_version.trim() {
        return Ok(None);
    }
    if matches!(
        record.state,
        UpdateConvergenceState::InProgress | UpdateConvergenceState::PendingRestart
    ) {
        return Ok(Some(record));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_handoff_helpers_distinguish_child_exit_and_convergence() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::pending_restart_with_failure_phase(
            "demo-app",
            "9999.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "2026-05-11T14:05:00Z".to_string(),
            "waiting for previous child to exit",
            RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE,
        );
        write_update_status(dir.path(), &record).unwrap();

        let child_exited = mark_restart_handoff_pending(
            dir.path(),
            "9999.0.0",
            "target child exited before restart handoff completed",
            RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE,
        )
        .unwrap()
        .expect("matching record should update");
        assert_eq!(child_exited.state, UpdateConvergenceState::PendingRestart);
        assert_eq!(
            child_exited.failure_phase.as_deref(),
            Some(RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE)
        );
        assert_eq!(child_exited.completed_at_utc.as_deref(), Some("2026-05-11T14:05:00Z"));

        let converged = mark_restart_handoff_converged(dir.path(), "9999.0.0")
            .unwrap()
            .expect("matching record should converge");
        assert_eq!(converged.state, UpdateConvergenceState::Converged);
        assert!(converged.supervisor_restart_confirmed);
        assert_eq!(converged.attempted_at_utc.as_deref(), Some("2026-05-11T14:00:00Z"));
        assert_eq!(converged.completed_at_utc.as_deref(), Some("2026-05-11T14:05:00Z"));
    }

    #[test]
    fn delayed_convergence_preserves_update_work_completed_at() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::pending_restart(
            "demo-app",
            "9999.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
            "2026-05-11T14:05:00Z".to_string(),
            "waiting for previous child to exit",
        );
        write_update_status(dir.path(), &record).unwrap();

        let converged = mark_restart_handoff_converged(dir.path(), "9999.0.0")
            .unwrap()
            .expect("matching record should converge");
        assert_eq!(converged.completed_at_utc.as_deref(), Some("2026-05-11T14:05:00Z"));
    }

    #[test]
    fn in_progress_convergence_falls_back_to_last_progress_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::in_progress(
            "demo-app",
            "9998.0.0",
            "9999.0.0",
            "stable",
            "2026-05-11T14:00:00Z".to_string(),
        )
        .with_completed_phase_at("finalize", "2026-05-11T14:04:00Z".to_string());
        write_update_status(dir.path(), &record).unwrap();

        let converged = mark_restart_handoff_converged(dir.path(), "9999.0.0")
            .unwrap()
            .expect("matching record should converge");
        assert_eq!(converged.attempted_at_utc.as_deref(), Some("2026-05-11T14:00:00Z"));
        assert_eq!(converged.completed_at_utc.as_deref(), Some("2026-05-11T14:04:00Z"));
    }
}
