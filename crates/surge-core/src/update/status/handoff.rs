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

    let attempted_at_utc = existing.attempted_at_utc.unwrap_or_else(now_utc_rfc3339);
    let record = UpdateStatusRecord::pending_restart_with_failure_phase(
        &existing.app_id,
        &existing.target_version,
        &existing.target_version,
        &existing.channel,
        attempted_at_utc,
        now_utc_rfc3339(),
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

    let record = UpdateStatusRecord::converged(
        &existing.app_id,
        &existing.target_version,
        &existing.channel,
        existing.attempted_at_utc,
        now_utc_rfc3339(),
        true,
    );
    write_update_status(install_dir, &record)?;
    Ok(Some(record))
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

        let converged = mark_restart_handoff_converged(dir.path(), "9999.0.0")
            .unwrap()
            .expect("matching record should converge");
        assert_eq!(converged.state, UpdateConvergenceState::Converged);
        assert!(converged.supervisor_restart_confirmed);
        assert_eq!(converged.attempted_at_utc.as_deref(), Some("2026-05-11T14:00:00Z"));
    }
}
