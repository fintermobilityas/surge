use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::error::{Result, SurgeError};
use crate::install::prune_version_snapshots;
use crate::platform::fs::atomic_rename;
use crate::releases::manifest::ReleaseEntry;
use crate::update::status::{self, FailureContext, UpdateStatusRecord, UpdateWorkerGuard};

use super::super::finalize::finalize_update_with_pre_restart;
use super::super::lifecycle::SupervisorRestartOutcome;
use super::super::progress::ProgressInfo;
use super::super::progress_substep::{PhaseProgressEmitter, labels as update_phase};
use super::super::{ApplyStrategy, UpdateInfo, UpdateManager, current_install, lifecycle};
use super::plan::{ExternalFinalizePlan, manager_from_plan, read_and_validate_plan, validate_materialized_target};
use super::quiesce::quiesce_updating_application;
use super::schedule::{marker_matches, write_handshake_marker};

const HELPER_ARM_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const TERMINAL_STATUS_WRITE_ATTEMPTS: u32 = 5;
const TERMINAL_STATUS_WRITE_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Complete a staged update from the stable supervisor helper process.
///
/// This entry point is for `surge-supervisor finalize-update`; applications
/// should use [`UpdateManager::download_and_apply`](super::super::UpdateManager::download_and_apply).
#[doc(hidden)]
pub async fn run_external_finalize(plan_path: &Path) -> Result<()> {
    let plan = read_and_validate_plan(plan_path)?;
    let mut manager = manager_from_plan(&plan)?;
    validate_materialized_target(&plan, &manager)?;

    let _worker_guard = UpdateWorkerGuard::take_over(
        &plan.install_dir,
        &plan.app_id,
        &plan.target_release.version,
        plan.updater_pid,
        plan.updater_start_time,
    )?;
    write_handshake_marker(&plan.ready_path(), &plan.operation_id)?;
    wait_until_armed(&plan).await?;
    write_handshake_marker(&plan.accepted_path(), &plan.operation_id)?;

    let mut recovery_required = false;
    let result = run_accepted_finalize(&plan, &mut manager, &mut recovery_required).await;
    match result {
        Ok(()) => {
            if let Err(error) =
                commit_previous_snapshot(&plan.install_dir, &plan.current_version, plan.release_retention_limit)
            {
                warn!(error = %error, "Failed to finalize previous-version snapshot after handoff acceptance");
            }
            cleanup_operation(&plan, "completed");
            Ok(())
        }
        Err(error) => {
            let recovery = if recovery_required {
                recover_previous_install(&plan, &manager).await
            } else {
                Ok(plan.active_app_dir())
            };
            let previous_app_dir = match recovery {
                Ok(previous_app_dir) => previous_app_dir,
                Err(recovery_error) => {
                    persist_external_failure(&plan, &error, Some(&recovery_error))?;
                    cleanup_operation(&plan, "failed");
                    return Err(SurgeError::Update(format!(
                        "External finalization failed: {error}; previous install recovery failed: {recovery_error}"
                    )));
                }
            };
            persist_external_failure(&plan, &error, None)?;

            if let Err(restart_error) = restart_previous_runtime(&plan, &previous_app_dir) {
                persist_external_failure(&plan, &error, Some(&restart_error))?;
                cleanup_operation(&plan, "failed");
                return Err(SurgeError::Update(format!(
                    "External finalization failed: {error}; previous runtime restart failed: {restart_error}"
                )));
            }

            cleanup_operation(&plan, "failed");
            Err(error)
        }
    }
}

async fn run_accepted_finalize(
    plan: &ExternalFinalizePlan,
    manager: &mut UpdateManager,
    recovery_required: &mut bool,
) -> Result<()> {
    persist_external_phase(plan, update_phase::STOPPING_SUPERVISOR);
    *recovery_required = true;
    lifecycle::request_supervisor_shutdown(&plan.install_dir, &plan.current_release_identity.supervisor_id).await?;

    persist_external_phase(plan, update_phase::WAITING_FOR_UPDATER_EXIT);
    quiesce_updating_application(plan.updater_pid, plan.updater_start_time, &plan.updater_exe)?;

    let installed_identity = current_install::load(manager)?;
    if installed_identity.as_ref() != Some(&plan.current_release_identity) {
        return Err(SurgeError::Update(
            "Installed application identity changed while the external finalizer waited for the updater to exit"
                .to_string(),
        ));
    }
    manager.current_release_identity = installed_identity;

    let info = UpdateInfo {
        available_releases: vec![plan.target_release.clone()],
        latest_version: plan.target_release.version.clone(),
        delta_available: false,
        download_size: 0,
        apply_releases: vec![plan.target_release.clone()],
        apply_strategy: ApplyStrategy::Full,
        fallback_reason: None,
    };
    let progress: Option<Arc<fn(ProgressInfo)>> = None;
    let progress_emitter = PhaseProgressEmitter {
        progress: progress.as_ref(),
        install_dir: &plan.install_dir,
        in_progress_template: &plan.in_progress_template,
    };
    let persist_pending = || persist_external_pending_restart(plan);
    let outcome = finalize_update_with_pre_restart(
        manager,
        &info,
        &plan.extracted_final_dir(),
        &plan.staging_dir(),
        &plan.artifact_cache_dir(),
        &progress_emitter,
        Some(&persist_pending),
        Some(&plan.restart_args),
    )
    .await?;

    match outcome {
        SupervisorRestartOutcome::PendingRestart { failure_phase, .. }
            if failure_phase == status::RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE =>
        {
            Ok(())
        }
        SupervisorRestartOutcome::PendingRestart { reason, .. } => Err(SurgeError::Supervisor(reason)),
        SupervisorRestartOutcome::NotApplicable => Err(SurgeError::Supervisor(
            "Target release supervisor was not started".to_string(),
        )),
        SupervisorRestartOutcome::ExternalFinalizeScheduled => Err(SurgeError::Supervisor(
            "External finalizer attempted to schedule another finalizer".to_string(),
        )),
    }
}

async fn wait_until_armed(plan: &ExternalFinalizePlan) -> Result<()> {
    let deadline = tokio::time::Instant::now() + HELPER_ARM_TIMEOUT;
    loop {
        if marker_matches(&plan.armed_path(), &plan.operation_id)? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "External finalizer was not armed within {}s; active application was left untouched",
                HELPER_ARM_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(HANDSHAKE_POLL_INTERVAL).await;
    }
}

fn persist_external_phase(plan: &ExternalFinalizePlan, phase: &'static str) {
    let record = plan
        .in_progress_template
        .clone()
        .with_current_phase_at(phase, status::now_utc_rfc3339());
    if let Err(error) = status::write_update_status(&plan.install_dir, &record) {
        warn!(error = %error, phase, "Failed to persist external finalizer progress");
    }
}

fn persist_external_pending_restart(plan: &ExternalFinalizePlan) -> Result<()> {
    let attempted_at_utc = plan
        .in_progress_template
        .attempted_at_utc
        .clone()
        .unwrap_or_else(status::now_utc_rfc3339);
    let record = UpdateStatusRecord::pending_restart_with_failure_phase(
        &plan.app_id,
        &plan.target_release.version,
        &plan.target_release.version,
        &plan.channel,
        attempted_at_utc,
        status::now_utc_rfc3339(),
        "external finalizer activated the target; waiting for helper exit and target startup proof",
        status::RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE,
    );
    write_terminal_status(&plan.install_dir, &record)
}

fn persist_external_failure(
    plan: &ExternalFinalizePlan,
    error: &SurgeError,
    recovery_error: Option<&SurgeError>,
) -> Result<()> {
    let attempted_at_utc = plan
        .in_progress_template
        .attempted_at_utc
        .clone()
        .unwrap_or_else(status::now_utc_rfc3339);
    let status_context = status::read_update_status(&plan.install_dir).ok().flatten();
    let reason = recovery_error.map_or_else(
        || error.to_string(),
        |recovery| format!("{error}; previous runtime recovery failed: {recovery}"),
    );
    let record = UpdateStatusRecord::failed_with_context(
        &plan.app_id,
        &plan.current_version,
        &plan.target_release.version,
        &plan.channel,
        attempted_at_utc,
        &reason,
        FailureContext::from_record(status_context.as_ref(), recovery_error.is_none()),
    );
    let record = if recovery_error.is_none() {
        let schedule = status::retry_schedule(plan.previous_attempt_status.as_ref(), &plan.target_release.version);
        record.with_retry_schedule_at(&schedule, status::next_retry_timestamp(chrono::Utc::now(), &schedule))
    } else {
        record
    };
    write_terminal_status(&plan.install_dir, &record)
}

fn write_terminal_status(install_dir: &Path, record: &UpdateStatusRecord) -> Result<()> {
    write_terminal_status_with(
        install_dir,
        record,
        TERMINAL_STATUS_WRITE_ATTEMPTS,
        TERMINAL_STATUS_WRITE_RETRY_DELAY,
        status::write_update_status,
    )
}

fn write_terminal_status_with<F>(
    install_dir: &Path,
    record: &UpdateStatusRecord,
    attempts: u32,
    retry_delay: Duration,
    mut write_status: F,
) -> Result<()>
where
    F: FnMut(&Path, &UpdateStatusRecord) -> Result<()>,
{
    if attempts == 0 {
        return Err(SurgeError::Config(
            "Terminal status persistence requires at least one attempt".to_string(),
        ));
    }

    for attempt in 1..=attempts {
        match write_status(install_dir, record) {
            Ok(()) => return Ok(()),
            Err(error) if attempt < attempts => {
                warn!(
                    error = %error,
                    attempt,
                    attempts,
                    "Failed to persist terminal external finalizer status; retrying"
                );
                if !retry_delay.is_zero() {
                    std::thread::sleep(retry_delay);
                }
            }
            Err(error) => {
                return Err(SurgeError::Update(format!(
                    "Failed to persist terminal external finalizer status after {attempts} attempts: {error}"
                )));
            }
        }
    }

    unreachable!("terminal status write loop always returns")
}

async fn recover_previous_install(plan: &ExternalFinalizePlan, manager: &UpdateManager) -> Result<std::path::PathBuf> {
    lifecycle::request_supervisor_shutdown(&plan.install_dir, &plan.current_release_identity.supervisor_id).await?;
    if plan.target_release.supervisor_id != plan.current_release_identity.supervisor_id {
        lifecycle::request_supervisor_shutdown(&plan.install_dir, &plan.target_release.supervisor_id).await?;
    }
    quiesce_updating_application(plan.updater_pid, plan.updater_start_time, &plan.updater_exe)?;
    restore_previous_app_dir(plan, manager)
}

fn restore_previous_app_dir(plan: &ExternalFinalizePlan, manager: &UpdateManager) -> Result<std::path::PathBuf> {
    let active_app_dir = plan.active_app_dir();
    let previous_swap_dir = plan.install_dir.join(".surge-app-prev");
    let previous_version_dir = plan.previous_version_dir();
    for candidate in [&previous_swap_dir, &previous_version_dir] {
        if candidate.is_dir()
            && current_install::load_previous_swap(manager, candidate)?.as_ref() == Some(&plan.current_release_identity)
        {
            replace_failed_target(&active_app_dir, candidate, &plan.operation_id)?;
            return Ok(active_app_dir);
        }
    }

    if active_app_dir.is_dir()
        && current_install::load_previous_swap(manager, &active_app_dir)?.as_ref()
            == Some(&plan.current_release_identity)
    {
        return Ok(active_app_dir);
    }

    Err(SurgeError::Update(
        "No application directory matching the pre-update release is available for recovery".to_string(),
    ))
}

fn replace_failed_target(active_app_dir: &Path, previous_app_dir: &Path, operation_id: &str) -> Result<()> {
    let install_dir = active_app_dir
        .parent()
        .ok_or_else(|| SurgeError::Config("Active application directory has no install root".to_string()))?;
    let failed_target_dir = install_dir.join(format!(".surge-app-failed-{operation_id}"));
    if failed_target_dir.exists() {
        std::fs::remove_dir_all(&failed_target_dir)?;
    }
    if active_app_dir.exists() {
        atomic_rename(active_app_dir, &failed_target_dir)?;
    }
    if let Err(error) = atomic_rename(previous_app_dir, active_app_dir) {
        if failed_target_dir.is_dir() && !active_app_dir.exists() {
            let _ = atomic_rename(&failed_target_dir, active_app_dir);
        }
        return Err(error);
    }
    let _ = std::fs::remove_dir_all(&failed_target_dir);
    Ok(())
}

fn commit_previous_snapshot(install_dir: &Path, current_version: &str, retention_limit: usize) -> Result<()> {
    let previous_swap_dir = install_dir.join(".surge-app-prev");
    if previous_swap_dir.is_dir() {
        if retention_limit == 0 {
            std::fs::remove_dir_all(&previous_swap_dir)?;
        } else {
            let previous_version_dir = install_dir.join(format!("app-{current_version}"));
            if previous_version_dir.exists() {
                std::fs::remove_dir_all(&previous_version_dir)?;
            }
            atomic_rename(&previous_swap_dir, &previous_version_dir)?;
        }
    }
    prune_version_snapshots(install_dir, retention_limit)?;
    Ok(())
}

fn restart_previous_runtime(plan: &ExternalFinalizePlan, previous_app_dir: &Path) -> Result<()> {
    let release = release_from_identity(&plan.current_release_identity);
    match lifecycle::restart_supervisor_after_update_with_args(
        &plan.install_dir,
        previous_app_dir,
        &release,
        std::process::id(),
        &plan.restart_args,
        None,
    ) {
        SupervisorRestartOutcome::PendingRestart { failure_phase, reason }
            if failure_phase == status::RESTART_HANDOFF_FAILED_PHASE =>
        {
            Err(SurgeError::Supervisor(reason))
        }
        SupervisorRestartOutcome::PendingRestart { .. } => Ok(()),
        SupervisorRestartOutcome::NotApplicable => Err(SurgeError::Supervisor(
            "Previous release supervisor was not restarted".to_string(),
        )),
        SupervisorRestartOutcome::ExternalFinalizeScheduled => Err(SurgeError::Supervisor(
            "Previous release recovery attempted to schedule another finalizer".to_string(),
        )),
    }
}

fn release_from_identity(identity: &current_install::ReleaseIdentity) -> ReleaseEntry {
    ReleaseEntry {
        version: identity.version.clone(),
        main_exe: identity.main_exe.clone(),
        supervisor_id: identity.supervisor_id.clone(),
        environment: identity.environment.clone(),
        ..ReleaseEntry::default()
    }
}

fn cleanup_operation(plan: &ExternalFinalizePlan, outcome: &str) {
    if let Err(error) = std::fs::remove_dir_all(plan.operation_dir()) {
        warn!(error = %error, outcome, "Failed to remove terminal external finalizer plan directory");
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[test]
    fn terminal_status_write_retries_transient_failure() {
        let temp = tempfile::tempdir().unwrap();
        let attempts = Cell::new(0_u32);
        let record = UpdateStatusRecord::converged("demo", "2.0.0", "test", None, status::now_utc_rfc3339(), true);

        write_terminal_status_with(temp.path(), &record, 3, Duration::ZERO, |_, _| {
            attempts.set(attempts.get() + 1);
            if attempts.get() < 3 {
                Err(SurgeError::Update("transient write failure".to_string()))
            } else {
                Ok(())
            }
        })
        .unwrap();

        assert_eq!(attempts.get(), 3);
    }

    #[test]
    fn terminal_status_write_reports_exhausted_retries() {
        let temp = tempfile::tempdir().unwrap();
        let record = UpdateStatusRecord::converged("demo", "2.0.0", "test", None, status::now_utc_rfc3339(), true);

        let error = write_terminal_status_with(temp.path(), &record, 2, Duration::ZERO, |_, _| {
            Err(SurgeError::Update("persistent write failure".to_string()))
        })
        .unwrap_err();

        assert!(error.to_string().contains("after 2 attempts"));
    }

    #[test]
    fn failed_target_is_replaced_by_previous_application() {
        let temp = tempfile::tempdir().unwrap();
        let active = temp.path().join("app");
        let previous = temp.path().join("app-1.0.0");
        std::fs::create_dir_all(&active).unwrap();
        std::fs::create_dir_all(&previous).unwrap();
        std::fs::write(active.join("version"), "2.0.0").unwrap();
        std::fs::write(previous.join("version"), "1.0.0").unwrap();

        replace_failed_target(&active, &previous, "operation").unwrap();

        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "1.0.0");
        assert!(!previous.exists());
        assert!(!temp.path().join(".surge-app-failed-operation").exists());
    }

    #[test]
    fn failed_recovery_rename_restores_the_target_directory() {
        let temp = tempfile::tempdir().unwrap();
        let active = temp.path().join("app");
        let missing_previous = temp.path().join("app-1.0.0");
        std::fs::create_dir_all(&active).unwrap();
        std::fs::write(active.join("version"), "2.0.0").unwrap();

        replace_failed_target(&active, &missing_previous, "operation").unwrap_err();

        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "2.0.0");
        assert!(!temp.path().join(".surge-app-failed-operation").exists());
    }

    #[test]
    fn accepted_handoff_preserves_the_exact_previous_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let previous_swap = temp.path().join(".surge-app-prev");
        let stale_version_snapshot = temp.path().join("app-1.0.0");
        std::fs::create_dir_all(&previous_swap).unwrap();
        std::fs::create_dir_all(&stale_version_snapshot).unwrap();
        std::fs::write(previous_swap.join("identity"), "exact-pre-update-bits").unwrap();
        std::fs::write(stale_version_snapshot.join("identity"), "stale-snapshot").unwrap();

        commit_previous_snapshot(temp.path(), "1.0.0", 1).unwrap();

        assert_eq!(
            std::fs::read_to_string(stale_version_snapshot.join("identity")).unwrap(),
            "exact-pre-update-bits"
        );
        assert!(!previous_swap.exists());
    }
}
