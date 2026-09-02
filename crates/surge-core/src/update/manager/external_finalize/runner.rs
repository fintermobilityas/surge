use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::error::{Result, SurgeError};
use crate::platform::fs::atomic_rename;
use crate::platform::process::{current_pid, spawn_detached};
use crate::releases::manifest::ReleaseEntry;
use crate::update::status::{self, FailureContext, UpdateStatusRecord, UpdateWorkerGuard};

use super::super::progress::ProgressInfo;
use super::super::progress_substep::{PhaseProgressEmitter, labels as update_phase};
use super::super::{
    ApplyStrategy, SupervisorRestartOutcome, UpdateInfo, UpdateManager, current_install, finalize_update,
};
use super::plan::{ExternalFinalizePlan, manager_from_plan, read_and_validate_plan, validate_materialized_target};
use super::schedule::{marker_matches, write_handshake_marker};

const HELPER_ARM_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const TERMINAL_STATUS_WRITE_ATTEMPTS: u32 = 5;
const TERMINAL_STATUS_WRITE_RETRY_DELAY: Duration = Duration::from_millis(200);

/// Complete a staged update from the stable helper process.
///
/// This API is intended for `surge-supervisor finalize-update`; consumers
/// should continue to use [`UpdateManager::download_and_apply`].
#[doc(hidden)]
pub async fn run_external_finalize<F>(plan_path: &Path, mut quiesce_updater: F) -> Result<()>
where
    F: FnMut(u32, &Path) -> Result<()>,
{
    let plan = read_and_validate_plan(plan_path)?;
    let mut manager = manager_from_plan(&plan)?;
    validate_materialized_target(&plan, &manager)?;

    let _worker_guard =
        UpdateWorkerGuard::take_over(&plan.install_dir, &plan.app_id, &plan.latest.version, plan.updater_pid)?;
    if let Err(error) = write_handshake_marker(&plan.ready_path(), &plan.operation_id) {
        if let Err(status_error) = persist_external_failure(&plan, &error, None) {
            return Err(SurgeError::Update(format!(
                "External finalizer failed before becoming ready: {error}; terminal status persistence also failed: {status_error}"
            )));
        }
        cleanup_operation(&plan, "failed");
        return Err(error);
    }

    if let Err(error) = wait_until_armed(&plan).await {
        if let Err(status_error) = persist_external_failure(&plan, &error, None) {
            return Err(SurgeError::Update(format!(
                "External finalizer failed before it was armed: {error}; terminal status persistence also failed: {status_error}"
            )));
        }
        cleanup_operation(&plan, "failed");
        return Err(error);
    }
    if let Err(error) = write_handshake_marker(&plan.accepted_path(), &plan.operation_id) {
        if let Err(status_error) = persist_external_failure(&plan, &error, None) {
            return Err(SurgeError::Update(format!(
                "External finalizer failed before accepting ownership: {error}; terminal status persistence also failed: {status_error}"
            )));
        }
        cleanup_operation(&plan, "failed");
        return Err(error);
    }

    let mut recovery_required = false;
    let result = run_armed_finalize(&plan, &mut manager, &mut quiesce_updater, &mut recovery_required).await;
    match result {
        Ok(outcome) => {
            persist_external_success(&plan, &outcome)?;
            cleanup_operation(&plan, "completed");
            Ok(())
        }
        Err(error) => {
            let recovery_error = if recovery_required {
                recover_previous_runtime(&plan, &manager, &mut quiesce_updater)
                    .await
                    .err()
            } else {
                None
            };
            let _ = std::fs::remove_file(plan.armed_path());
            let _ = std::fs::remove_file(plan.ready_path());
            if let Err(status_error) = persist_external_failure(&plan, &error, recovery_error.as_ref()) {
                return Err(SurgeError::Update(format!(
                    "External finalization failed: {error}; terminal status persistence also failed: {status_error}"
                )));
            }
            cleanup_operation(&plan, "failed");
            if let Some(recovery_error) = recovery_error {
                return Err(SurgeError::Update(format!(
                    "External finalization failed: {error}; previous runtime recovery also failed: {recovery_error}"
                )));
            }
            Err(error)
        }
    }
}

fn cleanup_operation(plan: &ExternalFinalizePlan, outcome: &str) {
    if let Err(error) = std::fs::remove_dir_all(plan.operation_dir()) {
        warn!(error = %error, outcome, "Failed to remove terminal external finalizer plan directory");
    }
}

async fn run_armed_finalize<F>(
    plan: &ExternalFinalizePlan,
    manager: &mut UpdateManager,
    quiesce_updater: &mut F,
    recovery_required: &mut bool,
) -> Result<SupervisorRestartOutcome>
where
    F: FnMut(u32, &Path) -> Result<()>,
{
    persist_external_phase(plan, update_phase::STOPPING_SUPERVISOR);
    super::super::lifecycle::request_supervisor_shutdown(
        &plan.install_dir,
        &plan.current_release_identity.supervisor_id,
    )
    .await?;
    *recovery_required = true;

    persist_external_phase(plan, update_phase::WAITING_FOR_UPDATER_EXIT);
    quiesce_updater(plan.updater_pid, &plan.updater_exe)?;

    let installed_identity = current_install::load(manager)?;
    if installed_identity.as_ref() != Some(&plan.current_release_identity) {
        return Err(SurgeError::Update(
            "Installed application identity changed while the external finalizer waited for the updater to exit"
                .to_string(),
        ));
    }
    manager.current_release_identity = installed_identity;

    let info = UpdateInfo {
        available_releases: vec![plan.latest.clone()],
        latest_version: plan.latest.version.clone(),
        delta_available: false,
        download_size: 0,
        apply_releases: vec![plan.latest.clone()],
        apply_strategy: ApplyStrategy::Full,
        fallback_reason: None,
    };
    let progress: Option<Arc<fn(ProgressInfo)>> = None;
    let progress_emitter = PhaseProgressEmitter {
        progress: progress.as_ref(),
        install_dir: &plan.install_dir,
        in_progress_template: &plan.in_progress_template,
    };
    finalize_update(
        manager,
        &info,
        &plan.extracted_final_dir(),
        &plan.staging_dir(),
        &plan.artifact_cache_dir(),
        &progress_emitter,
    )
    .await
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

fn persist_external_success(plan: &ExternalFinalizePlan, outcome: &SupervisorRestartOutcome) -> Result<()> {
    let completed_at_utc = status::now_utc_rfc3339();
    let attempted_at_utc = plan
        .in_progress_template
        .attempted_at_utc
        .clone()
        .unwrap_or_else(status::now_utc_rfc3339);
    let record = match outcome {
        SupervisorRestartOutcome::NotApplicable => UpdateStatusRecord::converged(
            &plan.app_id,
            &plan.latest.version,
            &plan.channel,
            Some(attempted_at_utc),
            completed_at_utc,
            false,
        ),
        SupervisorRestartOutcome::PendingRestart { reason, failure_phase } => {
            UpdateStatusRecord::pending_restart_with_failure_phase(
                &plan.app_id,
                &plan.latest.version,
                &plan.latest.version,
                &plan.channel,
                attempted_at_utc,
                completed_at_utc,
                reason,
                failure_phase,
            )
        }
        SupervisorRestartOutcome::ExternalFinalizeScheduled => {
            return Err(SurgeError::Update(
                "External finalizer unexpectedly scheduled another external finalizer".to_string(),
            ));
        }
    };
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
        &plan.latest.version,
        &plan.channel,
        attempted_at_utc,
        &reason,
        FailureContext::from_record(status_context.as_ref(), recovery_error.is_none()),
    );
    let record = if recovery_error.is_none() {
        let schedule = status::retry_schedule(plan.previous_attempt_status.as_ref(), &plan.latest.version);
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

async fn recover_previous_runtime<F>(
    plan: &ExternalFinalizePlan,
    manager: &UpdateManager,
    quiesce_updater: &mut F,
) -> Result<()>
where
    F: FnMut(u32, &Path) -> Result<()>,
{
    if plan.latest.supervisor_id != plan.current_release_identity.supervisor_id {
        super::super::lifecycle::request_supervisor_shutdown(&plan.install_dir, &plan.latest.supervisor_id).await?;
    }

    quiesce_updater(plan.updater_pid, &plan.updater_exe).map_err(|error| {
        SurgeError::Supervisor(format!(
            "Refusing to restart the previous runtime because application quiescence could not be confirmed: {error}"
        ))
    })?;

    let current_app_dir = restore_previous_app_dir(
        manager,
        &plan.current_app_dir,
        &plan.current_release_identity,
        &plan.operation_id,
    )?;

    if !current_app_dir.join(&plan.current_release_identity.main_exe).is_file() {
        return Err(SurgeError::Update(format!(
            "Previous application executable is unavailable at '{}'",
            current_app_dir.join(&plan.current_release_identity.main_exe).display()
        )));
    }

    let current_release = release_from_identity(&plan.current_release_identity);
    if current_release.supervisor_id.trim().is_empty() {
        let executable = current_app_dir.join(&current_release.main_exe);
        let args: [&str; 0] = [];
        let _ = spawn_detached(
            &executable,
            &args,
            Some(&plan.install_dir),
            &current_release.environment,
        )?;
        return Ok(());
    }

    match super::super::lifecycle::restart_supervisor_after_update_with_pid(
        &plan.install_dir,
        &current_app_dir,
        &current_release,
        current_pid(),
    ) {
        SupervisorRestartOutcome::PendingRestart { failure_phase, reason }
            if failure_phase == status::RESTART_HANDOFF_FAILED_PHASE =>
        {
            Err(SurgeError::Supervisor(reason))
        }
        SupervisorRestartOutcome::NotApplicable => Err(SurgeError::Supervisor(
            "Previous release supervisor was not restarted".to_string(),
        )),
        SupervisorRestartOutcome::PendingRestart { .. } => Ok(()),
        SupervisorRestartOutcome::ExternalFinalizeScheduled => Err(SurgeError::Supervisor(
            "Previous release recovery unexpectedly scheduled external finalization".to_string(),
        )),
    }
}

fn restore_previous_app_dir(
    manager: &UpdateManager,
    current_app_dir: &Path,
    current_identity: &super::super::current_install::ReleaseIdentity,
    operation_id: &str,
) -> Result<PathBuf> {
    let active_app_dir = manager.install_dir.join("app");
    let previous_swap_dir = manager.install_dir.join(".surge-app-prev");
    let previous_swap_matches = previous_swap_dir.is_dir()
        && current_install::load_previous_swap(manager, &previous_swap_dir)?.as_ref() == Some(current_identity);
    if previous_swap_matches {
        replace_failed_target(&active_app_dir, &previous_swap_dir, operation_id)?;
        return Ok(active_app_dir);
    }

    if current_app_dir != active_app_dir && current_app_dir.is_dir() {
        if current_install::load_previous_swap(manager, current_app_dir)?.as_ref() != Some(current_identity) {
            return Err(SurgeError::Update(
                "Previous application directory no longer matches the external finalizer plan".to_string(),
            ));
        }
        if active_app_dir.exists() {
            replace_failed_target(&active_app_dir, current_app_dir, operation_id)?;
            return Ok(active_app_dir);
        }
        return Ok(current_app_dir.to_path_buf());
    }

    if active_app_dir.is_dir()
        && current_install::load_previous_swap(manager, &active_app_dir)?.as_ref() == Some(current_identity)
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

fn release_from_identity(identity: &super::super::current_install::ReleaseIdentity) -> ReleaseEntry {
    ReleaseEntry {
        version: identity.version.clone(),
        main_exe: identity.main_exe.clone(),
        supervisor_id: identity.supervisor_id.clone(),
        environment: identity.environment.clone(),
        ..ReleaseEntry::default()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use crate::context::{Context, StorageProvider};
    use crate::install::{InstallProfile, RuntimeManifestMetadata, write_runtime_manifest};
    use crate::releases::manifest::ReleaseIndex;

    use super::*;

    #[test]
    fn rollback_restores_previous_app_and_removes_failed_target() {
        let temp = tempfile::tempdir().unwrap();
        let (manager, identity) = fixture_manager(&temp);
        let active = manager.install_dir.join("app");
        let previous = manager.install_dir.join(".surge-app-prev");
        write_app(&active, "2.0.0", "target");
        write_app(&previous, "1.0.0", "previous");

        assert_eq!(
            restore_previous_app_dir(&manager, &active, &identity, "operation").unwrap(),
            active
        );

        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "previous");
        assert!(!previous.exists());
        assert!(!manager.install_dir.join(".surge-app-failed-operation").exists());
    }

    #[test]
    fn rollback_is_noop_before_directory_swap() {
        let temp = tempfile::tempdir().unwrap();
        let (manager, identity) = fixture_manager(&temp);
        let active = manager.install_dir.join("app");
        write_app(&active, "1.0.0", "previous");

        assert_eq!(
            restore_previous_app_dir(&manager, &active, &identity, "operation").unwrap(),
            active
        );
        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "previous");
    }

    #[test]
    fn rollback_ignores_stale_previous_swap_before_new_swap() {
        let temp = tempfile::tempdir().unwrap();
        let (manager, identity) = fixture_manager(&temp);
        let active = manager.install_dir.join("app");
        let stale = manager.install_dir.join(".surge-app-prev");
        write_app(&active, "1.0.0", "current");
        write_app(&stale, "0.9.0", "stale");

        assert_eq!(
            restore_previous_app_dir(&manager, &active, &identity, "operation").unwrap(),
            active
        );
        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "current");
        assert_eq!(std::fs::read_to_string(stale.join("version")).unwrap(), "stale");
    }

    #[test]
    fn rollback_restores_legacy_versioned_app_after_target_activation() {
        let temp = tempfile::tempdir().unwrap();
        let (manager, identity) = fixture_manager(&temp);
        let active = manager.install_dir.join("app");
        let legacy = manager.install_dir.join("app-1.0.0");
        write_app(&active, "2.0.0", "target");
        write_app(&legacy, "1.0.0", "previous");
        assert_eq!(
            restore_previous_app_dir(&manager, &legacy, &identity, "operation").unwrap(),
            active
        );
        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "previous");
    }

    #[test]
    fn terminal_status_write_retries_transient_failure() {
        let temp = tempfile::tempdir().unwrap();
        let attempts = Cell::new(0);
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
        let attempts = Cell::new(0);
        let record = UpdateStatusRecord::converged("demo", "2.0.0", "test", None, status::now_utc_rfc3339(), true);

        let error = write_terminal_status_with(temp.path(), &record, 3, Duration::ZERO, |_, _| {
            attempts.set(attempts.get() + 1);
            Err(SurgeError::Update("persistent write failure".to_string()))
        })
        .unwrap_err();

        assert_eq!(attempts.get(), 3);
        assert!(error.to_string().contains("after 3 attempts"));
    }

    #[tokio::test]
    async fn recovery_refuses_to_restart_without_confirmed_application_quiescence() {
        let temp = tempfile::tempdir().unwrap();
        let (mut manager, identity) = fixture_manager(&temp);
        let active = manager.install_dir.join("app");
        write_app(&active, "1.0.0", "previous");
        manager.current_release_identity = Some(identity.clone());
        let latest = ReleaseEntry {
            version: "2.0.0".to_string(),
            main_exe: "demo".to_string(),
            supervisor_id: identity.supervisor_id.clone(),
            ..ReleaseEntry::default()
        };
        manager.cached_index = Some(ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![latest.clone()],
            ..ReleaseIndex::default()
        });
        let in_progress = UpdateStatusRecord::in_progress("demo", "1.0.0", "2.0.0", "test", status::now_utc_rfc3339());
        let plan = ExternalFinalizePlan::from_manager(
            &manager,
            &latest,
            active.join("demo"),
            active.clone(),
            &in_progress,
            None,
        )
        .unwrap();
        let attempts = Cell::new(0);
        let mut reject_quiescence = |pid, executable: &Path| {
            attempts.set(attempts.get() + 1);
            assert_eq!(pid, plan.updater_pid);
            assert_eq!(executable, plan.updater_exe);
            Err(SurgeError::Supervisor("matching process remains".to_string()))
        };

        let error = recover_previous_runtime(&plan, &manager, &mut reject_quiescence)
            .await
            .unwrap_err();

        assert_eq!(attempts.get(), 1);
        assert!(error.to_string().contains("quiescence could not be confirmed"));
        assert_eq!(std::fs::read_to_string(active.join("version")).unwrap(), "previous");
    }

    fn fixture_manager(
        temp: &tempfile::TempDir,
    ) -> (UpdateManager, super::super::super::current_install::ReleaseIdentity) {
        let store = temp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();
        let context = Arc::new(Context::new());
        context.set_storage(StorageProvider::Filesystem, store.to_str().unwrap(), "", "", "", "");
        let manager = UpdateManager::new(context, "demo", "1.0.0", "test", temp.path().to_str().unwrap()).unwrap();
        let identity = super::super::super::current_install::ReleaseIdentity {
            version: "1.0.0".to_string(),
            main_exe: "demo".to_string(),
            supervisor_id: "demo-supervisor".to_string(),
            environment: BTreeMap::new(),
        };
        (manager, identity)
    }

    fn write_app(app_dir: &Path, version: &str, marker: &str) {
        std::fs::create_dir_all(app_dir).unwrap();
        std::fs::write(app_dir.join("demo"), "fixture").unwrap();
        std::fs::write(app_dir.join("version"), marker).unwrap();
        let environment = BTreeMap::new();
        let profile = InstallProfile::new(
            "demo",
            "Demo",
            "demo",
            "demo",
            "demo-supervisor",
            "",
            &[],
            &[],
            &environment,
        );
        let metadata = RuntimeManifestMetadata::new(version, "test", "filesystem", ".", "", "");
        write_runtime_manifest(app_dir, &profile, &metadata).unwrap();
    }
}
