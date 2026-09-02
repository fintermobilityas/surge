use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::info;

use crate::error::{Result, SurgeError};
use crate::platform::fs::{atomic_rename, make_executable, write_file_atomic};
use crate::platform::process::{process_start_time, spawn_detached, supervisor_binary_name};
use crate::supervisor::state::read_restart_args;
use crate::update::status::UpdateStatusRecord;

use super::super::progress::ProgressInfo;
use super::super::progress_substep::{PhaseProgressEmitter, labels as update_phase};
use super::super::{UpdateInfo, UpdateManager};
use super::plan::{ExternalFinalizePlan, write_plan};

const EXTERNAL_TOOLS_DIR: &str = ".surge-tools";
const HELPER_READY_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_POLL_INTERVAL: Duration = Duration::from_millis(50);

struct SelfHostedUpdater {
    executable: PathBuf,
    start_time: u64,
}

pub(in crate::update::manager) async fn schedule_if_required<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    extracted_final_dir: &Path,
    in_progress_template: &UpdateStatusRecord,
    previous_attempt_status: Option<UpdateStatusRecord>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<bool>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    if manager.allow_in_process_swap {
        return Ok(false);
    }

    let Some(updater) = self_hosted_updater(manager)? else {
        return Ok(false);
    };
    let target_release = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
    let current_identity = manager.current_release_identity.as_ref().ok_or_else(|| {
        SurgeError::Update("External finalization requires the installed application identity".to_string())
    })?;
    if current_identity.supervisor_id.trim().is_empty() || target_release.supervisor_id.trim().is_empty() {
        return Err(SurgeError::Update(
            "A self-hosted update requires supervised current and target releases".to_string(),
        ));
    }
    let restart_args = read_restart_args(&manager.install_dir, &current_identity.supervisor_id)?;
    let plan = ExternalFinalizePlan::from_manager(
        manager,
        target_release,
        updater.executable,
        updater.start_time,
        restart_args,
        in_progress_template,
        previous_attempt_status,
    )?;
    if extracted_final_dir != plan.extracted_final_dir() {
        return Err(SurgeError::Update(format!(
            "External finalizer expected materialized payload at '{}', found '{}'",
            plan.extracted_final_dir().display(),
            extracted_final_dir.display()
        )));
    }

    progress_emitter.emit_substep(6, update_phase::STARTING_EXTERNAL_FINALIZER, 90);
    std::fs::create_dir_all(plan.operation_dir())?;

    let current_helper = plan.active_app_dir().join(supervisor_binary_name());
    let helper_path = install_stable_helper(&plan, &current_helper)?;
    write_plan(&plan)?;

    let plan_arg = plan.plan_path().to_string_lossy().into_owned();
    let args = ["finalize-update", "--plan", plan_arg.as_str()];
    let mut helper = match spawn_detached(&helper_path, &args, Some(&manager.install_dir), &BTreeMap::new()) {
        Ok(helper) => helper,
        Err(error) => {
            cleanup_unaccepted_operation(&plan);
            return Err(error);
        }
    };

    if let Err(error) = wait_for_marker(&plan, &mut helper, HandshakeStage::Ready).await {
        let _ = helper.kill();
        let _ = helper.wait();
        cleanup_unaccepted_operation(&plan);
        return Err(error);
    }
    if let Err(error) = write_handshake_marker(&plan.armed_path(), &plan.operation_id) {
        let _ = helper.kill();
        let _ = helper.wait();
        cleanup_unaccepted_operation(&plan);
        return Err(error);
    }
    if let Err(error) = wait_for_marker(&plan, &mut helper, HandshakeStage::Accepted).await {
        let _ = helper.kill();
        let _ = helper.wait();
        cleanup_unaccepted_operation(&plan);
        return Err(error);
    }

    progress_emitter.emit_substep(6, update_phase::WAITING_FOR_UPDATER_EXIT, 91);
    info!(
        target_version = %plan.target_release.version,
        helper = %helper_path.display(),
        "Transferred update finalization to stable helper"
    );
    Ok(true)
}

#[derive(Clone, Copy)]
enum HandshakeStage {
    Ready,
    Accepted,
}

impl HandshakeStage {
    fn path(self, plan: &ExternalFinalizePlan) -> PathBuf {
        match self {
            Self::Ready => plan.ready_path(),
            Self::Accepted => plan.accepted_path(),
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Ready => "become ready",
            Self::Accepted => "accept ownership",
        }
    }
}

async fn wait_for_marker(
    plan: &ExternalFinalizePlan,
    helper: &mut crate::platform::process::ProcessHandle,
    stage: HandshakeStage,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + HELPER_READY_TIMEOUT;
    loop {
        if marker_matches(&stage.path(plan), &plan.operation_id)? {
            return Ok(());
        }
        if !helper.poll_running() {
            let result = helper.wait()?;
            return Err(SurgeError::Update(format!(
                "External finalizer helper exited before it could {} (code {})",
                stage.description(),
                result.exit_code
            )));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "Timed out after {}s waiting for the external finalizer helper to {}",
                HELPER_READY_TIMEOUT.as_secs(),
                stage.description()
            )));
        }
        tokio::time::sleep(HANDSHAKE_POLL_INTERVAL).await;
    }
}

fn self_hosted_updater(manager: &UpdateManager) -> Result<Option<SelfHostedUpdater>> {
    let Some(identity) = manager.current_release_identity.as_ref() else {
        return Ok(None);
    };
    let active_app_dir = manager.install_dir.join("app");
    if !active_app_dir.is_dir() {
        return Ok(None);
    }
    let active_exe = resolve_active_executable(&active_app_dir, &identity.main_exe)?;
    let updater_exe = std::fs::canonicalize(std::env::current_exe()?).map_err(|error| {
        SurgeError::Platform(format!(
            "Failed to resolve the updating process executable before external finalization: {error}"
        ))
    })?;
    if !same_executable(&active_exe, &updater_exe) {
        return Ok(None);
    }
    let start_time = process_start_time(std::process::id())
        .ok_or_else(|| SurgeError::Platform("Could not capture exact identity for the updating process".to_string()))?;
    Ok(Some(SelfHostedUpdater {
        executable: updater_exe,
        start_time,
    }))
}

fn resolve_active_executable(active_app_dir: &Path, main_exe: &str) -> Result<PathBuf> {
    let active_app_root = std::fs::canonicalize(active_app_dir).map_err(|error| {
        SurgeError::Platform(format!(
            "Failed to resolve the active application directory before external finalization: {error}"
        ))
    })?;
    let unresolved_executable = active_app_root.join(main_exe);
    let active_exe = std::fs::canonicalize(&unresolved_executable).map_err(|error| {
        SurgeError::Platform(format!(
            "Failed to resolve the active application executable before external finalization: {error}"
        ))
    })?;
    if !active_exe.starts_with(&active_app_root) {
        return Err(SurgeError::Platform(format!(
            "Active application executable '{}' resolves outside the active application directory",
            unresolved_executable.display()
        )));
    }
    Ok(active_exe)
}

#[cfg(windows)]
fn same_executable(left: &Path, right: &Path) -> bool {
    left.to_string_lossy().eq_ignore_ascii_case(&right.to_string_lossy())
}

#[cfg(not(windows))]
fn same_executable(left: &Path, right: &Path) -> bool {
    left == right
}

fn install_stable_helper(plan: &ExternalFinalizePlan, current_helper: &Path) -> Result<PathBuf> {
    if !current_helper.is_file() {
        return Err(SurgeError::Update(format!(
            "Current release does not contain the external finalizer helper at '{}'",
            current_helper.display()
        )));
    }

    let tools_dir = plan.install_dir.join(EXTERNAL_TOOLS_DIR);
    std::fs::create_dir_all(&tools_dir)?;
    let helper_path = tools_dir.join(supervisor_binary_name());
    let temporary_path = tools_dir.join(format!("{}.{}.tmp", supervisor_binary_name(), plan.operation_id));
    let copied = std::fs::copy(current_helper, &temporary_path)?;
    let expected = std::fs::metadata(current_helper)?.len();
    if copied != expected {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(SurgeError::Update(format!(
            "Copied {copied} of {expected} helper bytes"
        )));
    }
    make_executable(&temporary_path)?;
    if helper_path.exists() {
        std::fs::remove_file(&helper_path)?;
    }
    atomic_rename(&temporary_path, &helper_path)?;
    Ok(helper_path)
}

fn cleanup_unaccepted_operation(plan: &ExternalFinalizePlan) {
    let _ = std::fs::remove_dir_all(plan.operation_dir());
}

pub(super) fn write_handshake_marker(path: &Path, operation_id: &str) -> Result<()> {
    write_file_atomic(path, operation_id.as_bytes())
}

pub(super) fn marker_matches(path: &Path, operation_id: &str) -> Result<bool> {
    match std::fs::read_to_string(path) {
        Ok(value) => Ok(value.trim() == operation_id),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn active_executable_symlink_cannot_escape_application_directory() {
        let temp = tempfile::tempdir().unwrap();
        let app = temp.path().join("app");
        let external = temp.path().join("external");
        std::fs::create_dir_all(&app).unwrap();
        std::fs::write(&external, "fixture").unwrap();

        std::os::unix::fs::symlink(&external, app.join("demo")).unwrap();

        let error = resolve_active_executable(&app, "demo").unwrap_err();
        assert!(error.to_string().contains("outside the active application directory"));
    }

    #[test]
    fn handshake_marker_requires_matching_operation() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("ready");
        write_handshake_marker(&marker, "operation-a").unwrap();

        assert!(marker_matches(&marker, "operation-a").unwrap());
        assert!(!marker_matches(&marker, "operation-b").unwrap());
    }
}
