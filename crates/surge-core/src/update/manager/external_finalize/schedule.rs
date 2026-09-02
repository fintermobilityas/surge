use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::info;

use crate::error::{Result, SurgeError};
use crate::platform::fs::{atomic_rename, make_executable, write_file_atomic};
use crate::platform::process::{spawn_detached, supervisor_binary_name};
use crate::update::status::UpdateStatusRecord;

use super::super::progress::ProgressInfo;
use super::super::progress_substep::{PhaseProgressEmitter, labels as update_phase};
use super::super::{UpdateInfo, UpdateManager, apply};
use super::plan::{ExternalFinalizePlan, write_plan};

const EXTERNAL_TOOLS_DIR: &str = ".surge-tools";
const HELPER_READY_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_POLL_INTERVAL: Duration = Duration::from_millis(50);

struct SelfHostedUpdater {
    executable: PathBuf,
    active_app_dir: PathBuf,
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
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
    if latest.supervisor_id.trim().is_empty() {
        return Err(SurgeError::Update(
            "A self-hosted update requires a supervised target release so finalization can run outside the active application"
                .to_string(),
        ));
    }

    let plan = ExternalFinalizePlan::from_manager(
        manager,
        latest,
        updater.executable,
        updater.active_app_dir.clone(),
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
    if plan.operations_dir().exists() {
        std::fs::remove_dir_all(plan.operations_dir())?;
    }
    std::fs::create_dir_all(plan.operation_dir())?;

    let current_helper = updater.active_app_dir.join(supervisor_binary_name());
    let helper_path = install_stable_helper(&plan, &current_helper)?;
    write_plan(&plan)?;

    let plan_arg = plan.plan_path().to_string_lossy().into_owned();
    let args = ["finalize-update", "--plan", plan_arg.as_str()];
    let environment = BTreeMap::new();
    let mut helper = match spawn_detached(&helper_path, &args, Some(&manager.install_dir), &environment) {
        Ok(helper) => helper,
        Err(error) => {
            let _ = std::fs::remove_dir_all(plan.operation_dir());
            return Err(error);
        }
    };

    if let Err(error) = wait_for_helper_ready(&plan, &mut helper).await {
        let _ = helper.kill();
        let _ = helper.wait();
        let _ = std::fs::remove_dir_all(plan.operation_dir());
        return Err(error);
    }

    if let Err(error) = write_handshake_marker(&plan.armed_path(), &plan.operation_id) {
        let _ = helper.kill();
        let _ = helper.wait();
        let _ = std::fs::remove_dir_all(plan.operation_dir());
        return Err(error);
    }
    if let Err(error) = wait_for_helper_accepted(&plan, &mut helper).await {
        let _ = helper.kill();
        let _ = helper.wait();
        let _ = std::fs::remove_dir_all(plan.operation_dir());
        return Err(error);
    }

    progress_emitter.emit_substep(6, update_phase::WAITING_FOR_UPDATER_EXIT, 91);
    info!(
        target_version = %plan.latest.version,
        helper = %helper_path.display(),
        "Transferred update finalization to stable helper"
    );
    Ok(true)
}

async fn wait_for_helper_accepted(
    plan: &ExternalFinalizePlan,
    helper: &mut crate::platform::process::ProcessHandle,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + HELPER_READY_TIMEOUT;
    loop {
        if marker_matches(&plan.accepted_path(), &plan.operation_id)? {
            return Ok(());
        }
        if !helper.poll_running() {
            let result = helper.wait()?;
            return Err(SurgeError::Update(format!(
                "External finalizer helper exited before accepting ownership with code {}",
                result.exit_code
            )));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "Timed out after {}s waiting for the external finalizer helper to accept ownership",
                HELPER_READY_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(HANDSHAKE_POLL_INTERVAL).await;
    }
}

fn self_hosted_updater(manager: &UpdateManager) -> Result<Option<SelfHostedUpdater>> {
    let Some(identity) = manager.current_release_identity.as_ref() else {
        return Ok(None);
    };
    let Some(current_app_dir) = apply::find_previous_app_dir(&manager.install_dir, &manager.current_version) else {
        return Ok(None);
    };
    let active_exe = resolve_active_executable(&current_app_dir, &identity.main_exe)?;
    let updater_exe = std::fs::canonicalize(std::env::current_exe()?).map_err(|error| {
        SurgeError::Platform(format!(
            "Failed to resolve the updating process executable before external finalization: {error}"
        ))
    })?;

    if same_executable(&active_exe, &updater_exe)? {
        Ok(Some(SelfHostedUpdater {
            executable: updater_exe,
            active_app_dir: current_app_dir,
        }))
    } else {
        Ok(None)
    }
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
            "Active application executable '{}' resolves outside the active application directory; refusing to signal a shared executable",
            unresolved_executable.display()
        )));
    }
    Ok(active_exe)
}

#[cfg(unix)]
fn same_executable(left: &Path, right: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let left = std::fs::metadata(left)?;
    let right = std::fs::metadata(right)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(windows)]
fn same_executable(left: &Path, right: &Path) -> Result<bool> {
    Ok(left.to_string_lossy().eq_ignore_ascii_case(&right.to_string_lossy()))
}

#[cfg(not(any(unix, windows)))]
fn same_executable(left: &Path, right: &Path) -> Result<bool> {
    Ok(left == right)
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
    let temporary_path = tools_dir.join(format!(".{}-{}.next", supervisor_binary_name(), plan.operation_id));
    if temporary_path.exists() {
        std::fs::remove_file(&temporary_path)?;
    }
    let copied = std::fs::copy(current_helper, &temporary_path)?;
    let expected = std::fs::metadata(current_helper)?.len();
    if copied != expected {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(SurgeError::Io(std::io::Error::new(
            std::io::ErrorKind::WriteZero,
            format!("Copied {copied} of {expected} helper bytes"),
        )));
    }
    make_executable(&temporary_path)?;
    if helper_path.exists() {
        std::fs::remove_file(&helper_path)?;
    }
    atomic_rename(&temporary_path, &helper_path)?;
    Ok(helper_path)
}

async fn wait_for_helper_ready(
    plan: &ExternalFinalizePlan,
    helper: &mut crate::platform::process::ProcessHandle,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + HELPER_READY_TIMEOUT;
    loop {
        if marker_matches(&plan.ready_path(), &plan.operation_id)? {
            return Ok(());
        }
        if !helper.poll_running() {
            let result = helper.wait()?;
            return Err(SurgeError::Update(format!(
                "External finalizer helper exited before becoming ready with code {}",
                result.exit_code
            )));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "Timed out after {}s waiting for the external finalizer helper to become ready",
                HELPER_READY_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(HANDSHAKE_POLL_INTERVAL).await;
    }
}

pub(super) fn write_handshake_marker(path: &Path, operation_id: &str) -> Result<()> {
    write_file_atomic(path, operation_id.as_bytes())
}

pub(super) fn marker_matches(path: &Path, operation_id: &str) -> Result<bool> {
    if !path.is_file() {
        return Ok(false);
    }
    Ok(std::fs::read(path)? == operation_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn executable_identity_accepts_hard_linked_active_path() {
        let temp = tempfile::tempdir().unwrap();
        let current = std::env::current_exe().unwrap();
        let active_root = temp.path().join("app");
        std::fs::create_dir(&active_root).unwrap();
        let active = active_root.join("active-app");
        std::fs::hard_link(&current, &active).unwrap();

        let resolved = resolve_active_executable(&active_root, "active-app").unwrap();
        assert!(same_executable(&current, &resolved).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn active_executable_symlink_cannot_escape_application_directory() {
        let temp = tempfile::tempdir().unwrap();
        let active_root = temp.path().join("app");
        std::fs::create_dir(&active_root).unwrap();
        let shared_executable = temp.path().join("shared-app");
        std::fs::write(&shared_executable, "shared").unwrap();
        std::os::unix::fs::symlink(&shared_executable, active_root.join("demo")).unwrap();

        let error = resolve_active_executable(&active_root, "demo").unwrap_err();
        assert!(error.to_string().contains("resolves outside"));
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
