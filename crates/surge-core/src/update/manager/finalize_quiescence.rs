use std::collections::BTreeMap;
use std::path::Path;

use tracing::warn;

use crate::error::{Result, SurgeError};
use crate::platform::process::ProcessIdentity;

use super::UpdateManager;
use super::lifecycle::{self, SupervisorRestartOutcome};

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_and_quiesce_active_app_before_swap(
    install_dir: &Path,
    current_app_dir: &Path,
    current_version: &str,
    current_main_exe: &str,
    current_supervisor_id: &str,
    current_environment: Option<&BTreeMap<String, String>>,
    supervisor_requires_restoration: bool,
    supervised_child_identity: Option<ProcessIdentity>,
    allow_in_process_swap: bool,
) -> Result<Option<lifecycle::PreparedAppQuiescence>> {
    let result = (|| {
        let prepared = lifecycle::prepare_app_quiescence(
            current_app_dir,
            current_main_exe,
            supervised_child_identity,
            allow_in_process_swap,
        )?;
        if let Some(prepared) = &prepared {
            lifecycle::terminate_prepared_app_processes(prepared)?;
        }
        Ok(prepared)
    })();
    let Err(error) = result else {
        return result;
    };

    Err(restore_previous_supervisor_after_quiescence_failure(
        install_dir,
        Some(current_app_dir),
        current_version,
        current_main_exe,
        current_supervisor_id,
        current_environment,
        supervisor_requires_restoration,
        supervised_child_identity,
        error,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn restore_previous_supervisor_after_quiescence_failure(
    install_dir: &Path,
    current_app_dir: Option<&Path>,
    current_version: &str,
    current_main_exe: &str,
    current_supervisor_id: &str,
    current_environment: Option<&BTreeMap<String, String>>,
    supervisor_requires_restoration: bool,
    supervised_child_identity: Option<ProcessIdentity>,
    error: SurgeError,
) -> SurgeError {
    if supervisor_requires_restoration
        && let Some(current_app_dir) = current_app_dir
        && let Some(current_environment) = current_environment
    {
        request_previous_supervisor_restoration(
            install_dir,
            current_app_dir,
            current_version,
            current_main_exe,
            current_supervisor_id,
            current_environment,
            supervised_child_identity,
        );
    }

    error
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_and_quiesce_previous_swap_before_reuse(
    manager: &UpdateManager,
    previous_swap_dir: &Path,
    current_app_dir: Option<&Path>,
    current_version: &str,
    current_main_exe: &str,
    current_supervisor_id: &str,
    current_environment: Option<&BTreeMap<String, String>>,
    supervisor_requires_restoration: bool,
    supervised_child_identity: Option<ProcessIdentity>,
) -> Result<Option<(lifecycle::PreparedAppQuiescence, String)>> {
    let restore_current = |error| {
        restore_previous_supervisor_after_quiescence_failure(
            &manager.install_dir,
            current_app_dir,
            current_version,
            current_main_exe,
            current_supervisor_id,
            current_environment,
            supervisor_requires_restoration,
            supervised_child_identity,
            error,
        )
    };
    let previous_swap_identity = match super::current_install::load_previous_swap(manager, previous_swap_dir) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            return Err(restore_current(SurgeError::Update(
                "Previous swap directory has no persisted process identity; refusing to delete or reuse it".to_string(),
            )));
        }
        Err(error) => return Err(restore_current(error)),
    };
    let previous_supervised_child_identity =
        match lifecycle::request_supervisor_shutdown(&manager.install_dir, &previous_swap_identity.supervisor_id).await
        {
            Ok(pid) => pid,
            Err(error) => return Err(restore_current(error)),
        };
    let result = (|| {
        let prepared = lifecycle::prepare_app_quiescence(
            previous_swap_dir,
            &previous_swap_identity.main_exe,
            previous_supervised_child_identity,
            manager.allow_in_process_swap,
        )?
        .ok_or_else(|| {
            SurgeError::Update(
                "Previous swap application entrypoint disappeared before it could be quiesced".to_string(),
            )
        })?;
        lifecycle::terminate_prepared_app_processes(&prepared)?;
        Ok::<_, SurgeError>(prepared)
    })();

    match result {
        Ok(prepared) => Ok(Some((prepared, previous_swap_identity.main_exe))),
        Err(error) => {
            if previous_supervised_child_identity.is_some()
                && (!supervisor_requires_restoration || previous_swap_identity.supervisor_id != current_supervisor_id)
            {
                request_previous_supervisor_restoration(
                    &manager.install_dir,
                    previous_swap_dir,
                    &previous_swap_identity.version,
                    &previous_swap_identity.main_exe,
                    &previous_swap_identity.supervisor_id,
                    &previous_swap_identity.environment,
                    previous_supervised_child_identity,
                );
            } else if previous_supervised_child_identity.is_some() {
                warn!(
                    supervisor_id = current_supervisor_id,
                    "Cannot restore both current and previous-swap children through the same supervisor identity"
                );
            }
            Err(restore_current(error))
        }
    }
}

fn request_previous_supervisor_restoration(
    install_dir: &Path,
    current_app_dir: &Path,
    current_version: &str,
    current_main_exe: &str,
    current_supervisor_id: &str,
    current_environment: &BTreeMap<String, String>,
    supervised_child_identity: Option<ProcessIdentity>,
) {
    let Some(supervised_child_identity) = supervised_child_identity else {
        warn!(
            supervisor_id = current_supervisor_id,
            "Cannot safely restore the previous supervisor because it did not provide its surviving child PID"
        );
        return;
    };
    let restore_outcome = lifecycle::restore_previous_supervisor_after_failed_quiescence(
        install_dir,
        current_app_dir,
        current_version,
        current_main_exe,
        current_supervisor_id,
        current_environment,
        supervised_child_identity,
    );
    match restore_outcome {
        SupervisorRestartOutcome::NotApplicable => {
            warn!(
                supervisor_id = current_supervisor_id,
                "Previous supervisor could not be restored after application quiescence failed"
            );
        }
        SupervisorRestartOutcome::PendingRestart { reason, failure_phase } => {
            warn!(
                supervisor_id = current_supervisor_id,
                reason, failure_phase, "Requested previous supervisor restoration after application quiescence failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    use crate::supervisor::state::{
        SupervisorTakeoverAcknowledgement, SupervisorTakeoverCommit, SupervisorTakeoverInstance,
        SupervisorTakeoverRequest, accept_supervisor_takeover_request, supervisor_pid_file,
        write_supervisor_takeover_acknowledgement, write_supervisor_takeover_commit, write_supervisor_takeover_request,
    };

    use super::*;

    #[test]
    fn quiescence_failure_restores_previous_supervisor_and_preserves_error() {
        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path();
        let active_app_dir = install_dir.join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();

        let app_path = active_app_dir.join("cwd-changing-demo-script");
        std::fs::write(&app_path, "#!/bin/sh\ncd /\nread _\n").unwrap();
        make_executable(&app_path);
        write_test_supervisor(&active_app_dir);

        let spawn_deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut child = loop {
            match Command::new("./cwd-changing-demo-script")
                .current_dir(&active_app_dir)
                .stdin(Stdio::piped())
                .spawn()
            {
                Ok(child) => break child,
                Err(error)
                    if error.raw_os_error() == Some(nix::errno::Errno::ETXTBSY as i32)
                        && std::time::Instant::now() < spawn_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("failed to launch changed-cwd app fixture: {error}"),
            }
        };
        let cwd_path = PathBuf::from(format!("/proc/{}/cwd", child.id()));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::fs::read_link(&cwd_path).ok().as_deref() != Some(Path::new("/")) {
            assert!(std::time::Instant::now() < deadline, "test child did not change cwd");
            std::thread::sleep(Duration::from_millis(10));
        }

        let current_environment = BTreeMap::from([("SURGE_TEST_MODE".to_string(), "preserved".to_string())]);
        let child_pid = child.id();
        let child_identity = crate::platform::process::process_identity(child_pid).unwrap().unwrap();
        let result = prepare_and_quiesce_active_app_before_swap(
            install_dir,
            &active_app_dir,
            "1.0.0",
            "cwd-changing-demo-script",
            "demo-supervisor",
            Some(&current_environment),
            true,
            Some(child_identity),
            false,
        );
        let child_still_running = child.try_wait().unwrap().is_none();
        if child_still_running {
            child.kill().unwrap();
        }
        let _ = child.wait();

        let error = result.err().unwrap();
        assert!(error.to_string().contains("process identity is ambiguous"));
        assert!(child_still_running);
        assert!(supervisor_pid_file(install_dir, "demo-supervisor").is_file());
        assert_eq!(
            crate::supervisor::state::read_supervisor_exe_path(install_dir, "demo-supervisor").as_deref(),
            Some(app_path.as_path())
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-environment")).unwrap(),
            "preserved"
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-watched.pid"))
                .unwrap()
                .trim(),
            child_pid.to_string()
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-watched.generation"))
                .unwrap()
                .trim(),
            child_identity.generation.to_string()
        );
    }

    #[tokio::test]
    async fn previous_swap_identity_failure_restores_current_supervisor() {
        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path().join("install");
        let store_dir = tmp.path().join("store");
        let active_app_dir = install_dir.join("app");
        let previous_swap_dir = install_dir.join(".surge-app-prev");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        std::fs::create_dir_all(&previous_swap_dir).unwrap();
        std::fs::create_dir_all(&store_dir).unwrap();

        let app_path = active_app_dir.join("current-app");
        std::fs::write(&app_path, "#!/bin/sh\nexit 0\n").unwrap();
        make_executable(&app_path);
        write_test_supervisor(&active_app_dir);

        let ctx = std::sync::Arc::new(crate::context::Context::new());
        ctx.set_storage(
            crate::context::StorageProvider::Filesystem,
            store_dir.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let manager = UpdateManager::new(ctx, "test-app", "1.0.0", "stable", install_dir.to_str().unwrap()).unwrap();

        let current_environment = BTreeMap::from([("SURGE_TEST_MODE".to_string(), "preserved".to_string())]);
        let error = prepare_and_quiesce_previous_swap_before_reuse(
            &manager,
            &previous_swap_dir,
            Some(&active_app_dir),
            "1.0.0",
            "current-app",
            "demo-supervisor",
            Some(&current_environment),
            true,
            Some(ProcessIdentity {
                pid: 4242,
                generation: 7,
            }),
        )
        .await
        .err()
        .unwrap();

        assert!(error.to_string().contains("no persisted process identity"));
        assert!(supervisor_pid_file(&install_dir, "demo-supervisor").is_file());
        assert_eq!(
            crate::supervisor::state::read_supervisor_exe_path(&install_dir, "demo-supervisor").as_deref(),
            Some(app_path.as_path())
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-environment")).unwrap(),
            "preserved"
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-watched.pid"))
                .unwrap()
                .trim(),
            "4242"
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-watched.generation"))
                .unwrap()
                .trim(),
            "7"
        );
    }

    #[tokio::test]
    async fn previous_swap_quiescence_failure_restores_its_supervisor_handoff() {
        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path().join("install");
        let store_dir = tmp.path().join("store");
        let previous_swap_dir = install_dir.join(".surge-app-prev");
        std::fs::create_dir_all(previous_swap_dir.join(".surge")).unwrap();
        std::fs::create_dir_all(&store_dir).unwrap();

        let app_path = previous_swap_dir.join("cwd-changing-demo-script");
        std::fs::write(&app_path, "#!/bin/sh\ncd /\nread _\n").unwrap();
        make_executable(&app_path);
        write_test_supervisor(&previous_swap_dir);
        std::fs::write(
            previous_swap_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            "id: test-app\nversion: 0.9.0\nmainExe: cwd-changing-demo-script\nsupervisorId: previous-supervisor\nenvironment:\n  SURGE_TEST_MODE: previous-preserved\n",
        )
        .unwrap();

        let spawn_deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut child = loop {
            match Command::new("./cwd-changing-demo-script")
                .current_dir(&previous_swap_dir)
                .stdin(Stdio::piped())
                .spawn()
            {
                Ok(child) => break child,
                Err(error)
                    if error.raw_os_error() == Some(nix::errno::Errno::ETXTBSY as i32)
                        && std::time::Instant::now() < spawn_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("failed to launch previous-swap fixture: {error}"),
            }
        };
        let cwd_path = PathBuf::from(format!("/proc/{}/cwd", child.id()));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::fs::read_link(&cwd_path).ok().as_deref() != Some(Path::new("/")) {
            assert!(std::time::Instant::now() < deadline, "test child did not change cwd");
            std::thread::sleep(Duration::from_millis(10));
        }
        let child_identity = crate::platform::process::process_identity(child.id()).unwrap().unwrap();
        write_accepted_takeover(&install_dir, "previous-supervisor", child_identity);

        let ctx = std::sync::Arc::new(crate::context::Context::new());
        ctx.set_storage(
            crate::context::StorageProvider::Filesystem,
            store_dir.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let manager = UpdateManager::new(ctx, "test-app", "1.0.0", "stable", install_dir.to_str().unwrap()).unwrap();

        let child_pid = child.id();
        let result = prepare_and_quiesce_previous_swap_before_reuse(
            &manager,
            &previous_swap_dir,
            None,
            "",
            "",
            "",
            None,
            false,
            None,
        )
        .await;
        let child_still_running = child.try_wait().unwrap().is_none();
        if child_still_running {
            child.kill().unwrap();
        }
        let _ = child.wait();

        let error = result.err().unwrap();
        assert!(error.to_string().contains("process identity is ambiguous"));
        assert!(child_still_running);
        assert!(supervisor_pid_file(&install_dir, "previous-supervisor").is_file());
        assert_eq!(
            crate::supervisor::state::read_supervisor_exe_path(&install_dir, "previous-supervisor").as_deref(),
            Some(app_path.as_path())
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-environment")).unwrap(),
            "previous-preserved"
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-watched.pid"))
                .unwrap()
                .trim(),
            child_pid.to_string()
        );
        assert_eq!(
            std::fs::read_to_string(install_dir.join("restored-watched.generation"))
                .unwrap()
                .trim(),
            child_identity.generation.to_string()
        );
    }

    fn write_accepted_takeover(install_dir: &Path, supervisor_id: &str, child_identity: ProcessIdentity) {
        let instance = SupervisorTakeoverInstance::new(42);
        let request = SupervisorTakeoverRequest::new(&instance, Duration::from_secs(5));
        let acknowledgement = SupervisorTakeoverAcknowledgement::new(&request, Some(child_identity));
        let commit = SupervisorTakeoverCommit::new(&acknowledgement);
        write_supervisor_takeover_request(install_dir, supervisor_id, &request).unwrap();
        write_supervisor_takeover_acknowledgement(install_dir, supervisor_id, &acknowledgement).unwrap();
        write_supervisor_takeover_commit(install_dir, supervisor_id, &commit).unwrap();
        assert!(accept_supervisor_takeover_request(install_dir, supervisor_id, &request).unwrap());
    }

    fn write_test_supervisor(active_app_dir: &Path) {
        let supervisor_path = active_app_dir.join(crate::platform::process::supervisor_binary_name());
        std::fs::write(
            &supervisor_path,
            r#"#!/bin/sh
id=""
dir=""
pid=""
generation=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --id) id="$2"; shift 2 ;;
    --dir) dir="$2"; shift 2 ;;
    --pid) pid="$2"; shift 2 ;;
    --generation) generation="$2"; shift 2 ;;
    *) shift ;;
  esac
done
printf '%s' "$SURGE_TEST_MODE" > "$dir/restored-environment"
printf '%s' "$pid" > "$dir/restored-watched.pid"
printf '%s' "$generation" > "$dir/restored-watched.generation"
echo $$ > "$dir/.surge-supervisor-$id.pid"
"#,
        )
        .unwrap();
        make_executable(&supervisor_path);
    }

    fn make_executable(path: &Path) {
        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }
}
