#[cfg(unix)]
use std::ffi::OsString;
use std::path::Path;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use tracing::{info, warn};

mod discovery;

#[cfg(unix)]
use self::discovery::AppProcess;
#[cfg(unix)]
use self::discovery::app_process_identities;
#[cfg(all(unix, test))]
use self::discovery::app_process_pids;
use crate::error::Result;
#[cfg(unix)]
use crate::error::SurgeError;
use crate::platform::process::current_pid;

#[cfg(unix)]
mod active_entrypoint;
#[cfg(unix)]
mod interpreted_main;

#[cfg(unix)]
pub(in crate::update::manager) struct PreparedAppQuiescence {
    main_exe: String,
    active_entrypoint: active_entrypoint::Identity,
    interpreted_main: Option<interpreted_main::Identity>,
}

pub(in crate::update::manager) fn terminate_superseded_app_processes(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
) -> Result<usize> {
    terminate_superseded_app_processes_except(install_dir, active_app_dir, main_exe, current_pid())
}

#[cfg(not(unix))]
pub(in crate::update::manager) fn terminate_active_app_processes_before_swap(
    active_app_dir: &Path,
    main_exe: &str,
    allow_in_process_swap: bool,
) -> Result<usize> {
    terminate_active_app_processes_except(active_app_dir, main_exe, current_pid(), allow_in_process_swap)
}

#[cfg(unix)]
pub(in crate::update::manager) fn prepare_app_quiescence(
    active_app_dir: &Path,
    main_exe: &str,
    allow_in_process_swap: bool,
) -> Result<Option<PreparedAppQuiescence>> {
    prepare_app_quiescence_except(active_app_dir, main_exe, current_pid(), allow_in_process_swap)
}

#[cfg(unix)]
fn prepare_app_quiescence_except(
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
    allow_in_process_swap: bool,
) -> Result<Option<PreparedAppQuiescence>> {
    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return Ok(None);
    }

    let active_entrypoint = active_entrypoint::Identity::resolve(active_app_dir, main_exe)?;
    let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved)?;
    if allow_in_process_swap {
        info!("In-process swap explicitly allowed, quiescing other active application processes only");
    } else {
        refuse_in_process_swap(&active_entrypoint, interpreted_main.as_ref(), protected_pid)?;
    }
    Ok(Some(PreparedAppQuiescence {
        main_exe: main_exe.to_string(),
        active_entrypoint,
        interpreted_main,
    }))
}

#[cfg(unix)]
pub(in crate::update::manager) fn terminate_prepared_app_processes(prepared: &PreparedAppQuiescence) -> Result<usize> {
    terminate_prepared_app_processes_except(prepared, current_pid())
}

#[cfg(unix)]
fn terminate_superseded_app_processes_except(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
) -> Result<usize> {
    terminate_matching_app_processes(main_exe, protected_pid, "superseded", |process| {
        Ok(is_superseded_app_exe(
            install_dir,
            active_app_dir,
            main_exe,
            &process.exe,
        ))
    })
}

#[cfg(all(unix, test))]
fn terminate_active_app_processes_except(
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
    allow_in_process_swap: bool,
) -> Result<usize> {
    let Some(prepared) = prepare_app_quiescence_except(active_app_dir, main_exe, protected_pid, allow_in_process_swap)?
    else {
        return Ok(0);
    };

    terminate_prepared_app_processes_except(&prepared, protected_pid)
}

#[cfg(unix)]
fn terminate_prepared_app_processes_except(prepared: &PreparedAppQuiescence, protected_pid: u32) -> Result<usize> {
    terminate_matching_app_processes(&prepared.main_exe, protected_pid, "active", |process| {
        is_active_app_process(&prepared.active_entrypoint, prepared.interpreted_main.as_ref(), process)
    })
}

#[cfg(unix)]
fn refuse_in_process_swap(
    active_entrypoint: &active_entrypoint::Identity,
    interpreted_main: Option<&interpreted_main::Identity>,
    protected_pid: u32,
) -> Result<()> {
    if protected_pid != current_pid() {
        return Ok(());
    }

    let process = AppProcess {
        exe: std::env::current_exe().map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to resolve updater process identity before application swap: {e}"
            ))
        })?,
        command: std::env::args_os().collect(),
        command_inspected: true,
        cwd: Some(std::env::current_dir().map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to resolve updater working directory before application swap: {e}"
            ))
        })?),
    };
    refuse_process_in_swap(active_entrypoint, interpreted_main, &process)
}

#[cfg(unix)]
fn refuse_process_in_swap(
    active_entrypoint: &active_entrypoint::Identity,
    interpreted_main: Option<&interpreted_main::Identity>,
    process: &AppProcess,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let active_metadata = std::fs::metadata(&active_entrypoint.resolved).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application identity before swap: {e}"
        ))
    })?;
    let updater_metadata = std::fs::metadata(&process.exe).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect updater process identity before application swap: {e}"
        ))
    })?;
    let same_executable =
        active_metadata.dev() == updater_metadata.dev() && active_metadata.ino() == updater_metadata.ino();
    if same_executable || is_active_app_process(active_entrypoint, interpreted_main, process)? {
        return Err(SurgeError::Platform(
            "The updater is running as the active application process; refusing an in-process directory swap. Apply the update from an external Surge updater."
                .to_string(),
        ));
    }

    Ok(())
}

#[cfg(unix)]
fn terminate_matching_app_processes<F>(
    main_exe: &str,
    protected_pid: u32,
    process_scope: &'static str,
    matches_exe: F,
) -> Result<usize>
where
    F: Fn(&AppProcess) -> Result<bool>,
{
    use nix::errno::Errno;
    use nix::sys::signal::Signal;

    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return Ok(0);
    }

    let identities = app_process_identities(protected_pid, &matches_exe)?;
    if identities.is_empty() {
        return Ok(0);
    }

    for identity in &identities {
        if let Err(e) = signal_pid(identity.pid, Signal::SIGTERM) {
            let pid = identity.pid;
            warn!(pid, error = %e, process_scope, "Failed to request app process termination");
        }
    }

    if wait_until_app_processes_exit(protected_pid, &matches_exe, Duration::from_secs(5))? {
        info!(count = identities.len(), process_scope, "Terminated app processes");
        return Ok(identities.len());
    }

    let remaining = app_process_identities(protected_pid, &matches_exe)?;
    for identity in &remaining {
        match signal_pid(identity.pid, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(e) => {
                let pid = identity.pid;
                warn!(pid, error = %e, process_scope, "Failed to force-kill app process");
            }
        }
    }

    if wait_until_app_processes_exit(protected_pid, &matches_exe, Duration::from_secs(2))? {
        info!(
            count = identities.len(),
            forced = remaining.len(),
            process_scope,
            "Force-killed app processes"
        );
        return Ok(identities.len());
    }

    Err(SurgeError::Platform(format!(
        "Timed out waiting for {process_scope} '{main_exe}' processes to exit"
    )))
}

#[cfg(unix)]
fn signal_pid(pid: u32, signal: nix::sys::signal::Signal) -> std::result::Result<(), nix::errno::Errno> {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(raw_pid) = i32::try_from(pid) else {
        return Ok(());
    };
    kill(Pid::from_raw(raw_pid), signal)
}

#[cfg(not(unix))]
fn terminate_superseded_app_processes_except(
    _install_dir: &Path,
    _active_app_dir: &Path,
    _main_exe: &str,
    _protected_pid: u32,
) -> Result<usize> {
    Ok(0)
}

#[cfg(not(unix))]
fn terminate_active_app_processes_except(
    _active_app_dir: &Path,
    _main_exe: &str,
    _protected_pid: u32,
    _allow_in_process_swap: bool,
) -> Result<usize> {
    Ok(0)
}

#[cfg(unix)]
fn wait_until_app_processes_exit<F>(protected_pid: u32, matches_exe: &F, timeout: Duration) -> Result<bool>
where
    F: Fn(&AppProcess) -> Result<bool>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if app_process_identities(protected_pid, matches_exe)?.is_empty() {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
const NATIVE_TEST_APP_FILTER: &str =
    "update::manager::lifecycle::process_quiescence::tests::native_app_process_fixture";

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(in crate::update::manager) fn spawn_native_test_app(app_path: &Path) -> std::process::Child {
    use std::os::unix::fs::PermissionsExt;
    use std::process::{Command, Stdio};

    std::fs::copy(std::env::current_exe().unwrap(), app_path).unwrap();
    let mut permissions = std::fs::metadata(app_path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(app_path, permissions).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        match Command::new(app_path)
            .args(["--exact", NATIVE_TEST_APP_FILTER, "--nocapture"])
            .env("SURGE_NATIVE_TEST_APP", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => break child,
            Err(error) if error.raw_os_error() == Some(nix::libc::ETXTBSY) && std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("failed to start native test app: {error}"),
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
pub(in crate::update::manager) fn wait_for_native_test_app(app_path: &Path, child_pid: u32) {
    let active_app_dir = app_path.parent().unwrap();
    let main_exe = app_path.file_name().unwrap().to_str().unwrap();
    let active_entrypoint = active_entrypoint::Identity::resolve(active_app_dir, main_exe).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !app_process_pids(u32::MAX, &|process| {
        is_active_app_process(&active_entrypoint, None, process)
    })
    .unwrap()
    .contains(&child_pid)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "test child did not enter the active app path"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
fn is_active_app_process(
    active_entrypoint: &active_entrypoint::Identity,
    interpreted_main: Option<&interpreted_main::Identity>,
    process: &AppProcess,
) -> Result<bool> {
    if active_entrypoint.matches_executable(&process.exe) {
        return Ok(true);
    }
    if active_entrypoint.matches_resolved_executable(&process.exe) {
        if process.command.iter().all(|argument| argument.is_empty()) {
            return Err(SurgeError::Platform(
                "Cannot inspect the command line of the active application executable before swap".to_string(),
            ));
        }
        let Some(argument) = process.command.first() else {
            return Err(SurgeError::Platform(
                "Cannot inspect the command line of the active application executable before swap".to_string(),
            ));
        };
        if active_entrypoint.matches_argument(argument, process.cwd.as_deref())? {
            return Ok(true);
        }
    }
    let Some(identity) = interpreted_main else {
        return Ok(false);
    };
    if process.command.iter().all(|argument| argument.is_empty()) && identity.executable_may_match(&process.exe)? {
        return Err(SurgeError::Platform(
            "Cannot inspect the command line of the active application interpreter before swap".to_string(),
        ));
    }
    let Some(argument) = process.command.get(identity.script_argument_index) else {
        return Ok(false);
    };
    if !active_entrypoint.matches_argument(argument, process.cwd.as_deref())? {
        return Ok(false);
    }
    if !process.command_inspected && identity.executable_may_match(&process.exe)? {
        return Err(SurgeError::Platform(
            "Cannot inspect the command line of the active application interpreter before swap".to_string(),
        ));
    }
    if !identity.matches_interpreter(&process.exe, process.command.first().map(OsString::as_os_str))? {
        return Ok(false);
    }
    Ok(true)
}

#[cfg(unix)]
fn is_superseded_app_exe(install_dir: &Path, active_app_dir: &Path, main_exe: &str, exe: &Path) -> bool {
    if exe == active_app_dir.join(main_exe) || exe.file_name().and_then(|name| name.to_str()) != Some(main_exe) {
        return false;
    }

    let Ok(relative) = exe.strip_prefix(install_dir) else {
        return false;
    };

    let mut components = relative.components();
    let Some(std::path::Component::Normal(first)) = components.next() else {
        return false;
    };
    let first = first.to_string_lossy();

    first == ".surge-app-prev" || first.starts_with("app-") || components.next().is_none()
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::*;

    #[cfg(unix)]
    #[test]
    fn superseded_app_exe_detection_matches_retained_directories_only() {
        let install_dir = Path::new("/opt/demo");
        let active_app_dir = install_dir.join("app");

        assert!(is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/app-1.0.0/demo")
        ));
        assert!(is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/.surge-app-prev/demo")
        ));
        assert!(is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/demo")
        ));
        assert!(!is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/app/demo")
        ));
        assert!(!is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/opt/demo/app-1.0.0/other")
        ));
        assert!(!is_superseded_app_exe(
            install_dir,
            &active_app_dir,
            "demo",
            Path::new("/srv/other/app-1.0.0/demo")
        ));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn active_app_process_is_terminated_before_swap() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo");
        let linked_active_app_dir = tmp.path().join("linked-app");
        symlink(&active_app_dir, &linked_active_app_dir).unwrap();

        let mut child = spawn_native_test_app(&app_path);
        let child_pid = child.id();
        wait_for_native_test_app(&app_path, child_pid);

        let terminated =
            terminate_active_app_processes_except(&linked_active_app_dir, "demo", u32::MAX, false).unwrap();
        let status = child.wait().unwrap();

        assert_eq!(terminated, 1);
        assert!(!status.success());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn native_app_process_fixture() {
        if std::env::var_os("SURGE_NATIVE_TEST_APP").is_some() {
            std::thread::sleep(Duration::from_secs(30));
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn in_process_updater_refuses_swap_without_signalling_itself() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        std::fs::hard_link(std::env::current_exe().unwrap(), active_app_dir.join("demo")).unwrap();

        let error = terminate_active_app_processes_except(&active_app_dir, "demo", current_pid(), false).unwrap_err();

        assert!(error.to_string().contains("refusing an in-process directory swap"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn in_process_updater_swaps_without_signalling_itself_when_explicitly_allowed() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        std::fs::hard_link(std::env::current_exe().unwrap(), active_app_dir.join("demo")).unwrap();

        let terminated = terminate_active_app_processes_except(&active_app_dir, "demo", current_pid(), true).unwrap();

        assert_eq!(terminated, 0);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_in_process_updater_refuses_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::from("/bin/sh"), app_path.into_os_string()],
            command_inspected: true,
            cwd: Some(active_app_dir),
        };

        let error = refuse_process_in_swap(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(error.to_string().contains("refusing an in-process directory swap"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn configured_argv0_does_not_match_an_unrelated_executable() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo");
        std::fs::write(&app_path, "fixture").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo").unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sleep").unwrap(),
            command: vec![app_path.into_os_string()],
            command_inspected: true,
            cwd: Some(active_app_dir),
        };

        assert!(!is_active_app_process(&active_entrypoint, None, &process).unwrap());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreter_argv0_does_not_match_an_unrelated_executable() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sleep").unwrap(),
            command: vec![OsString::from("/bin/sh"), app_path.into_os_string()],
            command_inspected: true,
            cwd: Some(active_app_dir),
        };

        assert!(!is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn missing_interpreter_command_line_refuses_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: Vec::new(),
            command_inspected: false,
            cwd: None,
        };

        let error = is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(error.to_string().contains("Cannot inspect the command line"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn empty_inspected_interpreter_command_line_refuses_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::new()],
            command_inspected: true,
            cwd: None,
        };

        let error = is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(error.to_string().contains("Cannot inspect the command line"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn all_empty_interpreter_command_line_refuses_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::new(), OsString::new()],
            command_inspected: true,
            cwd: None,
        };

        let error = is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(error.to_string().contains("Cannot inspect the command line"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn empty_inspected_symlink_command_line_refuses_swap() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        symlink("/bin/sleep", active_app_dir.join("demo")).unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo").unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sleep").unwrap(),
            command: vec![OsString::new()],
            command_inspected: true,
            cwd: None,
        };

        let error = is_active_app_process(&active_entrypoint, None, &process).unwrap_err();

        assert!(error.to_string().contains("Cannot inspect the command line"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn missing_interpreter_working_directory_refuses_relative_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::from("/bin/sh"), OsString::from("demo-script")],
            command_inspected: true,
            cwd: None,
        };

        let error = is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(error.to_string().contains("launch directory"));
        assert!(error.to_string().contains("process identity is ambiguous"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn missing_active_entrypoint_refuses_swap_without_signalling_running_process() {
        use std::os::unix::fs::symlink;
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo");
        symlink("/bin/sleep", &app_path).unwrap();

        let mut child = Command::new(&app_path).arg("30").spawn().unwrap();
        std::fs::remove_file(&app_path).unwrap();

        let error = terminate_active_app_processes_except(&active_app_dir, "demo", u32::MAX, false).unwrap_err();
        let child_still_running = child.try_wait().unwrap().is_none();
        if child_still_running {
            child.kill().unwrap();
        }
        let _ = child.wait();

        assert!(
            error
                .to_string()
                .contains("Failed to resolve active application executable before swap")
        );
        assert!(child_still_running);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn symlinked_active_entrypoint_does_not_terminate_other_processes_using_shared_target() {
        use std::os::unix::fs::symlink;
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo");
        symlink("/bin/sleep", &app_path).unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo").unwrap();
        assert!(active_entrypoint.requires_argument());

        let mut app_child = Command::new(&app_path).arg("30").spawn().unwrap();
        let app_child_pid = app_child.id();
        let mut unrelated_child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let unrelated_child_pid = unrelated_child.id();

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let matches = app_process_pids(u32::MAX, &|process| {
                is_active_app_process(&active_entrypoint, None, process)
            })
            .unwrap();
            if matches.contains(&app_child_pid) {
                assert!(!matches.contains(&unrelated_child_pid));
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "test child did not expose the symlinked app launch identity"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        let terminated = terminate_active_app_processes_except(&active_app_dir, "demo", u32::MAX, false);
        let app_status = app_child.wait().unwrap();
        let unrelated_still_running = unrelated_child.try_wait().unwrap().is_none();
        if unrelated_still_running {
            unrelated_child.kill().unwrap();
        }
        let _ = unrelated_child.wait();

        assert_eq!(terminated.unwrap(), 1);
        assert!(!app_status.success());
        assert!(unrelated_still_running);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn assert_interpreted_active_app_process_is_terminated_before_swap(shebang: &str) {
        use std::os::unix::fs::PermissionsExt;
        use std::process::{Command, Stdio};

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, format!("{shebang}\nread _\n")).unwrap();
        let mut permissions = std::fs::metadata(&app_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&app_path, permissions).unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();

        let spawn_deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut child = loop {
            let mut command = Command::new(&app_path);
            command.stdin(Stdio::piped());
            match command.spawn() {
                Ok(child) => break child,
                Err(error)
                    if error.raw_os_error() == Some(nix::errno::Errno::ETXTBSY as i32)
                        && std::time::Instant::now() < spawn_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("failed to launch interpreted app fixture: {error}"),
            }
        };
        let child_pid = child.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !app_process_pids(u32::MAX, &|process| {
            is_active_app_process(&active_entrypoint, Some(&interpreted_main), process)
        })
        .unwrap()
        .contains(&child_pid)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "test child did not expose the interpreted app launch identity"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        let terminated =
            terminate_active_app_processes_except(&active_app_dir, "demo-script", u32::MAX, false).unwrap();
        let status = child.wait().unwrap();

        assert_eq!(terminated, 1);
        assert!(!status.success());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_active_app_process_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/bin/sh");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_relative_entrypoint_with_changed_cwd_refuses_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\n").unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::from("/bin/sh"), OsString::from("./demo-script")],
            command_inspected: true,
            cwd: Some(std::path::PathBuf::from("/")),
        };

        let error = is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(error.to_string().contains("launch directory"));
        assert!(error.to_string().contains("process identity is ambiguous"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn prepared_interpreted_identity_catches_process_started_before_path_withdrawal() {
        use std::os::unix::fs::PermissionsExt;
        use std::process::{Command, Stdio};

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        let previous_app_dir = tmp.path().join(".surge-app-prev");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        let ready_path = tmp.path().join("ready");
        std::fs::write(&app_path, "#!/bin/sh\nprintf ready > \"$1\"\nread _\n").unwrap();
        let mut permissions = std::fs::metadata(&app_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&app_path, permissions).unwrap();

        let prepared = prepare_app_quiescence_except(&active_app_dir, "demo-script", u32::MAX, false)
            .unwrap()
            .unwrap();
        assert_eq!(terminate_prepared_app_processes_except(&prepared, u32::MAX).unwrap(), 0);

        let mut child = Command::new(&app_path)
            .arg(&ready_path)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let child_pid = child.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !ready_path.is_file() {
            assert!(
                std::time::Instant::now() < deadline,
                "test child did not finish loading the interpreted app"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            app_process_pids(u32::MAX, &|process| {
                is_active_app_process(&prepared.active_entrypoint, prepared.interpreted_main.as_ref(), process)
            })
            .unwrap()
            .contains(&child_pid)
        );

        std::fs::rename(&active_app_dir, &previous_app_dir).unwrap();
        let terminated = terminate_prepared_app_processes_except(&prepared, u32::MAX).unwrap();
        let status = child.wait().unwrap();

        assert_eq!(terminated, 1);
        assert!(!status.success());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_active_app_with_multiple_interpreter_options_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/usr/bin/env -S -i /bin/sh -e -u");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn env_interpreted_app_with_unavailable_launch_environment_refuses_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let interpreter_name = "surge-test-unresolvable-env-interpreter";
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, format!("#!/usr/bin/env {interpreter_name}\n")).unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::from(interpreter_name), app_path.into_os_string()],
            command_inspected: true,
            cwd: Some(active_app_dir),
        };

        let error = is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Failed to resolve active application env interpreter")
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn unrelated_script_operand_does_not_resolve_an_ambiguous_env_interpreter() {
        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        let interpreter_name = "surge-test-unresolvable-env-interpreter";
        std::fs::write(&app_path, format!("#!/usr/bin/env {interpreter_name}\n")).unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo-script").unwrap();
        let interpreted_main = interpreted_main::resolve(&active_entrypoint.resolved).unwrap().unwrap();
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sh").unwrap(),
            command: vec![OsString::from(interpreter_name), OsString::from("/other/script")],
            command_inspected: true,
            cwd: Some(active_app_dir),
        };

        assert!(!is_active_app_process(&active_entrypoint, Some(&interpreted_main), &process).unwrap());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn direct_interpreted_app_with_multiple_options_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/bin/sh -e -u");
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn env_interpreted_app_with_multiple_options_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/usr/bin/env /bin/sh -e -u");
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn empty_argv0_does_not_shift_a_symlinked_entrypoint_operand() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo");
        symlink("/bin/sleep", &app_path).unwrap();
        let active_entrypoint = active_entrypoint::Identity::resolve(&active_app_dir, "demo").unwrap();
        let mut command = vec![0];
        command.extend_from_slice(app_path.as_os_str().as_encoded_bytes());
        command.push(0);
        let process = AppProcess {
            exe: std::fs::canonicalize("/bin/sleep").unwrap(),
            command: discovery::parse_proc_cmdline(&command),
            command_inspected: true,
            cwd: Some(active_app_dir),
        };

        assert!(!is_active_app_process(&active_entrypoint, None, &process).unwrap());
    }
}
