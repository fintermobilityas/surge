use std::path::Path;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use tracing::{info, warn};

mod discovery;

#[cfg(unix)]
use self::discovery::AppProcess;
#[cfg(unix)]
use self::discovery::app_process_pids;
use crate::error::Result;
#[cfg(unix)]
use crate::error::SurgeError;
use crate::platform::process::current_pid;

#[cfg(unix)]
mod interpreted_main;
pub(in crate::update::manager) fn terminate_superseded_app_processes(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
) -> Result<usize> {
    terminate_superseded_app_processes_except(install_dir, active_app_dir, main_exe, current_pid())
}

pub(in crate::update::manager) fn terminate_active_app_processes_before_swap(
    active_app_dir: &Path,
    main_exe: &str,
    allow_in_process_swap: bool,
) -> Result<usize> {
    terminate_active_app_processes_except(active_app_dir, main_exe, current_pid(), allow_in_process_swap)
}

#[cfg(unix)]
fn terminate_superseded_app_processes_except(
    install_dir: &Path,
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
) -> Result<usize> {
    terminate_matching_app_processes(main_exe, protected_pid, "superseded", |process| {
        is_superseded_app_exe(install_dir, active_app_dir, main_exe, &process.exe)
    })
}

#[cfg(unix)]
fn terminate_active_app_processes_except(
    active_app_dir: &Path,
    main_exe: &str,
    protected_pid: u32,
    allow_in_process_swap: bool,
) -> Result<usize> {
    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return Ok(0);
    }

    let active_app_root = std::fs::canonicalize(active_app_dir).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to resolve active application directory before swap: {e}"
        ))
    })?;
    let active_exe = std::fs::canonicalize(active_app_root.join(main_exe)).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to resolve active application executable before swap: {e}"
        ))
    })?;
    if !active_exe.starts_with(&active_app_root) {
        return Err(SurgeError::Platform(format!(
            "Active application executable '{}' resolves outside the active application directory; refusing to signal a shared executable",
            active_app_root.join(main_exe).display()
        )));
    }
    if updater_runs_from_active_exe(&active_exe, protected_pid)? {
        if !allow_in_process_swap {
            return Err(SurgeError::Platform(
                "The updater is running from the active application executable; refusing an in-process directory swap. Apply the update from an external Surge updater."
                    .to_string(),
            ));
        }
        info!(
            "The updater is running from the active application executable; in-process swap explicitly allowed, quiescing other active application processes only"
        );
    }
    let interpreted_main = interpreted_main::resolve(&active_exe)?;
    terminate_matching_app_processes(main_exe, protected_pid, "active", |process| {
        is_active_app_process(&active_exe, interpreted_main.as_ref(), process)
    })
}

#[cfg(unix)]
fn updater_runs_from_active_exe(active_exe: &Path, protected_pid: u32) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    if protected_pid != current_pid() {
        return Ok(false);
    }

    let updater_exe = std::env::current_exe().map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to resolve updater process identity before application swap: {e}"
        ))
    })?;
    let active_metadata = std::fs::metadata(active_exe).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application identity before swap: {e}"
        ))
    })?;
    let updater_metadata = std::fs::metadata(&updater_exe).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect updater process identity before application swap: {e}"
        ))
    })?;
    Ok(active_metadata.dev() == updater_metadata.dev() && active_metadata.ino() == updater_metadata.ino())
}

#[cfg(unix)]
fn terminate_matching_app_processes<F>(
    main_exe: &str,
    protected_pid: u32,
    process_scope: &'static str,
    matches_exe: F,
) -> Result<usize>
where
    F: Fn(&AppProcess) -> bool,
{
    use nix::errno::Errno;
    use nix::sys::signal::Signal;

    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return Ok(0);
    }

    let pids = app_process_pids(protected_pid, &matches_exe)?;
    if pids.is_empty() {
        return Ok(0);
    }

    for pid in &pids {
        if let Err(e) = signal_pid(*pid, Signal::SIGTERM) {
            warn!(pid, error = %e, process_scope, "Failed to request app process termination");
        }
    }

    if wait_until_app_processes_exit(protected_pid, &matches_exe, Duration::from_secs(5))? {
        info!(count = pids.len(), process_scope, "Terminated app processes");
        return Ok(pids.len());
    }

    let remaining = app_process_pids(protected_pid, &matches_exe)?;
    for pid in &remaining {
        match signal_pid(*pid, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(e) => {
                warn!(pid, error = %e, process_scope, "Failed to force-kill app process");
            }
        }
    }

    if wait_until_app_processes_exit(protected_pid, &matches_exe, Duration::from_secs(2))? {
        info!(
            count = pids.len(),
            forced = remaining.len(),
            process_scope,
            "Force-killed app processes"
        );
        return Ok(pids.len());
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
    F: Fn(&AppProcess) -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if app_process_pids(protected_pid, matches_exe)?.is_empty() {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn is_active_app_exe(active_exe: &Path, exe: &Path) -> bool {
    exe == active_exe
}

#[cfg(target_os = "macos")]
fn is_active_app_exe(active_exe: &Path, exe: &Path) -> bool {
    exe == active_exe || std::fs::canonicalize(exe).is_ok_and(|resolved| resolved == active_exe)
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
    let resolved_app_path = std::fs::canonicalize(app_path).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !app_process_pids(u32::MAX, &|process| is_active_app_exe(&resolved_app_path, &process.exe))
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
    active_exe: &Path,
    interpreted_main: Option<&interpreted_main::Identity>,
    process: &AppProcess,
) -> bool {
    is_active_app_exe(active_exe, &process.exe)
        || process
            .command
            .first()
            .is_some_and(|argument| process_argument_resolves_to(argument, process.cwd.as_deref(), active_exe))
        || interpreted_main.is_some_and(|identity| {
            identity.matches_interpreter(&process.exe, process.command.first().map(OsString::as_os_str))
                && process
                    .command
                    .get(identity.script_argument_index)
                    .is_some_and(|argument| process_argument_resolves_to(argument, process.cwd.as_deref(), active_exe))
        })
}

#[cfg(unix)]
fn process_argument_resolves_to(argument: &std::ffi::OsStr, cwd: Option<&Path>, expected: &Path) -> bool {
    let argument = Path::new(argument);
    if argument.as_os_str().is_empty() {
        return false;
    }
    let candidate = if argument.is_absolute() {
        argument.to_path_buf()
    } else if let Some(cwd) = cwd {
        cwd.join(argument)
    } else {
        return false;
    };
    std::fs::canonicalize(candidate).is_ok_and(|resolved| resolved == expected)
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

#[cfg(all(test, unix))]
mod tests {
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
    fn assert_interpreted_active_app_process_is_terminated_before_swap(
        shebang: &str,
        isolated_env_interpreter: Option<&str>,
    ) {
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let (shebang, child_path) = if let Some(interpreter_name) = isolated_env_interpreter {
            let interpreter_dir = tmp.path().join("child-path");
            std::fs::create_dir_all(&interpreter_dir).unwrap();
            let interpreter_path = interpreter_dir.join(interpreter_name);
            std::os::unix::fs::symlink("/bin/sh", &interpreter_path).unwrap();
            let updater_path = std::env::var_os("PATH").unwrap_or_default();
            let mut child_paths = vec![interpreter_dir];
            child_paths.extend(std::env::split_paths(&updater_path));
            (
                format!("#!/usr/bin/env {interpreter_name}"),
                Some(std::env::join_paths(child_paths).unwrap()),
            )
        } else {
            (shebang.to_string(), None)
        };
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, format!("{shebang}\nwhile :; do sleep 1; done\n")).unwrap();
        let mut permissions = std::fs::metadata(&app_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&app_path, permissions).unwrap();
        let resolved_app_path = std::fs::canonicalize(&app_path).unwrap();
        let interpreted_main = interpreted_main::resolve(&resolved_app_path).unwrap().unwrap();

        let spawn_deadline = std::time::Instant::now() + Duration::from_secs(1);
        let mut child = loop {
            let mut command = Command::new(&app_path);
            if let Some(child_path) = &child_path {
                command.env("PATH", child_path);
            }
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
            is_active_app_process(&resolved_app_path, Some(&interpreted_main), process)
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

        let terminated = terminate_active_app_processes_except(&active_app_dir, "demo-script", u32::MAX, false).unwrap();
        let status = child.wait().unwrap();

        assert_eq!(terminated, 1);
        assert!(!status.success());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn shared_symlink_entrypoint_refuses_swap_without_signalling_unrelated_process() {
        use std::os::unix::fs::symlink;
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        symlink("/bin/sleep", active_app_dir.join("demo")).unwrap();

        let mut unrelated_child = Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let error = terminate_active_app_processes_except(&active_app_dir, "demo", u32::MAX, false).unwrap_err();
        let unrelated_still_running = unrelated_child.try_wait().unwrap().is_none();
        if unrelated_still_running {
            unrelated_child.kill().unwrap();
        }
        let _ = unrelated_child.wait();

        assert!(
            error
                .to_string()
                .contains("resolves outside the active application directory")
        );
        assert!(unrelated_still_running);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_active_app_process_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/bin/sh", None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_active_app_with_multiple_interpreter_options_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/usr/bin/env -S /bin/sh -e -u", None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn env_interpreted_app_uses_launched_process_identity_when_paths_differ() {
        assert_interpreted_active_app_process_is_terminated_before_swap("", Some("surge-test-env-interpreter"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn direct_interpreted_app_with_multiple_options_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/bin/sh -e -u", None);
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn env_interpreted_app_with_multiple_options_is_terminated_before_swap() {
        assert_interpreted_active_app_process_is_terminated_before_swap("#!/usr/bin/env /bin/sh -e -u", None);
    }
}
