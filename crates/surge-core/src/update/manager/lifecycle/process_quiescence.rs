#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::io::Read;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
use tracing::{info, warn};

use crate::error::Result;
#[cfg(unix)]
use crate::error::SurgeError;
use crate::platform::process::current_pid;

#[cfg(unix)]
struct AppProcess {
    exe: PathBuf,
    command: Vec<OsString>,
    cwd: Option<PathBuf>,
}

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
) -> Result<usize> {
    terminate_active_app_processes_except(active_app_dir, main_exe, current_pid())
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
fn terminate_active_app_processes_except(active_app_dir: &Path, main_exe: &str, protected_pid: u32) -> Result<usize> {
    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return Ok(0);
    }

    let active_exe = match std::fs::canonicalize(active_app_dir.join(main_exe)) {
        Ok(active_exe) => active_exe,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(SurgeError::Platform(format!(
                "Failed to resolve active application executable before swap: {e}"
            )));
        }
    };
    let interpreted_main = is_interpreted_main(&active_exe)?;
    terminate_matching_app_processes(main_exe, protected_pid, "active", |process| {
        is_active_app_process(&active_exe, interpreted_main, process)
    })
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

#[cfg(target_os = "linux")]
fn app_process_pids<F>(protected_pid: u32, matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> bool,
{
    use std::os::unix::ffi::OsStringExt;

    let entries = std::fs::read_dir("/proc")
        .map_err(|e| SurgeError::Platform(format!("Failed to enumerate processes from /proc: {e}")))?;

    Ok(entries
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<u32>().ok())
        .filter(|pid| *pid != protected_pid)
        .filter_map(|pid| {
            let exe = std::fs::read_link(format!("/proc/{pid}/exe"))
                .map(normalize_proc_exe_path)
                .ok()?;
            let command = std::fs::read(format!("/proc/{pid}/cmdline")).map_or_else(
                |_| Vec::new(),
                |bytes| {
                    bytes
                        .split(|byte| *byte == 0)
                        .filter(|argument| !argument.is_empty())
                        .map(|argument| OsString::from_vec(argument.to_vec()))
                        .collect()
                },
            );
            let process = AppProcess {
                exe,
                command,
                cwd: std::fs::read_link(format!("/proc/{pid}/cwd")).ok(),
            };
            matches_exe(&process).then_some(pid)
        })
        .collect())
}

#[cfg(target_os = "macos")]
fn app_process_pids<F>(protected_pid: u32, matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> bool,
{
    use sysinfo::{ProcessesToUpdate, System};

    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::All, true);
    Ok(system
        .processes()
        .iter()
        .filter_map(|(pid, process)| {
            let pid = pid.as_u32();
            if pid == protected_pid {
                return None;
            }
            let app_process = AppProcess {
                exe: process.exe()?.to_path_buf(),
                command: process.cmd().to_vec(),
                cwd: process.cwd().map(Path::to_path_buf),
            };
            matches_exe(&app_process).then_some(pid)
        })
        .collect())
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn app_process_pids<F>(_protected_pid: u32, _matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> bool,
{
    Err(SurgeError::Platform(
        "Active application process discovery is unsupported on this Unix platform".to_string(),
    ))
}

#[cfg(target_os = "linux")]
fn normalize_proc_exe_path(path: PathBuf) -> PathBuf {
    let normalized = {
        let path_text = path.to_string_lossy();
        path_text.strip_suffix(" (deleted)").map(PathBuf::from)
    };
    normalized.unwrap_or(path)
}

#[cfg(unix)]
fn is_active_app_exe(active_exe: &Path, exe: &Path) -> bool {
    exe == active_exe
}

#[cfg(unix)]
fn is_interpreted_main(active_exe: &Path) -> Result<bool> {
    let mut file = std::fs::File::open(active_exe).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application executable before swap: {e}"
        ))
    })?;
    let mut prefix = [0_u8; 2];
    let read = file.read(&mut prefix).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application executable before swap: {e}"
        ))
    })?;
    Ok(read == prefix.len() && prefix == *b"#!")
}

#[cfg(unix)]
fn is_active_app_process(active_exe: &Path, interpreted_main: bool, process: &AppProcess) -> bool {
    is_active_app_exe(active_exe, &process.exe)
        || interpreted_main
            && process
                .command
                .iter()
                .take(3)
                .any(|argument| process_argument_resolves_to(argument, process.cwd.as_deref(), active_exe))
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

#[cfg(test)]
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
        use std::os::unix::fs::{PermissionsExt, symlink};
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo");
        std::fs::copy("/bin/sleep", &app_path).unwrap();
        let mut permissions = std::fs::metadata(&app_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&app_path, permissions).unwrap();
        let linked_active_app_dir = tmp.path().join("linked-app");
        symlink(&active_app_dir, &linked_active_app_dir).unwrap();
        let resolved_app_path = std::fs::canonicalize(&app_path).unwrap();

        let mut child = Command::new(&app_path).arg("30").spawn().unwrap();
        let child_pid = child.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
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

        let terminated = terminate_active_app_processes_except(&linked_active_app_dir, "demo", u32::MAX).unwrap();
        let status = child.wait().unwrap();

        assert_eq!(terminated, 1);
        assert!(!status.success());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn interpreted_active_app_process_is_terminated_before_swap() {
        use std::os::unix::fs::PermissionsExt;
        use std::process::Command;

        let tmp = tempfile::tempdir().unwrap();
        let active_app_dir = tmp.path().join("app");
        std::fs::create_dir_all(&active_app_dir).unwrap();
        let app_path = active_app_dir.join("demo-script");
        std::fs::write(&app_path, "#!/bin/sh\nwhile :; do sleep 1; done\n").unwrap();
        let mut permissions = std::fs::metadata(&app_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&app_path, permissions).unwrap();
        let resolved_app_path = std::fs::canonicalize(&app_path).unwrap();

        let mut child = Command::new(&app_path).spawn().unwrap();
        let child_pid = child.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !app_process_pids(u32::MAX, &|process| {
            is_active_app_process(&resolved_app_path, true, process)
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

        let terminated = terminate_active_app_processes_except(&active_app_dir, "demo-script", u32::MAX).unwrap();
        let status = child.wait().unwrap();

        assert_eq!(terminated, 1);
        assert!(!status.success());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_exe_deleted_suffix_is_ignored_for_matching() {
        assert_eq!(
            normalize_proc_exe_path(PathBuf::from("/opt/demo/app-1.0.0/demo (deleted)")),
            PathBuf::from("/opt/demo/app-1.0.0/demo")
        );
    }
}
