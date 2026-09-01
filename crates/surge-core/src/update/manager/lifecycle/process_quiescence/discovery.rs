//! OS-specific discovery of processes that run a given application
//! executable, used by the pre-swap quiescence step.

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "macos")]
use std::path::Path;

#[cfg(unix)]
use crate::error::Result;
#[cfg(target_os = "linux")]
use crate::error::SurgeError;

#[cfg(unix)]
pub(super) struct AppProcess {
    pub(super) exe: PathBuf,
    pub(super) command: Vec<OsString>,
    pub(super) command_inspected: bool,
    pub(super) cwd: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
pub(super) fn app_process_pids<F>(protected_pid: u32, matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> Result<bool>,
{
    let entries = std::fs::read_dir("/proc")
        .map_err(|e| SurgeError::Platform(format!("Failed to enumerate processes from /proc: {e}")))?;

    let mut pids = Vec::new();
    for entry in entries.filter_map(std::result::Result::ok) {
        let Some(pid) = entry.file_name().to_string_lossy().parse::<u32>().ok() else {
            continue;
        };
        if pid == protected_pid {
            continue;
        }
        let Ok(mut exe) = std::fs::read_link(format!("/proc/{pid}/exe")).map(normalize_proc_exe_path) else {
            continue;
        };
        let Some((command, command_inspected)) = inspect_linux_process_command(pid, &mut exe)? else {
            continue;
        };
        let process = AppProcess {
            exe,
            command,
            command_inspected,
            cwd: std::fs::read_link(format!("/proc/{pid}/cwd")).ok(),
        };
        if matches_exe(&process)? {
            pids.push(pid);
        }
    }

    Ok(pids)
}

#[cfg(target_os = "macos")]
pub(super) fn app_process_pids<F>(protected_pid: u32, matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> Result<bool>,
{
    use sysinfo::{ProcessesToUpdate, System};

    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::All, true);
    let mut pids = Vec::new();
    for (pid, process) in system.processes() {
        let pid = pid.as_u32();
        if pid == protected_pid {
            continue;
        }
        let Some(exe) = process.exe() else {
            continue;
        };
        let app_process = AppProcess {
            exe: exe.to_path_buf(),
            command: process.cmd().to_vec(),
            command_inspected: !process.cmd().is_empty(),
            cwd: process.cwd().map(Path::to_path_buf),
        };
        if matches_exe(&app_process)? {
            pids.push(pid);
        }
    }

    Ok(pids)
}

#[cfg(target_os = "linux")]
pub(super) fn parse_proc_cmdline(bytes: &[u8]) -> Vec<OsString> {
    use std::os::unix::ffi::OsStringExt;

    if bytes.is_empty() {
        return Vec::new();
    }

    let arguments = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    arguments
        .split(|byte| *byte == 0)
        .map(|argument| OsString::from_vec(argument.to_vec()))
        .collect()
}

#[cfg(target_os = "linux")]
fn inspect_linux_process_command(pid: u32, executable: &mut PathBuf) -> Result<Option<(Vec<OsString>, bool)>> {
    let deadline = std::time::Instant::now() + Duration::from_millis(100);
    let mut command = Vec::new();
    let mut command_inspected = false;
    loop {
        if let Ok(bytes) = std::fs::read(format!("/proc/{pid}/cmdline")) {
            command = parse_proc_cmdline(&bytes);
            command_inspected = true;
        }

        let executable_changed = std::fs::read_link(format!("/proc/{pid}/exe"))
            .map(normalize_proc_exe_path)
            .is_ok_and(|observed| {
                if observed == *executable {
                    false
                } else {
                    *executable = observed;
                    true
                }
            });
        if executable_changed {
            command.clear();
            command_inspected = false;
        }
        if !executable_changed && command.iter().any(|argument| !argument.is_empty()) {
            return Ok(Some((command, command_inspected)));
        }
        if linux_process_is_gone_or_zombie(pid)? {
            return Ok(None);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(Some((command, command_inspected)));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(target_os = "linux")]
fn linux_process_is_gone_or_zombie(pid: u32) -> Result<bool> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(SurgeError::Platform(format!(
                "Failed to inspect process PID {pid} state before swap: {error}"
            )));
        }
    };
    proc_stat_is_zombie(&stat)
        .ok_or_else(|| SurgeError::Platform(format!("Failed to parse process PID {pid} state before swap")))
}

#[cfg(target_os = "linux")]
fn proc_stat_is_zombie(stat: &str) -> Option<bool> {
    let state = stat.rsplit_once(") ")?.1.as_bytes().first()?;
    Some(matches!(*state, b'Z' | b'X'))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub(super) fn app_process_pids<F>(_protected_pid: u32, _matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> Result<bool>,
{
    use crate::error::SurgeError;

    Err(SurgeError::Platform(
        "Active application process discovery is unsupported on this Unix platform".to_string(),
    ))
}

#[cfg(target_os = "linux")]
fn normalize_proc_exe_path(path: PathBuf) -> PathBuf {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    match path.try_exists() {
        Ok(false) => {}
        Ok(true) | Err(_) => return path,
    }

    let Some(path_bytes) = path.as_os_str().as_bytes().strip_suffix(b" (deleted)") else {
        return path;
    };
    PathBuf::from(OsString::from_vec(path_bytes.to_vec()))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_exe_deleted_suffix_is_ignored_for_matching() {
        assert_eq!(
            normalize_proc_exe_path(PathBuf::from("/opt/demo/app-1.0.0/demo (deleted)")),
            PathBuf::from("/opt/demo/app-1.0.0/demo")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_exe_path_preserves_existing_filename_with_deleted_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let executable = tmp.path().join("demo (deleted)");
        std::fs::write(&executable, "fixture").unwrap();

        assert_eq!(normalize_proc_exe_path(executable.clone()), executable);
    }

    #[test]
    fn proc_cmdline_preserves_empty_argument_positions() {
        assert_eq!(
            parse_proc_cmdline(b"\0/opt/demo/app/demo\0\0"),
            vec![OsString::new(), OsString::from("/opt/demo/app/demo"), OsString::new()]
        );
        assert!(parse_proc_cmdline(b"").is_empty());
    }

    #[test]
    fn proc_stat_recognizes_gone_process_states() {
        assert_eq!(proc_stat_is_zombie("123 (demo) Z 1"), Some(true));
        assert_eq!(proc_stat_is_zombie("123 (demo) X 1"), Some(true));
        assert_eq!(proc_stat_is_zombie("123 (demo) S 1"), Some(false));
        assert_eq!(proc_stat_is_zombie("malformed"), None);
    }
}
