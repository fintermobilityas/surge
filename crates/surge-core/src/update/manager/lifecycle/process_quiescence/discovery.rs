//! OS-specific discovery of processes that run a given application
//! executable, used by the pre-swap quiescence step.

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(target_os = "macos")]
use std::path::Path;

#[cfg(unix)]
use crate::error::Result;

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
    use std::os::unix::ffi::OsStringExt;

    use crate::error::SurgeError;

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
        let Ok(exe) = std::fs::read_link(format!("/proc/{pid}/exe")).map(normalize_proc_exe_path) else {
            continue;
        };
        let command = std::fs::read(format!("/proc/{pid}/cmdline"));
        let command_inspected = command.is_ok();
        let command = command.map_or_else(
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
}
