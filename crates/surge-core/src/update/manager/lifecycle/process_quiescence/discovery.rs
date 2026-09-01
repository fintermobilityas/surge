//! OS-specific discovery of processes that run a given application
//! executable, used by the pre-swap quiescence step.

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
use crate::error::{Result, SurgeError};

#[cfg(unix)]
pub(super) struct AppProcess {
    pub(super) exe: PathBuf,
    pub(super) command: Vec<OsString>,
    pub(super) cwd: Option<PathBuf>,
}

#[cfg(target_os = "linux")]
pub(super) fn app_process_pids<F>(protected_pid: u32, matches_exe: &F) -> Result<Vec<u32>>
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
pub(super) fn app_process_pids<F>(protected_pid: u32, matches_exe: &F) -> Result<Vec<u32>>
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
pub(super) fn app_process_pids<F>(_protected_pid: u32, _matches_exe: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> bool,
{
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
