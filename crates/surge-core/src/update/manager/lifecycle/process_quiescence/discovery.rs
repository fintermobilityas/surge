//! OS-specific discovery of processes that run a given application
//! executable, used by the pre-swap quiescence step.

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;

#[cfg(unix)]
use crate::error::{Result, SurgeError};
#[cfg(unix)]
use crate::platform::process::{ProcessIdentity, process_identity, process_identity_matches};

#[cfg(unix)]
pub(super) struct AppProcess {
    pub(super) exe: std::path::PathBuf,
    pub(super) command: Vec<OsString>,
    pub(super) command_inspected: bool,
    pub(super) cwd: Option<std::path::PathBuf>,
}

#[cfg(unix)]
pub(super) fn app_process_identities<F, E, C>(
    protected_pid: u32,
    matches_process: &F,
    executable_may_match: &E,
    command_may_match: &C,
) -> Result<Vec<ProcessIdentity>>
where
    F: Fn(&AppProcess) -> Result<bool>,
    E: Fn(&Path) -> Result<bool>,
    C: Fn(&[OsString], Option<&Path>) -> Result<bool>,
{
    app_process_identities_impl(protected_pid, matches_process, executable_may_match, command_may_match)
}

#[cfg(all(unix, test))]
pub(super) fn app_process_pids<F>(protected_pid: u32, matches_process: &F) -> Result<Vec<u32>>
where
    F: Fn(&AppProcess) -> Result<bool>,
{
    app_process_identities(protected_pid, matches_process, &|_| Ok(true), &|_, _| Ok(true))
        .map(|identities| identities.into_iter().map(|identity| identity.pid).collect())
}

#[cfg(target_os = "linux")]
fn app_process_identities_impl<F, E, C>(
    protected_pid: u32,
    matches_process: &F,
    executable_may_match: &E,
    command_may_match: &C,
) -> Result<Vec<ProcessIdentity>>
where
    F: Fn(&AppProcess) -> Result<bool>,
    E: Fn(&Path) -> Result<bool>,
    C: Fn(&[OsString], Option<&Path>) -> Result<bool>,
{
    let entries = std::fs::read_dir("/proc")
        .map_err(|error| SurgeError::Platform(format!("Failed to enumerate processes from /proc: {error}")))?;
    let current_uid = read_linux_effective_uid("self")
        .map_err(|error| SurgeError::Platform(format!("Failed to inspect updater process ownership: {error}")))?;

    let mut identities = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if process_exited_during_inspection(&error) => continue,
            Err(error) => {
                return Err(SurgeError::Platform(format!(
                    "Failed while enumerating processes from /proc: {error}"
                )));
            }
        };
        let Some(pid) = entry.file_name().to_string_lossy().parse::<u32>().ok() else {
            continue;
        };
        if pid == protected_pid {
            continue;
        }

        let uid = match read_linux_effective_uid(&pid.to_string()) {
            Ok(uid) => uid,
            Err(error) if process_exited_during_inspection(&error) => continue,
            Err(error) => return Err(process_inspection_error(pid, "ownership", &error)),
        };
        if uid != current_uid {
            continue;
        }
        let Some(identity) = inspected_process_identity(pid)? else {
            continue;
        };

        let mut executable = std::fs::read_link(format!("/proc/{pid}/exe")).map(normalize_proc_exe_path);
        let command = match inspect_proc_command(pid, executable.as_mut().ok()) {
            Ok(Some(command)) => Ok(command),
            Ok(None) => continue,
            Err(error) => Err(error),
        };
        if executable.as_ref().is_err_and(process_exited_during_inspection)
            || command.as_ref().is_err_and(process_exited_during_inspection)
        {
            continue;
        }

        let cwd = match std::fs::read_link(format!("/proc/{pid}/cwd")) {
            Ok(cwd) => Some(cwd),
            Err(_) if inspected_identity_still_matches(identity)? => None,
            Err(_) => continue,
        };
        let (executable, command) = match (executable, command) {
            (Ok(executable), Ok(command)) => (executable, command),
            (Err(error), Ok(command)) => {
                if command_may_match(&command, cwd.as_deref())? {
                    return Err(process_inspection_error(pid, "executable", &error));
                }
                continue;
            }
            (Ok(executable), Err(error)) => {
                if executable_may_match(&executable)? {
                    return Err(process_inspection_error(pid, "command line", &error));
                }
                continue;
            }
            (Err(error), Err(_)) => return Err(process_inspection_error(pid, "executable", &error)),
        };
        let process = AppProcess {
            exe: executable,
            command,
            command_inspected: true,
            cwd,
        };
        if matches_process(&process)? && inspected_identity_still_matches(identity)? {
            identities.push(identity);
        }
    }

    Ok(identities)
}

#[cfg(target_os = "linux")]
fn read_linux_effective_uid(process: &str) -> std::io::Result<u32> {
    let status = std::fs::read_to_string(format!("/proc/{process}/status"))?;
    parse_linux_effective_uid(&status).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("process {process} status has no valid effective UID"),
        )
    })
}

#[cfg(target_os = "linux")]
fn parse_linux_effective_uid(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[cfg(target_os = "linux")]
fn inspect_proc_command(pid: u32, mut executable: Option<&mut PathBuf>) -> std::io::Result<Option<Vec<OsString>>> {
    let mut command = read_proc_arguments(pid, "cmdline");
    if executable.is_none() {
        return command.map(Some);
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    loop {
        if command.as_ref().is_err_and(process_exited_during_inspection) {
            return Ok(None);
        }
        if command.is_err() {
            return command.map(Some);
        }

        let observed = match std::fs::read_link(format!("/proc/{pid}/exe")) {
            Ok(observed) => normalize_proc_exe_path(observed),
            Err(error) if process_exited_during_inspection(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let executable_changed = executable.as_mut().is_some_and(|executable| {
            if observed == **executable {
                false
            } else {
                **executable = observed;
                true
            }
        });
        if !executable_changed
            && command
                .as_ref()
                .is_ok_and(|arguments| arguments.iter().any(|argument| !argument.is_empty()))
        {
            return command.map(Some);
        }
        if process_identity(pid)?.is_none() {
            return Ok(None);
        }
        if std::time::Instant::now() >= deadline {
            return command.map(Some);
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        command = read_proc_arguments(pid, "cmdline");
    }
}

#[cfg(target_os = "linux")]
fn read_proc_arguments(pid: u32, field: &str) -> std::io::Result<Vec<OsString>> {
    std::fs::read(format!("/proc/{pid}/{field}")).map(|bytes| parse_proc_cmdline(&bytes))
}

#[cfg(target_os = "linux")]
pub(super) fn parse_proc_cmdline(bytes: &[u8]) -> Vec<OsString> {
    use std::os::unix::ffi::OsStringExt;

    if bytes.is_empty() {
        return Vec::new();
    }
    bytes
        .strip_suffix(&[0])
        .unwrap_or(bytes)
        .split(|byte| *byte == 0)
        .map(|argument| OsString::from_vec(argument.to_vec()))
        .collect()
}

#[cfg(target_os = "linux")]
fn process_exited_during_inspection(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || matches!(error.raw_os_error(), Some(nix::libc::ESRCH))
}

#[cfg(target_os = "linux")]
fn process_inspection_error(pid: u32, field: &str, error: &std::io::Error) -> SurgeError {
    SurgeError::Platform(format!(
        "Failed to inspect process {pid} {field} before application swap: {error}"
    ))
}

#[cfg(target_os = "macos")]
fn app_process_identities_impl<F, E, C>(
    protected_pid: u32,
    matches_process: &F,
    _executable_may_match: &E,
    _command_may_match: &C,
) -> Result<Vec<ProcessIdentity>>
where
    F: Fn(&AppProcess) -> Result<bool>,
    E: Fn(&Path) -> Result<bool>,
    C: Fn(&[OsString], Option<&Path>) -> Result<bool>,
{
    use sysinfo::{ProcessesToUpdate, System};

    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::All, true);
    let mut identities = Vec::new();
    for (pid, process) in system.processes() {
        let pid = pid.as_u32();
        if pid == protected_pid {
            continue;
        }
        let Some(identity) = inspected_process_identity(pid)? else {
            continue;
        };
        let Some(exe) = process.exe() else {
            continue;
        };
        let app_process = AppProcess {
            exe: exe.to_path_buf(),
            command: process.cmd().to_vec(),
            command_inspected: !process.cmd().is_empty(),
            cwd: process.cwd().map(Path::to_path_buf),
        };
        if matches_process(&app_process)? && inspected_identity_still_matches(identity)? {
            identities.push(identity);
        }
    }

    Ok(identities)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn app_process_identities_impl<F, E, C>(
    _protected_pid: u32,
    _matches_process: &F,
    _executable_may_match: &E,
    _command_may_match: &C,
) -> Result<Vec<ProcessIdentity>>
where
    F: Fn(&AppProcess) -> Result<bool>,
    E: Fn(&Path) -> Result<bool>,
    C: Fn(&[OsString], Option<&Path>) -> Result<bool>,
{
    Err(SurgeError::Platform(
        "Active application process discovery is unsupported on this Unix platform".to_string(),
    ))
}

#[cfg(unix)]
fn inspected_process_identity(pid: u32) -> Result<Option<ProcessIdentity>> {
    process_identity(pid).map_err(|error| {
        SurgeError::Platform(format!(
            "Failed to inspect process {pid} generation before application swap: {error}"
        ))
    })
}

#[cfg(unix)]
fn inspected_identity_still_matches(identity: ProcessIdentity) -> Result<bool> {
    process_identity_matches(identity).map_err(|error| {
        SurgeError::Platform(format!(
            "Failed to revalidate process {} generation before application swap: {error}",
            identity.pid
        ))
    })
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

    #[test]
    fn effective_uid_is_read_from_status_credentials() {
        let status = "Name:\tdemo\nUid:\t1000\t1001\t1002\t1003\nGid:\t1000\t1000\t1000\t1000\n";
        assert_eq!(parse_linux_effective_uid(status), Some(1001));
        assert_eq!(parse_linux_effective_uid("Name:\tdemo\n"), None);
    }

    #[test]
    fn process_inspection_failures_are_not_treated_as_exit_races() {
        let exited = std::io::Error::from_raw_os_error(nix::libc::ENOENT);
        let denied = std::io::Error::from_raw_os_error(nix::libc::EACCES);

        assert!(process_exited_during_inspection(&exited));
        assert!(!process_exited_during_inspection(&denied));
        assert!(
            process_inspection_error(42, "command line", &denied)
                .to_string()
                .contains("Failed to inspect process 42 command line")
        );
    }

    #[test]
    fn proc_arguments_preserve_empty_positions_and_drop_only_the_terminator() {
        assert_eq!(
            parse_proc_cmdline(b"\0/opt/demo/app/demo\0\0"),
            vec![OsString::new(), OsString::from("/opt/demo/app/demo"), OsString::new()]
        );
        assert!(parse_proc_cmdline(b"").is_empty());
    }

    #[test]
    fn proc_exe_deleted_suffix_is_ignored_for_matching() {
        assert_eq!(
            normalize_proc_exe_path(PathBuf::from("/opt/demo/app-1.0.0/demo (deleted)")),
            PathBuf::from("/opt/demo/app-1.0.0/demo")
        );
    }

    #[test]
    fn proc_exe_path_preserves_existing_filename_with_deleted_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let executable = tmp.path().join("demo (deleted)");
        std::fs::write(&executable, "fixture").unwrap();

        assert_eq!(normalize_proc_exe_path(executable.clone()), executable);
    }
}
