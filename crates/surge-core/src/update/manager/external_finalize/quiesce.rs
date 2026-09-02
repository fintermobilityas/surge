use std::path::{Path, PathBuf};
use std::time::Duration;

use sysinfo::{Pid, ProcessStatus, ProcessesToUpdate, System};

use crate::error::{Result, SurgeError};
use crate::platform::process::{PidLiveness, probe_process_identity, process_start_time};

const UPDATER_EXIT_GRACE: Duration = Duration::from_secs(20);
const TERMINATE_GRACE: Duration = Duration::from_secs(5);
const KILL_GRACE: Duration = Duration::from_secs(2);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessIdentity {
    pid: u32,
    start_time: u64,
    executable: PathBuf,
}

pub(super) fn quiesce_updating_application(
    updater_pid: u32,
    updater_start_time: u64,
    updater_exe: &Path,
) -> Result<()> {
    quiesce_updating_application_with_timeouts(
        updater_pid,
        updater_start_time,
        updater_exe,
        UPDATER_EXIT_GRACE,
        TERMINATE_GRACE,
        KILL_GRACE,
    )
}

fn quiesce_updating_application_with_timeouts(
    updater_pid: u32,
    updater_start_time: u64,
    updater_exe: &Path,
    exit_grace: Duration,
    terminate_grace: Duration,
    kill_grace: Duration,
) -> Result<()> {
    let expected_executable = normalize_executable(updater_exe);
    let updater = ProcessIdentity {
        pid: updater_pid,
        start_time: updater_start_time,
        executable: expected_executable.clone(),
    };
    if wait_until_identity_exits(&updater, exit_grace)? {
        tracing::info!(pid = updater_pid, "Updating application exited before finalization");
    }

    let mut remaining = matching_processes(&expected_executable)?;
    include_running_identity(&mut remaining, &updater)?;
    if remaining.is_empty() {
        return Ok(());
    }

    for process in &remaining {
        signal_process(process, false)?;
    }
    if wait_until_no_matching_processes(&expected_executable, &updater, terminate_grace)? {
        tracing::info!(
            count = remaining.len(),
            "Stopped updating application processes before swap"
        );
        return Ok(());
    }

    remaining = matching_processes(&expected_executable)?;
    include_running_identity(&mut remaining, &updater)?;
    for process in &remaining {
        signal_process(process, true)?;
    }
    if wait_until_no_matching_processes(&expected_executable, &updater, kill_grace)? {
        tracing::warn!(
            count = remaining.len(),
            "Force-stopped updating application processes before swap"
        );
        return Ok(());
    }

    Err(SurgeError::Supervisor(format!(
        "Timed out waiting for processes running '{}' to exit before the application swap",
        expected_executable.display()
    )))
}

fn include_running_identity(processes: &mut Vec<ProcessIdentity>, identity: &ProcessIdentity) -> Result<()> {
    if identity_is_running(identity)?
        && !processes
            .iter()
            .any(|process| process.pid == identity.pid && process.start_time == identity.start_time)
    {
        processes.push(identity.clone());
    }
    Ok(())
}

fn matching_processes(expected_executable: &Path) -> Result<Vec<ProcessIdentity>> {
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::All, true);
    let own_pid = std::process::id();
    let mut matching = Vec::new();
    for (pid, process) in system.processes() {
        let pid = pid.as_u32();
        if pid == own_pid {
            continue;
        }
        let Some(executable) = process.exe().map(normalize_executable) else {
            continue;
        };
        if !executable_paths_equal(&executable, expected_executable) {
            continue;
        }
        let start_time = process_start_time(pid).ok_or_else(|| {
            SurgeError::Supervisor(format!(
                "Could not resolve creation identity for application process {pid}"
            ))
        })?;
        matching.push(ProcessIdentity {
            pid,
            start_time,
            executable,
        });
    }
    Ok(matching)
}

fn wait_until_identity_exits(identity: &ProcessIdentity, timeout: Duration) -> Result<bool> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let metadata_error = match identity_is_running(identity) {
            Ok(false) => return Ok(true),
            Ok(true) => None,
            Err(error) => Some(error),
        };
        if std::time::Instant::now() >= deadline {
            return metadata_error.map_or(Ok(false), Err);
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn wait_until_no_matching_processes(
    expected_executable: &Path,
    updater: &ProcessIdentity,
    timeout: Duration,
) -> Result<bool> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let metadata_error = match identity_is_running(updater) {
            Ok(false) => {
                if matching_processes(expected_executable)?.is_empty() {
                    return Ok(true);
                }
                None
            }
            Ok(true) => None,
            Err(error) => Some(error),
        };
        if std::time::Instant::now() >= deadline {
            return metadata_error.map_or(Ok(false), Err);
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn identity_is_running(identity: &ProcessIdentity) -> Result<bool> {
    match probe_process_identity(identity.pid, identity.start_time) {
        PidLiveness::Dead => return Ok(false),
        PidLiveness::Unknown => {
            return Err(SurgeError::Supervisor(format!(
                "Could not revalidate creation identity for process {}",
                identity.pid
            )));
        }
        PidLiveness::Alive => {}
    }

    let system_pid = Pid::from_u32(identity.pid);
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[system_pid]), true);
    let process = system.process(system_pid).ok_or_else(|| {
        SurgeError::Supervisor(format!(
            "Process {} is alive but missing from the process metadata snapshot",
            identity.pid
        ))
    })?;
    if matches!(process.status(), ProcessStatus::Dead | ProcessStatus::Zombie) {
        return Ok(false);
    }
    let executable = process.exe().ok_or_else(|| {
        SurgeError::Supervisor(format!(
            "Could not resolve executable for live process {}",
            identity.pid
        ))
    })?;
    if !executable_paths_equal(&normalize_executable(executable), &identity.executable) {
        return Err(SurgeError::Supervisor(format!(
            "Executable identity changed for live process {}",
            identity.pid
        )));
    }
    Ok(true)
}

#[cfg(unix)]
fn signal_process(identity: &ProcessIdentity, force: bool) -> Result<()> {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    if !identity_is_running(identity)? {
        return Ok(());
    }
    let raw_pid = i32::try_from(identity.pid)
        .map_err(|_| SurgeError::Supervisor(format!("Process id {} is outside signal range", identity.pid)))?;
    let signal = if force { Signal::SIGKILL } else { Signal::SIGTERM };
    match kill(Pid::from_raw(raw_pid), signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(error) => Err(SurgeError::Supervisor(format!(
            "Failed to signal updating process {}: {error}",
            identity.pid
        ))),
    }
}

#[cfg(windows)]
fn signal_process(identity: &ProcessIdentity, _force: bool) -> Result<()> {
    if !identity_is_running(identity)? {
        return Ok(());
    }
    let system_pid = Pid::from_u32(identity.pid);
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[system_pid]), true);
    let Some(process) = system.process(system_pid) else {
        return Ok(());
    };
    if process.kill() || !identity_is_running(identity)? {
        Ok(())
    } else {
        Err(SurgeError::Supervisor(format!(
            "Failed to stop updating process {}",
            identity.pid
        )))
    }
}

#[cfg(not(any(unix, windows)))]
fn signal_process(_identity: &ProcessIdentity, _force: bool) -> Result<()> {
    Err(SurgeError::Supervisor(
        "External update finalization is unsupported on this platform".to_string(),
    ))
}

fn normalize_executable(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(windows)]
fn executable_paths_equal(left: &Path, right: &Path) -> bool {
    left.to_string_lossy().eq_ignore_ascii_case(&right.to_string_lossy())
}

#[cfg(not(windows))]
fn executable_paths_equal(left: &Path, right: &Path) -> bool {
    left == right
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn quiesce_stops_only_the_exact_updating_executable() {
        let temp = tempfile::tempdir().unwrap();
        let source = if Path::new("/bin/sleep").is_file() {
            Path::new("/bin/sleep")
        } else {
            Path::new("/usr/bin/sleep")
        };
        let executable = temp.path().join("surge-external-finalize-sleep");
        std::fs::copy(source, &executable).unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let mut child = std::process::Command::new(&executable).arg("30").spawn().unwrap();
        let start_time = process_start_time(child.id()).unwrap();

        quiesce_updating_application_with_timeouts(
            child.id(),
            start_time,
            &executable,
            Duration::from_millis(500),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .unwrap();

        assert!(child.wait().unwrap().code().is_none_or(|code| code != 0));
    }

    #[test]
    fn mismatched_process_metadata_fails_closed_without_signalling() {
        let mut child = std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let start_time = process_start_time(child.id()).unwrap();
        let unrelated = tempfile::NamedTempFile::new().unwrap();

        let error = quiesce_updating_application_with_timeouts(
            child.id(),
            start_time,
            unrelated.path(),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap_err();

        assert!(error.to_string().contains("Executable identity changed"));
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
