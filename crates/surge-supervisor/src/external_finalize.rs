use std::path::{Path, PathBuf};
use std::time::Duration;

use surge_core::error::{Result, SurgeError};
use sysinfo::{Pid, ProcessesToUpdate, System};

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

pub(super) fn quiesce_updating_application(updater_pid: u32, updater_exe: &Path) -> Result<()> {
    quiesce_updating_application_with_timeouts(
        updater_pid,
        updater_exe,
        UPDATER_EXIT_GRACE,
        TERMINATE_GRACE,
        KILL_GRACE,
    )
}

fn quiesce_updating_application_with_timeouts(
    updater_pid: u32,
    updater_exe: &Path,
    exit_grace: Duration,
    terminate_grace: Duration,
    kill_grace: Duration,
) -> Result<()> {
    let expected_executable = normalize_executable(updater_exe);
    let original = process_identity(updater_pid, &expected_executable)?;

    if let Some(original) = original
        && wait_until_identity_exits(&original, exit_grace)?
    {
        tracing::info!(pid = updater_pid, "Updating application exited before finalization");
    }

    let mut remaining = matching_processes(&expected_executable)?;
    if remaining.is_empty() {
        return Ok(());
    }

    for process in &remaining {
        signal_process(process, false)?;
    }
    if wait_until_no_matching_processes(&expected_executable, terminate_grace)? {
        tracing::info!(
            count = remaining.len(),
            "Stopped updating application processes before swap"
        );
        return Ok(());
    }

    remaining = matching_processes(&expected_executable)?;
    for process in &remaining {
        signal_process(process, true)?;
    }
    if wait_until_no_matching_processes(&expected_executable, kill_grace)? {
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

fn process_identity(pid: u32, expected_executable: &Path) -> Result<Option<ProcessIdentity>> {
    let system_pid = Pid::from_u32(pid);
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[system_pid]), true);
    let Some(process) = system.process(system_pid) else {
        return Ok(None);
    };
    let Some(executable) = process.exe() else {
        return Err(SurgeError::Supervisor(format!(
            "Could not resolve executable for updating process {pid}"
        )));
    };
    let executable = normalize_executable(executable);
    if !executable_paths_equal(&executable, expected_executable) {
        return Ok(None);
    }
    Ok(Some(ProcessIdentity {
        pid,
        start_time: process.start_time(),
        executable,
    }))
}

fn matching_processes(expected_executable: &Path) -> Result<Vec<ProcessIdentity>> {
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::All, true);
    let own_pid = std::process::id();
    Ok(system
        .processes()
        .iter()
        .filter_map(|(pid, process)| {
            let pid = pid.as_u32();
            if pid == own_pid {
                return None;
            }
            let executable = normalize_executable(process.exe()?);
            executable_paths_equal(&executable, expected_executable).then_some(ProcessIdentity {
                pid,
                start_time: process.start_time(),
                executable,
            })
        })
        .collect())
}

fn wait_until_identity_exits(identity: &ProcessIdentity, timeout: Duration) -> Result<bool> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if !identity_is_running(identity)? {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn wait_until_no_matching_processes(expected_executable: &Path, timeout: Duration) -> Result<bool> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if matching_processes(expected_executable)?.is_empty() {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn identity_is_running(identity: &ProcessIdentity) -> Result<bool> {
    let Some(current) = process_identity(identity.pid, &identity.executable)? else {
        return Ok(false);
    };
    Ok(current.start_time == identity.start_time)
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
    let system_pid = Pid::from_u32(identity.pid);
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[system_pid]), true);
    let Some(process) = system.process(system_pid) else {
        return Ok(());
    };
    let Some(executable) = process.exe() else {
        return Err(SurgeError::Supervisor(format!(
            "Could not revalidate executable for updating process {}",
            identity.pid
        )));
    };
    if process.start_time() != identity.start_time
        || !executable_paths_equal(&normalize_executable(executable), &identity.executable)
    {
        return Ok(());
    }
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
    use super::*;

    #[test]
    fn quiesce_stops_only_the_exact_updating_executable() {
        use std::os::unix::fs::PermissionsExt;

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

        quiesce_updating_application_with_timeouts(
            child.id(),
            &executable,
            Duration::ZERO,
            Duration::from_secs(2),
            Duration::from_secs(1),
        )
        .unwrap();

        assert!(child.wait().unwrap().code().is_none_or(|code| code != 0));
    }

    #[test]
    fn mismatched_executable_does_not_stop_process() {
        let mut child = std::process::Command::new("/bin/sleep").arg("30").spawn().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let unrelated = temp.path().join("not-the-running-process");
        std::fs::write(&unrelated, "fixture").unwrap();

        quiesce_updating_application_with_timeouts(
            child.id(),
            &unrelated,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        child.wait().unwrap();
    }
}
