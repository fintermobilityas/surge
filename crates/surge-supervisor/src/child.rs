use std::path::Path;
use std::process::{Child, Command, ExitStatus};

#[cfg(windows)]
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::SupervisorError;
use crate::handoff;
use crate::ownership::supervisor_was_superseded;

const RESTART_HANDOFF_STABILITY_WINDOW: std::time::Duration = std::time::Duration::from_secs(4);

pub(crate) fn spawn_supervised_child(
    exe_path: &Path,
    install_dir: &Path,
    child_args: &[String],
) -> Result<Child, SupervisorError> {
    tracing::info!("Starting child process: {}", exe_path.display());

    let mut command = Command::new(exe_path);
    command.current_dir(install_dir).args(child_args);

    // Put the child in its own process group so a group-scoped signal or
    // `pkill -g` aimed at the supervisor cannot also take down the child.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    let child = command.spawn()?;
    tracing::info!("Child process started with PID {}", child.id());
    Ok(child)
}

pub(crate) fn wait_for_supervised_child(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    pid_file: &Path,
    own_pid: u32,
    install_dir: &Path,
    pending_handoff_version: &mut Option<String>,
) -> Result<Option<ExitStatus>, SupervisorError> {
    let Some(version) = pending_handoff_version.clone() else {
        return wait_for_child_exit_status(child, shutdown, stop_file, pid_file, own_pid);
    };

    match wait_for_child_startup_or_stop(
        child,
        shutdown,
        stop_file,
        pid_file,
        own_pid,
        RESTART_HANDOFF_STABILITY_WINDOW,
    )? {
        StartupOutcome::Running => {
            handoff::record_restart_handoff_converged(install_dir, &version);
            *pending_handoff_version = None;
            wait_for_child_exit_status(child, shutdown, stop_file, pid_file, own_pid)
        }
        StartupOutcome::Exited(status) => {
            handoff::record_restart_handoff_child_exited(install_dir, &version, status);
            Ok(Some(status))
        }
        StartupOutcome::StopRequested => {
            tracing::info!("Stop requested, exiting supervisor loop and leaving child running");
            Ok(None)
        }
        StartupOutcome::ShutdownRequested => {
            tracing::info!("Shutdown signal received, child terminated and supervisor loop is exiting");
            Ok(None)
        }
        StartupOutcome::Superseded => Ok(None),
    }
}

fn wait_for_child_exit_status(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    pid_file: &Path,
    own_pid: u32,
) -> Result<Option<ExitStatus>, SupervisorError> {
    match wait_for_child_or_stop(child, shutdown, stop_file, pid_file, own_pid)? {
        WaitOutcome::Exited(status) => Ok(Some(status)),
        WaitOutcome::ObservedProcessExited => unreachable!(),
        WaitOutcome::StopRequested => {
            tracing::info!("Stop requested, exiting supervisor loop and leaving child running");
            Ok(None)
        }
        WaitOutcome::ShutdownRequested => {
            tracing::info!("Shutdown signal received, child terminated and supervisor loop is exiting");
            Ok(None)
        }
        WaitOutcome::Superseded => Ok(None),
    }
}

pub(crate) enum WaitOutcome {
    Exited(std::process::ExitStatus),
    ObservedProcessExited,
    StopRequested,
    ShutdownRequested,
    Superseded,
}

enum StartupOutcome {
    Running,
    Exited(std::process::ExitStatus),
    StopRequested,
    ShutdownRequested,
    Superseded,
}

fn wait_for_child_startup_or_stop(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    pid_file: &Path,
    own_pid: u32,
    stable_for: std::time::Duration,
) -> Result<StartupOutcome, SupervisorError> {
    let deadline = std::time::Instant::now() + stable_for;
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            terminate_child_process(child)?;
            return Ok(StartupOutcome::ShutdownRequested);
        }

        if supervisor_was_superseded(pid_file, own_pid) {
            return Ok(StartupOutcome::Superseded);
        }

        if stop_file.exists() {
            return Ok(StartupOutcome::StopRequested);
        }

        if let Some(status) = child.try_wait()? {
            return Ok(StartupOutcome::Exited(status));
        }

        if std::time::Instant::now() >= deadline {
            return Ok(StartupOutcome::Running);
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

pub(crate) fn wait_for_pid_or_stop(
    pid: u32,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    pid_file: &Path,
    own_pid: u32,
) -> WaitOutcome {
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return WaitOutcome::ShutdownRequested;
        }

        if supervisor_was_superseded(pid_file, own_pid) {
            return WaitOutcome::Superseded;
        }

        if stop_file.exists() {
            return WaitOutcome::StopRequested;
        }

        if !is_process_running(pid) {
            return WaitOutcome::ObservedProcessExited;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn wait_for_child_or_stop(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    pid_file: &Path,
    own_pid: u32,
) -> Result<WaitOutcome, SupervisorError> {
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            terminate_child_process(child)?;
            return Ok(WaitOutcome::ShutdownRequested);
        }

        if supervisor_was_superseded(pid_file, own_pid) {
            return Ok(WaitOutcome::Superseded);
        }

        if stop_file.exists() {
            return Ok(WaitOutcome::StopRequested);
        }

        if let Some(status) = child.try_wait()? {
            return Ok(WaitOutcome::Exited(status));
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

pub(crate) fn wait_before_restart(
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    pid_file: &Path,
    own_pid: u32,
    delay: std::time::Duration,
) -> bool {
    let deadline = std::time::Instant::now() + delay;
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            tracing::info!("Shutdown signal received during restart delay, not restarting");
            return false;
        }

        if supervisor_was_superseded(pid_file, own_pid) {
            return false;
        }

        if stop_file.exists() {
            tracing::info!("Stop requested during restart delay, not restarting");
            return false;
        }

        if std::time::Instant::now() >= deadline {
            return true;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

fn terminate_child_process(child: &mut Child) -> Result<(), SupervisorError> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let _ = kill(Pid::from_raw(child.id() as i32), Signal::SIGTERM);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[cfg(not(unix))]
    {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
    }

    let _ = child.kill();
    let _ = child.wait();
    Ok(())
}

#[cfg(unix)]
fn is_process_running(pid: u32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(raw_pid) = i32::try_from(pid) else {
        return false;
    };

    matches!(kill(Pid::from_raw(raw_pid), None), Ok(()) | Err(Errno::EPERM))
}

#[cfg(windows)]
fn is_process_running(pid: u32) -> bool {
    let watched_pid = Pid::from_u32(pid);
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[watched_pid]), true);
    system.process(watched_pid).is_some()
}
