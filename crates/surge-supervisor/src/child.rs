use std::path::Path;
use std::process::{Child, Command, ExitStatus};

#[cfg(windows)]
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::SupervisorError;
use crate::handoff;
use crate::ownership::supervisor_was_superseded;

const RESTART_HANDOFF_STABILITY_WINDOW: std::time::Duration = std::time::Duration::from_secs(4);

pub(crate) struct StopTriggers<'a> {
    legacy_stop_file: &'a Path,
    takeover_request_file: Option<&'a Path>,
}

impl<'a> StopTriggers<'a> {
    pub(crate) fn new(legacy_stop_file: &'a Path, takeover_request_file: Option<&'a Path>) -> Self {
        Self {
            legacy_stop_file,
            takeover_request_file,
        }
    }

    pub(crate) fn requested(&self) -> bool {
        self.legacy_requested() || self.takeover_requested()
    }

    pub(crate) fn legacy_requested(&self) -> bool {
        self.legacy_stop_file.exists()
    }

    pub(crate) fn takeover_requested(&self) -> bool {
        self.takeover_request_file.is_some_and(Path::exists)
    }
}

pub(crate) fn spawn_supervised_child(
    exe_path: &Path,
    install_dir: &Path,
    child_args: &[String],
) -> Result<Child, SupervisorError> {
    tracing::info!("Starting child process: {}", exe_path.display());

    let mut command = Command::new(exe_path);
    command.current_dir(install_dir).args(child_args);
    surge_core::platform::process::close_inherited_descriptors(&mut command);

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
    stop_triggers: &StopTriggers<'_>,
    pid_file: &Path,
    own_pid: u32,
    install_dir: &Path,
    pending_handoff_version: &mut Option<String>,
) -> Result<SupervisedChildOutcome, SupervisorError> {
    let Some(version) = pending_handoff_version.clone() else {
        return wait_for_child_exit_status(child, shutdown, stop_triggers, pid_file, own_pid);
    };

    match wait_for_child_startup_or_stop(
        child,
        shutdown,
        stop_triggers,
        pid_file,
        own_pid,
        RESTART_HANDOFF_STABILITY_WINDOW,
    )? {
        StartupOutcome::Running => {
            handoff::record_restart_handoff_converged(install_dir, &version);
            *pending_handoff_version = None;
            wait_for_child_exit_status(child, shutdown, stop_triggers, pid_file, own_pid)
        }
        StartupOutcome::Exited(status) => {
            handoff::record_restart_handoff_child_exited(install_dir, &version, status);
            Ok(SupervisedChildOutcome::Exited(status))
        }
        StartupOutcome::StopRequested => {
            tracing::info!("Stop requested, exiting supervisor loop and leaving child running");
            Ok(SupervisedChildOutcome::StopRequested)
        }
        StartupOutcome::ShutdownRequested => {
            tracing::info!("Shutdown signal received, child terminated and supervisor loop is exiting");
            Ok(SupervisedChildOutcome::ShutdownRequested)
        }
        StartupOutcome::Superseded => Ok(SupervisedChildOutcome::Superseded),
    }
}

fn wait_for_child_exit_status(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_triggers: &StopTriggers<'_>,
    pid_file: &Path,
    own_pid: u32,
) -> Result<SupervisedChildOutcome, SupervisorError> {
    match wait_for_child_or_stop(child, shutdown, stop_triggers, pid_file, own_pid)? {
        WaitOutcome::Exited(status) => Ok(SupervisedChildOutcome::Exited(status)),
        WaitOutcome::ObservedProcessExited => unreachable!(),
        WaitOutcome::StopRequested => {
            tracing::info!("Stop requested, exiting supervisor loop and leaving child running");
            Ok(SupervisedChildOutcome::StopRequested)
        }
        WaitOutcome::ShutdownRequested => {
            tracing::info!("Shutdown signal received, child terminated and supervisor loop is exiting");
            Ok(SupervisedChildOutcome::ShutdownRequested)
        }
        WaitOutcome::Superseded => Ok(SupervisedChildOutcome::Superseded),
    }
}

#[derive(Debug)]
pub(crate) enum SupervisedChildOutcome {
    Exited(ExitStatus),
    StopRequested,
    ShutdownRequested,
    Superseded,
}

pub(crate) enum WaitOutcome {
    Exited(std::process::ExitStatus),
    ObservedProcessExited,
    StopRequested,
    ShutdownRequested,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestartWaitOutcome {
    DelayElapsed,
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
    stop_triggers: &StopTriggers<'_>,
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

        if stop_triggers.requested() {
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
    stop_triggers: &StopTriggers<'_>,
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

        if stop_triggers.requested() {
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
    stop_triggers: &StopTriggers<'_>,
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

        if stop_triggers.requested() {
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
    stop_triggers: &StopTriggers<'_>,
    pid_file: &Path,
    own_pid: u32,
    delay: std::time::Duration,
) -> RestartWaitOutcome {
    let deadline = std::time::Instant::now() + delay;
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            tracing::info!("Shutdown signal received during restart delay, not restarting");
            return RestartWaitOutcome::ShutdownRequested;
        }

        if supervisor_was_superseded(pid_file, own_pid) {
            return RestartWaitOutcome::Superseded;
        }

        if stop_triggers.requested() {
            tracing::info!("Stop requested during restart delay, not restarting");
            return RestartWaitOutcome::StopRequested;
        }

        if std::time::Instant::now() >= deadline {
            return RestartWaitOutcome::DelayElapsed;
        }

        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

pub(crate) fn terminate_child_process(child: &mut Child) -> Result<(), SupervisorError> {
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn shutdown_wins_when_stop_and_shutdown_are_simultaneous() {
        let dir = tempfile::tempdir().unwrap();
        let stop_file = dir.path().join("stop");
        let pid_file = dir.path().join("pid");
        let own_pid = std::process::id();
        std::fs::write(&stop_file, "stop").unwrap();
        std::fs::write(&pid_file, own_pid.to_string()).unwrap();
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        let mut pending_handoff = None;
        let stop_triggers = StopTriggers::new(&stop_file, None);

        let outcome = wait_for_supervised_child(
            &mut child,
            &shutdown,
            &stop_triggers,
            &pid_file,
            own_pid,
            dir.path(),
            &mut pending_handoff,
        )
        .unwrap();

        assert!(matches!(outcome, SupervisedChildOutcome::ShutdownRequested));
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn restart_delay_reports_stop_request() {
        let dir = tempfile::tempdir().unwrap();
        let stop_file = dir.path().join("stop");
        let pid_file = dir.path().join("pid");
        let own_pid = std::process::id();
        std::fs::write(&stop_file, "stop").unwrap();
        std::fs::write(&pid_file, own_pid.to_string()).unwrap();
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_triggers = StopTriggers::new(&stop_file, None);

        let outcome = wait_before_restart(
            &shutdown,
            &stop_triggers,
            &pid_file,
            own_pid,
            std::time::Duration::from_secs(1),
        );

        assert_eq!(outcome, RestartWaitOutcome::StopRequested);
    }
}

#[cfg(unix)]
pub(crate) fn is_process_running(pid: u32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(raw_pid) = i32::try_from(pid) else {
        return false;
    };

    matches!(kill(Pid::from_raw(raw_pid), None), Ok(()) | Err(Errno::EPERM))
}

#[cfg(windows)]
pub(crate) fn is_process_running(pid: u32) -> bool {
    let watched_pid = Pid::from_u32(pid);
    let mut system = System::new();
    let _ = system.refresh_processes(ProcessesToUpdate::Some(&[watched_pid]), true);
    system.process(watched_pid).is_some()
}
