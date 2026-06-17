#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_wrap)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus};

use clap::{Parser, Subcommand};
use surge_core::supervisor::state::{supervisor_pid_file, supervisor_stop_file};
#[cfg(windows)]
use sysinfo::{Pid, ProcessesToUpdate, System};
use thiserror::Error;

mod handoff;
mod ownership;

#[cfg(all(test, unix))]
use ownership::current_supervisor_owns_pid_file;
use ownership::{remove_owned_supervisor_state, supervisor_was_superseded};

const RESTART_HANDOFF_STABILITY_WINDOW: std::time::Duration = std::time::Duration::from_secs(4);

#[derive(Parser)]
#[command(
    name = "surge-supervisor",
    version,
    about = "Process supervisor for the Surge update framework"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Supervise a child process with crash-restart and signal handling
    Run {
        /// Unique supervisor identifier
        #[arg(long)]
        id: String,

        /// Application install directory
        #[arg(long)]
        dir: PathBuf,

        /// Path to the application executable
        #[arg(long)]
        exe: PathBuf,

        /// Arguments to pass to the child process
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,

        /// Enable verbose logging
        #[arg(long, short = 'v')]
        verbose: bool,
    },

    /// Watch an already-running process and relaunch the app after it exits
    Watch {
        /// Unique supervisor identifier
        #[arg(long)]
        id: String,

        /// Application install directory
        #[arg(long)]
        dir: PathBuf,

        /// PID of the currently running application process
        #[arg(long)]
        pid: u32,

        /// Target version whose update handoff should converge after replacement child startup
        #[arg(long)]
        handoff_version: Option<String>,

        /// Path to the application executable
        #[arg(long)]
        exe: PathBuf,

        /// Arguments to pass when launching the replacement child process
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,

        /// Enable verbose logging
        #[arg(long, short = 'v')]
        verbose: bool,
    },

    /// Print SHA-256 hash of a file
    #[command(name = "sha256")]
    Sha256 {
        /// File to hash
        file: PathBuf,
    },
}

#[derive(Debug, Error)]
enum SupervisorError {
    #[error("Executable not found: {0}")]
    ExecutableNotFound(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

fn init_tracing(verbose: bool) {
    let filter = if verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .init();
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            id,
            dir,
            exe,
            args,
            verbose,
        } => {
            init_tracing(verbose);
            if let Err(e) = run_supervisor(&id, &dir, &exe, &args, None, None) {
                tracing::error!("{e}");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Commands::Watch {
            id,
            dir,
            pid,
            handoff_version,
            exe,
            args,
            verbose,
        } => {
            init_tracing(verbose);
            if let Err(e) = run_supervisor(&id, &dir, &exe, &args, Some(pid), handoff_version.as_deref()) {
                tracing::error!("{e}");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Commands::Sha256 { file } => match surge_core::crypto::sha256::sha256_hex_file(&file) {
            Ok(hash) => {
                println!("{hash}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        },
    }
}

fn run_supervisor(
    supervisor_id: &str,
    install_dir: &Path,
    exe_path: &Path,
    args: &[String],
    watched_pid: Option<u32>,
    handoff_version: Option<&str>,
) -> Result<(), SupervisorError> {
    tracing::info!(
        "Surge supervisor '{supervisor_id}' starting, exe: {}",
        exe_path.display()
    );

    if !exe_path.is_file() {
        return Err(SupervisorError::ExecutableNotFound(exe_path.display().to_string()));
    }

    let pid_file = supervisor_pid_file(install_dir, supervisor_id);
    let stop_file = supervisor_stop_file(install_dir, supervisor_id);

    if stop_file.exists() {
        let _ = std::fs::remove_file(&stop_file);
    }
    let own_pid = std::process::id();
    write_pid_file(&pid_file, own_pid)?;

    let shutdown = install_signal_handlers();

    // On the first child start, all args are passed in their original order.
    // After that, one-shot lifecycle args are drained so crash-restarts don't
    // re-fire lifecycle callbacks.
    let first_child_args = args.to_vec();
    let restart_args = handoff::without_lifecycle_args(args);
    let mut next_child_args = Some(first_child_args);
    let mut pending_handoff_version = handoff::pending_restart_handoff_version(watched_pid, handoff_version, args);
    let mut watched_pid = watched_pid;

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) || stop_file.exists() {
            tracing::info!("Shutdown signal received, exiting supervisor loop");
            break;
        }

        if supervisor_was_superseded(&pid_file, own_pid) {
            break;
        }

        if let Some(pid) = watched_pid.take() {
            tracing::info!("Watching running process PID {pid} before relaunch");
            match wait_for_pid_or_stop(pid, &shutdown, &stop_file, &pid_file, own_pid) {
                WaitOutcome::ObservedProcessExited => {
                    if supervisor_was_superseded(&pid_file, own_pid) {
                        break;
                    }
                    tracing::info!("Observed process PID {pid} exited, starting replacement child");
                }
                WaitOutcome::StopRequested => {
                    tracing::info!("Stop requested, exiting supervisor loop and leaving watched process running");
                    break;
                }
                WaitOutcome::ShutdownRequested => {
                    tracing::info!("Shutdown signal received, supervisor loop is exiting");
                    break;
                }
                WaitOutcome::Superseded => {
                    break;
                }
                WaitOutcome::Exited(_) => unreachable!(),
            }
        }

        if supervisor_was_superseded(&pid_file, own_pid) {
            break;
        }

        let child_args = next_child_args.take().unwrap_or_else(|| restart_args.clone());

        tracing::info!("Starting child process: {}", exe_path.display());

        let mut child = Command::new(exe_path)
            .current_dir(install_dir)
            .args(&child_args)
            .spawn()?;

        tracing::info!("Child process started with PID {}", child.id());

        let Some(status) = wait_for_supervised_child(
            &mut child,
            &shutdown,
            &stop_file,
            &pid_file,
            own_pid,
            install_dir,
            &mut pending_handoff_version,
        )?
        else {
            break;
        };

        if supervisor_was_superseded(&pid_file, own_pid) {
            break;
        }

        if shutdown.load(std::sync::atomic::Ordering::Acquire) || stop_file.exists() {
            tracing::info!("Child exited with {status} after shutdown signal, not restarting");
            break;
        }

        if status.success() {
            tracing::info!("Child exited successfully (code 0), not restarting");
            break;
        }

        tracing::warn!("Child exited with {status}, restarting in 2 seconds...");
        if !wait_before_restart(
            &shutdown,
            &stop_file,
            &pid_file,
            own_pid,
            std::time::Duration::from_secs(2),
        ) {
            break;
        }
    }

    remove_owned_supervisor_state(&pid_file, &stop_file, own_pid);

    tracing::info!("Supervisor '{supervisor_id}' exiting");
    Ok(())
}

fn wait_for_supervised_child(
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

fn write_pid_file(path: &Path, own_pid: u32) -> Result<(), SupervisorError> {
    std::fs::write(path, own_pid.to_string())?;
    tracing::debug!("Wrote PID file: {} (pid={})", path.display(), own_pid);
    Ok(())
}

enum WaitOutcome {
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

fn wait_for_pid_or_stop(
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

fn wait_before_restart(
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

fn install_signal_handlers() -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    #[cfg(unix)]
    {
        use signal_hook::consts::signal::{SIGINT, SIGTERM};
        use signal_hook::flag;

        if let Err(e) = flag::register(SIGTERM, shutdown.clone()) {
            tracing::error!("Failed to register SIGTERM handler: {e}");
        }

        if let Err(e) = flag::register(SIGINT, shutdown.clone()) {
            tracing::error!("Failed to register SIGINT handler: {e}");
        }
    }

    #[cfg(windows)]
    {
        let shutdown_clone = shutdown.clone();
        ctrlc_handler(shutdown_clone);
    }

    shutdown
}

#[cfg(windows)]
fn ctrlc_handler(shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if shutdown.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::path::{Path, PathBuf};

    use super::*;

    #[cfg(unix)]
    struct TestInstallDir {
        path: PathBuf,
    }

    #[cfg(unix)]
    impl TestInstallDir {
        fn new(name: &str) -> Self {
            let unique = format!(
                "{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let path = std::env::temp_dir().join(unique);
            std::fs::create_dir(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    #[cfg(unix)]
    impl Drop for TestInstallDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[cfg(unix)]
    fn wait_until(timeout: std::time::Duration, mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if predicate() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        predicate()
    }

    #[test]
    fn watch_command_accepts_handoff_version_before_trailing_args() {
        let cli = Cli::try_parse_from([
            "surge-supervisor",
            "watch",
            "--id",
            "demo-supervisor",
            "--dir",
            "/tmp/demo",
            "--pid",
            "42",
            "--handoff-version",
            "2.0.0",
            "--exe",
            "/tmp/demo/demo-app",
            "--",
            "--app-mode",
        ])
        .unwrap();

        let Commands::Watch {
            handoff_version, args, ..
        } = cli.command
        else {
            panic!("expected watch command");
        };

        assert_eq!(handoff_version.as_deref(), Some("2.0.0"));
        assert_eq!(args, vec!["--app-mode"]);
    }

    #[cfg(unix)]
    #[test]
    fn watched_supervisor_exits_when_pid_file_is_overwritten_before_watched_pid_exits() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TestInstallDir::new("surge-supervisor-superseded-watch");
        let install_dir = tmp.path();
        let child_started = install_dir.join("target-child-started");
        let exe_path = install_dir.join("target-child");
        std::fs::write(
            &exe_path,
            format!(
                "#!/bin/sh\n\
                 echo started > '{}'\n",
                child_started.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&exe_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&exe_path, permissions).unwrap();

        let mut watched_child = Command::new("sh").arg("-c").arg("sleep 5").spawn().unwrap();
        let watched_pid = watched_child.id();
        let pid_file = supervisor_pid_file(install_dir, "demo-supervisor");
        let stop_file = supervisor_stop_file(install_dir, "demo-supervisor");
        let replacement_pid = if std::process::id() == 1 { 2 } else { 1 };
        let install_dir_for_thread = install_dir.to_path_buf();
        let exe_path_for_thread = exe_path.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let supervisor = std::thread::spawn(move || {
            let result = run_supervisor(
                "demo-supervisor",
                &install_dir_for_thread,
                &exe_path_for_thread,
                &[],
                Some(watched_pid),
                None,
            );
            tx.send(result).unwrap();
        });

        if !wait_until(std::time::Duration::from_secs(3), || {
            current_supervisor_owns_pid_file(&pid_file, std::process::id())
        }) {
            let _ = std::fs::write(&stop_file, "stop");
            let _ = watched_child.kill();
            let _ = watched_child.wait();
            supervisor.join().unwrap();
            panic!("supervisor did not write its pid file before the test timed out");
        }

        std::fs::write(&pid_file, replacement_pid.to_string()).unwrap();
        let Ok(supervisor_result) = rx.recv_timeout(std::time::Duration::from_secs(3)) else {
            let _ = std::fs::write(&stop_file, "stop");
            let _ = watched_child.kill();
            let _ = watched_child.wait();
            supervisor.join().unwrap();
            panic!("superseded supervisor did not exit before the watched process exited");
        };
        supervisor.join().unwrap();
        supervisor_result.unwrap();

        let _ = watched_child.kill();
        let _ = watched_child.wait();

        assert!(
            !child_started.exists(),
            "superseded supervisor must not start a replacement child"
        );
        assert_eq!(
            std::fs::read_to_string(pid_file).unwrap().trim(),
            replacement_pid.to_string()
        );
    }

    #[cfg(unix)]
    #[test]
    fn unowned_supervisor_cleanup_preserves_new_owner_files() {
        let tmp = TestInstallDir::new("surge-supervisor-unowned-cleanup");
        let install_dir = tmp.path();
        let pid_file = supervisor_pid_file(install_dir, "demo-supervisor");
        let stop_file = supervisor_stop_file(install_dir, "demo-supervisor");
        let replacement_pid = if std::process::id() == 1 { 2 } else { 1 };

        std::fs::write(&pid_file, replacement_pid.to_string()).unwrap();
        std::fs::write(&stop_file, "stop").unwrap();

        remove_owned_supervisor_state(&pid_file, &stop_file, std::process::id());

        assert_eq!(
            std::fs::read_to_string(pid_file).unwrap().trim(),
            replacement_pid.to_string()
        );
        assert_eq!(std::fs::read_to_string(stop_file).unwrap(), "stop");
    }

    #[cfg(unix)]
    #[test]
    fn owned_supervisor_cleanup_removes_owned_state_files() {
        let tmp = TestInstallDir::new("surge-supervisor-owned-cleanup");
        let install_dir = tmp.path();
        let pid_file = supervisor_pid_file(install_dir, "demo-supervisor");
        let stop_file = supervisor_stop_file(install_dir, "demo-supervisor");

        std::fs::write(&pid_file, format!("{}\n", std::process::id())).unwrap();
        std::fs::write(&stop_file, "stop").unwrap();

        remove_owned_supervisor_state(&pid_file, &stop_file, std::process::id());

        assert!(!pid_file.exists());
        assert!(!stop_file.exists());
    }

    #[cfg(unix)]
    #[test]
    fn run_supervisor_preserves_first_run_flag_value_order() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TestInstallDir::new("surge-supervisor-first-run-order");
        let install_dir = tmp.path();
        let args_path = install_dir.join("args.txt");
        let exe_path = install_dir.join("target-child");
        std::fs::write(
            &exe_path,
            format!(
                "#!/bin/sh\n\
                 printf '%s\\n' \"$@\" > '{}'\n",
                args_path.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&exe_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&exe_path, permissions).unwrap();

        run_supervisor(
            "demo-supervisor",
            install_dir,
            &exe_path,
            &[
                "--app-mode".to_string(),
                "service".to_string(),
                "--surge-first-run".to_string(),
                "2.0.0".to_string(),
            ],
            None,
            None,
        )
        .unwrap();

        let args = std::fs::read_to_string(args_path).unwrap();
        assert_eq!(args, "--app-mode\nservice\n--surge-first-run\n2.0.0\n");
    }

    #[cfg(unix)]
    #[test]
    fn watched_handoff_converges_when_target_child_survives_stability_window() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TestInstallDir::new("surge-supervisor-handoff-converges");
        let install_dir = tmp.path();
        let child_started = install_dir.join("target-child-started");
        let exe_path = install_dir.join("target-child");
        std::fs::write(
            &exe_path,
            format!(
                "#!/bin/sh\n\
                 echo started > '{}'\n\
                 sleep 5\n",
                child_started.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&exe_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&exe_path, permissions).unwrap();

        let pending = surge_core::update::status::UpdateStatusRecord::pending_restart_with_failure_phase(
            "demo-app",
            "2.0.0",
            "2.0.0",
            "stable",
            "2026-05-20T10:00:00Z".to_string(),
            "2026-05-20T10:00:01Z".to_string(),
            "waiting for old child",
            surge_core::update::status::RESTART_HANDOFF_WAITING_FOR_OLD_CHILD_PHASE,
        );
        surge_core::update::status::write_update_status(install_dir, &pending).unwrap();

        let mut watched_child = Command::new("sh").arg("-c").arg("sleep 0.1").spawn().unwrap();
        let watched_pid = watched_child.id();
        watched_child.wait().unwrap();
        run_supervisor(
            "demo-supervisor",
            install_dir,
            &exe_path,
            &[],
            Some(watched_pid),
            Some("2.0.0"),
        )
        .unwrap();

        assert!(child_started.exists(), "replacement child should have started");
        let status = surge_core::update::status::read_update_status(install_dir)
            .unwrap()
            .expect("status record should remain present");
        assert_eq!(
            status.state,
            surge_core::update::status::UpdateConvergenceState::Converged
        );
        assert_eq!(status.target_version, "2.0.0");
        assert_eq!(status.installed_version, "2.0.0");
        assert!(status.supervisor_restart_confirmed);
        assert_eq!(status.failure_phase, None);
        assert_eq!(status.reason, None);
    }
}
