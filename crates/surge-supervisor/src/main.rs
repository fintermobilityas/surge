#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_wrap)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus};

use clap::{Parser, Subcommand};
use surge_core::supervisor::state::{supervisor_pid_file, supervisor_stop_file};
use surge_core::update::status::{
    RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE, mark_restart_handoff_converged, mark_restart_handoff_pending,
};
#[cfg(windows)]
use sysinfo::{Pid, ProcessesToUpdate, System};
use thiserror::Error;

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
    write_pid_file(&pid_file)?;

    let shutdown = install_signal_handlers();

    // Separate one-shot lifecycle args (--surge-*) from regular args.
    // On the first child start, all args are passed. After that, lifecycle
    // args are drained so crash-restarts don't re-fire lifecycle callbacks.
    let mut lifecycle_args: Vec<String> = args.iter().filter(|a| a.starts_with("--surge-")).cloned().collect();
    let regular_args: Vec<String> = args.iter().filter(|a| !a.starts_with("--surge-")).cloned().collect();
    let mut pending_handoff_version = pending_restart_handoff_version(watched_pid, handoff_version, args);
    let mut watched_pid = watched_pid;

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) || stop_file.exists() {
            tracing::info!("Shutdown signal received, exiting supervisor loop");
            break;
        }

        if let Some(pid) = watched_pid.take() {
            tracing::info!("Watching running process PID {pid} before relaunch");
            match wait_for_pid_or_stop(pid, &shutdown, &stop_file) {
                WaitOutcome::ObservedProcessExited => {
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
                WaitOutcome::Exited(_) => unreachable!(),
            }
        }

        let mut child_args = regular_args.clone();
        child_args.append(&mut lifecycle_args);

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
            install_dir,
            &mut pending_handoff_version,
        )?
        else {
            break;
        };

        if shutdown.load(std::sync::atomic::Ordering::Acquire) || stop_file.exists() {
            tracing::info!("Child exited with {status} after shutdown signal, not restarting");
            break;
        }

        if status.success() {
            tracing::info!("Child exited successfully (code 0), not restarting");
            break;
        }

        tracing::warn!("Child exited with {status}, restarting in 2 seconds...");
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    if pid_file.exists() {
        let _ = std::fs::remove_file(&pid_file);
    }
    if stop_file.exists() {
        let _ = std::fs::remove_file(&stop_file);
    }

    tracing::info!("Supervisor '{supervisor_id}' exiting");
    Ok(())
}

fn pending_restart_handoff_version(
    watched_pid: Option<u32>,
    handoff_version: Option<&str>,
    args: &[String],
) -> Option<String> {
    watched_pid?;
    handoff_version
        .map(str::trim)
        .filter(|version| !version.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| first_run_version(args))
}

fn first_run_version(args: &[String]) -> Option<String> {
    args.iter().enumerate().find_map(|(index, arg)| {
        if arg != "--surge-first-run" {
            return None;
        }
        args.get(index + 1)
            .filter(|candidate| !candidate.starts_with("--"))
            .or_else(|| {
                index
                    .checked_sub(1)
                    .and_then(|prev| args.get(prev))
                    .filter(|candidate| !candidate.starts_with("--"))
            })
            .map(|version| version.trim().to_string())
            .filter(|version| !version.is_empty())
    })
}

fn record_restart_handoff_converged(install_dir: &Path, version: &str) {
    match mark_restart_handoff_converged(install_dir, version) {
        Ok(Some(_)) => {
            tracing::info!(version, "Restart handoff converged after target child startup");
        }
        Ok(None) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = "no matching pending restart handoff status record",
                "Failed to record restart handoff convergence"
            );
        }
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = %e,
                "Failed to record restart handoff convergence"
            );
        }
    }
}

fn record_restart_handoff_child_exited(install_dir: &Path, version: &str, status: ExitStatus) {
    let reason = format!("target child exited with {status} before restart handoff completed");
    match mark_restart_handoff_pending(install_dir, version, &reason, RESTART_HANDOFF_TARGET_CHILD_EXITED_PHASE) {
        Ok(Some(_)) => {
            tracing::warn!(version, %status, "Restart handoff target child exited before startup proof completed");
        }
        Ok(None) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = "no matching pending restart handoff status record",
                "Failed to record restart handoff child exit"
            );
        }
        Err(e) => {
            tracing::warn!(
                install_root = %install_dir.display(),
                version,
                reason = %e,
                "Failed to record restart handoff child exit"
            );
        }
    }
}

fn wait_for_supervised_child(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    install_dir: &Path,
    pending_handoff_version: &mut Option<String>,
) -> Result<Option<ExitStatus>, SupervisorError> {
    let Some(version) = pending_handoff_version.clone() else {
        return wait_for_child_exit_status(child, shutdown, stop_file);
    };

    match wait_for_child_startup_or_stop(child, shutdown, stop_file, RESTART_HANDOFF_STABILITY_WINDOW)? {
        StartupOutcome::Running => {
            record_restart_handoff_converged(install_dir, &version);
            *pending_handoff_version = None;
            wait_for_child_exit_status(child, shutdown, stop_file)
        }
        StartupOutcome::Exited(status) => {
            record_restart_handoff_child_exited(install_dir, &version, status);
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
    }
}

fn wait_for_child_exit_status(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
) -> Result<Option<ExitStatus>, SupervisorError> {
    match wait_for_child_or_stop(child, shutdown, stop_file)? {
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
    }
}

fn write_pid_file(path: &Path) -> Result<(), SupervisorError> {
    let pid = std::process::id();
    std::fs::write(path, pid.to_string())?;
    tracing::debug!("Wrote PID file: {} (pid={})", path.display(), pid);
    Ok(())
}

enum WaitOutcome {
    Exited(std::process::ExitStatus),
    ObservedProcessExited,
    StopRequested,
    ShutdownRequested,
}

enum StartupOutcome {
    Running,
    Exited(std::process::ExitStatus),
    StopRequested,
    ShutdownRequested,
}

fn wait_for_child_startup_or_stop(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
    stable_for: std::time::Duration,
) -> Result<StartupOutcome, SupervisorError> {
    let deadline = std::time::Instant::now() + stable_for;
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            terminate_child_process(child)?;
            return Ok(StartupOutcome::ShutdownRequested);
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
) -> WaitOutcome {
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return WaitOutcome::ShutdownRequested;
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
) -> Result<WaitOutcome, SupervisorError> {
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            terminate_child_process(child)?;
            return Ok(WaitOutcome::ShutdownRequested);
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
    use std::path::{Path, PathBuf};

    use super::*;

    struct TestInstallDir {
        path: PathBuf,
    }

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

    impl Drop for TestInstallDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn pending_handoff_version_uses_explicit_watch_target() {
        let args = vec!["--app-mode".to_string()];

        let version = pending_restart_handoff_version(Some(42), Some(" 2.0.0 "), &args);

        assert_eq!(version.as_deref(), Some("2.0.0"));
    }

    #[test]
    fn pending_handoff_version_falls_back_to_first_run_arg() {
        let args = vec!["--surge-first-run".to_string(), "2.0.0".to_string()];

        let version = pending_restart_handoff_version(Some(42), None, &args);

        assert_eq!(version.as_deref(), Some("2.0.0"));
    }

    #[test]
    fn pending_handoff_version_requires_watched_pid() {
        let args = vec!["--surge-first-run".to_string(), "2.0.0".to_string()];

        let version = pending_restart_handoff_version(None, Some("2.0.0"), &args);

        assert_eq!(version, None);
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
