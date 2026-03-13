#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_wrap)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode};

use clap::{Parser, Subcommand};
use surge_core::supervisor::state::{supervisor_pid_file, supervisor_stop_file};
#[cfg(windows)]
use sysinfo::{Pid, ProcessesToUpdate, System};
use thiserror::Error;

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
            if let Err(e) = run_supervisor(&id, &dir, &exe, &args, None) {
                tracing::error!("{e}");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Commands::Watch {
            id,
            dir,
            pid,
            exe,
            args,
            verbose,
        } => {
            init_tracing(verbose);
            if let Err(e) = run_supervisor(&id, &dir, &exe, &args, Some(pid)) {
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

        let status = match wait_for_child_or_stop(&mut child, &shutdown, &stop_file)? {
            WaitOutcome::Exited(status) => status,
            WaitOutcome::ObservedProcessExited => unreachable!(),
            WaitOutcome::StopRequested => {
                tracing::info!("Stop requested, exiting supervisor loop and leaving child running");
                break;
            }
            WaitOutcome::ShutdownRequested => {
                tracing::info!("Shutdown signal received, child terminated and supervisor loop is exiting");
                break;
            }
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
        use nix::sys::signal::{SigSet, Signal};

        // Block SIGTERM/SIGINT on the main thread *before* spawning the
        // handler thread. Spawned threads inherit the signal mask, so these
        // signals will be blocked process-wide and can only be consumed by
        // sigwait() in the handler thread. Without this, SIGTERM hits the
        // main thread's default handler, killing the process before PID-file
        // cleanup runs.
        let mut sigset = SigSet::empty();
        sigset.add(Signal::SIGTERM);
        sigset.add(Signal::SIGINT);
        let _ = sigset.thread_block();

        let shutdown_clone = shutdown.clone();
        std::thread::spawn(move || match sigset.wait() {
            Ok(sig) => {
                tracing::info!("Received signal: {sig}");
                shutdown_clone.store(true, std::sync::atomic::Ordering::Release);
            }
            Err(e) => {
                tracing::error!("Signal wait error: {e}");
            }
        });
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
