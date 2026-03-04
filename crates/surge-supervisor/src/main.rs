#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode};

use clap::Parser;
use thiserror::Error;

#[derive(Parser)]
#[command(
    name = "surge-supervisor",
    version,
    about = "Process supervisor for the Surge update framework"
)]
struct Cli {
    /// Unique supervisor identifier
    #[arg(long)]
    supervisor_id: String,

    /// Application install directory
    #[arg(long)]
    install_dir: PathBuf,

    /// Path to the application executable
    #[arg(long)]
    exe_path: PathBuf,

    /// Arguments to pass to the child process
    #[arg(trailing_var_arg = true)]
    args: Vec<String>,

    /// Enable verbose logging
    #[arg(long, short = 'v')]
    verbose: bool,
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
    init_tracing(cli.verbose);

    if let Err(e) = run(&cli) {
        tracing::error!("{e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

fn run(cli: &Cli) -> Result<(), SupervisorError> {
    tracing::info!(
        "Surge supervisor '{}' starting, exe: {}",
        cli.supervisor_id,
        cli.exe_path.display()
    );

    if !cli.exe_path.is_file() {
        return Err(SupervisorError::ExecutableNotFound(cli.exe_path.display().to_string()));
    }

    // Write PID file
    let pid_file = cli
        .install_dir
        .join(format!(".surge-supervisor-{}.pid", cli.supervisor_id));
    let stop_file = cli
        .install_dir
        .join(format!(".surge-supervisor-{}.stop", cli.supervisor_id));

    if stop_file.exists() {
        let _ = std::fs::remove_file(&stop_file);
    }
    write_pid_file(&pid_file)?;

    // Install signal handlers
    let shutdown = install_signal_handlers();

    // Separate one-shot lifecycle args (--surge-*) from regular args.
    // On the first child start, all args are passed. After that, lifecycle
    // args are drained so crash-restarts don't re-fire lifecycle callbacks.
    let mut lifecycle_args: Vec<String> = cli.args.iter().filter(|a| a.starts_with("--surge-")).cloned().collect();
    let regular_args: Vec<String> = cli
        .args
        .iter()
        .filter(|a| !a.starts_with("--surge-"))
        .cloned()
        .collect();

    // Main supervision loop
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) || stop_file.exists() {
            tracing::info!("Shutdown signal received, exiting supervisor loop");
            break;
        }

        let mut child_args = regular_args.clone();
        child_args.append(&mut lifecycle_args);

        tracing::info!("Starting child process: {}", cli.exe_path.display());

        let mut child = Command::new(&cli.exe_path)
            .current_dir(&cli.install_dir)
            .args(&child_args)
            .spawn()?;

        tracing::info!("Child process started with PID {}", child.id());

        let status = match wait_for_child_or_stop(&mut child, &shutdown, &stop_file)? {
            Some(status) => status,
            None => {
                tracing::info!("Stop requested, child terminated and supervisor loop is exiting");
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

    // Clean up PID file
    if pid_file.exists() {
        let _ = std::fs::remove_file(&pid_file);
    }
    if stop_file.exists() {
        let _ = std::fs::remove_file(&stop_file);
    }

    tracing::info!("Supervisor '{}' exiting", cli.supervisor_id);
    Ok(())
}

fn write_pid_file(path: &Path) -> Result<(), SupervisorError> {
    let pid = std::process::id();
    std::fs::write(path, pid.to_string())?;
    tracing::debug!("Wrote PID file: {} (pid={})", path.display(), pid);
    Ok(())
}

fn wait_for_child_or_stop(
    child: &mut Child,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop_file: &Path,
) -> Result<Option<std::process::ExitStatus>, SupervisorError> {
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) || stop_file.exists() {
            terminate_child_process(child)?;
            return Ok(None);
        }

        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
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

/// Install signal handlers and return a shared shutdown flag.
fn install_signal_handlers() -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    #[cfg(unix)]
    {
        let shutdown_clone = shutdown.clone();
        std::thread::spawn(move || {
            use nix::sys::signal::{SigSet, Signal};
            let mut sigset = SigSet::empty();
            sigset.add(Signal::SIGTERM);
            sigset.add(Signal::SIGINT);
            // Block these signals in this thread so we can wait for them
            let _ = sigset.thread_block();
            match sigset.wait() {
                Ok(sig) => {
                    tracing::info!("Received signal: {sig}");
                    shutdown_clone.store(true, std::sync::atomic::Ordering::Release);
                }
                Err(e) => {
                    tracing::error!("Signal wait error: {e}");
                }
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
    // On Windows, use a simple thread-based approach for Ctrl-C
    std::thread::spawn(move || {
        // This is a simplified handler; production code would use SetConsoleCtrlHandler
        loop {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if shutdown.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
        }
    });
}
