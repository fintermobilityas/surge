#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::{Command, ExitCode};

use clap::Parser;

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

fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(
        "Surge supervisor '{}' starting, exe: {}",
        cli.supervisor_id,
        cli.exe_path.display()
    );

    if !cli.exe_path.is_file() {
        return Err(format!("Executable not found: {}", cli.exe_path.display()).into());
    }

    // Write PID file
    let pid_file = cli
        .install_dir
        .join(format!(".surge-supervisor-{}.pid", cli.supervisor_id));
    write_pid_file(&pid_file)?;

    // Install signal handlers
    let shutdown = install_signal_handlers();

    // Main supervision loop
    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            tracing::info!("Shutdown signal received, exiting supervisor loop");
            break;
        }

        tracing::info!("Starting child process: {}", cli.exe_path.display());

        let mut child = Command::new(&cli.exe_path)
            .current_dir(&cli.install_dir)
            .args(&cli.args)
            .spawn()?;

        tracing::info!("Child process started with PID {}", child.id());

        let status = child.wait()?;

        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
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

    tracing::info!("Supervisor '{}' exiting", cli.supervisor_id);
    Ok(())
}

fn write_pid_file(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let pid = std::process::id();
    std::fs::write(path, pid.to_string())?;
    tracing::debug!("Wrote PID file: {} (pid={})", path.display(), pid);
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
        let _ = ctrlc_handler(shutdown_clone);
    }

    shutdown
}

#[cfg(windows)]
fn ctrlc_handler(shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}
