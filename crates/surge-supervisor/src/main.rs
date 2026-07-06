#![forbid(unsafe_code)]
#![allow(clippy::cast_possible_wrap)]

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use surge_core::supervisor::state::{read_supervisor_exe_path, supervisor_pid_file, supervisor_stop_file};
use thiserror::Error;

mod child;
mod handoff;
mod ownership;

use child::{
    WaitOutcome, spawn_supervised_child, wait_before_restart, wait_for_pid_or_stop, wait_for_supervised_child,
};
#[cfg(all(test, unix))]
use ownership::current_supervisor_owns_pid_file;
use ownership::{remove_owned_supervisor_state, supervisor_was_superseded};

/// Delay before relaunching a supervised child that has exited. Applied to both
/// crash exits and clean (code 0) exits so a child that exits immediately cannot
/// spin the supervisor in a tight relaunch loop.
const CHILD_RESTART_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

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

        /// Path to the application executable. Optional: when omitted it is
        /// resolved from the supervisor exe state file written by the spawner.
        #[arg(long)]
        exe: Option<PathBuf>,

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

        /// Path to the application executable. Optional: when omitted it is
        /// resolved from the supervisor exe state file written by the spawner.
        #[arg(long)]
        exe: Option<PathBuf>,

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

    #[error("No executable path: pass --exe or write the supervisor exe state file for id '{0}' in {1}")]
    MissingExecutablePath(String, String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

fn resolve_exe_path(
    cli_exe: Option<&Path>,
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<PathBuf, SupervisorError> {
    if let Some(exe) = cli_exe
        && !exe.as_os_str().is_empty()
    {
        return Ok(exe.to_path_buf());
    }

    read_supervisor_exe_path(install_dir, supervisor_id).ok_or_else(|| {
        SupervisorError::MissingExecutablePath(supervisor_id.to_string(), install_dir.display().to_string())
    })
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
            let exe_path = match resolve_exe_path(exe.as_deref(), &dir, &id) {
                Ok(exe_path) => exe_path,
                Err(e) => {
                    tracing::error!("{e}");
                    return ExitCode::FAILURE;
                }
            };
            if let Err(e) = run_supervisor(&id, &dir, &exe_path, &args, None, None) {
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
            let exe_path = match resolve_exe_path(exe.as_deref(), &dir, &id) {
                Ok(exe_path) => exe_path,
                Err(e) => {
                    tracing::error!("{e}");
                    return ExitCode::FAILURE;
                }
            };
            if let Err(e) = run_supervisor(&id, &dir, &exe_path, &args, Some(pid), handoff_version.as_deref()) {
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
        let mut child = spawn_supervised_child(exe_path, install_dir, &child_args)?;

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
            tracing::info!(
                "Child exited successfully (code 0), restarting in {} seconds...",
                CHILD_RESTART_BACKOFF.as_secs()
            );
        } else {
            tracing::warn!(
                "Child exited with {status}, restarting in {} seconds...",
                CHILD_RESTART_BACKOFF.as_secs()
            );
        }

        if !wait_before_restart(&shutdown, &stop_file, &pid_file, own_pid, CHILD_RESTART_BACKOFF) {
            break;
        }
    }

    remove_owned_supervisor_state(&pid_file, &stop_file, own_pid);

    tracing::info!("Supervisor '{supervisor_id}' exiting");
    Ok(())
}

fn write_pid_file(path: &Path, own_pid: u32) -> Result<(), SupervisorError> {
    std::fs::write(path, own_pid.to_string())?;
    tracing::debug!("Wrote PID file: {} (pid={})", path.display(), own_pid);
    Ok(())
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
    #[cfg(unix)]
    use std::process::Command;

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
        let first_run_marker = install_dir.join("first-run-done");
        let exe_path = install_dir.join("target-child");
        std::fs::write(
            &exe_path,
            format!(
                "#!/bin/sh\n\
                 if [ ! -f '{marker}' ]; then\n\
                 printf '%s\\n' \"$@\" > '{args}'\n\
                 touch '{marker}'\n\
                 fi\n",
                marker = first_run_marker.display(),
                args = args_path.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&exe_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&exe_path, permissions).unwrap();

        let install_dir_for_thread = install_dir.to_path_buf();
        let exe_path_for_thread = exe_path.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let supervisor = std::thread::spawn(move || {
            let result = run_supervisor(
                "demo-supervisor",
                &install_dir_for_thread,
                &exe_path_for_thread,
                &[
                    "--app-mode".to_string(),
                    "service".to_string(),
                    "--surge-first-run".to_string(),
                    "2.0.0".to_string(),
                ],
                None,
                None,
            );
            tx.send(result).unwrap();
        });

        let stop_file = supervisor_stop_file(install_dir, "demo-supervisor");
        let first_child_ran = wait_until(std::time::Duration::from_secs(5), || first_run_marker.exists());
        let args = std::fs::read_to_string(&args_path);
        std::fs::write(&stop_file, "stop").unwrap();

        let supervisor_result = rx.recv_timeout(std::time::Duration::from_secs(5));
        supervisor.join().unwrap();
        supervisor_result
            .expect("supervisor did not exit after stop file")
            .unwrap();

        assert!(first_child_ran, "first child should have recorded its argv");
        assert_eq!(args.unwrap(), "--app-mode\nservice\n--surge-first-run\n2.0.0\n");
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
                Some("2.0.0"),
            );
            tx.send(result).unwrap();
        });

        let stop_file = supervisor_stop_file(install_dir, "demo-supervisor");
        let converged = wait_until(std::time::Duration::from_secs(15), || {
            surge_core::update::status::read_update_status(install_dir)
                .ok()
                .flatten()
                .is_some_and(|status| status.state == surge_core::update::status::UpdateConvergenceState::Converged)
        });
        std::fs::write(&stop_file, "stop").unwrap();

        let supervisor_result = rx.recv_timeout(std::time::Duration::from_secs(10));
        supervisor.join().unwrap();
        supervisor_result
            .expect("supervisor did not exit after stop file")
            .unwrap();

        assert!(
            converged,
            "restart handoff should converge once the target child survives the stability window"
        );
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

    #[cfg(unix)]
    #[test]
    fn clean_child_exit_triggers_restart_in_steady_state() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TestInstallDir::new("surge-supervisor-clean-exit-restart");
        let install_dir = tmp.path();
        let runs_path = install_dir.join("child-runs.log");
        let exe_path = install_dir.join("target-child");
        std::fs::write(
            &exe_path,
            format!("#!/bin/sh\necho run >> '{}'\nexit 0\n", runs_path.display()),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&exe_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&exe_path, permissions).unwrap();

        let install_dir_for_thread = install_dir.to_path_buf();
        let exe_path_for_thread = exe_path.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let supervisor = std::thread::spawn(move || {
            let result = run_supervisor(
                "demo-supervisor",
                &install_dir_for_thread,
                &exe_path_for_thread,
                &[],
                None,
                None,
            );
            tx.send(result).unwrap();
        });

        let restarted = wait_until(std::time::Duration::from_secs(10), || {
            std::fs::read_to_string(&runs_path).is_ok_and(|contents| contents.lines().count() >= 2)
        });

        let stop_file = supervisor_stop_file(install_dir, "demo-supervisor");
        std::fs::write(&stop_file, "stop").unwrap();
        let supervisor_result = rx.recv_timeout(std::time::Duration::from_secs(10));
        supervisor.join().unwrap();
        supervisor_result
            .expect("supervisor did not exit after stop file")
            .unwrap();

        assert!(
            restarted,
            "supervisor must relaunch a child that exits cleanly (code 0) instead of stopping supervision"
        );
    }

    #[test]
    fn resolve_exe_path_prefers_explicit_flag() {
        let resolved = resolve_exe_path(
            Some(Path::new("/opt/demo/app/demo")),
            Path::new("/opt/demo"),
            "demo-supervisor",
        )
        .unwrap();
        assert_eq!(resolved, PathBuf::from("/opt/demo/app/demo"));
    }

    #[cfg(unix)]
    #[test]
    fn resolve_exe_path_reads_state_file_when_flag_absent() {
        let tmp = TestInstallDir::new("surge-supervisor-exe-state");
        let install_dir = tmp.path();
        let exe = install_dir.join("app").join("demo-app");
        surge_core::supervisor::state::write_supervisor_exe_path(install_dir, "demo-supervisor", &exe).unwrap();

        let resolved = resolve_exe_path(None, install_dir, "demo-supervisor").unwrap();
        assert_eq!(resolved, exe);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_exe_path_errors_when_flag_and_state_file_absent() {
        let tmp = TestInstallDir::new("surge-supervisor-exe-missing");
        let install_dir = tmp.path();

        let err = resolve_exe_path(None, install_dir, "demo-supervisor").unwrap_err();

        assert!(matches!(err, SupervisorError::MissingExecutablePath(..)));
    }
}
