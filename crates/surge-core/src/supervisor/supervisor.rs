//! Process supervisor: manages a child process lifecycle.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use crate::error::{Result, SurgeError};
use crate::platform::process::{ProcessHandle, spawn_process};

/// The current state of a supervised process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// Process is not running.
    Stopped,
    /// Process is starting up.
    Starting,
    /// Process is running normally.
    Running,
    /// Process is shutting down.
    Stopping,
    /// Process has terminated unexpectedly.
    Crashed,
}

impl std::fmt::Display for ProcessState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => write!(f, "stopped"),
            Self::Starting => write!(f, "starting"),
            Self::Running => write!(f, "running"),
            Self::Stopping => write!(f, "stopping"),
            Self::Crashed => write!(f, "crashed"),
        }
    }
}

/// Information about a supervised process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    /// Current state of the process.
    pub state: ProcessState,
    /// Process ID (0 if not running).
    pub pid: u32,
    /// Last exit code (0 if never exited).
    pub exit_code: i32,
    /// Version of the application being supervised.
    pub version: String,
    /// Path to the executable.
    pub exe_path: PathBuf,
    /// Working directory.
    pub working_dir: PathBuf,
}

impl Default for ProcessInfo {
    fn default() -> Self {
        Self {
            state: ProcessState::Stopped,
            pid: 0,
            exit_code: 0,
            version: String::new(),
            exe_path: PathBuf::new(),
            working_dir: PathBuf::new(),
        }
    }
}

/// Manages the lifecycle of a supervised child process.
#[allow(dead_code)]
pub struct Supervisor {
    supervisor_id: String,
    install_dir: PathBuf,
    info: ProcessInfo,
    handle: Option<ProcessHandle>,
}

impl Supervisor {
    /// Create a new supervisor.
    pub fn new(supervisor_id: &str, install_dir: &str) -> Self {
        Self {
            supervisor_id: supervisor_id.to_string(),
            install_dir: PathBuf::from(install_dir),
            info: ProcessInfo::default(),
            handle: None,
        }
    }

    /// Start the supervised process.
    ///
    /// # Arguments
    ///
    /// * `exe_path` - Path to the executable to run
    /// * `working_dir` - Working directory for the process
    /// * `args` - Command-line arguments
    pub fn start(&mut self, exe_path: &str, working_dir: &str, args: &[&str]) -> Result<()> {
        if self.is_running() {
            return Err(SurgeError::Supervisor("Process is already running".to_string()));
        }

        self.info.state = ProcessState::Starting;
        self.info.exe_path = PathBuf::from(exe_path);
        self.info.working_dir = PathBuf::from(working_dir);

        info!(
            supervisor_id = %self.supervisor_id,
            exe = %exe_path,
            "Starting supervised process"
        );

        let exe = Path::new(exe_path);
        let wd = Path::new(working_dir);

        let handle = spawn_process(exe, args, Some(wd))?;
        self.info.pid = handle.pid();
        self.info.state = ProcessState::Running;
        self.handle = Some(handle);

        info!(pid = self.info.pid, "Process started");

        Ok(())
    }

    /// Stop the supervised process.
    ///
    /// Sends a termination signal and waits up to `timeout_ms` milliseconds
    /// for the process to exit. If the process does not exit in time, it is
    /// force-killed.
    pub fn stop(&mut self, timeout_ms: u64) -> Result<()> {
        let handle = if let Some(h) = self.handle.as_mut() {
            h
        } else {
            self.info.state = ProcessState::Stopped;
            return Ok(());
        };

        if !handle.is_running() {
            self.info.state = ProcessState::Stopped;
            self.handle = None;
            return Ok(());
        }

        self.info.state = ProcessState::Stopping;

        info!(
            supervisor_id = %self.supervisor_id,
            pid = self.info.pid,
            "Stopping supervised process"
        );

        // Try graceful termination first
        if let Err(e) = handle.terminate() {
            warn!("Graceful terminate failed: {e}");
        }

        // Wait for a bit
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if !handle.is_running() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                warn!("Process did not stop in time, force killing");
                handle.kill()?;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Collect exit status
        let result = handle.wait()?;
        self.info.exit_code = result.exit_code;
        self.info.state = ProcessState::Stopped;
        self.info.pid = 0;
        self.handle = None;

        info!(exit_code = self.info.exit_code, "Process stopped");

        Ok(())
    }

    /// Restart the supervised process, optionally with a new executable or arguments.
    ///
    /// Stops the current process (if running), then starts a new one.
    pub fn restart(&mut self, new_exe_path: Option<&str>, new_args: Option<&[&str]>) -> Result<()> {
        let exe_path = new_exe_path.map_or_else(|| self.info.exe_path.to_string_lossy().into_owned(), String::from);
        let working_dir = self.info.working_dir.to_string_lossy().into_owned();
        let default_args: Vec<&str> = Vec::new();
        let args = new_args.unwrap_or(&default_args);

        info!(
            supervisor_id = %self.supervisor_id,
            exe = %exe_path,
            "Restarting supervised process"
        );

        // Stop if currently running
        if self.is_running() {
            self.stop(5000)?;
        }

        self.start(&exe_path, &working_dir, args)
    }

    /// Check if the supervised process is currently running.
    ///
    /// Also updates the internal state if the process has exited unexpectedly.
    pub fn is_running(&mut self) -> bool {
        if let Some(handle) = self.handle.as_mut() {
            if handle.is_running() {
                return true;
            }
            // Process exited unexpectedly
            if self.info.state == ProcessState::Running {
                self.info.state = ProcessState::Crashed;
                warn!(
                    supervisor_id = %self.supervisor_id,
                    pid = self.info.pid,
                    "Supervised process crashed"
                );
            }
            false
        } else {
            false
        }
    }

    /// Get information about the supervised process.
    pub fn process_info(&self) -> &ProcessInfo {
        &self.info
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_state_display() {
        assert_eq!(ProcessState::Stopped.to_string(), "stopped");
        assert_eq!(ProcessState::Starting.to_string(), "starting");
        assert_eq!(ProcessState::Running.to_string(), "running");
        assert_eq!(ProcessState::Stopping.to_string(), "stopping");
        assert_eq!(ProcessState::Crashed.to_string(), "crashed");
    }

    #[test]
    fn test_supervisor_initial_state() {
        let mut sv = Supervisor::new("test-sv", "/tmp/install");
        assert!(!sv.is_running());
        assert_eq!(sv.process_info().state, ProcessState::Stopped);
        assert_eq!(sv.process_info().pid, 0);
    }

    #[test]
    fn test_process_info_default() {
        let info = ProcessInfo::default();
        assert_eq!(info.state, ProcessState::Stopped);
        assert_eq!(info.pid, 0);
        assert_eq!(info.exit_code, 0);
        assert!(info.version.is_empty());
    }

    #[test]
    fn test_supervisor_stop_when_not_running() {
        let mut sv = Supervisor::new("test-sv", "/tmp/install");
        // Stopping when no process is running should succeed
        let result = sv.stop(1000);
        assert!(result.is_ok());
    }
}
