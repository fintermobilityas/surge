//! Process supervisor: manages a child process lifecycle.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

use std::collections::BTreeMap;

use crate::error::{Result, SurgeError};
use crate::platform::process::{ProcessHandle, spawn_process};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    Stopped,
    Starting,
    Running,
    Stopping,
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

#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub state: ProcessState,
    pub pid: u32,
    pub exit_code: i32,
    pub version: String,
    pub exe_path: PathBuf,
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

#[allow(dead_code)]
pub struct Supervisor {
    id: String,
    install_dir: PathBuf,
    info: ProcessInfo,
    handle: Option<ProcessHandle>,
}

impl Supervisor {
    pub fn new(supervisor_id: &str, install_dir: &str) -> Self {
        Self {
            id: supervisor_id.to_string(),
            install_dir: PathBuf::from(install_dir),
            info: ProcessInfo::default(),
            handle: None,
        }
    }

    pub fn start(&mut self, exe_path: &str, working_dir: &str, args: &[&str]) -> Result<()> {
        if self.refresh_running_state() {
            return Err(SurgeError::Supervisor("Process is already running".to_string()));
        }

        self.info.state = ProcessState::Starting;
        self.info.exe_path = PathBuf::from(exe_path);
        self.info.working_dir = PathBuf::from(working_dir);

        info!(
            supervisor_id = %self.id,
            exe = %exe_path,
            "Starting supervised process"
        );

        let exe = Path::new(exe_path);
        let wd = Path::new(working_dir);

        let handle = spawn_process(exe, args, Some(wd), &BTreeMap::new())?;
        self.info.pid = handle.pid();
        self.info.state = ProcessState::Running;
        self.handle = Some(handle);

        info!(pid = self.info.pid, "Process started");

        Ok(())
    }

    /// Sends SIGTERM and waits up to `timeout_ms` before force-killing.
    pub fn stop(&mut self, timeout_ms: u64) -> Result<()> {
        let Some(handle) = self.handle.as_mut() else {
            self.info.state = ProcessState::Stopped;
            return Ok(());
        };

        if !handle.poll_running() {
            self.info.state = ProcessState::Stopped;
            self.handle = None;
            return Ok(());
        }

        self.info.state = ProcessState::Stopping;

        info!(
            supervisor_id = %self.id,
            pid = self.info.pid,
            "Stopping supervised process"
        );

        if let Err(e) = handle.terminate() {
            warn!("Graceful terminate failed: {e}");
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if !handle.poll_running() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                warn!("Process did not stop in time, force killing");
                handle.kill()?;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let result = handle.wait()?;
        self.info.exit_code = result.exit_code;
        self.info.state = ProcessState::Stopped;
        self.info.pid = 0;
        self.handle = None;

        info!(exit_code = self.info.exit_code, "Process stopped");

        Ok(())
    }

    pub fn restart(&mut self, new_exe_path: Option<&str>, new_args: Option<&[&str]>) -> Result<()> {
        let exe_path = new_exe_path.map_or_else(|| self.info.exe_path.to_string_lossy().into_owned(), String::from);
        let working_dir = self.info.working_dir.to_string_lossy().into_owned();
        let default_args: Vec<&str> = Vec::new();
        let args = new_args.unwrap_or(&default_args);

        info!(
            supervisor_id = %self.id,
            exe = %exe_path,
            "Restarting supervised process"
        );

        if self.refresh_running_state() {
            self.stop(5000)?;
        }

        self.start(&exe_path, &working_dir, args)
    }

    /// Also transitions state to `Crashed` if the process exited unexpectedly.
    pub fn refresh_running_state(&mut self) -> bool {
        if let Some(handle) = self.handle.as_mut() {
            if handle.poll_running() {
                return true;
            }
            if self.info.state == ProcessState::Running {
                self.info.state = ProcessState::Crashed;
                warn!(
                    supervisor_id = %self.id,
                    pid = self.info.pid,
                    "Supervised process crashed"
                );
            }
            false
        } else {
            false
        }
    }

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
        assert!(!sv.refresh_running_state());
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
