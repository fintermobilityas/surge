use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use crate::error::{Result, SurgeError};

pub struct ProcessHandle {
    child: Child,
}

pub struct ProcessResult {
    pub exit_code: i32,
    pub timed_out: bool,
}

impl ProcessHandle {
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn poll_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn wait(&mut self) -> Result<ProcessResult> {
        let status = self.child.wait()?;
        Ok(ProcessResult {
            exit_code: status.code().unwrap_or(-1),
            timed_out: false,
        })
    }

    #[cfg(unix)]
    pub fn terminate(&self) -> Result<()> {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        let pid = i32::try_from(self.child.id())
            .map_err(|_| SurgeError::Platform("Process id exceeds platform signal limits".to_string()))?;
        kill(Pid::from_raw(pid), Signal::SIGTERM)
            .map_err(|e| SurgeError::Platform(format!("Failed to send SIGTERM: {e}")))?;
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn terminate(&mut self) -> Result<()> {
        self.child
            .kill()
            .map_err(|e| SurgeError::Platform(format!("Failed to terminate process: {e}")))?;
        Ok(())
    }

    pub fn kill(&mut self) -> Result<()> {
        self.child
            .kill()
            .map_err(|e| SurgeError::Platform(format!("Failed to kill process: {e}")))?;
        Ok(())
    }
}

pub fn spawn_process(
    exe: &Path,
    args: &[&str],
    working_dir: Option<&Path>,
    envs: &BTreeMap<String, String>,
) -> Result<ProcessHandle> {
    spawn_impl(exe, args, working_dir, envs, Stdio::inherit(), Stdio::inherit(), false)
}

/// Spawn a process fully detached (stdin/stdout/stderr = null).
pub fn spawn_detached(
    exe: &Path,
    args: &[&str],
    working_dir: Option<&Path>,
    envs: &BTreeMap<String, String>,
) -> Result<ProcessHandle> {
    spawn_impl(exe, args, working_dir, envs, Stdio::null(), Stdio::null(), true)
}

fn spawn_impl(
    exe: &Path,
    args: &[&str],
    working_dir: Option<&Path>,
    envs: &BTreeMap<String, String>,
    stdout: Stdio,
    stderr: Stdio,
    detached: bool,
) -> Result<ProcessHandle> {
    let mut cmd = Command::new(exe);
    cmd.args(args).stdin(Stdio::null()).stdout(stdout).stderr(stderr);

    #[cfg(not(unix))]
    let _ = detached;

    #[cfg(unix)]
    if detached {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    if let Some(wd) = working_dir {
        cmd.current_dir(wd);
    }

    cmd.envs(envs);

    let child = cmd
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to spawn {}: {e}", exe.display())))?;

    Ok(ProcessHandle { child })
}

#[must_use]
pub fn supervisor_binary_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "surge-supervisor.exe"
    } else {
        "surge-supervisor"
    }
}

#[must_use]
pub fn current_pid() -> u32 {
    std::process::id()
}

/// Returns `true` when a process with the given pid currently exists.
///
/// Used to classify persisted update-worker markers: a dead worker pid must
/// abandon its attempt immediately instead of waiting out the progress
/// staleness window, while a live pid (this process or a concurrent updater)
/// must never be reclassified as abandoned.
#[must_use]
pub fn is_pid_alive(pid: u32) -> bool {
    #[cfg(any(unix, windows))]
    {
        #[cfg(unix)]
        {
            use nix::errno::Errno;
            use nix::sys::signal::kill;
            use nix::unistd::Pid;

            let Ok(raw) = i32::try_from(pid) else {
                return false;
            };
            if raw <= 0 {
                return false;
            }
            // Signal 0 probes existence without delivering a signal.
            match kill(Pid::from_raw(raw), None) {
                // EPERM: the process exists but belongs to another user.
                // EINTR: the probe was interrupted before the lookup finished,
                // which only happens while the process still exists.
                Ok(()) | Err(Errno::EPERM | Errno::EINTR) => true,
                Err(_) => false,
            }
        }
        #[cfg(windows)]
        {
            // Probe with `tasklist` (safe std::process only; raw OpenProcess
            // FFI is outside surge-core's allowed FFI boundary). The check
            // runs once per update-attempt start, so the process spawn cost
            // is acceptable.
            let Ok(output) = std::process::Command::new("tasklist")
                .args(["/FI", &format!("PID eq {pid}"), "/NH"])
                .output()
            else {
                return false;
            };
            if !output.status.success() {
                return false;
            }
            // `/NH` omits the header; each line starts with the image name
            // followed by the pid column.
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.split_whitespace().any(|field| field == pid.to_string()))
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// On Unix uses `execv`; on Windows spawns the process and exits with its code.
#[cfg(unix)]
pub fn exec_replace(exe: &Path, args: &[&str]) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let exe_c =
        CString::new(exe.as_os_str().as_bytes()).map_err(|e| SurgeError::Platform(format!("Invalid exe path: {e}")))?;

    let args_c: std::result::Result<Vec<CString>, _> = std::iter::once(Ok(exe_c.clone()))
        .chain(args.iter().map(|a| CString::new(*a)))
        .collect();
    let args_c = args_c.map_err(|e| SurgeError::Platform(format!("Invalid argument: {e}")))?;

    nix::unistd::execv(&exe_c, &args_c).map_err(|e| SurgeError::Platform(format!("execv failed: {e}")))?;

    unreachable!()
}

#[cfg(not(unix))]
pub fn exec_replace(exe: &Path, args: &[&str]) -> Result<()> {
    let mut handle = spawn_process(exe, args, None, &BTreeMap::new())?;
    let result = handle.wait()?;
    std::process::exit(result.exit_code);
}

#[cfg(test)]
mod tests {
    use super::is_pid_alive;
    use std::time::{Duration, Instant};

    #[test]
    fn current_process_is_reported_alive() {
        assert!(is_pid_alive(std::process::id()));
    }

    #[test]
    fn exited_process_is_reported_dead() {
        let mut child = std::process::Command::new(if cfg!(target_os = "windows") { "cmd" } else { "sh" })
            .args(if cfg!(target_os = "windows") {
                ["/c", "exit /b 0"]
            } else {
                ["-c", "exit 0"]
            })
            .spawn()
            .expect("spawn helper");
        let pid = child.id();
        let _ = child.wait();

        let deadline = Instant::now() + Duration::from_secs(5);
        while is_pid_alive(pid) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!is_pid_alive(pid));
        assert!(!is_pid_alive(0));
    }
}
