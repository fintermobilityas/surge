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
        kill(Pid::from_raw(self.child.id() as i32), Signal::SIGTERM)
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
