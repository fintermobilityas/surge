use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use crate::error::{Result, SurgeError};

mod descriptors;
mod identity;

pub use identity::{
    ProcessIdentity, ProcessSignalOutcome, StableProcessHandle, process_identity, process_identity_matches,
};

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
    close_inherited_descriptors(&mut cmd);

    let child = cmd
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to spawn {}: {e}", exe.display())))?;

    Ok(ProcessHandle { child })
}

/// Configure `command` so the child starts with only stdin, stdout and stderr open.
///
/// A spawned child otherwise inherits every descriptor of the spawning process that
/// is not marked close-on-exec. For Surge that spawning process is often the
/// supervised application itself (starting its supervisor, or handing off an
/// update), and the leaked descriptors keep kernel state alive that belongs to the
/// application: an exclusive evdev grab, a listening socket, a lock file. The
/// replacement application then finds its own resources "busy" until every
/// process in the chain has exited. Marking the inherited descriptors close-on-exec
/// in the child before `exec` removes that coupling without disturbing the standard
/// library's spawn-error reporting. Unix only; a no-op elsewhere.
pub fn close_inherited_descriptors(command: &mut Command) {
    descriptors::close_inherited_before_exec(command);
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

/// Liveness of the process identified by `pid`.
///
/// `Unknown` means the probe itself failed (the probe utility could not be
/// spawned, or it exited abnormally); callers must not treat `Unknown` as
/// proof that the process is dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PidLiveness {
    Alive,
    Dead,
    Unknown,
}

/// Probe the liveness of `pid` without delivering any signal.
#[must_use]
pub fn probe_pid_liveness(pid: u32) -> PidLiveness {
    #[cfg(unix)]
    {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        let Ok(raw) = i32::try_from(pid) else {
            return PidLiveness::Unknown;
        };
        if raw <= 0 {
            // Invalid pid: no such process can exist.
            return PidLiveness::Dead;
        }
        // Signal 0 probes existence without delivering a signal. The
        // kernel answer is conclusive (ESRCH = no such process).
        match kill(Pid::from_raw(raw), None) {
            // EPERM: the process exists but belongs to another user.
            // EINTR: the probe was interrupted before the lookup
            // finished, which only happens while the process exists.
            Ok(()) | Err(Errno::EPERM | Errno::EINTR) => PidLiveness::Alive,
            Err(_) => PidLiveness::Dead,
        }
    }
    #[cfg(windows)]
    {
        // Probe with `tasklist` (safe std::process only; raw OpenProcess
        // FFI is outside surge-core's allowed FFI boundary). The check
        // runs once per update-attempt start, so the process spawn cost
        // is acceptable.
        //
        // PID 0 is not a valid Windows process id; reject it up front so
        // tasklist filter edge cases can never report it alive.
        if pid == 0 {
            return PidLiveness::Dead;
        }
        let Ok(output) = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
        else {
            // The probe could not run; absence of output is not proof
            // of absence.
            return PidLiveness::Unknown;
        };
        if !output.status.success() {
            // tasklist exits 0 even when nothing matches, so a failure
            // status is a probe failure, not a no-match.
            return PidLiveness::Unknown;
        }
        // `/NH` omits the header; the PID is the second whitespace-
        // separated column (image name, PID, session, ...). Matching the
        // PID column instead of any field avoids image-name collisions.
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.split_whitespace().nth(1))
            .any(|field| field == pid.to_string())
            .then_some(PidLiveness::Alive)
            .unwrap_or(PidLiveness::Dead)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        PidLiveness::Unknown
    }
}

/// Returns `true` only when the liveness probe positively identifies a live
/// process with the given pid (see [`probe_pid_liveness`]).
///
/// Used to classify persisted update-worker markers: a dead worker pid must
/// abandon its attempt immediately instead of waiting out the progress
/// staleness window, while a live pid (this process or a concurrent updater)
/// must never be reclassified as abandoned.
#[must_use]
pub fn is_pid_alive(pid: u32) -> bool {
    matches!(probe_pid_liveness(pid), PidLiveness::Alive)
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
    #[cfg(target_os = "linux")]
    #[test]
    fn spawned_child_does_not_inherit_open_descriptors() {
        use super::spawn_process;
        use std::collections::BTreeMap;
        use std::os::fd::AsRawFd;

        use nix::fcntl::{FcntlArg, FdFlag, fcntl};

        // Rust opens files close-on-exec by default; clear the flag so the descriptor
        // would be inherited by a child spawned without descriptor hygiene.
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        fcntl(tmp.as_file(), FcntlArg::F_SETFD(FdFlag::empty())).expect("clear close-on-exec");
        let inherited = tmp.as_file().as_raw_fd();
        let probe = format!("test ! -e /proc/self/fd/{inherited}");

        let mut handle = spawn_process(std::path::Path::new("/bin/sh"), &["-c", &probe], None, &BTreeMap::new())
            .expect("spawn probe shell");
        let result = handle.wait().expect("wait probe shell");

        assert_eq!(result.exit_code, 0, "descriptor {inherited} leaked into the child");
    }

    #[cfg(unix)]
    #[test]
    fn spawning_a_missing_executable_still_reports_the_error() {
        use super::spawn_process;
        use std::collections::BTreeMap;

        let result = spawn_process(
            std::path::Path::new("/nonexistent/surge-missing-executable"),
            &[],
            None,
            &BTreeMap::new(),
        );

        assert!(
            matches!(result, Err(crate::error::SurgeError::Platform(_))),
            "exec failure must surface through spawn(), not vanish with the error pipe"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn spawned_child_keeps_standard_streams() {
        use super::spawn_process;
        use std::collections::BTreeMap;

        let mut handle = spawn_process(
            std::path::Path::new("/bin/sh"),
            &[
                "-c",
                "test -e /proc/self/fd/0 && test -e /proc/self/fd/1 && test -e /proc/self/fd/2",
            ],
            None,
            &BTreeMap::new(),
        )
        .expect("spawn probe shell");
        let result = handle.wait().expect("wait probe shell");

        assert_eq!(result.exit_code, 0);
    }

    use super::{PidLiveness, is_pid_alive, probe_pid_liveness};
    use std::time::{Duration, Instant};

    #[test]
    fn current_process_is_reported_alive() {
        assert!(is_pid_alive(std::process::id()));
    }

    #[test]
    fn probe_pid_liveness_distinguishes_alive_dead_and_invalid() {
        assert_eq!(probe_pid_liveness(std::process::id()), PidLiveness::Alive);
        // PID 0 is not a valid process id on any supported platform.
        assert_eq!(probe_pid_liveness(0), PidLiveness::Dead);
    }

    #[test]
    fn is_pid_alive_reflects_a_positive_probe() {
        assert!(is_pid_alive(std::process::id()));
        assert!(!is_pid_alive(0));
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
        assert_eq!(probe_pid_liveness(pid), PidLiveness::Dead);
    }
}
