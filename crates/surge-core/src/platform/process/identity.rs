use std::io;

use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;

#[cfg(target_os = "macos")]
mod macos;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessIdentity {
    pub pid: u32,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessSignalOutcome {
    Delivered,
    Exited,
}

#[derive(Debug)]
pub struct StableProcessHandle {
    identity: ProcessIdentity,
    #[cfg(target_os = "linux")]
    pidfd: OwnedFd,
}

impl StableProcessHandle {
    pub fn open(expected: ProcessIdentity) -> io::Result<Option<Self>> {
        #[cfg(target_os = "linux")]
        {
            let raw_pid = i32::try_from(expected.pid)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds platform limits"))?;
            let pid = rustix::process::Pid::from_raw(raw_pid)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "process id must be positive"))?;
            let pidfd = match rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty()) {
                Ok(pidfd) => pidfd,
                Err(rustix::io::Errno::SRCH) => return Ok(None),
                Err(error) => return Err(error.into()),
            };

            if process_identity(expected.pid)? != Some(expected) {
                return Ok(None);
            }

            Ok(Some(Self {
                identity: expected,
                pidfd,
            }))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = expected;
            Err(stable_handles_unsupported())
        }
    }

    #[must_use]
    pub fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    pub fn is_running(&self) -> io::Result<bool> {
        process_identity_matches(self.identity)
    }

    pub fn terminate(&self) -> io::Result<ProcessSignalOutcome> {
        self.send_signal(ProcessSignal::Terminate)
    }

    pub fn kill(&self) -> io::Result<ProcessSignalOutcome> {
        self.send_signal(ProcessSignal::Kill)
    }

    #[cfg(target_os = "linux")]
    fn send_signal(&self, signal: ProcessSignal) -> io::Result<ProcessSignalOutcome> {
        let signal = match signal {
            ProcessSignal::Terminate => rustix::process::Signal::TERM,
            ProcessSignal::Kill => rustix::process::Signal::KILL,
        };
        match rustix::process::pidfd_send_signal(&self.pidfd, signal) {
            Ok(()) => Ok(ProcessSignalOutcome::Delivered),
            Err(rustix::io::Errno::SRCH) => Ok(ProcessSignalOutcome::Exited),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn send_signal(&self, _signal: ProcessSignal) -> io::Result<ProcessSignalOutcome> {
        Err(stable_handles_unsupported())
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessSignal {
    Terminate,
    Kill,
}

pub fn process_identity(pid: u32) -> io::Result<Option<ProcessIdentity>> {
    if pid == 0 {
        return Ok(None);
    }
    process_identity_impl(pid)
}

pub fn process_identity_matches(expected: ProcessIdentity) -> io::Result<bool> {
    process_identity(expected.pid).map(|current| current == Some(expected))
}

#[cfg(target_os = "linux")]
fn process_identity_impl(pid: u32) -> io::Result<Option<ProcessIdentity>> {
    let stat = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat,
        Err(error) if process_disappeared(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let stat = parse_linux_process_stat(pid, &stat)?;
    if stat.is_zombie && !linux_thread_group_has_live_member(pid)? {
        return Ok(None);
    }
    Ok(Some(ProcessIdentity {
        pid,
        generation: stat.generation,
    }))
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LinuxProcessStat {
    generation: u64,
    is_zombie: bool,
}

#[cfg(target_os = "linux")]
fn parse_linux_process_stat(pid: u32, stat: &str) -> io::Result<LinuxProcessStat> {
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid} stat has no command boundary"),
        ));
    };
    let mut fields = fields.split_whitespace();
    let state = fields
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("process {pid} stat has no state")))?;
    let generation = fields
        .nth(18)
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process {pid} stat has no valid start time"),
            )
        })?;
    Ok(LinuxProcessStat {
        generation,
        is_zombie: matches!(state.as_bytes(), [b'Z' | b'X']),
    })
}

#[cfg(target_os = "linux")]
fn linux_thread_group_has_live_member(pid: u32) -> io::Result<bool> {
    let tasks = match std::fs::read_dir(format!("/proc/{pid}/task")) {
        Ok(tasks) => tasks,
        Err(error) if process_disappeared(&error) => return Ok(false),
        Err(error) => return Err(error),
    };
    for task in tasks {
        let task = match task {
            Ok(task) => task,
            Err(error) if process_disappeared(&error) => continue,
            Err(error) => return Err(error),
        };
        let Some(tid) = task.file_name().to_string_lossy().parse::<u32>().ok() else {
            continue;
        };
        let stat = match std::fs::read_to_string(format!("/proc/{pid}/task/{tid}/stat")) {
            Ok(stat) => stat,
            Err(error) if process_disappeared(&error) => continue,
            Err(error) => return Err(error),
        };
        if !linux_process_stat_is_zombie(tid, &stat)? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn linux_process_stat_is_zombie(pid: u32, stat: &str) -> io::Result<bool> {
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("process {pid} stat has no command boundary"),
        ));
    };
    let state = fields
        .split_whitespace()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("process {pid} stat has no state")))?;
    Ok(matches!(state.as_bytes(), [b'Z' | b'X']))
}

#[cfg(target_os = "linux")]
fn process_disappeared(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound || matches!(error.raw_os_error(), Some(libc::ESRCH))
}

#[cfg(target_os = "macos")]
fn process_identity_impl(pid: u32) -> io::Result<Option<ProcessIdentity>> {
    macos::process_identity(pid)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn process_identity_impl(pid: u32) -> io::Result<Option<ProcessIdentity>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("process generation lookup is unsupported for PID {pid}"),
    ))
}

#[cfg(not(target_os = "linux"))]
fn stable_handles_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "stable process handles are unavailable on this platform",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_stat_parser_uses_start_time_after_complex_command_name() {
        let stat = "42 (worker ) name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 123456 20";
        assert_eq!(
            parse_linux_process_stat(42, stat).unwrap(),
            LinuxProcessStat {
                generation: 123_456,
                is_zombie: false,
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_stat_parser_retains_zombie_generation_for_thread_group_check() {
        let stat = "42 (worker) Z 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 123456 20";
        assert_eq!(
            parse_linux_process_stat(42, stat).unwrap(),
            LinuxProcessStat {
                generation: 123_456,
                is_zombie: true,
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_thread_state_distinguishes_live_workers_from_zombies() {
        assert!(!linux_process_stat_is_zombie(43, "43 (worker) S 1").unwrap());
        assert!(linux_process_stat_is_zombie(43, "43 (worker) Z 1").unwrap());
        assert!(linux_process_stat_is_zombie(43, "43 (worker) X 1").unwrap());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stable_handle_rejects_a_different_generation() {
        let identity = process_identity(std::process::id()).unwrap().unwrap();
        let mismatched = ProcessIdentity {
            generation: identity.generation.wrapping_add(1),
            ..identity
        };
        assert!(StableProcessHandle::open(mismatched).unwrap().is_none());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stable_handle_signals_the_verified_process() {
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        let identity = process_identity(child.id()).unwrap().unwrap();
        let handle = StableProcessHandle::open(identity).unwrap().unwrap();

        assert_eq!(handle.identity(), identity);
        assert_eq!(handle.terminate().unwrap(), ProcessSignalOutcome::Delivered);
        assert!(!child.wait().unwrap().success());
        assert!(!handle.is_running().unwrap());
    }
}
