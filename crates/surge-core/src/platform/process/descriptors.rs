//! Native process boundary for descriptor hygiene and creation identity.
#![allow(unsafe_code)]

use std::process::Command;

#[cfg(unix)]
const FIRST_INHERITABLE_DESCRIPTOR: libc::c_int = 3;

#[cfg(unix)]
const MAX_SCANNED_DESCRIPTOR: libc::c_int = 65_536;

/// `close_range(2)` flag: set `FD_CLOEXEC` on the range instead of closing it.
#[cfg(target_os = "linux")]
const CLOSE_RANGE_CLOEXEC: libc::c_int = 1 << 2;

/// Install a pre-exec hook that marks every descriptor above stderr close-on-exec.
///
/// The descriptors are marked rather than closed: the standard library keeps a
/// private close-on-exec pipe open in the child to report `chdir`, `pre_exec` and
/// `execve` failures back to `spawn()`, and closing it would turn a missing
/// executable into a successful spawn. Marked descriptors vanish at `exec`, while
/// the error channel keeps working until then.
pub(super) fn close_inherited_before_exec(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        // SAFETY: the closure runs in the forked child between `fork` and `exec` and
        // only calls async-signal-safe functions (`close_range`, `sysconf`, `fcntl`).
        // It does not allocate, take locks, or touch state shared with the parent.
        unsafe {
            command.pre_exec(|| {
                mark_close_on_exec_from(FIRST_INHERITABLE_DESCRIPTOR);
                Ok(())
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = command;
    }
}

#[cfg(unix)]
fn mark_close_on_exec_from(first: libc::c_int) {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a plain syscall with integer arguments; with `CLOSE_RANGE_CLOEXEC`
        // it only flips the close-on-exec flag on the descriptors in the range.
        let first_unsigned = libc::c_uint::try_from(first).unwrap_or(0);
        let marked = unsafe {
            libc::syscall(
                libc::SYS_close_range,
                first_unsigned,
                libc::c_uint::MAX,
                CLOSE_RANGE_CLOEXEC,
            )
        };
        if marked == 0 {
            return;
        }
    }

    // SAFETY: `sysconf` and `fcntl` are async-signal-safe and take only integer
    // arguments; an unused descriptor number merely fails with `EBADF` and is skipped.
    unsafe {
        let limit = libc::sysconf(libc::_SC_OPEN_MAX);
        let last = libc::c_int::try_from(limit).map_or(MAX_SCANNED_DESCRIPTOR, |open_max| {
            if open_max > 0 {
                open_max.min(MAX_SCANNED_DESCRIPTOR)
            } else {
                MAX_SCANNED_DESCRIPTOR
            }
        });
        for descriptor in first..last {
            let flags = libc::fcntl(descriptor, libc::F_GETFD);
            if flags >= 0 && flags & libc::FD_CLOEXEC == 0 {
                libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub(super) fn process_start_time(pid: u32) -> Option<u64> {
    if pid == 0 {
        return None;
    }

    let stat = std::fs::read(format!("/proc/{pid}/stat")).ok()?;
    parse_linux_process_start_time(&stat)
}

#[cfg(target_os = "linux")]
fn parse_linux_process_start_time(stat: &[u8]) -> Option<u64> {
    let command_end = stat.iter().rposition(|byte| *byte == b')')?;
    let fields_after_command = std::str::from_utf8(stat.get(command_end + 1..)?).ok()?;
    fields_after_command.split_ascii_whitespace().nth(19)?.parse().ok()
}

#[cfg(target_os = "macos")]
pub(super) fn process_start_time(pid: u32) -> Option<u64> {
    let pid = libc::pid_t::try_from(pid).ok()?;
    if pid <= 0 {
        return None;
    }

    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).ok()?;
    // SAFETY: `info` points to writable storage of exactly the size passed to
    // proc_pidinfo. A full-size return is required before the value is read.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast::<libc::c_void>(),
            size,
        )
    };
    if written != size {
        return None;
    }

    // SAFETY: proc_pidinfo reported that it initialized the complete structure.
    let info = unsafe { info.assume_init() };
    info.pbi_start_tvsec
        .checked_mul(1_000_000)?
        .checked_add(info.pbi_start_tvusec)
}

#[cfg(windows)]
pub(super) fn process_start_time(pid: u32) -> Option<u64> {
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
    };

    if pid == 0 {
        return None;
    }

    // SAFETY: OpenProcess is called with a non-inheritable handle and a numeric
    // PID. The returned handle is checked and closed on every path below.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        return None;
    }

    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: all pointers reference initialized writable FILETIME values and
    // `handle` remains valid until CloseHandle immediately after the call.
    let succeeded = unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
    // SAFETY: a process handle with synchronize access is waitable. A live process
    // must remain unsignaled during this zero-timeout probe.
    let wait_status = unsafe { WaitForSingleObject(handle, 0) };
    // SAFETY: `handle` is the live owned handle returned by OpenProcess above.
    let _ = unsafe { CloseHandle(handle) };
    if succeeded == 0 || wait_status != WAIT_TIMEOUT {
        return None;
    }

    Some((u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(super) fn process_start_time(pid: u32) -> Option<u64> {
    let _ = pid;
    None
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn linux_process_start_time_reads_proc_stat_field() {
        let start_time = process_start_time(std::process::id()).expect("current process start time");
        assert!(start_time > 0);
    }

    #[test]
    fn linux_process_start_time_accepts_non_utf8_command_name() {
        let stat = b"123 (non-utf8-\xff) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 4242 20";
        assert_eq!(parse_linux_process_start_time(stat), Some(4242));
    }
}
