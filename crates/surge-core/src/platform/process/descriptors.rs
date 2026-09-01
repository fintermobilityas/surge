//! Descriptor hygiene for spawned children: an `unsafe` boundary around the
//! post-fork, pre-exec hook that marks inherited descriptors close-on-exec.
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
