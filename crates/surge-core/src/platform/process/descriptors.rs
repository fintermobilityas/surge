//! Descriptor hygiene for spawned children: an `unsafe` boundary around the
//! post-fork, pre-exec hook that closes inherited descriptors.
#![allow(unsafe_code)]

use std::process::Command;

#[cfg(unix)]
const FIRST_INHERITABLE_DESCRIPTOR: libc::c_int = 3;

#[cfg(unix)]
const MAX_SCANNED_DESCRIPTOR: libc::c_int = 65_536;

/// Install a pre-exec hook that closes every descriptor above stderr in the child.
pub(super) fn close_inherited_before_exec(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        // SAFETY: the closure runs in the forked child between `fork` and `exec` and
        // only calls async-signal-safe functions (`close_range`, `sysconf`, `close`).
        // It does not allocate, take locks, or touch state shared with the parent.
        unsafe {
            command.pre_exec(|| {
                close_descriptors_from(FIRST_INHERITABLE_DESCRIPTOR);
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
fn close_descriptors_from(first: libc::c_int) {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: a plain syscall with integer arguments; `close_range(2)` closes every
        // descriptor in the inclusive range and has no memory-safety preconditions.
        let first_unsigned = libc::c_uint::try_from(first).unwrap_or(0);
        let closed = unsafe { libc::syscall(libc::SYS_close_range, first_unsigned, libc::c_uint::MAX, 0) };
        if closed == 0 {
            return;
        }
    }

    // SAFETY: `sysconf` and `close` are async-signal-safe and take only integer
    // arguments; closing an unused descriptor number merely fails with `EBADF`.
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
            libc::close(descriptor);
        }
    }
}
