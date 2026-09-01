//! macOS process-generation lookup through the narrow `proc_pidinfo` boundary.
#![allow(unsafe_code)]

use std::io;
use std::mem::{MaybeUninit, size_of};

use super::ProcessIdentity;

pub(super) fn process_identity(pid: u32) -> io::Result<Option<ProcessIdentity>> {
    let raw_pid = i32::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "process id exceeds platform limits"))?;
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let info_size = i32::try_from(size_of::<libc::proc_bsdinfo>())
        .map_err(|_| io::Error::other("proc_bsdinfo size exceeds platform limits"))?;

    // SAFETY: `info` points to writable storage of exactly `info_size` bytes.
    // `proc_pidinfo` initializes that storage when it reports a full-size result.
    let written =
        unsafe { libc::proc_pidinfo(raw_pid, libc::PROC_PIDTBSDINFO, 0, info.as_mut_ptr().cast(), info_size) };
    if written == 0 {
        let error = io::Error::last_os_error();
        return match error.raw_os_error() {
            Some(libc::ESRCH | libc::ENOENT) => Ok(None),
            _ => Err(error),
        };
    }
    if written != info_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("proc_pidinfo returned {written} bytes, expected {info_size}"),
        ));
    }

    // SAFETY: the full structure was initialized by the successful call above.
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != pid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("proc_pidinfo returned PID {} while querying PID {pid}", info.pbi_pid),
        ));
    }
    if info.pbi_status == libc::SZOMB {
        return Ok(None);
    }

    let generation = info
        .pbi_start_tvsec
        .checked_mul(1_000_000)
        .and_then(|seconds| seconds.checked_add(info.pbi_start_tvusec))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "process start time exceeds generation limits",
            )
        })?;
    Ok(Some(ProcessIdentity { pid, generation }))
}
