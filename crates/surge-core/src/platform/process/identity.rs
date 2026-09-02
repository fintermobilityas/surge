//! Native process-creation identity probes used to distinguish PID reuse.
#![allow(unsafe_code)]

#[cfg(target_os = "linux")]
pub(super) fn process_start_time(pid: u32) -> Option<u64> {
    if pid == 0 {
        return None;
    }
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields_after_command = stat.get(stat.rfind(')')? + 1..)?;
    fields_after_command.split_whitespace().nth(19)?.parse().ok()
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
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
    use windows_sys::Win32::System::Threading::{GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    if pid == 0 {
        return None;
    }
    // SAFETY: OpenProcess is called with a non-inheritable handle and a numeric
    // PID. The returned handle is checked and closed on every path below.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
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
    // SAFETY: `handle` is the live owned handle returned by OpenProcess above.
    let _ = unsafe { CloseHandle(handle) };
    if succeeded == 0 {
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
}
