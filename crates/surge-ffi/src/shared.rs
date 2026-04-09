use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::{Arc, Mutex};

use surge_core::context::Context;
use surge_core::error::SurgeError;
use surge_core::update::manager::ProgressInfo;

use crate::SurgeProgressFfi;
use crate::handles::{SurgeContextHandle, SurgeErrorOwned};
use crate::utils::lock_recover;

pub(crate) const SURGE_OK: i32 = 0;
pub(crate) const SURGE_ERROR: i32 = -1;
pub(crate) const SURGE_CANCELLED: i32 = -2;
pub(crate) const SURGE_NOT_FOUND: i32 = -3;

pub(crate) const SURGE_PHASE_CHECK: i32 = 0;
pub(crate) const SURGE_PHASE_DOWNLOAD: i32 = 1;
#[allow(dead_code)]
pub(crate) const SURGE_PHASE_VERIFY: i32 = 2;
#[allow(dead_code)]
pub(crate) const SURGE_PHASE_EXTRACT: i32 = 3;
#[allow(dead_code)]
pub(crate) const SURGE_PHASE_APPLY_DELTA: i32 = 4;
#[allow(dead_code)]
pub(crate) const SURGE_PHASE_FINALIZE: i32 = 5;

pub(crate) type SurgeProgressCallback = Option<extern "C" fn(*const SurgeProgressFfi, *mut c_void)>;
pub(crate) type SurgeEventCallback = Option<extern "C" fn(*const c_char, *mut c_void)>;

/// # Safety
///
/// Only safe when the pointer is valid for the duration of the async call
/// and only accessed from the calling thread (via `Runtime::block_on`).
pub(crate) struct ProgressBridge {
    pub(crate) cb: extern "C" fn(*const SurgeProgressFfi, *mut c_void),
    pub(crate) user_data: usize,
}

impl ProgressBridge {
    /// Core phases are 1-indexed; FFI phases are 0-indexed.
    pub(crate) fn invoke(&self, pi: &ProgressInfo) {
        let ffi = SurgeProgressFfi {
            phase: pi.phase.saturating_sub(1),
            phase_percent: pi.phase_percent,
            total_percent: pi.total_percent,
            bytes_done: pi.bytes_done,
            bytes_total: pi.bytes_total,
            items_done: pi.items_done,
            items_total: pi.items_total,
            speed_bytes_per_sec: pi.speed_bytes_per_sec,
        };
        (self.cb)(&ffi, self.user_data as *mut c_void);
    }
}

pub(crate) fn make_pack_progress(phase: i32, items_done: i32, items_total: i32) -> SurgeProgressFfi {
    let pct = if items_total > 0 {
        items_done * 100 / items_total
    } else {
        0
    };
    SurgeProgressFfi {
        phase,
        phase_percent: pct,
        total_percent: pct,
        bytes_done: 0,
        bytes_total: 0,
        items_done: i64::from(items_done),
        items_total: i64::from(items_total),
        speed_bytes_per_sec: 0.0,
    }
}

/// # Safety
///
/// `p` must be null or point to a valid NUL-terminated C string.
pub(crate) unsafe fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        // SAFETY: Caller guarantees `p` is either null (handled above) or
        // a valid NUL-terminated C string.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

pub(crate) fn catch_ffi<F: FnOnce() -> i32 + std::panic::UnwindSafe>(f: F) -> i32 {
    match std::panic::catch_unwind(f) {
        Ok(code) => code,
        Err(_) => SURGE_ERROR,
    }
}

pub(crate) fn set_ctx_error(handle: &SurgeContextHandle, e: &SurgeError) -> i32 {
    let code = e.error_code() as i32;
    handle.set_last_error(code, &e.to_string());
    handle.ctx.set_error(e);
    code
}

pub(crate) fn set_shared_error(
    ctx: &Arc<Context>,
    last_error: &Arc<Mutex<Option<SurgeErrorOwned>>>,
    e: &SurgeError,
) -> i32 {
    let code = e.error_code() as i32;
    let mut slot = lock_recover(last_error.as_ref());
    *slot = Some(SurgeErrorOwned::new(code, &e.to_string()));
    ctx.set_error(e);
    code
}

pub(crate) fn clear_shared_error(ctx: &Arc<Context>, last_error: &Arc<Mutex<Option<SurgeErrorOwned>>>) {
    let mut slot = lock_recover(last_error.as_ref());
    *slot = None;
    ctx.clear_error();
}

pub(crate) fn try_len(size: i64) -> Option<usize> {
    usize::try_from(size).ok().filter(|len| *len > 0)
}

pub(crate) fn try_len_allow_zero(size: i64) -> Option<usize> {
    usize::try_from(size).ok()
}

pub(crate) fn try_index(index: i32, len: usize) -> Option<usize> {
    let idx = usize::try_from(index).ok()?;
    if idx < len { Some(idx) } else { None }
}

/// # Safety
///
/// `argv` must point to at least `argc` elements when `argc > 0`, and each
/// non-null element must be a valid NUL-terminated C string.
pub(crate) unsafe fn collect_argv(argc: c_int, argv: *const *const c_char) -> Vec<String> {
    let Ok(count) = usize::try_from(argc) else {
        return Vec::new();
    };
    if count == 0 || argv.is_null() {
        return Vec::new();
    }

    // SAFETY: Caller guarantees `argv` points to at least `count` entries.
    let argv_slice = unsafe { std::slice::from_raw_parts(argv, count) };

    let mut args = Vec::with_capacity(count);
    for &arg_ptr in argv_slice {
        if arg_ptr.is_null() {
            continue;
        }
        // SAFETY: Caller guarantees each non-null argv element points to
        // a valid NUL-terminated C string.
        args.push(unsafe { cstr_to_string(arg_ptr) });
    }
    args
}

/// Thin wrapper around platform `malloc` for allocating buffers that C callers
/// will free with `free()`.
///
/// Returns a pointer to `size` bytes of uninitialized memory, or null on failure.
pub(crate) fn libc_malloc(size: usize) -> *mut u8 {
    unsafe extern "C" {
        fn malloc(size: usize) -> *mut c_void;
    }
    // SAFETY: Calling C `malloc` with any `usize` is valid; failure is
    // reported with a null pointer and handled by callers.
    unsafe { malloc(size).cast::<u8>() }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use super::{collect_argv, try_index, try_len};
    use std::ffi::c_int;

    #[test]
    fn try_len_rejects_invalid_values() {
        assert_eq!(try_len(0), None);
        assert_eq!(try_len(-1), None);
    }

    #[test]
    fn try_len_accepts_positive_values() {
        assert_eq!(try_len(1), Some(1));
        assert_eq!(try_len(42), Some(42));
    }

    #[test]
    fn try_index_bounds_checking() {
        assert_eq!(try_index(-1, 3), None);
        assert_eq!(try_index(0, 3), Some(0));
        assert_eq!(try_index(2, 3), Some(2));
        assert_eq!(try_index(3, 3), None);
    }

    #[test]
    fn collect_argv_skips_null_entries() {
        let arg0 = CString::new("--surge-first-run").unwrap();
        let arg1 = CString::new("--surge-updated=1.2.3").unwrap();
        let argv = [arg0.as_ptr(), ptr::null(), arg1.as_ptr()];

        // SAFETY: `argv` points to valid C strings for the duration of the call.
        let args = unsafe { collect_argv(argv.len() as c_int, argv.as_ptr()) };
        assert_eq!(args, vec!["--surge-first-run", "--surge-updated=1.2.3"]);
    }
}
