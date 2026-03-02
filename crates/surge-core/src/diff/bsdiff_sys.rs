//! Raw FFI bindings to the vendored bsdiff library.

use std::ffi::c_void;
use std::os::raw::{c_char, c_int};

pub const BSDIFF_SUCCESS: c_int = 0;
pub const BSDIFF_MODE_READ: c_int = 0;
pub const BSDIFF_MODE_WRITE: c_int = 1;

/// Stream interface for bsdiff.
#[repr(C)]
pub struct BsdiffStream {
    pub state: *mut c_void,
    pub close: Option<unsafe extern "C" fn(*mut c_void)>,
    pub get_mode: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub seek: Option<unsafe extern "C" fn(*mut c_void, i64, c_int) -> c_int>,
    pub tell: Option<unsafe extern "C" fn(*mut c_void, *mut i64) -> c_int>,
    pub read: Option<unsafe extern "C" fn(*mut c_void, *mut c_void, usize, *mut usize) -> c_int>,
    pub write: Option<unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> c_int>,
    pub flush: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub get_buffer: Option<unsafe extern "C" fn(*mut c_void, *mut *const c_void, *mut usize) -> c_int>,
}

/// Patch packer interface for bsdiff.
#[repr(C)]
pub struct BsdiffPatchPacker {
    pub state: *mut c_void,
    pub close: Option<unsafe extern "C" fn(*mut c_void)>,
    pub get_mode: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
    pub read_new_size: Option<unsafe extern "C" fn(*mut c_void, *mut i64) -> c_int>,
    pub read_entry_header: Option<unsafe extern "C" fn(*mut c_void, *mut i64, *mut i64, *mut i64) -> c_int>,
    pub read_entry_diff: Option<unsafe extern "C" fn(*mut c_void, *mut c_void, usize, *mut usize) -> c_int>,
    pub read_entry_extra: Option<unsafe extern "C" fn(*mut c_void, *mut c_void, usize, *mut usize) -> c_int>,
    pub write_new_size: Option<unsafe extern "C" fn(*mut c_void, i64) -> c_int>,
    pub write_entry_header: Option<unsafe extern "C" fn(*mut c_void, i64, i64, i64) -> c_int>,
    pub write_entry_diff: Option<unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> c_int>,
    pub write_entry_extra: Option<unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> c_int>,
    pub flush: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
}

/// Context for bsdiff callbacks.
#[repr(C)]
pub struct BsdiffCtx {
    pub opaque: *mut c_void,
    pub log_error: Option<unsafe extern "C" fn(*mut c_void, *const c_char)>,
}

unsafe extern "C" {
    pub fn bsdiff_open_memory_stream(
        mode: c_int,
        buffer: *const c_void,
        size: usize,
        stream: *mut BsdiffStream,
    ) -> c_int;

    pub fn bsdiff_close_stream(stream: *mut BsdiffStream);

    pub fn bsdiff_open_bz2_patch_packer(
        mode: c_int,
        stream: *mut BsdiffStream,
        packer: *mut BsdiffPatchPacker,
    ) -> c_int;

    pub fn bsdiff_close_patch_packer(packer: *mut BsdiffPatchPacker);

    pub fn bsdiff(
        ctx: *mut BsdiffCtx,
        oldfile: *mut BsdiffStream,
        newfile: *mut BsdiffStream,
        packer: *mut BsdiffPatchPacker,
    ) -> c_int;

    pub fn bspatch(
        ctx: *mut BsdiffCtx,
        oldfile: *mut BsdiffStream,
        newfile: *mut BsdiffStream,
        packer: *mut BsdiffPatchPacker,
    ) -> c_int;
}
