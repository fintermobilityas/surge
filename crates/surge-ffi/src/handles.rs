//! Opaque FFI handle types that own Rust objects across the C boundary.
//!
//! Each handle is created with `Box::new()` and returned as a raw pointer.
//! The corresponding `_destroy` function reclaims ownership with `Box::from_raw()`.

use std::ffi::{CString, c_char};
use std::sync::Arc;

use surge_core::context::Context;

// ---------------------------------------------------------------------------
//  FFI error struct (matches `surge_error` in surge_api.h)
// ---------------------------------------------------------------------------

/// FFI-safe error information returned by `surge_context_last_error`.
///
/// Layout: `{ code: i32, message: *const c_char }` -- the `_message_owned`
/// field keeps the `CString` alive so that `message` remains valid.
#[repr(C)]
pub struct SurgeErrorFfi {
    pub code: i32,
    pub message: *const c_char,
    /// Backing storage for `message`.  Not visible to C callers.
    _message_owned: CString,
}

impl SurgeErrorFfi {
    pub fn new(code: i32, msg: &str) -> Self {
        let owned = CString::new(msg).unwrap_or_else(|_| CString::new("(invalid utf-8 in error message)").unwrap());
        let ptr = owned.as_ptr();
        Self {
            code,
            message: ptr,
            _message_owned: owned,
        }
    }
}

// ---------------------------------------------------------------------------
//  Context handle
// ---------------------------------------------------------------------------

/// Opaque handle wrapping a `Context` + tokio `Runtime`.
///
/// Returned by `surge_context_create`, destroyed by `surge_context_destroy`.
pub struct SurgeContextHandle {
    pub ctx: Arc<Context>,
    pub runtime: tokio::runtime::Runtime,
    /// Cached last-error for FFI return.  We use `UnsafeCell` because the
    /// pointer returned by `surge_context_last_error` must remain stable
    /// until the next API call on the same context.
    pub last_error: std::cell::UnsafeCell<Option<SurgeErrorFfi>>,
}

impl SurgeContextHandle {
    /// Store an error so that `surge_context_last_error` can return it.
    ///
    /// # Safety
    /// Must only be called while no other thread is reading `last_error`.
    /// In practice this is fine because the C API contract says the pointer
    /// from `last_error()` is valid only until the *next* API call.
    pub unsafe fn set_last_error(&self, code: i32, msg: &str) {
        unsafe {
            let slot = &mut *self.last_error.get();
            *slot = Some(SurgeErrorFfi::new(code, msg));
        }
    }

    /// Clear any previously stored error.
    ///
    /// # Safety
    /// Same constraints as `set_last_error`.
    pub unsafe fn clear_last_error(&self) {
        unsafe {
            let slot = &mut *self.last_error.get();
            *slot = None;
        }
    }

    /// Return a pointer to the cached error, or null if none.
    ///
    /// # Safety
    /// The returned pointer is valid until the next mutable access to `last_error`.
    pub unsafe fn get_last_error(&self) -> *const SurgeErrorFfi {
        unsafe {
            let slot = &*self.last_error.get();
            match slot {
                Some(e) => std::ptr::from_ref::<SurgeErrorFfi>(e),
                None => std::ptr::null(),
            }
        }
    }
}

// SAFETY: The `UnsafeCell<Option<SurgeErrorFfi>>` is only accessed from the
// FFI boundary where the C API contract guarantees single-threaded access
// per context (except `surge_cancel` which does not touch `last_error`).
unsafe impl Send for SurgeContextHandle {}
unsafe impl Sync for SurgeContextHandle {}

// ---------------------------------------------------------------------------
//  Update manager handle
// ---------------------------------------------------------------------------

/// Opaque handle for the update manager.
///
/// Holds a raw pointer back to its parent `SurgeContextHandle` so that async
/// operations can use the tokio runtime and the `Context`.
pub struct SurgeUpdateManagerHandle {
    pub ctx_handle: *const SurgeContextHandle,
    pub app_id: String,
    pub current_version: String,
    pub channel: String,
    pub install_dir: String,
}

// SAFETY: The raw pointer is only dereferenced while the parent context is alive
// (the C API contract requires the context to outlive the manager).
unsafe impl Send for SurgeUpdateManagerHandle {}
unsafe impl Sync for SurgeUpdateManagerHandle {}

// ---------------------------------------------------------------------------
//  Releases info handle
// ---------------------------------------------------------------------------

/// Represents a single release entry returned from an update check.
pub struct ReleaseEntryFfi {
    pub version: String,
    pub channel: String,
    pub full_size: i64,
    pub is_genesis: bool,
}

/// Opaque handle for releases info (returned from `surge_update_check`).
pub struct SurgeReleasesInfoHandle {
    pub releases: Vec<ReleaseEntryFfi>,
    /// Cached `CString`s for version/channel accessors so that the returned
    /// `*const c_char` pointers remain valid for the lifetime of the handle.
    pub cached_strings: Vec<(CString, CString)>,
}

impl SurgeReleasesInfoHandle {
    /// Build cached CStrings from the release entries.
    pub fn cache_strings(&mut self) {
        self.cached_strings = self
            .releases
            .iter()
            .map(|r| {
                let ver = CString::new(r.version.as_str()).unwrap_or_default();
                let ch = CString::new(r.channel.as_str()).unwrap_or_default();
                (ver, ch)
            })
            .collect();
    }
}

// ---------------------------------------------------------------------------
//  Pack context handle
// ---------------------------------------------------------------------------

/// Opaque handle for the pack builder.
pub struct SurgePackContextHandle {
    pub ctx_handle: *const SurgeContextHandle,
    pub manifest_path: String,
    pub app_id: String,
    pub rid: String,
    pub version: String,
    pub artifacts_dir: String,
}

// SAFETY: Same reasoning as SurgeUpdateManagerHandle.
unsafe impl Send for SurgePackContextHandle {}
unsafe impl Sync for SurgePackContextHandle {}
