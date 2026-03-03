//! Opaque FFI handle types that own Rust objects across the C boundary.
//!
//! Each handle is created with `Box::new()` and returned as a raw pointer.
//! The corresponding `_destroy` function reclaims ownership with `Box::from_raw()`.

use std::ffi::{CString, c_char};
use std::sync::{Arc, Mutex, MutexGuard};

use surge_core::context::Context;
use surge_core::pack::builder::PackBuilder;
use surge_core::update::manager::UpdateInfo;

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
        let owned = CString::new(msg).unwrap_or_default();
        let ptr = owned.as_ptr();
        Self {
            code,
            message: ptr,
            _message_owned: owned,
        }
    }
}

/// Thread-safe owned error state shared between related FFI handles.
pub struct SurgeErrorOwned {
    pub code: i32,
    pub message: CString,
}

impl SurgeErrorOwned {
    pub fn new(code: i32, msg: &str) -> Self {
        let message = CString::new(msg).unwrap_or_default();
        Self { code, message }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
//  Context handle
// ---------------------------------------------------------------------------

/// Opaque handle wrapping a `Context` + tokio `Runtime`.
///
/// Returned by `surge_context_create`, destroyed by `surge_context_destroy`.
pub struct SurgeContextHandle {
    pub ctx: Arc<Context>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    /// Cached FFI shape used by `surge_context_last_error`.
    pub last_error: Mutex<Option<SurgeErrorFfi>>,
    /// Cached last-error for FFI return.
    ///
    /// The returned pointer from `surge_context_last_error` remains valid
    /// until the next API call mutates this slot.
    pub shared_last_error: Arc<Mutex<Option<SurgeErrorOwned>>>,
}

impl SurgeContextHandle {
    /// Store an error so that `surge_context_last_error` can return it.
    pub fn set_last_error(&self, code: i32, msg: &str) {
        {
            let mut shared = lock_recover(&self.shared_last_error);
            *shared = Some(SurgeErrorOwned::new(code, msg));
        }
        let mut slot = lock_recover(&self.last_error);
        *slot = Some(SurgeErrorFfi::new(code, msg));
    }

    /// Clear any previously stored error.
    pub fn clear_last_error(&self) {
        {
            let mut shared = lock_recover(&self.shared_last_error);
            *shared = None;
        }
        let mut slot = lock_recover(&self.last_error);
        *slot = None;
        self.ctx.clear_error();
    }

    /// Return a pointer to the cached error, or null if none.
    pub fn get_last_error(&self) -> *const SurgeErrorFfi {
        let snapshot = {
            let shared = lock_recover(&self.shared_last_error);
            shared.as_ref().map(|e| (e.code, e.message.clone()))
        };

        let mut slot = lock_recover(&self.last_error);
        *slot = snapshot.map(|(code, message)| {
            let ptr = message.as_ptr();
            SurgeErrorFfi {
                code,
                message: ptr,
                _message_owned: message,
            }
        });
        match slot.as_ref() {
            Some(e) => std::ptr::from_ref::<SurgeErrorFfi>(e),
            None => std::ptr::null(),
        }
    }
}

// ---------------------------------------------------------------------------
//  Update manager handle
// ---------------------------------------------------------------------------

/// Opaque handle for the update manager.
///
/// Clones shared context/runtime/error state from `surge_context_create`.
pub struct SurgeUpdateManagerHandle {
    pub ctx: Arc<Context>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    pub last_error: Arc<Mutex<Option<SurgeErrorOwned>>>,
    pub app_id: String,
    pub current_version: String,
    pub channel: String,
    pub install_dir: String,
}

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
    /// Full update info from the core library, preserved so that
    /// `surge_update_download_and_apply` can pass the complete
    /// `ReleaseEntry` data (filenames, hashes, etc.) back to the core.
    pub update_info: Option<UpdateInfo>,
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
    pub ctx: Arc<Context>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    pub last_error: Arc<Mutex<Option<SurgeErrorOwned>>>,
    pub manifest_path: String,
    pub app_id: String,
    pub rid: String,
    pub version: String,
    pub artifacts_dir: String,
    /// Persisted `PackBuilder` between `surge_pack_build` and `surge_pack_push`.
    pub builder: Mutex<Option<PackBuilder>>,
}
