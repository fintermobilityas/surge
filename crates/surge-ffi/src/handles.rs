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

/// `_message_owned` keeps the `CString` alive so `message` stays valid.
#[repr(C)]
pub struct SurgeErrorFfi {
    pub code: i32,
    pub message: *const c_char,
    _message_owned: CString,
}

impl SurgeErrorFfi {
    pub fn new(code: i32, msg: &str) -> Self {
        let owned = to_lossy_cstring(msg);
        let ptr = owned.as_ptr();
        Self {
            code,
            message: ptr,
            _message_owned: owned,
        }
    }
}

pub struct SurgeErrorOwned {
    pub code: i32,
    pub message: CString,
}

impl SurgeErrorOwned {
    pub fn new(code: i32, msg: &str) -> Self {
        let message = to_lossy_cstring(msg);
        Self { code, message }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn to_lossy_cstring(value: &str) -> CString {
    let mut bytes = value.as_bytes().to_vec();
    bytes.retain(|b| *b != 0);
    CString::new(bytes).unwrap_or_default()
}

// ---------------------------------------------------------------------------
//  Context handle
// ---------------------------------------------------------------------------

pub struct SurgeContextHandle {
    pub ctx: Arc<Context>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    pub last_error: Mutex<Option<SurgeErrorFfi>>,
    /// Pointer from `surge_context_last_error` stays valid until the next
    /// error-mutating API call.
    pub shared_last_error: Arc<Mutex<Option<SurgeErrorOwned>>>,
}

impl SurgeContextHandle {
    pub fn set_last_error(&self, code: i32, msg: &str) {
        {
            let mut shared = lock_recover(&self.shared_last_error);
            *shared = Some(SurgeErrorOwned::new(code, msg));
        }
        let mut slot = lock_recover(&self.last_error);
        *slot = Some(SurgeErrorFfi::new(code, msg));
    }

    pub fn clear_last_error(&self) {
        {
            let mut shared = lock_recover(&self.shared_last_error);
            *shared = None;
        }
        let mut slot = lock_recover(&self.last_error);
        *slot = None;
        self.ctx.clear_error();
    }

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

pub struct ReleaseEntryFfi {
    pub version: String,
    pub channel: String,
    pub full_size: i64,
    pub is_genesis: bool,
}

pub struct SurgeReleasesInfoHandle {
    pub releases: Vec<ReleaseEntryFfi>,
    /// Cached `CString`s so returned `*const c_char` pointers stay valid.
    pub cached_strings: Vec<(CString, CString)>,
    /// Preserved for `surge_update_download_and_apply` to forward full
    /// `ReleaseEntry` data back to the core.
    pub update_info: Option<UpdateInfo>,
}

impl SurgeReleasesInfoHandle {
    pub fn cache_strings(&mut self) {
        self.cached_strings = self
            .releases
            .iter()
            .map(|r| {
                (
                    to_lossy_cstring(r.version.as_str()),
                    to_lossy_cstring(r.channel.as_str()),
                )
            })
            .collect();
    }
}

// ---------------------------------------------------------------------------
//  Pack context handle
// ---------------------------------------------------------------------------

pub struct SurgePackContextHandle {
    pub ctx: Arc<Context>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    pub last_error: Arc<Mutex<Option<SurgeErrorOwned>>>,
    pub manifest_path: String,
    pub app_id: String,
    pub rid: String,
    pub version: String,
    pub artifacts_dir: String,
    /// Lives between `surge_pack_build` and `surge_pack_push`.
    pub builder: Mutex<Option<PackBuilder>>,
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::{ReleaseEntryFfi, SurgeErrorFfi, SurgeReleasesInfoHandle};

    #[test]
    fn error_strings_strip_embedded_nuls() {
        let err = SurgeErrorFfi::new(-1, "bad\0message");
        // SAFETY: `SurgeErrorFfi::new` stores a valid NUL-terminated CString
        // and `message` points to that owned storage for `err`'s lifetime.
        let message = unsafe { CStr::from_ptr(err.message) }
            .to_str()
            .expect("valid UTF-8 after sanitization");
        assert_eq!(message, "badmessage");
    }

    #[test]
    fn release_cache_strings_strip_embedded_nuls() {
        let mut info = SurgeReleasesInfoHandle {
            releases: vec![ReleaseEntryFfi {
                version: "1.0\0.0".to_string(),
                channel: "st\0able".to_string(),
                full_size: 1,
                is_genesis: true,
            }],
            cached_strings: Vec::new(),
            update_info: None,
        };
        info.cache_strings();
        assert_eq!(info.cached_strings.len(), 1);
        assert_eq!(info.cached_strings[0].0.to_str().expect("valid version"), "1.0.0");
        assert_eq!(info.cached_strings[0].1.to_str().expect("valid channel"), "stable");
    }
}
