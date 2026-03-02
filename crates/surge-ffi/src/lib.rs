//! C API (`cdylib`) for the Surge update framework.
//!
//! This crate produces `libsurge.so` / `surge.dll` / `libsurge.dylib` and
//! exports every function declared in `surge_api.h`.  All 29 public symbols
//! use `#[no_mangle] pub unsafe extern "C"` and catch panics at the boundary.

mod handles;

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;

use surge_core::context::{Context, ResourceBudget, StorageProvider};
use surge_core::diff::wrapper::{bsdiff_buffers, bspatch_buffers};
use surge_core::lock::mutex::DistributedMutex;
use surge_core::pack::builder::PackBuilder;
use surge_core::supervisor::supervisor::Supervisor;
use surge_core::update::manager::{ProgressInfo, UpdateManager};

use crate::handles::{
    ReleaseEntryFfi, SurgeContextHandle, SurgeErrorFfi, SurgePackContextHandle, SurgeReleasesInfoHandle,
    SurgeUpdateManagerHandle,
};

// ---------------------------------------------------------------------------
//  Result codes (mirrors surge_result in surge_api.h)
// ---------------------------------------------------------------------------

const SURGE_OK: i32 = 0;
const SURGE_ERROR: i32 = -1;
const SURGE_CANCELLED: i32 = -2;
const SURGE_NOT_FOUND: i32 = -3;

// ---------------------------------------------------------------------------
//  Progress phases (mirrors surge_progress_phase in surge_api.h)
// ---------------------------------------------------------------------------

const SURGE_PHASE_CHECK: i32 = 0;
const SURGE_PHASE_DOWNLOAD: i32 = 1;
#[allow(dead_code)]
const SURGE_PHASE_VERIFY: i32 = 2;
#[allow(dead_code)]
const SURGE_PHASE_EXTRACT: i32 = 3;
#[allow(dead_code)]
const SURGE_PHASE_APPLY_DELTA: i32 = 4;
#[allow(dead_code)]
const SURGE_PHASE_FINALIZE: i32 = 5;

// ---------------------------------------------------------------------------
//  #[repr(C)] structs matching surge_api.h
// ---------------------------------------------------------------------------

/// Matches `surge_progress` in surge_api.h.
#[repr(C)]
pub struct SurgeProgressFfi {
    pub phase: i32,
    pub phase_percent: i32,
    pub total_percent: i32,
    pub bytes_done: i64,
    pub bytes_total: i64,
    pub items_done: i64,
    pub items_total: i64,
    pub speed_bytes_per_sec: f64,
}

/// Matches `surge_resource_budget` in surge_api.h.
#[repr(C)]
pub struct SurgeResourceBudgetFfi {
    pub max_memory_bytes: i64,
    pub max_threads: i32,
    pub max_concurrent_downloads: i32,
    pub max_download_speed_bps: i64,
    pub zstd_compression_level: i32,
}

/// Matches `surge_bsdiff_ctx` in surge_api.h.
#[repr(C)]
pub struct SurgeBsdiffCtxFfi {
    pub older: *const u8,
    pub older_size: i64,
    pub newer: *const u8,
    pub newer_size: i64,
    pub patch: *mut u8,
    pub patch_size: i64,
    pub status: i32,
}

/// Matches `surge_bspatch_ctx` in surge_api.h.
#[repr(C)]
pub struct SurgeBspatchCtxFfi {
    pub older: *const u8,
    pub older_size: i64,
    pub newer: *mut u8,
    pub newer_size: i64,
    pub patch: *const u8,
    pub patch_size: i64,
    pub status: i32,
}

/// Callback type matching `surge_progress_callback` in surge_api.h.
type SurgeProgressCallback = Option<unsafe extern "C" fn(*const SurgeProgressFfi, *mut c_void)>;

/// Callback type matching `surge_event_callback` in surge_api.h.
type SurgeEventCallback = Option<unsafe extern "C" fn(*const c_char, *mut c_void)>;

// ---------------------------------------------------------------------------
//  Helpers
// ---------------------------------------------------------------------------

/// Bridges a C progress callback + user_data pointer so that it satisfies
/// the `Send + Sync` bounds required by `UpdateManager::download_and_apply`.
///
/// # Safety
///
/// Only safe when the pointer is valid for the duration of the async call
/// and only accessed from the calling thread (via `Runtime::block_on`).
struct ProgressBridge {
    cb: unsafe extern "C" fn(*const SurgeProgressFfi, *mut c_void),
    user_data: *mut c_void,
}

// SAFETY: The C API contract guarantees single-threaded access per context,
// and we only use this with Runtime::block_on (same thread).
unsafe impl Send for ProgressBridge {}
unsafe impl Sync for ProgressBridge {}

impl ProgressBridge {
    /// Convert a core `ProgressInfo` to its FFI representation and invoke
    /// the C callback.  Core phases are 1-indexed; FFI phases are 0-indexed.
    fn invoke(&self, pi: &ProgressInfo) {
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
        unsafe { (self.cb)(&ffi, self.user_data) };
    }
}

/// Build a `SurgeProgressFfi` from pack-style `(items_done, items_total)` counters.
fn make_pack_progress(phase: i32, items_done: i32, items_total: i32) -> SurgeProgressFfi {
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

/// Convert a nullable C string pointer to a Rust `&str`, defaulting to `""`.
///
/// # Safety
///
/// `p` must be null or point to a valid NUL-terminated C string.
unsafe fn cstr_to_str<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        ""
    } else {
        unsafe { CStr::from_ptr(p) }.to_str().unwrap_or("")
    }
}

/// Run a closure, catching panics and returning `SURGE_ERROR` on panic.
/// On success the closure should return a `surge_result` code.
fn catch_ffi<F: FnOnce() -> i32 + std::panic::UnwindSafe>(f: F) -> i32 {
    match std::panic::catch_unwind(f) {
        Ok(code) => code,
        Err(_) => SURGE_ERROR,
    }
}

/// Store an error on the context handle and return the appropriate error code.
///
/// # Safety
///
/// `handle` must be null or point to a valid `SurgeContextHandle`.
unsafe fn set_ctx_error(handle: *const SurgeContextHandle, e: &surge_core::error::SurgeError) -> i32 {
    if handle.is_null() {
        SURGE_ERROR
    } else {
        let h = unsafe { &*handle };
        let code = e.error_code() as i32;
        unsafe { h.set_last_error(code, &e.to_string()) };
        h.ctx.set_error(e);
        code
    }
}

// =========================================================================
//  1. Lifecycle (3 functions)
// =========================================================================

/// Create a new Surge context.
///
/// Returns a new context handle, or null on allocation/runtime failure.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_context_create() -> *mut SurgeContextHandle {
    let result = std::panic::catch_unwind(|| {
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(_) => return ptr::null_mut(),
        };

        let ctx = Arc::new(Context::new());
        let handle = Box::new(SurgeContextHandle {
            ctx,
            runtime,
            last_error: std::cell::UnsafeCell::new(None),
        });

        Box::into_raw(handle)
    });

    result.unwrap_or(ptr::null_mut())
}

/// Destroy a Surge context and release all associated resources.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_context_destroy(ctx: *mut SurgeContextHandle) {
    if !ctx.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(ctx) });
        }));
    }
}

/// Retrieve the last error that occurred on `ctx`.
///
/// Returns a pointer to an internal `surge_error` struct, or null if no error.
/// The pointer is valid until the next API call on the same context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_context_last_error(ctx: *const SurgeContextHandle) -> *const SurgeErrorFfi {
    if ctx.is_null() {
        return ptr::null();
    }
    let handle = unsafe { &*ctx };
    unsafe { handle.get_last_error() }
}

// =========================================================================
//  2. Configuration (3 functions)
// =========================================================================

/// Configure the cloud or local storage backend.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_config_set_storage(
    ctx: *mut SurgeContextHandle,
    provider: i32,
    bucket: *const c_char,
    region: *const c_char,
    access_key: *const c_char,
    secret_key: *const c_char,
    endpoint: *const c_char,
) -> i32 {
    if ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        let prov = if let Some(p) = StorageProvider::from_i32(provider) {
            p
        } else {
            let e = surge_core::error::SurgeError::Config(format!("Invalid storage provider: {provider}"));
            return unsafe { set_ctx_error(ctx, &e) };
        };

        let bucket_s = unsafe { cstr_to_str(bucket) };
        let region_s = unsafe { cstr_to_str(region) };
        let access_s = unsafe { cstr_to_str(access_key) };
        let secret_s = unsafe { cstr_to_str(secret_key) };
        let endpoint_s = unsafe { cstr_to_str(endpoint) };

        handle
            .ctx
            .set_storage(prov, bucket_s, region_s, access_s, secret_s, endpoint_s);
        SURGE_OK
    }))
}

/// Configure the distributed lock server URL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_config_set_lock_server(ctx: *mut SurgeContextHandle, url: *const c_char) -> i32 {
    if ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        let url_s = unsafe { cstr_to_str(url) };
        handle.ctx.set_lock_server(url_s);
        SURGE_OK
    }))
}

/// Set resource budget limits (memory, threads, bandwidth).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_config_set_resource_budget(
    ctx: *mut SurgeContextHandle,
    budget: *const SurgeResourceBudgetFfi,
) -> i32 {
    if ctx.is_null() || budget.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        let b = unsafe { &*budget };
        let rb = ResourceBudget {
            max_memory_bytes: b.max_memory_bytes,
            max_threads: b.max_threads,
            max_concurrent_downloads: b.max_concurrent_downloads,
            max_download_speed_bps: b.max_download_speed_bps,
            zstd_compression_level: b.zstd_compression_level,
        };
        handle.ctx.set_resource_budget(rb);
        SURGE_OK
    }))
}

// =========================================================================
//  3. Update manager (4 functions)
// =========================================================================

/// Create an update manager bound to a specific application.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_create(
    ctx: *mut SurgeContextHandle,
    app_id: *const c_char,
    current_version: *const c_char,
    channel: *const c_char,
    install_dir: *const c_char,
) -> *mut SurgeUpdateManagerHandle {
    if ctx.is_null() {
        return ptr::null_mut();
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        let app_id_s = unsafe { cstr_to_str(app_id) }.to_string();
        let version_s = unsafe { cstr_to_str(current_version) }.to_string();
        let channel_s = unsafe { cstr_to_str(channel) }.to_string();
        let install_s = unsafe { cstr_to_str(install_dir) }.to_string();

        if app_id_s.is_empty() || version_s.is_empty() || channel_s.is_empty() || install_s.is_empty() {
            let e =
                surge_core::error::SurgeError::Config("app_id, version, channel, and install_dir are required".into());
            unsafe { set_ctx_error(ctx, &e) };
            return ptr::null_mut();
        }

        let mgr = Box::new(SurgeUpdateManagerHandle {
            ctx_handle: ctx,
            app_id: app_id_s,
            current_version: version_s,
            channel: channel_s,
            install_dir: install_s,
        });

        Box::into_raw(mgr)
    }));

    result.unwrap_or(ptr::null_mut())
}

/// Destroy an update manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_destroy(mgr: *mut SurgeUpdateManagerHandle) {
    if !mgr.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(mgr) });
        }));
    }
}

/// Check for available updates.
///
/// Returns `SURGE_OK` if updates are available, `SURGE_NOT_FOUND` if up-to-date.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_check(
    mgr: *mut SurgeUpdateManagerHandle,
    info: *mut *mut SurgeReleasesInfoHandle,
) -> i32 {
    if mgr.is_null() || info.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let mgr_ref = unsafe { &*mgr };
        let ctx_handle = mgr_ref.ctx_handle;
        if ctx_handle.is_null() {
            return SURGE_ERROR;
        }
        let handle = unsafe { &*ctx_handle };
        unsafe { handle.clear_last_error() };

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let mut update_mgr = match UpdateManager::new(
            handle.ctx.clone(),
            &mgr_ref.app_id,
            &mgr_ref.current_version,
            &mgr_ref.channel,
            &mgr_ref.install_dir,
        ) {
            Ok(m) => m,
            Err(e) => return unsafe { set_ctx_error(ctx_handle, &e) },
        };

        let result = handle.runtime.block_on(update_mgr.check_for_updates());

        match result {
            Ok(Some(update_info)) => {
                let ffi_releases: Vec<ReleaseEntryFfi> = update_info
                    .available_releases
                    .iter()
                    .map(|r| ReleaseEntryFfi {
                        version: r.version.clone(),
                        channel: r.channels.first().cloned().unwrap_or_default(),
                        full_size: r.full_size,
                        is_genesis: r.is_genesis,
                    })
                    .collect();

                let mut releases_handle = Box::new(SurgeReleasesInfoHandle {
                    releases: ffi_releases,
                    cached_strings: Vec::new(),
                    update_info: Some(update_info),
                });
                releases_handle.cache_strings();

                unsafe { *info = Box::into_raw(releases_handle) };
                SURGE_OK
            }
            Ok(None) => {
                let releases_handle = Box::new(SurgeReleasesInfoHandle {
                    releases: Vec::new(),
                    cached_strings: Vec::new(),
                    update_info: None,
                });
                unsafe { *info = Box::into_raw(releases_handle) };
                SURGE_NOT_FOUND
            }
            Err(e) => unsafe { set_ctx_error(ctx_handle, &e) },
        }
    }))
}

/// Download and apply an update described by `info`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_download_and_apply(
    mgr: *mut SurgeUpdateManagerHandle,
    info: *const SurgeReleasesInfoHandle,
    progress_cb: SurgeProgressCallback,
    user_data: *mut c_void,
) -> i32 {
    if mgr.is_null() || info.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let mgr_ref = unsafe { &*mgr };
        let ctx_handle = mgr_ref.ctx_handle;
        if ctx_handle.is_null() {
            return SURGE_ERROR;
        }
        let handle = unsafe { &*ctx_handle };
        unsafe { handle.clear_last_error() };

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let info_ref = unsafe { &*info };

        let update_info = match info_ref.update_info.as_ref() {
            Some(ui) => ui,
            None => {
                let e = surge_core::error::SurgeError::Update("No update info available".into());
                return unsafe { set_ctx_error(ctx_handle, &e) };
            }
        };

        if update_info.available_releases.is_empty() {
            let e = surge_core::error::SurgeError::Update("No releases to apply".into());
            return unsafe { set_ctx_error(ctx_handle, &e) };
        }

        let update_mgr = match UpdateManager::new(
            handle.ctx.clone(),
            &mgr_ref.app_id,
            &mgr_ref.current_version,
            &mgr_ref.channel,
            &mgr_ref.install_dir,
        ) {
            Ok(m) => m,
            Err(e) => return unsafe { set_ctx_error(ctx_handle, &e) },
        };

        // Map core ProgressInfo → SurgeProgressFfi via the C callback.
        // Handle Some/None explicitly to give the compiler a concrete type
        // for the generic F in download_and_apply<F: Send + Sync>.
        let result = if let Some(cb) = progress_cb {
            let bridge = ProgressBridge { cb, user_data };
            let progress_fn = move |pi: ProgressInfo| {
                bridge.invoke(&pi);
            };
            handle
                .runtime
                .block_on(update_mgr.download_and_apply(update_info, Some(progress_fn)))
        } else {
            handle
                .runtime
                .block_on(update_mgr.download_and_apply(update_info, None::<fn(ProgressInfo)>))
        };

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => unsafe { set_ctx_error(ctx_handle, &e) },
        }
    }))
}

// =========================================================================
//  4. Release-info accessors (6 functions)
// =========================================================================

/// Return the number of releases in `info`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_releases_count(info: *const SurgeReleasesInfoHandle) -> i32 {
    if info.is_null() {
        return 0;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let h = unsafe { &*info };
        h.releases.len() as i32
    }));

    result.unwrap_or(0)
}

/// Free a releases-info structure returned by `surge_update_check`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_releases_destroy(info: *mut SurgeReleasesInfoHandle) {
    if !info.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(info) });
        }));
    }
}

/// Return the version string for release at `index`.
///
/// The returned pointer is valid for the lifetime of the `info` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_release_version(info: *const SurgeReleasesInfoHandle, index: i32) -> *const c_char {
    if info.is_null() {
        return ptr::null();
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let h = unsafe { &*info };
        let idx = index as usize;
        if idx >= h.cached_strings.len() {
            return ptr::null();
        }
        h.cached_strings[idx].0.as_ptr()
    }));

    result.unwrap_or(ptr::null())
}

/// Return the channel string for release at `index`.
///
/// The returned pointer is valid for the lifetime of the `info` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_release_channel(info: *const SurgeReleasesInfoHandle, index: i32) -> *const c_char {
    if info.is_null() {
        return ptr::null();
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let h = unsafe { &*info };
        let idx = index as usize;
        if idx >= h.cached_strings.len() {
            return ptr::null();
        }
        h.cached_strings[idx].1.as_ptr()
    }));

    result.unwrap_or(ptr::null())
}

/// Return the full-package size in bytes for release at `index`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_release_full_size(info: *const SurgeReleasesInfoHandle, index: i32) -> i64 {
    if info.is_null() {
        return 0;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let h = unsafe { &*info };
        let idx = index as usize;
        if idx >= h.releases.len() {
            return 0;
        }
        h.releases[idx].full_size
    }));

    result.unwrap_or(0)
}

/// Return non-zero if release at `index` is a genesis (initial) release.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_release_is_genesis(info: *const SurgeReleasesInfoHandle, index: i32) -> i32 {
    if info.is_null() {
        return 0;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let h = unsafe { &*info };
        let idx = index as usize;
        if idx >= h.releases.len() {
            return 0;
        }
        i32::from(h.releases[idx].is_genesis)
    }));

    result.unwrap_or(0)
}

// =========================================================================
//  5. Binary diff / patch -- bsdiff / bspatch (4 functions)
// =========================================================================

/// Create a binary diff patch.
///
/// On success, `ctx->patch` and `ctx->patch_size` are set.
/// Free the patch buffer with `surge_bsdiff_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_bsdiff(ctx: *mut SurgeBsdiffCtxFfi) -> i32 {
    if ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let c = unsafe { &mut *ctx };

        if c.older.is_null() || c.older_size <= 0 || c.newer.is_null() || c.newer_size <= 0 {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        }

        let older = unsafe { std::slice::from_raw_parts(c.older, c.older_size as usize) };
        let newer = unsafe { std::slice::from_raw_parts(c.newer, c.newer_size as usize) };

        match bsdiff_buffers(older, newer) {
            Ok(patch) => {
                let len = patch.len();
                let boxed = patch.into_boxed_slice();
                let ptr = Box::into_raw(boxed).cast::<u8>();
                c.patch = ptr;
                c.patch_size = len as i64;
                c.status = SURGE_OK;
                SURGE_OK
            }
            Err(e) => {
                c.patch = ptr::null_mut();
                c.patch_size = 0;
                c.status = SURGE_ERROR;
                tracing::error!("bsdiff failed: {e}");
                SURGE_ERROR
            }
        }
    }))
}

/// Apply a binary diff patch.
///
/// On success, `ctx->newer` and `ctx->newer_size` are set.
/// Free the output buffer with `surge_bspatch_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_bspatch(ctx: *mut SurgeBspatchCtxFfi) -> i32 {
    if ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let c = unsafe { &mut *ctx };

        if c.older.is_null() || c.older_size <= 0 || c.patch.is_null() || c.patch_size <= 0 {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        }

        let older = unsafe { std::slice::from_raw_parts(c.older, c.older_size as usize) };
        let patch = unsafe { std::slice::from_raw_parts(c.patch, c.patch_size as usize) };

        match bspatch_buffers(older, patch) {
            Ok(newer) => {
                let len = newer.len();
                let boxed = newer.into_boxed_slice();
                let ptr = Box::into_raw(boxed).cast::<u8>();
                c.newer = ptr;
                c.newer_size = len as i64;
                c.status = SURGE_OK;
                SURGE_OK
            }
            Err(e) => {
                c.newer = ptr::null_mut();
                c.newer_size = 0;
                c.status = SURGE_ERROR;
                tracing::error!("bspatch failed: {e}");
                SURGE_ERROR
            }
        }
    }))
}

/// Free the patch buffer allocated by `surge_bsdiff`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_bsdiff_free(ctx: *mut SurgeBsdiffCtxFfi) {
    if ctx.is_null() {
        return;
    }

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let c = unsafe { &mut *ctx };
        if !c.patch.is_null() && c.patch_size > 0 {
            // Reconstruct the boxed slice and drop it.
            let slice_ptr = core::ptr::slice_from_raw_parts_mut(c.patch, c.patch_size as usize);
            drop(unsafe { Box::from_raw(slice_ptr) });
            c.patch = ptr::null_mut();
            c.patch_size = 0;
        }
    }));
}

/// Free the newer buffer allocated by `surge_bspatch`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_bspatch_free(ctx: *mut SurgeBspatchCtxFfi) {
    if ctx.is_null() {
        return;
    }

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let c = unsafe { &mut *ctx };
        if !c.newer.is_null() && c.newer_size > 0 {
            let slice_ptr = core::ptr::slice_from_raw_parts_mut(c.newer, c.newer_size as usize);
            drop(unsafe { Box::from_raw(slice_ptr) });
            c.newer = ptr::null_mut();
            c.newer_size = 0;
        }
    }));
}

// =========================================================================
//  6. Pack builder (4 functions)
// =========================================================================

/// Create a new pack context for building release packages.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_pack_create(
    ctx: *mut SurgeContextHandle,
    manifest_path: *const c_char,
    app_id: *const c_char,
    rid: *const c_char,
    version: *const c_char,
    artifacts_dir: *const c_char,
) -> *mut SurgePackContextHandle {
    if ctx.is_null() {
        return ptr::null_mut();
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        let manifest_s = unsafe { cstr_to_str(manifest_path) }.to_string();
        let app_id_s = unsafe { cstr_to_str(app_id) }.to_string();
        let rid_s = unsafe { cstr_to_str(rid) }.to_string();
        let version_s = unsafe { cstr_to_str(version) }.to_string();
        let artifacts_s = unsafe { cstr_to_str(artifacts_dir) }.to_string();

        if manifest_s.is_empty() || app_id_s.is_empty() || version_s.is_empty() || artifacts_s.is_empty() {
            let e = surge_core::error::SurgeError::Config(
                "manifest_path, app_id, version, and artifacts_dir are required".into(),
            );
            unsafe { set_ctx_error(ctx, &e) };
            return ptr::null_mut();
        }

        let pack = Box::new(SurgePackContextHandle {
            ctx_handle: ctx,
            manifest_path: manifest_s,
            app_id: app_id_s,
            rid: rid_s,
            version: version_s,
            artifacts_dir: artifacts_s,
            builder: std::cell::UnsafeCell::new(None),
        });

        Box::into_raw(pack)
    }));

    result.unwrap_or(ptr::null_mut())
}

/// Build release packages (full + delta).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_pack_build(
    pack_ctx: *mut SurgePackContextHandle,
    progress_cb: SurgeProgressCallback,
    user_data: *mut c_void,
) -> i32 {
    if pack_ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let pack = unsafe { &*pack_ctx };
        let ctx_handle = pack.ctx_handle;
        if ctx_handle.is_null() {
            return SURGE_ERROR;
        }
        let handle = unsafe { &*ctx_handle };
        unsafe { handle.clear_last_error() };

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let mut builder = match PackBuilder::new(
            handle.ctx.clone(),
            &pack.manifest_path,
            &pack.app_id,
            &pack.rid,
            &pack.version,
            &pack.artifacts_dir,
        ) {
            Ok(b) => b,
            Err(e) => return unsafe { set_ctx_error(ctx_handle, &e) },
        };

        let progress_fn = progress_cb.map(|cb| {
            move |done: i32, total: i32| {
                let ffi = make_pack_progress(SURGE_PHASE_CHECK, done, total);
                unsafe { cb(&ffi, user_data) };
            }
        });

        let result = handle
            .runtime
            .block_on(builder.build(progress_fn.as_ref().map(|f| f as &dyn Fn(i32, i32))));

        match result {
            Ok(()) => {
                // Store the builder so surge_pack_push can use it.
                unsafe {
                    let slot = &mut *pack.builder.get();
                    *slot = Some(builder);
                }
                SURGE_OK
            }
            Err(e) => unsafe { set_ctx_error(ctx_handle, &e) },
        }
    }))
}

/// Push built packages to the configured storage backend.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_pack_push(
    pack_ctx: *mut SurgePackContextHandle,
    channel: *const c_char,
    progress_cb: SurgeProgressCallback,
    user_data: *mut c_void,
) -> i32 {
    if pack_ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let pack = unsafe { &*pack_ctx };
        let ctx_handle = pack.ctx_handle;
        if ctx_handle.is_null() {
            return SURGE_ERROR;
        }
        let handle = unsafe { &*ctx_handle };
        unsafe { handle.clear_last_error() };

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let channel_s = unsafe { cstr_to_str(channel) };

        // Take the builder that was stored by surge_pack_build.
        let builder = unsafe {
            let slot = &mut *pack.builder.get();
            slot.take()
        };

        let builder = match builder {
            Some(b) => b,
            None => {
                let e =
                    surge_core::error::SurgeError::Pack("No builder available. Call surge_pack_build first.".into());
                return unsafe { set_ctx_error(ctx_handle, &e) };
            }
        };

        let progress_fn = progress_cb.map(|cb| {
            move |done: i32, total: i32| {
                let ffi = make_pack_progress(SURGE_PHASE_DOWNLOAD, done, total);
                unsafe { cb(&ffi, user_data) };
            }
        });

        let result = handle
            .runtime
            .block_on(builder.push(channel_s, progress_fn.as_ref().map(|f| f as &dyn Fn(i32, i32))));

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => unsafe { set_ctx_error(ctx_handle, &e) },
        }
    }))
}

/// Destroy a pack context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_pack_destroy(pack_ctx: *mut SurgePackContextHandle) {
    if !pack_ctx.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(pack_ctx) });
        }));
    }
}

// =========================================================================
//  7. Distributed lock (2 functions)
// =========================================================================

/// Acquire a named distributed lock.
///
/// On success, `*challenge_out` receives a C string that must be passed to
/// `surge_lock_release`. The caller must free it with `free()`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_lock_acquire(
    ctx: *mut SurgeContextHandle,
    name: *const c_char,
    timeout_seconds: i32,
    challenge_out: *mut *mut c_char,
) -> i32 {
    if ctx.is_null() || name.is_null() || challenge_out.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let name_s = unsafe { cstr_to_str(name) };

        let mut mutex = DistributedMutex::new(handle.ctx.clone(), name_s);
        let result = handle.runtime.block_on(mutex.try_acquire(timeout_seconds));

        match result {
            Ok(true) => {
                let token = mutex.challenge().unwrap_or("");
                let c_challenge = CString::new(token).unwrap_or_default();
                let len = c_challenge.as_bytes_with_nul().len();
                let buf = unsafe { libc_malloc(len) }.cast::<c_char>();
                if buf.is_null() {
                    let e = surge_core::error::SurgeError::Other("malloc failed".into());
                    return unsafe { set_ctx_error(ctx, &e) };
                }
                unsafe {
                    ptr::copy_nonoverlapping(c_challenge.as_ptr(), buf, len);
                    *challenge_out = buf;
                }
                SURGE_OK
            }
            Ok(false) => {
                let e = surge_core::error::SurgeError::Lock("Lock is held by another process".into());
                unsafe { set_ctx_error(ctx, &e) }
            }
            Err(e) => unsafe { set_ctx_error(ctx, &e) },
        }
    }))
}

/// Release a previously acquired distributed lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_lock_release(
    ctx: *mut SurgeContextHandle,
    name: *const c_char,
    challenge: *const c_char,
) -> i32 {
    if ctx.is_null() || name.is_null() || challenge.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        unsafe { handle.clear_last_error() };

        let name_s = unsafe { cstr_to_str(name) };
        let challenge_s = unsafe { cstr_to_str(challenge) };

        let mut mutex = DistributedMutex::new(handle.ctx.clone(), name_s);
        mutex.set_challenge(challenge_s.to_string());

        let result = handle.runtime.block_on(mutex.try_release());

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => unsafe { set_ctx_error(ctx, &e) },
        }
    }))
}

// =========================================================================
//  8. Supervisor (1 function)
// =========================================================================

/// Start a supervised process.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_supervisor_start(
    exe_path: *const c_char,
    working_dir: *const c_char,
    supervisor_id: *const c_char,
    argc: c_int,
    argv: *const *const c_char,
) -> i32 {
    catch_ffi(std::panic::AssertUnwindSafe(|| {
        if exe_path.is_null() || working_dir.is_null() || supervisor_id.is_null() {
            return SURGE_ERROR;
        }

        let exe_s = unsafe { cstr_to_str(exe_path) };
        let wd_s = unsafe { cstr_to_str(working_dir) };
        let sup_id = unsafe { cstr_to_str(supervisor_id) };

        // Collect argv into a Vec<&str>.
        let mut args: Vec<&str> = Vec::new();
        if argc > 0 && !argv.is_null() {
            for i in 0..argc as isize {
                let arg_ptr = unsafe { *argv.offset(i) };
                if !arg_ptr.is_null() {
                    args.push(unsafe { cstr_to_str(arg_ptr) });
                }
            }
        }

        let mut supervisor = Supervisor::new(sup_id, wd_s);

        match supervisor.start(exe_s, wd_s, &args) {
            Ok(()) => SURGE_OK,
            Err(e) => {
                tracing::error!("supervisor_start failed: {e}");
                SURGE_ERROR
            }
        }
    }))
}

// =========================================================================
//  9. Lifecycle events (1 function)
// =========================================================================

/// Process application lifecycle events.
///
/// Call early in `main()` to handle first-run, post-install, and post-update
/// hooks.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_process_events(
    argc: c_int,
    argv: *const *const c_char,
    on_first_run: SurgeEventCallback,
    on_installed: SurgeEventCallback,
    on_updated: SurgeEventCallback,
    user_data: *mut c_void,
) -> i32 {
    catch_ffi(std::panic::AssertUnwindSafe(|| {
        // Collect argv for inspection.
        let mut args: Vec<&str> = Vec::new();
        if argc > 0 && !argv.is_null() {
            for i in 0..argc as isize {
                let arg_ptr = unsafe { *argv.offset(i) };
                if !arg_ptr.is_null() {
                    args.push(unsafe { cstr_to_str(arg_ptr) });
                }
            }
        }

        // TODO: When surge_core::platform lifecycle detection is implemented,
        // inspect command-line flags / sentinel files to determine which
        // event to fire.  For now, look for surge-specific CLI flags.
        for arg in &args {
            match *arg {
                "--surge-first-run" => {
                    if let Some(cb) = on_first_run {
                        let version = CString::new("0.0.0").unwrap();
                        unsafe { cb(version.as_ptr(), user_data) };
                    }
                }
                "--surge-installed" => {
                    if let Some(cb) = on_installed {
                        let version = CString::new("0.0.0").unwrap();
                        unsafe { cb(version.as_ptr(), user_data) };
                    }
                }
                _ => {
                    if arg.starts_with("--surge-updated=")
                        && let Some(cb) = on_updated
                    {
                        let ver = &arg["--surge-updated=".len()..];
                        let version = CString::new(ver).unwrap_or_default();
                        unsafe { cb(version.as_ptr(), user_data) };
                    }
                }
            }
        }

        SURGE_OK
    }))
}

// =========================================================================
//  10. Cancellation (1 function)
// =========================================================================

/// Request cancellation of any in-progress operation on `ctx`.
///
/// Thread-safe: may be called from any thread.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_cancel(ctx: *mut SurgeContextHandle) -> i32 {
    if ctx.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let handle = unsafe { &*ctx };
        handle.ctx.cancel();
        SURGE_OK
    }))
}

// =========================================================================
//  Internal helpers
// =========================================================================

/// Thin wrapper around platform `malloc` for allocating buffers that C callers
/// will free with `free()`.
///
/// # Safety
///
/// Returns a pointer to `size` bytes of uninitialized memory, or null on failure.
unsafe fn libc_malloc(size: usize) -> *mut u8 {
    unsafe extern "C" {
        fn malloc(size: usize) -> *mut c_void;
    }
    unsafe { malloc(size).cast::<u8>() }
}
