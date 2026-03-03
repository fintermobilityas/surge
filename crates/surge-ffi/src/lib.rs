#![deny(unsafe_op_in_unsafe_fn)]

//! C API (`cdylib`) for the Surge update framework.
//!
//! This crate produces `libsurge.so` / `surge.dll` / `libsurge.dylib` and
//! exports every function declared in `surge_api.h`.  All 29 public symbols
//! use `#[no_mangle] pub unsafe extern "C"` and catch panics at the boundary.

mod handles;

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard};

use surge_core::context::{Context, ResourceBudget, StorageProvider};
use surge_core::diff::wrapper::{bsdiff_buffers, bspatch_buffers};
use surge_core::lock::mutex::DistributedMutex;
use surge_core::pack::builder::PackBuilder;
use surge_core::supervisor::supervisor::Supervisor;
use surge_core::update::manager::{ProgressInfo, UpdateManager};

use crate::handles::{
    ReleaseEntryFfi, SurgeContextHandle, SurgeErrorFfi, SurgeErrorOwned, SurgePackContextHandle,
    SurgeReleasesInfoHandle, SurgeUpdateManagerHandle,
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
type SurgeProgressCallback = Option<extern "C" fn(*const SurgeProgressFfi, *mut c_void)>;

/// Callback type matching `surge_event_callback` in surge_api.h.
type SurgeEventCallback = Option<extern "C" fn(*const c_char, *mut c_void)>;

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
    cb: extern "C" fn(*const SurgeProgressFfi, *mut c_void),
    user_data: usize,
}

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
        (self.cb)(&ffi, self.user_data as *mut c_void);
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

fn to_lossy_cstring(value: &str) -> CString {
    let mut bytes = value.as_bytes().to_vec();
    bytes.retain(|b| *b != 0);
    CString::new(bytes).unwrap_or_default()
}

/// Convert a nullable C string pointer to an owned UTF-8 string.
///
/// # Safety
///
/// `p` must be null or point to a valid NUL-terminated C string.
unsafe fn cstr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        // SAFETY: Caller guarantees `p` is either null (handled above) or
        // a valid NUL-terminated C string.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
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
fn set_ctx_error(handle: *const SurgeContextHandle, e: &surge_core::error::SurgeError) -> i32 {
    if handle.is_null() {
        SURGE_ERROR
    } else {
        // SAFETY: `handle` is checked non-null above and is an FFI-owned
        // `SurgeContextHandle` pointer managed by this API.
        let h = unsafe { &*handle };
        let code = e.error_code() as i32;
        h.set_last_error(code, &e.to_string());
        h.ctx.set_error(e);
        code
    }
}

fn set_shared_error(
    ctx: &Arc<Context>,
    last_error: &Arc<Mutex<Option<SurgeErrorOwned>>>,
    e: &surge_core::error::SurgeError,
) -> i32 {
    let code = e.error_code() as i32;
    let mut slot = lock_recover(last_error.as_ref());
    *slot = Some(SurgeErrorOwned::new(code, &e.to_string()));
    ctx.set_error(e);
    code
}

fn clear_shared_error(ctx: &Arc<Context>, last_error: &Arc<Mutex<Option<SurgeErrorOwned>>>) {
    let mut slot = lock_recover(last_error.as_ref());
    *slot = None;
    ctx.clear_error();
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn try_len(size: i64) -> Option<usize> {
    usize::try_from(size).ok().filter(|len| *len > 0)
}

fn try_len_allow_zero(size: i64) -> Option<usize> {
    usize::try_from(size).ok()
}

fn try_index(index: i32, len: usize) -> Option<usize> {
    let idx = usize::try_from(index).ok()?;
    if idx < len { Some(idx) } else { None }
}

/// # Safety
///
/// `argv` must point to at least `argc` elements when `argc > 0`, and each
/// non-null element must be a valid NUL-terminated C string.
unsafe fn collect_argv(argc: c_int, argv: *const *const c_char) -> Vec<String> {
    let Ok(count) = usize::try_from(argc) else {
        return Vec::new();
    };
    if count == 0 || argv.is_null() {
        return Vec::new();
    }

    let mut args = Vec::with_capacity(count);
    for i in 0..count {
        // SAFETY: Caller guarantees `argv` has `count` elements.
        let arg_ptr = unsafe { *argv.add(i) };
        if arg_ptr.is_null() {
            continue;
        }
        args.push(unsafe { cstr_to_string(arg_ptr) });
    }
    args
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
            Ok(rt) => Arc::new(rt),
            Err(_) => return ptr::null_mut(),
        };

        let ctx = Arc::new(Context::new());
        let handle = Box::new(SurgeContextHandle {
            ctx,
            runtime,
            last_error: std::sync::Mutex::new(None),
            shared_last_error: Arc::new(std::sync::Mutex::new(None)),
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
    handle.get_last_error()
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
        handle.clear_last_error();

        let prov = if let Some(p) = StorageProvider::from_i32(provider) {
            p
        } else {
            let e = surge_core::error::SurgeError::Config(format!("Invalid storage provider: {provider}"));
            return set_ctx_error(ctx, &e);
        };

        let bucket_s = unsafe { cstr_to_string(bucket) };
        let region_s = unsafe { cstr_to_string(region) };
        let access_s = unsafe { cstr_to_string(access_key) };
        let secret_s = unsafe { cstr_to_string(secret_key) };
        let endpoint_s = unsafe { cstr_to_string(endpoint) };

        handle
            .ctx
            .set_storage(prov, &bucket_s, &region_s, &access_s, &secret_s, &endpoint_s);
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
        handle.clear_last_error();

        let url_s = unsafe { cstr_to_string(url) };
        handle.ctx.set_lock_server(&url_s);
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
        handle.clear_last_error();

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
        handle.clear_last_error();

        let app_id_s = unsafe { cstr_to_string(app_id) };
        let version_s = unsafe { cstr_to_string(current_version) };
        let channel_s = unsafe { cstr_to_string(channel) };
        let install_s = unsafe { cstr_to_string(install_dir) };

        if app_id_s.is_empty() || version_s.is_empty() || channel_s.is_empty() || install_s.is_empty() {
            let e =
                surge_core::error::SurgeError::Config("app_id, version, channel, and install_dir are required".into());
            set_ctx_error(ctx, &e);
            return ptr::null_mut();
        }

        let mgr = Box::new(SurgeUpdateManagerHandle {
            ctx: handle.ctx.clone(),
            runtime: handle.runtime.clone(),
            last_error: handle.shared_last_error.clone(),
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

/// Change the update channel used by this manager.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_set_channel(
    mgr: *mut SurgeUpdateManagerHandle,
    channel: *const c_char,
) -> i32 {
    if mgr.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let mgr_ref = unsafe { &mut *mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        let channel_s = unsafe { cstr_to_string(channel) };
        let channel_s = channel_s.trim().to_string();
        if channel_s.is_empty() {
            let e = surge_core::error::SurgeError::Config("channel is required".into());
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

        mgr_ref.channel = channel_s;
        SURGE_OK
    }))
}

/// Change the current installed version baseline used for update checks.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_set_current_version(
    mgr: *mut SurgeUpdateManagerHandle,
    current_version: *const c_char,
) -> i32 {
    if mgr.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        let mgr_ref = unsafe { &mut *mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        let version_s = unsafe { cstr_to_string(current_version) };
        let version_s = version_s.trim().to_string();
        if version_s.is_empty() {
            let e = surge_core::error::SurgeError::Config("current_version is required".into());
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

        mgr_ref.current_version = version_s;
        SURGE_OK
    }))
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
        unsafe { *info = ptr::null_mut() };
        let mgr_ref = unsafe { &*mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        if mgr_ref.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let mut update_mgr = match UpdateManager::new(
            mgr_ref.ctx.clone(),
            &mgr_ref.app_id,
            &mgr_ref.current_version,
            &mgr_ref.channel,
            &mgr_ref.install_dir,
        ) {
            Ok(m) => m,
            Err(e) => return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e),
        };

        let result = mgr_ref.runtime.block_on(update_mgr.check_for_updates());

        match result {
            Ok(Some(update_info)) => {
                let ffi_releases: Vec<ReleaseEntryFfi> = update_info
                    .available_releases
                    .iter()
                    .map(|r| ReleaseEntryFfi {
                        version: r.version.clone(),
                        channel: mgr_ref.channel.clone(),
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
            Err(e) => set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e),
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
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        if mgr_ref.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let info_ref = unsafe { &*info };

        let update_info = match info_ref.update_info.as_ref() {
            Some(ui) => ui,
            None => {
                let e = surge_core::error::SurgeError::Update("No update info available".into());
                return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
            }
        };

        if update_info.available_releases.is_empty() {
            let e = surge_core::error::SurgeError::Update("No releases to apply".into());
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

        let update_mgr = match UpdateManager::new(
            mgr_ref.ctx.clone(),
            &mgr_ref.app_id,
            &mgr_ref.current_version,
            &mgr_ref.channel,
            &mgr_ref.install_dir,
        ) {
            Ok(m) => m,
            Err(e) => return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e),
        };

        // Map core ProgressInfo → SurgeProgressFfi via the C callback.
        // Handle Some/None explicitly to give the compiler a concrete type
        // for the generic F in download_and_apply<F: Send + Sync>.
        let result = if let Some(cb) = progress_cb {
            let bridge = ProgressBridge {
                cb,
                user_data: user_data as usize,
            };
            let progress_fn = move |pi: ProgressInfo| {
                bridge.invoke(&pi);
            };
            mgr_ref
                .runtime
                .block_on(update_mgr.download_and_apply(update_info, Some(progress_fn)))
        } else {
            mgr_ref
                .runtime
                .block_on(update_mgr.download_and_apply(update_info, None::<fn(ProgressInfo)>))
        };

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e),
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
        i32::try_from(h.releases.len()).unwrap_or(i32::MAX)
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
        let Some(idx) = try_index(index, h.cached_strings.len()) else {
            return ptr::null();
        };
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
        let Some(idx) = try_index(index, h.cached_strings.len()) else {
            return ptr::null();
        };
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
        let Some(idx) = try_index(index, h.releases.len()) else {
            return 0;
        };
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
        let Some(idx) = try_index(index, h.releases.len()) else {
            return 0;
        };
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

        let Some(older_size) = try_len(c.older_size) else {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        };
        let Some(newer_size) = try_len(c.newer_size) else {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        };
        if c.older.is_null() || c.newer.is_null() {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        }

        let older = unsafe { std::slice::from_raw_parts(c.older, older_size) };
        let newer = unsafe { std::slice::from_raw_parts(c.newer, newer_size) };

        match bsdiff_buffers(older, newer) {
            Ok(patch) => {
                let len = patch.len();
                let boxed = patch.into_boxed_slice();
                let ptr = Box::into_raw(boxed).cast::<u8>();
                c.patch = ptr;
                let Some(patch_size) = i64::try_from(len).ok() else {
                    c.patch = ptr::null_mut();
                    c.patch_size = 0;
                    c.status = SURGE_ERROR;
                    return SURGE_ERROR;
                };
                c.patch_size = patch_size;
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

        let Some(older_size) = try_len(c.older_size) else {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        };
        let Some(patch_size) = try_len(c.patch_size) else {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        };
        if c.older.is_null() || c.patch.is_null() {
            c.status = SURGE_ERROR;
            return SURGE_ERROR;
        }

        let older = unsafe { std::slice::from_raw_parts(c.older, older_size) };
        let patch = unsafe { std::slice::from_raw_parts(c.patch, patch_size) };

        match bspatch_buffers(older, patch) {
            Ok(newer) => {
                let len = newer.len();
                let boxed = newer.into_boxed_slice();
                let ptr = Box::into_raw(boxed).cast::<u8>();
                c.newer = ptr;
                let Some(newer_size) = i64::try_from(len).ok() else {
                    c.newer = ptr::null_mut();
                    c.newer_size = 0;
                    c.status = SURGE_ERROR;
                    return SURGE_ERROR;
                };
                c.newer_size = newer_size;
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
        if !c.patch.is_null() {
            let Some(patch_size) = try_len_allow_zero(c.patch_size) else {
                c.patch = ptr::null_mut();
                c.patch_size = 0;
                return;
            };
            // Reconstruct the boxed slice and drop it.
            let slice_ptr = core::ptr::slice_from_raw_parts_mut(c.patch, patch_size);
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
        if !c.newer.is_null() {
            let Some(newer_size) = try_len_allow_zero(c.newer_size) else {
                c.newer = ptr::null_mut();
                c.newer_size = 0;
                return;
            };
            let slice_ptr = core::ptr::slice_from_raw_parts_mut(c.newer, newer_size);
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
        handle.clear_last_error();

        let manifest_s = unsafe { cstr_to_string(manifest_path) };
        let app_id_s = unsafe { cstr_to_string(app_id) };
        let rid_s = unsafe { cstr_to_string(rid) };
        let version_s = unsafe { cstr_to_string(version) };
        let artifacts_s = unsafe { cstr_to_string(artifacts_dir) };

        if manifest_s.is_empty() || app_id_s.is_empty() || version_s.is_empty() || artifacts_s.is_empty() {
            let e = surge_core::error::SurgeError::Config(
                "manifest_path, app_id, version, and artifacts_dir are required".into(),
            );
            set_ctx_error(ctx, &e);
            return ptr::null_mut();
        }

        let pack = Box::new(SurgePackContextHandle {
            ctx: handle.ctx.clone(),
            runtime: handle.runtime.clone(),
            last_error: handle.shared_last_error.clone(),
            manifest_path: manifest_s,
            app_id: app_id_s,
            rid: rid_s,
            version: version_s,
            artifacts_dir: artifacts_s,
            builder: std::sync::Mutex::new(None),
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
        clear_shared_error(&pack.ctx, &pack.last_error);

        if pack.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let mut builder = match PackBuilder::new(
            pack.ctx.clone(),
            &pack.manifest_path,
            &pack.app_id,
            &pack.rid,
            &pack.version,
            &pack.artifacts_dir,
        ) {
            Ok(b) => b,
            Err(e) => return set_shared_error(&pack.ctx, &pack.last_error, &e),
        };

        let progress_fn = progress_cb.map(|cb| {
            move |done: i32, total: i32| {
                let ffi = make_pack_progress(SURGE_PHASE_CHECK, done, total);
                cb(&ffi, user_data);
            }
        });

        let result = pack
            .runtime
            .block_on(builder.build(progress_fn.as_ref().map(|f| f as &dyn Fn(i32, i32))));

        match result {
            Ok(()) => {
                // Store the builder so surge_pack_push can use it.
                let mut slot = lock_recover(&pack.builder);
                *slot = Some(builder);
                SURGE_OK
            }
            Err(e) => set_shared_error(&pack.ctx, &pack.last_error, &e),
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
        clear_shared_error(&pack.ctx, &pack.last_error);

        if pack.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let channel_s = unsafe { cstr_to_string(channel) };

        // Take the builder that was stored by surge_pack_build.
        let builder = {
            let mut slot = lock_recover(&pack.builder);
            slot.take()
        };

        let builder = match builder {
            Some(b) => b,
            None => {
                let e =
                    surge_core::error::SurgeError::Pack("No builder available. Call surge_pack_build first.".into());
                return set_shared_error(&pack.ctx, &pack.last_error, &e);
            }
        };

        let progress_fn = progress_cb.map(|cb| {
            move |done: i32, total: i32| {
                let ffi = make_pack_progress(SURGE_PHASE_DOWNLOAD, done, total);
                cb(&ffi, user_data);
            }
        });

        let result = pack
            .runtime
            .block_on(builder.push(&channel_s, progress_fn.as_ref().map(|f| f as &dyn Fn(i32, i32))));

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => set_shared_error(&pack.ctx, &pack.last_error, &e),
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
        unsafe { *challenge_out = ptr::null_mut() };
        let handle = unsafe { &*ctx };
        handle.clear_last_error();

        if handle.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        let name_s = unsafe { cstr_to_string(name) };

        let mut mutex = DistributedMutex::new(handle.ctx.clone(), &name_s);
        let result = handle.runtime.block_on(mutex.try_acquire(timeout_seconds));

        match result {
            Ok(true) => {
                let token = mutex.challenge().unwrap_or("");
                let c_challenge = to_lossy_cstring(token);
                let len = c_challenge.as_bytes_with_nul().len();
                let buf = unsafe { libc_malloc(len) }.cast::<c_char>();
                if buf.is_null() {
                    let e = surge_core::error::SurgeError::Other("malloc failed".into());
                    return set_ctx_error(ctx, &e);
                }
                unsafe {
                    ptr::copy_nonoverlapping(c_challenge.as_ptr(), buf, len);
                    *challenge_out = buf;
                }
                SURGE_OK
            }
            Ok(false) => {
                let e = surge_core::error::SurgeError::Lock("Lock is held by another process".into());
                set_ctx_error(ctx, &e)
            }
            Err(e) => set_ctx_error(ctx, &e),
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
        handle.clear_last_error();

        let name_s = unsafe { cstr_to_string(name) };
        let challenge_s = unsafe { cstr_to_string(challenge) };

        let mut mutex = DistributedMutex::new(handle.ctx.clone(), &name_s);
        mutex.set_challenge(challenge_s);

        let result = handle.runtime.block_on(mutex.try_release());

        match result {
            Ok(()) => SURGE_OK,
            Err(e) => set_ctx_error(ctx, &e),
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

        let exe_s = unsafe { cstr_to_string(exe_path) };
        let wd_s = unsafe { cstr_to_string(working_dir) };
        let sup_id = unsafe { cstr_to_string(supervisor_id) };

        // Collect argv into a Vec<&str>.
        let args_owned = unsafe { collect_argv(argc, argv) };
        let args: Vec<&str> = args_owned.iter().map(String::as_str).collect();

        let mut supervisor = Supervisor::new(&sup_id, &wd_s);

        match supervisor.start(&exe_s, &wd_s, &args) {
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
        static ZERO_VERSION: &[u8] = b"0.0.0\0";

        // Collect argv for inspection.
        let args = unsafe { collect_argv(argc, argv) };

        // TODO: When surge_core::platform lifecycle detection is implemented,
        // inspect command-line flags / sentinel files to determine which
        // event to fire.  For now, look for surge-specific CLI flags.
        for arg in &args {
            match arg.as_str() {
                "--surge-first-run" => {
                    if let Some(cb) = on_first_run
                        && let Ok(version) = CStr::from_bytes_with_nul(ZERO_VERSION)
                    {
                        cb(version.as_ptr(), user_data);
                    }
                }
                "--surge-installed" => {
                    if let Some(cb) = on_installed
                        && let Ok(version) = CStr::from_bytes_with_nul(ZERO_VERSION)
                    {
                        cb(version.as_ptr(), user_data);
                    }
                }
                _ => {
                    if arg.starts_with("--surge-updated=")
                        && let Some(cb) = on_updated
                    {
                        let ver = &arg["--surge-updated=".len()..];
                        let version = to_lossy_cstring(ver);
                        cb(version.as_ptr(), user_data);
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let argv = [arg0.as_ptr(), std::ptr::null(), arg1.as_ptr()];

        let args = unsafe { collect_argv(argv.len() as c_int, argv.as_ptr()) };
        assert_eq!(args, vec!["--surge-first-run", "--surge-updated=1.2.3"]);
    }

    #[test]
    fn manager_set_channel_updates_context_last_error() {
        let ctx = unsafe { surge_context_create() };
        assert!(!ctx.is_null());

        let app_id = CString::new("demo").unwrap();
        let version = CString::new("1.0.0").unwrap();
        let channel = CString::new("stable").unwrap();
        let install_dir = CString::new("/tmp/demo").unwrap();

        let mgr = unsafe {
            surge_update_manager_create(
                ctx,
                app_id.as_ptr(),
                version.as_ptr(),
                channel.as_ptr(),
                install_dir.as_ptr(),
            )
        };
        assert!(!mgr.is_null());

        let empty = CString::new("").unwrap();
        let rc = unsafe { surge_update_manager_set_channel(mgr, empty.as_ptr()) };
        assert_ne!(rc, SURGE_OK);

        let last = unsafe { surge_context_last_error(ctx) };
        assert!(!last.is_null());
        let msg = unsafe { CStr::from_ptr((*last).message) }
            .to_str()
            .unwrap_or_default()
            .to_string();
        assert!(msg.contains("channel"));

        unsafe {
            surge_update_manager_destroy(mgr);
            surge_context_destroy(ctx);
        }
    }

    #[test]
    fn manager_remains_usable_after_context_destroy() {
        let ctx = unsafe { surge_context_create() };
        assert!(!ctx.is_null());

        let app_id = CString::new("demo").unwrap();
        let version = CString::new("1.0.0").unwrap();
        let channel = CString::new("stable").unwrap();
        let install_dir = CString::new("/tmp/demo").unwrap();

        let mgr = unsafe {
            surge_update_manager_create(
                ctx,
                app_id.as_ptr(),
                version.as_ptr(),
                channel.as_ptr(),
                install_dir.as_ptr(),
            )
        };
        assert!(!mgr.is_null());

        unsafe { surge_context_destroy(ctx) };

        let test_channel = CString::new("test").unwrap();
        let rc = unsafe { surge_update_manager_set_channel(mgr, test_channel.as_ptr()) };
        assert_eq!(rc, SURGE_OK);

        unsafe { surge_update_manager_destroy(mgr) };
    }

    #[test]
    fn update_check_clears_output_pointer_on_failure() {
        let ctx = unsafe { surge_context_create() };
        assert!(!ctx.is_null());

        let app_id = CString::new("demo").unwrap();
        let version = CString::new("1.0.0").unwrap();
        let channel = CString::new("stable").unwrap();
        let install_dir = CString::new("/tmp/demo").unwrap();

        let mgr = unsafe {
            surge_update_manager_create(
                ctx,
                app_id.as_ptr(),
                version.as_ptr(),
                channel.as_ptr(),
                install_dir.as_ptr(),
            )
        };
        assert!(!mgr.is_null());

        let stale = Box::new(SurgeReleasesInfoHandle {
            releases: Vec::new(),
            cached_strings: Vec::new(),
            update_info: None,
        });
        let mut info_ptr = Box::into_raw(stale);
        assert!(!info_ptr.is_null());

        let rc = unsafe { surge_update_check(mgr, &mut info_ptr) };
        assert_ne!(rc, SURGE_OK);
        assert!(info_ptr.is_null());

        unsafe {
            surge_update_manager_destroy(mgr);
            surge_context_destroy(ctx);
        }
    }

    #[test]
    fn bspatch_free_releases_zero_length_buffer() {
        let empty: Box<[u8]> = Vec::new().into_boxed_slice();
        let ptr = Box::into_raw(empty).cast::<u8>();
        let mut ctx = SurgeBspatchCtxFfi {
            older: std::ptr::null(),
            older_size: 0,
            newer: ptr,
            newer_size: 0,
            patch: std::ptr::null(),
            patch_size: 0,
            status: 0,
        };

        unsafe { surge_bspatch_free(&mut ctx) };
        assert!(ctx.newer.is_null());
        assert_eq!(ctx.newer_size, 0);
    }

    #[test]
    fn bsdiff_free_releases_zero_length_buffer() {
        let empty: Box<[u8]> = Vec::new().into_boxed_slice();
        let ptr = Box::into_raw(empty).cast::<u8>();
        let mut ctx = SurgeBsdiffCtxFfi {
            older: std::ptr::null(),
            older_size: 0,
            newer: std::ptr::null(),
            newer_size: 0,
            patch: ptr,
            patch_size: 0,
            status: 0,
        };

        unsafe { surge_bsdiff_free(&mut ctx) };
        assert!(ctx.patch.is_null());
        assert_eq!(ctx.patch_size, 0);
    }

    #[test]
    fn pack_push_without_build_sets_context_error() {
        let ctx = unsafe { surge_context_create() };
        assert!(!ctx.is_null());

        let manifest = CString::new("placeholder.yml").unwrap();
        let app_id = CString::new("demo").unwrap();
        let rid = CString::new("linux-x64").unwrap();
        let version = CString::new("1.0.0").unwrap();
        let artifacts = CString::new("artifacts").unwrap();

        let pack = unsafe {
            surge_pack_create(
                ctx,
                manifest.as_ptr(),
                app_id.as_ptr(),
                rid.as_ptr(),
                version.as_ptr(),
                artifacts.as_ptr(),
            )
        };
        assert!(!pack.is_null());

        let channel = CString::new("stable").unwrap();
        let rc = unsafe { surge_pack_push(pack, channel.as_ptr(), None, std::ptr::null_mut()) };
        assert_ne!(rc, SURGE_OK);

        let last = unsafe { surge_context_last_error(ctx) };
        assert!(!last.is_null());
        let msg = unsafe { CStr::from_ptr((*last).message) }
            .to_str()
            .unwrap_or_default()
            .to_string();
        assert!(msg.contains("Call surge_pack_build first"));

        unsafe {
            surge_pack_destroy(pack);
            surge_context_destroy(ctx);
        }
    }

    #[test]
    fn pack_handle_survives_context_destroy() {
        let ctx = unsafe { surge_context_create() };
        assert!(!ctx.is_null());

        let manifest = CString::new("placeholder.yml").unwrap();
        let app_id = CString::new("demo").unwrap();
        let rid = CString::new("linux-x64").unwrap();
        let version = CString::new("1.0.0").unwrap();
        let artifacts = CString::new("artifacts").unwrap();

        let pack = unsafe {
            surge_pack_create(
                ctx,
                manifest.as_ptr(),
                app_id.as_ptr(),
                rid.as_ptr(),
                version.as_ptr(),
                artifacts.as_ptr(),
            )
        };
        assert!(!pack.is_null());

        unsafe { surge_context_destroy(ctx) };

        let channel = CString::new("stable").unwrap();
        let rc = unsafe { surge_pack_push(pack, channel.as_ptr(), None, std::ptr::null_mut()) };
        assert_ne!(rc, SURGE_OK);

        unsafe { surge_pack_destroy(pack) };
    }
}
