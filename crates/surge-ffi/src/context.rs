use std::ffi::c_char;
use std::ptr;
use std::sync::Arc;

use surge_core::context::{Context, ResourceBudget, StorageProvider};

use crate::SurgeResourceBudgetFfi;
use crate::handles::SurgeContextHandle;
use crate::shared::{SURGE_ERROR, SURGE_OK, catch_ffi, cstr_to_string, ffi_trace, set_ctx_error};

/// Create a new Surge context.
///
/// Returns a new context handle, or null on allocation/runtime failure.
#[unsafe(no_mangle)]
pub extern "C" fn surge_context_create() -> *mut SurgeContextHandle {
    ffi_trace("surge_context_create: enter");
    let result = std::panic::catch_unwind(|| {
        ffi_trace("surge_context_create: creating tokio runtime");
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(rt) => {
                ffi_trace("surge_context_create: tokio runtime ready");
                Arc::new(rt)
            }
            Err(_) => {
                ffi_trace("surge_context_create: tokio runtime failed");
                return ptr::null_mut();
            }
        };

        ffi_trace("surge_context_create: creating context handle");
        let ctx = Arc::new(Context::new());
        let handle = Box::new(SurgeContextHandle {
            ctx,
            runtime,
            last_error: std::sync::Mutex::new(None),
            shared_last_error: Arc::new(std::sync::Mutex::new(None)),
        });

        ffi_trace("surge_context_create: success");
        Box::into_raw(handle)
    });

    result.unwrap_or_else(|_| {
        ffi_trace("surge_context_create: panic");
        ptr::null_mut()
    })
}

/// Destroy a Surge context and release all associated resources.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_context_destroy(ctx: *mut SurgeContextHandle) {
    if !ctx.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // SAFETY: `ctx` is owned by the caller and must be reclaimed exactly once.
            drop(unsafe { Box::from_raw(ctx) });
        }));
    }
}

/// Retrieve the last error that occurred on `ctx`.
///
/// Returns a pointer to an internal `surge_error` struct, or null if no error.
/// The pointer is valid until the next API call on the same context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_context_last_error(
    ctx: *const SurgeContextHandle,
) -> *const crate::handles::SurgeErrorFfi {
    if ctx.is_null() {
        return ptr::null();
    }
    // SAFETY: `ctx` is checked non-null above and must remain valid for this call.
    let handle = unsafe { &*ctx };
    handle.get_last_error()
}

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
        // SAFETY: `ctx` is checked for non-null above and owned by this FFI
        // layer for the duration of this call.
        let handle = unsafe { &*ctx };
        handle.clear_last_error();

        let prov = if let Some(p) = StorageProvider::from_i32(provider) {
            p
        } else {
            let e = surge_core::error::SurgeError::Config(format!("Invalid storage provider: {provider}"));
            return set_ctx_error(handle, &e);
        };

        // SAFETY: C string pointers are nullable by API contract and each
        // non-null pointer is expected to reference a valid NUL-terminated
        // string for the duration of this call.
        let (bucket_s, region_s, access_s, secret_s, endpoint_s) = unsafe {
            (
                cstr_to_string(bucket),
                cstr_to_string(region),
                cstr_to_string(access_key),
                cstr_to_string(secret_key),
                cstr_to_string(endpoint),
            )
        };

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
        // SAFETY: `ctx` is checked for non-null above and points to a live
        // context handle while this call executes.
        let handle = unsafe { &*ctx };
        handle.clear_last_error();

        // SAFETY: `url` follows the C API contract for nullable C strings.
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
        // SAFETY: `ctx` and `budget` are checked non-null above and both
        // pointers are expected to reference initialized values by API
        // contract.
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
