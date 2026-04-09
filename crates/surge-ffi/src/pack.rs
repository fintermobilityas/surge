use std::ffi::c_char;
use std::ffi::c_void;
use std::ptr;

use surge_core::pack::builder::PackBuilder;

use crate::handles::{SurgeContextHandle, SurgePackContextHandle};
use crate::shared::{
    SURGE_CANCELLED, SURGE_ERROR, SURGE_OK, SURGE_PHASE_CHECK, SURGE_PHASE_DOWNLOAD, SurgeProgressCallback, catch_ffi,
    clear_shared_error, cstr_to_string, make_pack_progress, set_ctx_error, set_shared_error,
};
use crate::utils::lock_recover;

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
        // SAFETY: `ctx` is checked non-null above.
        let handle = unsafe { &*ctx };
        handle.clear_last_error();

        // SAFETY: string pointers follow this API's nullable C string contract.
        let (manifest_s, app_id_s, rid_s, version_s, artifacts_s) = unsafe {
            (
                cstr_to_string(manifest_path),
                cstr_to_string(app_id),
                cstr_to_string(rid),
                cstr_to_string(version),
                cstr_to_string(artifacts_dir),
            )
        };

        if manifest_s.is_empty() || app_id_s.is_empty() || version_s.is_empty() || artifacts_s.is_empty() {
            let e = surge_core::error::SurgeError::Config(
                "manifest_path, app_id, version, and artifacts_dir are required".into(),
            );
            set_ctx_error(handle, &e);
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
        // SAFETY: `pack_ctx` is checked non-null above.
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
        // SAFETY: `pack_ctx` is checked non-null above.
        let pack = unsafe { &*pack_ctx };
        clear_shared_error(&pack.ctx, &pack.last_error);

        if pack.ctx.is_cancelled() {
            return SURGE_CANCELLED;
        }

        // SAFETY: `channel` follows the nullable C string contract.
        let channel_s = unsafe { cstr_to_string(channel) };

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
            // SAFETY: `pack_ctx` is owned by the caller and must be reclaimed exactly once.
            drop(unsafe { Box::from_raw(pack_ctx) });
        }));
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use crate::{surge_context_create, surge_context_destroy, surge_context_last_error};

    use super::{SURGE_OK, surge_pack_create, surge_pack_destroy, surge_pack_push};

    #[test]
    fn pack_push_without_build_sets_context_error() {
        let ctx = surge_context_create();
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
        let ctx = surge_context_create();
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
