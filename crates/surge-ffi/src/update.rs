use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use surge_core::config::manifest::{InstallArtifactCachePolicy, InstallArtifactCacheRetention};
use surge_core::update::manager::{ProgressInfo, UpdateManager};

use crate::handles::{ReleaseEntryFfi, SurgeReleasesInfoHandle, SurgeUpdateManagerHandle};
use crate::shared::{
    ProgressBridge, SURGE_CANCELLED, SURGE_ERROR, SURGE_NOT_FOUND, SURGE_OK, SurgeProgressCallback, catch_ffi,
    clear_shared_error, cstr_to_string, set_ctx_error, set_shared_error,
};
/// Create an update manager bound to a specific application.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_create(
    ctx: *mut crate::handles::SurgeContextHandle,
    app_id: *const c_char,
    current_version: *const c_char,
    channel: *const c_char,
    install_dir: *const c_char,
) -> *mut SurgeUpdateManagerHandle {
    if ctx.is_null() {
        return ptr::null_mut();
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `ctx` is checked non-null above and refers to a valid
        // handle for the duration of this call.
        let handle = unsafe { &*ctx };
        handle.clear_last_error();

        // SAFETY: string pointer inputs follow the FFI contract described in
        // this API; nulls map to empty strings.
        let (app_id_s, version_s, channel_s, install_s) = unsafe {
            (
                cstr_to_string(app_id),
                cstr_to_string(current_version),
                cstr_to_string(channel),
                cstr_to_string(install_dir),
            )
        };

        if app_id_s.is_empty() || version_s.is_empty() || channel_s.is_empty() || install_s.is_empty() {
            let e =
                surge_core::error::SurgeError::Config("app_id, version, channel, and install_dir are required".into());
            set_ctx_error(handle, &e);
            return ptr::null_mut();
        }

        let mgr = Box::new(SurgeUpdateManagerHandle {
            ctx: handle.ctx.clone(),
            runtime: handle.runtime.clone(),
            last_error: handle.shared_last_error.clone(),
            app_id: app_id_s,
            current_version: version_s,
            channel: channel_s,
            release_retention_limit: 1,
            artifact_retention_policy: InstallArtifactCachePolicy::default(),
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
            // SAFETY: `mgr` is owned by the caller and must be reclaimed exactly once.
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
        // SAFETY: `mgr` is checked non-null above.
        let mgr_ref = unsafe { &mut *mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        // SAFETY: `channel` follows the nullable C string contract.
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
        // SAFETY: `mgr` is checked non-null above.
        let mgr_ref = unsafe { &mut *mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        // SAFETY: `current_version` follows the nullable C string contract.
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

/// Change the number of old app versions retained after successful updates.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_set_release_retention_limit(
    mgr: *mut SurgeUpdateManagerHandle,
    release_retention_limit: c_int,
) -> i32 {
    if mgr.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `mgr` is checked non-null above.
        let mgr_ref = unsafe { &mut *mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        if release_retention_limit < 0 {
            let e = surge_core::error::SurgeError::Config("release_retention_limit cannot be negative".into());
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

        mgr_ref.release_retention_limit = usize::try_from(release_retention_limit).unwrap_or(usize::MAX);
        SURGE_OK
    }))
}

/// Change local artifact cache retention applied after successful updates.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_update_manager_set_artifact_retention_policy(
    mgr: *mut SurgeUpdateManagerHandle,
    retention: c_int,
    keep_full_count: c_int,
) -> i32 {
    if mgr.is_null() {
        return SURGE_ERROR;
    }

    catch_ffi(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `mgr` is checked non-null above.
        let mgr_ref = unsafe { &mut *mgr };
        clear_shared_error(&mgr_ref.ctx, &mgr_ref.last_error);

        let retention = match retention {
            0 => InstallArtifactCacheRetention::ReleaseGraph,
            1 => InstallArtifactCacheRetention::LatestFull,
            2 => InstallArtifactCacheRetention::JustInstalled,
            3 => InstallArtifactCacheRetention::None,
            value => {
                let e = surge_core::error::SurgeError::Config(format!(
                    "artifact_retention_policy has unsupported mode {value}"
                ));
                return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
            }
        };

        if keep_full_count <= 0 {
            let e = surge_core::error::SurgeError::Config("keep_full_count must be greater than zero".into());
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

        mgr_ref.artifact_retention_policy = InstallArtifactCachePolicy {
            retention,
            keep_full_count: u32::try_from(keep_full_count).unwrap_or(u32::MAX),
        };
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
        // SAFETY: `mgr`/`info` are checked non-null above. The out pointer is
        // cleared first to avoid leaking stale values on early exits.
        let mgr_ref = unsafe {
            *info = ptr::null_mut();
            &*mgr
        };
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
        update_mgr.set_release_retention_limit(mgr_ref.release_retention_limit);
        if let Err(e) = update_mgr.set_artifact_retention_policy(mgr_ref.artifact_retention_policy) {
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

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

                // SAFETY: `info` is a valid out pointer checked above.
                unsafe { *info = Box::into_raw(releases_handle) };
                SURGE_OK
            }
            Ok(None) => {
                let releases_handle = Box::new(SurgeReleasesInfoHandle {
                    releases: Vec::new(),
                    cached_strings: Vec::new(),
                    update_info: None,
                });
                // SAFETY: `info` is a valid out pointer checked above.
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
        // SAFETY: `mgr` and `info` are checked non-null above and must remain valid for this call.
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
        update_mgr.set_release_retention_limit(mgr_ref.release_retention_limit);
        if let Err(e) = update_mgr.set_artifact_retention_policy(mgr_ref.artifact_retention_policy) {
            return set_shared_error(&mgr_ref.ctx, &mgr_ref.last_error, &e);
        }

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

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use crate::{surge_context_create, surge_context_destroy, surge_context_last_error};

    use super::{
        SURGE_OK, SurgeReleasesInfoHandle, surge_update_check, surge_update_manager_create,
        surge_update_manager_destroy, surge_update_manager_set_artifact_retention_policy,
        surge_update_manager_set_channel, surge_update_manager_set_release_retention_limit,
    };

    #[test]
    fn manager_set_channel_updates_context_last_error() {
        let ctx = surge_context_create();
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
    fn manager_set_release_retention_limit_updates_context_last_error() {
        let ctx = surge_context_create();
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

        let rc = unsafe { surge_update_manager_set_release_retention_limit(mgr, -1) };
        assert_ne!(rc, SURGE_OK);

        let last = unsafe { surge_context_last_error(ctx) };
        assert!(!last.is_null());
        let msg = unsafe { CStr::from_ptr((*last).message) }
            .to_str()
            .unwrap_or_default()
            .to_string();
        assert!(msg.contains("release_retention_limit"));

        unsafe {
            surge_update_manager_destroy(mgr);
            surge_context_destroy(ctx);
        }
    }

    #[test]
    fn manager_set_artifact_retention_policy_validates_inputs() {
        let ctx = surge_context_create();
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

        let rc = unsafe { surge_update_manager_set_artifact_retention_policy(mgr, 1, 2) };
        assert_eq!(rc, SURGE_OK);

        let rc = unsafe { surge_update_manager_set_artifact_retention_policy(mgr, 1, 0) };
        assert_ne!(rc, SURGE_OK);

        let last = unsafe { surge_context_last_error(ctx) };
        assert!(!last.is_null());
        let msg = unsafe { CStr::from_ptr((*last).message) }
            .to_str()
            .unwrap_or_default()
            .to_string();
        assert!(msg.contains("keep_full_count"));

        let rc = unsafe { surge_update_manager_set_artifact_retention_policy(mgr, 99, 1) };
        assert_ne!(rc, SURGE_OK);

        let last = unsafe { surge_context_last_error(ctx) };
        assert!(!last.is_null());
        let msg = unsafe { CStr::from_ptr((*last).message) }
            .to_str()
            .unwrap_or_default()
            .to_string();
        assert!(msg.contains("artifact_retention_policy"));

        unsafe {
            surge_update_manager_destroy(mgr);
            surge_context_destroy(ctx);
        }
    }

    #[test]
    fn manager_remains_usable_after_context_destroy() {
        let ctx = surge_context_create();
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
        let ctx = surge_context_create();
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
}
