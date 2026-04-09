use std::ffi::c_char;
use std::ptr;

use crate::handles::SurgeReleasesInfoHandle;
use crate::shared::try_index;

/// Return the number of releases in `info`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn surge_releases_count(info: *const SurgeReleasesInfoHandle) -> i32 {
    if info.is_null() {
        return 0;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: `info` is checked non-null above and must remain valid for this call.
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
            // SAFETY: `info` is owned by the caller and must be reclaimed exactly once.
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
        // SAFETY: `info` is checked non-null above and must remain valid for this call.
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
        // SAFETY: `info` is checked non-null above and must remain valid for this call.
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
        // SAFETY: `info` is checked non-null above and must remain valid for this call.
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
        // SAFETY: `info` is checked non-null above and must remain valid for this call.
        let h = unsafe { &*info };
        let Some(idx) = try_index(index, h.releases.len()) else {
            return 0;
        };
        i32::from(h.releases[idx].is_genesis)
    }));

    result.unwrap_or(0)
}
