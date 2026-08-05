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

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use crate::handles::{ReleaseEntryFfi, SurgeReleasesInfoHandle};

    use super::{
        surge_release_channel, surge_release_full_size, surge_release_is_genesis, surge_release_version,
        surge_releases_count, surge_releases_destroy,
    };

    fn releases_handle() -> *mut SurgeReleasesInfoHandle {
        let mut info = Box::new(SurgeReleasesInfoHandle {
            releases: vec![ReleaseEntryFfi {
                version: "1.2.3".to_string(),
                channel: "beta".to_string(),
                full_size: 42,
                is_genesis: true,
            }],
            cached_strings: Vec::new(),
            update_info: None,
        });
        info.cache_strings();
        Box::into_raw(info)
    }

    #[test]
    fn release_string_accessors_are_borrowed_for_handle_lifetime() {
        let info = releases_handle();

        let (version, channel) = unsafe {
            // SAFETY: `info` is a live test-owned handle and index zero exists.
            (surge_release_version(info, 0), surge_release_channel(info, 0))
        };
        assert!(!version.is_null());
        assert!(!channel.is_null());
        unsafe {
            // SAFETY: both non-null strings are cached in the still-live
            // `info` handle and remain NUL-terminated for its lifetime.
            assert_eq!(CStr::from_ptr(version).to_bytes(), b"1.2.3");
            assert_eq!(CStr::from_ptr(channel).to_bytes(), b"beta");
            assert_eq!(surge_releases_count(info), 1);
            assert_eq!(surge_release_full_size(info, 0), 42);
            assert_eq!(surge_release_is_genesis(info, 0), 1);
        }

        // Other accessors do not invalidate the borrowed string storage.
        unsafe {
            // SAFETY: the borrowed strings remain valid until `info` is
            // destroyed below.
            assert_eq!(CStr::from_ptr(version).to_bytes(), b"1.2.3");
            assert_eq!(CStr::from_ptr(channel).to_bytes(), b"beta");
        }

        unsafe {
            // SAFETY: `info` is a live owned handle and is destroyed once.
            surge_releases_destroy(info);
        }
    }

    #[test]
    fn release_accessors_reject_null_and_invalid_indices() {
        unsafe {
            // SAFETY: these accessors explicitly accept null handles and
            // return neutral values without dereferencing them.
            assert_eq!(surge_releases_count(ptr::null()), 0);
            assert!(surge_release_version(ptr::null(), 0).is_null());
            assert!(surge_release_channel(ptr::null(), 0).is_null());
            assert_eq!(surge_release_full_size(ptr::null(), 0), 0);
            assert_eq!(surge_release_is_genesis(ptr::null(), 0), 0);
        }

        let info = releases_handle();
        for index in [-1, 1, i32::MAX] {
            unsafe {
                // SAFETY: `info` is live; each invalid index is intentionally
                // passed to accessors that validate it before indexing.
                assert!(surge_release_version(info, index).is_null());
                assert!(surge_release_channel(info, index).is_null());
                assert_eq!(surge_release_full_size(info, index), 0);
                assert_eq!(surge_release_is_genesis(info, index), 0);
            }
        }

        unsafe {
            // SAFETY: `info` is destroyed exactly once; null destruction is an
            // explicitly supported no-op.
            surge_releases_destroy(info);
            surge_releases_destroy(ptr::null_mut());
        }
    }
}
