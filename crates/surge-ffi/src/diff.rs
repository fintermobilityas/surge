use std::ptr;

use surge_core::diff::wrapper::{bsdiff_buffers, bspatch_buffers};

use crate::shared::{SURGE_ERROR, SURGE_OK, catch_ffi, try_len, try_len_allow_zero};
use crate::{SurgeBsdiffCtxFfi, SurgeBspatchCtxFfi};

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
        // SAFETY: `ctx` is checked non-null above and must remain valid for this call.
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

        // SAFETY: the pointers and lengths were validated above.
        let older = unsafe { std::slice::from_raw_parts(c.older, older_size) };
        // SAFETY: the pointers and lengths were validated above.
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
        // SAFETY: `ctx` is checked non-null above and must remain valid for this call.
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

        // SAFETY: the pointers and lengths were validated above.
        let older = unsafe { std::slice::from_raw_parts(c.older, older_size) };
        // SAFETY: the pointers and lengths were validated above.
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
        // SAFETY: `ctx` is checked non-null above and must remain valid for this call.
        let c = unsafe { &mut *ctx };
        if !c.patch.is_null() {
            let Some(patch_size) = try_len_allow_zero(c.patch_size) else {
                c.patch = ptr::null_mut();
                c.patch_size = 0;
                return;
            };
            let slice_ptr = core::ptr::slice_from_raw_parts_mut(c.patch, patch_size);
            // SAFETY: `slice_ptr` was originally allocated from a boxed slice by this module.
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
        // SAFETY: `ctx` is checked non-null above and must remain valid for this call.
        let c = unsafe { &mut *ctx };
        if !c.newer.is_null() {
            let Some(newer_size) = try_len_allow_zero(c.newer_size) else {
                c.newer = ptr::null_mut();
                c.newer_size = 0;
                return;
            };
            let slice_ptr = core::ptr::slice_from_raw_parts_mut(c.newer, newer_size);
            // SAFETY: `slice_ptr` was originally allocated from a boxed slice by this module.
            drop(unsafe { Box::from_raw(slice_ptr) });
            c.newer = ptr::null_mut();
            c.newer_size = 0;
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::{SurgeBsdiffCtxFfi, SurgeBspatchCtxFfi, surge_bsdiff_free, surge_bspatch_free};

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
}
