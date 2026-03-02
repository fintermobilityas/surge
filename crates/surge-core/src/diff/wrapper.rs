//! Safe wrapper around the vendored bsdiff/bspatch C library.

use std::ffi::c_void;
use std::mem::MaybeUninit;

use crate::diff::bsdiff_sys::*;
use crate::error::{Result, SurgeError};

/// Create a binary diff patch from two buffers.
pub fn bsdiff_buffers(older: &[u8], newer: &[u8]) -> Result<Vec<u8>> {
    unsafe {
        // Open memory streams for old and new data
        let mut old_stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
        let mut new_stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
        let mut patch_stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
        let mut packer = MaybeUninit::<BsdiffPatchPacker>::zeroed().assume_init();
        let mut ctx = BsdiffCtx {
            opaque: std::ptr::null_mut(),
            log_error: None,
        };

        let rc = bsdiff_open_memory_stream(BSDIFF_MODE_READ, older.as_ptr().cast(), older.len(), &mut old_stream);
        if rc != BSDIFF_SUCCESS {
            return Err(SurgeError::Diff(format!("Failed to open old stream: {rc}")));
        }

        let rc = bsdiff_open_memory_stream(BSDIFF_MODE_READ, newer.as_ptr().cast(), newer.len(), &mut new_stream);
        if rc != BSDIFF_SUCCESS {
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("Failed to open new stream: {rc}")));
        }

        // Patch output stream (write mode, initial capacity)
        let rc = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE, std::ptr::null(), 0, &mut patch_stream);
        if rc != BSDIFF_SUCCESS {
            bsdiff_close_stream(&mut new_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("Failed to open patch stream: {rc}")));
        }

        let rc = bsdiff_open_bz2_patch_packer(BSDIFF_MODE_WRITE, &mut patch_stream, &mut packer);
        if rc != BSDIFF_SUCCESS {
            bsdiff_close_stream(&mut patch_stream);
            bsdiff_close_stream(&mut new_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("Failed to open packer: {rc}")));
        }

        let rc = bsdiff(&mut ctx, &mut old_stream, &mut new_stream, &mut packer);

        if rc != BSDIFF_SUCCESS {
            bsdiff_close_patch_packer(&mut packer);
            // packer close already closes patch_stream
            bsdiff_close_stream(&mut new_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("bsdiff failed: {rc}")));
        }

        // bsdiff() calls flush on the packer before returning, so the stream
        // now contains the complete compressed patch data. Get the buffer
        // BEFORE closing the packer, because close_patch_packer frees the stream.
        let mut buf_ptr: *const c_void = std::ptr::null();
        let mut buf_size: usize = 0;
        if let Some(get_buf) = patch_stream.get_buffer {
            let _ = get_buf(patch_stream.state, &mut buf_ptr, &mut buf_size);
        }

        // Copy the patch data before closing frees the memory.
        let patch = if buf_ptr.is_null() || buf_size == 0 {
            bsdiff_close_patch_packer(&mut packer);
            bsdiff_close_stream(&mut new_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff("Empty patch buffer".to_string()));
        } else {
            std::slice::from_raw_parts(buf_ptr.cast::<u8>(), buf_size).to_vec()
        };

        // Now close everything. Packer close frees patch_stream.
        bsdiff_close_patch_packer(&mut packer);
        bsdiff_close_stream(&mut new_stream);
        bsdiff_close_stream(&mut old_stream);

        Ok(patch)
    }
}

/// Apply a binary diff patch to reconstruct the newer buffer.
pub fn bspatch_buffers(older: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    unsafe {
        let mut old_stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
        let mut new_stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
        let mut patch_stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
        let mut packer = MaybeUninit::<BsdiffPatchPacker>::zeroed().assume_init();
        let mut ctx = BsdiffCtx {
            opaque: std::ptr::null_mut(),
            log_error: None,
        };

        let rc = bsdiff_open_memory_stream(BSDIFF_MODE_READ, older.as_ptr().cast(), older.len(), &mut old_stream);
        if rc != BSDIFF_SUCCESS {
            return Err(SurgeError::Diff(format!("Failed to open old stream: {rc}")));
        }

        let rc = bsdiff_open_memory_stream(BSDIFF_MODE_READ, patch.as_ptr().cast(), patch.len(), &mut patch_stream);
        if rc != BSDIFF_SUCCESS {
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("Failed to open patch stream: {rc}")));
        }

        let rc = bsdiff_open_bz2_patch_packer(BSDIFF_MODE_READ, &mut patch_stream, &mut packer);
        if rc != BSDIFF_SUCCESS {
            bsdiff_close_stream(&mut patch_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("Failed to open packer: {rc}")));
        }

        // New file output stream
        let rc = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE, std::ptr::null(), 0, &mut new_stream);
        if rc != BSDIFF_SUCCESS {
            bsdiff_close_patch_packer(&mut packer);
            bsdiff_close_stream(&mut patch_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("Failed to open new stream: {rc}")));
        }

        let rc = bspatch(&mut ctx, &mut old_stream, &mut new_stream, &mut packer);

        if rc != BSDIFF_SUCCESS {
            bsdiff_close_patch_packer(&mut packer);
            // packer close already closes patch_stream
            bsdiff_close_stream(&mut new_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff(format!("bspatch failed: {rc}")));
        }

        // Get the result from new_stream BEFORE closing packer.
        // (The packer wraps patch_stream, not new_stream, but let's be safe.)
        let mut buf_ptr: *const c_void = std::ptr::null();
        let mut buf_size: usize = 0;
        if let Some(get_buffer) = new_stream.get_buffer {
            let rc = get_buffer(new_stream.state, &mut buf_ptr, &mut buf_size);
            if rc != BSDIFF_SUCCESS {
                bsdiff_close_patch_packer(&mut packer);
                bsdiff_close_stream(&mut new_stream);
                bsdiff_close_stream(&mut old_stream);
                return Err(SurgeError::Diff("Failed to get new buffer".to_string()));
            }
        } else {
            bsdiff_close_patch_packer(&mut packer);
            bsdiff_close_stream(&mut new_stream);
            bsdiff_close_stream(&mut old_stream);
            return Err(SurgeError::Diff("No get_buffer on new stream".to_string()));
        }

        let result = if buf_ptr.is_null() || buf_size == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(buf_ptr.cast::<u8>(), buf_size).to_vec()
        };

        // Now close everything. Packer closes patch_stream.
        bsdiff_close_patch_packer(&mut packer);
        bsdiff_close_stream(&mut new_stream);
        bsdiff_close_stream(&mut old_stream);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bsdiff_bspatch_roundtrip() {
        let old = b"Hello, World! This is the old version of the file.";
        let new = b"Hello, World! This is the NEW version of the file with changes.";

        let patch = bsdiff_buffers(old, new).unwrap();
        assert!(!patch.is_empty());

        let reconstructed = bspatch_buffers(old, &patch).unwrap();
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_bsdiff_identical() {
        let data = b"identical content";
        let patch = bsdiff_buffers(data, data).unwrap();
        let reconstructed = bspatch_buffers(data, &patch).unwrap();
        assert_eq!(reconstructed, data);
    }
}

#[cfg(test)]
mod debug_tests {
    use super::*;
    use std::mem::MaybeUninit;

    #[test]
    fn test_memory_stream_get_buffer() {
        unsafe {
            let mut stream = MaybeUninit::<BsdiffStream>::zeroed().assume_init();
            let rc = bsdiff_open_memory_stream(BSDIFF_MODE_WRITE, std::ptr::null(), 0, &mut stream);
            assert_eq!(rc, BSDIFF_SUCCESS, "open_memory_stream failed");
            assert!(
                stream.get_buffer.is_some(),
                "get_buffer should be set after open_memory_stream"
            );

            // Write some data
            if let Some(write_fn) = stream.write {
                let data = b"hello";
                let rc = write_fn(stream.state, data.as_ptr().cast(), data.len());
                assert_eq!(rc, BSDIFF_SUCCESS, "write failed");
            }

            // Get buffer
            let mut buf_ptr: *const std::ffi::c_void = std::ptr::null();
            let mut buf_size: usize = 0;
            let get_buf = stream.get_buffer.unwrap();
            let rc = get_buf(stream.state, &mut buf_ptr, &mut buf_size);
            assert_eq!(rc, BSDIFF_SUCCESS, "get_buffer failed");
            assert!(!buf_ptr.is_null(), "buffer should not be null");
            assert_eq!(buf_size, 5, "buffer size should be 5");

            bsdiff_close_stream(&mut stream);
        }
    }
}
