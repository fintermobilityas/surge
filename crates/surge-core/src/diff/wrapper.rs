//! Safe wrapper around the vendored bsdiff/bspatch C library.

use std::ffi::c_void;
use std::mem::MaybeUninit;

use crate::diff::bsdiff_sys::{
    BSDIFF_MODE_READ, BSDIFF_MODE_WRITE, BSDIFF_SUCCESS, BsdiffCtx, BsdiffPatchPacker, BsdiffStream, bsdiff,
    bsdiff_close_patch_packer, bsdiff_close_stream, bsdiff_open_bz2_patch_packer, bsdiff_open_memory_stream, bspatch,
};
use crate::error::{Result, SurgeError};

struct StreamHandle {
    inner: BsdiffStream,
    owned: bool,
}

impl StreamHandle {
    fn open(mode: i32, buffer: *const c_void, size: usize, label: &str) -> Result<Self> {
        let mut stream = MaybeUninit::<BsdiffStream>::zeroed();
        // SAFETY: `stream` points to writable memory for the C initializer,
        // and `buffer/size` follow the contract of bsdiff_open_memory_stream.
        let rc = unsafe { bsdiff_open_memory_stream(mode, buffer, size, stream.as_mut_ptr()) };
        if rc != BSDIFF_SUCCESS {
            return Err(SurgeError::Diff(format!("Failed to open {label} stream: {rc}")));
        }

        Ok(Self {
            // SAFETY: Successful initializer call populated the struct.
            inner: unsafe { stream.assume_init() },
            owned: true,
        })
    }

    fn as_mut(&mut self) -> &mut BsdiffStream {
        &mut self.inner
    }

    fn release(&mut self) {
        self.owned = false;
    }

    fn copy_buffer(&mut self, label: &str, allow_empty: bool) -> Result<Vec<u8>> {
        let get_buffer = self
            .inner
            .get_buffer
            .ok_or_else(|| SurgeError::Diff(format!("No get_buffer on {label} stream")))?;

        let mut buf_ptr: *const c_void = std::ptr::null();
        let mut buf_size: usize = 0;
        // SAFETY: Callback comes from initialized C stream and pointers are valid outputs.
        let rc = unsafe { get_buffer(self.inner.state, &raw mut buf_ptr, &raw mut buf_size) };
        if rc != BSDIFF_SUCCESS {
            return Err(SurgeError::Diff(format!("Failed to get {label} buffer: {rc}")));
        }

        if buf_size == 0 {
            return if allow_empty {
                Ok(Vec::new())
            } else {
                Err(SurgeError::Diff(format!("Empty {label} buffer")))
            };
        }

        if buf_ptr.is_null() {
            return Err(SurgeError::Diff(format!("Null {label} buffer pointer")));
        }

        // SAFETY: `buf_ptr`/`buf_size` were returned by `get_buffer` above.
        Ok(unsafe { std::slice::from_raw_parts(buf_ptr.cast::<u8>(), buf_size) }.to_vec())
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        if self.owned {
            // SAFETY: `inner` is an initialized bsdiff stream that this handle owns.
            unsafe { bsdiff_close_stream(&raw mut self.inner) };
        }
    }
}

struct PackerHandle {
    inner: BsdiffPatchPacker,
}

impl PackerHandle {
    fn open(mode: i32, stream: &mut StreamHandle) -> Result<Self> {
        let mut packer = MaybeUninit::<BsdiffPatchPacker>::zeroed();
        // SAFETY: `packer` points to writable memory and `stream` is initialized.
        let rc = unsafe { bsdiff_open_bz2_patch_packer(mode, stream.as_mut(), packer.as_mut_ptr()) };
        if rc != BSDIFF_SUCCESS {
            return Err(SurgeError::Diff(format!("Failed to open packer: {rc}")));
        }

        // bsdiff packer close owns/cleans the wrapped stream.
        stream.release();

        Ok(Self {
            // SAFETY: Successful initializer call populated the struct.
            inner: unsafe { packer.assume_init() },
        })
    }

    fn as_mut(&mut self) -> &mut BsdiffPatchPacker {
        &mut self.inner
    }
}

impl Drop for PackerHandle {
    fn drop(&mut self) {
        // SAFETY: `inner` is an initialized packer and this handle owns it.
        unsafe { bsdiff_close_patch_packer(&raw mut self.inner) };
    }
}

fn new_ctx() -> BsdiffCtx {
    BsdiffCtx {
        opaque: std::ptr::null_mut(),
        log_error: None,
    }
}

/// Create a binary diff patch from two buffers.
pub fn bsdiff_buffers(older: &[u8], newer: &[u8]) -> Result<Vec<u8>> {
    let mut old_stream = StreamHandle::open(BSDIFF_MODE_READ, older.as_ptr().cast(), older.len(), "old")?;
    let mut new_stream = StreamHandle::open(BSDIFF_MODE_READ, newer.as_ptr().cast(), newer.len(), "new")?;
    let mut patch_stream = StreamHandle::open(BSDIFF_MODE_WRITE, std::ptr::null(), 0, "patch")?;
    let mut packer = PackerHandle::open(BSDIFF_MODE_WRITE, &mut patch_stream)?;
    let mut ctx = new_ctx();

    // SAFETY: All pointers reference initialized C-ABI structs valid for the call duration.
    let rc = unsafe { bsdiff(&raw mut ctx, old_stream.as_mut(), new_stream.as_mut(), packer.as_mut()) };
    if rc != BSDIFF_SUCCESS {
        return Err(SurgeError::Diff(format!("bsdiff failed: {rc}")));
    }

    patch_stream.copy_buffer("patch", false)
}

/// Apply a binary diff patch to reconstruct the newer buffer.
pub fn bspatch_buffers(older: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    let mut old_stream = StreamHandle::open(BSDIFF_MODE_READ, older.as_ptr().cast(), older.len(), "old")?;
    let mut patch_stream = StreamHandle::open(BSDIFF_MODE_READ, patch.as_ptr().cast(), patch.len(), "patch")?;
    let mut packer = PackerHandle::open(BSDIFF_MODE_READ, &mut patch_stream)?;
    let mut new_stream = StreamHandle::open(BSDIFF_MODE_WRITE, std::ptr::null(), 0, "new")?;
    let mut ctx = new_ctx();

    // SAFETY: All pointers reference initialized C-ABI structs valid for the call duration.
    let rc = unsafe { bspatch(&raw mut ctx, old_stream.as_mut(), new_stream.as_mut(), packer.as_mut()) };
    if rc != BSDIFF_SUCCESS {
        return Err(SurgeError::Diff(format!("bspatch failed: {rc}")));
    }

    new_stream.copy_buffer("new", true)
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

    #[test]
    fn test_memory_stream_get_buffer() {
        let mut stream = StreamHandle::open(BSDIFF_MODE_WRITE, std::ptr::null(), 0, "test").unwrap();
        assert!(
            stream.as_mut().get_buffer.is_some(),
            "get_buffer should be set after open_memory_stream"
        );

        if let Some(write_fn) = stream.as_mut().write {
            let data = b"hello";
            // SAFETY: `write_fn` belongs to the initialized stream and points to valid bytes.
            let rc = unsafe { write_fn(stream.as_mut().state, data.as_ptr().cast(), data.len()) };
            assert_eq!(rc, BSDIFF_SUCCESS, "write failed");
        }

        let buffer = stream.copy_buffer("test", false).unwrap();
        assert_eq!(buffer, b"hello");
    }
}
