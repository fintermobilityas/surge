//! File-based chunked bspatch: apply a chunked patch directly against a file,
//! optionally in place, with byte progress and a streamed SHA-256.

use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::diff::wrapper;
use crate::error::{Result, SurgeError};

use super::format::{ChunkedPatchData, deserialize_patch};
use super::{ByteProgress, chunk_len_for_index, read_exact_chunk, usize_to_u64_saturating};

/// Apply a chunked binary diff patch directly against a file, writing the
/// reconstructed file to `output_path` without materializing the entire output
/// in memory.
pub fn chunked_bspatch_file(older_path: &Path, patch: &[u8], output_path: &Path) -> Result<()> {
    chunked_bspatch_file_with_progress(older_path, patch, output_path, None)
}

/// Apply a chunked binary diff patch directly against a file and report output
/// bytes as chunks are reconstructed.
pub fn chunked_bspatch_file_with_progress(
    older_path: &Path,
    patch: &[u8],
    output_path: &Path,
    progress: Option<&ByteProgress<'_>>,
) -> Result<()> {
    bspatch_file(older_path, patch, output_path, progress, None, false)?;
    Ok(())
}

/// Like `chunked_bspatch_file_with_progress`, but also returns the SHA-256
/// (hex) of the reconstructed file, computed while writing so the caller can
/// verify the result without re-reading it.
pub fn chunked_bspatch_file_with_progress_and_sha256(
    older_path: &Path,
    patch: &[u8],
    output_path: &Path,
    progress: Option<&ByteProgress<'_>>,
) -> Result<String> {
    let mut hasher = Sha256::new();
    bspatch_file(older_path, patch, output_path, progress, Some(&mut hasher), false)?;
    Ok(hex::encode(hasher.finalize()))
}

/// Outcome of a bspatch that may patch the target file in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedBspatchResult {
    /// SHA-256 (hex) of the reconstructed file.
    pub target_hash: String,
    /// `true` when `older_path` was patched in place and `output_path` was
    /// left untouched; `false` when the reconstructed file was written to
    /// `output_path` and the caller must move it into place.
    pub applied_in_place: bool,
}

/// Like `chunked_bspatch_file_with_progress_and_sha256`, but first attempts
/// an in-place patch: when the patch uses the format version 2 identity
/// bitset and the reconstructed file has the same size as the source, the
/// unchanged chunks are left untouched and only changed chunks are rewritten
/// at their existing offsets (chunk boundaries align 1:1 at equal sizes).
/// The target hash is then computed with one full read of the patched file.
/// When in-place is not possible the standard write-to-`output_path` flow
/// runs instead.
pub fn chunked_bspatch_file_with_progress_and_sha256_in_place(
    target_path: &Path,
    patch: &[u8],
    output_path: &Path,
    progress: Option<&ByteProgress<'_>>,
) -> Result<ChunkedBspatchResult> {
    let mut hasher = Sha256::new();
    let applied_in_place = bspatch_file(target_path, patch, output_path, progress, Some(&mut hasher), true)?;
    Ok(ChunkedBspatchResult {
        target_hash: hex::encode(hasher.finalize()),
        applied_in_place,
    })
}

fn bspatch_file(
    older_path: &Path,
    patch: &[u8],
    output_path: &Path,
    progress: Option<&ByteProgress<'_>>,
    mut hasher: Option<&mut Sha256>,
    allow_in_place: bool,
) -> Result<bool> {
    let decoded = deserialize_patch(patch)?;
    let ChunkedPatchData {
        old_size,
        new_size,
        chunk_size,
        chunks: chunk_patches,
        identity,
    } = decoded;
    let actual_old_size = usize::try_from(fs::metadata(older_path)?.len())
        .map_err(|_| SurgeError::Diff("old file exceeds platform limits".into()))?;
    if actual_old_size != old_size {
        return Err(SurgeError::Diff(format!(
            "old file size mismatch: expected {old_size}, got {actual_old_size}"
        )));
    }

    // Same-size format v2 patches rewrite only the changed chunks in place,
    // skipping the full read + write of the unchanged 64 MiB chunks.
    if allow_in_place && old_size == new_size && identity.iter().any(|marked| *marked) {
        bspatch_file_in_place(
            older_path,
            old_size,
            new_size,
            chunk_size,
            &chunk_patches,
            &identity,
            progress,
        )?;
        if let Some(h) = hasher.as_mut() {
            hash_entire_file_into(older_path, h)?;
        }
        return Ok(true);
    }

    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut old_file = fs::File::open(older_path)?;
    let mut output = fs::File::create(output_path)?;
    let mut bytes_written = 0usize;

    for (idx, chunk_patch) in chunk_patches.iter().enumerate() {
        let old_chunk_len = chunk_len_for_index(old_size, idx, chunk_size);
        let old_chunk = read_exact_chunk(&mut old_file, old_chunk_len)?;
        let new_chunk = if identity[idx] {
            if old_chunk.is_empty() {
                return Err(SurgeError::Diff("identity chunk has no old content".into()));
            }
            old_chunk
        } else if old_chunk.is_empty() {
            (*chunk_patch).to_vec()
        } else if chunk_patch.is_empty() {
            Vec::new()
        } else {
            wrapper::bspatch_buffers(&old_chunk, chunk_patch)?
        };
        output.write_all(&new_chunk)?;
        if let Some(h) = hasher.as_mut() {
            h.update(&new_chunk);
        }
        bytes_written = bytes_written.saturating_add(new_chunk.len());
        if let Some(cb) = progress {
            cb(
                usize_to_u64_saturating(bytes_written),
                usize_to_u64_saturating(new_size),
            );
        }
    }
    output.flush()?;

    if bytes_written != new_size {
        return Err(SurgeError::Diff(format!(
            "reconstructed size mismatch: expected {new_size}, got {bytes_written}"
        )));
    }

    Ok(false)
}

fn bspatch_file_in_place(
    path: &Path,
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: &[&[u8]],
    identity: &[bool],
    progress: Option<&ByteProgress<'_>>,
) -> Result<()> {
    let mut file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    let mut offset = 0usize;
    for (idx, chunk_patch) in chunks.iter().enumerate() {
        let chunk_len = chunk_len_for_index(old_size, idx, chunk_size);
        let new_chunk = if identity[idx] {
            None
        } else if chunk_len == 0 {
            if !chunk_patch.is_empty() {
                return Err(SurgeError::Diff(
                    "in-place patch carries a payload for an empty chunk".into(),
                ));
            }
            None
        } else {
            file.seek(SeekFrom::Start(usize_to_u64_saturating(offset)))?;
            let mut old_chunk = vec![0u8; chunk_len];
            file.read_exact(&mut old_chunk)?;
            Some(if chunk_patch.is_empty() {
                Vec::new()
            } else {
                wrapper::bspatch_buffers(&old_chunk, chunk_patch)?
            })
        };
        if let Some(bytes) = &new_chunk {
            file.seek(SeekFrom::Start(usize_to_u64_saturating(offset)))?;
            file.write_all(bytes)?;
        }
        offset = offset.saturating_add(chunk_len);
        if let Some(cb) = progress {
            cb(usize_to_u64_saturating(offset), usize_to_u64_saturating(new_size));
        }
    }
    file.flush()?;
    Ok(())
}

fn hash_entire_file_into(path: &Path, hasher: &mut Sha256) -> Result<()> {
    let mut file = fs::File::open(path)?;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(())
}
