//! File-based chunked bspatch: apply a chunked patch directly against a file,
//! optionally in place, with byte progress and a streamed SHA-256.

use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::crypto::sha256::sha256_raw;
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
    let (_, _) = bspatch_file(older_path, patch, output_path, progress, None, false)?;
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
    let (_, _) = bspatch_file(older_path, patch, output_path, progress, Some(&mut hasher), false)?;
    Ok(hex::encode(hasher.finalize()))
}

/// Outcome of a bspatch that may patch the target file in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedBspatchResult {
    /// SHA-256 (hex) of the reconstructed file. `None` when the in-place
    /// verify path proved the target from the patch's per-chunk target
    /// digests instead (see `chunk_hashes_verified`); the caller then pins
    /// the file via those digests plus the verified basis hash and the
    /// chain's final full-archive check.
    pub target_hash: Option<String>,
    /// `true` when `older_path` was patched in place and `output_path` was
    /// left untouched; `false` when the reconstructed file was written to
    /// `output_path` and the caller must move it into place.
    pub applied_in_place: bool,
    /// `true` when every rewritten chunk was verified against the patch's
    /// recorded target digest (format version 3); the carried chunks are
    /// pinned by the basis hash and the file hash was not re-read.
    pub chunk_hashes_verified: bool,
}

/// Like `chunked_bspatch_file_with_progress_and_sha256`, but first
/// attempts an in-place patch: when the patch uses an identity-bitset
/// format (version 2 or 3) and the reconstructed file has the same size
/// as the source, the unchanged chunks are left untouched and only
/// changed chunks are rewritten at their existing offsets (chunk
/// boundaries align 1:1 at equal sizes).
///
/// Target verification depends on the format: for version 3, every
/// changed chunk is derived and verified against its recorded target
/// digest **before the first write**, so a stale digest fails with the
/// target untouched; `target_hash` is `None` and `chunk_hashes_verified`
/// is `true`, and the caller pins the file via the per-chunk digests plus
/// the verified basis hash. For version 2 (no digests) the target is
/// computed with one full read of the patched file and returned as
/// `target_hash`. When in-place is not possible the standard
/// write-to-`output_path` flow runs instead (always with a `Some` target
/// hash).
pub fn chunked_bspatch_file_with_progress_and_sha256_in_place(
    target_path: &Path,
    patch: &[u8],
    output_path: &Path,
    progress: Option<&ByteProgress<'_>>,
) -> Result<ChunkedBspatchResult> {
    let mut hasher = Sha256::new();
    let (applied_in_place, chunk_hashes_verified) =
        bspatch_file(target_path, patch, output_path, progress, Some(&mut hasher), true)?;
    let target_hash = if applied_in_place && chunk_hashes_verified {
        // Every rewritten chunk matched its recorded target digest in
        // memory; the full-file re-read is skipped.
        None
    } else {
        Some(hex::encode(hasher.finalize()))
    };
    Ok(ChunkedBspatchResult {
        target_hash,
        applied_in_place,
        chunk_hashes_verified,
    })
}

/// `(applied_in_place, chunk_hashes_verified)`: when the in-place path
/// verified every rewritten chunk against its recorded target digest, the
/// caller skips the full-file target re-read.
fn bspatch_file(
    older_path: &Path,
    patch: &[u8],
    output_path: &Path,
    progress: Option<&ByteProgress<'_>>,
    mut hasher: Option<&mut Sha256>,
    allow_in_place: bool,
) -> Result<(bool, bool)> {
    let decoded = deserialize_patch(patch)?;
    let ChunkedPatchData {
        old_size,
        new_size,
        chunk_size,
        chunks: chunk_patches,
        identity,
        chunk_hashes,
    } = decoded;
    let actual_old_size = usize::try_from(fs::metadata(older_path)?.len())
        .map_err(|_| SurgeError::Diff("old file exceeds platform limits".into()))?;
    if actual_old_size != old_size {
        return Err(SurgeError::Diff(format!(
            "old file size mismatch: expected {old_size}, got {actual_old_size}"
        )));
    }

    // Same-size identity-bitset patches rewrite only the changed chunks in
    // place, skipping the full read + write of the unchanged 64 MiB chunks.
    if allow_in_place && old_size == new_size && identity.iter().any(|marked| *marked) {
        let (changed_written, changed_verified) = bspatch_file_in_place(
            older_path,
            old_size,
            new_size,
            chunk_size,
            &chunk_patches,
            &identity,
            Some(chunk_hashes.as_slice()),
            progress,
        )?;
        // Format version 3 digests prove every rewritten chunk in memory;
        // the carried chunks are pinned by the verified basis hash and the
        // chain's final full-archive check, so the full-file target re-read
        // is skipped. v1/v2 patches (no digests) keep the full read.
        let digests_verified = changed_written > 0 && changed_verified == changed_written;
        if let Some(h) = hasher.as_mut().filter(|_| !digests_verified) {
            hash_entire_file_into(older_path, h)?;
        }
        return Ok((true, digests_verified));
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

    Ok((false, false))
}

/// Rewrite the changed chunks in place, in two passes: first derive and
/// digest-check **every** changed chunk (no writes), then re-derive and
/// write them. A stale digest therefore never leaves the target partially
/// patched. Returns `(changed_written, changed_verified)`.
fn bspatch_file_in_place(
    path: &Path,
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: &[&[u8]],
    identity: &[bool],
    chunk_hashes: Option<&[Option<[u8; 32]>]>,
    progress: Option<&ByteProgress<'_>>,
) -> Result<(usize, usize)> {
    // The bsdiff apply (decompress the per-byte diff string + add the old
    // chunk) is the expensive part of a chunk patch, so pass 1 keeps the
    // derived chunks for pass 2 instead of re-deriving them - bounded by
    // MAX_CACHED_IN_PLACE_BYTES so an all-changes patch cannot cache the
    // whole file. Over the bound, pass 2 re-derives as before.
    const MAX_CACHED_IN_PLACE_BYTES: usize = 32 * 1024 * 1024;

    let mut file = fs::OpenOptions::new().read(true).write(true).open(path)?;

    // Pass 1: read + patch + digest-check every changed chunk without
    // writing anything, so a digest failure fails closed before the first
    // byte of the target is modified.
    let mut changed_verified = 0usize;
    let mut cached: Option<Vec<(usize, Vec<u8>)>> = Some(Vec::new());
    let mut cached_bytes = 0usize;
    let mut offset = 0usize;
    for (idx, chunk_patch) in chunks.iter().enumerate() {
        let chunk_len = chunk_len_for_index(old_size, idx, chunk_size);
        if !identity[idx] {
            let (new_chunk, verified) = read_patched_chunk(
                &mut file,
                idx,
                offset,
                chunk_len,
                chunk_patch,
                chunk_hashes.and_then(|hashes| hashes[idx]),
            )?;
            if verified {
                changed_verified += 1;
            }
            if cached.is_some() {
                cached_bytes = cached_bytes.saturating_add(new_chunk.len());
                if cached_bytes > MAX_CACHED_IN_PLACE_BYTES {
                    cached = None;
                } else if let Some(entries) = cached.as_mut() {
                    entries.push((offset, new_chunk));
                }
            }
        }
        offset = offset.saturating_add(chunk_len);
    }

    // Pass 2: write every changed chunk. bspatch is a pure function of the
    // old chunk and the patch, so the bytes written are exactly the bytes
    // pass 1 verified (cached or re-derived).
    let mut changed_written = 0usize;
    let mut cache_idx = 0usize;
    offset = 0usize;
    for (idx, chunk_patch) in chunks.iter().enumerate() {
        let chunk_len = chunk_len_for_index(old_size, idx, chunk_size);
        if !identity[idx] {
            let new_chunk = match cached.as_ref() {
                Some(entries) => {
                    let (offset_, bytes) = entries[cache_idx].clone();
                    cache_idx += 1;
                    debug_assert_eq!(offset_, offset);
                    bytes
                }
                None => {
                    read_patched_chunk(
                        &mut file,
                        idx,
                        offset,
                        chunk_len,
                        chunk_patch,
                        chunk_hashes.and_then(|hashes| hashes[idx]),
                    )?
                    .0
                }
            };
            changed_written += 1;
            file.seek(SeekFrom::Start(usize_to_u64_saturating(offset)))?;
            file.write_all(&new_chunk)?;
        }
        offset = offset.saturating_add(chunk_len);
        if let Some(cb) = progress {
            cb(usize_to_u64_saturating(offset), usize_to_u64_saturating(new_size));
        }
    }
    file.flush()?;
    Ok((changed_written, changed_verified))
}

/// Read the old chunk at `offset`, apply its patch, and verify the result
/// against the patch's recorded target digest when one is present. Returns
/// the new chunk and whether a recorded digest was verified.
fn read_patched_chunk(
    file: &mut fs::File,
    idx: usize,
    offset: usize,
    chunk_len: usize,
    chunk_patch: &[u8],
    recorded: Option<[u8; 32]>,
) -> Result<(Vec<u8>, bool)> {
    if chunk_len == 0 {
        if !chunk_patch.is_empty() {
            return Err(SurgeError::Diff(
                "in-place patch carries a payload for an empty chunk".into(),
            ));
        }
        return verify_empty_target(idx, recorded);
    }
    if chunk_patch.is_empty() {
        return verify_empty_target(idx, recorded);
    }
    file.seek(SeekFrom::Start(usize_to_u64_saturating(offset)))?;
    let mut old_chunk = vec![0u8; chunk_len];
    file.read_exact(&mut old_chunk)?;
    let new_chunk = wrapper::bspatch_buffers(&old_chunk, chunk_patch)?;
    let verified = if let Some(expected) = recorded {
        let actual: [u8; 32] = sha256_raw(&new_chunk)
            .try_into()
            .map_err(|_| SurgeError::Diff("chunk target digest has an invalid length".into()))?;
        if actual != expected {
            return Err(SurgeError::Diff(format!(
                "chunk {idx} target digest mismatch: patch records a stale digest"
            )));
        }
        true
    } else {
        false
    };
    Ok((new_chunk, verified))
}

/// An empty target chunk (trailing chunk or empty patch payload) must
/// match the recorded empty digest when one is present.
fn verify_empty_target(idx: usize, recorded: Option<[u8; 32]>) -> Result<(Vec<u8>, bool)> {
    if let Some(expected) = recorded {
        let actual: [u8; 32] = sha256_raw(b"")
            .try_into()
            .map_err(|_| SurgeError::Diff("chunk target digest has an invalid length".into()))?;
        if actual != expected {
            return Err(SurgeError::Diff(format!(
                "chunk {idx} target digest mismatch: patch records a stale digest"
            )));
        }
        return Ok((Vec::new(), true));
    }
    Ok((Vec::new(), false))
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
