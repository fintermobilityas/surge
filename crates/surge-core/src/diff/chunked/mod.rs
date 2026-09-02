//! Chunked bsdiff/bspatch for large files.
//!
//! Splits files into fixed-size aligned chunks and diffs each pair independently.
//! This reduces peak memory from O(8 × file_size) to O(8 × chunk_size) and
//! enables parallel processing across chunks.

mod format;

mod bspatch;

pub use bspatch::{
    ChunkedBspatchResult, chunked_bspatch_file, chunked_bspatch_file_with_progress,
    chunked_bspatch_file_with_progress_and_sha256, chunked_bspatch_file_with_progress_and_sha256_in_place,
};
pub use format::has_magic_prefix;
use format::{ChunkedPatchData, SerializedChunk, deserialize_patch, serialize_patch};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use crate::crypto::sha256::sha256_raw;
use crate::error::{Result, SurgeError};

use super::wrapper;

/// Default chunk size: 64 MiB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Progress callback for file-backed patch application: (bytes_done, bytes_total).
pub type ByteProgress<'a> = dyn Fn(u64, u64) + 'a;

/// On-disk format written for a chunked patch.
///
/// Readers accept both; the choice only affects what publishers emit. `Legacy`
/// (format version 1) is the default because every reader in the field can apply
/// it. `IdentityChunks` (format version 2) skips the bsdiff payload for chunks that
/// did not change, which is much cheaper to produce and apply for large files with
/// small edits, but readers older than the version that introduced it reject the
/// patch — publish it only once every client has been upgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChunkedPatchFormat {
    /// Format version 1: every chunk carries a bsdiff payload.
    #[default]
    Legacy,
    /// Format version 2: unchanged chunks are marked in an identity bitset and carry no payload.
    IdentityChunks,
    /// Format version 3: the identity bitset plus a SHA-256 target digest for
    /// every changed chunk, letting the in-place applier verify rewritten
    /// chunks in memory instead of re-reading the whole target file.
    IdentityChunksWithTargetHashes,
}

/// Options for chunked diff/patch operations.
pub struct ChunkedDiffOptions {
    /// Size of each chunk in bytes. Both files are split at these boundaries.
    pub chunk_size: usize,
    /// Maximum number of threads for parallel processing. 0 = auto (memory-aware).
    pub max_threads: usize,
    /// Patch format to write. Ignored when applying a patch.
    pub format: ChunkedPatchFormat,
}

impl Default for ChunkedDiffOptions {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_threads: 0,
            format: ChunkedPatchFormat::default(),
        }
    }
}

impl ChunkedDiffOptions {
    fn effective_threads(&self) -> usize {
        let cpu_count = thread::available_parallelism().map_or(1, std::num::NonZero::get);

        if self.max_threads != 0 {
            return self.max_threads.min(cpu_count);
        }

        // Memory-aware: each concurrent bsdiff needs ~10× chunk_size
        // (suffix array ≈ 8×, plus old+new chunk buffers).
        let mem_per_thread = self.chunk_size.saturating_mul(10);
        if mem_per_thread == 0 {
            return cpu_count;
        }

        let available = available_memory_bytes();
        // Reserve 20% headroom for OS and other allocations
        let usable = available * 4 / 5;
        let mem_threads = (usable / mem_per_thread).max(1);

        cpu_count.min(mem_threads)
    }
}

/// Returns available system memory in bytes.
///
/// On Linux, reads `MemAvailable` from `/proc/meminfo`.
/// Falls back to a conservative 4 GiB estimate on other platforms or on error.
fn available_memory_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if let Some(rest) = line.strip_prefix("MemAvailable:")
                    && let Some(kb_str) = rest.trim().strip_suffix("kB")
                    && let Ok(kb) = kb_str.trim().parse::<usize>()
                {
                    return kb * 1024;
                }
            }
        }
    }
    // Conservative fallback: 4 GiB
    4 * 1024 * 1024 * 1024
}

fn lock_mutex<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn into_inner<T>(m: Mutex<T>) -> T {
    m.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Create a chunked binary diff patch.
///
/// Splits `older` and `newer` into aligned chunks of `opts.chunk_size` bytes,
/// diffs each pair with bsdiff, and packs the results into a single buffer.
///
/// If a chunk exists only in the newer file (file grew), it is stored verbatim.
/// If a chunk exists only in the older file (file shrank), it is omitted
/// (the patch records the new file size so bspatch knows when to stop).
/// Identity-chunk marking is available in both identity-bitset formats.
fn identity_format_enabled(format: ChunkedPatchFormat) -> bool {
    matches!(
        format,
        ChunkedPatchFormat::IdentityChunks | ChunkedPatchFormat::IdentityChunksWithTargetHashes
    )
}

pub fn chunked_bsdiff(older: &[u8], newer: &[u8], opts: &ChunkedDiffOptions) -> Result<Vec<u8>> {
    let chunk_size = opts.chunk_size;
    if chunk_size == 0 {
        return Err(SurgeError::Diff("chunk_size must be > 0".into()));
    }

    let num_old_chunks = older.len().div_ceil(chunk_size);
    let num_new_chunks = newer.len().div_ceil(chunk_size);
    let num_chunks = num_old_chunks.max(num_new_chunks);
    let num_threads = opts.effective_threads();

    // Parallel chunk diffing
    let work_counter = AtomicUsize::new(0);
    let results: Mutex<Vec<SerializedChunk>> = Mutex::new(Vec::with_capacity(num_chunks));
    let error: Mutex<Option<SurgeError>> = Mutex::new(None);

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn(|| {
                loop {
                    if lock_mutex(&error).is_some() {
                        return;
                    }

                    let idx = work_counter.fetch_add(1, Ordering::Relaxed);
                    if idx >= num_chunks {
                        return;
                    }

                    let old_start = idx * chunk_size;
                    let new_start = idx * chunk_size;

                    let old_chunk = if old_start < older.len() {
                        let end = (old_start + chunk_size).min(older.len());
                        &older[old_start..end]
                    } else {
                        &[]
                    };

                    let new_chunk = if new_start < newer.len() {
                        let end = (new_start + chunk_size).min(newer.len());
                        &newer[new_start..end]
                    } else {
                        &[]
                    };

                    let (patch, identity) = if old_chunk.is_empty() {
                        (new_chunk.to_vec(), false)
                    } else if new_chunk.is_empty() {
                        (Vec::new(), false)
                    } else if old_chunk == new_chunk && identity_format_enabled(opts.format) {
                        // Unchanged chunk: record the identity marker instead
                        // of paying for a whole-chunk bsdiff.
                        (Vec::new(), true)
                    } else {
                        match wrapper::bsdiff_buffers(old_chunk, new_chunk) {
                            Ok(p) => (p, false),
                            Err(e) => {
                                *lock_mutex(&error) = Some(e);
                                return;
                            }
                        }
                    };

                    // v3 records the target digest of every changed chunk;
                    // the chunk's target content is `new_chunk` in all
                    // branches, so no extra read is needed.
                    let target_hash = if opts.format == ChunkedPatchFormat::IdentityChunksWithTargetHashes && !identity
                    {
                        match sha256_raw(new_chunk).try_into() {
                            Ok(digest) => Some(digest),
                            Err(_) => {
                                *lock_mutex(&error) =
                                    Some(SurgeError::Diff("sha256 digest has an invalid length".into()));
                                return;
                            }
                        }
                    } else {
                        None
                    };

                    lock_mutex(&results).push(SerializedChunk {
                        idx,
                        patch,
                        identity,
                        target_hash,
                    });
                }
            });
        }
    });

    if let Some(e) = into_inner(error) {
        return Err(e);
    }

    let mut chunks = into_inner(results);
    chunks.sort_by_key(|chunk| chunk.idx);

    serialize_patch(older.len(), newer.len(), chunk_size, &chunks, opts.format)
}

/// Apply a chunked patch to reconstruct the newer file.
pub fn chunked_bspatch(older: &[u8], patch: &[u8], opts: &ChunkedDiffOptions) -> Result<Vec<u8>> {
    let decoded = deserialize_patch(patch)?;
    let ChunkedPatchData {
        old_size,
        new_size,
        chunk_size,
        chunks: chunk_patches,
        identity,
        chunk_hashes,
    } = decoded;

    if older.len() != old_size {
        return Err(SurgeError::Diff(format!(
            "old file size mismatch: expected {old_size}, got {}",
            older.len()
        )));
    }

    let num_chunks = chunk_patches.len();
    let thread_opts = ChunkedDiffOptions {
        chunk_size,
        max_threads: opts.max_threads,
        format: opts.format,
    };
    let num_threads = thread_opts.effective_threads();

    let work_counter = AtomicUsize::new(0);
    let results: Mutex<Vec<(usize, Vec<u8>)>> = Mutex::new(Vec::with_capacity(num_chunks));
    let error: Mutex<Option<SurgeError>> = Mutex::new(None);

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn(|| {
                loop {
                    if lock_mutex(&error).is_some() {
                        return;
                    }

                    let idx = work_counter.fetch_add(1, Ordering::Relaxed);
                    if idx >= num_chunks {
                        return;
                    }

                    let chunk_patch = chunk_patches[idx];
                    let old_start = idx * chunk_size;

                    let old_chunk = if old_start < older.len() {
                        let end = (old_start + chunk_size).min(older.len());
                        &older[old_start..end]
                    } else {
                        &[]
                    };

                    let new_chunk = if identity[idx] {
                        if old_chunk.is_empty() {
                            *lock_mutex(&error) = Some(SurgeError::Diff("identity chunk has no old content".into()));
                            return;
                        }
                        old_chunk.to_vec()
                    } else if old_chunk.is_empty() {
                        chunk_patch.to_vec()
                    } else if chunk_patch.is_empty() {
                        Vec::new()
                    } else {
                        match wrapper::bspatch_buffers(old_chunk, chunk_patch) {
                            Ok(data) => data,
                            Err(e) => {
                                *lock_mutex(&error) = Some(e);
                                return;
                            }
                        }
                    };
                    let expected_hash = (!identity[idx]).then_some(chunk_hashes[idx]).flatten();
                    if let Some(expected) = expected_hash {
                        let actual: [u8; 32] = match sha256_raw(&new_chunk).try_into() {
                            Ok(digest) => digest,
                            Err(_) => {
                                *lock_mutex(&error) =
                                    Some(SurgeError::Diff("sha256 digest has an invalid length".into()));
                                return;
                            }
                        };
                        if actual != expected {
                            *lock_mutex(&error) = Some(SurgeError::Diff(format!(
                                "chunk {idx} target digest mismatch: patch records a stale digest"
                            )));
                            return;
                        }
                    }
                    lock_mutex(&results).push((idx, new_chunk));
                }
            });
        }
    });

    if let Some(e) = into_inner(error) {
        return Err(e);
    }

    let mut chunks = into_inner(results);
    chunks.sort_by_key(|(idx, _)| *idx);

    // Concatenate all chunks
    let mut output = Vec::with_capacity(new_size);
    for (_, data) in chunks {
        output.extend_from_slice(&data);
    }

    if output.len() != new_size {
        return Err(SurgeError::Diff(format!(
            "reconstructed size mismatch: expected {new_size}, got {}",
            output.len()
        )));
    }

    Ok(output)
}

/// Create a chunked binary diff patch directly from two files without loading
/// either file fully into memory.
pub fn chunked_bsdiff_files(older_path: &Path, newer_path: &Path, opts: &ChunkedDiffOptions) -> Result<Vec<u8>> {
    let chunk_size = opts.chunk_size;
    if chunk_size == 0 {
        return Err(SurgeError::Diff("chunk_size must be > 0".into()));
    }

    let old_size = usize::try_from(fs::metadata(older_path)?.len())
        .map_err(|_| SurgeError::Diff("old file exceeds platform limits".into()))?;
    let new_size = usize::try_from(fs::metadata(newer_path)?.len())
        .map_err(|_| SurgeError::Diff("new file exceeds platform limits".into()))?;
    let num_old_chunks = old_size.div_ceil(chunk_size);
    let num_new_chunks = new_size.div_ceil(chunk_size);
    let num_chunks = num_old_chunks.max(num_new_chunks);
    let num_threads = opts.effective_threads();
    let work_counter = AtomicUsize::new(0);
    let results: Mutex<Vec<SerializedChunk>> = Mutex::new(Vec::with_capacity(num_chunks));
    let error: Mutex<Option<SurgeError>> = Mutex::new(None);

    thread::scope(|scope| {
        for _ in 0..num_threads {
            scope.spawn(|| {
                let mut old_file = match fs::File::open(older_path) {
                    Ok(file) => file,
                    Err(err) => {
                        *lock_mutex(&error) = Some(SurgeError::Io(err));
                        return;
                    }
                };
                let mut new_file = match fs::File::open(newer_path) {
                    Ok(file) => file,
                    Err(err) => {
                        *lock_mutex(&error) = Some(SurgeError::Io(err));
                        return;
                    }
                };

                loop {
                    if lock_mutex(&error).is_some() {
                        return;
                    }

                    let idx = work_counter.fetch_add(1, Ordering::Relaxed);
                    if idx >= num_chunks {
                        return;
                    }

                    let old_chunk_len = chunk_len_for_index(old_size, idx, chunk_size);
                    let new_chunk_len = chunk_len_for_index(new_size, idx, chunk_size);
                    let old_chunk = match read_chunk_at(&mut old_file, idx, chunk_size, old_chunk_len) {
                        Ok(chunk) => chunk,
                        Err(err) => {
                            *lock_mutex(&error) = Some(err);
                            return;
                        }
                    };
                    let new_chunk = match read_chunk_at(&mut new_file, idx, chunk_size, new_chunk_len) {
                        Ok(chunk) => chunk,
                        Err(err) => {
                            *lock_mutex(&error) = Some(err);
                            return;
                        }
                    };

                    let (patch, identity) = if old_chunk.is_empty() {
                        (new_chunk.clone(), false)
                    } else if new_chunk.is_empty() {
                        (Vec::new(), false)
                    } else if old_chunk == new_chunk && identity_format_enabled(opts.format) {
                        // Unchanged chunk: record the identity marker instead
                        // of paying for a whole-chunk bsdiff.
                        (Vec::new(), true)
                    } else {
                        match wrapper::bsdiff_buffers(&old_chunk, &new_chunk) {
                            Ok(patch) => (patch, false),
                            Err(err) => {
                                *lock_mutex(&error) = Some(err);
                                return;
                            }
                        }
                    };
                    let target_hash = if opts.format == ChunkedPatchFormat::IdentityChunksWithTargetHashes && !identity
                    {
                        match sha256_raw(&new_chunk).try_into() {
                            Ok(digest) => Some(digest),
                            Err(_) => {
                                *lock_mutex(&error) =
                                    Some(SurgeError::Diff("sha256 digest has an invalid length".into()));
                                return;
                            }
                        }
                    } else {
                        None
                    };
                    lock_mutex(&results).push(SerializedChunk {
                        idx,
                        patch,
                        identity,
                        target_hash,
                    });
                }
            });
        }
    });

    if let Some(err) = into_inner(error) {
        return Err(err);
    }
    let mut chunks = into_inner(results);
    chunks.sort_by_key(|chunk| chunk.idx);
    serialize_patch(old_size, new_size, chunk_size, &chunks, opts.format)
}

pub(super) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(super) fn chunk_len_for_index(total_len: usize, chunk_idx: usize, chunk_size: usize) -> usize {
    let start = chunk_idx.saturating_mul(chunk_size);
    if start >= total_len {
        0
    } else {
        (total_len - start).min(chunk_size)
    }
}

pub(super) fn read_exact_chunk(file: &mut fs::File, chunk_len: usize) -> Result<Vec<u8>> {
    let mut chunk = vec![0u8; chunk_len];
    if chunk_len == 0 {
        return Ok(chunk);
    }
    file.read_exact(&mut chunk)?;
    Ok(chunk)
}

fn read_chunk_at(file: &mut fs::File, chunk_idx: usize, chunk_size: usize, chunk_len: usize) -> Result<Vec<u8>> {
    let start = u64::try_from(chunk_idx.saturating_mul(chunk_size))
        .map_err(|_| SurgeError::Diff("chunk offset exceeds supported limits".into()))?;
    file.seek(SeekFrom::Start(start))?;
    read_exact_chunk(file, chunk_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_roundtrip_identical() {
        let data = vec![42u8; 1024];
        let opts = ChunkedDiffOptions {
            chunk_size: 256,
            max_threads: 1,
            format: ChunkedPatchFormat::Legacy,
        };
        let patch = chunked_bsdiff(&data, &data, &opts).expect("bsdiff");
        let reconstructed = chunked_bspatch(&data, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_chunked_roundtrip_different() {
        let old = vec![1u8; 2048];
        let mut new = old.clone();
        new[100] = 99;
        new[1500] = 200;

        let opts = ChunkedDiffOptions {
            chunk_size: 512,
            max_threads: 2,
            format: ChunkedPatchFormat::Legacy,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_file_grew() {
        let old = vec![1u8; 512];
        let mut new = old.clone();
        new.extend_from_slice(&[2u8; 512]);

        let opts = ChunkedDiffOptions {
            chunk_size: 512,
            max_threads: 1,
            format: ChunkedPatchFormat::Legacy,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_file_shrank() {
        let old = vec![1u8; 1024];
        let new = vec![1u8; 300];

        let opts = ChunkedDiffOptions {
            chunk_size: 512,
            max_threads: 1,
            format: ChunkedPatchFormat::Legacy,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_parallel() {
        let old = vec![0u8; 4096];
        let mut new = old.clone();
        for i in (0..4096).step_by(100) {
            new[i] = 0xFF;
        }

        let opts = ChunkedDiffOptions {
            chunk_size: 512,
            max_threads: 4,
            format: ChunkedPatchFormat::Legacy,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_file_roundtrip_avoids_full_in_memory_inputs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("old.bin");
        let new_path = tmp.path().join("new.bin");
        let rebuilt_path = tmp.path().join("rebuilt.bin");

        let old = vec![7u8; 2 * 1024 * 1024];
        let mut new = old.clone();
        new[123] = 9;
        new[1_234_567] = 3;
        std::fs::write(&old_path, &old).expect("write old");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::Legacy,
            },
        )
        .expect("build patch");
        chunked_bspatch_file(&old_path, &patch, &rebuilt_path).expect("apply patch");

        assert_eq!(std::fs::read(&rebuilt_path).expect("read rebuilt"), new);
    }

    #[test]
    fn test_chunked_v2_identity_bit_without_old_content_rejected() {
        // Corrupt v2 patch: file grew so the last chunk has no old content,
        // but its identity bit is set.
        let chunk = 256usize;
        let old: Vec<u8> = vec![1u8; chunk];
        let new: Vec<u8> = vec![2u8; 2 * chunk];
        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
            format: ChunkedPatchFormat::IdentityChunks,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        let header = 4 + 1 + 8 + 8 + 8 + 4;
        let bitset = header;
        let chunk0_len_off = bitset + 1;
        let chunk0_len = u64::from_le_bytes(patch[chunk0_len_off..chunk0_len_off + 8].try_into().unwrap()) as usize;
        let chunk1_len_off = chunk0_len_off + 8 + chunk0_len;
        let mut corrupted = patch.clone();
        corrupted[bitset] |= 1u8 << 1; // identity bit for chunk 1 (no old content)
        // Zero chunk 1's payload length so deserialize accepts the marker.
        corrupted[chunk1_len_off..chunk1_len_off + 8].copy_from_slice(&0u64.to_le_bytes());
        let err = chunked_bspatch(&old, &corrupted, &opts).unwrap_err();
        assert!(err.to_string().contains("identity chunk has no old content"), "{err}");
    }

    #[test]
    fn test_chunked_bspatch_file_with_sha256_matches_output() {
        use crate::crypto::sha256::sha256_hex;

        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("old.bin");
        let new_path = tmp.path().join("new.bin");
        let rebuilt_path = tmp.path().join("rebuilt.bin");

        let old = vec![7u8; 2 * 1024 * 1024];
        let mut new = old.clone();
        new[123] = 9;
        new[1_234_567] = 3;
        std::fs::write(&old_path, &old).expect("write old");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::IdentityChunks,
            },
        )
        .expect("build patch");
        let hash =
            chunked_bspatch_file_with_progress_and_sha256(&old_path, &patch, &rebuilt_path, None).expect("apply patch");

        assert_eq!(hash, sha256_hex(&new));
        assert_eq!(std::fs::read(&rebuilt_path).expect("read rebuilt"), new);
    }

    #[test]
    fn in_place_bspatch_same_size_format2_patches_target_directly() {
        use crate::crypto::sha256::sha256_hex;
        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("target.bin");
        let new_path = tmp.path().join("new.bin");
        let unused_output = tmp.path().join("unused.bin");

        let old = vec![11u8; 1024 * 1024];
        let mut new = old.clone();
        new[70_000] = 200; // inside the second 256 KiB chunk
        std::fs::write(&old_path, &old).expect("write target");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::IdentityChunks,
            },
        )
        .expect("build patch");

        let result = chunked_bspatch_file_with_progress_and_sha256_in_place(&old_path, &patch, &unused_output, None)
            .expect("apply patch");
        assert!(result.applied_in_place);
        assert!(!unused_output.exists(), "in-place patch must not write the output path");
        assert_eq!(result.target_hash, Some(sha256_hex(&new)));
        assert!(
            !result.chunk_hashes_verified,
            "v2 patches have no digests: the full read must run"
        );
        assert_eq!(std::fs::read(&old_path).expect("read target"), new);
    }

    #[test]
    fn in_place_bspatch_all_identity_is_a_noop() {
        use crate::crypto::sha256::sha256_hex;
        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("target.bin");
        let new_path = tmp.path().join("new.bin");
        let unused_output = tmp.path().join("unused.bin");

        let old = vec![42u8; 700 * 1024];
        let new = old.clone();
        std::fs::write(&old_path, &old).expect("write target");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::IdentityChunks,
            },
        )
        .expect("build patch");

        let result = chunked_bspatch_file_with_progress_and_sha256_in_place(&old_path, &patch, &unused_output, None)
            .expect("apply patch");
        assert!(result.applied_in_place);
        assert_eq!(result.target_hash, Some(sha256_hex(&old)));
        assert!(
            !result.chunk_hashes_verified,
            "all-identity patches have no changed chunk to digest-verify"
        );
        assert_eq!(std::fs::read(&old_path).expect("read target"), old);
    }

    #[test]
    fn in_place_bspatch_falls_back_when_sizes_differ() {
        use crate::crypto::sha256::sha256_hex;
        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("target.bin");
        let new_path = tmp.path().join("new.bin");
        let output = tmp.path().join("rebuilt.bin");

        let old = vec![5u8; 300 * 1024];
        let mut new = old.clone();
        new[1000] = 9;
        new.extend_from_slice(&[1u8; 1000]); // file grows
        std::fs::write(&old_path, &old).expect("write target");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::IdentityChunks,
            },
        )
        .expect("build patch");

        let result = chunked_bspatch_file_with_progress_and_sha256_in_place(&old_path, &patch, &output, None)
            .expect("apply patch");
        assert!(!result.applied_in_place);
        assert_eq!(result.target_hash, Some(sha256_hex(&new)));
        assert_eq!(std::fs::read(&output).expect("read rebuilt"), new);
        assert_eq!(std::fs::read(&old_path).expect("read target"), old);
    }

    #[test]
    fn in_place_bspatch_falls_back_for_legacy_format() {
        use crate::crypto::sha256::sha256_hex;
        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("target.bin");
        let new_path = tmp.path().join("new.bin");
        let output = tmp.path().join("rebuilt.bin");

        let old = vec![33u8; 600 * 1024];
        let mut new = old.clone();
        new[123_456] = 77;
        std::fs::write(&old_path, &old).expect("write target");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::Legacy,
            },
        )
        .expect("build patch");

        let result = chunked_bspatch_file_with_progress_and_sha256_in_place(&old_path, &patch, &output, None)
            .expect("apply patch");
        assert!(!result.applied_in_place);
        assert_eq!(result.target_hash, Some(sha256_hex(&new)));
        assert_eq!(std::fs::read(&output).expect("read rebuilt"), new);
        assert_eq!(std::fs::read(&old_path).expect("read target"), old);
    }

    #[test]
    fn in_place_bspatch_v3_digests_verify_without_full_read() {
        use crate::crypto::sha256::sha256_hex;
        let tmp = tempfile::tempdir().expect("tempdir");
        let old_path = tmp.path().join("target.bin");
        let new_path = tmp.path().join("new.bin");
        let unused_output = tmp.path().join("unused.bin");

        let old = vec![11u8; 1024 * 1024];
        let mut new = old.clone();
        new[70_000] = 200; // inside the second 256 KiB chunk

        std::fs::write(&old_path, &old).expect("write target");
        std::fs::write(&new_path, &new).expect("write new");

        let patch = chunked_bsdiff_files(
            &old_path,
            &new_path,
            &ChunkedDiffOptions {
                chunk_size: 256 * 1024,
                max_threads: 1,
                format: ChunkedPatchFormat::IdentityChunksWithTargetHashes,
            },
        )
        .expect("build patch");

        let result = chunked_bspatch_file_with_progress_and_sha256_in_place(&old_path, &patch, &unused_output, None)
            .expect("apply patch");
        assert!(result.applied_in_place);
        assert!(
            result.chunk_hashes_verified,
            "v3 digests must verify the rewritten chunk"
        );
        assert_eq!(result.target_hash, None, "digest-verified path skips the full read");
        assert_eq!(std::fs::read(&old_path).expect("read target"), new);
        assert!(!unused_output.exists(), "in-place patch must not write the output path");

        // A tampered recorded digest must be rejected.
        let decoded = super::format::deserialize_patch(&patch).expect("decode");
        let header = 4 + 1 + 8 + 8 + 8 + 4;
        let bitset = decoded.chunks.len().div_ceil(8);
        let chunk0_len = u64::from_le_bytes(patch[header + bitset..header + bitset + 8].try_into().unwrap()) as usize;
        let mut tampered = patch.clone();
        tampered[header + bitset + 8 + chunk0_len] ^= 0xFF;
        let err = chunked_bspatch_file_with_progress_and_sha256_in_place(
            &tmp.path().join("target2.bin"),
            &tampered,
            &unused_output,
            None,
        );
        // Re-create the target file first: the previous apply already
        // patched target.bin in place.
        let _ = err;
        std::fs::write(&old_path, &old).expect("restore target");
        let err = chunked_bspatch_file_with_progress_and_sha256_in_place(&old_path, &tampered, &unused_output, None)
            .unwrap_err();
        assert!(err.to_string().contains("target digest mismatch"), "{err}");
        assert_eq!(
            std::fs::read(&old_path).expect("read target"),
            old,
            "failed apply must leave the target untouched"
        );
        let _ = sha256_hex(&old);
    }
}
