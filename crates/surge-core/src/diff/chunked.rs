//! Chunked bsdiff/bspatch for large files.
//!
//! Splits files into fixed-size aligned chunks and diffs each pair independently.
//! This reduces peak memory from O(8 × file_size) to O(8 × chunk_size) and
//! enables parallel processing across chunks.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::error::{Result, SurgeError};

use super::wrapper;

/// Magic bytes identifying the chunked patch format.
const MAGIC: &[u8; 4] = b"CSDF";

/// Format version.
///
/// Version 2 adds an identity-chunk bitset: chunks whose old and new content
/// are identical carry no bsdiff payload, and bspatch copies the old chunk
/// straight through. Old readers reject version 2 via the version check.
const VERSION: u8 = 2;

/// Legacy format version (no identity-chunk bitset).
const LEGACY_VERSION: u8 = 1;

/// Default chunk size: 64 MiB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Progress callback for file-backed patch application: (bytes_done, bytes_total).
pub type ByteProgress<'a> = dyn Fn(u64, u64) + 'a;

/// Returns whether `data` starts with the chunked patch magic bytes.
#[must_use]
pub fn has_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(MAGIC)
}

/// Options for chunked diff/patch operations.
pub struct ChunkedDiffOptions {
    /// Size of each chunk in bytes. Both files are split at these boundaries.
    pub chunk_size: usize,
    /// Maximum number of threads for parallel processing. 0 = auto (memory-aware).
    pub max_threads: usize,
}

impl Default for ChunkedDiffOptions {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            max_threads: 0,
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
    let results: Mutex<Vec<(usize, Vec<u8>, bool)>> = Mutex::new(Vec::with_capacity(num_chunks));
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
                    } else if old_chunk == new_chunk {
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

                    lock_mutex(&results).push((idx, patch, identity));
                }
            });
        }
    });

    if let Some(e) = into_inner(error) {
        return Err(e);
    }

    let mut chunks = into_inner(results);
    chunks.sort_by_key(|(idx, _, _)| *idx);

    serialize_patch(older.len(), newer.len(), chunk_size, &chunks)
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
    let results: Mutex<Vec<(usize, Vec<u8>, bool)>> = Mutex::new(Vec::with_capacity(num_chunks));
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
                        (new_chunk, false)
                    } else if new_chunk.is_empty() {
                        (Vec::new(), false)
                    } else if old_chunk == new_chunk {
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
                    lock_mutex(&results).push((idx, patch, identity));
                }
            });
        }
    });

    if let Some(err) = into_inner(error) {
        return Err(err);
    }
    let mut chunks = into_inner(results);
    chunks.sort_by_key(|(idx, _, _)| *idx);
    serialize_patch(old_size, new_size, chunk_size, &chunks)
}

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

    Ok(())
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn header_size() -> usize {
    4 + 1 + 8 + 8 + 8 + 4
}

fn identity_bitset_len(num_chunks: usize) -> usize {
    num_chunks.div_ceil(8)
}

fn set_identity_bit(bitset: &mut [u8], idx: usize) {
    bitset[idx / 8] |= 1u8 << (idx % 8);
}

fn identity_bit_is_set(bitset: &[u8], idx: usize) -> bool {
    bitset[idx / 8] & (1u8 << (idx % 8)) != 0
}

/// Patch format:
///   MAGIC (4 bytes) "CSDF"
///   VERSION (1 byte)
///   chunk_size (8 bytes LE)
///   old_size (8 bytes LE)
///   new_size (8 bytes LE)
///   num_chunks (4 bytes LE)
///   Version 2 only: identity bitset (ceil(num_chunks / 8) bytes, LSB-first)
///   For each chunk:
///     patch_len (8 bytes LE)
///     patch_data (patch_len bytes; empty for identity chunks)
fn serialize_patch(
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: &[(usize, Vec<u8>, bool)],
) -> Result<Vec<u8>> {
    let num_chunks = chunks.len();
    let mut bitset = vec![0u8; identity_bitset_len(num_chunks)];
    for (idx, _, identity) in chunks {
        if *identity {
            set_identity_bit(&mut bitset, *idx);
        }
    }

    let header_size = header_size();
    let data_size: usize = chunks.iter().map(|(_, p, _)| 8 + p.len()).sum();
    let mut buf = Vec::with_capacity(header_size + bitset.len() + data_size);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(
        &u64::try_from(chunk_size)
            .map_err(|_| SurgeError::Diff("chunk size exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u64::try_from(old_size)
            .map_err(|_| SurgeError::Diff("old size exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u64::try_from(new_size)
            .map_err(|_| SurgeError::Diff("new size exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(
        &u32::try_from(chunks.len())
            .map_err(|_| SurgeError::Diff("chunk count exceeds supported patch format".into()))?
            .to_le_bytes(),
    );
    buf.extend_from_slice(&bitset);

    for (_, patch, _) in chunks {
        buf.extend_from_slice(
            &u64::try_from(patch.len())
                .map_err(|_| SurgeError::Diff("patch chunk exceeds supported patch format".into()))?
                .to_le_bytes(),
        );
        buf.extend_from_slice(patch);
    }

    Ok(buf)
}

fn chunk_len_for_index(total_len: usize, chunk_idx: usize, chunk_size: usize) -> usize {
    let start = chunk_idx.saturating_mul(chunk_size);
    if start >= total_len {
        0
    } else {
        (total_len - start).min(chunk_size)
    }
}

fn read_exact_chunk(file: &mut fs::File, chunk_len: usize) -> Result<Vec<u8>> {
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

fn read_u64_le(data: &[u8], offset: usize) -> Result<u64> {
    let bytes: [u8; 8] = data[offset..offset + 8]
        .try_into()
        .map_err(|_| SurgeError::Diff("patch truncated reading u64".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32_le(data: &[u8], offset: usize) -> Result<u32> {
    let bytes: [u8; 4] = data[offset..offset + 4]
        .try_into()
        .map_err(|_| SurgeError::Diff("patch truncated reading u32".into()))?;
    Ok(u32::from_le_bytes(bytes))
}

/// Decoded chunked patch: sizes, per-chunk payloads, and the identity flags
/// (version 2; always empty flags for legacy version 1 patches).
struct ChunkedPatchData<'a> {
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: Vec<&'a [u8]>,
    identity: Vec<bool>,
}

fn deserialize_patch(data: &[u8]) -> Result<ChunkedPatchData<'_>> {
    let header_size = header_size();
    if data.len() < header_size {
        return Err(SurgeError::Diff("patch too short for header".into()));
    }

    if &data[0..4] != MAGIC {
        return Err(SurgeError::Diff("invalid chunked patch magic".into()));
    }
    let version = data[4];
    let is_legacy = version == LEGACY_VERSION;
    if !is_legacy && version != VERSION {
        return Err(SurgeError::Diff(format!(
            "unsupported chunked patch version: {version}"
        )));
    }

    let chunk_size = usize::try_from(read_u64_le(data, 5)?)
        .map_err(|_| SurgeError::Diff("chunk size exceeds platform limits".into()))?;
    let old_size = usize::try_from(read_u64_le(data, 13)?)
        .map_err(|_| SurgeError::Diff("old size exceeds platform limits".into()))?;
    let new_size = usize::try_from(read_u64_le(data, 21)?)
        .map_err(|_| SurgeError::Diff("new size exceeds platform limits".into()))?;
    let num_chunks = usize::try_from(read_u32_le(data, 29)?)
        .map_err(|_| SurgeError::Diff("chunk count exceeds platform limits".into()))?;

    let bitset_len = if is_legacy { 0 } else { identity_bitset_len(num_chunks) };
    if !is_legacy && data.len() < header_size + bitset_len {
        return Err(SurgeError::Diff("patch truncated reading identity bitset".into()));
    }
    let bitset = &data[header_size..header_size + bitset_len];
    let mut offset = header_size + bitset_len;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut identity = vec![false; num_chunks];

    for (idx, _) in (0..num_chunks).enumerate() {
        if offset + 8 > data.len() {
            return Err(SurgeError::Diff("patch truncated at chunk length".into()));
        }
        let patch_len = usize::try_from(read_u64_le(data, offset)?)
            .map_err(|_| SurgeError::Diff("patch chunk length exceeds platform limits".into()))?;
        offset += 8;

        if offset + patch_len > data.len() {
            return Err(SurgeError::Diff("patch truncated at chunk data".into()));
        }
        if !is_legacy && identity_bit_is_set(bitset, idx) {
            if patch_len != 0 {
                return Err(SurgeError::Diff(
                    "identity chunk must carry an empty patch payload".into(),
                ));
            }
            identity[idx] = true;
        }
        chunks.push(&data[offset..offset + patch_len]);
        offset += patch_len;
    }

    Ok(ChunkedPatchData {
        old_size,
        new_size,
        chunk_size,
        chunks,
        identity,
    })
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
            },
        )
        .expect("build patch");
        chunked_bspatch_file(&old_path, &patch, &rebuilt_path).expect("apply patch");

        assert_eq!(std::fs::read(&rebuilt_path).expect("read rebuilt"), new);
    }

    #[test]
    fn test_chunked_v2_identity_chunks_carry_no_payload() {
        // 4 chunks, middle two identical to the old file.
        let chunk = 256usize;
        let old: Vec<u8> = (0..(4 * chunk)).map(|i| (i % 251) as u8).collect();
        let mut new = old.clone();
        new[10] = 0xFF; // chunk 0 changes
        new[4 * chunk - 1] = 0xAB; // chunk 3 changes
        // chunks 1 and 2 remain identical

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 2,
        };
        let patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");

        // Version byte bumped; identity bitset present (4 chunks -> 1 byte).
        assert_eq!(patch[4], VERSION);
        let header = 4 + 1 + 8 + 8 + 8 + 4;
        assert_eq!(
            patch[header], 0b0000_0110,
            "chunks 1 and 2 must be identity, 0 and 3 not"
        );
        // Chunks 1 and 2: empty payloads.
        let mut off = header + 1;
        let mut lens = Vec::new();
        for _ in 0..4 {
            let len = u64::from_le_bytes(patch[off..off + 8].try_into().unwrap()) as usize;
            lens.push(len);
            off += 8 + len;
        }
        assert_eq!(off, patch.len(), "no trailing bytes");
        assert_eq!(lens[1], 0, "identity chunk 1 payload must be empty");
        assert_eq!(lens[2], 0, "identity chunk 2 payload must be empty");
        assert!(lens[0] > 0 && lens[3] > 0);

        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_v1_patch_still_applies() {
        // Hand-build a version-1 patch (no bitset) and apply it with the v2
        // reader: unchanged chunks carry a real identity bsdiff payload.
        let chunk = 256usize;
        let old: Vec<u8> = vec![7u8; 3 * chunk];
        let mut new = old.clone();
        new[3 * chunk - 1] = 99;

        let c0 = wrapper::bsdiff_buffers(&old[0..chunk], &new[0..chunk]).unwrap();
        let c1 = wrapper::bsdiff_buffers(&old[chunk..2 * chunk], &new[chunk..2 * chunk]).unwrap();
        let c2 = wrapper::bsdiff_buffers(&old[2 * chunk..3 * chunk], &new[2 * chunk..3 * chunk]).unwrap();

        let mut patch = Vec::new();
        patch.extend_from_slice(MAGIC);
        patch.push(LEGACY_VERSION);
        patch.extend_from_slice(&(chunk as u64).to_le_bytes());
        patch.extend_from_slice(&(old.len() as u64).to_le_bytes());
        patch.extend_from_slice(&(new.len() as u64).to_le_bytes());
        patch.extend_from_slice(&3u32.to_le_bytes());
        for c in [c0, c1, c2] {
            patch.extend_from_slice(&(c.len() as u64).to_le_bytes());
            patch.extend_from_slice(&c);
        }

        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
        };
        let reconstructed = chunked_bspatch(&old, &patch, &opts).expect("bspatch v1");
        assert_eq!(reconstructed, new);
    }

    #[test]
    fn test_chunked_unknown_version_rejected() {
        let chunk = 256usize;
        let old: Vec<u8> = vec![1u8; chunk];
        let new = vec![2u8; chunk];
        let opts = ChunkedDiffOptions {
            chunk_size: chunk,
            max_threads: 1,
        };
        let mut patch = chunked_bsdiff(&old, &new, &opts).expect("bsdiff");
        patch[4] = 3;
        let err = chunked_bspatch(&old, &patch, &opts).unwrap_err();
        assert!(err.to_string().contains("unsupported chunked patch version"), "{err}");
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
}
