//! Chunked bsdiff/bspatch for large files.
//!
//! Splits files into fixed-size aligned chunks and diffs each pair independently.
//! This reduces peak memory from O(8 × file_size) to O(8 × chunk_size) and
//! enables parallel processing across chunks.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use crate::error::{Result, SurgeError};

use super::wrapper;

/// Magic bytes identifying the chunked patch format.
const MAGIC: &[u8; 4] = b"CSDF";

/// Format version.
const VERSION: u8 = 1;

/// Default chunk size: 64 MiB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024 * 1024;

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
        let cpu_count = thread::available_parallelism().map(std::num::NonZero::get).unwrap_or(1);

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
    let results: Mutex<Vec<(usize, Vec<u8>)>> = Mutex::new(Vec::with_capacity(num_chunks));
    let error: Mutex<Option<SurgeError>> = Mutex::new(None);

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn(|| {
                loop {
                    // Check for error from another thread
                    if error.lock().unwrap().is_some() {
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

                    let patch = if old_chunk.is_empty() {
                        // New chunk with no old counterpart — store verbatim
                        // (bsdiff against empty produces overhead; raw is simpler)
                        new_chunk.to_vec()
                    } else if new_chunk.is_empty() {
                        // Old chunk removed — empty patch
                        Vec::new()
                    } else {
                        match wrapper::bsdiff_buffers(old_chunk, new_chunk) {
                            Ok(p) => p,
                            Err(e) => {
                                *error.lock().unwrap() = Some(e);
                                return;
                            }
                        }
                    };

                    results.lock().unwrap().push((idx, patch));
                }
            });
        }
    });

    // Check for errors
    if let Some(e) = error.into_inner().unwrap() {
        return Err(e);
    }

    // Sort results by chunk index
    let mut chunks = results.into_inner().unwrap();
    chunks.sort_by_key(|(idx, _)| *idx);

    // Serialize: header + chunk patches
    serialize_patch(older.len(), newer.len(), chunk_size, &chunks)
}

/// Apply a chunked patch to reconstruct the newer file.
pub fn chunked_bspatch(older: &[u8], patch: &[u8], opts: &ChunkedDiffOptions) -> Result<Vec<u8>> {
    let (old_size, new_size, chunk_size, chunk_patches) = deserialize_patch(patch)?;

    if older.len() != old_size {
        return Err(SurgeError::Diff(format!(
            "old file size mismatch: expected {old_size}, got {}",
            older.len()
        )));
    }

    let num_chunks = chunk_patches.len();
    let num_threads = opts.effective_threads();

    let work_counter = AtomicUsize::new(0);
    let results: Mutex<Vec<(usize, Vec<u8>)>> = Mutex::new(Vec::with_capacity(num_chunks));
    let error: Mutex<Option<SurgeError>> = Mutex::new(None);

    thread::scope(|s| {
        for _ in 0..num_threads {
            s.spawn(|| {
                loop {
                    if error.lock().unwrap().is_some() {
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

                    let new_chunk = if old_chunk.is_empty() {
                        // No old data — patch IS the verbatim new data
                        chunk_patch.to_vec()
                    } else if chunk_patch.is_empty() {
                        // Chunk was removed
                        Vec::new()
                    } else {
                        match wrapper::bspatch_buffers(old_chunk, chunk_patch) {
                            Ok(data) => data,
                            Err(e) => {
                                *error.lock().unwrap() = Some(e);
                                return;
                            }
                        }
                    };

                    results.lock().unwrap().push((idx, new_chunk));
                }
            });
        }
    });

    if let Some(e) = error.into_inner().unwrap() {
        return Err(e);
    }

    let mut chunks = results.into_inner().unwrap();
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

/// Patch format:
///   MAGIC (4 bytes) "CSDF"
///   VERSION (1 byte)
///   chunk_size (8 bytes LE)
///   old_size (8 bytes LE)
///   new_size (8 bytes LE)
///   num_chunks (4 bytes LE)
///   For each chunk:
///     patch_len (8 bytes LE)
///     patch_data (patch_len bytes)
fn serialize_patch(
    old_size: usize,
    new_size: usize,
    chunk_size: usize,
    chunks: &[(usize, Vec<u8>)],
) -> Result<Vec<u8>> {
    let header_size = 4 + 1 + 8 + 8 + 8 + 4;
    let data_size: usize = chunks.iter().map(|(_, p)| 8 + p.len()).sum();
    let mut buf = Vec::with_capacity(header_size + data_size);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&(chunk_size as u64).to_le_bytes());
    buf.extend_from_slice(&(old_size as u64).to_le_bytes());
    buf.extend_from_slice(&(new_size as u64).to_le_bytes());
    buf.extend_from_slice(&(chunks.len() as u32).to_le_bytes());

    for (_, patch) in chunks {
        buf.extend_from_slice(&(patch.len() as u64).to_le_bytes());
        buf.extend_from_slice(patch);
    }

    Ok(buf)
}

fn deserialize_patch(data: &[u8]) -> Result<(usize, usize, usize, Vec<&[u8]>)> {
    let header_size = 4 + 1 + 8 + 8 + 8 + 4;
    if data.len() < header_size {
        return Err(SurgeError::Diff("patch too short for header".into()));
    }

    if &data[0..4] != MAGIC {
        return Err(SurgeError::Diff("invalid chunked patch magic".into()));
    }
    if data[4] != VERSION {
        return Err(SurgeError::Diff(format!(
            "unsupported chunked patch version: {}",
            data[4]
        )));
    }

    let chunk_size = u64::from_le_bytes(data[5..13].try_into().unwrap()) as usize;
    let old_size = u64::from_le_bytes(data[13..21].try_into().unwrap()) as usize;
    let new_size = u64::from_le_bytes(data[21..29].try_into().unwrap()) as usize;
    let num_chunks = u32::from_le_bytes(data[29..33].try_into().unwrap()) as usize;

    let mut offset = header_size;
    let mut chunks = Vec::with_capacity(num_chunks);

    for _ in 0..num_chunks {
        if offset + 8 > data.len() {
            return Err(SurgeError::Diff("patch truncated at chunk length".into()));
        }
        let patch_len = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;

        if offset + patch_len > data.len() {
            return Err(SurgeError::Diff("patch truncated at chunk data".into()));
        }
        chunks.push(&data[offset..offset + patch_len]);
        offset += patch_len;
    }

    Ok((old_size, new_size, chunk_size, chunks))
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
        let patch = chunked_bsdiff(&data, &data, &opts).unwrap();
        let reconstructed = chunked_bspatch(&data, &patch, &opts).unwrap();
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
        let patch = chunked_bsdiff(&old, &new, &opts).unwrap();
        let reconstructed = chunked_bspatch(&old, &patch, &opts).unwrap();
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
        let patch = chunked_bsdiff(&old, &new, &opts).unwrap();
        let reconstructed = chunked_bspatch(&old, &patch, &opts).unwrap();
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
        let patch = chunked_bsdiff(&old, &new, &opts).unwrap();
        let reconstructed = chunked_bspatch(&old, &patch, &opts).unwrap();
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
        let patch = chunked_bsdiff(&old, &new, &opts).unwrap();
        let reconstructed = chunked_bspatch(&old, &patch, &opts).unwrap();
        assert_eq!(reconstructed, new);
    }
}
