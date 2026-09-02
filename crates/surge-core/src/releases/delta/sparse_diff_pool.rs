//! Bounded worker pool for the sparse delta build's per-file pipelines.
//!
//! Each changed file needs three independent CPU-bound passes (newer
//! SHA-256, basis SHA-256, chunked diff). Files run across a bounded pool
//! with the thread budget split evenly; the chunked diff output is
//! independent of its thread count (per-chunk results are serialized by
//! chunk index), so the patch bytes stay identical to the single-file
//! build.

use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff};
use crate::error::{Result, SurgeError};

use super::sparse_ops::SparseFileOp;

/// Maximum number of changed-file pipelines to run in parallel. Each
/// pipeline is the three independent CPU passes over one file (newer
/// hash, basis hash, chunked diff) with the thread budget split evenly,
/// so the cap keeps a single large file on the full budget and avoids
/// oversubscription on many-file changes.
pub(super) const MAX_PARALLEL_FILE_PASSES: usize = 4;

pub(super) enum FileWork<'a> {
    Changed {
        mode: u32,
        newer: &'a [u8],
        older: &'a [u8],
    },
    New {
        mode: u32,
        content: &'a [u8],
    },
}

pub(super) struct PathItem<'a> {
    pub(super) path: String,
    pub(super) immediate: Option<SparseFileOp>,
    pub(super) file_work: Option<FileWork<'a>>,
}

#[derive(Debug)]
pub(super) enum FileWorkResult {
    WriteFile {
        payload: Vec<u8>,
        sha256: String,
    },
    PatchFile {
        payload: Vec<u8>,
        basis_sha256: String,
        sha256: String,
    },
}

pub(super) fn work_mode(work: &FileWork<'_>) -> u32 {
    match work {
        FileWork::Changed { mode, .. } | FileWork::New { mode, .. } => *mode,
    }
}

pub(super) fn file_work_pipeline(work: &FileWork<'_>, diff_options: &ChunkedDiffOptions) -> Result<FileWorkResult> {
    match work {
        FileWork::New { content, .. } => {
            let sha256 = sha256_hex(content);
            Ok(FileWorkResult::WriteFile {
                payload: content.to_vec(),
                sha256,
            })
        }
        FileWork::Changed { newer, older, .. } => {
            // Three independent CPU-bound passes over the same file: both
            // hashes on workers while the chunked diff runs on this thread.
            let (new_sha256, basis_sha256, patch) = std::thread::scope(|s| -> Result<(String, String, Vec<u8>)> {
                let newer_handle = s.spawn(|| sha256_hex(newer));
                let basis_handle = s.spawn(|| sha256_hex(older));
                let patch = chunked_bsdiff(older, newer, diff_options)?;
                let new_sha256 = newer_handle
                    .join()
                    .map_err(|_| SurgeError::Archive("Hash worker panicked".to_string()))?;
                let basis_sha256 = basis_handle
                    .join()
                    .map_err(|_| SurgeError::Archive("Hash worker panicked".to_string()))?;
                Ok((new_sha256, basis_sha256, patch))
            })?;
            if patch.len() < newer.len() {
                Ok(FileWorkResult::PatchFile {
                    payload: patch,
                    basis_sha256,
                    sha256: new_sha256,
                })
            } else {
                Ok(FileWorkResult::WriteFile {
                    payload: newer.to_vec(),
                    sha256: new_sha256,
                })
            }
        }
    }
}
