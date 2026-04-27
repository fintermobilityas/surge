use std::fs;

use serde::{Deserialize, Serialize};

use crate::archive::extractor::extract_to;
use crate::archive::packer::ArchivePacker;
use crate::crypto::sha256::sha256_hex_file;
use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff_files};
use crate::error::{Result, SurgeError};

use super::fs_apply::{apply_sparse_file_ops_with_progress, sparse_file_ops_work_units};
use super::tree::{TreeEntryKind, collect_tree_entries, files_identical};
use super::{DeltaApplyProgress, DeltaApplyProgressCallback};

pub(super) const SPARSE_FILE_OPS_MAGIC: &[u8; 4] = b"SFD1";

const SPARSE_FILE_OPS_HEADER_LEN: usize = 12;

#[derive(Debug, Serialize, Deserialize)]
struct SparseFileDeltaManifest {
    compression_level: i32,
    zstd_workers: u32,
    ops: Vec<SparseFileOp>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum SparseFileOp {
    Delete {
        path: String,
    },
    EnsureDir {
        path: String,
        mode: u32,
    },
    SetMode {
        path: String,
        mode: u32,
    },
    WriteFile {
        path: String,
        mode: u32,
        payload_offset: u64,
        payload_len: u64,
        sha256: String,
    },
    PatchFile {
        path: String,
        mode: u32,
        payload_offset: u64,
        payload_len: u64,
        basis_sha256: String,
        sha256: String,
    },
    WriteSymlink {
        path: String,
        target: String,
    },
}

pub fn build_sparse_file_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
    diff_options: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    let older_dir = tempfile::tempdir()?;
    let newer_dir = tempfile::tempdir()?;
    extract_to(older_archive, older_dir.path(), None)?;
    extract_to(newer_archive, newer_dir.path(), None)?;

    let older_tree = collect_tree_entries(older_dir.path())?;
    let newer_tree = collect_tree_entries(newer_dir.path())?;

    let mut ops = Vec::new();
    let mut payloads = Vec::new();

    let mut delete_paths: Vec<&String> = older_tree.keys().collect();
    delete_paths.sort_by(|left, right| path_depth(right).cmp(&path_depth(left)).then_with(|| right.cmp(left)));
    for path in delete_paths {
        if newer_tree
            .get(path)
            .is_none_or(|newer| newer.kind != older_tree[path].kind)
        {
            ops.push(SparseFileOp::Delete { path: path.clone() });
        }
    }

    let mut new_paths: Vec<&String> = newer_tree.keys().collect();
    new_paths.sort();
    for path in new_paths {
        let newer = &newer_tree[path];
        let older = older_tree.get(path);
        match newer.kind {
            TreeEntryKind::Directory => {
                if older.is_none_or(|entry| entry.kind != TreeEntryKind::Directory || entry.mode != newer.mode) {
                    ops.push(SparseFileOp::EnsureDir {
                        path: path.clone(),
                        mode: newer.mode,
                    });
                }
            }
            TreeEntryKind::Symlink => {
                if older.is_none_or(|entry| {
                    entry.kind != TreeEntryKind::Symlink || entry.symlink_target != newer.symlink_target
                }) {
                    ops.push(SparseFileOp::WriteSymlink {
                        path: path.clone(),
                        target: newer.symlink_target.clone().unwrap_or_default(),
                    });
                }
            }
            TreeEntryKind::File => {
                if let Some(older) = older
                    && older.kind == TreeEntryKind::File
                    && files_identical(&older.source_path, &newer.source_path)?
                {
                    if older.mode != newer.mode {
                        ops.push(SparseFileOp::SetMode {
                            path: path.clone(),
                            mode: newer.mode,
                        });
                    }
                    continue;
                }

                let new_sha256 = sha256_hex_file(&newer.source_path)?;
                let raw_len = usize::try_from(fs::metadata(&newer.source_path)?.len())
                    .map_err(|_| SurgeError::Archive("Updated file exceeds platform limits".to_string()))?;
                let use_patch = if let Some(older) = older {
                    if older.kind == TreeEntryKind::File {
                        let patch = chunked_bsdiff_files(&older.source_path, &newer.source_path, diff_options)?;
                        if patch.len() < raw_len {
                            let (payload_offset, payload_len) = append_payload(&mut payloads, &patch)?;
                            ops.push(SparseFileOp::PatchFile {
                                path: path.clone(),
                                mode: newer.mode,
                                payload_offset,
                                payload_len,
                                basis_sha256: sha256_hex_file(&older.source_path)?,
                                sha256: new_sha256.clone(),
                            });
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                };

                if !use_patch {
                    let raw_payload = fs::read(&newer.source_path)?;
                    let (payload_offset, payload_len) = append_payload(&mut payloads, &raw_payload)?;
                    ops.push(SparseFileOp::WriteFile {
                        path: path.clone(),
                        mode: newer.mode,
                        payload_offset,
                        payload_len,
                        sha256: new_sha256,
                    });
                }
            }
        }
    }

    encode_sparse_file_ops_payload(
        &SparseFileDeltaManifest {
            compression_level,
            zstd_workers,
            ops,
        },
        &payloads,
    )
}

pub(super) fn sparse_file_patch_archive_encoding(patch: &[u8]) -> Result<(i32, u32)> {
    let (manifest, _) = decode_sparse_file_ops_payload(patch)?;
    Ok((manifest.compression_level, manifest.zstd_workers))
}

pub(super) fn apply_sparse_file_patch_with_progress(
    older: &[u8],
    patch: &[u8],
    progress: Option<&DeltaApplyProgressCallback<'_>>,
) -> Result<Vec<u8>> {
    let (manifest, payloads) = decode_sparse_file_ops_payload(patch)?;
    let working_dir = tempfile::tempdir()?;

    let extract_units = usize_to_u64_saturating(older.len()).max(1);
    let ops_units = sparse_file_ops_work_units(&manifest.ops);
    let repack_units = extract_units
        .saturating_add(usize_to_u64_saturating(payloads.len()))
        .max(1);
    let total_units = extract_units.saturating_add(ops_units).saturating_add(repack_units);

    emit_progress(progress, 0, total_units);

    let extract_progress = |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
        let units_done = if bytes_total > 0 {
            scale_units(extract_units, bytes_done, bytes_total)
        } else {
            scale_units(extract_units, items_done, items_total)
        };
        emit_progress(progress, units_done, total_units);
    };
    extract_to(
        older,
        working_dir.path(),
        progress.map(|_| &extract_progress as &crate::archive::extractor::ExtractProgress<'_>),
    )?;
    emit_progress(progress, extract_units, total_units);

    let ops_progress = |done: u64, total: u64| {
        emit_progress(
            progress,
            extract_units.saturating_add(scale_units(ops_units, done, total)),
            total_units,
        );
    };
    apply_sparse_file_ops_with_progress(
        working_dir.path(),
        &manifest.ops,
        payloads,
        progress.map(|_| &ops_progress as &super::fs_apply::SparseOpProgress<'_>),
    )?;
    let repack_start_units = extract_units.saturating_add(ops_units);
    emit_progress(progress, repack_start_units, total_units);

    let mut packer = if manifest.zstd_workers > 1 {
        ArchivePacker::with_threads(manifest.compression_level, manifest.zstd_workers)?
    } else {
        ArchivePacker::new(manifest.compression_level)?
    };

    let add_directory_units = repack_units.saturating_mul(9) / 10;
    let repack_progress = |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
        let units_done = if bytes_total > 0 {
            scale_units(add_directory_units, bytes_done, bytes_total)
        } else {
            scale_units(add_directory_units, items_done, items_total)
        };
        emit_progress(progress, repack_start_units.saturating_add(units_done), total_units);
    };
    packer.add_directory_with_progress(
        working_dir.path(),
        "",
        progress.map(|_| &repack_progress as &crate::archive::packer::PackProgress<'_>),
    )?;
    emit_progress(
        progress,
        repack_start_units.saturating_add(add_directory_units),
        total_units,
    );

    let rebuilt = packer.finalize()?;
    emit_progress(progress, total_units, total_units);
    Ok(rebuilt)
}

fn encode_sparse_file_ops_payload(manifest: &SparseFileDeltaManifest, payloads: &[u8]) -> Result<Vec<u8>> {
    let manifest_bytes = serde_json::to_vec(manifest)?;
    let manifest_len = u64::try_from(manifest_bytes.len())
        .map_err(|_| SurgeError::Archive("Sparse delta manifest exceeds supported size".to_string()))?;
    let mut encoded = Vec::with_capacity(SPARSE_FILE_OPS_HEADER_LEN + manifest_bytes.len() + payloads.len());
    encoded.extend_from_slice(SPARSE_FILE_OPS_MAGIC);
    encoded.extend_from_slice(&manifest_len.to_le_bytes());
    encoded.extend_from_slice(&manifest_bytes);
    encoded.extend_from_slice(payloads);
    Ok(encoded)
}

fn decode_sparse_file_ops_payload(data: &[u8]) -> Result<(SparseFileDeltaManifest, &[u8])> {
    if data.len() < SPARSE_FILE_OPS_HEADER_LEN {
        return Err(SurgeError::Update("Sparse delta payload is truncated".to_string()));
    }
    if !data.starts_with(SPARSE_FILE_OPS_MAGIC) {
        return Err(SurgeError::Update("Sparse delta payload magic is invalid".to_string()));
    }
    let manifest_len = u64::from_le_bytes(
        data[SPARSE_FILE_OPS_MAGIC.len()..SPARSE_FILE_OPS_HEADER_LEN]
            .try_into()
            .map_err(|_| SurgeError::Update("Sparse delta payload header is invalid".to_string()))?,
    );
    let manifest_len = usize::try_from(manifest_len)
        .map_err(|_| SurgeError::Update("Sparse delta manifest exceeds platform limits".to_string()))?;
    let manifest_end = SPARSE_FILE_OPS_HEADER_LEN.saturating_add(manifest_len);
    if manifest_end > data.len() {
        return Err(SurgeError::Update("Sparse delta manifest is truncated".to_string()));
    }
    let manifest: SparseFileDeltaManifest = serde_json::from_slice(&data[SPARSE_FILE_OPS_HEADER_LEN..manifest_end])?;
    Ok((manifest, &data[manifest_end..]))
}

fn append_payload(buffer: &mut Vec<u8>, payload: &[u8]) -> Result<(u64, u64)> {
    let offset = u64::try_from(buffer.len())
        .map_err(|_| SurgeError::Archive("Sparse delta payload exceeds supported size".to_string()))?;
    let len = u64::try_from(payload.len())
        .map_err(|_| SurgeError::Archive("Sparse delta payload exceeds supported size".to_string()))?;
    buffer.extend_from_slice(payload);
    Ok((offset, len))
}

fn path_depth(path: &str) -> usize {
    std::path::Path::new(path).components().count()
}

fn emit_progress(progress: Option<&DeltaApplyProgressCallback<'_>>, units_done: u64, units_total: u64) {
    if let Some(cb) = progress {
        cb(DeltaApplyProgress {
            units_done: units_done.min(units_total),
            units_total,
        });
    }
}

fn scale_units(units: u64, done: u64, total: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    units.saturating_mul(done.min(total)) / total
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
