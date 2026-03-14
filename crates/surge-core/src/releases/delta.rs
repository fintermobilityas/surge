use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::archive::extractor::extract_to;
use crate::archive::packer::ArchivePacker;
use crate::config::constants::IO_CHUNK_SIZE;
use crate::crypto::sha256::sha256_hex_file;
use crate::diff::chunked::{
    ChunkedDiffOptions, chunked_bsdiff, chunked_bsdiff_files, chunked_bspatch, chunked_bspatch_file, has_magic_prefix,
};
use crate::diff::wrapper::{bsdiff_buffers, bspatch_buffers};
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    COMPRESSION_ZSTD, DIFF_ALGORITHM_BSDIFF, DIFF_ALGORITHM_FILE_OPS, DeltaArtifact, PATCH_FORMAT_BSDIFF4,
    PATCH_FORMAT_BSDIFF4_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_V1,
    PATCH_FORMAT_SPARSE_FILE_OPS_V1,
};

const LEGACY_ARCHIVE_BSDIFF_MAGIC: &[u8; 4] = b"ATB4";
const LEGACY_ARCHIVE_CHUNKED_MAGIC: &[u8; 4] = b"ATC4";
const ARCHIVE_BSDIFF_MAGIC: &[u8; 4] = b"ATB5";
const ARCHIVE_CHUNKED_MAGIC: &[u8; 4] = b"ATC5";
const SPARSE_FILE_OPS_MAGIC: &[u8; 4] = b"SFD1";
const ARCHIVE_PATCH_HEADER_LEN: usize = 12;
const LEGACY_ARCHIVE_PATCH_HEADER_LEN: usize = 8;
const SPARSE_FILE_OPS_HEADER_LEN: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq)]
enum TreeEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone)]
struct TreeEntry {
    source_path: PathBuf,
    kind: TreeEntryKind,
    mode: u32,
    symlink_target: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SparseFileDeltaManifest {
    compression_level: i32,
    zstd_workers: u32,
    ops: Vec<SparseFileOp>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SparseFileOp {
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

fn normalized_or_default<'a>(value: &'a str, default: &'a str) -> &'a str {
    let trimmed = value.trim();
    if trimmed.is_empty() { default } else { trimmed }
}

#[must_use]
pub fn has_archive_bsdiff_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(ARCHIVE_BSDIFF_MAGIC) || data.starts_with(LEGACY_ARCHIVE_BSDIFF_MAGIC)
}

#[must_use]
pub fn has_archive_chunked_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(ARCHIVE_CHUNKED_MAGIC) || data.starts_with(LEGACY_ARCHIVE_CHUNKED_MAGIC)
}

#[must_use]
pub fn has_sparse_file_ops_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(SPARSE_FILE_OPS_MAGIC)
}

#[must_use]
pub fn patch_format_from_magic_prefix(data: &[u8]) -> Option<&'static str> {
    if has_sparse_file_ops_magic_prefix(data) {
        return Some(PATCH_FORMAT_SPARSE_FILE_OPS_V1);
    }
    if has_archive_chunked_magic_prefix(data) {
        return Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3);
    }
    if has_archive_bsdiff_magic_prefix(data) {
        return Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3);
    }
    if has_magic_prefix(data) {
        return Some(PATCH_FORMAT_CHUNKED_BSDIFF_V1);
    }
    None
}

pub fn build_archive_bsdiff_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older_archive)?;
    let newer_tar = decode_archive_bytes(newer_archive)?;
    let patch = bsdiff_buffers(&older_tar, &newer_tar)?;
    Ok(encode_archive_patch_payload(
        *ARCHIVE_BSDIFF_MAGIC,
        compression_level,
        zstd_workers,
        &patch,
    ))
}

pub fn build_archive_chunked_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
    opts: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older_archive)?;
    let newer_tar = decode_archive_bytes(newer_archive)?;
    let patch = chunked_bsdiff(&older_tar, &newer_tar, opts)?;
    Ok(encode_archive_patch_payload(
        *ARCHIVE_CHUNKED_MAGIC,
        compression_level,
        zstd_workers,
        &patch,
    ))
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

fn encode_archive_patch_payload(magic: [u8; 4], compression_level: i32, zstd_workers: u32, patch: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(ARCHIVE_PATCH_HEADER_LEN + patch.len());
    payload.extend_from_slice(&magic);
    payload.extend_from_slice(&compression_level.to_le_bytes());
    payload.extend_from_slice(&zstd_workers.to_le_bytes());
    payload.extend_from_slice(patch);
    payload
}

fn decode_archive_patch_payload<'a>(
    data: &'a [u8],
    expected_magic: [u8; 4],
    legacy_magic: Option<[u8; 4]>,
    legacy_payload_magic: Option<&'static [u8]>,
) -> Result<(i32, u32, &'a [u8])> {
    if data.len() < LEGACY_ARCHIVE_PATCH_HEADER_LEN {
        return Err(SurgeError::Update("Archive delta payload is truncated".to_string()));
    }
    let matches_expected = data.starts_with(&expected_magic);
    let matches_legacy = legacy_magic.is_some_and(|magic| data.starts_with(&magic));
    if !matches_expected && !matches_legacy {
        return Err(SurgeError::Update("Archive delta payload magic is invalid".to_string()));
    }

    let compression_level = i32::from_le_bytes(
        data[expected_magic.len()..expected_magic.len() + std::mem::size_of::<i32>()]
            .try_into()
            .map_err(|_| SurgeError::Update("Archive delta payload header is invalid".to_string()))?,
    );

    if matches_legacy {
        let legacy_payload_offset = expected_magic.len() + std::mem::size_of::<i32>();
        if legacy_payload_magic.is_some_and(|magic| data[legacy_payload_offset..].starts_with(magic)) {
            return Ok((compression_level, 0, &data[legacy_payload_offset..]));
        }
    }

    if data.len() < ARCHIVE_PATCH_HEADER_LEN {
        return Err(SurgeError::Update(
            "Archive delta payload header is invalid".to_string(),
        ));
    }

    let worker_offset = expected_magic.len() + std::mem::size_of::<i32>();
    let zstd_workers = u32::from_le_bytes(
        data[worker_offset..ARCHIVE_PATCH_HEADER_LEN]
            .try_into()
            .map_err(|_| SurgeError::Update("Archive delta payload header is invalid".to_string()))?,
    );
    Ok((compression_level, zstd_workers, &data[ARCHIVE_PATCH_HEADER_LEN..]))
}

fn decode_archive_bytes(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("Failed to decode archive bytes: {e}")))
}

fn encode_archive_bytes(data: &[u8], compression_level: i32, zstd_workers: u32) -> Result<Vec<u8>> {
    if zstd_workers > 1 {
        let mut encoder = zstd::Encoder::new(Vec::new(), compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to create zstd encoder: {e}")))?;
        encoder
            .multithread(zstd_workers)
            .map_err(|e| SurgeError::Archive(format!("Failed to enable multi-threaded zstd: {e}")))?;
        encoder
            .write_all(data)
            .map_err(|e| SurgeError::Archive(format!("Failed to encode archive bytes: {e}")))?;
        return encoder
            .finish()
            .map_err(|e| SurgeError::Archive(format!("Failed to finalize zstd encoder: {e}")));
    }

    zstd::encode_all(data, compression_level)
        .map_err(|e| SurgeError::Archive(format!("Failed to encode archive bytes: {e}")))
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

#[must_use]
pub fn is_supported_delta(delta: &DeltaArtifact) -> bool {
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let compression = normalized_or_default(&delta.compression, COMPRESSION_ZSTD);
    let algorithm = delta.algorithm.trim();

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        return compression.eq_ignore_ascii_case(COMPRESSION_ZSTD)
            && (algorithm.is_empty() || algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS));
    }

    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);

    algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF)
        && compression.eq_ignore_ascii_case(COMPRESSION_ZSTD)
        && (patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3))
}

pub fn decode_delta_patch(data: &[u8], delta: &DeltaArtifact) -> Result<Vec<u8>> {
    let compression = normalized_or_default(&delta.compression, COMPRESSION_ZSTD);
    if compression.eq_ignore_ascii_case(COMPRESSION_ZSTD) {
        return zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("{e}")));
    }
    Err(SurgeError::Update(format!(
        "Unsupported delta compression '{}'",
        delta.compression
    )))
}

pub fn apply_delta_patch(older: &[u8], patch: &[u8], delta: &DeltaArtifact) -> Result<Vec<u8>> {
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let algorithm = delta.algorithm.trim();

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        if !algorithm.is_empty() && !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS) {
            return Err(SurgeError::Update(format!(
                "Unsupported delta algorithm/format '{}/{}'",
                delta.algorithm, delta.patch_format
            )));
        }
        return apply_sparse_file_patch(older, patch);
    }

    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);

    if !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF) {
        return Err(SurgeError::Update(format!(
            "Unsupported delta algorithm/format '{}/{}'",
            delta.algorithm, delta.patch_format
        )));
    }

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4) {
        return bspatch_buffers(older, patch);
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1) {
        return chunked_bspatch(older, patch, &ChunkedDiffOptions::default());
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3) {
        let older_tar = decode_archive_bytes(older)?;
        let (compression_level, zstd_workers, archive_patch) = decode_archive_patch_payload(
            patch,
            *ARCHIVE_BSDIFF_MAGIC,
            Some(*LEGACY_ARCHIVE_BSDIFF_MAGIC),
            Some(b"BSDIFF40"),
        )?;
        let newer_tar = bspatch_buffers(&older_tar, archive_patch)?;
        return encode_archive_bytes(&newer_tar, compression_level, zstd_workers);
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3) {
        let older_tar = decode_archive_bytes(older)?;
        let (compression_level, zstd_workers, archive_patch) = decode_archive_patch_payload(
            patch,
            *ARCHIVE_CHUNKED_MAGIC,
            Some(*LEGACY_ARCHIVE_CHUNKED_MAGIC),
            Some(b"CSDF"),
        )?;
        let newer_tar = chunked_bspatch(&older_tar, archive_patch, &ChunkedDiffOptions::default())?;
        return encode_archive_bytes(&newer_tar, compression_level, zstd_workers);
    }

    Err(SurgeError::Update(format!(
        "Unsupported delta algorithm/format '{}/{}'",
        delta.algorithm, delta.patch_format
    )))
}

fn apply_sparse_file_patch(older: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    let (manifest, payloads) = decode_sparse_file_ops_payload(patch)?;
    let working_dir = tempfile::tempdir()?;
    extract_to(older, working_dir.path(), None)?;
    apply_sparse_file_ops(working_dir.path(), &manifest.ops, payloads)?;

    let mut packer = if manifest.zstd_workers > 1 {
        ArchivePacker::with_threads(manifest.compression_level, manifest.zstd_workers)?
    } else {
        ArchivePacker::new(manifest.compression_level)?
    };
    packer.add_directory(working_dir.path(), "")?;
    packer.finalize()
}

fn apply_sparse_file_ops(root: &Path, ops: &[SparseFileOp], payloads: &[u8]) -> Result<()> {
    for op in ops {
        match op {
            SparseFileOp::Delete { path } => {
                let target = resolve_relative_path(root, path)?;
                remove_path_if_exists(&target)?;
            }
            SparseFileOp::EnsureDir { path, mode } => {
                let target = resolve_relative_path(root, path)?;
                fs::create_dir_all(&target)?;
                set_mode(&target, *mode)?;
            }
            SparseFileOp::SetMode { path, mode } => {
                let target = resolve_relative_path(root, path)?;
                set_mode(&target, *mode)?;
            }
            SparseFileOp::WriteFile {
                path,
                mode,
                payload_offset,
                payload_len,
                sha256,
            } => {
                let target = resolve_relative_path(root, path)?;
                let payload = payload_slice(payloads, *payload_offset, *payload_len)?;
                remove_path_if_exists(&target)?;
                if let Some(parent) = target.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&target, payload)?;
                set_mode(&target, *mode)?;
                verify_file_sha256(&target, sha256)?;
            }
            SparseFileOp::PatchFile {
                path,
                mode,
                payload_offset,
                payload_len,
                basis_sha256,
                sha256,
            } => {
                let target = resolve_relative_path(root, path)?;
                verify_file_sha256(&target, basis_sha256)?;
                let patch_bytes = payload_slice(payloads, *payload_offset, *payload_len)?;
                let temp_path = patched_temp_path(&target);
                if temp_path.exists() {
                    fs::remove_file(&temp_path)?;
                }
                chunked_bspatch_file(&target, patch_bytes, &temp_path)?;
                fs::remove_file(&target)?;
                fs::rename(&temp_path, &target)?;
                set_mode(&target, *mode)?;
                verify_file_sha256(&target, sha256)?;
            }
            SparseFileOp::WriteSymlink { path, target } => {
                let link_path = resolve_relative_path(root, path)?;
                remove_path_if_exists(&link_path)?;
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                create_symlink(target, &link_path)?;
            }
        }
    }
    Ok(())
}

fn collect_tree_entries(root: &Path) -> Result<BTreeMap<String, TreeEntry>> {
    let mut entries = BTreeMap::new();
    collect_tree_entries_recursive(root, root, &mut entries)?;
    Ok(entries)
}

fn collect_tree_entries_recursive(
    root: &Path,
    current: &Path,
    entries: &mut BTreeMap<String, TreeEntry>,
) -> Result<()> {
    let mut children = fs::read_dir(current)?.collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
    children.sort_by_key(std::fs::DirEntry::file_name);

    for child in children {
        let path = child.path();
        let metadata = fs::symlink_metadata(&path)?;
        let relative = normalize_relative_path(root, &path)?;
        let file_type = metadata.file_type();

        let entry = if file_type.is_dir() {
            TreeEntry {
                source_path: path.clone(),
                kind: TreeEntryKind::Directory,
                mode: normalized_mode(&metadata, true),
                symlink_target: None,
            }
        } else if file_type.is_symlink() {
            TreeEntry {
                source_path: path.clone(),
                kind: TreeEntryKind::Symlink,
                mode: 0,
                symlink_target: Some(fs::read_link(&path)?.to_string_lossy().replace('\\', "/")),
            }
        } else if file_type.is_file() {
            TreeEntry {
                source_path: path.clone(),
                kind: TreeEntryKind::File,
                mode: normalized_mode(&metadata, false),
                symlink_target: None,
            }
        } else {
            return Err(SurgeError::Archive(format!(
                "Unsupported filesystem entry while building sparse delta: {}",
                path.display()
            )));
        };
        entries.insert(relative.clone(), entry);

        if file_type.is_dir() {
            collect_tree_entries_recursive(root, &path, entries)?;
        }
    }

    Ok(())
}

fn normalize_relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|e| SurgeError::Archive(format!("Failed to strip archive root '{}': {e}", path.display())))?;
    let mut normalized = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            _ => {
                return Err(SurgeError::Archive(format!(
                    "Invalid archive path while building sparse delta: {}",
                    relative.display()
                )));
            }
        }
    }
    Ok(normalized.to_string_lossy().replace('\\', "/"))
}

fn resolve_relative_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let mut resolved = PathBuf::from(root);
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(segment) => resolved.push(segment),
            _ => {
                return Err(SurgeError::Update(format!("Invalid sparse delta path '{relative}'")));
            }
        }
    }
    Ok(resolved)
}

fn append_payload(buffer: &mut Vec<u8>, payload: &[u8]) -> Result<(u64, u64)> {
    let offset = u64::try_from(buffer.len())
        .map_err(|_| SurgeError::Archive("Sparse delta payload exceeds supported size".to_string()))?;
    let len = u64::try_from(payload.len())
        .map_err(|_| SurgeError::Archive("Sparse delta payload exceeds supported size".to_string()))?;
    buffer.extend_from_slice(payload);
    Ok((offset, len))
}

fn payload_slice(payloads: &[u8], offset: u64, len: u64) -> Result<&[u8]> {
    let start = usize::try_from(offset)
        .map_err(|_| SurgeError::Update("Sparse delta payload offset exceeds platform limits".to_string()))?;
    let len = usize::try_from(len)
        .map_err(|_| SurgeError::Update("Sparse delta payload length exceeds platform limits".to_string()))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| SurgeError::Update("Sparse delta payload range overflows".to_string()))?;
    payloads
        .get(start..end)
        .ok_or_else(|| SurgeError::Update("Sparse delta payload range is invalid".to_string()))
}

fn files_identical(left: &Path, right: &Path) -> Result<bool> {
    let left_len = fs::metadata(left)?.len();
    let right_len = fs::metadata(right)?.len();
    if left_len != right_len {
        return Ok(false);
    }

    let mut left_file = fs::File::open(left)?;
    let mut right_file = fs::File::open(right)?;
    let mut left_buf = vec![0u8; IO_CHUNK_SIZE];
    let mut right_buf = vec![0u8; IO_CHUNK_SIZE];

    loop {
        let left_read = left_file.read(&mut left_buf)?;
        let right_read = right_file.read(&mut right_buf)?;
        if left_read != right_read {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
        if left_buf[..left_read] != right_buf[..right_read] {
            return Ok(false);
        }
    }
}

fn verify_file_sha256(path: &Path, expected_sha256: &str) -> Result<()> {
    let expected = expected_sha256.trim();
    if expected.is_empty() {
        return Ok(());
    }
    let actual = sha256_hex_file(path)?;
    if actual != expected {
        return Err(SurgeError::Update(format!(
            "Sparse delta file hash mismatch for '{}': expected {expected}, got {actual}",
            path.display()
        )));
    }
    Ok(())
}

fn remove_path_if_exists(path: &Path) -> Result<()> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

fn patched_temp_path(target: &Path) -> PathBuf {
    let file_name = target
        .file_name()
        .map_or_else(|| "patched".to_string(), |name| name.to_string_lossy().into_owned());
    target.with_file_name(format!(".{file_name}.surge-patch"))
}

fn path_depth(path: &str) -> usize {
    Path::new(path).components().count()
}

#[cfg(unix)]
fn normalized_mode(metadata: &fs::Metadata, is_dir: bool) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    let mode = metadata.permissions().mode() & 0o777;
    if mode == 0 {
        if is_dir { 0o755 } else { 0o644 }
    } else {
        mode
    }
}

#[cfg(not(unix))]
fn normalized_mode(_metadata: &fs::Metadata, is_dir: bool) -> u32 {
    if is_dir { 0o755 } else { 0o644 }
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &str, link_path: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link_path)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink(target: &str, link_path: &Path) -> Result<()> {
    std::os::windows::fs::symlink_file(target, link_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::crypto::sha256::sha256_hex;
    use crate::releases::manifest::DeltaArtifact;

    fn make_archive(version: &str, compression_level: i32, zstd_workers: u32) -> Vec<u8> {
        let mut packer = if zstd_workers > 1 {
            ArchivePacker::with_threads(compression_level, zstd_workers).unwrap()
        } else {
            ArchivePacker::new(compression_level).unwrap()
        };
        let banner = format!("console write for {version}\n");
        packer.add_buffer("Program.cs", banner.as_bytes(), 0o644).unwrap();
        packer
            .add_buffer("demoapp.csproj", b"<Project Sdk=\"Microsoft.NET.Sdk\" />\n", 0o644)
            .unwrap();
        packer
            .add_buffer("assets/payload.bin", &vec![b'Z'; 8 * 1024 * 1024], 0o644)
            .unwrap();
        packer
            .add_buffer("assets/aux.bin", &vec![b'Q'; 4 * 1024 * 1024], 0o644)
            .unwrap();
        packer.finalize().unwrap()
    }

    #[test]
    fn test_patch_format_from_magic_prefix_detects_archive_formats() {
        assert_eq!(
            patch_format_from_magic_prefix(SPARSE_FILE_OPS_MAGIC),
            Some(PATCH_FORMAT_SPARSE_FILE_OPS_V1)
        );
        assert_eq!(
            patch_format_from_magic_prefix(LEGACY_ARCHIVE_BSDIFF_MAGIC),
            Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3)
        );
        assert_eq!(
            patch_format_from_magic_prefix(ARCHIVE_BSDIFF_MAGIC),
            Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3)
        );
        assert_eq!(
            patch_format_from_magic_prefix(LEGACY_ARCHIVE_CHUNKED_MAGIC),
            Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3)
        );
        assert_eq!(
            patch_format_from_magic_prefix(ARCHIVE_CHUNKED_MAGIC),
            Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3)
        );
    }

    #[test]
    fn test_archive_bsdiff_patch_roundtrip_rebuilds_full_archive_bytes() {
        let zstd_workers = 4;
        let full_v1 = make_archive("1.0.0", 7, zstd_workers);
        let full_v2 = make_archive("1.1.0", 7, zstd_workers);
        let patch = build_archive_bsdiff_patch(&full_v1, &full_v2, 7, zstd_workers).unwrap();
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::bsdiff_archive_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }

    #[test]
    fn test_legacy_archive_bsdiff_patch_magic_roundtrip_rebuilds_full_archive_bytes() {
        let zstd_workers = 4;
        let full_v1 = make_archive("1.0.0", 7, zstd_workers);
        let full_v2 = make_archive("1.1.0", 7, zstd_workers);
        let mut patch = build_archive_bsdiff_patch(&full_v1, &full_v2, 7, zstd_workers).unwrap();
        patch[..LEGACY_ARCHIVE_BSDIFF_MAGIC.len()].copy_from_slice(LEGACY_ARCHIVE_BSDIFF_MAGIC);
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::with_patch_format(
            "primary",
            "1.0.0",
            PATCH_FORMAT_BSDIFF4_ARCHIVE_V3,
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }

    #[test]
    fn test_archive_chunked_patch_roundtrip_rebuilds_full_archive_bytes() {
        let zstd_workers = 4;
        let full_v1 = make_archive("1.0.0", 11, zstd_workers);
        let full_v2 = make_archive("1.1.0", 11, zstd_workers);
        let patch =
            build_archive_chunked_patch(&full_v1, &full_v2, 11, zstd_workers, &ChunkedDiffOptions::default()).unwrap();
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }

    #[test]
    fn test_legacy_archive_chunked_patch_magic_roundtrip_rebuilds_full_archive_bytes() {
        let zstd_workers = 4;
        let full_v1 = make_archive("1.0.0", 11, zstd_workers);
        let full_v2 = make_archive("1.1.0", 11, zstd_workers);
        let mut patch =
            build_archive_chunked_patch(&full_v1, &full_v2, 11, zstd_workers, &ChunkedDiffOptions::default()).unwrap();
        patch[..LEGACY_ARCHIVE_CHUNKED_MAGIC.len()].copy_from_slice(LEGACY_ARCHIVE_CHUNKED_MAGIC);
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::with_patch_format(
            "primary",
            "1.0.0",
            PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3,
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }

    #[test]
    fn test_archive_patch_payload_rejects_invalid_magic() {
        let err =
            decode_archive_patch_payload(b"BAD!\x03\0\0\0payload", *ARCHIVE_BSDIFF_MAGIC, None, None).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn test_sparse_file_patch_roundtrip_rebuilds_full_archive_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let old_dir = dir.path().join("old");
        let new_dir = dir.path().join("new");
        std::fs::create_dir_all(old_dir.join("bin")).unwrap();
        std::fs::create_dir_all(new_dir.join("bin")).unwrap();
        std::fs::create_dir_all(new_dir.join("models")).unwrap();
        std::fs::write(old_dir.join("bin").join("runtime.bin"), vec![b'A'; 512 * 1024]).unwrap();
        std::fs::write(old_dir.join("config.json"), br#"{"version":1}"#).unwrap();
        std::fs::write(new_dir.join("bin").join("runtime.bin"), {
            let mut bytes = vec![b'A'; 512 * 1024];
            bytes[1234] = b'B';
            bytes
        })
        .unwrap();
        std::fs::write(new_dir.join("config.json"), br#"{"version":2}"#).unwrap();
        std::fs::write(new_dir.join("models").join("model-v2.bin"), vec![b'Z'; 512 * 1024]).unwrap();

        let mut old_packer = ArchivePacker::new(7).unwrap();
        old_packer.add_directory(&old_dir, "").unwrap();
        let full_v1 = old_packer.finalize().unwrap();

        let mut new_packer = ArchivePacker::new(7).unwrap();
        new_packer.add_directory(&new_dir, "").unwrap();
        let full_v2 = new_packer.finalize().unwrap();

        let patch = build_sparse_file_patch(
            &full_v1,
            &full_v2,
            7,
            0,
            &ChunkedDiffOptions {
                chunk_size: 128 * 1024,
                max_threads: 1,
            },
        )
        .unwrap();
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::sparse_file_ops_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }
}
