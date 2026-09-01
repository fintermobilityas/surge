//! In-memory sparse delta construction: compare two packed archives without
//! extracting them to disk. Entry metadata comes from the tar headers and
//! file contents are held as slices of the decompressed tar, so the
//! publisher-side delta build avoids two full tree extractions and the
//! on-disk byte-by-byte comparison.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Component, PathBuf};

use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff};
use crate::error::{Result, SurgeError};

use super::sparse_ops::{
    SparseFileDeltaManifest, SparseFileOp, append_payload, encode_sparse_file_ops_payload, path_depth,
};
use super::tree::TreeEntryKind;

struct MemEntry<'a> {
    kind: TreeEntryKind,
    mode: u32,
    symlink_target: Option<String>,
    /// Zero-copy borrow into the decoded tar buffer. A desync between the
    /// iterator and the block-position tracking would make this slice point
    /// at the wrong bytes, but that cannot go silent: every file op carries a
    /// SHA-256 of this content, which the apply side re-verifies fail-closed.
    content: &'a [u8],
}

fn decode_tar(archive: &[u8]) -> Result<Vec<u8>> {
    let mut decoder =
        zstd::Decoder::new(archive).map_err(|e| SurgeError::Archive(format!("Failed to create zstd decoder: {e}")))?;
    let mut tar_bytes = Vec::new();
    decoder
        .read_to_end(&mut tar_bytes)
        .map_err(|e| SurgeError::Archive(format!("Failed to decompress archive: {e}")))?;
    Ok(tar_bytes)
}

fn collect_tree_entries_in_memory(tar_bytes: &[u8]) -> Result<BTreeMap<String, MemEntry<'_>>> {
    let mut entries = BTreeMap::new();
    let mut archive = tar::Archive::new(std::io::Cursor::new(tar_bytes));
    // Tar is a sequence of 512-byte header blocks followed by data rounded up
    // to 512-byte blocks. Track the header offset manually so file contents
    // can be borrowed from `tar_bytes` instead of copied.
    let mut block_pos = 0usize;
    for entry in archive
        .entries()
        .map_err(|e| SurgeError::Archive(format!("Failed to read archive entries: {e}")))?
    {
        let entry = entry.map_err(|e| SurgeError::Archive(format!("Bad archive entry: {e}")))?;
        let size = usize::try_from(entry.size())
            .map_err(|_| SurgeError::Archive("Archive entry size exceeds supported limits".into()))?;
        let data_start = block_pos + 512;
        block_pos = data_start + size + (512 - (size % 512)) % 512;
        let relative = normalize_entry_path(entry.path()?.as_ref())?;
        if relative.is_empty() {
            continue; // archive root
        }
        let entry_type = entry.header().entry_type();
        // Long-name / long-link / PAX extension entries carry no payload of
        // their own; the iterator folds them into the following entry.
        if entry_type.is_gnu_longname()
            || entry_type.is_gnu_longlink()
            || entry_type.is_pax_local_extensions()
            || entry_type.is_pax_global_extensions()
        {
            continue;
        }
        let mode = entry.header().mode()?;
        let symlink_target = entry
            .header()
            .link_name()?
            .map(|target| target.to_string_lossy().replace('\\', "/"));
        let content = if entry_type.is_file() {
            let end = data_start + size;
            if end > tar_bytes.len() {
                return Err(SurgeError::Archive(
                    "Truncated archive entry while building sparse delta".into(),
                ));
            }
            &tar_bytes[data_start..end]
        } else {
            &[]
        };
        let entry = if entry_type.is_dir() {
            MemEntry {
                kind: TreeEntryKind::Directory,
                mode: normalized_header_mode(mode, true),
                symlink_target: None,
                content,
            }
        } else if entry_type.is_symlink() {
            MemEntry {
                kind: TreeEntryKind::Symlink,
                mode: 0,
                symlink_target,
                content,
            }
        } else if entry_type.is_file() {
            MemEntry {
                kind: TreeEntryKind::File,
                mode: normalized_header_mode(mode, false),
                symlink_target: None,
                content,
            }
        } else {
            return Err(SurgeError::Archive(format!(
                "Unsupported archive entry while building sparse delta: {relative}"
            )));
        };
        entries.insert(relative, entry);
    }
    Ok(entries)
}

/// Mirror the disk-based `collect_tree_entries` normalization: zero modes
/// fall back to the conventional defaults, matching what an extracted tree
/// would report through the filesystem.
fn normalized_header_mode(mode: u32, is_dir: bool) -> u32 {
    if mode == 0 {
        if is_dir { 0o755 } else { 0o644 }
    } else {
        mode
    }
}

fn normalize_entry_path(path: &std::path::Path) -> Result<String> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            _ => {
                return Err(SurgeError::Archive(format!(
                    "Invalid archive path while building sparse delta: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(normalized.to_string_lossy().replace('\\', "/"))
}

pub(super) fn build_sparse_file_patch_in_memory(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
    diff_options: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    // Decode both archives concurrently: the zstd decode is single-threaded
    // CPU work, and the two archives are independent.
    let (older_tar, newer_tar) = std::thread::scope(|s| -> Result<(Vec<u8>, Vec<u8>)> {
        let h_old = s.spawn(|| decode_tar(older_archive));
        let h_new = s.spawn(|| decode_tar(newer_archive));
        let older_tar = h_old
            .join()
            .map_err(|_| SurgeError::Archive("Archive decode worker panicked".to_string()))??;
        let newer_tar = h_new
            .join()
            .map_err(|_| SurgeError::Archive("Archive decode worker panicked".to_string()))??;
        Ok((older_tar, newer_tar))
    })?;
    let older_tree = collect_tree_entries_in_memory(&older_tar)?;
    let newer_tree = collect_tree_entries_in_memory(&newer_tar)?;

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
                let identical = older
                    .filter(|entry| entry.kind == TreeEntryKind::File)
                    .is_some_and(|entry| entry.content == newer.content);
                if identical {
                    if let Some(older) = older
                        && older.mode != newer.mode
                    {
                        ops.push(SparseFileOp::SetMode {
                            path: path.clone(),
                            mode: newer.mode,
                        });
                    }
                    continue;
                }

                let raw_len = newer.content.len();
                let new_sha256 = sha256_hex(newer.content);
                let use_patch = if let Some(older) = older
                    && older.kind == TreeEntryKind::File
                {
                    // Hash the basis while the chunked diff runs on this
                    // thread: both are CPU-bound passes over the same file.
                    let (patch, basis_sha256) = std::thread::scope(|s| -> Result<(Vec<u8>, String)> {
                        let basis_handle = s.spawn(|| sha256_hex(older.content));
                        let patch = chunked_bsdiff(older.content, newer.content, diff_options)?;
                        let basis_sha256 = basis_handle
                            .join()
                            .map_err(|_| SurgeError::Archive("Basis hash worker panicked".to_string()))?;
                        Ok((patch, basis_sha256))
                    })?;
                    if patch.len() < raw_len {
                        let (payload_offset, payload_len) = append_payload(&mut payloads, &patch)?;
                        ops.push(SparseFileOp::PatchFile {
                            path: path.clone(),
                            mode: newer.mode,
                            payload_offset,
                            payload_len,
                            basis_sha256,
                            sha256: new_sha256.clone(),
                        });
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                if !use_patch {
                    let (payload_offset, payload_len) = append_payload(&mut payloads, newer.content)?;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn pack_tree(entries: &[(&str, Vec<u8>, u32, bool)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, data, mode, is_dir) in entries {
            let mut header = tar::Header::new_gnu();
            if *is_dir {
                header.set_entry_type(tar::EntryType::Directory);
                header.set_size(0);
            } else {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_size(u64::try_from(data.len()).expect("size fits"));
            }
            header.set_mode(*mode);
            header.set_cksum();
            let data_ref: &[u8] = if *is_dir { &[] } else { data.as_slice() };
            builder.append_data(&mut header, path, data_ref).expect("append entry");
        }
        let tar_bytes = builder.into_inner().expect("tar bytes");
        zstd::encode_all(std::io::Cursor::new(tar_bytes), 3).expect("zstd")
    }

    fn entry_map(entries: &[(&str, Vec<u8>, u32, bool)]) -> BTreeMap<String, (TreeEntryKind, u32)> {
        entries
            .iter()
            .map(|(path, _, mode, is_dir)| {
                (
                    path.to_string(),
                    (
                        if *is_dir {
                            TreeEntryKind::Directory
                        } else {
                            TreeEntryKind::File
                        },
                        *mode,
                    ),
                )
            })
            .collect()
    }

    #[test]
    fn in_memory_tree_collect_matches_expected_metadata() {
        let archive = pack_tree(&[
            ("bin", vec![], 0o755, true),
            ("bin/tool", vec![1, 2, 3], 0o755, false),
            ("readme", vec![b'x'], 0o644, false),
        ]);
        let tar_bytes = decode_tar(&archive).expect("decode");
        let tree = collect_tree_entries_in_memory(&tar_bytes).expect("collect");
        assert_eq!(tree.len(), 3);
        let expected = entry_map(&[
            ("bin", vec![], 0o755, true),
            ("bin/tool", vec![1, 2, 3], 0o755, false),
            ("readme", vec![b'x'], 0o644, false),
        ]);
        for (path, (kind, mode)) in &expected {
            let entry = &tree[path];
            assert_eq!(entry.kind, *kind, "kind for {path}");
            assert_eq!(entry.mode, *mode, "mode for {path}");
        }
        assert_eq!(tree["bin/tool"].content, &[1, 2, 3]);
    }
}
