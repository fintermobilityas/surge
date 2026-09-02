//! In-memory sparse delta construction: compare two packed archives without
//! extracting them to disk. Entry metadata comes from the tar headers and
//! file contents are borrowed as slices of the decompressed tar, so the
//! publisher-side delta build avoids two full tree extractions and the
//! on-disk byte-by-byte comparison.
//!
//! Consecutive chain steps share a tree (step N+1's older archive is step N's
//! newer archive), so a decoded tree can be carried across builds through
//! [`SparseTreeReuse`]; the archive SHA-256 makes a stale reuse fail closed
//! to a cold decode instead of building a wrong patch.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Component, PathBuf};

#[cfg(test)]
use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::ChunkedDiffOptions;
use crate::error::{Result, SurgeError};

use super::sparse_diff_pool::{
    FileWork, FileWorkResult, MAX_PARALLEL_FILE_PASSES, PathItem, file_work_pipeline, work_mode,
};
use super::sparse_ops::{
    SparseFileDeltaManifest, SparseFileOp, append_payload, encode_sparse_file_ops_payload, path_depth,
};
use super::tree::TreeEntryKind;

/// One entry of a decoded sparse tree. File contents are referenced by
/// offset into the decoded tar buffer instead of copied or borrowed, so the
/// tree can outlive the function that collected it.
#[derive(Debug)]
pub(super) struct SparseTreeEntry {
    pub(super) kind: TreeEntryKind,
    pub(super) mode: u32,
    pub(super) symlink_target: Option<String>,
    pub(super) data_start: usize,
    pub(super) content_len: usize,
}

/// Borrowed view over one decoded tree: the tar buffer plus the entry map.
pub(super) struct SparseTree<'a> {
    pub(super) buffer: &'a [u8],
    pub(super) entries: &'a BTreeMap<String, SparseTreeEntry>,
}

impl SparseTree<'_> {
    fn get(&self, path: &str) -> Option<&SparseTreeEntry> {
        self.entries.get(path)
    }

    fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.keys()
    }

    fn iter(&self) -> impl Iterator<Item = (&String, &SparseTreeEntry)> {
        self.entries.iter()
    }

    fn content(&self, entry: &SparseTreeEntry) -> &[u8] {
        let end = entry.data_start + entry.content_len;
        &self.buffer[entry.data_start..end]
    }
}

/// A decoded sparse tree carried across consecutive delta builds.
///
/// The publisher builds chain deltas sequentially in one process; the older
/// archive of step N+1 is exactly the newer archive of step N, so reusing the
/// decode drops one zstd decode and one collect pass per published version.
/// `archive_sha256` binds the tree to the archive it was decoded from: any
/// other archive (a checkpoint full, a rebuilt package, a different RID)
/// fails the match and falls back to a cold decode.
#[derive(Debug)]
pub struct SparseTreeReuse {
    pub(super) archive_sha256: String,
    pub(super) buffer: Vec<u8>,
    pub(super) entries: BTreeMap<String, SparseTreeEntry>,
}

impl SparseTreeReuse {
    fn new(archive_sha256: &str, buffer: Vec<u8>) -> Result<Self> {
        let entries = collect_tree_entries_in_memory(&buffer)?;
        Ok(Self {
            archive_sha256: archive_sha256.to_string(),
            buffer,
            entries,
        })
    }

    pub(super) fn matches(&self, archive_sha256: &str) -> bool {
        self.archive_sha256 == archive_sha256
    }

    pub(super) fn tree(&self) -> SparseTree<'_> {
        SparseTree {
            buffer: &self.buffer,
            entries: &self.entries,
        }
    }
}

pub(super) fn decode_tar(archive: &[u8]) -> Result<Vec<u8>> {
    let mut decoder =
        zstd::Decoder::new(archive).map_err(|e| SurgeError::Archive(format!("Failed to create zstd decoder: {e}")))?;
    let mut tar_bytes = Vec::new();
    decoder
        .read_to_end(&mut tar_bytes)
        .map_err(|e| SurgeError::Archive(format!("Failed to decompress archive: {e}")))?;
    Ok(tar_bytes)
}

pub(super) fn collect_tree_entries_in_memory(tar_bytes: &[u8]) -> Result<BTreeMap<String, SparseTreeEntry>> {
    let mut entries = BTreeMap::new();
    let mut archive = tar::Archive::new(std::io::Cursor::new(tar_bytes));
    // Tar is a sequence of 512-byte header blocks followed by data rounded up
    // to 512-byte blocks. Track the header offset manually so file contents
    // can be referenced by offset into `tar_bytes` instead of copied.
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
        let (data_start, content_len) = if entry_type.is_file() {
            let end = data_start + size;
            if end > tar_bytes.len() {
                return Err(SurgeError::Archive(
                    "Truncated archive entry while building sparse delta".into(),
                ));
            }
            (data_start, size)
        } else {
            (0, 0)
        };
        let entry = if entry_type.is_dir() {
            SparseTreeEntry {
                kind: TreeEntryKind::Directory,
                mode: normalized_header_mode(mode, true),
                symlink_target: None,
                data_start,
                content_len,
            }
        } else if entry_type.is_symlink() {
            SparseTreeEntry {
                kind: TreeEntryKind::Symlink,
                mode: 0,
                symlink_target,
                data_start,
                content_len,
            }
        } else if entry_type.is_file() {
            SparseTreeEntry {
                kind: TreeEntryKind::File,
                mode: normalized_header_mode(mode, false),
                symlink_target: None,
                data_start,
                content_len,
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

/// Build the sparse delta patch and return it together with a decoded tree of
/// the newer archive for the next chain step.
///
/// `older_tree` is reused only when its `archive_sha256` matches
/// `older_archive_sha256`; anything else triggers a cold decode of
/// `older_archive`.
pub fn build_sparse_file_patch_with_tree(
    older_tree: Option<&SparseTreeReuse>,
    older_archive: &[u8],
    older_archive_sha256: &str,
    newer_archive: &[u8],
    newer_archive_sha256: &str,
    compression_level: i32,
    zstd_workers: u32,
    diff_options: &ChunkedDiffOptions,
) -> Result<(Vec<u8>, SparseTreeReuse)> {
    let reuse = older_tree.filter(|tree| tree.matches(older_archive_sha256));
    // The two archives are independent; decode concurrently when the older
    // tree cannot be reused. When it can be, only the newer archive is
    // decoded, on this thread.
    let (older_buffer, newer_buffer): (Option<Vec<u8>>, Option<Vec<u8>>) = match reuse {
        Some(_) => (None, Some(decode_tar(newer_archive)?)),
        None => {
            let buffers = std::thread::scope(|s| -> Result<(Vec<u8>, Vec<u8>)> {
                let h_old = s.spawn(|| decode_tar(older_archive));
                let h_new = s.spawn(|| decode_tar(newer_archive));
                let older_buffer = h_old
                    .join()
                    .map_err(|_| SurgeError::Archive("Archive decode worker panicked".to_string()))??;
                let newer_buffer = h_new
                    .join()
                    .map_err(|_| SurgeError::Archive("Archive decode worker panicked".to_string()))??;
                Ok((older_buffer, newer_buffer))
            })?;
            (Some(buffers.0), Some(buffers.1))
        }
    };
    let cold_older = older_buffer
        .map(|buffer| collect_tree_entries_in_memory(&buffer).map(|entries| (buffer, entries)))
        .transpose()?;
    let older_tree = if let Some(tree) = reuse {
        tree.tree()
    } else {
        match cold_older.as_ref() {
            Some((buffer, entries)) => SparseTree { buffer, entries },
            None => return Err(SurgeError::Archive("Missing older archive decode".to_string())),
        }
    };
    let newer_buffer = match newer_buffer {
        Some(buffer) => buffer,
        None => return Err(SurgeError::Archive("Missing newer archive decode".to_string())),
    };
    let newer_entries = collect_tree_entries_in_memory(&newer_buffer)?;
    let newer_tree = SparseTree {
        buffer: &newer_buffer,
        entries: &newer_entries,
    };

    let patch = build_patch_from_trees(&older_tree, &newer_tree, compression_level, zstd_workers, diff_options)?;
    let next_tree = SparseTreeReuse::new(newer_archive_sha256, newer_buffer)?;
    Ok((patch, next_tree))
}

pub(super) fn build_patch_from_trees(
    older_tree: &SparseTree<'_>,
    newer_tree: &SparseTree<'_>,
    compression_level: i32,
    zstd_workers: u32,
    diff_options: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    // Phase 1: classify entries sequentially. The heavy per-file pipeline
    // (hashes + chunked diff) is queued as work; everything else yields its
    // op immediately. Ops keep the historical order (deletes first, then
    // path-sorted), so payload bytes stay identical to the sequential build.
    let mut delete_entries: Vec<(&String, &SparseTreeEntry)> = older_tree.iter().collect();
    delete_entries
        .sort_by(|(left, _), (right, _)| path_depth(right).cmp(&path_depth(left)).then_with(|| right.cmp(left)));
    let mut ops: Vec<SparseFileOp> = delete_entries
        .iter()
        .filter(|(path, older_entry)| newer_tree.get(path).is_none_or(|newer| newer.kind != older_entry.kind))
        .map(|(path, _)| SparseFileOp::Delete { path: (*path).clone() })
        .collect();
    let mut payloads: Vec<u8> = Vec::new();

    let mut path_items: Vec<PathItem> = Vec::new();
    let mut new_paths: Vec<&String> = newer_tree.keys().collect();
    new_paths.sort();
    for path in new_paths {
        let Some(newer) = newer_tree.get(path) else {
            continue; // collected key
        };
        let older = older_tree.get(path);
        let (immediate, file_work) = match newer.kind {
            TreeEntryKind::Directory => {
                if older.is_none_or(|entry| entry.kind != TreeEntryKind::Directory || entry.mode != newer.mode) {
                    (
                        Some(SparseFileOp::EnsureDir {
                            path: path.clone(),
                            mode: newer.mode,
                        }),
                        None,
                    )
                } else {
                    (None, None)
                }
            }
            TreeEntryKind::Symlink => {
                if older.is_none_or(|entry| {
                    entry.kind != TreeEntryKind::Symlink || entry.symlink_target != newer.symlink_target
                }) {
                    (
                        Some(SparseFileOp::WriteSymlink {
                            path: path.clone(),
                            target: newer.symlink_target.clone().unwrap_or_default(),
                        }),
                        None,
                    )
                } else {
                    (None, None)
                }
            }
            TreeEntryKind::File => {
                let newer_content = newer_tree.content(newer);
                let older_file = older.filter(|entry| entry.kind == TreeEntryKind::File);
                match older_file {
                    Some(older_entry) if older_tree.content(older_entry) == newer_content => {
                        if newer.mode == older_entry.mode {
                            (None, None)
                        } else {
                            (
                                Some(SparseFileOp::SetMode {
                                    path: path.clone(),
                                    mode: newer.mode,
                                }),
                                None,
                            )
                        }
                    }
                    Some(older_entry) => (
                        None,
                        Some(FileWork::Changed {
                            mode: newer.mode,
                            newer: newer_content,
                            older: older_tree.content(older_entry),
                        }),
                    ),
                    None => (
                        None,
                        Some(FileWork::New {
                            mode: newer.mode,
                            content: newer_content,
                        }),
                    ),
                }
            }
        };
        path_items.push(PathItem {
            path: path.clone(),
            immediate,
            file_work,
        });
    }

    // Phase 2: run the per-file pipelines across a bounded worker pool.
    // The chunked diff output is independent of its thread count (per-chunk
    // results are serialized by chunk index), so splitting the budget across
    // files keeps the patch bytes identical to the single-file build.
    // Largest files first: the cold-cache cost of a big chunked diff
    // (random chunk access while the page cache fills) should run on the
    // full thread budget before smaller files share the machine.
    let mut work_items: Vec<(usize, &FileWork<'_>)> = path_items
        .iter()
        .enumerate()
        .filter_map(|(i, item)| item.file_work.as_ref().map(|work| (i, work)))
        .collect();
    work_items.sort_by_key(|(_, work)| {
        std::cmp::Reverse(match work {
            FileWork::Changed { newer, .. } => newer.len(),
            FileWork::New { content, .. } => content.len(),
        })
    });
    let budget = usize::try_from(zstd_workers).unwrap_or(1).max(1);
    let parallelism = work_items.len().min(MAX_PARALLEL_FILE_PASSES).min(budget).max(1);
    let split_options;
    let file_diff_options: &ChunkedDiffOptions = if parallelism > 1 {
        let cpu = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let base = diff_options.max_threads.max(1);
        let per_file = (base.min(cpu) / parallelism).max(1);
        split_options = ChunkedDiffOptions {
            chunk_size: diff_options.chunk_size,
            max_threads: per_file,
            format: diff_options.format,
        };
        &split_options
    } else {
        diff_options
    };

    let work_counter = std::sync::atomic::AtomicUsize::new(0);
    let results: std::sync::Mutex<Vec<(usize, Result<FileWorkResult>)>> =
        std::sync::Mutex::new(Vec::with_capacity(work_items.len()));
    std::thread::scope(|s| {
        for _ in 0..parallelism {
            s.spawn(|| {
                loop {
                    let pos = work_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if pos >= work_items.len() {
                        return;
                    }
                    let (index, work) = work_items[pos];
                    let result = file_work_pipeline(work, file_diff_options);
                    let mut guard = results.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    guard.push((index, result));
                }
            });
        }
    });
    let mut results_by_index: std::collections::HashMap<usize, Result<FileWorkResult>> = results
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .into_iter()
        .collect();

    // Phase 3: assemble ops and payloads in path order.
    for (item_index, item) in path_items.iter_mut().enumerate() {
        if let Some(op) = item.immediate.take() {
            ops.push(op);
            continue;
        }
        let Some(work) = item.file_work.as_ref() else {
            continue;
        };
        let Some(result) = results_by_index.remove(&item_index) else {
            unreachable!("work result missing for '{}'", item.path);
        };
        match result {
            Ok(FileWorkResult::WriteFile { payload, sha256 }) => {
                let (payload_offset, payload_len) = append_payload(&mut payloads, &payload)?;
                ops.push(SparseFileOp::WriteFile {
                    path: item.path.clone(),
                    mode: work_mode(work),
                    payload_offset,
                    payload_len,
                    sha256,
                });
            }
            Ok(FileWorkResult::PatchFile {
                payload,
                basis_sha256,
                sha256,
            }) => {
                let (payload_offset, payload_len) = append_payload(&mut payloads, &payload)?;
                ops.push(SparseFileOp::PatchFile {
                    path: item.path.clone(),
                    mode: work_mode(work),
                    payload_offset,
                    payload_len,
                    basis_sha256,
                    sha256,
                });
            }
            Err(e) => return Err(e),
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

/// In-memory build without cross-step reuse (the standalone entry point used
/// by one-off delta builds).
pub(super) fn build_sparse_file_patch_in_memory(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
    diff_options: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    let (patch, _next_tree) = build_sparse_file_patch_with_tree(
        None,
        older_archive,
        "",
        newer_archive,
        "",
        compression_level,
        zstd_workers,
        diff_options,
    )?;
    Ok(patch)
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
        let buffer = &tar_bytes[..];
        let view = SparseTree { buffer, entries: &tree };
        assert_eq!(view.content(&tree["bin/tool"]), &[1, 2, 3]);
    }

    #[test]
    fn reused_tree_builds_byte_identical_patch() {
        let opts = ChunkedDiffOptions {
            chunk_size: 256 * 1024,
            max_threads: 1,
            format: crate::diff::chunked::ChunkedPatchFormat::IdentityChunks,
        };
        let v1 = pack_tree(&[
            ("a", vec![0x11u8; 400_000], 0o644, false),
            ("b", b"steady\n".to_vec(), 0o644, false),
        ]);
        let v2 = pack_tree(&[
            (
                "a",
                {
                    let mut data = vec![0x11u8; 400_000];
                    data[5000] = 0x77;
                    data
                },
                0o644,
                false,
            ),
            ("b", b"steady\n".to_vec(), 0o644, false),
        ]);
        let v2_sha = sha256_hex(&v2);

        // Cold build: no reuse.
        let (_cold_patch, v2_tree) =
            build_sparse_file_patch_with_tree(None, &v1, "", &v2, &v2_sha, 3, 1, &opts).expect("cold build");
        // Warm build of the next step reusing v2's tree as the older tree.
        let v3 = pack_tree(&[
            (
                "a",
                {
                    let mut data = vec![0x11u8; 400_000];
                    data[5000] = 0x77;
                    data[8000] = 0x33;
                    data
                },
                0o644,
                false,
            ),
            ("b", b"steady\n".to_vec(), 0o644, false),
        ]);
        let v3_cold = build_sparse_file_patch_with_tree(None, &v2, "", &v3, "", 3, 1, &opts).expect("v3 cold");
        let v3_warm =
            build_sparse_file_patch_with_tree(Some(&v2_tree), &v2, &v2_sha, &v3, "", 3, 1, &opts).expect("v3 warm");
        assert_eq!(v3_cold.0, v3_warm.0, "reused tree must produce the same patch bytes");

        // A mismatched archive hash must fall back to a cold decode rather
        // than reuse the stale tree: building v2->v3 with the v1-era hash
        // guard still succeeds and stays correct.
        let v3_forced_cold = build_sparse_file_patch_with_tree(Some(&v2_tree), &v2, "wrong-hash", &v3, "", 3, 1, &opts)
            .expect("forced cold build");
        assert_eq!(v3_cold.0, v3_forced_cold.0, "hash mismatch must cold-decode, not reuse");
    }
}
