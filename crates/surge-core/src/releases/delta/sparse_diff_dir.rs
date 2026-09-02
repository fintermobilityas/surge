//! Sparse delta build with a packed staging directory as the newer side.
//!
//! The publisher packs the full package from a staging root first and
//! builds the sparse delta in the same `build()`, so the newer tree is
//! known without decoding the zstd frame that was just written: the
//! directory is walked with the packer's exact semantics and file
//! contents are read from the page cache.

use std::collections::BTreeMap;

use crate::diff::chunked::ChunkedDiffOptions;
use crate::error::{Result, SurgeError};

use super::sparse_diff::{
    SparseTree, SparseTreeEntry, SparseTreeReuse, build_patch_from_trees, collect_tree_entries_in_memory, decode_tar,
};
use super::tree::{TreeEntryKind, collect_tree_entries_with_executables};

/// Build the sparse delta patch using a packed staging directory as the
/// newer side instead of decoding the newer archive.
///
/// The publisher just packed `newer_root` into the newer archive, so its
/// tree is known without a zstd decode: the directory is walked the same
/// way the packer packs it (including the executable-bit overrides) and
/// file contents are read straight from disk (page cache, not zstd).
/// The patch bytes are identical to the archive-based build because the
/// archive was packed from these exact files in the same build; the
/// client's full-archive SHA-256 check still guards the whole chain.
pub fn build_sparse_file_patch_with_tree_from_directory(
    older_tree: Option<&SparseTreeReuse>,
    older_archive: &[u8],
    older_archive_sha256: &str,
    newer_root: &std::path::Path,
    executable_paths: &std::collections::BTreeSet<String>,
    newer_archive_sha256: &str,
    compression_level: i32,
    zstd_workers: u32,
    diff_options: &ChunkedDiffOptions,
) -> Result<(Vec<u8>, SparseTreeReuse)> {
    let reuse = older_tree.filter(|tree| tree.matches(older_archive_sha256));
    let older_buffer: Option<Vec<u8>> = match reuse {
        Some(_) => None,
        None => Some(decode_tar(older_archive)?),
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

    let newer = collect_tree_from_directory(newer_root, executable_paths)?;
    let newer_tree = SparseTree {
        buffer: &newer.buffer,
        entries: &newer.entries,
    };

    let patch = build_patch_from_trees(&older_tree, &newer_tree, compression_level, zstd_workers, diff_options)?;
    let next_tree = SparseTreeReuse {
        archive_sha256: newer_archive_sha256.to_string(),
        buffer: newer.buffer,
        entries: newer.entries,
    };
    Ok((patch, next_tree))
}

/// Walk the packed staging directory and materialize file contents into a
/// single buffer so the newer side shares the in-memory tree representation
/// (offset + length) with the decoded-archive path.
fn collect_tree_from_directory(
    root: &std::path::Path,
    executable_paths: &std::collections::BTreeSet<String>,
) -> Result<SparseTreeReuse> {
    let disk_entries = collect_tree_entries_with_executables(root, executable_paths)?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut entries = BTreeMap::new();
    // BTreeMap iteration is sorted: deterministic materialization.
    for (relative, entry) in &disk_entries {
        let (data_start, content_len) = if entry.kind == TreeEntryKind::File {
            let data_start = buffer.len();
            let expected_len = std::fs::metadata(&entry.source_path)?.len();
            let mut file = std::fs::File::open(&entry.source_path)
                .map_err(|e| SurgeError::Archive(format!("Failed to read packed file '{relative}': {e}")))?;
            std::io::Read::read_to_end(&mut file, &mut buffer)
                .map_err(|e| SurgeError::Archive(format!("Failed to read packed file '{relative}': {e}")))?;
            let content_len = buffer.len() - data_start;
            if u64::try_from(content_len) != Ok(expected_len) {
                return Err(SurgeError::Archive(format!(
                    "Packed file '{relative}' changed size while reading"
                )));
            }
            (data_start, content_len)
        } else {
            (0, 0)
        };
        entries.insert(
            relative.clone(),
            SparseTreeEntry {
                kind: entry.kind,
                mode: entry.mode,
                symlink_target: entry.symlink_target.clone(),
                data_start,
                content_len,
            },
        );
    }
    Ok(SparseTreeReuse {
        archive_sha256: String::new(),
        buffer,
        entries,
    })
}
