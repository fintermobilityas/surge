use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use crate::config::constants::IO_CHUNK_SIZE;
use crate::error::{Result, SurgeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TreeEntryKind {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone)]
pub(super) struct TreeEntry {
    pub(super) source_path: PathBuf,
    pub(super) kind: TreeEntryKind,
    pub(super) mode: u32,
    pub(super) symlink_target: Option<String>,
}

pub(super) fn collect_tree_entries(root: &Path) -> Result<BTreeMap<String, TreeEntry>> {
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

pub(super) fn files_identical(left: &Path, right: &Path) -> Result<bool> {
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
