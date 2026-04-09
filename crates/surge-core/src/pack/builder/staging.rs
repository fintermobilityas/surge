use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::archive::packer::ArchivePacker;
use crate::error::{Result, SurgeError};

use super::BundledArtifact;

pub(super) fn materialize_canonical_pack_root(
    source_root: &Path,
    bundled: &[BundledArtifact],
) -> Result<tempfile::TempDir> {
    let parent = source_root.parent().unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    mirror_tree(source_root, staging.path())?;
    for artifact in bundled {
        materialize_file_like(&artifact.source, &staging.path().join(&artifact.archive_name))?;
    }
    Ok(staging)
}

pub(crate) fn build_canonical_archive_from_directory(
    source_root: &Path,
    compression_level: i32,
    n_workers: u32,
    excluded_relative_paths: &BTreeSet<String>,
) -> Result<Vec<u8>> {
    let staging_root = if excluded_relative_paths.is_empty() {
        None
    } else {
        Some(stage_directory_for_canonical_archive(
            source_root,
            excluded_relative_paths,
        )?)
    };
    let pack_root = staging_root.as_ref().map_or(source_root, tempfile::TempDir::path);

    let mut packer = ArchivePacker::with_threads(compression_level, n_workers)?;
    packer.add_directory(pack_root, "")?;
    packer.finalize()
}

fn stage_directory_for_canonical_archive(
    source_root: &Path,
    excluded_relative_paths: &BTreeSet<String>,
) -> Result<tempfile::TempDir> {
    let parent = source_root.parent().unwrap_or_else(|| Path::new("."));
    let staging = tempfile::tempdir_in(parent)?;
    mirror_tree_filtered(source_root, staging.path(), Path::new(""), excluded_relative_paths)?;
    Ok(staging)
}

fn mirror_tree(source_root: &Path, dest_root: &Path) -> Result<()> {
    std::fs::create_dir_all(dest_root)?;
    let mut entries = std::fs::read_dir(source_root)?.collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let source = entry.path();
        let dest = dest_root.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&source)?;
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            mirror_tree(&source, &dest)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            materialize_file_like(&source, &dest)?;
        } else {
            return Err(SurgeError::Pack(format!(
                "Unsupported filesystem entry while staging full package: {}",
                source.display()
            )));
        }
    }

    Ok(())
}

fn mirror_tree_filtered(
    source_root: &Path,
    dest_root: &Path,
    relative_root: &Path,
    excluded_relative_paths: &BTreeSet<String>,
) -> Result<()> {
    std::fs::create_dir_all(dest_root)?;
    let mut entries = std::fs::read_dir(source_root)?.collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let relative_path = archive_child_path_for_staging(relative_root, &entry.file_name());
        if excluded_relative_paths.contains(&archive_path_to_string_for_staging(&relative_path)) {
            continue;
        }

        let source = entry.path();
        let dest = dest_root.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&source)?;
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            mirror_tree_filtered(&source, &dest, &relative_path, excluded_relative_paths)?;
        } else if file_type.is_file() || file_type.is_symlink() {
            materialize_file_like(&source, &dest)?;
        } else {
            return Err(SurgeError::Pack(format!(
                "Unsupported filesystem entry while staging canonical archive: {}",
                source.display()
            )));
        }
    }

    Ok(())
}

fn archive_child_path_for_staging(prefix: &Path, child_name: &std::ffi::OsStr) -> PathBuf {
    if prefix.as_os_str().is_empty() {
        PathBuf::from(child_name)
    } else {
        prefix.join(child_name)
    }
}

fn archive_path_to_string_for_staging(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn materialize_file_like(source: &Path, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let metadata = std::fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        let target = std::fs::read_link(source)?;
        create_symlink_for_staging(&target, dest)?;
        return Ok(());
    }

    if let Err(err) = std::fs::hard_link(source, dest) {
        std::fs::copy(source, dest).map_err(|copy_err| {
            SurgeError::Pack(format!(
                "Failed to stage bundled artifact '{}' via hard link ({err}) or copy ({copy_err})",
                source.display()
            ))
        })?;
    }

    Ok(())
}

#[cfg(unix)]
fn create_symlink_for_staging(target: &Path, dest: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, dest)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink_for_staging(target: &Path, dest: &Path) -> Result<()> {
    let metadata = std::fs::metadata(target)?;
    if metadata.is_dir() {
        std::os::windows::fs::symlink_dir(target, dest)?;
    } else {
        std::os::windows::fs::symlink_file(target, dest)?;
    }
    Ok(())
}
