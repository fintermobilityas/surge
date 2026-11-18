//! Flushing freshly written install content to stable storage.
//!
//! A rename is journalled within seconds, while the data of the files it
//! exposes can sit in the page cache far longer. Swapping an application
//! directory in before its files are flushed therefore lets a host reset leave
//! a complete directory listing of 0-byte files. Everything that replaces an
//! install goes through these helpers first.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use crate::error::{Result, SurgeError};

/// Flush every regular file and directory below `root`, and `root` itself.
///
/// Symlinks are not followed: a link is persisted with the directory that
/// holds it. An explicit stack is used instead of recursion so a deep tree
/// cannot overflow the stack.
pub fn sync_tree(root: &Path) -> Result<()> {
    let mut pending = vec![root.to_path_buf()];
    let mut directories = Vec::new();

    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory).map_err(|e| flush_error(&directory, &e))?;
        for entry in entries {
            let entry = entry.map_err(|e| flush_error(&directory, &e))?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| flush_error(&path, &e))?;
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file() {
                sync_file(&path)?;
            }
        }
        directories.push(directory);
    }

    // Children first, so a directory is flushed after the entries it lists.
    for directory in directories.iter().rev() {
        sync_directory(directory)?;
    }
    Ok(())
}

/// Flush a directory so the entries created, renamed or removed in it survive
/// a reset. Directories cannot be flushed through the Windows file API, so this
/// is a no-op there.
#[cfg(unix)]
pub fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|e| flush_error(path, &e))
}

#[cfg(not(unix))]
pub fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

/// Write `data` to `path` in place and flush both the file and its directory.
pub fn write_file_synced(path: &Path, data: &[u8]) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(data)?;
    file.sync_all().map_err(|e| flush_error(path, &e))?;
    drop(file);
    sync_parent_directory(path)
}

/// Replace `path` atomically (temp file + rename) and flush the new content and
/// the rename, so a reset leaves either the previous or the new content.
pub fn write_file_durable(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        SurgeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no parent directory",
        ))
    })?;
    fs::create_dir_all(parent)?;

    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    tmp.write_all(data)?;
    tmp.as_file().sync_all().map_err(|e| flush_error(path, &e))?;
    tmp.persist(path).map_err(|e| SurgeError::Io(e.error))?;
    sync_directory(parent)
}

fn sync_parent_directory(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => sync_directory(parent),
        _ => Ok(()),
    }
}

#[cfg(unix)]
fn sync_file(path: &Path) -> Result<()> {
    match File::open(path) {
        Ok(file) => file.sync_all().map_err(|e| flush_error(path, &e)),
        // A payload file its owner cannot read cannot be opened for a flush
        // either. Refusing the whole install for it would be a regression.
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
            tracing::warn!(path = %path.display(), error = %e, "Cannot open file to flush it to disk; skipping");
            Ok(())
        }
        Err(e) => Err(flush_error(path, &e)),
    }
}

/// Windows flushes need a writable handle, which read-only or locked payload
/// files do not give, so a flush that cannot be done there is skipped.
#[cfg(not(unix))]
fn sync_file(path: &Path) -> Result<()> {
    let flushed = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all());
    if let Err(e) = flushed {
        tracing::debug!(path = %path.display(), error = %e, "Could not flush file to disk; skipping");
    }
    Ok(())
}

fn flush_error(path: &Path, error: &std::io::Error) -> SurgeError {
    SurgeError::Io(std::io::Error::new(
        error.kind(),
        format!("Failed to flush '{}' to disk: {error}", path.display()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_tree_flushes_nested_files_and_empty_directories() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path().join("payload");
        fs::create_dir_all(root.join("lib/nested")).expect("nested directory should be created");
        fs::create_dir_all(root.join("empty")).expect("empty directory should be created");
        fs::write(root.join("sampleapp"), b"binary").expect("file should be written");
        fs::write(root.join("lib/nested/dependency.so"), b"library").expect("file should be written");
        fs::write(root.join("lib/zero"), b"").expect("file should be written");

        sync_tree(&root).expect("tree should be flushed");

        assert_eq!(
            fs::read(root.join("sampleapp")).expect("file should be readable"),
            b"binary"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sync_tree_does_not_follow_symlinks() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let root = tmp.path().join("payload");
        fs::create_dir_all(&root).expect("payload directory should be created");
        fs::write(root.join("sampleapp"), b"binary").expect("file should be written");
        std::os::unix::fs::symlink(tmp.path().join("missing-target"), root.join("dangling"))
            .expect("dangling symlink should be created");
        std::os::unix::fs::symlink(&root, root.join("loop")).expect("looping symlink should be created");

        sync_tree(&root).expect("symlinks must be skipped, not followed");
    }

    #[test]
    fn sync_tree_reports_the_missing_root() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let missing = tmp.path().join("missing");

        let err = sync_tree(&missing).expect_err("a missing tree cannot be flushed");

        assert!(err.to_string().contains("missing"), "unexpected error: {err}");
    }

    #[test]
    fn write_file_durable_replaces_existing_content() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let path = tmp.path().join("state/status.json");

        write_file_durable(&path, b"first").expect("first write should succeed");
        write_file_durable(&path, b"second").expect("second write should succeed");

        assert_eq!(fs::read(&path).expect("file should be readable"), b"second");
        let leftovers = fs::read_dir(path.parent().expect("parent"))
            .expect("parent should be listable")
            .count();
        assert_eq!(leftovers, 1, "no temp file may be left behind");
    }

    #[test]
    fn write_file_synced_truncates_existing_content() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let path = tmp.path().join("manifest.yml");
        fs::write(&path, b"a much longer previous manifest").expect("file should be written");

        write_file_synced(&path, b"short").expect("write should succeed");

        assert_eq!(fs::read(&path).expect("file should be readable"), b"short");
    }
}
