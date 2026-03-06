use std::fs;
use std::path::Path;

use crate::error::{Result, SurgeError};

/// Atomically rename a file (or directory). Falls back to copy+delete on cross-device moves.
pub fn atomic_rename(from: &Path, to: &Path) -> Result<()> {
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Cross-device link: fallback to copy + delete
            if e.raw_os_error() == Some(18) {
                // EXDEV on Linux
                if from.is_dir() {
                    copy_directory(from, to)?;
                    fs::remove_dir_all(from)?;
                } else {
                    fs::copy(from, to)?;
                    fs::remove_file(from)?;
                }
                Ok(())
            } else {
                Err(e.into())
            }
        }
    }
}

/// Copy a file with progress reporting.
pub fn copy_file_with_progress(
    src: &Path,
    dst: &Path,
    progress: Option<&(dyn Fn(u64, u64) + Send + Sync)>,
) -> Result<u64> {
    use std::io::{Read, Write};

    let mut reader = fs::File::open(src)?;
    let total = reader.metadata()?.len();

    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut writer = fs::File::create(dst)?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut done = 0u64;

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        done += n as u64;
        if let Some(cb) = progress {
            cb(done, total);
        }
    }

    writer.flush()?;
    Ok(done)
}

/// Recursively copy a directory tree.
pub fn copy_directory(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let target_path = dst.join(entry.file_name());

        if ty.is_dir() {
            copy_directory(&entry.path(), &target_path)?;
        } else {
            fs::copy(entry.path(), &target_path)?;
        }
    }

    Ok(())
}

/// Read entire file contents.
pub fn read_file(path: &Path) -> Result<Vec<u8>> {
    Ok(fs::read(path)?)
}

/// Write data atomically: write to temp file then rename.
pub fn write_file_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        SurgeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no parent directory",
        ))
    })?;
    fs::create_dir_all(parent)?;

    let tmp = tempfile::NamedTempFile::new_in(parent)?;
    fs::write(tmp.path(), data)?;
    tmp.persist(path).map_err(|e| SurgeError::Io(e.error))?;
    Ok(())
}

/// Create a unique temporary directory.
pub fn create_temp_dir() -> Result<std::path::PathBuf> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_path_buf();
    // Keep the directory alive by leaking the handle (caller must clean up).
    std::mem::forget(dir);
    Ok(path)
}

/// List subdirectory names in a directory.
pub fn list_directories(path: &Path) -> Result<Vec<String>> {
    let mut dirs = Vec::new();
    if !path.exists() {
        return Ok(dirs);
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && let Some(name) = entry.file_name().to_str()
        {
            dirs.push(name.to_string());
        }
    }
    dirs.sort();
    Ok(dirs)
}

/// Compute total size of a directory recursively.
pub fn directory_size(path: &Path) -> Result<u64> {
    let mut total = 0;
    if !path.exists() {
        return Ok(0);
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += directory_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

/// Make a file executable (Unix only).
#[cfg(unix)]
pub fn make_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = fs::metadata(path)?;
    let mut perms = meta.permissions();
    let mode = perms.mode() | 0o111;
    perms.set_mode(mode);
    fs::set_permissions(path, perms)?;
    Ok(())
}

/// Make a file executable (no-op on Windows).
#[cfg(not(unix))]
pub fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}
