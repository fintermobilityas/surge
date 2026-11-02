//! Extract tar + zstd archives.

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

pub type ExtractProgress<'a> = dyn Fn(u64, u64, u64, u64) + 'a;

pub fn extract_to(data: &[u8], dest_dir: &Path, progress: Option<&ExtractProgress<'_>>) -> Result<()> {
    std::fs::create_dir_all(dest_dir)?;

    let decoder =
        zstd::Decoder::new(data).map_err(|e| SurgeError::Archive(format!("Failed to create zstd decoder: {e}")))?;
    let archive = tar::Archive::new(decoder);

    // First pass: count entries for progress
    let entries_list = list_entries_from_bytes(data)?;
    let total_files = entries_list.len() as u64;
    let total_bytes: u64 = entries_list.iter().map(|e| e.size).sum();

    // Second pass: actually extract
    let decoder2 =
        zstd::Decoder::new(data).map_err(|e| SurgeError::Archive(format!("Failed to create zstd decoder: {e}")))?;
    let mut archive2 = tar::Archive::new(decoder2);
    let _ = archive; // drop first archive

    let dest_canonical = dest_dir.canonicalize().unwrap_or_else(|_| dest_dir.to_path_buf());
    let mut files_done = 0u64;
    let mut bytes_done = 0u64;

    for entry in archive2
        .entries()
        .map_err(|e| SurgeError::Archive(format!("Failed to read entries: {e}")))?
    {
        let mut entry = entry.map_err(|e| SurgeError::Archive(format!("Bad entry: {e}")))?;
        let entry_path = entry
            .path()
            .map_err(|e| SurgeError::Archive(format!("Bad path: {e}")))?
            .into_owned();
        let entry_type = entry.header().entry_type();
        let declared_size = entry.size();

        // Security: prevent path traversal
        let full_path = dest_dir.join(&entry_path);
        let full_canonical = full_path.canonicalize().unwrap_or_else(|_| full_path.clone());
        if !full_canonical.starts_with(&dest_canonical) && !full_path.starts_with(dest_dir) {
            return Err(SurgeError::Archive(format!(
                "Path traversal detected: {}",
                entry_path.display()
            )));
        }

        let unpacked = entry
            .unpack_in(dest_dir)
            .map_err(|e| SurgeError::Archive(format!("Failed to extract: {e}")))?;
        if !unpacked {
            continue;
        }

        // A tar header can declare a size that the writer never delivered, so
        // verify what actually landed on disk instead of trusting the header.
        let written_size = if entry_type.is_file() {
            let written_size = std::fs::symlink_metadata(&full_path)
                .map_err(|e| {
                    SurgeError::Archive(format!("Failed to stat extracted file '{}': {e}", entry_path.display()))
                })?
                .len();
            if written_size != declared_size {
                return Err(SurgeError::Archive(format!(
                    "Extracted file '{}' is {written_size} bytes but the archive declares {declared_size} bytes",
                    entry_path.display()
                )));
            }
            written_size
        } else {
            // Directories, symlinks and hard links: a symlink header size is not
            // the size of its target, so only regular files are size-checked.
            declared_size
        };

        bytes_done += written_size;
        files_done += 1;

        if let Some(cb) = &progress {
            cb(files_done, total_files, bytes_done, total_bytes);
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    pub path: PathBuf,
    pub size: u64,
    pub is_dir: bool,
}

pub fn list_entries_from_bytes(data: &[u8]) -> Result<Vec<ArchiveEntry>> {
    let decoder =
        zstd::Decoder::new(data).map_err(|e| SurgeError::Archive(format!("Failed to create zstd decoder: {e}")))?;
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();

    for entry in archive
        .entries()
        .map_err(|e| SurgeError::Archive(format!("Failed to read entries: {e}")))?
    {
        let entry = entry.map_err(|e| SurgeError::Archive(format!("Bad entry: {e}")))?;
        let path = entry
            .path()
            .map_err(|e| SurgeError::Archive(format!("Bad path: {e}")))?
            .into_owned();
        let is_dir = entry.header().entry_type().is_dir();
        entries.push(ArchiveEntry {
            path,
            size: entry.size(),
            is_dir,
        });
    }

    Ok(entries)
}

pub fn read_entry(data: &[u8], entry_path: &str) -> Result<Vec<u8>> {
    let decoder =
        zstd::Decoder::new(data).map_err(|e| SurgeError::Archive(format!("Failed to create zstd decoder: {e}")))?;
    let mut archive = tar::Archive::new(decoder);

    for entry in archive
        .entries()
        .map_err(|e| SurgeError::Archive(format!("Failed to read entries: {e}")))?
    {
        let mut entry = entry.map_err(|e| SurgeError::Archive(format!("Bad entry: {e}")))?;
        let path = entry
            .path()
            .map_err(|e| SurgeError::Archive(format!("Bad path: {e}")))?;

        if path.to_str() == Some(entry_path) {
            let mut buf = Vec::new();
            entry
                .read_to_end(&mut buf)
                .map_err(|e| SurgeError::Archive(format!("Failed to read entry: {e}")))?;
            return Ok(buf);
        }
    }

    Err(SurgeError::NotFound(format!("Entry not found: {entry_path}")))
}

pub fn extract_file_to(archive_path: &Path, dest_dir: &Path) -> Result<()> {
    let data = std::fs::read(archive_path)?;
    extract_to(&data, dest_dir, None)
}

pub fn extract_file_to_with_progress(
    archive_path: &Path,
    dest_dir: &Path,
    progress: Option<&ExtractProgress<'_>>,
) -> Result<()> {
    let data = std::fs::read(archive_path)?;
    extract_to(&data, dest_dir, progress)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::packer::ArchivePacker;

    #[test]
    fn test_archive_roundtrip() {
        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("hello.txt", b"Hello World", 0o644).unwrap();
        packer.add_buffer("sub/nested.txt", b"Nested content", 0o644).unwrap();
        let archive_data = packer.finalize().unwrap();

        let entries = list_entries_from_bytes(&archive_data).unwrap();
        assert_eq!(entries.len(), 2);

        let content = read_entry(&archive_data, "hello.txt").unwrap();
        assert_eq!(content, b"Hello World");

        let dir = tempfile::tempdir().unwrap();
        extract_to(&archive_data, dir.path(), None).unwrap();
        assert!(dir.path().join("hello.txt").exists());
        assert!(dir.path().join("sub/nested.txt").exists());
    }

    fn collect_progress(archive: &[u8], dest_dir: &Path) -> Vec<(u64, u64, u64, u64)> {
        let observed = std::sync::Mutex::new(Vec::new());
        let progress = |files_done: u64, files_total: u64, bytes_done: u64, bytes_total: u64| {
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((files_done, files_total, bytes_done, bytes_total));
        };
        extract_to(archive, dest_dir, Some(&progress)).unwrap();
        observed.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn extract_reports_actual_written_bytes() {
        let blob = vec![0x5a_u8; 4096];
        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("hello.txt", b"Hello World", 0o644).unwrap();
        packer.add_buffer("nested/blob.bin", &blob, 0o644).unwrap();
        let archive = packer.finalize().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let observed = collect_progress(&archive, dir.path());

        let (files_done, files_total, bytes_done, bytes_total) = *observed.last().unwrap();
        let expected_bytes = 11 + 4096;
        assert_eq!(files_done, 2);
        assert_eq!(files_total, 2);
        assert_eq!(bytes_done, expected_bytes);
        assert_eq!(bytes_total, expected_bytes);
        assert_eq!(
            std::fs::metadata(dir.path().join("nested/blob.bin")).unwrap().len(),
            4096
        );
    }

    #[test]
    fn extract_accepts_legitimately_empty_files() {
        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("empty.txt", b"", 0o644).unwrap();
        let archive = packer.finalize().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let observed = collect_progress(&archive, dir.path());

        let (files_done, _files_total, bytes_done, bytes_total) = *observed.last().unwrap();
        assert_eq!(files_done, 1);
        assert_eq!(bytes_done, 0);
        assert_eq!(bytes_total, 0);
        assert_eq!(std::fs::metadata(dir.path().join("empty.txt")).unwrap().len(), 0);
    }

    #[test]
    #[cfg(unix)]
    fn extract_does_not_size_check_symlink_entries() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(source_dir.join("native")).unwrap();
        let library = source_dir.join("native/libfoo.so.1.2.3");
        std::fs::write(&library, vec![0xfe_u8; 64 * 1024]).unwrap();
        symlink("libfoo.so.1.2.3", source_dir.join("native/libfoo.so")).unwrap();

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_directory(&source_dir, "").unwrap();
        let archive = packer.finalize().unwrap();

        let dest_dir = tmp.path().join("extracted");
        let observed = collect_progress(&archive, &dest_dir);

        let extracted_link = dest_dir.join("native/libfoo.so");
        let link_metadata = std::fs::symlink_metadata(&extracted_link).unwrap();
        assert!(link_metadata.file_type().is_symlink());
        assert_eq!(
            std::fs::read_link(&extracted_link).unwrap(),
            Path::new("libfoo.so.1.2.3")
        );
        // Only the regular library contributes bytes; the symlink header is 0.
        assert_eq!(observed.last().unwrap().2, 64 * 1024);
    }
}
