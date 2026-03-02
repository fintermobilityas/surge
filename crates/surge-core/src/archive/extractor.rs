//! Extract tar + zstd archives.

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

/// Progress callback for extraction: (files_done, files_total, bytes_done, bytes_total).
pub type ExtractProgress = dyn Fn(u64, u64, u64, u64);

/// Extract a tar.zst archive from bytes to a destination directory.
pub fn extract_to(data: &[u8], dest_dir: &Path, progress: Option<&ExtractProgress>) -> Result<()> {
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

        // Security: prevent path traversal
        let full_path = dest_dir.join(&entry_path);
        let full_canonical = full_path.canonicalize().unwrap_or_else(|_| full_path.clone());
        if !full_canonical.starts_with(&dest_canonical) && !full_path.starts_with(dest_dir) {
            return Err(SurgeError::Archive(format!(
                "Path traversal detected: {}",
                entry_path.display()
            )));
        }

        entry
            .unpack_in(dest_dir)
            .map_err(|e| SurgeError::Archive(format!("Failed to extract: {e}")))?;

        let size = entry.size();
        bytes_done += size;
        files_done += 1;

        if let Some(cb) = &progress {
            cb(files_done, total_files, bytes_done, total_bytes);
        }
    }

    Ok(())
}

/// An entry in an archive listing.
#[derive(Debug, Clone)]
pub struct ArchiveEntry {
    pub path: PathBuf,
    pub size: u64,
    pub is_dir: bool,
}

/// List all entries in a tar.zst archive from bytes.
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

/// Read a single entry from a tar.zst archive.
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

/// Extract a tar.zst file from disk to a destination directory.
pub fn extract_file_to(archive_path: &Path, dest_dir: &Path) -> Result<()> {
    let data = std::fs::read(archive_path)?;
    extract_to(&data, dest_dir, None)
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
}
