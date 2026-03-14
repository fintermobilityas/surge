//! Create tar + zstd archives.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

pub struct ArchivePacker {
    builder: tar::Builder<zstd::Encoder<'static, Vec<u8>>>,
}

impl ArchivePacker {
    pub fn new(compression_level: i32) -> Result<Self> {
        Self::with_threads(compression_level, 0)
    }

    pub fn with_threads(compression_level: i32, n_workers: u32) -> Result<Self> {
        let mut encoder = zstd::Encoder::new(Vec::new(), compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to create zstd encoder: {e}")))?;
        if n_workers > 0 {
            encoder
                .multithread(n_workers)
                .map_err(|e| SurgeError::Archive(format!("Failed to enable multi-threaded zstd: {e}")))?;
        }
        let builder = tar::Builder::new(encoder);
        Ok(Self { builder })
    }

    pub fn add_file(&mut self, source: &Path, archive_path: &str) -> Result<()> {
        let mut file = std::fs::File::open(source)?;
        self.builder
            .append_file(archive_path, &mut file)
            .map_err(|e| SurgeError::Archive(format!("Failed to add file: {e}")))?;
        Ok(())
    }

    pub fn add_directory(&mut self, source_dir: &Path, prefix: &str) -> Result<()> {
        self.add_directory_recursive(source_dir, Path::new(prefix))
    }

    pub fn add_buffer(&mut self, archive_path: &str, data: &[u8], mode: u32) -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(mode);
        header.set_cksum();

        self.builder
            .append_data(&mut header, archive_path, data)
            .map_err(|e| SurgeError::Archive(format!("Failed to add buffer: {e}")))?;
        Ok(())
    }

    pub fn finalize(self) -> Result<Vec<u8>> {
        let encoder = self
            .builder
            .into_inner()
            .map_err(|e| SurgeError::Archive(format!("Failed to finalize tar: {e}")))?;
        let data = encoder
            .finish()
            .map_err(|e| SurgeError::Archive(format!("Failed to finalize zstd: {e}")))?;
        Ok(data)
    }

    pub fn finalize_to_file(self, path: &Path) -> Result<()> {
        let data = self.finalize()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(path)?;
        file.write_all(&data)?;
        Ok(())
    }

    fn add_directory_recursive(&mut self, source_dir: &Path, archive_prefix: &Path) -> Result<()> {
        if !archive_prefix.as_os_str().is_empty() {
            self.add_directory_entry(archive_prefix, source_dir)?;
        }

        let mut entries = fs::read_dir(source_dir)?.collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
        entries.sort_by(|left, right| {
            archive_child_path(archive_prefix, left.path().file_name().unwrap_or_default()).cmp(&archive_child_path(
                archive_prefix,
                right.path().file_name().unwrap_or_default(),
            ))
        });

        for entry in entries {
            let source_path = entry.path();
            let archive_path = archive_child_path(archive_prefix, &entry.file_name());
            let metadata = fs::symlink_metadata(&source_path)?;

            if metadata.is_dir() {
                self.add_directory_recursive(&source_path, &archive_path)?;
                continue;
            }

            if metadata.is_file() {
                self.add_file_entry(&source_path, &archive_path, &metadata)?;
                continue;
            }

            if metadata.file_type().is_symlink() {
                self.add_symlink_entry(&source_path, &archive_path, &metadata)?;
                continue;
            }

            return Err(SurgeError::Archive(format!(
                "Unsupported filesystem entry while packing: {}",
                source_path.display()
            )));
        }

        Ok(())
    }

    fn add_directory_entry(&mut self, archive_path: &Path, source_path: &Path) -> Result<()> {
        let metadata = fs::symlink_metadata(source_path)?;
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Directory);
        header.set_size(0);
        header.set_mode(normalized_mode(&metadata, true));
        set_normalized_header_metadata(&mut header);
        header.set_cksum();
        self.builder
            .append_data(&mut header, archive_path_to_string(archive_path), std::io::empty())
            .map_err(|e| SurgeError::Archive(format!("Failed to add directory: {e}")))?;
        Ok(())
    }

    fn add_file_entry(&mut self, source_path: &Path, archive_path: &Path, metadata: &fs::Metadata) -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(metadata.len());
        header.set_mode(normalized_mode(metadata, false));
        set_normalized_header_metadata(&mut header);
        header.set_cksum();

        let mut file = fs::File::open(source_path)?;
        self.builder
            .append_data(&mut header, archive_path_to_string(archive_path), &mut file)
            .map_err(|e| SurgeError::Archive(format!("Failed to add file: {e}")))?;
        Ok(())
    }

    fn add_symlink_entry(&mut self, source_path: &Path, archive_path: &Path, metadata: &fs::Metadata) -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(normalized_mode(metadata, false));
        let link_target = fs::read_link(source_path)?;
        header
            .set_link_name(&link_target)
            .map_err(|e| SurgeError::Archive(format!("Failed to set symlink target: {e}")))?;
        set_normalized_header_metadata(&mut header);
        header.set_cksum();

        self.builder
            .append_data(&mut header, archive_path_to_string(archive_path), std::io::empty())
            .map_err(|e| SurgeError::Archive(format!("Failed to add symlink: {e}")))?;
        Ok(())
    }
}

fn archive_child_path(prefix: &Path, child_name: &std::ffi::OsStr) -> PathBuf {
    if prefix.as_os_str().is_empty() {
        PathBuf::from(child_name)
    } else {
        prefix.join(child_name)
    }
}

fn archive_path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn set_normalized_header_metadata(header: &mut tar::Header) {
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::extractor::extract_to;

    #[test]
    fn test_packer_buffer_roundtrip() {
        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("test.txt", b"hello world", 0o644).unwrap();
        let data = packer.finalize().unwrap();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_add_directory_roundtrip_is_deterministic_after_extract_and_repack() {
        let tmp = tempfile::tempdir().unwrap();
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(source_dir.join("nested")).unwrap();
        std::fs::create_dir_all(source_dir.join("empty")).unwrap();
        std::fs::write(source_dir.join("nested").join("payload.txt"), b"payload").unwrap();
        std::fs::write(source_dir.join("root.txt"), b"root").unwrap();

        let mut first = ArchivePacker::new(7).unwrap();
        first.add_directory(&source_dir, "").unwrap();
        let archive = first.finalize().unwrap();

        let extracted_dir = tmp.path().join("extracted");
        extract_to(&archive, &extracted_dir, None).unwrap();

        let mut second = ArchivePacker::new(7).unwrap();
        second.add_directory(&extracted_dir, "").unwrap();
        let repacked = second.finalize().unwrap();

        assert_eq!(repacked, archive);
    }
}
