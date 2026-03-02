//! Create tar + zstd archives.

use std::io::Write;
use std::path::Path;

use crate::error::{Result, SurgeError};

/// Builder for creating tar.zst archives.
pub struct ArchivePacker {
    builder: tar::Builder<zstd::Encoder<'static, Vec<u8>>>,
}

impl ArchivePacker {
    /// Create a new archive packer with the given zstd compression level.
    pub fn new(compression_level: i32) -> Result<Self> {
        let encoder = zstd::Encoder::new(Vec::new(), compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to create zstd encoder: {e}")))?;
        let builder = tar::Builder::new(encoder);
        Ok(Self { builder })
    }

    /// Add a file from disk to the archive.
    pub fn add_file(&mut self, source: &Path, archive_path: &str) -> Result<()> {
        let mut file = std::fs::File::open(source)?;
        self.builder
            .append_file(archive_path, &mut file)
            .map_err(|e| SurgeError::Archive(format!("Failed to add file: {e}")))?;
        Ok(())
    }

    /// Add all files in a directory recursively.
    pub fn add_directory(&mut self, source_dir: &Path, prefix: &str) -> Result<()> {
        self.builder
            .append_dir_all(prefix, source_dir)
            .map_err(|e| SurgeError::Archive(format!("Failed to add directory: {e}")))?;
        Ok(())
    }

    /// Add a buffer as a file entry in the archive.
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

    /// Finalize the archive and return the compressed bytes.
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

    /// Finalize the archive and write to a file.
    pub fn finalize_to_file(self, path: &Path) -> Result<()> {
        let data = self.finalize()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(path)?;
        file.write_all(&data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packer_buffer_roundtrip() {
        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("test.txt", b"hello world", 0o644).unwrap();
        let data = packer.finalize().unwrap();
        assert!(!data.is_empty());
    }
}
