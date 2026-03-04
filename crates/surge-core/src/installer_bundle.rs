use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Result, SurgeError};

const FOOTER_MAGIC: [u8; 8] = *b"SRGINST1";
const FOOTER_SIZE: usize = FOOTER_MAGIC.len() + 8 + 8;

pub fn write_embedded_installer(launcher: &Path, payload_archive: &Path, output: &Path) -> Result<()> {
    let mut launcher_file = File::open(launcher).map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to open installer launcher '{}': {e}",
            launcher.display()
        ))
    })?;
    let mut payload_file = File::open(payload_archive).map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to open installer payload archive '{}': {e}",
            payload_archive.display()
        ))
    })?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut output_file = File::create(output)
        .map_err(|e| SurgeError::Pack(format!("Failed to create installer '{}': {e}", output.display())))?;

    std::io::copy(&mut launcher_file, &mut output_file).map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to write launcher bytes into '{}': {e}",
            output.display()
        ))
    })?;
    let payload_offset = output_file
        .stream_position()
        .map_err(|e| SurgeError::Pack(format!("Failed to determine payload offset: {e}")))?;
    let payload_len = std::io::copy(&mut payload_file, &mut output_file)
        .map_err(|e| SurgeError::Pack(format!("Failed to append payload archive bytes: {e}")))?;

    output_file
        .write_all(&FOOTER_MAGIC)
        .map_err(|e| SurgeError::Pack(format!("Failed to write installer footer magic: {e}")))?;
    output_file
        .write_all(&payload_offset.to_le_bytes())
        .map_err(|e| SurgeError::Pack(format!("Failed to write installer footer offset: {e}")))?;
    output_file
        .write_all(&payload_len.to_le_bytes())
        .map_err(|e| SurgeError::Pack(format!("Failed to write installer footer length: {e}")))?;
    output_file
        .flush()
        .map_err(|e| SurgeError::Pack(format!("Failed to flush installer output: {e}")))?;

    Ok(())
}

pub fn read_embedded_payload(executable: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(executable).map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to open installer executable '{}': {e}",
            executable.display()
        ))
    })?;
    let len = file
        .metadata()
        .map_err(|e| {
            SurgeError::Pack(format!(
                "Failed to read installer metadata '{}': {e}",
                executable.display()
            ))
        })?
        .len();
    if len < FOOTER_SIZE as u64 {
        return Err(SurgeError::Pack(format!(
            "Installer '{}' does not contain an embedded payload footer",
            executable.display()
        )));
    }

    file.seek(SeekFrom::Start(len - FOOTER_SIZE as u64))
        .map_err(|e| SurgeError::Pack(format!("Failed to seek installer footer: {e}")))?;

    let mut footer = [0u8; FOOTER_SIZE];
    file.read_exact(&mut footer)
        .map_err(|e| SurgeError::Pack(format!("Failed to read installer footer: {e}")))?;

    if footer[..FOOTER_MAGIC.len()] != FOOTER_MAGIC {
        return Err(SurgeError::Pack(format!(
            "Installer '{}' has invalid embedded payload footer magic",
            executable.display()
        )));
    }

    let offset_start = FOOTER_MAGIC.len();
    let len_start = offset_start + 8;
    let mut offset_bytes = [0u8; 8];
    offset_bytes.copy_from_slice(&footer[offset_start..len_start]);
    let mut payload_len_bytes = [0u8; 8];
    payload_len_bytes.copy_from_slice(&footer[len_start..len_start + 8]);

    let payload_offset = u64::from_le_bytes(offset_bytes);
    let payload_len = u64::from_le_bytes(payload_len_bytes);
    let payload_end = payload_offset.saturating_add(payload_len);
    let footer_start = len - FOOTER_SIZE as u64;
    if payload_end > footer_start {
        return Err(SurgeError::Pack(format!(
            "Installer '{}' has an invalid payload range ({payload_offset}..{payload_end})",
            executable.display()
        )));
    }

    let payload_size = usize::try_from(payload_len)
        .map_err(|_| SurgeError::Pack(format!("Embedded payload is too large ({payload_len} bytes)")))?;
    let mut payload = vec![0u8; payload_size];
    file.seek(SeekFrom::Start(payload_offset))
        .map_err(|e| SurgeError::Pack(format!("Failed to seek embedded payload: {e}")))?;
    file.read_exact(&mut payload)
        .map_err(|e| SurgeError::Pack(format!("Failed to read embedded payload: {e}")))?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::extractor::read_entry;
    use crate::archive::packer::ArchivePacker;

    #[test]
    fn embedded_installer_roundtrip() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");
        let launcher = tmp.path().join("surge-installer");
        std::fs::write(&launcher, b"launcher-bytes").expect("launcher should be written");

        let payload_archive = tmp.path().join("payload.tar.zst");
        let mut packer = ArchivePacker::new(1).expect("packer should be created");
        packer
            .add_buffer("installer.yml", b"schema: 1\n", 0o644)
            .expect("manifest should be added");
        packer
            .finalize_to_file(&payload_archive)
            .expect("payload archive should be written");

        let installer = tmp.path().join("Setup.bin");
        write_embedded_installer(&launcher, &payload_archive, &installer).expect("installer should be produced");
        let payload = read_embedded_payload(&installer).expect("payload should be readable");
        let installer_manifest = read_entry(&payload, "installer.yml").expect("installer manifest should exist");
        assert_eq!(installer_manifest, b"schema: 1\n");
    }
}
