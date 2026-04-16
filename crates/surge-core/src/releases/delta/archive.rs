use std::io::Write;

use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff, chunked_bspatch};
use crate::diff::wrapper::{bsdiff_buffers, bspatch_buffers};
use crate::error::{Result, SurgeError};

pub(super) const LEGACY_ARCHIVE_BSDIFF_MAGIC: &[u8; 4] = b"ATB4";
pub(super) const LEGACY_ARCHIVE_CHUNKED_MAGIC: &[u8; 4] = b"ATC4";
pub(super) const ARCHIVE_BSDIFF_MAGIC: &[u8; 4] = b"ATB5";
pub(super) const ARCHIVE_CHUNKED_MAGIC: &[u8; 4] = b"ATC5";

const ARCHIVE_PATCH_HEADER_LEN: usize = 12;
const LEGACY_ARCHIVE_PATCH_HEADER_LEN: usize = 8;

pub fn build_archive_bsdiff_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older_archive)?;
    let newer_tar = decode_archive_bytes(newer_archive)?;
    let patch = bsdiff_buffers(&older_tar, &newer_tar)?;
    Ok(encode_archive_patch_payload(
        *ARCHIVE_BSDIFF_MAGIC,
        compression_level,
        zstd_workers,
        &patch,
    ))
}

pub fn build_archive_chunked_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    zstd_workers: u32,
    opts: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older_archive)?;
    let newer_tar = decode_archive_bytes(newer_archive)?;
    let patch = chunked_bsdiff(&older_tar, &newer_tar, opts)?;
    Ok(encode_archive_patch_payload(
        *ARCHIVE_CHUNKED_MAGIC,
        compression_level,
        zstd_workers,
        &patch,
    ))
}

pub(super) fn apply_archive_bsdiff_patch(older: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older)?;
    let (compression_level, zstd_workers, archive_patch) = decode_archive_patch_payload(
        patch,
        *ARCHIVE_BSDIFF_MAGIC,
        Some(*LEGACY_ARCHIVE_BSDIFF_MAGIC),
        Some(b"BSDIFF40"),
    )?;
    let newer_tar = bspatch_buffers(&older_tar, archive_patch)?;
    encode_archive_bytes(&newer_tar, compression_level, zstd_workers)
}

pub(super) fn apply_archive_chunked_patch(older: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older)?;
    let (compression_level, zstd_workers, archive_patch) = decode_archive_patch_payload(
        patch,
        *ARCHIVE_CHUNKED_MAGIC,
        Some(*LEGACY_ARCHIVE_CHUNKED_MAGIC),
        Some(b"CSDF"),
    )?;
    let newer_tar = chunked_bspatch(&older_tar, archive_patch, &ChunkedDiffOptions::default())?;
    encode_archive_bytes(&newer_tar, compression_level, zstd_workers)
}

pub(super) fn archive_patch_archive_encoding(data: &[u8], patch_format: &str) -> Result<Option<(i32, u32)>> {
    if patch_format.eq_ignore_ascii_case(crate::releases::manifest::PATCH_FORMAT_BSDIFF4_ARCHIVE_V3) {
        let (compression_level, zstd_workers, _) = decode_archive_patch_payload(
            data,
            *ARCHIVE_BSDIFF_MAGIC,
            Some(*LEGACY_ARCHIVE_BSDIFF_MAGIC),
            Some(b"BSDIFF40"),
        )?;
        return Ok(Some((compression_level, zstd_workers)));
    }

    if patch_format.eq_ignore_ascii_case(crate::releases::manifest::PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3) {
        let (compression_level, zstd_workers, _) = decode_archive_patch_payload(
            data,
            *ARCHIVE_CHUNKED_MAGIC,
            Some(*LEGACY_ARCHIVE_CHUNKED_MAGIC),
            Some(b"CSDF"),
        )?;
        return Ok(Some((compression_level, zstd_workers)));
    }

    Ok(None)
}

fn encode_archive_patch_payload(magic: [u8; 4], compression_level: i32, zstd_workers: u32, patch: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(ARCHIVE_PATCH_HEADER_LEN + patch.len());
    payload.extend_from_slice(&magic);
    payload.extend_from_slice(&compression_level.to_le_bytes());
    payload.extend_from_slice(&zstd_workers.to_le_bytes());
    payload.extend_from_slice(patch);
    payload
}

pub(super) fn decode_archive_patch_payload<'a>(
    data: &'a [u8],
    expected_magic: [u8; 4],
    legacy_magic: Option<[u8; 4]>,
    legacy_payload_magic: Option<&'static [u8]>,
) -> Result<(i32, u32, &'a [u8])> {
    if data.len() < LEGACY_ARCHIVE_PATCH_HEADER_LEN {
        return Err(SurgeError::Update("Archive delta payload is truncated".to_string()));
    }
    let matches_expected = data.starts_with(&expected_magic);
    let matches_legacy = legacy_magic.is_some_and(|magic| data.starts_with(&magic));
    if !matches_expected && !matches_legacy {
        return Err(SurgeError::Update("Archive delta payload magic is invalid".to_string()));
    }

    let compression_level = i32::from_le_bytes(
        data[expected_magic.len()..expected_magic.len() + std::mem::size_of::<i32>()]
            .try_into()
            .map_err(|_| SurgeError::Update("Archive delta payload header is invalid".to_string()))?,
    );

    if matches_legacy {
        let legacy_payload_offset = expected_magic.len() + std::mem::size_of::<i32>();
        if legacy_payload_magic.is_some_and(|magic| data[legacy_payload_offset..].starts_with(magic)) {
            return Ok((compression_level, 0, &data[legacy_payload_offset..]));
        }
    }

    if data.len() < ARCHIVE_PATCH_HEADER_LEN {
        return Err(SurgeError::Update(
            "Archive delta payload header is invalid".to_string(),
        ));
    }

    let worker_offset = expected_magic.len() + std::mem::size_of::<i32>();
    let zstd_workers = u32::from_le_bytes(
        data[worker_offset..ARCHIVE_PATCH_HEADER_LEN]
            .try_into()
            .map_err(|_| SurgeError::Update("Archive delta payload header is invalid".to_string()))?,
    );
    Ok((compression_level, zstd_workers, &data[ARCHIVE_PATCH_HEADER_LEN..]))
}

fn decode_archive_bytes(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("Failed to decode archive bytes: {e}")))
}

fn encode_archive_bytes(data: &[u8], compression_level: i32, zstd_workers: u32) -> Result<Vec<u8>> {
    if zstd_workers > 1 {
        let mut encoder = zstd::Encoder::new(Vec::new(), compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to create zstd encoder: {e}")))?;
        encoder
            .multithread(zstd_workers)
            .map_err(|e| SurgeError::Archive(format!("Failed to enable multi-threaded zstd: {e}")))?;
        encoder
            .write_all(data)
            .map_err(|e| SurgeError::Archive(format!("Failed to encode archive bytes: {e}")))?;
        return encoder
            .finish()
            .map_err(|e| SurgeError::Archive(format!("Failed to finalize zstd encoder: {e}")));
    }

    zstd::encode_all(data, compression_level)
        .map_err(|e| SurgeError::Archive(format!("Failed to encode archive bytes: {e}")))
}
