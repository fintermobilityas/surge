use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff, chunked_bspatch, has_magic_prefix};
use crate::diff::wrapper::{bsdiff_buffers, bspatch_buffers};
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    COMPRESSION_ZSTD, DIFF_ALGORITHM_BSDIFF, DeltaArtifact, PATCH_FORMAT_BSDIFF4, PATCH_FORMAT_BSDIFF4_ARCHIVE_V2,
    PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2, PATCH_FORMAT_CHUNKED_BSDIFF_V1,
};

const ARCHIVE_BSDIFF_MAGIC: &[u8; 4] = b"ATB4";
const ARCHIVE_CHUNKED_MAGIC: &[u8; 4] = b"ATC4";
const ARCHIVE_PATCH_HEADER_LEN: usize = 8;

fn normalized_or_default<'a>(value: &'a str, default: &'a str) -> &'a str {
    let trimmed = value.trim();
    if trimmed.is_empty() { default } else { trimmed }
}

#[must_use]
pub fn has_archive_bsdiff_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(ARCHIVE_BSDIFF_MAGIC)
}

#[must_use]
pub fn has_archive_chunked_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(ARCHIVE_CHUNKED_MAGIC)
}

#[must_use]
pub fn patch_format_from_magic_prefix(data: &[u8]) -> Option<&'static str> {
    if has_archive_chunked_magic_prefix(data) {
        return Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2);
    }
    if has_archive_bsdiff_magic_prefix(data) {
        return Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V2);
    }
    if has_magic_prefix(data) {
        return Some(PATCH_FORMAT_CHUNKED_BSDIFF_V1);
    }
    None
}

pub fn build_archive_bsdiff_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older_archive)?;
    let newer_tar = decode_archive_bytes(newer_archive)?;
    let patch = bsdiff_buffers(&older_tar, &newer_tar)?;
    Ok(encode_archive_patch_payload(
        *ARCHIVE_BSDIFF_MAGIC,
        compression_level,
        &patch,
    ))
}

pub fn build_archive_chunked_patch(
    older_archive: &[u8],
    newer_archive: &[u8],
    compression_level: i32,
    opts: &ChunkedDiffOptions,
) -> Result<Vec<u8>> {
    let older_tar = decode_archive_bytes(older_archive)?;
    let newer_tar = decode_archive_bytes(newer_archive)?;
    let patch = chunked_bsdiff(&older_tar, &newer_tar, opts)?;
    Ok(encode_archive_patch_payload(
        *ARCHIVE_CHUNKED_MAGIC,
        compression_level,
        &patch,
    ))
}

fn encode_archive_patch_payload(magic: [u8; 4], compression_level: i32, patch: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(ARCHIVE_PATCH_HEADER_LEN + patch.len());
    payload.extend_from_slice(&magic);
    payload.extend_from_slice(&compression_level.to_le_bytes());
    payload.extend_from_slice(patch);
    payload
}

fn decode_archive_patch_payload(data: &[u8], expected_magic: [u8; 4]) -> Result<(i32, &[u8])> {
    if data.len() < ARCHIVE_PATCH_HEADER_LEN {
        return Err(SurgeError::Update("Archive delta payload is truncated".to_string()));
    }
    if !data.starts_with(&expected_magic) {
        return Err(SurgeError::Update("Archive delta payload magic is invalid".to_string()));
    }

    let compression_level = i32::from_le_bytes(
        data[expected_magic.len()..ARCHIVE_PATCH_HEADER_LEN]
            .try_into()
            .map_err(|_| SurgeError::Update("Archive delta payload header is invalid".to_string()))?,
    );
    Ok((compression_level, &data[ARCHIVE_PATCH_HEADER_LEN..]))
}

fn decode_archive_bytes(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("Failed to decode archive bytes: {e}")))
}

fn encode_archive_bytes(data: &[u8], compression_level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, compression_level)
        .map_err(|e| SurgeError::Archive(format!("Failed to encode archive bytes: {e}")))
}

#[must_use]
pub fn is_supported_delta(delta: &DeltaArtifact) -> bool {
    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let compression = normalized_or_default(&delta.compression, COMPRESSION_ZSTD);

    algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF)
        && compression.eq_ignore_ascii_case(COMPRESSION_ZSTD)
        && (patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V2)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2))
}

pub fn decode_delta_patch(data: &[u8], delta: &DeltaArtifact) -> Result<Vec<u8>> {
    let compression = normalized_or_default(&delta.compression, COMPRESSION_ZSTD);
    if compression.eq_ignore_ascii_case(COMPRESSION_ZSTD) {
        return zstd::decode_all(data).map_err(|e| SurgeError::Archive(format!("{e}")));
    }
    Err(SurgeError::Update(format!(
        "Unsupported delta compression '{}'",
        delta.compression
    )))
}

pub fn apply_delta_patch(older: &[u8], patch: &[u8], delta: &DeltaArtifact) -> Result<Vec<u8>> {
    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);

    if !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF) {
        return Err(SurgeError::Update(format!(
            "Unsupported delta algorithm/format '{}/{}'",
            delta.algorithm, delta.patch_format
        )));
    }

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4) {
        return bspatch_buffers(older, patch);
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1) {
        return chunked_bspatch(older, patch, &ChunkedDiffOptions::default());
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V2) {
        let older_tar = decode_archive_bytes(older)?;
        let (compression_level, archive_patch) = decode_archive_patch_payload(patch, *ARCHIVE_BSDIFF_MAGIC)?;
        let newer_tar = bspatch_buffers(&older_tar, archive_patch)?;
        return encode_archive_bytes(&newer_tar, compression_level);
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2) {
        let older_tar = decode_archive_bytes(older)?;
        let (compression_level, archive_patch) = decode_archive_patch_payload(patch, *ARCHIVE_CHUNKED_MAGIC)?;
        let newer_tar = chunked_bspatch(&older_tar, archive_patch, &ChunkedDiffOptions::default())?;
        return encode_archive_bytes(&newer_tar, compression_level);
    }

    Err(SurgeError::Update(format!(
        "Unsupported delta algorithm/format '{}/{}'",
        delta.algorithm, delta.patch_format
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::crypto::sha256::sha256_hex;
    use crate::releases::manifest::DeltaArtifact;

    fn make_archive(version: &str, compression_level: i32) -> Vec<u8> {
        let mut packer = ArchivePacker::new(compression_level).unwrap();
        let banner = format!("console write for {version}\n");
        packer.add_buffer("Program.cs", banner.as_bytes(), 0o644).unwrap();
        packer
            .add_buffer("demoapp.csproj", b"<Project Sdk=\"Microsoft.NET.Sdk\" />\n", 0o644)
            .unwrap();
        packer
            .add_buffer("assets/payload.bin", &vec![b'Z'; 512 * 1024], 0o644)
            .unwrap();
        packer.finalize().unwrap()
    }

    #[test]
    fn test_patch_format_from_magic_prefix_detects_archive_formats() {
        assert_eq!(
            patch_format_from_magic_prefix(ARCHIVE_BSDIFF_MAGIC),
            Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V2)
        );
        assert_eq!(
            patch_format_from_magic_prefix(ARCHIVE_CHUNKED_MAGIC),
            Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2)
        );
    }

    #[test]
    fn test_archive_bsdiff_patch_roundtrip_rebuilds_full_archive_bytes() {
        let full_v1 = make_archive("1.0.0", 7);
        let full_v2 = make_archive("1.1.0", 7);
        let patch = build_archive_bsdiff_patch(&full_v1, &full_v2, 7).unwrap();
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::bsdiff_archive_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }

    #[test]
    fn test_archive_chunked_patch_roundtrip_rebuilds_full_archive_bytes() {
        let full_v1 = make_archive("1.0.0", 11);
        let full_v2 = make_archive("1.1.0", 11);
        let patch = build_archive_chunked_patch(&full_v1, &full_v2, 11, &ChunkedDiffOptions::default()).unwrap();
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
        let delta = DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta.tar.zst",
            i64::try_from(delta_bytes.len()).unwrap(),
            &sha256_hex(&delta_bytes),
        );

        let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
        let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v2);
    }

    #[test]
    fn test_archive_patch_payload_rejects_invalid_magic() {
        let err = decode_archive_patch_payload(b"BAD!\x03\0\0\0payload", *ARCHIVE_BSDIFF_MAGIC).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }
}
