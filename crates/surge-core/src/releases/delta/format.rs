use crate::diff::chunked::has_magic_prefix;
use crate::releases::manifest::{
    COMPRESSION_ZSTD, DIFF_ALGORITHM_BSDIFF, DIFF_ALGORITHM_FILE_OPS, DeltaArtifact, PATCH_FORMAT_BSDIFF4,
    PATCH_FORMAT_BSDIFF4_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_V1,
    PATCH_FORMAT_SPARSE_FILE_OPS_V1,
};

use super::archive::{
    ARCHIVE_BSDIFF_MAGIC, ARCHIVE_CHUNKED_MAGIC, LEGACY_ARCHIVE_BSDIFF_MAGIC, LEGACY_ARCHIVE_CHUNKED_MAGIC,
};
use super::sparse_ops::SPARSE_FILE_OPS_MAGIC;

pub(super) fn normalized_or_default<'a>(value: &'a str, default: &'a str) -> &'a str {
    let trimmed = value.trim();
    if trimmed.is_empty() { default } else { trimmed }
}

#[must_use]
pub fn has_archive_bsdiff_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(ARCHIVE_BSDIFF_MAGIC) || data.starts_with(LEGACY_ARCHIVE_BSDIFF_MAGIC)
}

#[must_use]
pub fn has_archive_chunked_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(ARCHIVE_CHUNKED_MAGIC) || data.starts_with(LEGACY_ARCHIVE_CHUNKED_MAGIC)
}

#[must_use]
pub fn has_sparse_file_ops_magic_prefix(data: &[u8]) -> bool {
    data.starts_with(SPARSE_FILE_OPS_MAGIC)
}

#[must_use]
pub fn patch_format_from_magic_prefix(data: &[u8]) -> Option<&'static str> {
    if has_sparse_file_ops_magic_prefix(data) {
        return Some(PATCH_FORMAT_SPARSE_FILE_OPS_V1);
    }
    if has_archive_chunked_magic_prefix(data) {
        return Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3);
    }
    if has_archive_bsdiff_magic_prefix(data) {
        return Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3);
    }
    if has_magic_prefix(data) {
        return Some(PATCH_FORMAT_CHUNKED_BSDIFF_V1);
    }
    None
}

#[must_use]
pub fn is_supported_delta(delta: &DeltaArtifact) -> bool {
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let compression = normalized_or_default(&delta.compression, COMPRESSION_ZSTD);
    let algorithm = delta.algorithm.trim();

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        return compression.eq_ignore_ascii_case(COMPRESSION_ZSTD)
            && (algorithm.is_empty() || algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS));
    }

    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);

    algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF)
        && compression.eq_ignore_ascii_case(COMPRESSION_ZSTD)
        && (patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3))
}
