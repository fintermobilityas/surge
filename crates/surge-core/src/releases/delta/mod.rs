mod archive;
mod format;
mod fs_apply;
mod sparse_ops;
#[cfg(test)]
mod tests;
mod tree;

use crate::diff::chunked::{ChunkedDiffOptions, chunked_bspatch};
use crate::diff::wrapper::bspatch_buffers;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    COMPRESSION_ZSTD, DIFF_ALGORITHM_BSDIFF, DIFF_ALGORITHM_FILE_OPS, DeltaArtifact, PATCH_FORMAT_BSDIFF4,
    PATCH_FORMAT_BSDIFF4_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_V1,
    PATCH_FORMAT_SPARSE_FILE_OPS_V1,
};

pub use self::archive::{build_archive_bsdiff_patch, build_archive_chunked_patch};
use self::format::normalized_or_default;
pub use self::format::{
    has_archive_bsdiff_magic_prefix, has_archive_chunked_magic_prefix, has_sparse_file_ops_magic_prefix,
    is_supported_delta, patch_format_from_magic_prefix,
};
pub(crate) use self::sparse_ops::apply_sparse_file_patch_to_directory;
pub use self::sparse_ops::build_sparse_file_patch;

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
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let algorithm = delta.algorithm.trim();

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        if !algorithm.is_empty() && !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS) {
            return Err(SurgeError::Update(format!(
                "Unsupported delta algorithm/format '{}/{}'",
                delta.algorithm, delta.patch_format
            )));
        }
        return sparse_ops::apply_sparse_file_patch(older, patch);
    }

    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);

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
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3) {
        return archive::apply_archive_bsdiff_patch(older, patch);
    }
    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3) {
        return archive::apply_archive_chunked_patch(older, patch);
    }

    Err(SurgeError::Update(format!(
        "Unsupported delta algorithm/format '{}/{}'",
        delta.algorithm, delta.patch_format
    )))
}

#[must_use]
pub fn is_sparse_file_ops_delta(delta: &DeltaArtifact) -> bool {
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    if !patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        return false;
    }

    let algorithm = delta.algorithm.trim();
    algorithm.is_empty() || algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS)
}
