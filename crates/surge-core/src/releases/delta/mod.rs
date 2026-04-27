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
pub use self::sparse_ops::build_sparse_file_patch;

/// Progress information for CPU/disk work while applying one delta artifact.
#[derive(Debug, Clone, Copy)]
pub struct DeltaApplyProgress {
    /// Work units completed within the current delta artifact.
    pub units_done: u64,
    /// Total work units expected for the current delta artifact.
    pub units_total: u64,
}

/// Callback used while rebuilding an archive from a delta artifact.
pub type DeltaApplyProgressCallback<'a> = dyn Fn(DeltaApplyProgress) + Send + Sync + 'a;

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
    apply_delta_patch_with_progress(older, patch, delta, None)
}

pub fn apply_delta_patch_with_progress(
    older: &[u8],
    patch: &[u8],
    delta: &DeltaArtifact,
    progress: Option<&DeltaApplyProgressCallback<'_>>,
) -> Result<Vec<u8>> {
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let algorithm = delta.algorithm.trim();

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        if !algorithm.is_empty() && !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS) {
            return Err(SurgeError::Update(format!(
                "Unsupported delta algorithm/format '{}/{}'",
                delta.algorithm, delta.patch_format
            )));
        }
        return sparse_ops::apply_sparse_file_patch_with_progress(older, patch, progress);
    }

    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);

    if !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF) {
        return Err(SurgeError::Update(format!(
            "Unsupported delta algorithm/format '{}/{}'",
            delta.algorithm, delta.patch_format
        )));
    }

    emit_delta_apply_progress(progress, 0, 1);
    let result = if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4) {
        bspatch_buffers(older, patch)
    } else if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1) {
        chunked_bspatch(older, patch, &ChunkedDiffOptions::default())
    } else if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3) {
        archive::apply_archive_bsdiff_patch(older, patch)
    } else if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3) {
        archive::apply_archive_chunked_patch(older, patch)
    } else {
        Err(SurgeError::Update(format!(
            "Unsupported delta algorithm/format '{}/{}'",
            delta.algorithm, delta.patch_format
        )))
    };
    if result.is_ok() {
        emit_delta_apply_progress(progress, 1, 1);
    }
    result
}

fn emit_delta_apply_progress(progress: Option<&DeltaApplyProgressCallback<'_>>, units_done: u64, units_total: u64) {
    if let Some(cb) = progress {
        cb(DeltaApplyProgress {
            units_done: units_done.min(units_total),
            units_total,
        });
    }
}

pub fn delta_target_archive_encoding(patch: &[u8], delta: &DeltaArtifact) -> Result<Option<(i32, u32)>> {
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let algorithm = delta.algorithm.trim();

    if patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1) {
        if !algorithm.is_empty() && !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS) {
            return Err(SurgeError::Update(format!(
                "Unsupported delta algorithm/format '{}/{}'",
                delta.algorithm, delta.patch_format
            )));
        }
        return sparse_ops::sparse_file_patch_archive_encoding(patch).map(Some);
    }

    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);
    if !algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF) {
        return Err(SurgeError::Update(format!(
            "Unsupported delta algorithm/format '{}/{}'",
            delta.algorithm, delta.patch_format
        )));
    }

    archive::archive_patch_archive_encoding(patch, patch_format)
}
