use crate::diff::chunked::chunked_bspatch;
use crate::diff::wrapper::bspatch_buffers;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    COMPRESSION_ZSTD, DIFF_ALGORITHM_BSDIFF, DeltaArtifact, PATCH_FORMAT_BSDIFF4, PATCH_FORMAT_CHUNKED_BSDIFF_V1,
};

fn normalized_or_default<'a>(value: &'a str, default: &'a str) -> &'a str {
    let trimmed = value.trim();
    if trimmed.is_empty() { default } else { trimmed }
}

#[must_use]
pub fn is_supported_delta(delta: &DeltaArtifact) -> bool {
    let algorithm = normalized_or_default(&delta.algorithm, DIFF_ALGORITHM_BSDIFF);
    let patch_format = normalized_or_default(&delta.patch_format, PATCH_FORMAT_BSDIFF4);
    let compression = normalized_or_default(&delta.compression, COMPRESSION_ZSTD);

    algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_BSDIFF)
        && compression.eq_ignore_ascii_case(COMPRESSION_ZSTD)
        && (patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4)
            || patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1))
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
        return chunked_bspatch(older, patch, &Default::default());
    }

    Err(SurgeError::Update(format!(
        "Unsupported delta algorithm/format '{}/{}'",
        delta.algorithm, delta.patch_format
    )))
}
