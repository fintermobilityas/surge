use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::config::manifest::PackDeltaStrategy;
use crate::context::ResourceBudget;
use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::{ChunkedDiffOptions, DEFAULT_CHUNK_SIZE};
use crate::error::{Result, SurgeError};
use crate::releases::delta::{build_archive_bsdiff_patch, build_archive_chunked_patch, build_sparse_file_patch};
use crate::releases::manifest::{
    PATCH_FORMAT_BSDIFF4_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3, PATCH_FORMAT_SPARSE_FILE_OPS_V1,
    ReleaseIndex, decompress_release_index,
};
use crate::releases::restore::{
    find_previous_release_for_rid, restore_full_archive_for_version, sorted_releases_for_rid,
};

use tracing::{info, warn};

use super::{PackBuilder, PackageArtifact};

impl PackBuilder {
    pub(super) async fn build_delta_package(&self) -> Result<Option<PackageArtifact>> {
        let data = match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(d) => d,
            Err(SurgeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let index = decompress_release_index(&data)?;

        let Some(previous_release) = find_previous_release_for_rid(&index, &self.rid, &self.version) else {
            return Ok(None);
        };

        let budget = self.ctx.resource_budget();
        let prev_data =
            restore_full_archive_for_version(self.storage.as_ref(), &index, &self.rid, &previous_release.version)
                .await?;
        let new_data = self
            .artifacts
            .iter()
            .find(|a| !a.is_delta)
            .map(PackageArtifact::bytes)
            .ok_or_else(|| SurgeError::Pack("Full package not yet built".to_string()))?;
        if should_publish_checkpoint_full(
            &index,
            &self.rid,
            self.pack_policy.max_chain_length,
            self.pack_policy.checkpoint_every,
        ) {
            info!(
                app_id = %self.app_id,
                version = %self.version,
                rid = %self.rid,
                "Skipping delta package and publishing a checkpoint full"
            );
            return Ok(None);
        }
        let n_workers = budget.effective_zstd_workers();
        let diff_options = chunked_diff_options(&budget, prev_data.len(), new_data.len());

        let (patch, patch_format) = match self.pack_policy.delta_strategy {
            PackDeltaStrategy::SparseFileOps => (
                build_sparse_file_patch(
                    &prev_data,
                    new_data,
                    budget.zstd_compression_level,
                    n_workers,
                    &diff_options,
                )?,
                PATCH_FORMAT_SPARSE_FILE_OPS_V1.to_string(),
            ),
            PackDeltaStrategy::ArchiveChunkedBsdiff => (
                build_archive_chunked_patch(
                    &prev_data,
                    new_data,
                    budget.zstd_compression_level,
                    n_workers,
                    &diff_options,
                )?,
                PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3.to_string(),
            ),
            PackDeltaStrategy::ArchiveBsdiff => (
                build_archive_bsdiff_patch(&prev_data, new_data, budget.zstd_compression_level, n_workers)?,
                PATCH_FORMAT_BSDIFF4_ARCHIVE_V3.to_string(),
            ),
        };

        let delta_filename = format!("{}-{}-{}-delta.tar.zst", self.app_id, self.version, self.rid);
        let compressed = zstd_encode_mt(patch.as_slice(), budget.zstd_compression_level, n_workers)?;

        let sha256 = sha256_hex(&compressed);
        let size = i64::try_from(compressed.len())
            .map_err(|_| SurgeError::Archive(format!("Delta is too large: {} bytes", compressed.len())))?;
        let full_size = i64::try_from(new_data.len())
            .map_err(|_| SurgeError::Archive(format!("Archive is too large: {} bytes", new_data.len())))?;

        if should_fallback_to_full(size, full_size) {
            warn!(
                app_id = %self.app_id,
                version = %self.version,
                rid = %self.rid,
                delta_size = size,
                full_size,
                "Skipping pathological delta package and publishing a full checkpoint"
            );
            return Ok(None);
        }

        Ok(Some(PackageArtifact {
            filename: delta_filename,
            size,
            sha256,
            is_delta: true,
            from_version: previous_release.version.clone(),
            patch_format,
            zstd_compression_level: budget.zstd_compression_level,
            zstd_workers: n_workers,
            bytes: compressed,
        }))
    }
}

fn should_publish_checkpoint_full(
    index: &ReleaseIndex,
    rid: &str,
    max_chain_length: u32,
    checkpoint_every: u32,
) -> bool {
    let releases = sorted_releases_for_rid(index, rid);
    let mut deltas_since_checkpoint = 0u32;
    for release in releases.iter().rev() {
        if release.selected_delta().is_none() {
            break;
        }
        deltas_since_checkpoint = deltas_since_checkpoint.saturating_add(1);
    }

    deltas_since_checkpoint >= max_chain_length || deltas_since_checkpoint.saturating_add(1) >= checkpoint_every
}

fn should_fallback_to_full(delta_size: i64, full_size: i64) -> bool {
    if delta_size <= 0 || full_size <= 0 {
        return false;
    }
    delta_size >= full_size
}

fn chunked_diff_options(budget: &ResourceBudget, older_len: usize, newer_len: usize) -> ChunkedDiffOptions {
    const MIN_CHUNK_SIZE: usize = 4 * 1024 * 1024;
    const BYTES_PER_THREAD_FACTOR: usize = 12;

    let requested_threads = usize::try_from(budget.max_threads).ok().unwrap_or(0);
    let planning_threads = if requested_threads == 0 {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
    } else {
        requested_threads
    };
    let archive_len = older_len.max(newer_len).max(1);

    let mut chunk_size = DEFAULT_CHUNK_SIZE.min(archive_len);
    if let Ok(memory_budget) = usize::try_from(budget.max_memory_bytes)
        && memory_budget > 0
    {
        let per_thread_budget = memory_budget / planning_threads.max(1);
        let budget_chunk_size = per_thread_budget / BYTES_PER_THREAD_FACTOR;
        chunk_size = chunk_size.min(budget_chunk_size.max(MIN_CHUNK_SIZE));
    }
    chunk_size = chunk_size.clamp(1, archive_len.max(MIN_CHUNK_SIZE));

    let chunk_count = archive_len.div_ceil(chunk_size).max(1);

    ChunkedDiffOptions {
        chunk_size,
        max_threads: if requested_threads == 0 {
            0
        } else {
            requested_threads.min(chunk_count)
        },
    }
}

fn zstd_encode_mt(data: &[u8], compression_level: i32, n_workers: u32) -> Result<Vec<u8>> {
    if n_workers > 0 {
        use std::io::Write;
        let mut encoder = zstd::Encoder::new(Vec::new(), compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to create zstd encoder: {e}")))?;
        encoder
            .multithread(n_workers)
            .map_err(|e| SurgeError::Archive(format!("Failed to enable multi-threaded zstd: {e}")))?;
        encoder
            .write_all(data)
            .map_err(|e| SurgeError::Archive(format!("Failed to compress delta: {e}")))?;
        encoder
            .finish()
            .map_err(|e| SurgeError::Archive(format!("Failed to finalize zstd encoder: {e}")))
    } else {
        zstd::encode_all(data, compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to compress delta: {e}")))
    }
}
