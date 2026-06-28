use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::releases::delta::{
    DeltaApplyProgress, apply_delta_patch_with_progress, decode_delta_patch, is_supported_delta,
};

use super::super::progress::{
    ProgressInfo, average_speed_bytes_per_sec, clamp_progress_percent, clamp_progress_percent_u64, emit_progress,
    phase_total_percent,
};
use super::super::progress_substep::{PhaseProgressEmitter, labels as apply_phase};
use super::super::{UpdateInfo, UpdateManager};

pub(super) async fn apply_target_deltas<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    mut rebuilt_archive: Vec<u8>,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
    apply_delta_total_items: i64,
    apply_delta_total_bytes: i64,
) -> Result<Vec<u8>>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let apply_delta_started_at = Instant::now();
    let mut apply_delta_items_done = 0i64;
    let mut apply_delta_bytes_done = 0i64;

    progress_emitter.emit_substep(5, apply_phase::APPLYING_TARGET_DELTAS, 60);
    for release in &info.apply_releases {
        manager.ctx.check_cancelled()?;

        let Some(delta) = release.selected_delta() else {
            return Err(SurgeError::Update(format!(
                "Delta update path is missing delta filename for {}",
                release.version
            )));
        };

        if !is_supported_delta(&delta) {
            return Err(SurgeError::Update(format!(
                "Delta {} for {} uses unsupported descriptor (algorithm='{}', format='{}', compression='{}')",
                delta.filename, release.version, delta.algorithm, delta.patch_format, delta.compression
            )));
        }

        let delta_path = staging_dir.join(&delta.filename);
        let delta_compressed = tokio::fs::read(&delta_path).await?;
        let patch = decode_delta_patch(delta_compressed.as_slice(), &delta)
            .map_err(|e| SurgeError::Archive(format!("Failed to decompress delta {}: {e}", delta.filename)))?;
        let progress_for_delta = progress.cloned();
        let completed_bytes_before_delta = apply_delta_bytes_done;
        let completed_items_before_delta = apply_delta_items_done;
        let current_delta_bytes = delta.size.max(0);
        let delta_progress = move |delta_progress: DeltaApplyProgress| {
            let bytes_done = completed_bytes_before_delta.saturating_add(scale_progress_units_i64(
                current_delta_bytes,
                delta_progress.units_done,
                delta_progress.units_total,
            ));
            let phase_percent = if apply_delta_total_bytes > 0 {
                clamp_progress_percent(bytes_done, apply_delta_total_bytes)
            } else {
                scale_apply_delta_items_percent(
                    completed_items_before_delta,
                    apply_delta_total_items,
                    delta_progress.units_done,
                    delta_progress.units_total,
                )
            };
            emit_progress(
                progress_for_delta.as_ref(),
                ProgressInfo {
                    phase: 5,
                    phase_label: apply_phase::APPLYING_TARGET_DELTAS,
                    phase_percent,
                    total_percent: phase_total_percent(60, 20, phase_percent),
                    bytes_done,
                    bytes_total: apply_delta_total_bytes,
                    items_done: completed_items_before_delta,
                    items_total: apply_delta_total_items,
                    speed_bytes_per_sec: average_speed_bytes_per_sec(
                        u64::try_from(bytes_done.max(0)).unwrap_or(u64::MAX),
                        apply_delta_started_at,
                    ),
                },
            );
            progress_emitter.persist_current_phase(apply_phase::APPLYING_TARGET_DELTAS);
        };

        rebuilt_archive = apply_delta_patch_with_progress(&rebuilt_archive, &patch, &delta, Some(&delta_progress))
            .map_err(|e| SurgeError::Update(format!("Failed to apply delta {}: {e}", delta.filename)))?;

        if !release.full_sha256.is_empty() {
            let hash = sha256_hex(&rebuilt_archive);
            if hash != release.full_sha256 {
                return Err(SurgeError::Update(format!(
                    "SHA-256 mismatch for rebuilt full archive {}: expected {}, got {hash}",
                    release.version, release.full_sha256
                )));
            }
        }

        apply_delta_items_done = apply_delta_items_done.saturating_add(1);
        apply_delta_bytes_done = apply_delta_bytes_done.saturating_add(delta.size.max(0));
        let phase_percent = clamp_progress_percent(apply_delta_items_done, apply_delta_total_items.max(1));
        emit_progress(
            progress,
            ProgressInfo {
                phase: 5,
                phase_label: apply_phase::APPLYING_TARGET_DELTAS,
                phase_percent,
                total_percent: phase_total_percent(60, 20, phase_percent),
                bytes_done: apply_delta_bytes_done,
                bytes_total: apply_delta_total_bytes,
                items_done: apply_delta_items_done,
                items_total: apply_delta_total_items,
                speed_bytes_per_sec: average_speed_bytes_per_sec(
                    u64::try_from(apply_delta_bytes_done.max(0)).unwrap_or(u64::MAX),
                    apply_delta_started_at,
                ),
            },
        );
    }

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            phase_label: apply_phase::APPLYING_TARGET_DELTAS,
            phase_percent: 100,
            total_percent: 80,
            bytes_done: apply_delta_total_bytes,
            bytes_total: apply_delta_total_bytes,
            items_done: apply_delta_total_items,
            items_total: apply_delta_total_items,
            speed_bytes_per_sec: average_speed_bytes_per_sec(
                u64::try_from(apply_delta_total_bytes.max(0)).unwrap_or(u64::MAX),
                apply_delta_started_at,
            ),
        },
    );

    Ok(rebuilt_archive)
}

fn scale_progress_units_i64(total: i64, done: u64, units_total: u64) -> i64 {
    if total <= 0 || units_total == 0 {
        return 0;
    }
    let total = u64::try_from(total).unwrap_or(u64::MAX);
    let scaled = total.saturating_mul(done.min(units_total)) / units_total;
    i64::try_from(scaled).unwrap_or(i64::MAX)
}

fn scale_apply_delta_items_percent(completed_items: i64, total_items: i64, done: u64, units_total: u64) -> i32 {
    let total_items = u64::try_from(total_items.max(1)).unwrap_or(u64::MAX);
    let completed_items = u64::try_from(completed_items.max(0)).unwrap_or(u64::MAX);
    let units_total = units_total.max(1);
    let done = done.min(units_total);
    let scaled_done = completed_items.saturating_mul(units_total).saturating_add(done);
    let scaled_total = total_items.saturating_mul(units_total);
    clamp_progress_percent_u64(scaled_done, scaled_total)
}
