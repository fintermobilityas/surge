use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use crate::archive::extractor::extract_to;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::releases::delta::{
    DeltaApplyProgress, apply_delta_patch_with_progress, apply_sparse_step_in_place, decode_delta_patch,
    is_supported_delta, sparse_step_units_for,
};
use crate::releases::manifest::{DIFF_ALGORITHM_FILE_OPS, DeltaArtifact, PATCH_FORMAT_SPARSE_FILE_OPS_V1};

use super::super::progress::{
    ProgressInfo, average_speed_bytes_per_sec, clamp_progress_percent, clamp_progress_percent_u64, emit_progress,
    phase_total_percent,
};
use super::super::progress_substep::{PhaseProgressEmitter, labels as apply_phase};
use super::super::{UpdateInfo, UpdateManager};
use super::{VerifyFailureBudget, is_verification_failure};

fn is_sparse_file_ops_delta(delta: &DeltaArtifact) -> bool {
    delta.patch_format.eq_ignore_ascii_case(PATCH_FORMAT_SPARSE_FILE_OPS_V1)
        && (delta.algorithm.trim().is_empty() || delta.algorithm.eq_ignore_ascii_case(DIFF_ALGORITHM_FILE_OPS))
}

pub(super) async fn apply_target_deltas<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    mut rebuilt_archive: Vec<u8>,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
    apply_delta_total_items: i64,
    apply_delta_total_bytes: i64,
    verify_budget: &mut VerifyFailureBudget,
) -> Result<Vec<u8>>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let apply_delta_started_at = Instant::now();
    let mut apply_delta_items_done = 0i64;
    let mut apply_delta_bytes_done = 0i64;
    // Carried extracted tree for consecutive sparse deltas: the starting
    // archive is extracted once and each step applies ops in place,
    // skipping the per-step re-extract. Per-step repack and the full
    // SHA-256 check below are unchanged.
    let mut chain_workdir: Option<tempfile::TempDir> = None;

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
        let patch = decode_delta_patch(delta_compressed.as_slice(), &delta).map_err(|e| {
            let error = SurgeError::Archive(format!("Failed to decompress delta {}: {e}", delta.filename));
            if is_verification_failure(&error) {
                // A budget-exhausting failure returns the bounded-abort
                // error instead of the raw decode error.
                if let Err(abort) = verify_budget.record_failure(&error.to_string()) {
                    return abort;
                }
            }
            error
        })?;
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

        let next_archive: Result<Vec<u8>> = if is_sparse_file_ops_delta(&delta) {
            let (existing, needs_extract) = match chain_workdir.take() {
                Some(wd) => (Some(wd), false),
                None => (None, true),
            };
            let extract_units = if needs_extract {
                u64::try_from(rebuilt_archive.len()).unwrap_or(u64::MAX).max(1)
            } else {
                0
            };
            let step_units = sparse_step_units_for(&patch, extract_units)?;
            let total_units = extract_units.saturating_add(step_units);
            let workdir = match existing {
                Some(wd) => wd,
                None => {
                    let wd = tempfile::tempdir()?;
                    let extract_progress = |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
                        let frac = if bytes_total > 0 {
                            bytes_done
                                .checked_mul(1000)
                                .and_then(|num| num.checked_div(bytes_total))
                                .unwrap_or(1000)
                        } else {
                            items_done
                                .checked_mul(1000)
                                .and_then(|num| num.checked_div(items_total.max(1)))
                                .unwrap_or(1000)
                        };
                        delta_progress(DeltaApplyProgress {
                            units_done: extract_units.saturating_mul(frac) / 1000,
                            units_total: total_units,
                        });
                    };
                    extract_to(
                        &rebuilt_archive,
                        wd.path(),
                        Some(&extract_progress as &crate::archive::extractor::ExtractProgress<'_>),
                    )?;
                    wd
                }
            };
            let applied = apply_sparse_step_in_place(workdir.path(), &patch, extract_units, Some(&delta_progress))?;
            chain_workdir = Some(workdir);
            Ok(applied)
        } else {
            // A non-sparse hop rebuilds from archive bytes; the carried
            // tree is stale at that point and gets dropped.
            apply_delta_patch_with_progress(&rebuilt_archive, &patch, &delta, Some(&delta_progress))
        };
        rebuilt_archive = next_archive.map_err(|e| {
            let error = SurgeError::Update(format!("Failed to apply delta {}: {e}", delta.filename));
            if is_verification_failure(&error) {
                // A budget-exhausting failure returns the bounded-abort
                // error instead of the raw apply error.
                if let Err(abort) = verify_budget.record_failure(&error.to_string()) {
                    return abort;
                }
            }
            error
        })?;

        if !release.full_sha256.is_empty() {
            let hash = sha256_hex(&rebuilt_archive);
            if hash != release.full_sha256 {
                let message = format!(
                    "SHA-256 mismatch for rebuilt full archive {}: expected {}, got {hash}",
                    release.version, release.full_sha256
                );
                verify_budget.record_failure(&message)?;
                return Err(SurgeError::Update(message));
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
