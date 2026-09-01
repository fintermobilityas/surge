use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::warn;

use crate::archive::extractor::extract_file_to_with_progress;
use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{cache_path_for_key, fetch_or_reuse_file};
use crate::releases::manifest::ReleaseEntry;

use super::progress::{
    ProgressInfo, average_speed_bytes_per_sec, clamp_progress_percent_u64, emit_progress, phase_total_percent,
    saturating_i64_from_u64,
};
use super::progress_substep::{HEARTBEAT_INTERVAL, PhaseProgressEmitter, labels as apply_phase};
use super::{ApplyStrategy, UpdateInfo, UpdateManager};

mod base;
mod delta;
pub(in crate::update::manager) mod full_fallback;
mod installed_app;

use self::base::{BaseFullArchiveSource, restore_base_full_archive, restore_release_graph_base_full_archive};
use self::delta::apply_target_deltas;
pub(super) use self::installed_app::find_previous_app_dir;
#[cfg(test)]
pub(super) use self::installed_app::synthesize_current_full_archive_from_installed_app;

/// Maximum verification-failure passes a single update attempt may pay for
/// while restoring and applying the current package.
///
/// The materialization ladder intentionally re-runs expensive work after
/// verification failures (installed-app base -> release-graph base -> full
/// package fallback), which is what makes a single corrupted artifact cheap
/// to recover from. When the whole chain is corrupt, however, every pass
/// fails verification and the attempt can pay for GB-scale restore/apply
/// work with no chance of converging (see #237). The budget caps how many
/// failed verification passes one attempt pays for; the failure beyond the
/// budget aborts the attempt with a readable, counted error and skips the
/// remaining expensive passes, so the failure is visible instead of
/// masquerading as healthy progress.
pub(super) const MAX_VERIFY_FAILURES: u32 = 3;

/// Marker prefix of the budget-abort error so the retry ladder can tell an
/// abort apart from an ordinary verification failure (which it would
/// otherwise retry with another expensive pass).
const VERIFY_ABORT_MARKER: &str = "verification failure budget exhausted";

/// Verification-failure budget for one update attempt. Each verification
/// failure (hash mismatch, patch decode/apply failure, corrupted artifact)
/// consumes the budget; once it is exhausted the attempt must fail with a
/// readable error instead of re-running expensive work.
pub(super) struct VerifyFailureBudget {
    failures: Vec<String>,
}

impl VerifyFailureBudget {
    #[must_use]
    pub(super) fn new() -> Self {
        Self { failures: Vec::new() }
    }

    /// Record a verification failure. Returns `Ok(())` while the budget is
    /// not exhausted and an abort error on the first failure beyond it.
    pub(super) fn record_failure(&mut self, context: &str) -> Result<()> {
        self.failures.push(context.to_string());
        if self.failures.len() <= usize::try_from(MAX_VERIFY_FAILURES).unwrap_or(usize::MAX) {
            return Ok(());
        }
        let last = self.failures.last().map_or("", String::as_str);
        Err(SurgeError::Update(format!(
            "{VERIFY_ABORT_MARKER}: {} verification failures in this attempt ({} allowed, last: {last}); the release artifacts for this update chain appear corrupt or inconsistent. The attempt fails and backs off; retry later or republish the affected release.",
            self.failures.len(),
            MAX_VERIFY_FAILURES
        )))
    }
}

/// Whether an error indicates a verification failure (content/hash mismatch
/// or a patch that could not be decoded/applied) as opposed to a transient
/// or environmental failure (network, IO, missing object).
fn is_verification_failure(error: &SurgeError) -> bool {
    match error {
        SurgeError::Integrity(_) | SurgeError::Diff(_) => true,
        SurgeError::Archive(message) | SurgeError::Update(message) | SurgeError::Storage(message) => {
            message.contains("SHA-256 mismatch")
                || message.contains("Failed to apply delta")
                || message.contains("Failed to decode")
                || message.contains("Failed to decompress")
        }
        _ => false,
    }
}

fn is_verify_abort(error: &SurgeError) -> bool {
    matches!(error, SurgeError::Update(message) if message.contains(VERIFY_ABORT_MARKER))
}

pub(super) async fn materialize_update_payload<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let mut verify_budget = VerifyFailureBudget::new();
    if matches!(info.apply_strategy, ApplyStrategy::Delta) {
        match materialize_delta_payload(
            manager,
            info,
            staging_dir,
            artifact_cache_dir,
            extract_dir,
            progress,
            progress_emitter,
            &mut verify_budget,
        )
        .await
        {
            Ok(path) => Ok(path),
            Err(SurgeError::Cancelled) => Err(SurgeError::Cancelled),
            Err(delta_error) if is_verify_abort(&delta_error) => {
                // The verification budget is exhausted: failing now (with the
                // bounded-abort error) instead of re-running the expensive
                // full-package pass is the point of the budget.
                Err(delta_error)
            }
            Err(delta_error) => {
                materialize_full_payload_after_delta_failure(
                    manager,
                    info,
                    staging_dir,
                    artifact_cache_dir,
                    extract_dir,
                    progress,
                    delta_error,
                    &mut verify_budget,
                )
                .await
            }
        }
    } else {
        materialize_full_payload(info, staging_dir, extract_dir, progress)
    }
}

fn materialize_full_payload<F>(
    info: &UpdateInfo,
    staging_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    materialize_full_payload_with_progress_range(info, staging_dir, extract_dir, progress, 60, 75, 80, 85)
}

fn materialize_full_payload_with_progress_range<F>(
    info: &UpdateInfo,
    staging_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    extract_total_percent_start: i32,
    extract_total_percent_end: i32,
    apply_total_percent_start: i32,
    apply_total_percent_end: i32,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
    let archive_path = staging_dir.join(&latest.full_filename);
    extract_archive_with_progress(
        &archive_path,
        extract_dir,
        progress,
        extract_total_percent_start,
        extract_total_percent_end,
    )?;

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            total_percent: apply_total_percent_start,
            ..ProgressInfo::default()
        },
    );
    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            phase_percent: 100,
            total_percent: apply_total_percent_end,
            ..ProgressInfo::default()
        },
    );

    Ok(extract_dir.to_path_buf())
}

async fn materialize_full_payload_after_delta_failure<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    delta_error: SurgeError,
    verify_budget: &mut VerifyFailureBudget,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;

    warn!(
        version = %latest.version,
        error = %delta_error,
        "Delta materialization failed; falling back to the full package"
    );

    let cache_path = cache_path_for_key(artifact_cache_dir, &latest.full_filename)?;
    let download_started_at = Instant::now();
    let full_size = u64::try_from(latest.full_size.max(0)).unwrap_or(u64::MAX);
    emit_progress(
        progress,
        ProgressInfo {
            phase: 2,
            phase_label: "download full package fallback",
            total_percent: 60,
            bytes_total: saturating_i64_from_u64(full_size),
            items_total: 1,
            ..ProgressInfo::default()
        },
    );
    let progress_for_download = progress.cloned();
    let download_progress = move |done: u64, total: u64| {
        let total = total.max(done).max(full_size);
        let phase_percent = clamp_progress_percent_u64(done, total);
        emit_progress(
            progress_for_download.as_ref(),
            ProgressInfo {
                phase: 2,
                phase_label: "download full package fallback",
                phase_percent,
                total_percent: phase_total_percent(60, 15, phase_percent),
                bytes_done: saturating_i64_from_u64(done),
                bytes_total: saturating_i64_from_u64(total),
                items_done: i64::from(phase_percent == 100),
                items_total: 1,
                speed_bytes_per_sec: average_speed_bytes_per_sec(done, download_started_at),
            },
        );
    };

    let fetched = fetch_or_reuse_file(
        manager.storage.as_ref(),
        &latest.full_filename,
        &cache_path,
        &latest.full_sha256,
        Some(&download_progress),
    )
    .await;
    // Publishers skip the full upload for non-checkpoint releases (see checkpoint retention), so the
    // release's own full object may legitimately be absent. Rebuild it from the newest available full
    // and the delta chain instead of failing the update.
    let fetched = match fetched {
        Err(SurgeError::NotFound(missing)) => {
            warn!(
                version = %latest.version,
                key = %latest.full_filename,
                "Full package is not in storage ({missing}); rebuilding it from the release graph"
            );
            full_fallback::restore_full_into_cache(manager, latest, &cache_path)
                .await
                .map(|()| ())
                .map_err(|restore_error| {
                    SurgeError::Update(format!(
                        "Delta materialization failed: {delta_error}; full package fallback failed: {missing}; \
                         rebuilding the full package from the release graph failed: {restore_error}"
                    ))
                })
        }
        other => other.map(|_| ()),
    };
    fetched.map_err(|fallback_error| {
        if is_verification_failure(&fallback_error) {
            // Terminal path: if this verification failure exhausts the
            // budget, surface the bounded-abort error instead of the raw
            // download error so the failure reason names the pattern.
            if let Err(abort) = verify_budget.record_failure(&format!(
                "full package fallback download of {}: {fallback_error}",
                latest.full_filename
            )) {
                return abort;
            }
        }
        SurgeError::Update(format!(
            "Delta materialization failed: {delta_error}; full package fallback failed: {fallback_error}"
        ))
    })?;

    let stage_path = staging_dir.join(&latest.full_filename);
    if let Some(parent) = stage_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(&cache_path, &stage_path).await?;
    if extract_dir.exists() {
        tokio::fs::remove_dir_all(extract_dir).await?;
    }
    tokio::fs::create_dir_all(extract_dir).await?;

    materialize_full_payload_with_progress_range(info, staging_dir, extract_dir, progress, 75, 90, 90, 90)
}

async fn materialize_delta_payload<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    progress_emitter: &PhaseProgressEmitter<'_, F>,
    verify_budget: &mut VerifyFailureBudget,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let apply_delta_total_items = i64::try_from(info.apply_releases.len()).unwrap_or(i64::MAX);
    let apply_delta_total_bytes = info
        .apply_releases
        .iter()
        .filter_map(ReleaseEntry::selected_delta)
        .fold(0i64, |acc, delta| acc.saturating_add(delta.size.max(0)));

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            total_percent: 60,
            bytes_total: apply_delta_total_bytes,
            items_total: apply_delta_total_items,
            ..ProgressInfo::default()
        },
    );

    let base_archive = restore_base_full_archive(
        manager,
        info,
        artifact_cache_dir,
        progress,
        progress_emitter,
        verify_budget,
    )
    .await?;
    let rebuilt_archive = match apply_target_deltas(
        manager,
        info,
        staging_dir,
        base_archive.archive,
        progress,
        progress_emitter,
        apply_delta_total_items,
        apply_delta_total_bytes,
        verify_budget,
    )
    .await
    {
        Ok(archive) => archive,
        Err(delta_error)
            if base_archive.source == BaseFullArchiveSource::InstalledApp
                && !is_verify_abort(&delta_error)
                && should_retry_delta_with_release_graph(&delta_error) =>
        {
            warn!(
                error = %delta_error,
                "Installed app base did not produce a valid delta result; retrying with release graph base"
            );
            let release_graph_base = restore_release_graph_base_full_archive(
                manager,
                artifact_cache_dir,
                progress,
                progress_emitter,
                verify_budget,
            )
            .await?;
            apply_target_deltas(
                manager,
                info,
                staging_dir,
                release_graph_base.archive,
                progress,
                progress_emitter,
                apply_delta_total_items,
                apply_delta_total_bytes,
                verify_budget,
            )
            .await
            .map_err(|retry_error| {
                SurgeError::Update(format!(
                    "Installed-app delta application failed: {delta_error}; release-graph retry failed: {retry_error}"
                ))
            })?
        }
        Err(delta_error) => return Err(delta_error),
    };

    let rebuilt_archive_path = staging_dir.join("rebuilt-full.tar.zst");
    progress_emitter
        .run_with_heartbeat(
            5,
            apply_phase::WRITING_REBUILT_PACKAGE,
            80,
            HEARTBEAT_INTERVAL,
            tokio::fs::write(&rebuilt_archive_path, &rebuilt_archive),
        )
        .await?;
    progress_emitter.emit_substep(5, apply_phase::EXTRACTING_REBUILT_PACKAGE, 80);
    extract_archive_with_progress(&rebuilt_archive_path, extract_dir, progress, 80, 90)?;

    let source = extract_dir.join(&info.latest_version);
    if source.exists() {
        Ok(source)
    } else {
        Ok(extract_dir.to_path_buf())
    }
}

fn should_retry_delta_with_release_graph(error: &SurgeError) -> bool {
    matches!(
        error,
        SurgeError::Update(_) | SurgeError::Integrity(_) | SurgeError::Diff(_) | SurgeError::Archive(_)
    )
}

fn extract_archive_with_progress<F>(
    archive_path: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
    total_percent_start: i32,
    total_percent_end: i32,
) -> Result<()>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    emit_progress(
        progress,
        ProgressInfo {
            phase: 4,
            total_percent: total_percent_start,
            ..ProgressInfo::default()
        },
    );

    let extract_started_at = Instant::now();
    let progress_for_extract = progress.cloned();
    let extract_progress = move |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
        let phase_percent = if bytes_total > 0 {
            clamp_progress_percent_u64(bytes_done, bytes_total)
        } else {
            clamp_progress_percent_u64(items_done, items_total)
        };
        emit_progress(
            progress_for_extract.as_ref(),
            ProgressInfo {
                phase: 4,
                phase_percent,
                total_percent: phase_total_percent(
                    total_percent_start,
                    total_percent_end - total_percent_start,
                    phase_percent,
                ),
                bytes_done: saturating_i64_from_u64(bytes_done),
                bytes_total: saturating_i64_from_u64(bytes_total),
                items_done: saturating_i64_from_u64(items_done),
                items_total: saturating_i64_from_u64(items_total),
                speed_bytes_per_sec: average_speed_bytes_per_sec(bytes_done, extract_started_at),
                ..ProgressInfo::default()
            },
        );
    };

    extract_file_to_with_progress(archive_path, extract_dir, Some(&extract_progress))?;

    emit_progress(
        progress,
        ProgressInfo {
            phase: 4,
            phase_percent: 100,
            total_percent: total_percent_end,
            ..ProgressInfo::default()
        },
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_VERIFY_FAILURES, VerifyFailureBudget, is_verification_failure, is_verify_abort};
    use crate::error::SurgeError;

    #[test]
    fn verify_budget_allows_bounded_failures_then_aborts() {
        let mut budget = VerifyFailureBudget::new();
        for attempt in 1..=usize::try_from(MAX_VERIFY_FAILURES).unwrap() {
            budget
                .record_failure(&format!("failure {attempt}"))
                .unwrap_or_else(|e| panic!("failure {attempt} should be within budget: {e}"));
        }
        let abort = budget
            .record_failure("one failure too many")
            .expect_err("the failure beyond the budget must abort");
        let message = abort.to_string();
        assert!(
            message.contains("verification failure budget exhausted"),
            "message was: {message}"
        );
        assert!(message.contains("one failure too many"), "message was: {message}");
        assert!(
            is_verify_abort(&abort),
            "the abort error must be recognized by the ladder"
        );
    }

    #[test]
    fn verify_abort_is_not_ordinary_verification_failure() {
        let mut budget = VerifyFailureBudget::new();
        for _ in 0..=MAX_VERIFY_FAILURES {
            let _ = budget.record_failure("fill");
        }
        let abort = budget.record_failure("overflow").unwrap_err();
        assert!(
            !is_verification_failure(&abort),
            "an abort must not be charged again by another ladder step"
        );
    }

    #[test]
    fn verification_failure_classification_covers_hash_and_patch_errors() {
        assert!(is_verification_failure(&SurgeError::Integrity(
            "SHA-256 mismatch".into()
        )));
        assert!(is_verification_failure(&SurgeError::Diff("patch corrupt".into())));
        assert!(is_verification_failure(&SurgeError::Update(
            "SHA-256 mismatch for rebuilt full archive".into()
        )));
        assert!(is_verification_failure(&SurgeError::Storage(
            "SHA-256 mismatch after download".into()
        )));
        assert!(is_verification_failure(&SurgeError::Archive(
            "Failed to decode delta artifact".into()
        )));
        assert!(is_verification_failure(&SurgeError::Archive(
            "Failed to decompress delta artifact: corrupt".into()
        )));

        assert!(!is_verification_failure(&SurgeError::Storage(
            "connection reset".into()
        )));
        assert!(!is_verification_failure(&SurgeError::NotFound("object missing".into())));
        assert!(!is_verification_failure(&SurgeError::Cancelled));
    }
}
