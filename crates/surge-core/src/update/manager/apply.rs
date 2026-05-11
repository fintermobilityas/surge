use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, warn};

use crate::archive::extractor::extract_file_to_with_progress;
use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::context::Context;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::pack::builder::build_canonical_archive_from_directory;
use crate::platform::detect::current_rid;
use crate::platform::fs::write_file_atomic;
use crate::releases::artifact_cache::cache_path_for_key;
use crate::releases::delta::{
    DeltaApplyProgress, apply_delta_patch_with_progress, decode_delta_patch, is_supported_delta,
};
use crate::releases::manifest::{ReleaseEntry, decompress_release_index};
use crate::releases::restore::{
    RestoreOptions, find_release_for_version_rid, restore_full_archive_for_version_with_options,
};
use crate::supervisor::stub::find_latest_app_dir;

use super::progress::{
    ProgressInfo, average_speed_bytes_per_sec, clamp_progress_percent, clamp_progress_percent_u64, emit_progress,
    phase_total_percent, saturating_i64_from_u64,
};
use super::{ApplyStrategy, UpdateInfo, UpdateManager};

pub(super) async fn materialize_update_payload<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    if matches!(info.apply_strategy, ApplyStrategy::Delta) {
        materialize_delta_payload(manager, info, staging_dir, artifact_cache_dir, extract_dir, progress).await
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
    let latest = info
        .apply_releases
        .last()
        .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
    let archive_path = staging_dir.join(&latest.full_filename);
    extract_archive_with_progress(&archive_path, extract_dir, progress, 60, 75)?;

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            total_percent: 80,
            ..ProgressInfo::default()
        },
    );
    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
            phase_percent: 100,
            total_percent: 85,
            ..ProgressInfo::default()
        },
    );

    Ok(extract_dir.to_path_buf())
}

async fn materialize_delta_payload<F>(
    manager: &UpdateManager,
    info: &UpdateInfo,
    staging_dir: &Path,
    artifact_cache_dir: &Path,
    extract_dir: &Path,
    progress: Option<&Arc<F>>,
) -> Result<PathBuf>
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    let apply_delta_started_at = Instant::now();
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

    let mut rebuilt_archive = restore_base_full_archive(manager, artifact_cache_dir).await?;
    let mut apply_delta_items_done = 0i64;
    let mut apply_delta_bytes_done = 0i64;

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
                    ..ProgressInfo::default()
                },
            );
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
                ..ProgressInfo::default()
            },
        );
    }

    emit_progress(
        progress,
        ProgressInfo {
            phase: 5,
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
            ..ProgressInfo::default()
        },
    );

    let rebuilt_archive_path = staging_dir.join("rebuilt-full.tar.zst");
    tokio::fs::write(&rebuilt_archive_path, &rebuilt_archive).await?;
    extract_archive_with_progress(&rebuilt_archive_path, extract_dir, progress, 80, 90)?;

    let source = extract_dir.join(&info.latest_version);
    if source.exists() {
        Ok(source)
    } else {
        Ok(extract_dir.to_path_buf())
    }
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

async fn restore_base_full_archive(manager: &UpdateManager, artifact_cache_dir: &Path) -> Result<Vec<u8>> {
    let index = if let Some(cached) = &manager.cached_index {
        cached.clone()
    } else {
        let data = manager.storage.get_object(RELEASES_FILE_COMPRESSED).await?;
        decompress_release_index(&data)?
    };
    let rid = current_rid();
    let current_release = find_release_for_version_rid(&index, &rid, &manager.current_version).ok_or_else(|| {
        SurgeError::Update(format!(
            "Current release {} ({rid}) was not found in the release index",
            manager.current_version
        ))
    })?;

    match restore_full_archive_for_version_with_options(
        manager.storage.as_ref(),
        &index,
        &rid,
        &manager.current_version,
        RestoreOptions {
            cache_dir: Some(artifact_cache_dir),
            progress: None,
        },
    )
    .await
    {
        Ok(archive) => Ok(archive),
        Err(restore_err) => synthesize_current_full_archive_from_installed_app(
            &manager.install_dir,
            &manager.current_version,
            current_release,
            artifact_cache_dir,
            &manager.ctx,
        )
        .map_err(|fallback_err| {
            SurgeError::Update(format!(
                "Failed to restore base full archive for {}: {restore_err}; installed-app fallback failed: {fallback_err}",
                manager.current_version
            ))
        }),
    }
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

pub(super) fn synthesize_current_full_archive_from_installed_app(
    install_dir: &Path,
    current_version: &str,
    current_release: &ReleaseEntry,
    artifact_cache_dir: &Path,
    ctx: &Arc<Context>,
) -> Result<Vec<u8>> {
    let app_dir = find_previous_app_dir(install_dir, current_version).ok_or_else(|| {
        SurgeError::NotFound(format!(
            "No active installed app directory was found for current version {current_version}"
        ))
    })?;

    let mut excluded_relative_paths = BTreeSet::new();
    excluded_relative_paths.insert(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH.to_string());
    excluded_relative_paths.insert(crate::install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH.to_string());
    if runtime_state_dir_contains_only_manifests(&app_dir)? {
        excluded_relative_paths.insert(".surge".to_string());
    }

    let budget = ctx.resource_budget();
    let archive = build_canonical_archive_from_directory(
        &app_dir,
        budget.zstd_compression_level,
        budget.effective_zstd_workers(),
        &excluded_relative_paths,
    )?;

    let mut cache_path = None;
    if !current_release.full_sha256.trim().is_empty() {
        let actual_sha256 = sha256_hex(&archive);
        if actual_sha256 == current_release.full_sha256 {
            cache_path = Some(cache_path_for_key(artifact_cache_dir, &current_release.full_filename)?);
        } else {
            warn!(
                version = %current_release.version,
                expected_sha256 = %current_release.full_sha256,
                actual_sha256 = %actual_sha256,
                "Installed app content reproduced the current package payload but not the original compressed full archive bytes; using synthesized archive for in-flight delta application without caching it"
            );
        }
    }

    if let Some(cache_path) = cache_path {
        write_file_atomic(&cache_path, &archive)?;
        debug!(
            version = %current_release.version,
            app_dir = %app_dir.display(),
            cache_path = %cache_path.display(),
            "Rebuilt current full archive from installed app content"
        );
    }
    Ok(archive)
}

pub(super) fn find_previous_app_dir(install_dir: &Path, current_version: &str) -> Option<PathBuf> {
    let active = install_dir.join("app");
    if active.is_dir() {
        return Some(active);
    }

    let explicit = install_dir.join(format!("app-{current_version}"));
    if explicit.is_dir() {
        return Some(explicit);
    }

    find_latest_app_dir(install_dir).ok()
}

fn runtime_state_dir_contains_only_manifests(app_dir: &Path) -> Result<bool> {
    let surge_dir = app_dir.join(".surge");
    if !surge_dir.exists() {
        return Ok(false);
    }
    if !surge_dir.is_dir() {
        return Ok(false);
    }

    let allowed = BTreeSet::from([
        crate::install::RUNTIME_MANIFEST_RELATIVE_PATH.to_string(),
        crate::install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH.to_string(),
    ]);
    let mut stack = vec![surge_dir];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)?.collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                stack.push(path);
                continue;
            }

            let relative = path
                .strip_prefix(app_dir)
                .map_err(|e| SurgeError::Update(format!("Failed to relativize installed app path: {e}")))?;
            let relative = relative.to_string_lossy().replace('\\', "/");
            if !allowed.contains(&relative) {
                return Ok(false);
            }
        }
    }

    Ok(true)
}
