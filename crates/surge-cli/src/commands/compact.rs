use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{compress_release_index, decompress_release_index};
use surge_core::releases::version::compare_versions;
use surge_core::storage;

/// Compact a channel to a single latest full release and prune stale artifacts.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and target");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let storage_config = super::build_app_scoped_storage_config(&manifest, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    print_stage_done(theme, 1, TOTAL_STAGES, &format!("Target: {app_id}/{rid}"));

    print_stage(theme, 2, TOTAL_STAGES, "Loading release index");
    let mut index = match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data)?,
        Err(SurgeError::NotFound(_)) => {
            print_stage_done(theme, 2, TOTAL_STAGES, "No release index found, nothing to compact");
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::Config(format!(
            "Release index belongs to '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    let total_before = index.releases.len();
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Loaded {total_before} release(s)"),
    );

    print_stage(theme, 3, TOTAL_STAGES, "Identifying latest release on channel");
    let latest_version = index
        .releases
        .iter()
        .filter(|r| r.rid == rid && r.channels.contains(&channel.to_string()))
        .max_by(|a, b| compare_versions(&a.version, &b.version))
        .map(|r| r.version.clone());

    let latest_version = match latest_version {
        Some(v) => v,
        None => {
            print_stage_done(
                theme,
                3,
                TOTAL_STAGES,
                &format!("No releases found on channel '{channel}' for RID '{rid}'"),
            );
            return Ok(());
        }
    };
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        &format!("Latest: v{latest_version}"),
    );

    print_stage(theme, 4, TOTAL_STAGES, "Removing old releases and pruning artifacts");
    let mut stale_filenames: BTreeSet<String> = BTreeSet::new();
    for release in &index.releases {
        if release.rid != rid {
            continue;
        }
        if release.version == latest_version {
            // Keep the latest but collect its delta for removal (compact = full only)
            let delta = release.delta_filename.trim();
            if !delta.is_empty() {
                stale_filenames.insert(delta.to_string());
            }
            continue;
        }
        // Collect all artifacts from non-latest releases
        let full = release.full_filename.trim();
        if !full.is_empty() {
            stale_filenames.insert(full.to_string());
        }
        let delta = release.delta_filename.trim();
        if !delta.is_empty() {
            stale_filenames.insert(delta.to_string());
        }
    }

    // Remove non-latest releases for this RID from the index
    index
        .releases
        .retain(|r| r.rid != rid || r.version == latest_version);

    // Clear delta fields from the retained latest release
    for release in &mut index.releases {
        if release.rid == rid && release.version == latest_version {
            release.delta_filename = String::new();
            release.delta_size = 0;
            release.delta_sha256 = String::new();
        }
    }

    // Delete stale blobs from storage
    let mut deleted = 0usize;
    for key in &stale_filenames {
        match backend.delete_object(key).await {
            Ok(()) | Err(SurgeError::NotFound(_)) => {
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!("Failed to delete {key}: {e}");
            }
        }
    }

    // Upload updated index
    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;

    let removed_releases = total_before - index.releases.len();
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!(
            "Removed {removed_releases} release(s), deleted {deleted} artifact(s)"
        ),
    );

    print_stage(theme, 5, TOTAL_STAGES, "Finalize compact summary");
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Compacted {app_id}/{rid} to v{latest_version} (full only) on '{channel}' in {}",
            format_duration(started.elapsed())
        ),
    );
    Ok(())
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.info(&format!("[{stage}/{total}] {text}")));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.success(&format!("[{stage}/{total}] {text}")));
}
