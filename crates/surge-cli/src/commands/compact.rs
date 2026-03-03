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
///
/// When `app_id` and `rid` are omitted, iterates over every app and target in the manifest.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let targets: Vec<(String, String)> = if let Some(app_id) = app_id {
        let app_id = app_id.to_string();
        let rid = super::resolve_rid(&manifest, &app_id, rid)?;
        vec![(app_id, rid)]
    } else {
        manifest
            .app_ids()
            .into_iter()
            .flat_map(|app_id| {
                manifest
                    .target_rids(&app_id)
                    .into_iter()
                    .map(move |rid| (app_id.clone(), rid))
            })
            .collect()
    };

    let theme = UiTheme::global();
    let total_targets = targets.len();
    println!(
        "{}",
        theme.info(&format!("Compacting {total_targets} target(s) on channel '{channel}'"))
    );
    println!();

    let mut errors = Vec::new();
    for (app_id, rid) in &targets {
        if let Err(e) = compact_single(&manifest, app_id, rid, channel).await {
            println!("{}", theme.warning(&format!("  Failed {app_id}/{rid}: {e}")));
            errors.push(format!("{app_id}/{rid}: {e}"));
        }
        println!();
    }

    if errors.is_empty() {
        println!(
            "{}",
            theme.success(&format!("All {total_targets} target(s) compacted successfully."))
        );
        Ok(())
    } else {
        Err(SurgeError::Storage(format!(
            "{} target(s) failed: {}",
            errors.len(),
            errors.join("; ")
        )))
    }
}

async fn compact_single(
    manifest: &SurgeManifest,
    app_id: &str,
    rid: &str,
    channel: &str,
) -> Result<()> {
    const TOTAL_STAGES: usize = 4;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, &format!("{app_id}/{rid}"));
    let storage_config = super::build_app_scoped_storage_config(manifest, app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    let mut index = match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data)?,
        Err(SurgeError::NotFound(_)) => {
            print_stage_done(theme, 1, TOTAL_STAGES, "No release index, skipped");
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let total_before = index.releases.len();

    print_stage(theme, 2, TOTAL_STAGES, "Finding latest release");
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
                2,
                TOTAL_STAGES,
                &format!("No releases on '{channel}', skipped"),
            );
            return Ok(());
        }
    };
    print_stage_done(theme, 2, TOTAL_STAGES, &format!("v{latest_version}"));

    print_stage(theme, 3, TOTAL_STAGES, "Pruning old releases and artifacts");
    let mut stale_filenames: BTreeSet<String> = BTreeSet::new();
    for release in &index.releases {
        if release.rid != rid {
            continue;
        }
        if release.version == latest_version {
            let delta = release.delta_filename.trim();
            if !delta.is_empty() {
                stale_filenames.insert(delta.to_string());
            }
            continue;
        }
        let full = release.full_filename.trim();
        if !full.is_empty() {
            stale_filenames.insert(full.to_string());
        }
        let delta = release.delta_filename.trim();
        if !delta.is_empty() {
            stale_filenames.insert(delta.to_string());
        }
    }

    index
        .releases
        .retain(|r| r.rid != rid || r.version == latest_version);

    for release in &mut index.releases {
        if release.rid == rid && release.version == latest_version {
            release.delta_filename = String::new();
            release.delta_size = 0;
            release.delta_sha256 = String::new();
        }
    }

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

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;

    let removed = total_before - index.releases.len();
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        &format!("Removed {removed} release(s), deleted {deleted} artifact(s)"),
    );

    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!(
            "Compacted to v{latest_version} (full only) in {}",
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
