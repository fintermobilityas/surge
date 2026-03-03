use std::path::Path;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{compress_release_index, decompress_release_index};
use surge_core::storage::{self, StorageBackend};

/// Promote a release version to a target channel.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    const TOTAL_STAGES: usize = 4;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and target release");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id(&manifest, app_id)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let storage_config = super::build_app_scoped_storage_config(&manifest, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    print_stage_done(theme, 1, TOTAL_STAGES, &format!("Target: {app_id}/{rid} v{version}"));

    print_stage(theme, 2, TOTAL_STAGES, "Loading release index");
    let mut index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    print_stage_done(theme, 2, TOTAL_STAGES, "Release index loaded");

    print_stage(theme, 3, TOTAL_STAGES, "Updating channel membership");
    let release = index
        .releases
        .iter_mut()
        .find(|release| release.version == version && release.rid == rid)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} not found for {app_id}/{rid}")))?;

    if release.channels.iter().any(|existing| existing == channel) {
        print_stage_done(
            theme,
            3,
            TOTAL_STAGES,
            &format!("Release already on channel '{channel}'; no changes applied"),
        );
        print_stage(theme, 4, TOTAL_STAGES, "Finalize promote summary");
        print_stage_done(
            theme,
            4,
            TOTAL_STAGES,
            &format!("Completed in {} (no-op)", format_duration(started.elapsed())),
        );
        return Ok(());
    }
    release.channels.push(channel.to_string());
    release.channels.sort();
    release.channels.dedup();

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;
    print_stage_done(theme, 3, TOTAL_STAGES, &format!("Added channel '{channel}' to release"));

    print_stage(theme, 4, TOTAL_STAGES, "Finalize promote summary");
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!(
            "Promoted {app_id} v{version} ({rid}) -> {channel} in {}",
            format_duration(started.elapsed())
        ),
    );
    Ok(())
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<surge_core::releases::manifest::ReleaseIndex> {
    let data = backend.get_object(RELEASES_FILE_COMPRESSED).await?;
    decompress_release_index(&data)
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.info(&format!("[{stage}/{total}] {text}")));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.success(&format!("[{stage}/{total}] {text}")));
}
