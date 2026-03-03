use std::path::Path;

use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{compress_release_index, decompress_release_index};
use surge_core::storage::{self, StorageBackend};

/// Demote (remove) a release version from a channel.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id(&manifest, app_id)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    tracing::info!("Demoting {app_id} v{version} ({rid}) from channel '{channel}'");

    let mut index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }

    let release = index
        .releases
        .iter_mut()
        .find(|release| release.version == version && release.rid == rid)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} not found for {app_id}/{rid}")))?;

    let before_len = release.channels.len();
    release.channels.retain(|existing| existing != channel);
    if release.channels.len() == before_len {
        return Err(SurgeError::NotFound(format!(
            "Release {version} is not on channel '{channel}'"
        )));
    }

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;

    tracing::info!("Demoted {app_id} v{version} ({rid}) from channel '{channel}'");
    Ok(())
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<surge_core::releases::manifest::ReleaseIndex> {
    let data = backend.get_object(RELEASES_FILE_COMPRESSED).await?;
    decompress_release_index(&data)
}

fn build_storage_config(manifest: &SurgeManifest) -> Result<surge_core::context::StorageConfig> {
    super::build_storage_config(manifest)
}
