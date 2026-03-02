use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// Demote (remove) a release version from a channel.
pub async fn execute(manifest_path: &Path, app_id: &str, version: &str, rid: &str, channel: &str) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    tracing::info!("Demoting {app_id} v{version} ({rid}) from channel '{channel}'");

    // Delete the release from the target channel
    let prefix = format!("{app_id}/{rid}/{channel}/{version}/");
    let listing = backend.list_objects(&prefix, None, 1000).await?;

    if listing.entries.is_empty() {
        return Err(SurgeError::NotFound(format!(
            "Release {version} not found in channel '{channel}' for {app_id}/{rid}"
        )));
    }

    for entry in &listing.entries {
        tracing::debug!("Deleting {}", entry.key);
        backend.delete_object(&entry.key).await?;
    }

    tracing::info!(
        "Demoted {app_id} v{version} ({rid}) from channel '{channel}' ({} objects removed)",
        listing.entries.len()
    );
    Ok(())
}

fn build_storage_config(manifest: &SurgeManifest) -> Result<StorageConfig> {
    let provider = match manifest.storage.provider.to_lowercase().as_str() {
        "s3" => StorageProvider::S3,
        "azure" => StorageProvider::AzureBlob,
        "gcs" => StorageProvider::Gcs,
        "filesystem" => StorageProvider::Filesystem,
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };

    let ctx = Context::new();
    ctx.set_storage(
        provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        "",
        "",
        &manifest.storage.endpoint,
    );
    Ok(ctx.storage_config())
}
