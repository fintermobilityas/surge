use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// Promote a release version to a target channel.
pub async fn execute(manifest_path: &Path, app_id: &str, version: &str, rid: &str, channel: &str) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    tracing::info!("Promoting {app_id} v{version} ({rid}) to channel '{channel}'");

    // Verify the release exists in storage
    let source_prefix = format!("{app_id}/{rid}/");
    let listing = backend.list_objects(&source_prefix, None, 1000).await?;

    let version_exists = listing.entries.iter().any(|e| e.key.contains(&format!("/{version}/")));

    if !version_exists {
        return Err(SurgeError::NotFound(format!(
            "Release {version} not found for {app_id}/{rid}"
        )));
    }

    // Copy/link the release to the target channel
    let full_key = format!("{app_id}/{rid}/{channel}/{version}/full.tar.zst");
    let source_data = find_and_download_release(&*backend, &listing.entries, version, "full.tar.zst").await?;
    backend.put_object(&full_key, &source_data, "application/zstd").await?;

    tracing::info!("Promoted {app_id} v{version} ({rid}) -> {channel}");
    Ok(())
}

async fn find_and_download_release(
    backend: &dyn storage::StorageBackend,
    entries: &[storage::ListEntry],
    version: &str,
    filename: &str,
) -> Result<Vec<u8>> {
    for entry in entries {
        if entry.key.contains(&format!("/{version}/")) && entry.key.ends_with(filename) {
            return backend.get_object(&entry.key).await;
        }
    }
    Err(SurgeError::NotFound(format!(
        "Release artifact {filename} not found for version {version}"
    )))
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
