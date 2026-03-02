use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// Push built packages to cloud storage.
pub async fn execute(
    manifest_path: &Path,
    app_id: &str,
    version: &str,
    rid: &str,
    channel: &str,
    packages_dir: &Path,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    if !packages_dir.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Packages directory does not exist: {}",
            packages_dir.display()
        )));
    }

    tracing::info!("Pushing {app_id} v{version} ({rid}) to channel '{channel}'");

    // Upload each package file
    let full_archive = packages_dir.join(format!("{app_id}-{version}-{rid}-full.tar.zst"));
    if full_archive.is_file() {
        let key = format!("{app_id}/{rid}/{channel}/{version}/full.tar.zst");
        tracing::info!("Uploading {}", key);
        backend.upload_from_file(&key, &full_archive, None).await?;
    } else {
        return Err(SurgeError::Storage(format!(
            "Full archive not found: {}",
            full_archive.display()
        )));
    }

    // Upload delta if present
    let delta_archive = packages_dir.join(format!("{app_id}-{version}-{rid}-delta.tar.zst"));
    if delta_archive.is_file() {
        let key = format!("{app_id}/{rid}/{channel}/{version}/delta.tar.zst");
        tracing::info!("Uploading {}", key);
        backend.upload_from_file(&key, &delta_archive, None).await?;
    }

    tracing::info!("Push complete for {app_id} v{version} ({rid}) -> {channel}");
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
