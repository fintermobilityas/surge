use std::path::Path;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// Migrate release data from one storage backend to another.
pub async fn execute(manifest_path: &Path, app_id: &str, rid: &str, dest_manifest_path: &Path) -> Result<()> {
    let src_manifest = SurgeManifest::from_file(manifest_path)?;
    let dest_manifest = SurgeManifest::from_file(dest_manifest_path)?;

    let src_config = build_storage_config(&src_manifest)?;
    let dest_config = build_storage_config(&dest_manifest)?;

    let src_backend = storage::create_storage_backend(&src_config)?;
    let dest_backend = storage::create_storage_backend(&dest_config)?;

    let prefix = format!("{app_id}/{rid}/");

    tracing::info!(
        "Migrating {app_id}/{rid} from {} to {}",
        src_manifest.storage.provider,
        dest_manifest.storage.provider
    );

    let mut marker: Option<String> = None;
    let mut migrated = 0u64;

    loop {
        let listing = src_backend.list_objects(&prefix, marker.as_deref(), 100).await?;

        for entry in &listing.entries {
            tracing::debug!("Migrating: {}", entry.key);
            let data = src_backend.get_object(&entry.key).await?;
            dest_backend
                .put_object(&entry.key, &data, "application/octet-stream")
                .await?;
            migrated += 1;
        }

        if listing.is_truncated {
            marker = listing.next_marker;
        } else {
            break;
        }
    }

    if let Ok(releases_data) = src_backend.get_object(RELEASES_FILE_COMPRESSED).await {
        dest_backend
            .put_object(RELEASES_FILE_COMPRESSED, &releases_data, "application/octet-stream")
            .await?;
        tracing::debug!("Migrated {}", RELEASES_FILE_COMPRESSED);
    }

    tracing::info!("Migration complete: {migrated} object(s) migrated");
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
    let mut cfg = ctx.storage_config();
    cfg.prefix.clone_from(&manifest.storage.prefix);
    Ok(cfg)
}
