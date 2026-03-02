use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// List releases and channels for an application.
pub async fn execute(manifest_path: &Path, app_id: &str, rid: &str, channel: Option<&str>) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    let prefix = match channel {
        Some(ch) => format!("{app_id}/{rid}/{ch}/"),
        None => format!("{app_id}/{rid}/"),
    };

    tracing::info!("Listing releases for {app_id}/{rid}");

    let mut marker: Option<String> = None;
    let mut total_entries = 0;

    loop {
        let listing = backend.list_objects(&prefix, marker.as_deref(), 1000).await?;

        for entry in &listing.entries {
            // Parse version from key path: {app_id}/{rid}/{channel}/{version}/...
            let parts: Vec<&str> = entry.key.split('/').collect();
            if parts.len() >= 4 {
                let ch = parts.get(2).unwrap_or(&"");
                let ver = parts.get(3).unwrap_or(&"");
                let file = parts.last().unwrap_or(&"");
                println!("{ch:<16} {ver:<16} {file:<32} {size:>12} bytes", size = entry.size);
            }
            total_entries += 1;
        }

        if listing.is_truncated {
            marker = listing.next_marker;
        } else {
            break;
        }
    }

    if total_entries == 0 {
        println!("No releases found.");
    } else {
        println!("\n{total_entries} object(s) found.");
    }

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
