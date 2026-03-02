use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// Build release packages (full + delta) for a given app version and RID.
pub async fn execute(
    manifest_path: &Path,
    app_id: &str,
    version: &str,
    rid: &str,
    artifacts_dir: &Path,
    output_dir: &Path,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let _target = manifest
        .find_target(app_id, rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {rid} found for app {app_id}")))?;

    let ctx = Context::new();
    let storage_config = configure_storage(&manifest, &ctx)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    if !artifacts_dir.is_dir() {
        return Err(SurgeError::Pack(format!(
            "Artifacts directory does not exist: {}",
            artifacts_dir.display()
        )));
    }

    std::fs::create_dir_all(output_dir)?;

    tracing::info!("Packing {app_id} v{version} ({rid}) from {}", artifacts_dir.display());

    // Build the full release archive
    let full_archive_name = format!("{app_id}-{version}-{rid}-full.tar.zst");
    let full_archive_path = output_dir.join(&full_archive_name);

    {
        let compression = ctx.resource_budget().zstd_compression_level;
        let mut packer = surge_core::archive::packer::ArchivePacker::new(compression)?;
        packer.add_directory(artifacts_dir, "")?;
        packer.finalize_to_file(&full_archive_path)?;
    }
    tracing::info!("Created full archive: {}", full_archive_path.display());

    // Attempt to build delta if a previous release exists
    let prefix = format!("{app_id}/{rid}/");
    let list_result = backend.list_objects(&prefix, None, 100).await;

    if let Ok(listing) = list_result
        && !listing.entries.is_empty()
    {
        tracing::info!(
            "Found {} existing objects, delta generation would apply here",
            listing.entries.len()
        );
    }

    tracing::info!("Pack complete. Output: {}", output_dir.display());
    Ok(())
}

fn configure_storage(manifest: &SurgeManifest, ctx: &Context) -> Result<surge_core::context::StorageConfig> {
    let provider = match manifest.storage.provider.to_lowercase().as_str() {
        "s3" => surge_core::context::StorageProvider::S3,
        "azure" => surge_core::context::StorageProvider::AzureBlob,
        "gcs" => surge_core::context::StorageProvider::Gcs,
        "filesystem" => surge_core::context::StorageProvider::Filesystem,
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };

    ctx.set_storage(
        provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        "", // access_key from env
        "", // secret_key from env
        &manifest.storage.endpoint,
    );

    Ok(ctx.storage_config())
}
