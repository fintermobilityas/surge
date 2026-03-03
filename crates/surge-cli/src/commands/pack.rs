use std::path::Path;
use std::sync::Arc;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::pack::builder::PackBuilder;

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
    manifest
        .find_target(app_id, rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {rid} found for app {app_id}")))?;

    if !artifacts_dir.is_dir() {
        return Err(SurgeError::Pack(format!(
            "Artifacts directory does not exist: {}",
            artifacts_dir.display()
        )));
    }

    std::fs::create_dir_all(output_dir)?;

    tracing::info!("Packing {app_id} v{version} ({rid}) from {}", artifacts_dir.display());

    let ctx = Arc::new(configure_context(&manifest)?);
    let manifest_path_s = manifest_path
        .to_str()
        .ok_or_else(|| SurgeError::Config(format!("Manifest path is not valid UTF-8: {}", manifest_path.display())))?;
    let artifacts_dir_s = artifacts_dir.to_str().ok_or_else(|| {
        SurgeError::Config(format!(
            "Artifacts directory is not valid UTF-8: {}",
            artifacts_dir.display()
        ))
    })?;

    let mut builder = PackBuilder::new(ctx, manifest_path_s, app_id, rid, version, artifacts_dir_s)?;
    builder.build(None).await?;

    for artifact in builder.artifacts() {
        let dest = output_dir.join(&artifact.filename);
        if artifact.path != dest {
            std::fs::copy(&artifact.path, &dest)?;
        }
        tracing::info!("Created {}", dest.display());
    }

    tracing::info!("Pack complete. Output: {}", output_dir.display());
    Ok(())
}

fn configure_context(manifest: &SurgeManifest) -> Result<Context> {
    let provider = match manifest.storage.provider.to_lowercase().as_str() {
        "s3" => surge_core::context::StorageProvider::S3,
        "azure" => surge_core::context::StorageProvider::AzureBlob,
        "gcs" => surge_core::context::StorageProvider::Gcs,
        "filesystem" => surge_core::context::StorageProvider::Filesystem,
        "github" | "github_releases" | "github-releases" => surge_core::context::StorageProvider::GitHubReleases,
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };

    let ctx = Context::new();
    ctx.set_storage(
        provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        "", // access_key from env
        "", // secret_key from env
        &manifest.storage.endpoint,
    );
    {
        let mut cfg = ctx.storage.lock().unwrap();
        cfg.prefix.clone_from(&manifest.storage.prefix);
    }

    Ok(ctx)
}
