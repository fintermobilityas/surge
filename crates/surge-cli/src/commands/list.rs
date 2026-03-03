use std::path::Path;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::decompress_release_index;
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

/// List releases and channels for an application.
pub async fn execute(manifest_path: &Path, app_id: &str, rid: &str, channel: Option<&str>) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    tracing::info!("Listing releases for {app_id}/{rid}");

    let index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }

    let mut releases: Vec<&surge_core::releases::manifest::ReleaseEntry> = index
        .releases
        .iter()
        .filter(|release| release.rid.is_empty() || release.rid == rid)
        .filter(|release| {
            channel.is_none_or(|requested_channel| release.channels.iter().any(|c| c == requested_channel))
        })
        .collect();

    releases.sort_by(|a, b| compare_versions(&a.version, &b.version));

    if releases.is_empty() {
        println!("No releases found.");
        return Ok(());
    }

    for release in releases {
        let channels = if release.channels.is_empty() {
            "-".to_string()
        } else {
            release.channels.join(",")
        };
        println!(
            "{version:<16} {channels:<20} {full_size:>12} {delta_size:>12} {kind}",
            version = release.version,
            full_size = release.full_size,
            delta_size = release.delta_size,
            kind = if release.is_genesis { "genesis" } else { "" },
        );
    }

    Ok(())
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<surge_core::releases::manifest::ReleaseIndex> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data),
        Err(SurgeError::NotFound(_)) => Ok(surge_core::releases::manifest::ReleaseIndex::default()),
        Err(e) => Err(e),
    }
}

fn build_storage_config(manifest: &SurgeManifest) -> Result<StorageConfig> {
    let provider = match manifest.storage.provider.to_lowercase().as_str() {
        "s3" => StorageProvider::S3,
        "azure" => StorageProvider::AzureBlob,
        "gcs" => StorageProvider::Gcs,
        "filesystem" => StorageProvider::Filesystem,
        "github" | "github_releases" | "github-releases" => StorageProvider::GitHubReleases,
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
