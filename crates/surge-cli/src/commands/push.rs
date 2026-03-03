use std::collections::BTreeSet;
use std::path::Path;

use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::crypto::sha256::sha256_hex_file;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index};
use surge_core::storage::{self, StorageBackend};

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
    let app = manifest
        .find_app(app_id)
        .ok_or_else(|| SurgeError::Config(format!("App '{app_id}' not found in manifest")))?;
    let target = manifest
        .find_target(app_id, rid)
        .ok_or_else(|| SurgeError::Config(format!("Target '{rid}' not found for app '{app_id}'")))?;
    let main_exe = if app.main_exe.is_empty() {
        app.id.clone()
    } else {
        app.main_exe.clone()
    };
    let icon = target.icon.clone();
    let shortcuts = target.shortcuts.clone();

    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    if !packages_dir.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Packages directory does not exist: {}",
            packages_dir.display()
        )));
    }

    tracing::info!("Pushing {app_id} v{version} ({rid}) to channel '{channel}'");

    let full_filename = format!("{app_id}-{version}-{rid}-full.tar.zst");
    let full_archive = packages_dir.join(&full_filename);
    if !full_archive.is_file() {
        return Err(SurgeError::Storage(format!(
            "Full archive not found: {}",
            full_archive.display()
        )));
    }

    backend.upload_from_file(&full_filename, &full_archive, None).await?;
    let full_size = std::fs::metadata(&full_archive)?.len() as i64;
    let full_sha256 = sha256_hex_file(&full_archive)?;

    let delta_filename = format!("{app_id}-{version}-{rid}-delta.tar.zst");
    let delta_archive = packages_dir.join(&delta_filename);
    let (delta_filename, delta_size, delta_sha256) = if delta_archive.is_file() {
        backend.upload_from_file(&delta_filename, &delta_archive, None).await?;
        (
            delta_filename,
            std::fs::metadata(&delta_archive)?.len() as i64,
            sha256_hex_file(&delta_archive)?,
        )
    } else {
        (String::new(), 0, String::new())
    };

    update_release_index(
        &*backend,
        app_id,
        version,
        rid,
        channel,
        full_filename,
        full_size,
        full_sha256,
        delta_filename,
        delta_size,
        delta_sha256,
        main_exe,
        icon,
        shortcuts,
    )
    .await?;

    tracing::info!("Push complete for {app_id} v{version} ({rid}) -> {channel}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_release_index(
    backend: &dyn StorageBackend,
    app_id: &str,
    version: &str,
    rid: &str,
    channel: &str,
    full_filename: String,
    full_size: i64,
    full_sha256: String,
    delta_filename: String,
    delta_size: i64,
    delta_sha256: String,
    main_exe: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
) -> Result<()> {
    let mut index = match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data)?,
        Err(SurgeError::NotFound(_)) => ReleaseIndex {
            schema: SCHEMA_VERSION,
            app_id: app_id.to_string(),
            ..ReleaseIndex::default()
        },
        Err(e) => return Err(e),
    };

    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::Storage(format!(
            "Release index belongs to '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    if index.app_id.is_empty() {
        index.app_id = app_id.to_string();
    }

    let mut channels = BTreeSet::new();
    channels.insert(channel.to_string());

    for existing in &index.releases {
        if existing.version == version && existing.rid == rid {
            for existing_channel in &existing.channels {
                channels.insert(existing_channel.clone());
            }
        }
    }

    let is_genesis_for_rid = !index
        .releases
        .iter()
        .any(|release| release.rid == rid || release.rid.is_empty());

    index
        .releases
        .retain(|release| !(release.version == version && release.rid == rid));

    index.releases.push(ReleaseEntry {
        version: version.to_string(),
        channels: channels.into_iter().collect(),
        os: detect_os_from_rid(rid),
        rid: rid.to_string(),
        is_genesis: is_genesis_for_rid,
        full_filename,
        full_size,
        full_sha256,
        delta_filename,
        delta_size,
        delta_sha256,
        created_utc: chrono::Utc::now().to_rfc3339(),
        release_notes: String::new(),
        main_exe,
        icon,
        shortcuts,
    });

    index.last_write_utc = chrono::Utc::now().to_rfc3339();

    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;

    Ok(())
}

fn detect_os_from_rid(rid: &str) -> String {
    rid.split('-').next().unwrap_or("unknown").to_string()
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
