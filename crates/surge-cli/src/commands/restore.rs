use std::path::Path;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::manifest::SurgeManifest;
use surge_core::context::{Context, StorageConfig, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::storage;

/// Restore releases from a local backup directory to storage.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    rid: Option<&str>,
    version: Option<&str>,
    backup_dir: &Path,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id(&manifest, app_id)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let storage_config = build_storage_config(&manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    if !backup_dir.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Backup directory does not exist: {}",
            backup_dir.display()
        )));
    }

    tracing::info!(
        "Restoring {app_id}/{rid}{} from {}",
        version.map_or(String::new(), |v| format!(" v{v}")),
        backup_dir.display()
    );

    let mut restored = 0u64;

    for entry in walkdir(backup_dir)? {
        let rel_path = entry
            .strip_prefix(backup_dir)
            .map_err(|e| SurgeError::Io(std::io::Error::other(e)))?;

        let rel_str = rel_path.to_string_lossy().replace('\\', "/");
        let key = rel_str.clone();

        if key != RELEASES_FILE_COMPRESSED
            && (!key.starts_with(&format!("{app_id}-")) || !key.contains(&format!("-{rid}-")))
        {
            continue;
        }

        if let Some(ver) = version
            && key != RELEASES_FILE_COMPRESSED
            && !key.contains(&format!("-{ver}-"))
        {
            continue;
        }

        tracing::debug!("Restoring: {key}");
        backend.upload_from_file(&key, &entry, None).await?;
        restored += 1;
    }

    if restored == 0 {
        tracing::warn!("No files found to restore");
    } else {
        tracing::info!("Restore complete: {restored} object(s) restored");
    }

    Ok(())
}

/// Recursively list all files in a directory.
fn walkdir(dir: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    walk_recursive(dir, &mut files)?;
    Ok(files)
}

fn walk_recursive(dir: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_recursive(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
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
