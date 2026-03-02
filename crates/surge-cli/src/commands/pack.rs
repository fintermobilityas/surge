use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;
use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::{AppConfig, InstallerType, ShortcutLocation, SurgeManifest, TargetConfig};
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::pack::builder::PackBuilder;

/// Build release packages (full + delta) for a given app version and RID.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    artifacts_dir: &Path,
    output_dir: &Path,
) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id(&manifest, app_id)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let (app, target) = manifest
        .find_app_with_target(&app_id, &rid)
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

    let mut builder = PackBuilder::new(ctx, manifest_path_s, &app_id, &rid, version, artifacts_dir_s)?;
    builder.build(None).await?;

    for artifact in builder.artifacts() {
        let dest = output_dir.join(&artifact.filename);
        if artifact.path != dest {
            std::fs::copy(&artifact.path, &dest)?;
        }
        tracing::info!("Created {}", dest.display());
    }

    let full_filename = format!("{app_id}-{version}-{rid}-full.tar.zst");
    let full_package_path = output_dir.join(&full_filename);
    if !full_package_path.is_file() {
        return Err(SurgeError::Pack(format!(
            "Expected full package was not created: {}",
            full_package_path.display()
        )));
    }

    let installer_paths = build_installers(
        &manifest,
        app,
        &target,
        &app_id,
        &rid,
        version,
        artifacts_dir,
        output_dir,
        &full_package_path,
    )?;
    for installer in installer_paths {
        tracing::info!("Created {}", installer.display());
    }

    tracing::info!("Pack complete. Output: {}", output_dir.display());
    Ok(())
}

#[derive(Debug, Serialize)]
struct InstallerManifest {
    schema: i32,
    format: &'static str,
    ui: &'static str,
    installer_type: String,
    app_id: String,
    rid: String,
    version: String,
    channel: String,
    generated_utc: String,
    headless_default_if_no_display: bool,
    release_index_key: &'static str,
    storage: InstallerStorage,
    release: InstallerRelease,
    runtime: InstallerRuntime,
}

#[derive(Debug, Serialize)]
struct InstallerStorage {
    provider: String,
    bucket: String,
    region: String,
    endpoint: String,
    prefix: String,
}

#[derive(Debug, Serialize)]
struct InstallerRelease {
    full_filename: String,
    delta_filename: String,
}

#[derive(Debug, Serialize)]
struct InstallerRuntime {
    main_exe: String,
    install_directory: String,
    supervisor_id: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
    persistent_assets: Vec<String>,
    installers: Vec<String>,
    environment: BTreeMap<String, String>,
}

#[allow(clippy::too_many_arguments)]
fn build_installers(
    manifest: &SurgeManifest,
    app: &AppConfig,
    target: &TargetConfig,
    app_id: &str,
    rid: &str,
    version: &str,
    artifacts_dir: &Path,
    output_dir: &Path,
    full_package_path: &Path,
) -> Result<Vec<PathBuf>> {
    let installer_types = parse_installer_types(&target.installers, app_id, rid)?;
    if installer_types.is_empty() {
        return Ok(Vec::new());
    }

    let default_channel = app
        .channels
        .first()
        .cloned()
        .or_else(|| manifest.channels.first().map(|channel| channel.name.clone()))
        .unwrap_or_else(|| "stable".to_string());

    let installers_dir = output_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("installers")
        .join(app_id)
        .join(rid);
    std::fs::create_dir_all(&installers_dir)?;

    let full_filename = full_package_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| {
            SurgeError::Pack(format!(
                "Invalid full package path (missing filename): {}",
                full_package_path.display()
            ))
        })?;
    let expected_delta_filename = format!("{app_id}-{version}-{rid}-delta.tar.zst");
    let delta_filename = if output_dir.join(&expected_delta_filename).is_file() {
        expected_delta_filename
    } else {
        String::new()
    };

    let icon_asset = if target.icon.trim().is_empty() {
        None
    } else {
        let source = artifacts_dir.join(&target.icon);
        if source.is_file() {
            let archive_name = source
                .file_name()
                .map(|name| format!("assets/{}", name.to_string_lossy()))
                .ok_or_else(|| SurgeError::Pack(format!("Invalid icon path in artifacts: {}", source.display())))?;
            Some((source, archive_name))
        } else {
            None
        }
    };

    let mut generated = Vec::with_capacity(installer_types.len());
    for installer_type in installer_types {
        let installer_suffix = installer_type.as_str();
        let installer_filename =
            format!("Setup-{rid}-{app_id}-{default_channel}-{installer_suffix}.surge-installer.tar.zst");
        let installer_path = installers_dir.join(&installer_filename);

        let manifest_payload = InstallerManifest {
            schema: 1,
            format: "surge-installer-v1",
            ui: "imgui",
            installer_type: installer_type.as_str().to_string(),
            app_id: app_id.to_string(),
            rid: rid.to_string(),
            version: version.to_string(),
            channel: default_channel.clone(),
            generated_utc: chrono::Utc::now().to_rfc3339(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED,
            storage: InstallerStorage {
                provider: manifest.storage.provider.clone(),
                bucket: manifest.storage.bucket.clone(),
                region: manifest.storage.region.clone(),
                endpoint: manifest.storage.endpoint.clone(),
                prefix: manifest.storage.prefix.clone(),
            },
            release: InstallerRelease {
                full_filename: full_filename.clone(),
                delta_filename: delta_filename.clone(),
            },
            runtime: InstallerRuntime {
                main_exe: app.effective_main_exe(),
                install_directory: app.effective_install_directory(),
                supervisor_id: app.supervisor_id.clone(),
                icon: target.icon.clone(),
                shortcuts: target.shortcuts.clone(),
                persistent_assets: target.persistent_assets.clone(),
                installers: target.installers.clone(),
                environment: target.environment.clone(),
            },
        };
        let manifest_yaml = serde_yaml::to_string(&manifest_payload)?;

        let mut packer = ArchivePacker::new(DEFAULT_ZSTD_LEVEL)?;
        packer.add_buffer("installer.yml", manifest_yaml.as_bytes(), 0o644)?;
        if let Some((source, archive_name)) = &icon_asset {
            packer.add_file(source, archive_name)?;
        }
        if matches!(installer_type, InstallerType::Offline) {
            packer.add_file(full_package_path, &format!("payload/{full_filename}"))?;
        }
        packer.finalize_to_file(&installer_path)?;
        generated.push(installer_path);
    }

    Ok(generated)
}

fn parse_installer_types(installers: &[String], app_id: &str, rid: &str) -> Result<Vec<InstallerType>> {
    installers
        .iter()
        .map(|installer| {
            InstallerType::parse(installer).ok_or_else(|| {
                SurgeError::Config(format!(
                    "Unsupported installer '{installer}' for app '{app_id}' target '{rid}'. Supported values: web, offline"
                ))
            })
        })
        .collect()
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
