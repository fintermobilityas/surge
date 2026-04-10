use std::path::{Path, PathBuf};

use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::installer::{
    InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
};
use surge_core::config::manifest::{AppConfig, InstallerType, SurgeManifest, TargetConfig};
use surge_core::crypto::sha256::sha256_hex_file;
use surge_core::error::{Result, SurgeError};
use surge_core::installer_bundle;

use super::launchers::{
    ensure_host_compatible_rid, find_gui_installer_launcher_for_rid, find_installer_launcher_for_rid,
    find_surge_binary_for_rid, surge_binary_name_for_rid,
};
use super::resolution::installer_storage_prefix;

#[allow(clippy::too_many_arguments)]
pub(super) fn build_installers(
    manifest: &SurgeManifest,
    app: &AppConfig,
    target: &TargetConfig,
    app_id: &str,
    rid: &str,
    version: &str,
    channel: &str,
    manifest_root: &Path,
    artifacts_dir: &Path,
    output_dir: &Path,
    full_package_path: &Path,
) -> Result<Vec<PathBuf>> {
    build_installers_with_launcher(
        manifest,
        app,
        target,
        app_id,
        rid,
        version,
        channel,
        manifest_root,
        artifacts_dir,
        output_dir,
        full_package_path,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_installers_with_launcher(
    manifest: &SurgeManifest,
    app: &AppConfig,
    target: &TargetConfig,
    app_id: &str,
    rid: &str,
    version: &str,
    channel: &str,
    manifest_root: &Path,
    artifacts_dir: &Path,
    output_dir: &Path,
    full_package_path: &Path,
    launcher_override: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    let installer_types = parse_installer_types(&target.installers, app_id, rid)?;
    if installer_types.is_empty() {
        return Ok(Vec::new());
    }
    ensure_host_compatible_rid(rid)?;

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

    let icon_asset = resolve_installer_icon_asset(&target.icon, artifacts_dir, manifest_root)?;

    let requires_console_launcher = installer_types.iter().any(|installer_type| !installer_type.is_gui());
    let console_launcher = if requires_console_launcher {
        Some(find_installer_launcher_for_rid(rid, launcher_override)?)
    } else {
        None
    };
    let gui_launcher = if installer_types.iter().any(|t| t.is_gui()) {
        Some(find_gui_installer_launcher_for_rid(rid)?)
    } else {
        None
    };
    let surge_binary = find_surge_binary_for_rid(rid)?;
    let surge_binary_name = surge_binary_name_for_rid(rid).to_string();

    let mut generated = Vec::with_capacity(installer_types.len());
    for installer_type in installer_types {
        let installer_suffix = installer_type.as_str();
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let installer_filename = format!("Setup-{rid}-{app_id}-{channel}-{installer_suffix}.{installer_ext}");
        let installer_path = installers_dir.join(&installer_filename);

        let staging_dir =
            tempfile::tempdir().map_err(|e| SurgeError::Pack(format!("Failed to create staging directory: {e}")))?;
        let staging = staging_dir.path();

        let ui_mode = if installer_type.is_gui() {
            InstallerUi::Egui
        } else {
            InstallerUi::Console
        };
        let full_sha256 = sha256_hex_file(full_package_path)?;
        let manifest_payload = InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: ui_mode,
            installer_type: installer_type.as_str().to_string(),
            app_id: app_id.to_string(),
            rid: rid.to_string(),
            version: version.to_string(),
            channel: channel.to_string(),
            generated_utc: chrono::Utc::now().to_rfc3339(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: manifest.storage.provider.clone(),
                bucket: manifest.storage.bucket.clone(),
                region: manifest.storage.region.clone(),
                endpoint: manifest.storage.endpoint.clone(),
                prefix: installer_storage_prefix(manifest, app_id),
            },
            release: InstallerRelease {
                full_filename: full_filename.clone(),
                full_sha256: full_sha256.clone(),
                delta_filename: delta_filename.clone(),
                delta_algorithm: if delta_filename.is_empty() {
                    String::new()
                } else {
                    match manifest.effective_pack_policy().delta_strategy {
                        surge_core::config::manifest::PackDeltaStrategy::SparseFileOps => {
                            surge_core::releases::manifest::DIFF_ALGORITHM_FILE_OPS.to_string()
                        }
                        surge_core::config::manifest::PackDeltaStrategy::ArchiveChunkedBsdiff
                        | surge_core::config::manifest::PackDeltaStrategy::ArchiveBsdiff => {
                            surge_core::releases::manifest::DIFF_ALGORITHM_BSDIFF.to_string()
                        }
                    }
                },
                delta_patch_format: if delta_filename.is_empty() {
                    String::new()
                } else {
                    match manifest.effective_pack_policy().delta_strategy {
                        surge_core::config::manifest::PackDeltaStrategy::SparseFileOps => {
                            surge_core::releases::manifest::PATCH_FORMAT_SPARSE_FILE_OPS_V1.to_string()
                        }
                        surge_core::config::manifest::PackDeltaStrategy::ArchiveChunkedBsdiff => {
                            surge_core::releases::manifest::PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3.to_string()
                        }
                        surge_core::config::manifest::PackDeltaStrategy::ArchiveBsdiff => {
                            surge_core::releases::manifest::PATCH_FORMAT_BSDIFF4_ARCHIVE_V3.to_string()
                        }
                    }
                },
                delta_compression: if delta_filename.is_empty() {
                    String::new()
                } else {
                    surge_core::releases::manifest::COMPRESSION_ZSTD.to_string()
                },
            },
            runtime: InstallerRuntime {
                name: app.effective_name(),
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
        std::fs::write(staging.join("installer.yml"), manifest_yaml.as_bytes())?;

        std::fs::copy(&surge_binary, staging.join(&surge_binary_name))?;

        if let Some((source, _)) = &icon_asset {
            let assets_dir = staging.join("assets");
            std::fs::create_dir_all(&assets_dir)?;
            if let Some(filename) = source.file_name() {
                std::fs::copy(source, assets_dir.join(filename))?;
            }
        }

        if installer_type.is_offline() {
            let payload_dir = staging.join("payload");
            std::fs::create_dir_all(&payload_dir)?;
            std::fs::copy(full_package_path, payload_dir.join(&full_filename))?;
        }

        let payload_archive = tempfile::NamedTempFile::new()
            .map_err(|e| SurgeError::Pack(format!("Failed to create installer payload archive temp file: {e}")))?;
        let pack_policy = manifest.effective_pack_policy();
        let mut payload_packer = ArchivePacker::new(pack_policy.compression_level)?;
        payload_packer.add_directory(staging, "")?;
        payload_packer.finalize_to_file(payload_archive.path())?;
        let launcher = if installer_type.is_gui() {
            gui_launcher
                .as_ref()
                .ok_or_else(|| SurgeError::Pack("GUI installer launcher was not resolved".to_string()))?
        } else {
            console_launcher
                .as_ref()
                .ok_or_else(|| SurgeError::Pack("Console installer launcher was not resolved".to_string()))?
        };
        installer_bundle::write_embedded_installer(launcher, payload_archive.path(), &installer_path)?;
        surge_core::platform::fs::make_executable(&installer_path)?;
        generated.push(installer_path);
    }

    Ok(generated)
}

fn resolve_installer_icon_asset(
    icon: &str,
    artifacts_dir: &Path,
    manifest_root: &Path,
) -> Result<Option<(PathBuf, String)>> {
    let icon = icon.trim();
    if icon.is_empty() {
        return Ok(None);
    }

    let icon_path = Path::new(icon);
    let mut candidates: Vec<PathBuf> = Vec::new();
    if icon_path.is_absolute() {
        candidates.push(icon_path.to_path_buf());
    } else {
        candidates.push(artifacts_dir.join(icon_path));
        candidates.push(manifest_root.join(icon_path));
        if let Some(parent) = manifest_root.parent() {
            candidates.push(parent.join(icon_path));
        }
    }

    let source = candidates.into_iter().find(|candidate| candidate.is_file());
    let Some(source) = source else {
        return Ok(None);
    };

    let archive_name = source
        .file_name()
        .map(|name| format!("assets/{}", name.to_string_lossy()))
        .ok_or_else(|| SurgeError::Pack(format!("Invalid icon path: {}", source.display())))?;
    Ok(Some((source, archive_name)))
}

fn parse_installer_types(installers: &[String], app_id: &str, rid: &str) -> Result<Vec<InstallerType>> {
    installers
        .iter()
        .map(|installer| {
            InstallerType::parse(installer).ok_or_else(|| {
                SurgeError::Config(format!(
                    "Unsupported installer '{installer}' for app '{app_id}' target '{rid}'. Supported values: online, offline, online-gui, offline-gui"
                ))
            })
        })
        .collect()
}
