use std::path::{Path, PathBuf};

use crate::logline;
use surge_core::config::installer::InstallerManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::platform::paths::default_install_root;
use surge_core::storage;

/// Execute setup from an extracted installer directory.
///
/// This is called either directly via `surge setup [dir]` or auto-detected when
/// warp extracts the bundle and runs `surge` with no arguments.
pub async fn execute(dir: &Path, no_start: bool) -> Result<()> {
    let manifest_path = dir.join("installer.yml");
    if !manifest_path.is_file() {
        return Err(SurgeError::Config(format!(
            "No installer.yml found in '{}'",
            dir.display()
        )));
    }

    let manifest_bytes = std::fs::read(&manifest_path)?;
    let manifest: InstallerManifest = serde_yaml::from_slice(&manifest_bytes)?;

    logline::info(&format!(
        "Setting up {} v{} ({}/{})",
        manifest.runtime.name, manifest.version, manifest.app_id, manifest.rid
    ));

    let install_root = default_install_root(&manifest.app_id, &manifest.runtime.install_directory)?;

    super::stop_supervisor(&install_root, &manifest.runtime.supervisor_id).await?;

    let package = resolve_package(dir, &manifest).await?;

    let profile = InstallProfile::from_installer_manifest(&manifest, &manifest.runtime.shortcuts);

    core_install::install_package_locally_at_root(&profile, package.path(), &install_root)?;
    let active_app_dir = install_root.join("app");
    let runtime_manifest = core_install::RuntimeManifestMetadata::new(
        &manifest.version,
        &manifest.channel,
        &manifest.storage.provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &manifest.storage.endpoint,
    );
    core_install::write_runtime_manifest(&active_app_dir, &profile, &runtime_manifest)?;

    logline::success(&format!(
        "Installed '{}' to '{}'",
        manifest.app_id,
        install_root.display()
    ));

    if !no_start {
        match core_install::auto_start_after_install_sequence(
            &profile,
            &install_root,
            &active_app_dir,
            &manifest.version,
        ) {
            Ok(pid) => {
                logline::success(&format!("Started '{}' (pid {pid})", manifest.runtime.name));
            }
            Err(e) => {
                logline::warn(&format!("Auto-start failed: {e}"));
            }
        }
    }

    Ok(())
}

/// Resolve the full package: prefer local payload, fall back to downloading.
async fn resolve_package(dir: &Path, manifest: &InstallerManifest) -> Result<ResolvedPackage> {
    let full_filename = manifest.release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::Config(
            "Installer manifest has no full_filename in release section".to_string(),
        ));
    }

    let payload_path = dir.join("payload").join(full_filename);
    if payload_path.is_file() {
        logline::info(&format!("Using bundled payload: {}", payload_path.display()));
        return Ok(ResolvedPackage::Bundled(payload_path));
    }

    logline::info(&format!("Downloading package '{full_filename}' from storage"));

    let storage_config = build_storage_config_from_manifest(manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    let download_dir = tempfile::tempdir()?;
    let destination = download_dir.path().join(full_filename);

    backend.download_to_file(full_filename, &destination, None).await?;

    logline::success(&format!(
        "Downloaded '{}' ({})",
        full_filename,
        file_size_label(&destination)
    ));

    Ok(ResolvedPackage::Downloaded {
        path: destination,
        _guard: download_dir,
    })
}

fn build_storage_config_from_manifest(manifest: &InstallerManifest) -> Result<surge_core::context::StorageConfig> {
    surge_core::storage_config::build_storage_config_from_installer_manifest(manifest)
}

fn file_size_label(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => crate::formatters::format_bytes(meta.len()),
        Err(_) => "unknown size".to_string(),
    }
}

enum ResolvedPackage {
    Bundled(PathBuf),
    Downloaded { path: PathBuf, _guard: tempfile::TempDir },
}

impl ResolvedPackage {
    fn path(&self) -> &Path {
        match self {
            Self::Bundled(path) | Self::Downloaded { path, .. } => path,
        }
    }
}
