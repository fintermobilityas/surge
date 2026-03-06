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

    if let Err(e) = super::stop_supervisor(&install_root, &manifest.runtime.supervisor_id).await {
        logline::warn(&format!("Could not stop supervisor: {e}"));
    }
    stop_running_app(&install_root, &manifest.runtime.main_exe);

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

/// Kill any running process whose executable lives in the app directory.
/// This catches orphaned app processes that outlived their supervisor.
fn stop_running_app(install_root: &Path, main_exe: &str) {
    let main_exe = main_exe.trim();
    if main_exe.is_empty() {
        return;
    }

    let exe_path = install_root.join("app").join(main_exe);
    let exe_name = exe_path.to_string_lossy();

    #[cfg(unix)]
    {
        let status = std::process::Command::new("pkill").args(["-f", &*exe_name]).status();
        if matches!(status, Ok(s) if s.success()) {
            logline::info(&format!("Stopped running app process '{main_exe}'."));
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/FI", &format!("IMAGENAME eq {main_exe}")])
            .status();
    }

    let _ = &exe_name;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::config::installer::{InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi};
    use surge_core::platform::detect::current_rid;

    fn make_manifest(
        install_root: &Path,
        store_root: &Path,
        full_filename: &str,
        installer_type: &str,
    ) -> InstallerManifest {
        InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: InstallerUi::Console,
            installer_type: installer_type.to_string(),
            app_id: "demo-app".to_string(),
            rid: current_rid(),
            version: "1.2.3".to_string(),
            channel: "stable".to_string(),
            generated_utc: chrono::Utc::now().to_rfc3339(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: "filesystem".to_string(),
                bucket: store_root.to_string_lossy().to_string(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release: InstallerRelease {
                full_filename: full_filename.to_string(),
                delta_filename: String::new(),
                delta_algorithm: String::new(),
                delta_patch_format: String::new(),
                delta_compression: String::new(),
            },
            runtime: InstallerRuntime {
                name: "Demo App".to_string(),
                main_exe: "demoapp".to_string(),
                install_directory: install_root.to_string_lossy().to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: BTreeMap::new(),
            },
        }
    }

    fn write_archive(path: &Path, payload: &[u8]) {
        let mut packer = ArchivePacker::new(3).expect("archive packer");
        packer
            .add_buffer("demoapp", b"#!/bin/sh\necho demo\n", 0o755)
            .expect("demoapp entry");
        packer.add_buffer("payload.txt", payload, 0o644).expect("payload entry");
        packer.finalize_to_file(path).expect("archive file");
    }

    #[tokio::test]
    async fn execute_installs_bundled_payload_and_writes_runtime_manifest() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let payload_dir = installer_dir.join("payload");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";

        std::fs::create_dir_all(&payload_dir).expect("payload dir");
        std::fs::create_dir_all(&store_root).expect("store dir");

        let manifest = make_manifest(&install_root, &store_root, full_filename, "offline");
        let installer_yaml = serde_yaml::to_string(&manifest).expect("installer yaml");
        std::fs::write(installer_dir.join("installer.yml"), installer_yaml).expect("installer manifest");
        write_archive(&payload_dir.join(full_filename), b"bundled payload");

        execute(&installer_dir, true).await.expect("setup should succeed");

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload file"),
            "bundled payload"
        );
        assert!(active_app_dir.join("demoapp").is_file());
        assert!(!install_root.join(".surge-app-next").exists());
        assert!(!install_root.join(".surge-app-prev").exists());

        let runtime_manifest = active_app_dir.join(surge_core::install::RUNTIME_MANIFEST_RELATIVE_PATH);
        let runtime_yaml = std::fs::read_to_string(runtime_manifest).expect("runtime manifest");
        assert!(runtime_yaml.contains("id: demo-app"));
        assert!(runtime_yaml.contains("version: 1.2.3"));
        assert!(runtime_yaml.contains("channel: stable"));
    }

    #[tokio::test]
    async fn resolve_package_downloads_when_bundled_payload_is_missing() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let full_filename = "demo-app-1.2.3-full.tar.zst";
        let stored_archive = store_root.join(full_filename);

        std::fs::create_dir_all(&installer_dir).expect("installer dir");
        std::fs::create_dir_all(&store_root).expect("store dir");
        write_archive(&stored_archive, b"downloaded payload");

        let manifest = make_manifest(&install_root, &store_root, full_filename, "online");
        let package = resolve_package(&installer_dir, &manifest)
            .await
            .expect("downloaded package");

        match &package {
            ResolvedPackage::Downloaded { path, .. } => {
                assert!(path.is_file());
                assert_eq!(
                    std::fs::read(path).expect("downloaded bytes"),
                    std::fs::read(stored_archive).expect("stored bytes")
                );
            }
            ResolvedPackage::Bundled(_) => panic!("expected downloaded package"),
        }
    }
}
