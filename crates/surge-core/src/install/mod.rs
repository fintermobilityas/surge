mod activation;
mod launch;
mod persistent_assets;
mod runtime_manifest;

use std::collections::BTreeMap;

use crate::config::installer::InstallerManifest;
use crate::config::manifest::ShortcutLocation;

pub use self::activation::{
    install_package_locally, install_package_locally_at_root, install_package_locally_at_root_with_progress,
    prune_version_snapshots,
};
pub use self::launch::{auto_start_after_install, auto_start_after_install_sequence, launch_installed_application};
pub use self::persistent_assets::{copy_persistent_assets, validate_relative_persistent_asset_path};
pub use self::runtime_manifest::{
    LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH, RUNTIME_MANIFEST_RELATIVE_PATH, RuntimeManifestMetadata,
    read_runtime_manifest_version, storage_provider_manifest_name, write_runtime_manifest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallProgressStage {
    Extract,
    Activate,
    Shortcuts,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InstallProgress {
    pub stage: InstallProgressStage,
    pub phase_percent: i32,
    pub bytes_done: i64,
    pub bytes_total: i64,
    pub items_done: i64,
    pub items_total: i64,
}

pub type InstallProgressCallback<'a> = dyn Fn(InstallProgress) + Send + Sync + 'a;

fn emit_install_progress(progress: Option<&InstallProgressCallback<'_>>, snapshot: InstallProgress) {
    if let Some(cb) = progress {
        cb(snapshot);
    }
}

/// Shared profile for installing a package locally, usable from both
/// `surge install` (via `ReleaseEntry`) and `surge setup` (via `InstallerRuntime`).
pub struct InstallProfile<'a> {
    pub app_id: &'a str,
    pub display_name: &'a str,
    pub main_exe: &'a str,
    pub install_directory: &'a str,
    pub supervisor_id: &'a str,
    pub icon: &'a str,
    pub shortcuts: &'a [ShortcutLocation],
    pub persistent_assets: &'a [String],
    pub environment: &'a BTreeMap<String, String>,
}

impl<'a> InstallProfile<'a> {
    #[must_use]
    pub fn new(
        app_id: &'a str,
        display_name: &'a str,
        main_exe: &'a str,
        install_directory: &'a str,
        supervisor_id: &'a str,
        icon: &'a str,
        shortcuts: &'a [ShortcutLocation],
        persistent_assets: &'a [String],
        environment: &'a BTreeMap<String, String>,
    ) -> Self {
        Self {
            app_id,
            display_name,
            main_exe,
            install_directory,
            supervisor_id,
            icon,
            shortcuts,
            persistent_assets,
            environment,
        }
    }

    #[must_use]
    pub fn from_installer_manifest(manifest: &'a InstallerManifest, shortcuts: &'a [ShortcutLocation]) -> Self {
        Self::new(
            &manifest.app_id,
            &manifest.runtime.name,
            &manifest.runtime.main_exe,
            &manifest.runtime.install_directory,
            &manifest.runtime.supervisor_id,
            &manifest.runtime.icon,
            shortcuts,
            &manifest.runtime.persistent_assets,
            &manifest.runtime.environment,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::archive::packer::ArchivePacker;
    use crate::config::manifest::ShortcutLocation;
    use crate::context::StorageProvider;
    use crate::error::SurgeError;

    use super::{
        InstallProfile, LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH, RUNTIME_MANIFEST_RELATIVE_PATH, RuntimeManifestMetadata,
        install_package_locally_at_root, read_runtime_manifest_version, storage_provider_manifest_name,
        write_runtime_manifest,
    };

    #[test]
    fn storage_provider_manifest_name_maps_expected_values() {
        assert_eq!(storage_provider_manifest_name(None), "filesystem");
        assert_eq!(storage_provider_manifest_name(Some(StorageProvider::S3)), "s3");
        assert_eq!(
            storage_provider_manifest_name(Some(StorageProvider::AzureBlob)),
            "azure"
        );
        assert_eq!(storage_provider_manifest_name(Some(StorageProvider::Gcs)), "gcs");
        assert_eq!(
            storage_provider_manifest_name(Some(StorageProvider::Filesystem)),
            "filesystem"
        );
        assert_eq!(
            storage_provider_manifest_name(Some(StorageProvider::GitHubReleases)),
            "github_releases"
        );
    }

    #[test]
    fn write_runtime_manifest_creates_expected_file() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let environment = BTreeMap::new();
        let shortcuts: [ShortcutLocation; 0] = [];
        let persistent_assets: [String; 0] = [];
        let profile = InstallProfile::new(
            "demo-app",
            "Demo App",
            "demo",
            "demo-install",
            "demo-supervisor",
            "",
            &shortcuts,
            &persistent_assets,
            &environment,
        );
        let metadata =
            RuntimeManifestMetadata::new("1.2.3", "test", "azure", "demo-bucket", "", "https://example.invalid");

        let path = write_runtime_manifest(tmp.path(), &profile, &metadata).expect("runtime manifest should be written");
        let raw = std::fs::read_to_string(&path).expect("runtime manifest should be readable");
        let legacy_raw = std::fs::read_to_string(tmp.path().join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH))
            .expect("legacy runtime manifest should be readable");
        assert!(raw.contains("id: demo-app"));
        assert!(raw.contains("version: 1.2.3"));
        assert!(raw.contains("channel: test"));
        assert!(raw.contains("installDirectory: demo-install"));
        assert!(raw.contains("supervisorId: demo-supervisor"));
        assert!(raw.contains("provider: azure"));
        assert!(raw.contains("bucket: demo-bucket"));
        assert!(raw.contains("endpoint: https://example.invalid"));
        assert!(!raw.contains("region:"));
        assert_eq!(raw, legacy_raw);
    }

    #[test]
    fn read_runtime_manifest_version_prefers_current_manifest() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let current_path = tmp.path().join(RUNTIME_MANIFEST_RELATIVE_PATH);
        let legacy_path = tmp.path().join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH);
        std::fs::create_dir_all(current_path.parent().unwrap()).unwrap();
        std::fs::write(&current_path, "id: demo\nversion: 2.0.0\nchannel: test\n").unwrap();
        std::fs::write(&legacy_path, "id: demo\nversion: 1.0.0\nchannel: test\n").unwrap();

        let version = read_runtime_manifest_version(tmp.path()).unwrap();

        assert_eq!(version.as_deref(), Some("2.0.0"));
    }

    #[test]
    fn install_package_locally_preserves_declared_persistent_assets_and_prunes_snapshots() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let active_app_dir = install_root.join("app");
        let package_path = tmp.path().join("package.tar.zst");
        let environment = BTreeMap::new();
        let shortcuts: [ShortcutLocation; 0] = [];
        let persistent_assets = vec!["settings.json".to_string(), "state".to_string()];

        std::fs::create_dir_all(active_app_dir.join("state")).expect("state dir should exist");
        std::fs::create_dir_all(install_root.join("app-1.0.0")).expect("snapshot should exist");
        std::fs::create_dir_all(install_root.join("app-0.9.0")).expect("snapshot should exist");
        std::fs::write(active_app_dir.join("settings.json"), "persisted settings").expect("settings should exist");
        std::fs::write(active_app_dir.join("state").join("cache.bin"), "persisted cache").expect("state should exist");
        std::fs::write(active_app_dir.join("old.txt"), "remove me").expect("old file should exist");

        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer("demo", b"#!/bin/sh\necho demo\n", 0o755)
            .expect("main executable should be added");
        packer
            .add_buffer("settings.json", b"packaged settings", 0o644)
            .expect("settings should be added");
        packer
            .add_buffer("state/cache.bin", b"packaged cache", 0o644)
            .expect("state should be added");
        packer
            .add_buffer("payload.txt", b"new payload", 0o644)
            .expect("payload should be added");
        packer
            .finalize_to_file(&package_path)
            .expect("archive should be written");

        let profile = InstallProfile::new(
            "demo-app",
            "Demo App",
            "demo",
            "demo-install",
            "",
            "",
            &shortcuts,
            &persistent_assets,
            &environment,
        );

        install_package_locally_at_root(&profile, &package_path, &install_root).expect("install should succeed");

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("settings.json")).expect("settings should exist"),
            "persisted settings"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("state").join("cache.bin")).expect("state should exist"),
            "persisted cache"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload should exist"),
            "new payload"
        );
        assert!(
            !active_app_dir.join("old.txt").exists(),
            "undeclared assets should be removed"
        );
        assert!(
            !install_root.join("app-1.0.0").exists(),
            "stale snapshot should be pruned"
        );
        assert!(
            !install_root.join("app-0.9.0").exists(),
            "stale snapshot should be pruned"
        );
        assert!(
            !install_root.join(".surge-app-prev").exists(),
            "temporary previous dir should be removed"
        );
    }

    #[test]
    fn install_package_locally_restores_runtime_manifests_when_post_copy_work_fails() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let active_app_dir = install_root.join("app");
        let package_path = tmp.path().join("package.tar.zst");
        let environment = BTreeMap::new();
        let shortcuts = [ShortcutLocation::Desktop];
        let persistent_assets = vec![".surge".to_string()];
        let old_runtime_manifest = "id: demo-app\nversion: 1.0.0\nchannel: stable\n";
        let new_runtime_manifest = "id: demo-app\nversion: 1.1.0\nchannel: beta\n";

        std::fs::create_dir_all(active_app_dir.join(".surge")).expect(".surge dir should exist");
        std::fs::write(
            active_app_dir.join(RUNTIME_MANIFEST_RELATIVE_PATH),
            old_runtime_manifest,
        )
        .expect("old runtime manifest should exist");
        std::fs::write(
            active_app_dir.join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH),
            old_runtime_manifest,
        )
        .expect("old legacy runtime manifest should exist");
        std::fs::write(active_app_dir.join("payload.txt"), "old payload").expect("old payload should exist");

        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer(RUNTIME_MANIFEST_RELATIVE_PATH, new_runtime_manifest.as_bytes(), 0o644)
            .expect("runtime manifest should be added");
        packer
            .add_buffer(
                LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH,
                new_runtime_manifest.as_bytes(),
                0o644,
            )
            .expect("legacy runtime manifest should be added");
        packer
            .add_buffer("payload.txt", b"new payload", 0o644)
            .expect("payload should be added");
        packer
            .finalize_to_file(&package_path)
            .expect("archive should be written");

        let profile = InstallProfile::new(
            "demo-app",
            "Demo App",
            "",
            "demo-install",
            "",
            "",
            &shortcuts,
            &persistent_assets,
            &environment,
        );

        let err = install_package_locally_at_root(&profile, &package_path, &install_root)
            .expect_err("install should fail before shortcut creation");
        assert!(
            matches!(err, SurgeError::Config(ref message) if message.contains("shortcuts configured")),
            "unexpected error: {err}"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join(RUNTIME_MANIFEST_RELATIVE_PATH))
                .expect("runtime manifest should exist"),
            new_runtime_manifest
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH))
                .expect("legacy runtime manifest should exist"),
            new_runtime_manifest
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload should exist"),
            "new payload"
        );
    }
}
