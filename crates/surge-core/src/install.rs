use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde::Serialize;

use crate::archive::extractor::extract_file_to;
use crate::config::installer::InstallerManifest;
use crate::config::manifest::ShortcutLocation;
use crate::context::StorageProvider;
use crate::error::{Result, SurgeError};
use crate::platform::paths::default_install_root;
use crate::platform::process::spawn_detached;
use crate::platform::shortcuts::install_shortcuts;

pub const RUNTIME_MANIFEST_RELATIVE_PATH: &str = ".surge/runtime.yml";
pub const LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH: &str = ".surge/surge.yml";

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
    pub environment: &'a BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeManifestMetadata<'a> {
    pub version: &'a str,
    pub channel: &'a str,
    pub storage_provider: &'a str,
    pub storage_bucket: &'a str,
    pub storage_region: &'a str,
    pub storage_endpoint: &'a str,
}

impl<'a> RuntimeManifestMetadata<'a> {
    #[must_use]
    pub fn new(
        version: &'a str,
        channel: &'a str,
        storage_provider: &'a str,
        storage_bucket: &'a str,
        storage_region: &'a str,
        storage_endpoint: &'a str,
    ) -> Self {
        Self {
            version,
            channel,
            storage_provider,
            storage_bucket,
            storage_region,
            storage_endpoint,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeManifestFile<'a> {
    id: &'a str,
    version: &'a str,
    channel: &'a str,
    #[serde(rename = "installDirectory")]
    install_directory: &'a str,
    provider: &'a str,
    bucket: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    region: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    endpoint: &'a str,
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
            &manifest.runtime.environment,
        )
    }
}

#[must_use]
pub fn storage_provider_manifest_name(provider: Option<StorageProvider>) -> &'static str {
    match provider.unwrap_or(StorageProvider::Filesystem) {
        StorageProvider::S3 => "s3",
        StorageProvider::AzureBlob => "azure",
        StorageProvider::Gcs => "gcs",
        StorageProvider::Filesystem => "filesystem",
        StorageProvider::GitHubReleases => "github_releases",
    }
}

pub fn write_runtime_manifest(
    active_app_dir: &Path,
    profile: &InstallProfile<'_>,
    metadata: &RuntimeManifestMetadata<'_>,
) -> Result<std::path::PathBuf> {
    let manifest = RuntimeManifestFile {
        id: profile.app_id.trim(),
        version: metadata.version.trim(),
        channel: metadata.channel.trim(),
        install_directory: profile.install_directory.trim(),
        provider: metadata.storage_provider.trim(),
        bucket: metadata.storage_bucket.trim(),
        region: metadata.storage_region.trim(),
        endpoint: metadata.storage_endpoint.trim(),
    };

    if manifest.id.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: app id is empty".to_string(),
        ));
    }
    if manifest.version.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: version is empty".to_string(),
        ));
    }
    if manifest.channel.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: channel is empty".to_string(),
        ));
    }
    if manifest.provider.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: storage provider is empty".to_string(),
        ));
    }

    let runtime_manifest_path = active_app_dir.join(RUNTIME_MANIFEST_RELATIVE_PATH);
    if let Some(parent) = runtime_manifest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let legacy_manifest_path = active_app_dir.join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH);
    if let Some(parent) = legacy_manifest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut yaml = serde_yaml::to_string(&manifest)
        .map_err(|e| SurgeError::Config(format!("Failed to serialize runtime manifest: {e}")))?;
    if !yaml.ends_with('\n') {
        yaml.push('\n');
    }
    std::fs::write(&runtime_manifest_path, &yaml)?;
    std::fs::write(&legacy_manifest_path, yaml)?;
    Ok(runtime_manifest_path)
}

/// Resolve the install root and install the package there.
pub fn install_package_locally(profile: &InstallProfile<'_>, package_path: &Path) -> Result<std::path::PathBuf> {
    let install_root = default_install_root(profile.app_id, profile.install_directory)?;
    install_package_locally_at_root(profile, package_path, &install_root)?;
    Ok(install_root)
}

/// Extract a package into `install_root/app` with atomic swap, then create shortcuts.
pub fn install_package_locally_at_root(
    profile: &InstallProfile<'_>,
    package_path: &Path,
    install_root: &Path,
) -> Result<()> {
    std::fs::create_dir_all(install_root)?;

    let active_app_dir = install_root.join("app");
    let next_app_dir = install_root.join(".surge-app-next");
    let previous_app_dir = install_root.join(".surge-app-prev");

    if next_app_dir.is_dir() {
        std::fs::remove_dir_all(&next_app_dir)?;
    }
    if previous_app_dir.is_dir() {
        std::fs::remove_dir_all(&previous_app_dir)?;
    }

    extract_file_to(package_path, &next_app_dir)?;

    if active_app_dir.is_dir() {
        std::fs::rename(&active_app_dir, &previous_app_dir)?;
    }

    if let Err(rename_err) = std::fs::rename(&next_app_dir, &active_app_dir) {
        if previous_app_dir.is_dir() && !active_app_dir.exists() {
            let _ = std::fs::rename(&previous_app_dir, &active_app_dir);
        }
        return Err(SurgeError::Io(rename_err));
    }

    if !profile.shortcuts.is_empty() {
        let main_exe = profile.main_exe.trim();
        if main_exe.is_empty() {
            return Err(SurgeError::Config(format!(
                "App '{}' has shortcuts configured but no main executable metadata",
                profile.app_id
            )));
        }
        install_shortcuts(
            profile.app_id,
            profile.display_name,
            &active_app_dir,
            main_exe,
            profile.supervisor_id,
            profile.icon,
            profile.shortcuts,
            profile.environment,
        )?;
    }

    if previous_app_dir.is_dir() {
        std::fs::remove_dir_all(previous_app_dir)?;
    }

    Ok(())
}

/// Start the installed application, using the supervisor if configured.
pub fn auto_start_after_install(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
) -> Result<u32> {
    start_installed_application(profile, install_root, active_app_dir, true, true, false)
}

/// Launch the installed application for user-facing "Launch" actions.
///
/// Unlike `auto_start_after_install`, this does not pass the `--surge-installed`
/// lifecycle argument and should keep GUI apps open for immediate use.
pub fn launch_installed_application(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
) -> Result<u32> {
    start_installed_application(profile, install_root, active_app_dir, false, false, true)
}

fn start_installed_application(
    profile: &InstallProfile<'_>,
    install_root: &Path,
    active_app_dir: &Path,
    include_lifecycle_flag: bool,
    prefer_supervisor: bool,
    verify_running: bool,
) -> Result<u32> {
    let main_exe = profile.main_exe.trim();
    if main_exe.is_empty() {
        return Err(SurgeError::Config(
            "Cannot auto-start: no main executable in release metadata".to_string(),
        ));
    }

    let exe_path = active_app_dir.join(main_exe);

    let supervisor_id = profile.supervisor_id.trim();
    if prefer_supervisor && !supervisor_id.is_empty() {
        let supervisor_path = active_app_dir.join(crate::platform::process::supervisor_binary_name());

        let install_root_str = install_root.to_string_lossy();
        let exe_path_str = exe_path.to_string_lossy();
        let mut args: Vec<&str> = vec![
            "--supervisor-id",
            supervisor_id,
            "--install-dir",
            &install_root_str,
            "--exe-path",
            &exe_path_str,
        ];
        if include_lifecycle_flag {
            args.push("--");
            args.push("--surge-installed");
        }
        let mut handle = spawn_detached(&supervisor_path, &args, Some(install_root), profile.environment)?;
        let pid = handle.pid();
        if verify_running {
            verify_process_stays_running(&mut handle, "supervisor")?;
        }
        return Ok(pid);
    }

    let app_args: &[&str] = if include_lifecycle_flag {
        &["--surge-installed"]
    } else {
        &[]
    };
    let mut handle = spawn_detached(&exe_path, app_args, Some(install_root), profile.environment)?;
    let pid = handle.pid();
    if verify_running {
        verify_process_stays_running(&mut handle, "application")?;
    }
    Ok(pid)
}

fn verify_process_stays_running(
    handle: &mut crate::platform::process::ProcessHandle,
    process_label: &str,
) -> Result<()> {
    let check_interval = Duration::from_millis(200);
    let total_wait = Duration::from_secs(4);
    let checks = (total_wait.as_millis() / check_interval.as_millis()) as usize;

    for _ in 0..checks {
        std::thread::sleep(check_interval);
        if !handle.poll_running() {
            let result = handle.wait()?;
            return Err(SurgeError::Platform(format!(
                "Failed to launch {process_label}: process exited shortly after start with code {}",
                result.exit_code
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let profile = InstallProfile::new(
            "demo-app",
            "Demo App",
            "demo",
            "demo-install",
            "",
            "",
            &shortcuts,
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
        assert!(raw.contains("provider: azure"));
        assert!(raw.contains("bucket: demo-bucket"));
        assert!(raw.contains("endpoint: https://example.invalid"));
        assert!(!raw.contains("region:"));
        assert_eq!(raw, legacy_raw);
    }
}
