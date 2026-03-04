use std::collections::BTreeMap;
use std::path::Path;

use crate::archive::extractor::extract_file_to;
use crate::config::manifest::ShortcutLocation;
use crate::error::{Result, SurgeError};
use crate::platform::paths::default_install_root;
use crate::platform::process::spawn_detached;
use crate::platform::shortcuts::install_shortcuts;

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
    let main_exe = profile.main_exe.trim();
    if main_exe.is_empty() {
        return Err(SurgeError::Config(
            "Cannot auto-start: no main executable in release metadata".to_string(),
        ));
    }

    let exe_path = active_app_dir.join(main_exe);

    let supervisor_id = profile.supervisor_id.trim();
    if !supervisor_id.is_empty() {
        let supervisor_path = active_app_dir.join(crate::platform::process::supervisor_binary_name());

        let install_root_str = install_root.to_string_lossy();
        let exe_path_str = exe_path.to_string_lossy();
        let args: Vec<&str> = vec![
            "--supervisor-id",
            supervisor_id,
            "--install-dir",
            &install_root_str,
            "--exe-path",
            &exe_path_str,
            "--",
            "--surge-installed",
        ];
        let handle = spawn_detached(&supervisor_path, &args, Some(install_root), profile.environment)?;
        return Ok(handle.pid());
    }

    let handle = spawn_detached(
        &exe_path,
        &["--surge-installed"],
        Some(install_root),
        profile.environment,
    )?;
    Ok(handle.pid())
}
