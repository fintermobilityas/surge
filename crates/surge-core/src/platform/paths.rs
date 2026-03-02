use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

/// Return the current user's local app data directory.
///
/// This intentionally does not use XDG_DATA_HOME so behavior matches snapx.
pub fn local_app_data_dir() -> Result<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        if let Some(path) = std::env::var_os("LOCALAPPDATA") {
            return Ok(PathBuf::from(path));
        }
        if let Some(profile) = std::env::var_os("USERPROFILE") {
            return Ok(PathBuf::from(profile).join("AppData/Local"));
        }
        return Err(SurgeError::Platform(
            "Unable to determine Windows local app data directory".to_string(),
        ));
    }

    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")
            .ok_or_else(|| SurgeError::Platform("Unable to determine HOME directory".to_string()))?;
        return Ok(PathBuf::from(home).join("Library/Application Support"));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let home = std::env::var_os("HOME")
            .ok_or_else(|| SurgeError::Platform("Unable to determine HOME directory".to_string()))?;
        Ok(PathBuf::from(home).join(".local/share"))
    }

    #[cfg(not(any(windows, unix)))]
    {
        Err(SurgeError::Platform(
            "Unable to determine local app data directory on this platform".to_string(),
        ))
    }
}

/// Resolve an install directory rooted in local app data when relative.
///
/// - If `install_directory_name` is empty, uses `app_id`.
/// - If `install_directory_name` is absolute, returns it as-is.
/// - Otherwise returns `<local_app_data>/<install_directory_name>`.
pub fn default_install_root(app_id: &str, install_directory_name: &str) -> Result<PathBuf> {
    let name = if install_directory_name.trim().is_empty() {
        app_id.trim()
    } else {
        install_directory_name.trim()
    };

    if name.is_empty() {
        return Err(SurgeError::Config(
            "App id or install directory name is required".to_string(),
        ));
    }

    let candidate = Path::new(name);
    if candidate.is_absolute() {
        return Ok(candidate.to_path_buf());
    }

    Ok(local_app_data_dir()?.join(candidate))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_install_root_uses_app_id_when_install_directory_missing() {
        let path = default_install_root("demoapp", "").unwrap();
        assert!(path.ends_with("demoapp"));
    }

    #[test]
    fn test_default_install_root_preserves_absolute_paths() {
        #[cfg(target_os = "windows")]
        let absolute = "C:/apps/demoapp";
        #[cfg(not(target_os = "windows"))]
        let absolute = "/opt/demoapp";

        let path = default_install_root("demoapp", absolute).unwrap();
        assert_eq!(path, PathBuf::from(absolute));
    }
}
