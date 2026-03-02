//! Stub executable: resolves and launches the active installed application.

use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};
use crate::platform::fs::list_directories;
use crate::releases::version::compare_versions;

/// Resolve the active app directory under `install_dir`.
///
/// Preferred layout is a stable `app/` directory. For backwards compatibility,
/// this falls back to scanning versioned `app-{version}` directories and picks
/// the highest semantic version.
pub fn find_latest_app_dir(install_dir: &Path) -> Result<PathBuf> {
    let stable_app_dir = install_dir.join("app");
    if stable_app_dir.is_dir() {
        return Ok(stable_app_dir);
    }

    let dirs = list_directories(install_dir)?;

    let mut best_version: Option<String> = None;
    let mut best_dir: Option<String> = None;

    for dir_name in &dirs {
        if let Some(version) = dir_name.strip_prefix("app-") {
            if version.is_empty() {
                continue;
            }

            // Validate it looks like a version (starts with a digit)
            if !version.chars().next().is_some_and(|c| c.is_ascii_digit()) {
                continue;
            }

            if let Some(best) = &best_version {
                if compare_versions(version, best) == std::cmp::Ordering::Greater {
                    best_version = Some(version.to_string());
                    best_dir = Some(dir_name.clone());
                }
            } else {
                best_version = Some(version.to_string());
                best_dir = Some(dir_name.clone());
            }
        }
    }

    match best_dir {
        Some(dir) => Ok(install_dir.join(dir)),
        None => Err(SurgeError::NotFound(format!(
            "No app/ or app-* directories found in {}",
            install_dir.display()
        ))),
    }
}

/// Resolve the active app directory and exec into it.
///
/// Prefers `install_dir/app`, then falls back to latest `app-*` for legacy
/// installs, and replaces the current process with the application executable.
///
/// On Unix, this uses `execv` to replace the process image.
/// On Windows, this spawns the new process and exits.
///
/// # Arguments
///
/// * `install_dir` - The root installation directory
/// * `args` - Command-line arguments to pass to the application
pub fn exec_into_latest(install_dir: &Path, args: &[&str]) -> Result<()> {
    let app_dir = find_latest_app_dir(install_dir)?;

    // Look for the main executable in the app directory
    let exe_path = find_main_executable(&app_dir)?;

    tracing::info!(
        app_dir = %app_dir.display(),
        exe = %exe_path.display(),
        "Launching latest version"
    );

    crate::platform::process::exec_replace(&exe_path, args)
}

/// Find the main executable in an app directory.
///
/// Looks for common patterns:
/// 1. A file with the same name as the install directory
/// 2. A file named after the directory's parent
/// 3. The first executable file found
fn find_main_executable(app_dir: &Path) -> Result<PathBuf> {
    // First, look for an executable with the app directory name pattern
    if let Some(parent) = app_dir.parent()
        && let Some(parent_name) = parent.file_name().and_then(|n| n.to_str())
    {
        let candidate = app_dir.join(parent_name);
        if candidate.exists() && is_executable(&candidate) {
            return Ok(candidate);
        }

        // Try with common extensions
        for ext in &["", ".exe", ".bin"] {
            let candidate = app_dir.join(format!("{parent_name}{ext}"));
            if candidate.exists() && is_executable(&candidate) {
                return Ok(candidate);
            }
        }
    }

    // Scan for any executable file in the directory
    let entries = std::fs::read_dir(app_dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && is_executable(&path) {
            return Ok(path);
        }
    }

    Err(SurgeError::NotFound(format!(
        "No executable found in {}",
        app_dir.display()
    )))
}

/// Check if a path is an executable file.
#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    path.is_file()
        && std::fs::metadata(path)
            .map(|m| m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    // On Windows, check for common executable extensions
    path.is_file()
        && path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|ext| matches!(ext.to_lowercase().as_str(), "exe" | "cmd" | "bat"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_latest_app_dir_empty() {
        let dir = tempfile::tempdir().unwrap();
        let result = find_latest_app_dir(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_find_latest_app_dir_single() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("app-1.0.0")).unwrap();
        let result = find_latest_app_dir(dir.path()).unwrap();
        assert_eq!(result, dir.path().join("app-1.0.0"));
    }

    #[test]
    fn test_find_latest_app_dir_prefers_stable_app_directory() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("app")).unwrap();
        std::fs::create_dir(dir.path().join("app-9.9.9")).unwrap();
        let result = find_latest_app_dir(dir.path()).unwrap();
        assert_eq!(result, dir.path().join("app"));
    }

    #[test]
    fn test_find_latest_app_dir_multiple() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("app-1.0.0")).unwrap();
        std::fs::create_dir(dir.path().join("app-2.0.0")).unwrap();
        std::fs::create_dir(dir.path().join("app-1.5.0")).unwrap();
        let result = find_latest_app_dir(dir.path()).unwrap();
        assert_eq!(result, dir.path().join("app-2.0.0"));
    }

    #[test]
    fn test_find_latest_app_dir_ignores_non_app_dirs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("app-1.0.0")).unwrap();
        std::fs::create_dir(dir.path().join("other")).unwrap();
        std::fs::create_dir(dir.path().join("app-")).unwrap(); // invalid
        let result = find_latest_app_dir(dir.path()).unwrap();
        assert_eq!(result, dir.path().join("app-1.0.0"));
    }

    #[test]
    fn test_find_latest_app_dir_complex_versions() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("app-1.0.0")).unwrap();
        std::fs::create_dir(dir.path().join("app-1.0.1")).unwrap();
        std::fs::create_dir(dir.path().join("app-1.1.0")).unwrap();
        std::fs::create_dir(dir.path().join("app-10.0.0")).unwrap();
        let result = find_latest_app_dir(dir.path()).unwrap();
        assert_eq!(result, dir.path().join("app-10.0.0"));
    }
}
