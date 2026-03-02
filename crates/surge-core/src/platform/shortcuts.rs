use std::path::{Path, PathBuf};

use crate::config::manifest::ShortcutLocation;
use crate::error::{Result, SurgeError};

/// Create platform shortcuts for an installed release.
///
/// On Linux, this writes `.desktop` files into:
/// - `~/.local/share/applications` (desktop/start menu)
/// - `~/.config/autostart` (startup)
///
/// On other platforms, this is currently a no-op.
pub fn install_shortcuts(
    app_id: &str,
    app_dir: &Path,
    main_exe: &str,
    icon: &str,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    if shortcuts.is_empty() {
        return Ok(());
    }

    let main_exe_name = if main_exe.is_empty() { app_id } else { main_exe };
    let exe_path = resolve_target_path(app_dir, main_exe_name)?;
    let icon_path = if icon.is_empty() {
        exe_path.clone()
    } else {
        resolve_target_path(app_dir, icon)?
    };

    install_shortcuts_impl(app_id, &exe_path, &icon_path, shortcuts)
}

fn resolve_target_path(app_dir: &Path, relative_or_absolute: &str) -> Result<PathBuf> {
    let input_path = Path::new(relative_or_absolute);
    let path = if input_path.is_absolute() {
        input_path.to_path_buf()
    } else {
        app_dir.join(input_path)
    };

    if !path.exists() {
        return Err(SurgeError::Platform(format!(
            "Shortcut target path does not exist: {}",
            path.display()
        )));
    }

    Ok(path)
}

#[cfg(target_os = "linux")]
fn install_shortcuts_impl(
    app_id: &str,
    exe_path: &Path,
    icon_path: &Path,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    #[cfg(test)]
    if let Some(paths) = test_shortcut_paths_override() {
        return install_shortcuts_linux(app_id, exe_path, icon_path, shortcuts, &paths);
    }

    let paths = LinuxShortcutPaths::for_current_user()?;
    install_shortcuts_linux(app_id, exe_path, icon_path, shortcuts, &paths)
}

#[cfg(not(target_os = "linux"))]
fn install_shortcuts_impl(
    _app_id: &str,
    _exe_path: &Path,
    _icon_path: &Path,
    _shortcuts: &[ShortcutLocation],
) -> Result<()> {
    tracing::debug!("Shortcut installation is currently only implemented for Linux");
    Ok(())
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct LinuxShortcutPaths {
    applications_dir: PathBuf,
    autostart_dir: PathBuf,
}

#[cfg(target_os = "linux")]
impl LinuxShortcutPaths {
    fn for_current_user() -> Result<Self> {
        let home = std::env::var_os("HOME").ok_or_else(|| {
            SurgeError::Platform("Unable to determine HOME directory for shortcut installation".to_string())
        })?;
        let home = PathBuf::from(home);
        Ok(Self {
            applications_dir: home.join(".local/share/applications"),
            autostart_dir: home.join(".config/autostart"),
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
static TEST_SHORTCUT_PATHS_OVERRIDE: std::sync::Mutex<Option<LinuxShortcutPaths>> = std::sync::Mutex::new(None);

#[cfg(all(test, target_os = "linux"))]
fn test_shortcut_paths_override() -> Option<LinuxShortcutPaths> {
    TEST_SHORTCUT_PATHS_OVERRIDE.lock().ok()?.clone()
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn set_test_shortcut_paths_override(applications_dir: PathBuf, autostart_dir: PathBuf) {
    if let Ok(mut guard) = TEST_SHORTCUT_PATHS_OVERRIDE.lock() {
        *guard = Some(LinuxShortcutPaths {
            applications_dir,
            autostart_dir,
        });
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn clear_test_shortcut_paths_override() {
    if let Ok(mut guard) = TEST_SHORTCUT_PATHS_OVERRIDE.lock() {
        *guard = None;
    }
}

#[cfg(target_os = "linux")]
fn install_shortcuts_linux(
    app_id: &str,
    exe_path: &Path,
    icon_path: &Path,
    shortcuts: &[ShortcutLocation],
    paths: &LinuxShortcutPaths,
) -> Result<()> {
    let desktop_entry = build_desktop_entry(app_id, exe_path, icon_path);
    let file_name = format!("{}.desktop", sanitize_file_stem(app_id));

    for location in shortcuts {
        match location {
            ShortcutLocation::Desktop | ShortcutLocation::StartMenu => {
                std::fs::create_dir_all(&paths.applications_dir)?;
                let shortcut_path = paths.applications_dir.join(&file_name);
                crate::platform::fs::write_file_atomic(&shortcut_path, desktop_entry.as_bytes())?;
                crate::platform::fs::make_executable(&shortcut_path)?;
            }
            ShortcutLocation::Startup => {
                std::fs::create_dir_all(&paths.autostart_dir)?;
                let shortcut_path = paths.autostart_dir.join(&file_name);
                crate::platform::fs::write_file_atomic(&shortcut_path, desktop_entry.as_bytes())?;
                crate::platform::fs::make_executable(&shortcut_path)?;
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn build_desktop_entry(app_id: &str, exe_path: &Path, icon_path: &Path) -> String {
    let name = escape_desktop_value(app_id);
    let exe = escape_desktop_value(&exe_path.to_string_lossy());
    let icon = escape_desktop_value(&icon_path.to_string_lossy());

    format!(
        "[Desktop Entry]\nType=Application\nVersion=1.0\nName={name}\nExec=\"{exe}\"\nIcon={icon}\nTerminal=false\n"
    )
}

#[cfg(target_os = "linux")]
fn escape_desktop_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ")
}

#[cfg(target_os = "linux")]
fn sanitize_file_stem(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            result.push(ch);
        } else {
            result.push('-');
        }
    }

    if result.is_empty() {
        "surge-app".to_string()
    } else {
        result
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn test_install_shortcuts_linux_writes_desktop_and_startup_files() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("app-1.2.3");
        std::fs::create_dir_all(&app_dir).unwrap();

        let exe_path = app_dir.join("demoapp");
        std::fs::write(&exe_path, b"#!/bin/sh\necho hi\n").unwrap();
        crate::platform::fs::make_executable(&exe_path).unwrap();

        let icon_path = app_dir.join("icon.png");
        std::fs::write(&icon_path, b"png").unwrap();

        let paths = LinuxShortcutPaths {
            applications_dir: tmp.path().join("applications"),
            autostart_dir: tmp.path().join("autostart"),
        };

        install_shortcuts_linux(
            "demo-app",
            &exe_path,
            &icon_path,
            &[
                ShortcutLocation::Desktop,
                ShortcutLocation::StartMenu,
                ShortcutLocation::Startup,
            ],
            &paths,
        )
        .unwrap();

        let applications_shortcut = paths.applications_dir.join("demo-app.desktop");
        let startup_shortcut = paths.autostart_dir.join("demo-app.desktop");
        assert!(applications_shortcut.exists());
        assert!(startup_shortcut.exists());

        let content = std::fs::read_to_string(applications_shortcut).unwrap();
        assert!(content.contains("Name=demo-app"));
        assert!(content.contains("Exec=\""));
        assert!(content.contains("Icon="));
    }

    #[test]
    fn test_install_shortcuts_missing_main_exe_is_error() {
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("app-1.0.0");
        std::fs::create_dir_all(&app_dir).unwrap();

        let err = install_shortcuts("demo-app", &app_dir, "demo-app", "", &[ShortcutLocation::Desktop]).unwrap_err();

        assert!(err.to_string().contains("does not exist"));
    }
}
