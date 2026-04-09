mod linux;

#[cfg(target_os = "macos")]
mod macos;

mod shared;

#[cfg(target_os = "windows")]
mod windows;

use std::collections::BTreeMap;
use std::path::Path;

use crate::config::manifest::ShortcutLocation;
use crate::error::Result;

pub use self::linux::{LinuxShortcutFile, render_linux_shortcut_files};

#[cfg(all(test, target_os = "linux"))]
pub(crate) use self::linux::{
    clear_test_shortcut_paths_override, lock_test_shortcut_environment, lock_test_shortcut_environment_async,
    set_test_shortcut_paths_override,
};

/// Create platform shortcuts for an installed release.
///
/// Supported platforms:
/// - Linux: `.desktop` files in `~/.local/share/applications` and `~/.config/autostart`
/// - Windows: `.lnk` files in Desktop, Start Menu, and Startup folders
/// - macOS: `.app` launchers in Desktop/`~/Applications`, and LaunchAgent plists for startup
///
/// Desktop and StartMenu shortcuts launch the app directly.
/// Startup shortcuts launch via the supervisor when `supervisor_id` is set.
pub fn install_shortcuts(
    app_id: &str,
    name: &str,
    app_dir: &Path,
    main_exe: &str,
    supervisor_id: &str,
    icon: &str,
    shortcuts: &[ShortcutLocation],
    environment: &BTreeMap<String, String>,
) -> Result<()> {
    if shortcuts.is_empty() {
        return Ok(());
    }

    let display_name = if name.is_empty() { app_id } else { name };
    let main_exe_name = if main_exe.is_empty() { app_id } else { main_exe };
    let exe_path = shared::resolve_target_path(app_dir, main_exe_name)?;
    #[cfg(target_os = "linux")]
    let icon_path = linux::resolve_linux_shortcut_icon_path(app_id, app_dir, main_exe_name, icon)?;
    #[cfg(not(target_os = "linux"))]
    let icon_path = if icon.is_empty() {
        None
    } else {
        Some(shared::resolve_target_path(app_dir, icon)?)
    };

    let install_root = app_dir.parent().unwrap_or(app_dir);

    #[cfg(target_os = "linux")]
    {
        let effective_icon = icon_path.as_deref().unwrap_or(&exe_path);

        #[cfg(test)]
        if let Some(paths) = linux::test_shortcut_paths_override() {
            return linux::install_shortcuts(
                app_id,
                display_name,
                &exe_path,
                effective_icon,
                supervisor_id,
                install_root,
                shortcuts,
                environment,
                &paths,
            );
        }

        let paths = linux::LinuxShortcutPaths::for_current_user()?;
        linux::install_shortcuts(
            app_id,
            display_name,
            &exe_path,
            effective_icon,
            supervisor_id,
            install_root,
            shortcuts,
            environment,
            &paths,
        )
    }

    #[cfg(target_os = "windows")]
    {
        windows::install_shortcuts(
            display_name,
            &exe_path,
            icon_path.as_deref(),
            supervisor_id,
            install_root,
            shortcuts,
            environment,
        )
    }

    #[cfg(target_os = "macos")]
    {
        macos::install_shortcuts(
            display_name,
            &exe_path,
            icon_path.as_deref(),
            supervisor_id,
            install_root,
            shortcuts,
            environment,
        )
    }

    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        tracing::debug!("Shortcut installation is currently not implemented for this platform");
        Ok(())
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::linux::{LinuxShortcutPaths, install_shortcuts as install_shortcuts_linux};
    use super::*;

    #[test]
    fn test_install_shortcuts_linux_writes_desktop_and_startup_files() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let tmp = tempfile::tempdir().unwrap();
        let install_root = tmp.path().join("install");
        let app_dir = install_root.join("app");
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
            "Demo App",
            &exe_path,
            &icon_path,
            "",
            &install_root,
            &[
                ShortcutLocation::Desktop,
                ShortcutLocation::StartMenu,
                ShortcutLocation::Startup,
            ],
            &BTreeMap::new(),
            &paths,
        )
        .unwrap();

        let applications_shortcut = paths.applications_dir.join("demoapp.desktop");
        let startup_shortcut = paths.autostart_dir.join("demoapp.desktop");
        assert!(applications_shortcut.exists());
        assert!(startup_shortcut.exists());

        let content = std::fs::read_to_string(applications_shortcut).unwrap();
        assert!(content.contains("Name=Demo App"));
        assert!(content.contains("Exec=\""));
        assert!(content.contains("Icon="));
        assert!(content.contains("Path="));
        assert!(content.contains("StartupNotify=true"));
        assert!(content.contains("StartupWMClass=demoapp"));
    }

    #[test]
    fn test_install_shortcuts_linux_with_env_vars() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let tmp = tempfile::tempdir().unwrap();
        let install_root = tmp.path().join("install");
        let app_dir = install_root.join("app");
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

        let mut env = BTreeMap::new();
        env.insert("MY_VAR".to_string(), "my_value".to_string());

        install_shortcuts_linux(
            "demoapp",
            "demoapp",
            &exe_path,
            &icon_path,
            "",
            &install_root,
            &[ShortcutLocation::Desktop],
            &env,
            &paths,
        )
        .unwrap();

        let content = std::fs::read_to_string(paths.applications_dir.join("demoapp.desktop")).unwrap();
        assert!(
            content.contains("Exec=env MY_VAR=my_value"),
            "desktop entry should contain env vars: {content}"
        );
    }

    #[test]
    fn test_render_linux_shortcut_files_emits_application_and_autostart_entries() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let install_root = PathBuf::from("/home/demo/.local/share/demoapp");
        let exe_path = install_root.join("app").join("demoapp");
        let icon_path = install_root.join("app").join("icon.png");
        let mut env = BTreeMap::new();
        env.insert("DISPLAY".to_string(), ":0".to_string());

        let rendered = render_linux_shortcut_files(
            "demo-app",
            "Demo App",
            &exe_path,
            &icon_path,
            "demo-supervisor",
            &install_root,
            &[ShortcutLocation::Desktop, ShortcutLocation::Startup],
            &env,
        );

        assert_eq!(rendered.len(), 2);
        assert_eq!(rendered[0].file_name, "demoapp.desktop");
        assert!(rendered[0].content.contains("Exec=env DISPLAY=:0"));
        assert!(rendered[0].content.contains("StartupWMClass=demoapp"));
        assert!(rendered[1].content.contains("surge-supervisor"));
        assert!(rendered[1].content.contains("--id demo-supervisor"));
        assert!(rendered[1].content.contains("StartupWMClass=demoapp"));
    }

    #[test]
    fn test_install_shortcuts_linux_startup_with_supervisor() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let tmp = tempfile::tempdir().unwrap();
        let install_root = tmp.path().join("install");
        let app_dir = install_root.join("app");
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

        let mut env = BTreeMap::new();
        env.insert("MY_VAR".to_string(), "my_value".to_string());

        install_shortcuts_linux(
            "demoapp",
            "demoapp",
            &exe_path,
            &icon_path,
            "my-supervisor-id",
            &install_root,
            &[ShortcutLocation::Startup],
            &env,
            &paths,
        )
        .unwrap();

        let content = std::fs::read_to_string(paths.autostart_dir.join("demoapp.desktop")).unwrap();
        assert!(
            content.contains("Exec=env MY_VAR=my_value"),
            "startup entry should contain env vars: {content}"
        );
        assert!(
            content.contains("surge-supervisor"),
            "startup entry should reference supervisor: {content}"
        );
        assert!(
            content.contains("--id my-supervisor-id"),
            "startup entry should contain supervisor id: {content}"
        );
    }

    #[test]
    fn test_install_shortcuts_missing_main_exe_is_error() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let tmp = tempfile::tempdir().unwrap();
        let app_dir = tmp.path().join("app-1.0.0");
        std::fs::create_dir_all(&app_dir).unwrap();

        let err = install_shortcuts(
            "demo-app",
            "demo-app",
            &app_dir,
            "demo-app",
            "",
            "",
            &[ShortcutLocation::Desktop],
            &BTreeMap::new(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn test_install_shortcuts_falls_back_to_bundled_surge_icon() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let tmp = tempfile::tempdir().unwrap();
        let install_root = tmp.path().join("install");
        let app_dir = install_root.join("app");
        std::fs::create_dir_all(&app_dir).unwrap();

        let exe_path = app_dir.join("demoapp");
        std::fs::write(&exe_path, b"#!/bin/sh\necho hi\n").unwrap();
        crate::platform::fs::make_executable(&exe_path).unwrap();

        let applications_dir = tmp.path().join("applications");
        let autostart_dir = tmp.path().join("autostart");
        set_test_shortcut_paths_override(applications_dir.clone(), autostart_dir);
        let install_result = install_shortcuts(
            "demoapp",
            "demoapp",
            &app_dir,
            "demoapp",
            "",
            "",
            &[ShortcutLocation::Desktop],
            &BTreeMap::new(),
        );
        clear_test_shortcut_paths_override();
        install_result.expect("shortcut install should succeed");

        let fallback_icon = app_dir.join(".surge").join("surge-logo.svg");
        assert!(fallback_icon.is_file(), "fallback icon should be written");

        let desktop_entry = std::fs::read_to_string(applications_dir.join("demoapp.desktop")).unwrap();
        assert!(
            desktop_entry.contains(&format!("Icon={}", fallback_icon.display())),
            "desktop entry should reference fallback icon: {desktop_entry}"
        );
    }

    #[test]
    fn test_install_shortcuts_linux_removes_stale_legacy_desktop_entries() {
        let _shortcut_env_lock = lock_test_shortcut_environment();
        let tmp = tempfile::tempdir().unwrap();
        let install_root = tmp.path().join("install");
        let app_dir = install_root.join("app");
        std::fs::create_dir_all(&app_dir).unwrap();

        let exe_path = app_dir.join("horizon");
        std::fs::write(&exe_path, b"#!/bin/sh\necho hi\n").unwrap();
        crate::platform::fs::make_executable(&exe_path).unwrap();

        let icon_path = app_dir.join("icon.png");
        std::fs::write(&icon_path, b"png").unwrap();

        let paths = LinuxShortcutPaths {
            applications_dir: tmp.path().join("applications"),
            autostart_dir: tmp.path().join("autostart"),
        };
        std::fs::create_dir_all(&paths.applications_dir).unwrap();
        std::fs::create_dir_all(&paths.autostart_dir).unwrap();
        std::fs::write(paths.applications_dir.join("Horizon.desktop"), b"legacy").unwrap();
        std::fs::write(paths.applications_dir.join("horizon-linux-x64.desktop"), b"legacy").unwrap();
        std::fs::write(paths.autostart_dir.join("Horizon.desktop"), b"legacy").unwrap();
        std::fs::write(paths.autostart_dir.join("horizon-linux-x64.desktop"), b"legacy").unwrap();

        install_shortcuts_linux(
            "horizon-linux-x64",
            "Horizon",
            &exe_path,
            &icon_path,
            "",
            &install_root,
            &[ShortcutLocation::Desktop, ShortcutLocation::Startup],
            &BTreeMap::new(),
            &paths,
        )
        .unwrap();

        assert!(paths.applications_dir.join("horizon.desktop").exists());
        assert!(paths.autostart_dir.join("horizon.desktop").exists());
        assert!(!paths.applications_dir.join("Horizon.desktop").exists());
        assert!(!paths.applications_dir.join("horizon-linux-x64.desktop").exists());
        assert!(!paths.autostart_dir.join("Horizon.desktop").exists());
        assert!(!paths.autostart_dir.join("horizon-linux-x64.desktop").exists());
    }
}
