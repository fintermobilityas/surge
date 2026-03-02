use std::path::{Path, PathBuf};

use crate::config::manifest::ShortcutLocation;
use crate::error::{Result, SurgeError};

/// Create platform shortcuts for an installed release.
///
/// Supported platforms:
/// - Linux: `.desktop` files in `~/.local/share/applications` and `~/.config/autostart`
/// - Windows: `.lnk` files in Desktop, Start Menu, and Startup folders
/// - macOS: `.app` launchers in Desktop/`~/Applications`, and LaunchAgent plists for startup
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
        None
    } else {
        Some(resolve_target_path(app_dir, icon)?)
    };

    install_shortcuts_impl(app_id, &exe_path, icon_path.as_deref(), shortcuts)
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
    icon_path: Option<&Path>,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    #[cfg(test)]
    if let Some(paths) = test_shortcut_paths_override() {
        let effective_icon = icon_path.unwrap_or(exe_path);
        return install_shortcuts_linux(app_id, exe_path, effective_icon, shortcuts, &paths);
    }

    let paths = LinuxShortcutPaths::for_current_user()?;
    let effective_icon = icon_path.unwrap_or(exe_path);
    install_shortcuts_linux(app_id, exe_path, effective_icon, shortcuts, &paths)
}

#[cfg(target_os = "windows")]
fn install_shortcuts_impl(
    app_id: &str,
    exe_path: &Path,
    icon_path: Option<&Path>,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    let effective_icon = icon_path.unwrap_or(exe_path);
    install_shortcuts_windows(app_id, exe_path, effective_icon, shortcuts)
}

#[cfg(target_os = "macos")]
fn install_shortcuts_impl(
    app_id: &str,
    exe_path: &Path,
    icon_path: Option<&Path>,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    install_shortcuts_macos(app_id, exe_path, icon_path, shortcuts)
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
fn install_shortcuts_impl(
    _app_id: &str,
    _exe_path: &Path,
    _icon_path: Option<&Path>,
    _shortcuts: &[ShortcutLocation],
) -> Result<()> {
    tracing::debug!("Shortcut installation is currently not implemented for this platform");
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
    let desktop_entry = build_desktop_entry_linux(app_id, exe_path, icon_path);
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
fn build_desktop_entry_linux(app_id: &str, exe_path: &Path, icon_path: &Path) -> String {
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

#[cfg(target_os = "windows")]
struct WindowsShortcutPaths {
    desktop_dir: PathBuf,
    start_menu_dir: PathBuf,
    startup_dir: PathBuf,
}

#[cfg(target_os = "windows")]
impl WindowsShortcutPaths {
    fn for_current_user() -> Result<Self> {
        let user_profile = std::env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .ok_or_else(|| SurgeError::Platform("Unable to determine USERPROFILE".to_string()))?;
        let app_data = std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .ok_or_else(|| SurgeError::Platform("Unable to determine APPDATA".to_string()))?;

        Ok(Self {
            desktop_dir: user_profile.join("Desktop"),
            start_menu_dir: app_data.join("Microsoft/Windows/Start Menu/Programs"),
            startup_dir: app_data.join("Microsoft/Windows/Start Menu/Programs/Startup"),
        })
    }
}

#[cfg(target_os = "windows")]
fn install_shortcuts_windows(
    app_id: &str,
    exe_path: &Path,
    icon_path: &Path,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    let paths = WindowsShortcutPaths::for_current_user()?;
    let file_name = format!("{}.lnk", sanitize_file_stem(app_id));
    let working_dir = exe_path.parent().unwrap_or_else(|| Path::new("."));

    for location in shortcuts {
        let target_dir = match location {
            ShortcutLocation::Desktop => &paths.desktop_dir,
            ShortcutLocation::StartMenu => &paths.start_menu_dir,
            ShortcutLocation::Startup => &paths.startup_dir,
        };

        std::fs::create_dir_all(target_dir)?;
        let shortcut_path = target_dir.join(&file_name);
        create_windows_shortcut(&shortcut_path, exe_path, icon_path, working_dir)?;
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn create_windows_shortcut(shortcut_path: &Path, exe_path: &Path, icon_path: &Path, working_dir: &Path) -> Result<()> {
    let shortcut = escape_powershell_single_quoted(&shortcut_path.to_string_lossy());
    let exe = escape_powershell_single_quoted(&exe_path.to_string_lossy());
    let icon = escape_powershell_single_quoted(&icon_path.to_string_lossy());
    let working = escape_powershell_single_quoted(&working_dir.to_string_lossy());

    let script = format!(
        "$ws = New-Object -ComObject WScript.Shell; $lnk = $ws.CreateShortcut('{shortcut}'); $lnk.TargetPath = '{exe}'; $lnk.WorkingDirectory = '{working}'; $lnk.IconLocation = '{icon}'; $lnk.Save()"
    );

    run_powershell_script(&script)
}

#[cfg(target_os = "windows")]
fn run_powershell_script(script: &str) -> Result<()> {
    let mut last_error = String::new();

    for shell in ["powershell", "pwsh"] {
        match std::process::Command::new(shell)
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
            ])
            .arg(script)
            .output()
        {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                last_error = format!("{shell} exited with {}: {stderr}", output.status);
            }
            Err(e) => {
                last_error = format!("Failed to execute {shell}: {e}");
            }
        }
    }

    Err(SurgeError::Platform(format!(
        "Unable to create Windows shortcut: {last_error}"
    )))
}

#[cfg(target_os = "windows")]
fn escape_powershell_single_quoted(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(target_os = "macos")]
struct MacShortcutPaths {
    desktop_dir: PathBuf,
    applications_dir: PathBuf,
    launch_agents_dir: PathBuf,
}

#[cfg(target_os = "macos")]
impl MacShortcutPaths {
    fn for_current_user() -> Result<Self> {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| SurgeError::Platform("Unable to determine HOME directory".to_string()))?;

        Ok(Self {
            desktop_dir: home.join("Desktop"),
            applications_dir: home.join("Applications"),
            launch_agents_dir: home.join("Library/LaunchAgents"),
        })
    }
}

#[cfg(target_os = "macos")]
fn install_shortcuts_macos(
    app_id: &str,
    exe_path: &Path,
    icon_path: Option<&Path>,
    shortcuts: &[ShortcutLocation],
) -> Result<()> {
    let paths = MacShortcutPaths::for_current_user()?;
    let app_name = sanitize_file_stem(app_id);

    for location in shortcuts {
        match location {
            ShortcutLocation::Desktop => {
                std::fs::create_dir_all(&paths.desktop_dir)?;
                let app_bundle_dir = paths.desktop_dir.join(format!("{app_name}.app"));
                create_macos_app_bundle(&app_bundle_dir, app_id, exe_path, icon_path)?;
            }
            ShortcutLocation::StartMenu => {
                std::fs::create_dir_all(&paths.applications_dir)?;
                let app_bundle_dir = paths.applications_dir.join(format!("{app_name}.app"));
                create_macos_app_bundle(&app_bundle_dir, app_id, exe_path, icon_path)?;
            }
            ShortcutLocation::Startup => {
                std::fs::create_dir_all(&paths.launch_agents_dir)?;
                let plist_path = paths
                    .launch_agents_dir
                    .join(format!("com.surge.{}.plist", sanitize_file_stem(app_id).to_lowercase()));
                create_launch_agent_plist(&plist_path, app_id, exe_path)?;
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn create_macos_app_bundle(
    app_bundle_dir: &Path,
    app_id: &str,
    exe_path: &Path,
    icon_path: Option<&Path>,
) -> Result<()> {
    if app_bundle_dir.exists() {
        std::fs::remove_dir_all(app_bundle_dir)?;
    }

    let contents_dir = app_bundle_dir.join("Contents");
    let macos_dir = contents_dir.join("MacOS");
    let resources_dir = contents_dir.join("Resources");
    std::fs::create_dir_all(&macos_dir)?;
    std::fs::create_dir_all(&resources_dir)?;

    let launcher_name = sanitize_file_stem(app_id);
    let launcher_path = macos_dir.join(&launcher_name);
    let launcher_content = format!(
        "#!/bin/sh\nexec \"{}\" \"$@\"\n",
        escape_shell_double_quoted(&exe_path.to_string_lossy())
    );
    crate::platform::fs::write_file_atomic(&launcher_path, launcher_content.as_bytes())?;
    crate::platform::fs::make_executable(&launcher_path)?;

    let icon_file_stem = if let Some(icon_path) = icon_path {
        let icon_file_name = icon_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("AppIcon.icns");
        let copied_icon_path = resources_dir.join(icon_file_name);
        std::fs::copy(icon_path, &copied_icon_path)?;

        copied_icon_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(std::string::ToString::to_string)
    } else {
        None
    };

    let info_plist = build_macos_info_plist(app_id, &launcher_name, icon_file_stem.as_deref());
    crate::platform::fs::write_file_atomic(&contents_dir.join("Info.plist"), info_plist.as_bytes())?;

    Ok(())
}

#[cfg(target_os = "macos")]
fn create_launch_agent_plist(path: &Path, app_id: &str, exe_path: &Path) -> Result<()> {
    let label = format!("com.surge.{}", sanitize_file_stem(app_id).to_lowercase());
    let exe = escape_xml(&exe_path.to_string_lossy());
    let label_xml = escape_xml(&label);

    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{label_xml}</string>\n  <key>ProgramArguments</key>\n  <array>\n    <string>{exe}</string>\n  </array>\n  <key>RunAtLoad</key>\n  <true/>\n</dict>\n</plist>\n"
    );

    crate::platform::fs::write_file_atomic(path, plist.as_bytes())
}

#[cfg(target_os = "macos")]
fn build_macos_info_plist(app_id: &str, launcher_name: &str, icon_file_stem: Option<&str>) -> String {
    let app_name = escape_xml(app_id);
    let launcher = escape_xml(launcher_name);
    let bundle_identifier = escape_xml(&format!("com.surge.{}", sanitize_file_stem(app_id).to_lowercase()));

    let icon_section = icon_file_stem.map_or(String::new(), |icon| {
        format!(
            "  <key>CFBundleIconFile</key>\n  <string>{}</string>\n",
            escape_xml(icon)
        )
    });

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>CFBundleName</key>\n  <string>{app_name}</string>\n  <key>CFBundleDisplayName</key>\n  <string>{app_name}</string>\n  <key>CFBundleIdentifier</key>\n  <string>{bundle_identifier}</string>\n  <key>CFBundleVersion</key>\n  <string>1</string>\n  <key>CFBundlePackageType</key>\n  <string>APPL</string>\n  <key>CFBundleExecutable</key>\n  <string>{launcher}</string>\n{icon_section}</dict>\n</plist>\n"
    )
}

#[cfg(target_os = "macos")]
fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "macos")]
fn escape_shell_double_quoted(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`")
}

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
