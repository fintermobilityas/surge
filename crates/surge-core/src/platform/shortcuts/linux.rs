use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::manifest::ShortcutLocation;

#[cfg(target_os = "linux")]
use crate::error::{Result, SurgeError};

use super::shared::sanitize_file_stem;

#[cfg(target_os = "linux")]
use super::shared::resolve_target_path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxShortcutFile {
    pub location: ShortcutLocation,
    pub file_name: String,
    pub content: String,
}

#[cfg(target_os = "linux")]
pub(super) fn resolve_linux_shortcut_icon_path(
    app_id: &str,
    app_dir: &Path,
    main_exe_name: &str,
    configured_icon: &str,
) -> Result<Option<PathBuf>> {
    let configured_icon = configured_icon.trim();
    if !configured_icon.is_empty() {
        return Ok(Some(resolve_target_path(app_dir, configured_icon)?));
    }

    for candidate in linux_icon_candidates(app_id, app_dir, main_exe_name) {
        if candidate.is_file() {
            return Ok(Some(candidate));
        }
    }

    Ok(write_linux_fallback_icon(app_dir))
}

#[cfg(target_os = "linux")]
fn linux_icon_candidates(app_id: &str, app_dir: &Path, main_exe_name: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    let stems = [main_exe_name.trim(), app_id.trim(), "icon", "logo"];
    let exts = ["svg", "png", "xpm"];

    for stem in stems {
        if stem.is_empty() {
            continue;
        }
        for ext in exts {
            candidates.push(app_dir.join(format!("{stem}.{ext}")));
            candidates.push(app_dir.join(".surge").join(format!("{stem}.{ext}")));
        }
    }

    candidates
}

#[cfg(target_os = "linux")]
fn write_linux_fallback_icon(app_dir: &Path) -> Option<PathBuf> {
    const SURGE_FALLBACK_ICON_BYTES: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/logo.svg"));
    let icon_path = app_dir.join(".surge").join("surge-logo.svg");
    if let Some(parent) = icon_path.parent()
        && std::fs::create_dir_all(parent).is_err()
    {
        return None;
    }
    if std::fs::write(&icon_path, SURGE_FALLBACK_ICON_BYTES).is_err() {
        return None;
    }
    Some(icon_path)
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
pub(crate) struct LinuxShortcutPaths {
    pub(crate) applications_dir: PathBuf,
    pub(crate) autostart_dir: PathBuf,
}

#[cfg(target_os = "linux")]
impl LinuxShortcutPaths {
    pub(crate) fn for_current_user() -> Result<Self> {
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
static TEST_SHORTCUT_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn test_shortcut_paths_override() -> Option<LinuxShortcutPaths> {
    TEST_SHORTCUT_PATHS_OVERRIDE.lock().ok()?.clone()
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) fn lock_test_shortcut_environment() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_SHORTCUT_ENV_LOCK.blocking_lock()
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) async fn lock_test_shortcut_environment_async() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_SHORTCUT_ENV_LOCK.lock().await
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn install_shortcuts(
    app_id: &str,
    name: &str,
    exe_path: &Path,
    icon_path: &Path,
    supervisor_id: &str,
    install_root: &Path,
    shortcuts: &[ShortcutLocation],
    environment: &BTreeMap<String, String>,
    paths: &LinuxShortcutPaths,
) -> Result<()> {
    let legacy_file_names = linux_legacy_shortcut_file_names(app_id, name, exe_path);

    for rendered in render_linux_shortcut_files(
        app_id,
        name,
        exe_path,
        icon_path,
        supervisor_id,
        install_root,
        shortcuts,
        environment,
    ) {
        let target_dir = match rendered.location {
            ShortcutLocation::Desktop | ShortcutLocation::StartMenu => {
                std::fs::create_dir_all(&paths.applications_dir)?;
                &paths.applications_dir
            }
            ShortcutLocation::Startup => {
                std::fs::create_dir_all(&paths.autostart_dir)?;
                &paths.autostart_dir
            }
        };
        for stale_file_name in &legacy_file_names {
            if stale_file_name == &rendered.file_name {
                continue;
            }
            let stale_shortcut_path = target_dir.join(stale_file_name);
            if stale_shortcut_path.exists() {
                std::fs::remove_file(stale_shortcut_path)?;
            }
        }
        let shortcut_path = target_dir.join(&rendered.file_name);
        crate::platform::fs::write_file_atomic(&shortcut_path, rendered.content.as_bytes())?;
        crate::platform::fs::make_executable(&shortcut_path)?;
    }

    Ok(())
}

pub fn render_linux_shortcut_files(
    app_id: &str,
    name: &str,
    exe_path: &Path,
    icon_path: &Path,
    supervisor_id: &str,
    install_root: &Path,
    shortcuts: &[ShortcutLocation],
    environment: &BTreeMap<String, String>,
) -> Vec<LinuxShortcutFile> {
    let file_name = format!("{}.desktop", linux_desktop_id(app_id, name, exe_path));
    let mut desktop_entry: Option<String> = None;
    let mut startup_entry: Option<String> = None;
    let mut rendered = Vec::new();

    for location in shortcuts {
        let content = match location {
            ShortcutLocation::Desktop | ShortcutLocation::StartMenu => desktop_entry
                .get_or_insert_with(|| {
                    build_desktop_entry_linux(app_id, name, exe_path, icon_path, install_root, environment)
                })
                .clone(),
            ShortcutLocation::Startup => startup_entry
                .get_or_insert_with(|| {
                    build_startup_entry_linux(
                        app_id,
                        name,
                        exe_path,
                        icon_path,
                        supervisor_id,
                        install_root,
                        environment,
                    )
                })
                .clone(),
        };
        rendered.push(LinuxShortcutFile {
            location: *location,
            file_name: file_name.clone(),
            content,
        });
    }

    rendered
}

fn build_desktop_entry_linux(
    app_id: &str,
    name: &str,
    exe_path: &Path,
    icon_path: &Path,
    install_root: &Path,
    environment: &BTreeMap<String, String>,
) -> String {
    let display_name = escape_desktop_value(name);
    let startup_wm_class = escape_desktop_value(&linux_startup_wm_class(app_id, name, exe_path));
    let icon = escape_desktop_value(&icon_path.to_string_lossy());
    let working_dir = escape_desktop_value(&install_root.to_string_lossy());
    let exe = escape_desktop_value(&exe_path.to_string_lossy());
    let exec_line = build_exec_line_linux(&format!("\"{exe}\""), environment);

    format!(
        "[Desktop Entry]\nType=Application\nVersion=1.0\nName={display_name}\n{exec_line}\nIcon={icon}\nPath={working_dir}\nTerminal=false\nStartupNotify=true\nStartupWMClass={startup_wm_class}\n"
    )
}

fn build_startup_entry_linux(
    app_id: &str,
    name: &str,
    exe_path: &Path,
    icon_path: &Path,
    supervisor_id: &str,
    install_root: &Path,
    environment: &BTreeMap<String, String>,
) -> String {
    let display_name = escape_desktop_value(name);
    let startup_wm_class = escape_desktop_value(&linux_startup_wm_class(app_id, name, exe_path));
    let icon = escape_desktop_value(&icon_path.to_string_lossy());

    let exec_command = if supervisor_id.trim().is_empty() {
        let exe = escape_desktop_value(&exe_path.to_string_lossy());
        format!("\"{exe}\"")
    } else {
        let supervisor_path = install_root.join("app").join("surge-supervisor");
        let sup = escape_desktop_value(&supervisor_path.to_string_lossy());
        let exe = escape_desktop_value(&exe_path.to_string_lossy());
        let root = escape_desktop_value(&install_root.to_string_lossy());
        let sid = escape_desktop_value(supervisor_id);
        format!("\"{sup}\" run --id {sid} --dir \"{root}\" --exe \"{exe}\"")
    };
    let exec_line = build_exec_line_linux(&exec_command, environment);

    format!(
        "[Desktop Entry]\nType=Application\nVersion=1.0\nName={display_name}\n{exec_line}\nIcon={icon}\nTerminal=false\nStartupWMClass={startup_wm_class}\n"
    )
}

fn build_exec_line_linux(command: &str, environment: &BTreeMap<String, String>) -> String {
    if environment.is_empty() {
        format!("Exec={command}")
    } else {
        let env_prefix = environment
            .iter()
            .map(|(key, value)| format!("{}={}", escape_desktop_value(key), escape_desktop_value(value)))
            .collect::<Vec<_>>()
            .join(" ");
        format!("Exec=env {env_prefix} {command}")
    }
}

fn escape_desktop_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ")
}

fn linux_desktop_id(app_id: &str, name: &str, exe_path: &Path) -> String {
    let desktop_id_source = exe_path
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or(if app_id.trim().is_empty() {
            Some(name)
        } else {
            Some(app_id)
        })
        .map_or(name, str::trim);
    sanitize_file_stem(desktop_id_source)
}

fn linux_startup_wm_class(app_id: &str, name: &str, exe_path: &Path) -> String {
    let executable_stem = exe_path
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(file_name) = executable_stem {
        file_name.to_string()
    } else if !app_id.trim().is_empty() {
        app_id.trim().to_string()
    } else {
        sanitize_file_stem(name)
    }
}

#[cfg(target_os = "linux")]
fn linux_legacy_shortcut_file_names(app_id: &str, name: &str, exe_path: &Path) -> Vec<String> {
    let mut candidates = vec![
        format!("{}.desktop", sanitize_file_stem(name)),
        format!("{}.desktop", sanitize_file_stem(app_id)),
        format!("{}.desktop", linux_desktop_id(app_id, name, exe_path)),
    ];
    candidates.sort();
    candidates.dedup();
    candidates
}
