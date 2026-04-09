use std::path::{Path, PathBuf};

use surge_core::error::{Result, SurgeError};

std::thread_local! {
    static SURGE_INSTALLER_LAUNCHER_OVERRIDE: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
    static SURGE_INSTALLER_UI_LAUNCHER_OVERRIDE: std::cell::RefCell<Option<PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn set_surge_installer_launcher_override_for_test(path: &Path) {
    SURGE_INSTALLER_LAUNCHER_OVERRIDE.with(|cell| {
        *cell.borrow_mut() = Some(path.to_path_buf());
    });
}

#[cfg(test)]
pub(crate) fn set_surge_installer_ui_launcher_override_for_test(path: &Path) {
    SURGE_INSTALLER_UI_LAUNCHER_OVERRIDE.with(|cell| {
        *cell.borrow_mut() = Some(path.to_path_buf());
    });
}

pub(crate) fn find_installer_launcher_for_rid(rid: &str, override_path: Option<&Path>) -> Result<PathBuf> {
    find_launcher_for_rid(
        rid,
        override_path,
        SURGE_INSTALLER_LAUNCHER_OVERRIDE.with(|cell| cell.borrow().clone()),
        "SURGE_INSTALLER_LAUNCHER",
        installer_launcher_name_for_rid,
        Some("installer launcher"),
        "Installer launcher",
        "surge-installer",
    )
}

pub(super) fn find_gui_installer_launcher_for_rid(rid: &str) -> Result<PathBuf> {
    find_launcher_for_rid(
        rid,
        None,
        SURGE_INSTALLER_UI_LAUNCHER_OVERRIDE.with(|cell| cell.borrow().clone()),
        "SURGE_INSTALLER_UI_LAUNCHER",
        gui_installer_launcher_name_for_rid,
        None,
        "GUI installer launcher",
        "surge-installer-ui",
    )
}

fn find_launcher_for_rid(
    rid: &str,
    override_path: Option<&Path>,
    thread_override: Option<PathBuf>,
    env_var: &str,
    launcher_name_for_rid: fn(&str) -> &'static str,
    override_label: Option<&str>,
    not_found_label: &str,
    build_binary: &str,
) -> Result<PathBuf> {
    ensure_host_compatible_rid(rid)?;
    if let Some(path) = override_path {
        if path.is_file() {
            return Ok(path.to_path_buf());
        }
        let label = override_label.unwrap_or("launcher");
        return Err(SurgeError::Pack(format!(
            "Provided {label} path '{}' does not exist",
            path.display()
        )));
    }

    if let Some(path) = thread_override
        && path.is_file()
    {
        return Ok(path);
    }

    if let Ok(path) = std::env::var(env_var) {
        let candidate = PathBuf::from(&path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        return Err(SurgeError::Pack(format!(
            "{env_var} points to '{}' which does not exist",
            candidate.display()
        )));
    }

    let launcher_name = launcher_name_for_rid(rid);
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let candidate = parent.join(launcher_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    if let Ok(found) = which::which(launcher_name) {
        return Ok(found);
    }

    Err(SurgeError::Pack(format!(
        "{not_found_label} '{launcher_name}' not found. Use the official Surge release bundle for this platform, place '{build_binary}' next to surge, add it to PATH, or set {env_var}."
    )))
}

pub(crate) fn find_surge_binary_for_rid(rid: &str) -> Result<PathBuf> {
    ensure_host_compatible_rid(rid)?;
    if let Ok(path) = std::env::var("SURGE_INSTALLER_BINARY") {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        return Err(SurgeError::Pack(format!(
            "SURGE_INSTALLER_BINARY points to '{}' which does not exist",
            candidate.display()
        )));
    }

    let current_exe = std::env::current_exe()
        .map_err(|e| SurgeError::Pack(format!("Failed to determine current executable path: {e}")))?;
    if !current_exe.is_file() {
        return Err(SurgeError::Pack(format!(
            "Current executable path does not exist: {}",
            current_exe.display()
        )));
    }

    let parent = current_exe.parent().ok_or_else(|| {
        SurgeError::Pack(format!(
            "Failed to resolve executable directory for {}",
            current_exe.display()
        ))
    })?;

    let candidate = parent.join(surge_binary_name_for_rid(rid));
    if candidate.is_file() {
        return Ok(candidate);
    }

    Ok(current_exe)
}

pub(crate) fn surge_binary_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge.exe"
    } else {
        "surge"
    }
}

fn installer_launcher_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge-installer.exe"
    } else {
        "surge-installer"
    }
}

fn gui_installer_launcher_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge-installer-ui.exe"
    } else {
        "surge-installer-ui"
    }
}

pub(crate) fn ensure_host_compatible_rid(rid: &str) -> Result<()> {
    let target = parse_rid(rid).ok_or_else(|| {
        SurgeError::Pack(format!(
            "Unsupported target RID '{rid}'. Supported values use linux|win|windows|osx|macos and x86|x64|arm64."
        ))
    })?;
    let host_rid = surge_core::platform::detect::current_rid();
    let host = parse_rid(&host_rid).ok_or_else(|| {
        SurgeError::Pack(format!(
            "Unsupported host RID '{host_rid}'. Host-only installer generation is unavailable."
        ))
    })?;
    if target != host {
        return Err(SurgeError::Pack(format!(
            "Installer generation is host-only. Requested target RID '{rid}', but current host RID is '{host_rid}'."
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RidOs {
    Linux,
    Windows,
    MacOs,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RidArch {
    X86,
    X64,
    Arm64,
}

fn parse_rid(rid: &str) -> Option<(RidOs, RidArch)> {
    let mut parts = rid.trim().split('-');
    let raw_os = parts.next()?;
    let raw_arch = parts.next()?;
    let os = match raw_os {
        "linux" => RidOs::Linux,
        "win" | "windows" => RidOs::Windows,
        "osx" | "macos" => RidOs::MacOs,
        _ => return None,
    };
    let arch = match raw_arch {
        "x86" => RidArch::X86,
        "x64" => RidArch::X64,
        "arm64" => RidArch::Arm64,
        _ => return None,
    };
    Some((os, arch))
}
