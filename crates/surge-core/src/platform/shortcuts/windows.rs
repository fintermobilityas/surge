use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::manifest::ShortcutLocation;
use crate::error::{Result, SurgeError};

use super::shared::sanitize_file_stem;

struct WindowsShortcutPaths {
    desktop_dir: PathBuf,
    start_menu_dir: PathBuf,
    startup_dir: PathBuf,
}

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

#[allow(clippy::too_many_arguments)]
pub(super) fn install_shortcuts(
    name: &str,
    exe_path: &Path,
    icon_path: Option<&Path>,
    supervisor_id: &str,
    install_root: &Path,
    shortcuts: &[ShortcutLocation],
    _environment: &BTreeMap<String, String>,
) -> Result<()> {
    let paths = WindowsShortcutPaths::for_current_user()?;
    let file_name = format!("{}.lnk", sanitize_file_stem(name));
    let working_dir = install_root;
    let effective_icon = icon_path.unwrap_or(exe_path);

    for location in shortcuts {
        let target_dir = match location {
            ShortcutLocation::Desktop => &paths.desktop_dir,
            ShortcutLocation::StartMenu => &paths.start_menu_dir,
            ShortcutLocation::Startup => &paths.startup_dir,
        };

        let (shortcut_exe, shortcut_args) =
            if matches!(location, ShortcutLocation::Startup) && !supervisor_id.trim().is_empty() {
                let supervisor_path = install_root.join("app").join("surge-supervisor.exe");
                let args = format!(
                    "run --id {} --dir \"{}\" --exe \"{}\"",
                    supervisor_id,
                    install_root.display(),
                    exe_path.display()
                );
                (supervisor_path, args)
            } else {
                (exe_path.to_path_buf(), String::new())
            };

        std::fs::create_dir_all(target_dir)?;
        let shortcut_path = target_dir.join(&file_name);
        create_windows_shortcut(
            &shortcut_path,
            &shortcut_exe,
            effective_icon,
            working_dir,
            &shortcut_args,
        )?;
    }

    Ok(())
}

fn create_windows_shortcut(
    shortcut_path: &Path,
    exe_path: &Path,
    icon_path: &Path,
    working_dir: &Path,
    args: &str,
) -> Result<()> {
    let shortcut = escape_powershell_single_quoted(&shortcut_path.to_string_lossy());
    let exe = escape_powershell_single_quoted(&exe_path.to_string_lossy());
    let icon = escape_powershell_single_quoted(&icon_path.to_string_lossy());
    let working = escape_powershell_single_quoted(&working_dir.to_string_lossy());
    let args_escaped = escape_powershell_single_quoted(args);

    let script = format!(
        "$ws = New-Object -ComObject WScript.Shell; $lnk = $ws.CreateShortcut('{shortcut}'); $lnk.TargetPath = '{exe}'; $lnk.Arguments = '{args_escaped}'; $lnk.WorkingDirectory = '{working}'; $lnk.IconLocation = '{icon}'; $lnk.Save()"
    );

    run_powershell_script(&script)
}

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

fn escape_powershell_single_quoted(value: &str) -> String {
    value.replace('\'', "''")
}
