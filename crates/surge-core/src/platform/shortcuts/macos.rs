use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::config::manifest::ShortcutLocation;
use crate::error::{Result, SurgeError};

use super::shared::sanitize_file_stem;

struct MacShortcutPaths {
    desktop_dir: PathBuf,
    applications_dir: PathBuf,
    launch_agents_dir: PathBuf,
}

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
    let paths = MacShortcutPaths::for_current_user()?;
    let app_name = sanitize_file_stem(name);

    for location in shortcuts {
        match location {
            ShortcutLocation::Desktop => {
                std::fs::create_dir_all(&paths.desktop_dir)?;
                let app_bundle_dir = paths.desktop_dir.join(format!("{app_name}.app"));
                create_macos_app_bundle(&app_bundle_dir, name, exe_path, icon_path)?;
            }
            ShortcutLocation::StartMenu => {
                std::fs::create_dir_all(&paths.applications_dir)?;
                let app_bundle_dir = paths.applications_dir.join(format!("{app_name}.app"));
                create_macos_app_bundle(&app_bundle_dir, name, exe_path, icon_path)?;
            }
            ShortcutLocation::Startup => {
                std::fs::create_dir_all(&paths.launch_agents_dir)?;
                let plist_path = paths
                    .launch_agents_dir
                    .join(format!("com.surge.{}.plist", sanitize_file_stem(name).to_lowercase()));
                if supervisor_id.trim().is_empty() {
                    create_launch_agent_plist(&plist_path, name, exe_path)?;
                } else {
                    let supervisor_path = install_root.join("app").join("surge-supervisor");
                    create_launch_agent_plist_supervisor(
                        &plist_path,
                        name,
                        &supervisor_path,
                        supervisor_id,
                        install_root,
                        exe_path,
                    )?;
                }
            }
        }
    }

    Ok(())
}

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

fn create_launch_agent_plist(path: &Path, name: &str, exe_path: &Path) -> Result<()> {
    let label = format!("com.surge.{}", sanitize_file_stem(name).to_lowercase());
    let exe = escape_xml(&exe_path.to_string_lossy());
    let label_xml = escape_xml(&label);

    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{label_xml}</string>\n  <key>ProgramArguments</key>\n  <array>\n    <string>{exe}</string>\n  </array>\n  <key>RunAtLoad</key>\n  <true/>\n</dict>\n</plist>\n"
    );

    crate::platform::fs::write_file_atomic(path, plist.as_bytes())
}

fn create_launch_agent_plist_supervisor(
    path: &Path,
    name: &str,
    supervisor_path: &Path,
    supervisor_id: &str,
    install_root: &Path,
    exe_path: &Path,
) -> Result<()> {
    let label = format!("com.surge.{}", sanitize_file_stem(name).to_lowercase());
    let label_xml = escape_xml(&label);
    let sup = escape_xml(&supervisor_path.to_string_lossy());
    let root = escape_xml(&install_root.to_string_lossy());
    let exe = escape_xml(&exe_path.to_string_lossy());
    let sid = escape_xml(supervisor_id);

    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{label_xml}</string>\n  <key>ProgramArguments</key>\n  <array>\n    <string>{sup}</string>\n    <string>run</string>\n    <string>--id</string>\n    <string>{sid}</string>\n    <string>--dir</string>\n    <string>{root}</string>\n    <string>--exe</string>\n    <string>{exe}</string>\n  </array>\n  <key>RunAtLoad</key>\n  <true/>\n</dict>\n</plist>\n"
    );

    crate::platform::fs::write_file_atomic(path, plist.as_bytes())
}

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

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn escape_shell_double_quoted(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "\\$")
        .replace('`', "\\`")
}
