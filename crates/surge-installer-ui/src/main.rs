#![forbid(unsafe_code)]

mod app;
mod install;

use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use surge_core::archive::extractor::extract_to;
use surge_core::config::installer::InstallerManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::installer_bundle::read_embedded_payload;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let headless = std::env::args().any(|arg| arg == "--headless");
    let simulator = std::env::args().any(|arg| arg == "--simulator");
    if simulator && !cfg!(debug_assertions) {
        return Err(SurgeError::Config(
            "--simulator is only available in dev/debug builds".to_string(),
        ));
    }

    let executable = std::env::current_exe()
        .map_err(|e| SurgeError::Pack(format!("Failed to locate installer executable path: {e}")))?;
    let payload = read_embedded_payload(&executable)?;

    let extracted = tempfile::tempdir().map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to create temporary extraction directory for '{}': {e}",
            executable.display()
        ))
    })?;
    extract_to(&payload, extracted.path(), None)?;

    let manifest_path = extracted.path().join("installer.yml");
    let manifest_bytes = std::fs::read(&manifest_path).map_err(|e| {
        SurgeError::Config(format!(
            "Failed to read installer.yml from extracted payload at '{}': {e}",
            extracted.path().display()
        ))
    })?;
    let manifest: InstallerManifest = serde_yaml::from_slice(&manifest_bytes)
        .map_err(|e| SurgeError::Config(format!("Failed to parse installer.yml: {e}")))?;

    if headless || !has_display() {
        return run_headless(&manifest, extracted.path(), simulator);
    }

    let window_icon = app::load_window_icon(extracted.path(), &manifest.runtime.icon);

    let gui_install_error = Arc::new(Mutex::new(None::<String>));
    match launch_gui(
        manifest.clone(),
        extracted.path().to_path_buf(),
        window_icon,
        simulator,
        Arc::clone(&gui_install_error),
    ) {
        Ok(()) => {
            let install_error = gui_install_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(message) = install_error {
                Err(SurgeError::Pack(format!("GUI install failed: {message}")))
            } else {
                Ok(())
            }
        }
        Err(gui_err) => {
            eprintln!("GUI failed to start: {gui_err}");
            eprintln!("Falling back to headless mode...");
            run_headless(&manifest, extracted.path(), simulator)
        }
    }
}

fn has_display() -> bool {
    if cfg!(target_os = "linux") {
        std::env::var("DISPLAY").is_ok() || std::env::var("WAYLAND_DISPLAY").is_ok()
    } else {
        true
    }
}

fn launch_gui(
    manifest: InstallerManifest,
    staging_dir: std::path::PathBuf,
    window_icon: Option<egui::IconData>,
    simulator: bool,
    install_error: Arc<Mutex<Option<String>>>,
) -> std::result::Result<(), String> {
    let title = format!("Install {}", manifest.runtime.name);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([500.0, 440.0])
            .with_resizable(false)
            .with_icon(window_icon.unwrap_or_default()),
        centered: true,
        ..Default::default()
    };

    eframe::run_native(
        &title,
        options,
        Box::new(move |cc| {
            app::configure_theme(&cc.egui_ctx);
            Ok(Box::new(app::InstallerApp::new(
                manifest,
                staging_dir,
                simulator,
                install_error,
            )))
        }),
    )
    .map_err(|e| e.to_string())
}

fn run_headless(manifest: &InstallerManifest, staging_dir: &std::path::Path, simulator: bool) -> Result<()> {
    let shortcuts = manifest.runtime.shortcuts.clone();
    let install_root = install::run_headless(manifest, staging_dir, None, &shortcuts, simulator)
        .map_err(|e| SurgeError::Pack(format!("Headless install failed: {e}")))?;

    let profile = surge_core::install::InstallProfile::from_installer_manifest(manifest, &manifest.runtime.shortcuts);
    let active_app_dir = install_root.join("app");
    match surge_core::install::auto_start_after_install(&profile, &install_root, &active_app_dir) {
        Ok(pid) => eprintln!("Started '{}' (pid {pid})", manifest.runtime.name),
        Err(e) => eprintln!("Auto-start failed: {e}"),
    }

    Ok(())
}
