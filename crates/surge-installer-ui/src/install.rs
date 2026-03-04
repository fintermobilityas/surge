#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::Duration;

use surge_core::config::installer::InstallerManifest;
use surge_core::config::manifest::ShortcutLocation;
use surge_core::error::SurgeError;
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::platform::paths::default_install_root;
use surge_core::storage;
use surge_core::storage_config::build_storage_config_from_installer_manifest;

pub enum ProgressUpdate {
    Status(String),
    Progress(f32),
    Complete(PathBuf),
    Error(String),
}

struct ResolvedPackage {
    path: PathBuf,
    _download_dir_guard: Option<tempfile::TempDir>,
}

pub fn run_install(
    manifest: &InstallerManifest,
    staging_dir: &Path,
    install_dir_override: Option<&str>,
    shortcuts: &[ShortcutLocation],
    progress_tx: &Sender<ProgressUpdate>,
    ctx: &egui::Context,
    simulator: bool,
) {
    let result = run_install_inner(
        manifest,
        staging_dir,
        install_dir_override,
        shortcuts,
        progress_tx,
        ctx,
        simulator,
    );
    if let Err(e) = result {
        let _ = progress_tx.send(ProgressUpdate::Error(e.to_string()));
        ctx.request_repaint();
    }
}

fn run_install_inner(
    manifest: &InstallerManifest,
    staging_dir: &Path,
    install_dir_override: Option<&str>,
    shortcuts: &[ShortcutLocation],
    progress_tx: &Sender<ProgressUpdate>,
    ctx: &egui::Context,
    simulator: bool,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Resolving package...".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(0.05));
    simulate_progress(progress_tx, ctx, simulator, 0.05, 0.18, 10, 1500);

    let install_root = resolve_install_root(
        &manifest.app_id,
        &manifest.runtime.install_directory,
        install_dir_override,
    )?;

    let package = resolve_package_with_progress(staging_dir, manifest, progress_tx, ctx)?;

    send(progress_tx, ctx, ProgressUpdate::Status("Installing...".to_string()));
    send(progress_tx, ctx, ProgressUpdate::Progress(0.5));
    simulate_progress(progress_tx, ctx, simulator, 0.5, 0.72, 18, 3000);

    let profile = InstallProfile {
        app_id: &manifest.app_id,
        display_name: &manifest.runtime.name,
        main_exe: &manifest.runtime.main_exe,
        install_directory: &manifest.runtime.install_directory,
        supervisor_id: &manifest.runtime.supervisor_id,
        icon: &manifest.runtime.icon,
        shortcuts,
        environment: &manifest.runtime.environment,
    };

    core_install::install_package_locally_at_root(&profile, &package.path, &install_root)?;

    send(progress_tx, ctx, ProgressUpdate::Progress(0.9));
    simulate_progress(progress_tx, ctx, simulator, 0.9, 0.97, 14, 2500);
    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Creating shortcuts...".to_string()),
    );

    send(progress_tx, ctx, ProgressUpdate::Progress(1.0));
    send(progress_tx, ctx, ProgressUpdate::Complete(install_root));
    Ok(())
}

fn resolve_package_core(
    staging_dir: &Path,
    manifest: &InstallerManifest,
) -> std::result::Result<ResolvedPackage, Box<dyn std::error::Error + Send + Sync>> {
    let full_filename = manifest.release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(Box::new(SurgeError::Config(
            "Installer manifest has no full_filename in release section".to_string(),
        )));
    }

    let payload_path = staging_dir.join("payload").join(full_filename);
    if payload_path.is_file() {
        return Ok(ResolvedPackage {
            path: payload_path,
            _download_dir_guard: None,
        });
    }

    let storage_config = build_storage_config_from_installer_manifest(manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    let download_dir = tempfile::tempdir()?;
    let destination = download_dir.path().join(full_filename);

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    rt.block_on(backend.download_to_file(full_filename, &destination, None))?;

    Ok(ResolvedPackage {
        path: destination,
        _download_dir_guard: Some(download_dir),
    })
}

fn resolve_package_with_progress(
    staging_dir: &Path,
    manifest: &InstallerManifest,
    progress_tx: &Sender<ProgressUpdate>,
    ctx: &egui::Context,
) -> std::result::Result<ResolvedPackage, Box<dyn std::error::Error + Send + Sync>> {
    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Downloading package...".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(0.1));

    let package = resolve_package_core(staging_dir, manifest)?;

    send(progress_tx, ctx, ProgressUpdate::Progress(0.4));

    Ok(package)
}

fn resolve_install_root(
    app_id: &str,
    install_directory: &str,
    dir_override: Option<&str>,
) -> std::result::Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(dir) = dir_override.filter(|s| !s.trim().is_empty()) {
        Ok(PathBuf::from(dir))
    } else {
        Ok(default_install_root(app_id, install_directory)?)
    }
}

fn send(tx: &Sender<ProgressUpdate>, ctx: &egui::Context, update: ProgressUpdate) {
    let _ = tx.send(update);
    ctx.request_repaint();
}

fn simulate_progress(
    tx: &Sender<ProgressUpdate>,
    ctx: &egui::Context,
    enabled: bool,
    from: f32,
    to: f32,
    steps: u32,
    total_ms: u64,
) {
    if !enabled || steps == 0 || to <= from {
        return;
    }
    let per_step_ms = (total_ms / u64::from(steps)).max(1);
    for i in 1..=steps {
        let fraction = (i as f32) / (steps as f32);
        let progress = from + ((to - from) * fraction);
        send(tx, ctx, ProgressUpdate::Progress(progress));
        std::thread::sleep(Duration::from_millis(per_step_ms));
    }
}

pub fn run_headless(
    manifest: &InstallerManifest,
    staging_dir: &Path,
    install_dir_override: Option<&str>,
    shortcuts: &[ShortcutLocation],
    simulator: bool,
) -> std::result::Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    eprintln!(
        "Installing {} v{} ({}/{})",
        manifest.runtime.name, manifest.version, manifest.app_id, manifest.rid
    );

    let install_root = resolve_install_root(
        &manifest.app_id,
        &manifest.runtime.install_directory,
        install_dir_override,
    )?;

    eprintln!("Resolving package...");
    let package = resolve_package_core(staging_dir, manifest)?;

    if simulator {
        eprintln!("Simulator mode: delaying install for visual inspection...");
        std::thread::sleep(Duration::from_millis(2500));
    }

    let profile = InstallProfile {
        app_id: &manifest.app_id,
        display_name: &manifest.runtime.name,
        main_exe: &manifest.runtime.main_exe,
        install_directory: &manifest.runtime.install_directory,
        supervisor_id: &manifest.runtime.supervisor_id,
        icon: &manifest.runtime.icon,
        shortcuts,
        environment: &manifest.runtime.environment,
    };

    eprintln!("Installing to '{}'...", install_root.display());
    core_install::install_package_locally_at_root(&profile, &package.path, &install_root)?;
    eprintln!("Installed '{}' to '{}'", manifest.app_id, install_root.display());

    Ok(install_root)
}
