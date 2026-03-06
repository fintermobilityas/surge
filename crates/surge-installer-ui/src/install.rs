#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::Duration;

use surge_core::config::installer::InstallerManifest;
use surge_core::config::manifest::ShortcutLocation;
use surge_core::error::SurgeError;
use surge_core::install::{self as core_install, InstallProfile, InstallProgress, InstallProgressStage};
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

const RESOLVE_PROGRESS: f32 = 0.05;
const DOWNLOAD_START_PROGRESS: f32 = 0.10;
const DOWNLOAD_END_PROGRESS: f32 = 0.40;
const EXTRACT_START_PROGRESS: f32 = 0.45;
const EXTRACT_END_PROGRESS: f32 = 0.92;
const ACTIVATE_START_PROGRESS: f32 = 0.92;
const ACTIVATE_END_PROGRESS: f32 = 0.97;
const SHORTCUTS_START_PROGRESS: f32 = 0.97;
const SHORTCUTS_END_PROGRESS: f32 = 0.99;
const METADATA_PROGRESS: f32 = 0.995;

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
    _simulator: bool,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Resolving package...".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(RESOLVE_PROGRESS));

    let install_root = resolve_install_root(
        &manifest.app_id,
        &manifest.runtime.install_directory,
        install_dir_override,
    )?;

    let package = resolve_package_with_progress(staging_dir, manifest, progress_tx, ctx)?;

    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Preparing installation...".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(EXTRACT_START_PROGRESS));

    let profile = InstallProfile::from_installer_manifest(manifest, shortcuts);

    let install_progress = |progress: InstallProgress| match progress.stage {
        InstallProgressStage::Extract => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status(format_extract_status(progress)),
            );
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Progress(scale_progress(
                    EXTRACT_START_PROGRESS,
                    EXTRACT_END_PROGRESS,
                    progress.phase_percent,
                )),
            );
        }
        InstallProgressStage::Activate => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status("Activating installation...".to_string()),
            );
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Progress(scale_progress(
                    ACTIVATE_START_PROGRESS,
                    ACTIVATE_END_PROGRESS,
                    progress.phase_percent,
                )),
            );
        }
        InstallProgressStage::Shortcuts => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status("Creating shortcuts...".to_string()),
            );
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Progress(scale_progress(
                    SHORTCUTS_START_PROGRESS,
                    SHORTCUTS_END_PROGRESS,
                    progress.phase_percent,
                )),
            );
        }
    };

    core_install::install_package_locally_at_root_with_progress(
        &profile,
        &package.path,
        &install_root,
        Some(&install_progress),
    )?;
    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Writing runtime metadata...".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(METADATA_PROGRESS));
    let runtime_manifest = core_install::RuntimeManifestMetadata::new(
        &manifest.version,
        &manifest.channel,
        &manifest.storage.provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &manifest.storage.endpoint,
    );
    core_install::write_runtime_manifest(&install_root.join("app"), &profile, &runtime_manifest)?;

    send(progress_tx, ctx, ProgressUpdate::Progress(1.0));
    send(progress_tx, ctx, ProgressUpdate::Complete(install_root));
    Ok(())
}

fn resolve_package_core(
    staging_dir: &Path,
    manifest: &InstallerManifest,
    progress: Option<&surge_core::storage::TransferProgress<'_>>,
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
    rt.block_on(backend.download_to_file(full_filename, &destination, progress))?;

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
    let full_filename = manifest.release.full_filename.trim();
    let payload_path = staging_dir.join("payload").join(full_filename);
    if payload_path.is_file() {
        send(
            progress_tx,
            ctx,
            ProgressUpdate::Status("Using bundled package...".to_string()),
        );
        send(progress_tx, ctx, ProgressUpdate::Progress(DOWNLOAD_END_PROGRESS));
        return resolve_package_core(staging_dir, manifest, None);
    }

    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Downloading package... 0%".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(DOWNLOAD_START_PROGRESS));

    let download_progress = |done: u64, total: u64| {
        let phase_percent = if total > 0 {
            ((done.saturating_mul(100)) / total).clamp(0, 100) as u32
        } else {
            0
        };
        send(
            progress_tx,
            ctx,
            ProgressUpdate::Status(format!(
                "Downloading package... {}% ({}/{})",
                phase_percent,
                format_bytes(done),
                format_bytes(total),
            )),
        );
        send(
            progress_tx,
            ctx,
            ProgressUpdate::Progress(scale_progress(
                DOWNLOAD_START_PROGRESS,
                DOWNLOAD_END_PROGRESS,
                i32::try_from(phase_percent).unwrap_or(100),
            )),
        );
    };
    let package = resolve_package_core(staging_dir, manifest, Some(&download_progress))?;

    send(progress_tx, ctx, ProgressUpdate::Progress(DOWNLOAD_END_PROGRESS));

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

fn scale_progress(start: f32, end: f32, phase_percent: i32) -> f32 {
    let fraction = (phase_percent.clamp(0, 100) as f32) / 100.0;
    start + ((end - start) * fraction)
}

fn format_extract_status(progress: InstallProgress) -> String {
    if progress.bytes_total > 0 {
        format!(
            "Extracting files... {}% ({}/{})",
            progress.phase_percent,
            format_bytes(progress.bytes_done.max(0) as u64),
            format_bytes(progress.bytes_total.max(0) as u64),
        )
    } else if progress.items_total > 0 {
        format!(
            "Extracting files... {}% ({}/{})",
            progress.phase_percent, progress.items_done, progress.items_total
        )
    } else {
        format!("Extracting files... {}%", progress.phase_percent)
    }
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit_index = 0usize;
    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }

    if value >= 10.0 || unit_index == 0 {
        format!("{value:.0} {}", UNITS[unit_index])
    } else {
        format!("{value:.1} {}", UNITS[unit_index])
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
    let package = resolve_package_core(staging_dir, manifest, None)?;

    if simulator {
        eprintln!("Simulator mode: delaying install for visual inspection...");
        std::thread::sleep(Duration::from_millis(2500));
    }

    let profile = InstallProfile::from_installer_manifest(manifest, shortcuts);

    eprintln!("Installing to '{}'...", install_root.display());
    core_install::install_package_locally_at_root(&profile, &package.path, &install_root)?;
    let runtime_manifest = core_install::RuntimeManifestMetadata::new(
        &manifest.version,
        &manifest.channel,
        &manifest.storage.provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &manifest.storage.endpoint,
    );
    core_install::write_runtime_manifest(&install_root.join("app"), &profile, &runtime_manifest)?;
    eprintln!("Installed '{}' to '{}'", manifest.app_id, install_root.display());

    Ok(install_root)
}
