#![forbid(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::Duration;

use eframe::egui;

use surge_core::config::installer::InstallerManifest;
use surge_core::config::manifest::{InstallArtifactCacheRetention, ShortcutLocation};
use surge_core::install::{self as core_install, InstallProfile, InstallProgress, InstallProgressStage};
use surge_core::installer_package::{
    InstallerPackageStage, ResolveInstallerPackageOptions, ResolvedInstallerPackage, prune_install_artifact_cache,
    resolve_installer_package,
};
use surge_core::platform::paths::default_install_root;
use surge_core::releases::restore::RestoreProgress;

pub enum ProgressUpdate {
    Status(String),
    Progress(f32),
    Complete(PathBuf),
    Error(String),
}

type ResolvedPackage = ResolvedInstallerPackage;

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

    let package = resolve_package_with_progress(staging_dir, manifest, &install_root, progress_tx, ctx)?;

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
        package.path(),
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
    prune_artifact_cache_for_package(&install_root, &package, manifest);

    send(progress_tx, ctx, ProgressUpdate::Progress(1.0));
    send(progress_tx, ctx, ProgressUpdate::Complete(install_root));
    Ok(())
}

fn resolve_package_core(
    staging_dir: &Path,
    manifest: &InstallerManifest,
    install_root: &Path,
    download_progress: Option<&surge_core::storage::TransferProgress<'_>>,
    restore_progress: Option<&surge_core::releases::restore::RestoreProgressCallback<'_>>,
    stage: Option<&surge_core::installer_package::InstallerPackageStageCallback<'_>>,
) -> std::result::Result<ResolvedPackage, Box<dyn std::error::Error + Send + Sync>> {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    Ok(rt.block_on(resolve_installer_package(
        staging_dir,
        manifest,
        install_root,
        ResolveInstallerPackageOptions {
            download_progress,
            restore_progress,
            stage,
        },
    ))?)
}

fn resolve_package_with_progress(
    staging_dir: &Path,
    manifest: &InstallerManifest,
    install_root: &Path,
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
        return resolve_package_core(staging_dir, manifest, install_root, None, None, None);
    }

    send(
        progress_tx,
        ctx,
        ProgressUpdate::Status("Downloading package... 0%".to_string()),
    );
    send(progress_tx, ctx, ProgressUpdate::Progress(DOWNLOAD_START_PROGRESS));

    let download_progress = |done: u64, total: u64| {
        let phase_percent = percentage_u64(done, total);
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
    let restore_progress = |progress: RestoreProgress| {
        send(
            progress_tx,
            ctx,
            ProgressUpdate::Status(format_restore_status(progress)),
        );
        send(
            progress_tx,
            ctx,
            ProgressUpdate::Progress(scale_progress(
                DOWNLOAD_START_PROGRESS,
                DOWNLOAD_END_PROGRESS,
                restore_phase_percent(progress),
            )),
        );
    };
    let stage_progress = |stage: InstallerPackageStage| match stage {
        InstallerPackageStage::UsingBundledPayload => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status("Using bundled package...".to_string()),
            );
        }
        InstallerPackageStage::UsingCachedPackage => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status("Using cached package...".to_string()),
            );
        }
        InstallerPackageStage::PreparingPackage => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status("Preparing package... 0%".to_string()),
            );
        }
        InstallerPackageStage::DownloadingPackage => {
            send(
                progress_tx,
                ctx,
                ProgressUpdate::Status("Downloading package... 0%".to_string()),
            );
        }
    };
    let package = resolve_package_core(
        staging_dir,
        manifest,
        install_root,
        Some(&download_progress),
        Some(&restore_progress),
        Some(&stage_progress),
    )?;

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

fn restore_phase_percent(progress: RestoreProgress) -> i32 {
    if progress.bytes_total > 0 {
        return percentage(progress.bytes_done, progress.bytes_total);
    }
    if progress.items_total > 0 {
        return percentage(progress.items_done, progress.items_total);
    }
    0
}

fn format_restore_status(progress: RestoreProgress) -> String {
    let phase_percent = restore_phase_percent(progress);
    if progress.bytes_total > 0 {
        format!(
            "Preparing package... {}% ({}/{})",
            phase_percent,
            format_bytes(progress.bytes_done.max(0) as u64),
            format_bytes(progress.bytes_total.max(0) as u64),
        )
    } else if progress.items_total > 0 {
        format!(
            "Preparing package... {}% ({}/{})",
            phase_percent, progress.items_done, progress.items_total
        )
    } else {
        "Preparing package...".to_string()
    }
}

fn percentage(done: i64, total: i64) -> i32 {
    if total <= 0 {
        return 0;
    }
    let done = done.max(0);
    let total = total.max(done);
    ((done.saturating_mul(100)) / total).clamp(0, 100) as i32
}

fn percentage_u64(done: u64, total: u64) -> u32 {
    done.saturating_mul(100)
        .checked_div(total)
        .map_or(0, |percent| percent.clamp(0, 100) as u32)
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
    let package = resolve_package_core(staging_dir, manifest, &install_root, None, None, None)?;

    if simulator {
        eprintln!("Simulator mode: delaying install for visual inspection...");
        std::thread::sleep(Duration::from_millis(2500));
    }

    let profile = InstallProfile::from_installer_manifest(manifest, shortcuts);

    eprintln!("Installing to '{}'...", install_root.display());
    core_install::install_package_locally_at_root(&profile, package.path(), &install_root)?;
    let runtime_manifest = core_install::RuntimeManifestMetadata::new(
        &manifest.version,
        &manifest.channel,
        &manifest.storage.provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &manifest.storage.endpoint,
    );
    core_install::write_runtime_manifest(&install_root.join("app"), &profile, &runtime_manifest)?;
    prune_artifact_cache_for_package(&install_root, &package, manifest);
    eprintln!("Installed '{}' to '{}'", manifest.app_id, install_root.display());

    Ok(install_root)
}

fn prune_artifact_cache_for_package(install_root: &Path, package: &ResolvedPackage, manifest: &InstallerManifest) {
    let empty_retained_artifacts = std::collections::BTreeSet::new();
    let retained_artifacts = match package.retained_artifacts.as_ref() {
        Some(retained_artifacts) => retained_artifacts,
        None if manifest.effective_install_artifact_cache_policy().retention
            == InstallArtifactCacheRetention::LatestFull =>
        {
            &empty_retained_artifacts
        }
        None => return,
    };
    let _ = prune_install_artifact_cache(install_root, retained_artifacts, &manifest.release.full_filename);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::config::installer::{InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi};
    use surge_core::config::manifest::CacheManifestConfig;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, compress_release_index};

    fn make_manifest(install_root: &Path, store_root: &Path, full_filename: &str) -> InstallerManifest {
        InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: InstallerUi::Egui,
            installer_type: "online-gui".to_string(),
            app_id: "demo-app".to_string(),
            rid: current_rid(),
            version: "1.2.3".to_string(),
            channel: "stable".to_string(),
            generated_utc: "2026-03-24T00:00:00Z".to_string(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: "filesystem".to_string(),
                bucket: store_root.to_string_lossy().to_string(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release: InstallerRelease {
                full_filename: full_filename.to_string(),
                full_sha256: String::new(),
                delta_filename: String::new(),
                delta_algorithm: String::new(),
                delta_patch_format: String::new(),
                delta_compression: String::new(),
            },
            runtime: InstallerRuntime {
                name: "Demo App".to_string(),
                main_exe: "demoapp".to_string(),
                install_directory: install_root.to_string_lossy().to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: BTreeMap::new(),
            },
            cache: CacheManifestConfig::default(),
        }
    }

    fn write_archive(path: &Path, payload: &[u8]) {
        let mut packer = ArchivePacker::new(3).expect("archive packer");
        packer
            .add_buffer("demoapp", b"#!/bin/sh\necho demo\n", 0o755)
            .expect("demoapp entry");
        packer.add_buffer("payload.txt", payload, 0o644).expect("payload entry");
        packer.finalize_to_file(path).expect("archive file");
    }

    fn write_release_index_entries(store_root: &Path, app_id: &str, releases: Vec<ReleaseEntry>) {
        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases,
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, 3).expect("release index");
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).expect("write release index");
    }

    #[test]
    fn resolve_package_rebuilds_latest_full_from_retained_release_graph() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let installer_dir = temp_dir.path().join("installer");
        let install_root = temp_dir.path().join("installed-app");
        let store_root = temp_dir.path().join("store");
        let rid = current_rid();
        let base_full_filename = "demo-app-1.0.0-full.tar.zst";
        let target_full_filename = "demo-app-1.2.3-full.tar.zst";
        let delta_filename = "demo-app-1.2.3-delta.tar.zst";
        let base_archive_path = store_root.join(base_full_filename);
        let target_archive_path = temp_dir.path().join(target_full_filename);
        let delta_path = store_root.join(delta_filename);

        std::fs::create_dir_all(&installer_dir).expect("installer dir");
        std::fs::create_dir_all(&store_root).expect("store dir");
        write_archive(&base_archive_path, b"base payload");
        write_archive(&target_archive_path, b"target payload");

        let base_archive = std::fs::read(&base_archive_path).expect("base archive");
        let target_archive = std::fs::read(&target_archive_path).expect("target archive");
        let patch = bsdiff_buffers(&base_archive, &target_archive).expect("delta patch");
        let delta_bytes = zstd::encode_all(patch.as_slice(), 3).expect("delta bytes");
        std::fs::write(&delta_path, &delta_bytes).expect("write delta");

        let manifest = make_manifest(&install_root, &store_root, target_full_filename);

        let mut base_release = ReleaseEntry {
            version: "1.0.0".to_string(),
            channels: vec![manifest.channel.clone()],
            os: "linux".to_string(),
            rid: rid.clone(),
            is_genesis: false,
            full_filename: base_full_filename.to_string(),
            full_size: i64::try_from(base_archive.len()).expect("base size"),
            full_sha256: sha256_hex(&base_archive),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: manifest.generated_utc.clone(),
            release_notes: String::new(),
            name: manifest.runtime.name.clone(),
            main_exe: manifest.runtime.main_exe.clone(),
            install_directory: manifest.runtime.install_directory.clone(),
            supervisor_id: manifest.runtime.supervisor_id.clone(),
            icon: manifest.runtime.icon.clone(),
            shortcuts: manifest.runtime.shortcuts.clone(),
            persistent_assets: manifest.runtime.persistent_assets.clone(),
            installers: manifest.runtime.installers.clone(),
            environment: manifest.runtime.environment.clone(),
        };
        base_release.set_primary_delta(None);

        let mut target_release = ReleaseEntry {
            version: manifest.version.clone(),
            channels: vec![manifest.channel.clone()],
            os: "linux".to_string(),
            rid,
            is_genesis: false,
            full_filename: target_full_filename.to_string(),
            full_size: i64::try_from(target_archive.len()).expect("target size"),
            full_sha256: sha256_hex(&target_archive),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: manifest.generated_utc.clone(),
            release_notes: String::new(),
            name: manifest.runtime.name.clone(),
            main_exe: manifest.runtime.main_exe.clone(),
            install_directory: manifest.runtime.install_directory.clone(),
            supervisor_id: manifest.runtime.supervisor_id.clone(),
            icon: manifest.runtime.icon.clone(),
            shortcuts: manifest.runtime.shortcuts.clone(),
            persistent_assets: manifest.runtime.persistent_assets.clone(),
            installers: manifest.runtime.installers.clone(),
            environment: manifest.runtime.environment.clone(),
        };
        target_release.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            delta_filename,
            i64::try_from(delta_bytes.len()).expect("delta size"),
            &sha256_hex(&delta_bytes),
        )));

        write_release_index_entries(&store_root, &manifest.app_id, vec![base_release, target_release]);

        let package =
            resolve_package_core(&installer_dir, &manifest, &install_root, None, None, None).expect("resolved package");

        assert!(package.path().is_file());
        assert_eq!(
            std::fs::read(package.path()).expect("resolved bytes"),
            std::fs::read(&target_archive_path).expect("target bytes")
        );
        assert_eq!(
            package.path(),
            install_root
                .join(".surge-cache")
                .join("artifacts")
                .join(target_full_filename)
                .as_path()
        );
    }
}
