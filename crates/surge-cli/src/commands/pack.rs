#![allow(clippy::too_many_lines)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use crate::formatters::{format_byte_progress, format_bytes, format_duration};
use crate::logline;
use crate::ui::UiTheme;
use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::{PACK_DEFAULT_MAX_MEMORY_BYTES, RELEASES_FILE_COMPRESSED};
use surge_core::config::installer::{
    InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
};
use surge_core::config::manifest::{AppConfig, InstallerType, SurgeManifest, TargetConfig};
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::installer_bundle;
use surge_core::pack::builder::PackBuilder;
use surge_core::releases::artifact_cache::{CacheFetchOutcome, fetch_or_reuse_file};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::restore::{
    RestoreArtifactSpec, RestoreOptions, plan_full_archive_restore, restore_full_archive_for_version_with_options,
};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};
use surge_core::storage_config::build_storage_config;

#[derive(Debug, Clone)]
struct ResolvedInstallerPackage {
    app_id: String,
    rid: String,
    default_channel: String,
    selected_version: String,
    full_key: String,
    full_sha256: String,
    local_full_name: String,
    artifacts_dir: PathBuf,
}

/// Build release packages (full + delta) for a given app version and RID.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    artifacts_dir: Option<&Path>,
    output_dir: &Path,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and target");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let (app, target) = manifest
        .find_app_with_target(&app_id, &rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {rid} found for app {app_id}")))?;
    print_stage_done(theme, 1, TOTAL_STAGES, &format!("Target: {app_id}/{rid} v{version}"));

    print_stage(theme, 2, TOTAL_STAGES, "Validating artifacts and output directories");
    let artifacts_dir = artifacts_dir.map_or_else(
        || default_artifacts_dir(manifest_path, &app_id, &rid, version),
        PathBuf::from,
    );
    if !artifacts_dir.is_dir() {
        return Err(SurgeError::Pack(format!(
            "Artifacts directory does not exist: {}. Use --artifacts-dir to override.",
            artifacts_dir.display(),
        )));
    }

    std::fs::create_dir_all(output_dir)?;
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!(
            "Artifacts: {} | Output: {}",
            artifacts_dir.display(),
            output_dir.display()
        ),
    );

    print_stage(theme, 3, TOTAL_STAGES, "Building full/delta packages");
    let ctx = Arc::new(configure_context(manifest_path, &manifest, &app_id)?);
    let manifest_path_s = manifest_path
        .to_str()
        .ok_or_else(|| SurgeError::Config(format!("Manifest path is not valid UTF-8: {}", manifest_path.display())))?;
    let artifacts_dir_s = artifacts_dir.as_path().to_str().ok_or_else(|| {
        SurgeError::Config(format!(
            "Artifacts directory is not valid UTF-8: {}",
            artifacts_dir.display()
        ))
    })?;

    let mut builder = PackBuilder::new(ctx, manifest_path_s, &app_id, &rid, version, artifacts_dir_s)?;
    let build_started = Instant::now();
    let build_running = Arc::new(AtomicBool::new(true));
    let build_step = Arc::new(AtomicI32::new(0));
    let build_total = Arc::new(AtomicI32::new(2));
    let build_last_announced = Arc::new(AtomicI32::new(-1));

    let build_running_for_heartbeat = Arc::clone(&build_running);
    let build_step_for_heartbeat = Arc::clone(&build_step);
    let build_total_for_heartbeat = Arc::clone(&build_total);
    let heartbeat = thread::spawn(move || {
        while build_running_for_heartbeat.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(2));
            if !build_running_for_heartbeat.load(Ordering::Relaxed) {
                break;
            }
            let step_count = build_total_for_heartbeat.load(Ordering::Relaxed).max(1);
            let step_done = build_step_for_heartbeat.load(Ordering::Relaxed).clamp(0, step_count);
            logline::subtle(&format!(
                "  {} (elapsed {})",
                pack_build_phase_message(step_done, step_count),
                format_duration(build_started.elapsed())
            ));
        }
    });

    let build_step_for_progress = Arc::clone(&build_step);
    let build_total_for_progress = Arc::clone(&build_total);
    let build_last_announced_for_progress = Arc::clone(&build_last_announced);
    let pack_progress = move |done: i32, total: i32| {
        let step_count = total.max(1);
        let step_done = done.clamp(0, step_count);
        build_total_for_progress.store(step_count, Ordering::Relaxed);
        build_step_for_progress.store(step_done, Ordering::Relaxed);
        let previous = build_last_announced_for_progress.swap(step_done, Ordering::Relaxed);
        if previous != step_done {
            logline::subtle(&format!("  {}", pack_build_phase_message(step_done, step_count)));
        }
    };
    builder.build(Some(&pack_progress)).await?;
    build_running.store(false, Ordering::Relaxed);
    let _ = heartbeat.join();

    let artifact_paths = builder.write_artifacts_to(output_dir)?;
    let mut artifact_count = 0usize;
    for dest in &artifact_paths {
        artifact_count += 1;
        logline::subtle(&format!("  Created {} ({})", dest.display(), file_size_label(dest)));
    }
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        &format!("Built {artifact_count} package artifact(s)"),
    );

    let full_filename = format!("{app_id}-{version}-{rid}-full.tar.zst");
    let full_package_path = output_dir.join(&full_filename);
    if !full_package_path.is_file() {
        return Err(SurgeError::Pack(format!(
            "Expected full package was not created: {}",
            full_package_path.display()
        )));
    }

    print_stage(theme, 4, TOTAL_STAGES, "Building installer bundles");
    let installer_paths = build_installers(
        &manifest,
        app,
        &target,
        &app_id,
        &rid,
        version,
        manifest_path.parent().unwrap_or_else(|| Path::new(".")),
        artifacts_dir.as_path(),
        output_dir,
        &full_package_path,
    )?;
    let installer_count = installer_paths.len();
    for installer in &installer_paths {
        logline::subtle(&format!(
            "  Created {} ({})",
            installer.display(),
            file_size_label(installer)
        ));
    }
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!("Built {installer_count} installer artifact(s)"),
    );

    print_stage(theme, 5, TOTAL_STAGES, "Finalize pack summary");
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Completed in {} (packages: {artifact_count}, installers: {installer_count})",
            format_duration(started.elapsed())
        ),
    );
    Ok(())
}

/// Build installer bundles from an existing full package (no full/delta rebuild).
pub async fn execute_installers_only(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: Option<&str>,
    rid: Option<&str>,
    artifacts_dir: Option<&Path>,
    output_dir: &Path,
    package_file: Option<&Path>,
    upload_installers: bool,
) -> Result<()> {
    let theme = UiTheme::global();
    let started = Instant::now();
    let total_stages = if package_file.is_some() {
        4
    } else if upload_installers {
        6
    } else {
        5
    };

    print_stage(theme, 1, total_stages, "Resolving manifest and target");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    if upload_installers {
        let storage_config = build_storage_config(&manifest)?;
        super::ensure_mutating_storage_access(&storage_config, "upload installers")?;
    }
    let (backend, index, resolved) =
        resolve_installer_package(&manifest, manifest_path, app_id, version, rid, artifacts_dir).await?;
    print_stage_done(
        theme,
        1,
        total_stages,
        &format!(
            "Target: {}/{} (channel: {})",
            resolved.app_id, resolved.rid, resolved.default_channel
        ),
    );

    print_stage(theme, 2, total_stages, "Resolving release for installer build");
    print_stage_done(
        theme,
        2,
        total_stages,
        &format!("Selected release version {}", resolved.selected_version),
    );

    if let Some(package_file) = package_file {
        print_stage(theme, 3, total_stages, "Writing package manifest");
        let specs = plan_full_archive_restore(&*backend, &index, &resolved.rid, &resolved.selected_version).await?;
        write_package_manifest(package_file, &specs)?;
        print_stage_done(
            theme,
            3,
            total_stages,
            &format!("Wrote {} for {} artifact(s)", package_file.display(), specs.len()),
        );
        print_stage(theme, 4, total_stages, "Finalize restore-package summary");
        print_stage_done(
            theme,
            4,
            total_stages,
            &format!("Completed in {}", format_duration(started.elapsed())),
        );
        return Ok(());
    }

    if !resolved.artifacts_dir.is_dir() {
        logline::warn(&format!(
            "Artifacts directory not found: {}; installers will be built without icon assets",
            resolved.artifacts_dir.display()
        ));
    }

    std::fs::create_dir_all(output_dir)?;
    let (app, target) = manifest
        .find_app_with_target(&resolved.app_id, &resolved.rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {} found for app {}", resolved.rid, resolved.app_id)))?;

    let full_package_path = output_dir.join(&resolved.local_full_name);
    print_stage(theme, 3, total_stages, "Ensuring full package is available");
    match fetch_or_reuse_file(
        &*backend,
        &resolved.full_key,
        &full_package_path,
        &resolved.full_sha256,
        None,
    )
    .await
    {
        Ok(CacheFetchOutcome::ReusedLocal) => {
            print_stage_done(
                theme,
                3,
                total_stages,
                &format!(
                    "Using local package {} ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Ok(CacheFetchOutcome::DownloadedFresh) => {
            print_stage_done(
                theme,
                3,
                total_stages,
                &format!(
                    "Downloaded {} ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Ok(CacheFetchOutcome::DownloadedAfterInvalidLocal) => {
            logline::warn(&format!(
                "Local package '{}' failed checksum verification; redownloaded.",
                full_package_path.display()
            ));
            print_stage_done(
                theme,
                3,
                total_stages,
                &format!(
                    "Downloaded {} ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Err(SurgeError::NotFound(_)) => {
            let rebuilt = restore_full_archive_for_version_with_options(
                &*backend,
                &index,
                &resolved.rid,
                &resolved.selected_version,
                RestoreOptions {
                    cache_dir: Some(output_dir),
                    progress: None,
                },
            )
            .await?;
            std::fs::write(&full_package_path, rebuilt)?;
            print_stage_done(
                theme,
                3,
                total_stages,
                &format!(
                    "Rebuilt {} from release graph ({})",
                    full_package_path.display(),
                    file_size_label(&full_package_path)
                ),
            );
        }
        Err(e) => return Err(e),
    }

    print_stage(
        theme,
        4,
        total_stages,
        &format!(
            "Building installers for {} v{} ({})",
            resolved.app_id, resolved.selected_version, resolved.rid
        ),
    );

    let installer_paths = build_installers(
        &manifest,
        app,
        &target,
        &resolved.app_id,
        &resolved.rid,
        &resolved.selected_version,
        manifest_path.parent().unwrap_or_else(|| Path::new(".")),
        &resolved.artifacts_dir,
        output_dir,
        &full_package_path,
    )?;
    if installer_paths.is_empty() {
        print_stage_done(
            theme,
            4,
            total_stages,
            &format!(
                "No installers configured for {}/{}. Configure `installers: [online]` or `installers: [offline]` in the manifest.",
                resolved.app_id, resolved.rid
            ),
        );
        return Ok(());
    }
    for installer in &installer_paths {
        logline::subtle(&format!(
            "  Created {} ({})",
            installer.display(),
            file_size_label(installer)
        ));
    }

    print_stage_done(theme, 4, total_stages, "Installer bundles created");
    let finalize_stage = if upload_installers {
        print_stage(theme, 5, total_stages, "Uploading installers to storage");
        let upload_backend = build_installer_upload_backend(&manifest)?;
        upload_installers_to_storage(&*upload_backend, &installer_paths).await?;
        print_stage_done(theme, 5, total_stages, "Installer bundles uploaded");
        6
    } else {
        5
    };
    print_stage(
        theme,
        finalize_stage,
        total_stages,
        "Finalize restore-installers summary",
    );
    print_stage_done(
        theme,
        finalize_stage,
        total_stages,
        &format!("Completed in {}", format_duration(started.elapsed())),
    );

    Ok(())
}

async fn resolve_installer_package(
    manifest: &SurgeManifest,
    manifest_path: &Path,
    app_id: Option<&str>,
    version: Option<&str>,
    rid: Option<&str>,
    artifacts_dir: Option<&Path>,
) -> Result<(Box<dyn StorageBackend>, ReleaseIndex, ResolvedInstallerPackage)> {
    let app_id = super::resolve_app_id_with_rid_hint(manifest, app_id, rid)?;
    let rid = super::resolve_rid(manifest, &app_id, rid)?;
    let (app, _) = manifest
        .find_app_with_target(&app_id, &rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {rid} found for app {app_id}")))?;
    let default_channel = default_channel_for_app(manifest, app);
    let storage_config = super::build_app_scoped_storage_config(manifest, manifest_path, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    let index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    let selected_release =
        select_release_for_installers(&index.releases, &default_channel, version, &rid).ok_or_else(|| {
            SurgeError::NotFound(format!(
                "No release found for app '{}' rid '{}' on channel '{}'{}",
                app_id,
                rid,
                default_channel,
                version.map_or_else(String::new, |v| format!(" and version '{v}'"))
            ))
        })?;
    let full_key = selected_release.full_filename.trim();
    if full_key.is_empty() {
        return Err(SurgeError::Pack(format!(
            "Selected release {} for {}/{} does not define a full package filename",
            selected_release.version, app_id, rid
        )));
    }
    let local_full_name = Path::new(full_key)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| SurgeError::Pack(format!("Invalid full package key: {full_key}")))?
        .to_string();
    let artifacts_dir = artifacts_dir.map_or_else(
        || default_artifacts_dir(manifest_path, &app_id, &rid, &selected_release.version),
        PathBuf::from,
    );

    Ok((
        backend,
        index,
        ResolvedInstallerPackage {
            app_id,
            rid,
            default_channel,
            selected_version: selected_release.version.clone(),
            full_key: full_key.to_string(),
            full_sha256: selected_release.full_sha256.clone(),
            local_full_name,
            artifacts_dir,
        },
    ))
}

fn write_package_manifest(path: &Path, specs: &[RestoreArtifactSpec]) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut manifest = String::new();
    for spec in specs {
        manifest.push_str(spec.sha256.trim());
        manifest.push(' ');
        manifest.push_str(spec.key.trim());
        manifest.push('\n');
    }
    std::fs::write(path, manifest)?;
    Ok(())
}

fn build_installer_upload_backend(manifest: &SurgeManifest) -> Result<Box<dyn StorageBackend>> {
    let storage_config = build_storage_config(manifest)?;
    super::ensure_mutating_storage_access(&storage_config, "upload installers")?;
    storage::create_storage_backend(&storage_config)
}

async fn upload_installers_to_storage(backend: &dyn StorageBackend, installer_paths: &[PathBuf]) -> Result<()> {
    for installer_path in installer_paths {
        let filename = installer_path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| {
                SurgeError::Pack(format!(
                    "Invalid installer path (missing filename): {}",
                    installer_path.display()
                ))
            })?;
        let key = format!("installers/{filename}");
        upload_installer_with_feedback(backend, &key, installer_path).await?;
    }

    Ok(())
}

async fn upload_installer_with_feedback(backend: &dyn StorageBackend, key: &str, source_path: &Path) -> Result<u64> {
    let total_bytes = std::fs::metadata(source_path)?.len();
    logline::subtle(&format!("  Uploading installer {key} ({})", format_bytes(total_bytes)));

    let started = Instant::now();
    let upload_running = Arc::new(AtomicBool::new(true));
    let bytes_done = Arc::new(AtomicU64::new(0));

    let upload_running_for_heartbeat = Arc::clone(&upload_running);
    let bytes_done_for_heartbeat = Arc::clone(&bytes_done);
    let key_for_heartbeat = key.to_string();
    let heartbeat = thread::spawn(move || {
        while upload_running_for_heartbeat.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(5));
            if !upload_running_for_heartbeat.load(Ordering::Relaxed) {
                break;
            }

            let uploaded = bytes_done_for_heartbeat.load(Ordering::Relaxed).min(total_bytes);
            let progress = if uploaded == 0 {
                format!(
                    "uploaded 0 B / {} (elapsed {})",
                    format_bytes(total_bytes),
                    format_duration(started.elapsed())
                )
            } else {
                format!(
                    "{} (elapsed {})",
                    format_byte_progress(uploaded, total_bytes, "uploaded"),
                    format_duration(started.elapsed())
                )
            };
            logline::subtle(&format!("      {key_for_heartbeat}: {progress}"));
        }
    });

    let bytes_done_for_progress = Arc::clone(&bytes_done);
    let progress = move |done: u64, _total: u64| {
        bytes_done_for_progress.store(done.min(total_bytes), Ordering::Relaxed);
    };

    let upload_result = backend.upload_from_file(key, source_path, Some(&progress)).await;
    bytes_done.store(total_bytes, Ordering::Relaxed);
    upload_running.store(false, Ordering::Relaxed);
    let _ = heartbeat.join();
    upload_result?;

    logline::subtle(&format!(
        "      {key}: {} in {}",
        format_byte_progress(total_bytes, total_bytes, "uploaded"),
        format_duration(started.elapsed())
    ));

    Ok(total_bytes)
}

#[allow(clippy::too_many_arguments)]
fn build_installers(
    manifest: &SurgeManifest,
    app: &AppConfig,
    target: &TargetConfig,
    app_id: &str,
    rid: &str,
    version: &str,
    manifest_root: &Path,
    artifacts_dir: &Path,
    output_dir: &Path,
    full_package_path: &Path,
) -> Result<Vec<PathBuf>> {
    build_installers_with_launcher(
        manifest,
        app,
        target,
        app_id,
        rid,
        version,
        manifest_root,
        artifacts_dir,
        output_dir,
        full_package_path,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_installers_with_launcher(
    manifest: &SurgeManifest,
    app: &AppConfig,
    target: &TargetConfig,
    app_id: &str,
    rid: &str,
    version: &str,
    manifest_root: &Path,
    artifacts_dir: &Path,
    output_dir: &Path,
    full_package_path: &Path,
    launcher_override: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    let installer_types = parse_installer_types(&target.installers, app_id, rid)?;
    if installer_types.is_empty() {
        return Ok(Vec::new());
    }
    ensure_host_compatible_rid(rid)?;

    let default_channel = default_channel_for_app(manifest, app);

    let installers_dir = output_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("installers")
        .join(app_id)
        .join(rid);
    std::fs::create_dir_all(&installers_dir)?;

    let full_filename = full_package_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| {
            SurgeError::Pack(format!(
                "Invalid full package path (missing filename): {}",
                full_package_path.display()
            ))
        })?;
    let expected_delta_filename = format!("{app_id}-{version}-{rid}-delta.tar.zst");
    let delta_filename = if output_dir.join(&expected_delta_filename).is_file() {
        expected_delta_filename
    } else {
        String::new()
    };

    let icon_asset = resolve_installer_icon_asset(&target.icon, artifacts_dir, manifest_root)?;

    let requires_console_launcher = installer_types.iter().any(|installer_type| !installer_type.is_gui());
    let console_launcher = if requires_console_launcher {
        Some(find_installer_launcher_for_rid(rid, launcher_override)?)
    } else {
        None
    };
    let gui_launcher = if installer_types.iter().any(|t| t.is_gui()) {
        Some(find_gui_installer_launcher_for_rid(rid)?)
    } else {
        None
    };
    let surge_binary = find_surge_binary_for_rid(rid)?;
    let surge_binary_name = surge_binary_name_for_rid(rid).to_string();

    let mut generated = Vec::with_capacity(installer_types.len());
    for installer_type in installer_types {
        let installer_suffix = installer_type.as_str();
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let installer_filename = format!("Setup-{rid}-{app_id}-{default_channel}-{installer_suffix}.{installer_ext}");
        let installer_path = installers_dir.join(&installer_filename);

        let staging_dir =
            tempfile::tempdir().map_err(|e| SurgeError::Pack(format!("Failed to create staging directory: {e}")))?;
        let staging = staging_dir.path();

        let ui_mode = if installer_type.is_gui() {
            InstallerUi::Egui
        } else {
            InstallerUi::Console
        };
        let manifest_payload = InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: ui_mode,
            installer_type: installer_type.as_str().to_string(),
            app_id: app_id.to_string(),
            rid: rid.to_string(),
            version: version.to_string(),
            channel: default_channel.clone(),
            generated_utc: chrono::Utc::now().to_rfc3339(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: manifest.storage.provider.clone(),
                bucket: manifest.storage.bucket.clone(),
                region: manifest.storage.region.clone(),
                endpoint: manifest.storage.endpoint.clone(),
                prefix: installer_storage_prefix(manifest, app_id),
            },
            release: InstallerRelease {
                full_filename: full_filename.clone(),
                delta_filename: delta_filename.clone(),
                delta_algorithm: if delta_filename.is_empty() {
                    String::new()
                } else {
                    surge_core::releases::manifest::DIFF_ALGORITHM_BSDIFF.to_string()
                },
                delta_patch_format: if delta_filename.is_empty() {
                    String::new()
                } else {
                    match manifest.effective_pack_policy().delta_strategy {
                        surge_core::config::manifest::PackDeltaStrategy::ArchiveChunkedBsdiff => {
                            surge_core::releases::manifest::PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3.to_string()
                        }
                        surge_core::config::manifest::PackDeltaStrategy::ArchiveBsdiff => {
                            surge_core::releases::manifest::PATCH_FORMAT_BSDIFF4_ARCHIVE_V3.to_string()
                        }
                    }
                },
                delta_compression: if delta_filename.is_empty() {
                    String::new()
                } else {
                    surge_core::releases::manifest::COMPRESSION_ZSTD.to_string()
                },
            },
            runtime: InstallerRuntime {
                name: app.effective_name(),
                main_exe: app.effective_main_exe(),
                install_directory: app.effective_install_directory(),
                supervisor_id: app.supervisor_id.clone(),
                icon: target.icon.clone(),
                shortcuts: target.shortcuts.clone(),
                persistent_assets: target.persistent_assets.clone(),
                installers: target.installers.clone(),
                environment: target.environment.clone(),
            },
        };
        let manifest_yaml = serde_yaml::to_string(&manifest_payload)?;
        std::fs::write(staging.join("installer.yml"), manifest_yaml.as_bytes())?;

        std::fs::copy(&surge_binary, staging.join(&surge_binary_name))?;

        if let Some((source, _)) = &icon_asset {
            let assets_dir = staging.join("assets");
            std::fs::create_dir_all(&assets_dir)?;
            if let Some(filename) = source.file_name() {
                std::fs::copy(source, assets_dir.join(filename))?;
            }
        }

        if installer_type.is_offline() {
            let payload_dir = staging.join("payload");
            std::fs::create_dir_all(&payload_dir)?;
            std::fs::copy(full_package_path, payload_dir.join(&full_filename))?;
        }

        let payload_archive = tempfile::NamedTempFile::new()
            .map_err(|e| SurgeError::Pack(format!("Failed to create installer payload archive temp file: {e}")))?;
        let pack_policy = manifest.effective_pack_policy();
        let mut payload_packer = ArchivePacker::new(pack_policy.compression_level)?;
        payload_packer.add_directory(staging, "")?;
        payload_packer.finalize_to_file(payload_archive.path())?;
        let launcher = if installer_type.is_gui() {
            gui_launcher
                .as_ref()
                .ok_or_else(|| SurgeError::Pack("GUI installer launcher was not resolved".to_string()))?
        } else {
            console_launcher
                .as_ref()
                .ok_or_else(|| SurgeError::Pack("Console installer launcher was not resolved".to_string()))?
        };
        installer_bundle::write_embedded_installer(launcher, payload_archive.path(), &installer_path)?;
        surge_core::platform::fs::make_executable(&installer_path)?;
        generated.push(installer_path);
    }

    Ok(generated)
}

fn resolve_installer_icon_asset(
    icon: &str,
    artifacts_dir: &Path,
    manifest_root: &Path,
) -> Result<Option<(PathBuf, String)>> {
    let icon = icon.trim();
    if icon.is_empty() {
        return Ok(None);
    }

    let icon_path = Path::new(icon);
    let mut candidates: Vec<PathBuf> = Vec::new();
    if icon_path.is_absolute() {
        candidates.push(icon_path.to_path_buf());
    } else {
        candidates.push(artifacts_dir.join(icon_path));
        candidates.push(manifest_root.join(icon_path));
        if let Some(parent) = manifest_root.parent() {
            candidates.push(parent.join(icon_path));
        }
    }

    let source = candidates.into_iter().find(|candidate| candidate.is_file());
    let Some(source) = source else {
        return Ok(None);
    };

    let archive_name = source
        .file_name()
        .map(|name| format!("assets/{}", name.to_string_lossy()))
        .ok_or_else(|| SurgeError::Pack(format!("Invalid icon path: {}", source.display())))?;
    Ok(Some((source, archive_name)))
}

fn parse_installer_types(installers: &[String], app_id: &str, rid: &str) -> Result<Vec<InstallerType>> {
    installers
        .iter()
        .map(|installer| {
            InstallerType::parse(installer).ok_or_else(|| {
                SurgeError::Config(format!(
                    "Unsupported installer '{installer}' for app '{app_id}' target '{rid}'. Supported values: online, offline, online-gui, offline-gui"
                ))
            })
        })
        .collect()
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<ReleaseIndex> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data),
        Err(SurgeError::NotFound(_)) => Ok(ReleaseIndex::default()),
        Err(e) => Err(e),
    }
}

fn select_release_for_installers(
    releases: &[ReleaseEntry],
    channel: &str,
    version: Option<&str>,
    rid: &str,
) -> Option<ReleaseEntry> {
    let mut eligible: Vec<&ReleaseEntry> = releases
        .iter()
        .filter(|release| release.channels.iter().any(|c| c == channel))
        .collect();

    if let Some(requested) = version.map(str::trim).filter(|value| !value.is_empty()) {
        eligible.retain(|release| release.version == requested);
    }

    if eligible.is_empty() {
        return None;
    }

    let mut by_rid: Vec<&ReleaseEntry> = eligible.iter().copied().filter(|release| release.rid == rid).collect();
    by_rid.sort_by(|a, b| compare_versions(&b.version, &a.version));
    if let Some(release) = by_rid.first() {
        return Some((*release).clone());
    }

    let mut generic: Vec<&ReleaseEntry> = eligible
        .iter()
        .copied()
        .filter(|release| release.rid.trim().is_empty())
        .collect();
    generic.sort_by(|a, b| compare_versions(&b.version, &a.version));
    generic.first().map(|release| (*release).clone())
}

fn default_channel_for_app(manifest: &SurgeManifest, app: &AppConfig) -> String {
    app.channels
        .first()
        .cloned()
        .or_else(|| manifest.channels.first().map(|channel| channel.name.clone()))
        .unwrap_or_else(|| "stable".to_string())
}

fn installer_storage_prefix(manifest: &SurgeManifest, app_id: &str) -> String {
    if manifest.apps.len() > 1 {
        super::append_prefix(&manifest.storage.prefix, app_id)
    } else {
        manifest.storage.prefix.clone()
    }
}

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

fn find_gui_installer_launcher_for_rid(rid: &str) -> Result<PathBuf> {
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

    // current_exe is known to exist (checked above), use it as fallback.
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

pub(crate) fn default_artifacts_dir(manifest_path: &Path, app_id: &str, rid: &str, version: &str) -> PathBuf {
    manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("artifacts")
        .join(app_id)
        .join(rid)
        .join(version)
}

fn file_size_label(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => format_bytes(meta.len()),
        Err(_) => "unknown size".to_string(),
    }
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::info(&format!("[{stage}/{total}] {text}"));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::success(&format!("[{stage}/{total}] {text}"));
}

fn pack_build_phase_message(step_done: i32, step_count: i32) -> String {
    if step_done <= 0 {
        return format!("Packaging files (step 1/{step_count}: full archive)");
    }
    if step_done < step_count {
        return format!("Packaging files (step {}/{}: delta package)", step_done + 1, step_count);
    }
    "Finalizing package artifacts".to_string()
}

pub(crate) fn configure_context(manifest_path: &Path, manifest: &SurgeManifest, app_id: &str) -> Result<Context> {
    let ctx = super::build_app_scoped_storage_context(manifest, manifest_path, app_id)?;
    let pack_policy = manifest.effective_pack_policy();
    let mut budget = ctx.resource_budget();
    let available_threads = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);

    budget.max_threads = i32::try_from(available_threads).unwrap_or(i32::MAX);
    budget.max_memory_bytes = PACK_DEFAULT_MAX_MEMORY_BYTES;
    budget.zstd_compression_level = pack_policy.compression_level;
    ctx.set_resource_budget(budget);
    Ok(ctx)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use std::collections::BTreeMap;

    use super::*;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::installer_bundle::read_embedded_payload;
    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;
    use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index};

    fn set_installer_launcher_override(path: &Path) {
        set_surge_installer_launcher_override_for_test(path);
    }

    fn set_gui_installer_launcher_override(path: &Path) {
        set_surge_installer_ui_launcher_override_for_test(path);
    }

    fn create_stub_installer_launcher(dir: &Path, rid: &str) -> PathBuf {
        let ext = if rid.starts_with("win-") { ".exe" } else { "" };
        let stub_path = dir.join(format!("surge-installer{ext}"));
        std::fs::write(&stub_path, b"stub-launcher-bytes").expect("stub launcher write");
        make_executable(&stub_path).expect("stub launcher should be executable");
        stub_path
    }

    fn create_stub_gui_installer_launcher(dir: &Path, rid: &str) -> PathBuf {
        let ext = if rid.starts_with("win-") { ".exe" } else { "" };
        let stub_path = dir.join(format!("surge-installer-ui{ext}"));
        std::fs::write(&stub_path, b"stub-gui-launcher-bytes").expect("stub gui launcher write");
        make_executable(&stub_path).expect("stub gui launcher should be executable");
        stub_path
    }

    fn installer_payload(installer: &Path) -> Vec<u8> {
        read_embedded_payload(installer).expect("installer payload should be readable")
    }

    fn write_manifest(path: &Path, store_dir: &Path, app_id: &str, rid: &str) {
        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_id}
    main_exe: demoapp
    channels: [stable]
    target:
      rid: {rid}
      icon: icon.png
      installers: [online, offline]
",
            bucket = store_dir.display()
        );
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("manifest parent should be created");
        }
        std::fs::write(path, yaml).expect("manifest write should succeed");
    }

    fn make_release(version: &str, channel: &str, rid: &str, full_filename: &str, full_sha256: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec![channel.to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: true,
            full_filename: full_filename.to_string(),
            full_size: 1,
            full_sha256: full_sha256.to_string(),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demoapp".to_string(),
            install_directory: "demoapp".to_string(),
            supervisor_id: String::new(),
            icon: "icon.png".to_string(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: vec!["online".to_string(), "offline".to_string()],
            environment: BTreeMap::new(),
        }
    }

    fn write_release_index(store_dir: &Path, app_id: &str, releases: Vec<ReleaseEntry>) {
        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases,
            ..ReleaseIndex::default()
        };
        let data = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).expect("index compression");
        std::fs::write(store_dir.join(RELEASES_FILE_COMPRESSED), data).expect("index write should succeed");
    }

    #[tokio::test]
    async fn execute_installers_only_creates_online_and_offline_installers() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let app_id = "installer-app";
        let rid = current_rid();
        let version = "2.0.0";
        let stub = create_stub_installer_launcher(tmp.path(), &rid);
        set_installer_launcher_override(&stub);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let full_name = format!("{app_id}-{version}-{rid}-full.tar.zst");
        write_release_index(
            &store_dir,
            app_id,
            vec![make_release(
                version,
                "stable",
                &rid,
                &full_name,
                &sha256_hex(b"full package bytes"),
            )],
        );
        std::fs::write(packages_dir.join(&full_name), b"full package bytes").expect("full package should be written");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            Some(version),
            Some(&rid),
            Some(&artifacts_dir),
            &packages_dir,
            None,
            false,
        )
        .await
        .expect("installer generation should succeed");

        let installers_dir = packages_dir
            .parent()
            .expect("parent should exist")
            .join("installers")
            .join(app_id)
            .join(&rid);
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let online = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-online.{installer_ext}"));
        let offline = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.{installer_ext}"));
        assert!(online.exists(), "online installer should exist");
        assert!(offline.exists(), "offline installer should exist");

        let offline_data = installer_payload(&offline);
        let entries = surge_core::archive::extractor::list_entries_from_bytes(&offline_data)
            .expect("offline installer should be a valid archive");
        assert!(
            entries
                .iter()
                .any(|entry| entry.path.to_string_lossy().contains("installer.yml")),
            "offline installer staging should include installer.yml"
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.path.to_string_lossy().contains(&full_name)),
            "offline installer staging should embed the full package"
        );
    }

    #[tokio::test]
    async fn execute_installers_only_rebuilds_missing_direct_full_from_deltas() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let manifest_dir = tmp.path().join(".surge");
        let manifest_path = manifest_dir.join("surge.yml");
        let app_id = "installer-app";
        let rid = current_rid();
        let latest_version = "2.1.0";
        let previous_version = "2.0.0";
        let packages_dir = tmp.path().join("packages");
        let stub = create_stub_installer_launcher(tmp.path(), &rid);
        set_installer_launcher_override(&stub);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let default_artifacts = manifest_dir
            .join("artifacts")
            .join(app_id)
            .join(&rid)
            .join(latest_version);
        std::fs::create_dir_all(&default_artifacts).expect("default artifacts dir should be created");
        std::fs::write(default_artifacts.join("icon.png"), b"icon").expect("icon should be written");

        let previous_full_bytes = b"previous full package bytes".to_vec();
        let latest_full_bytes = b"latest full package bytes".to_vec();
        let latest_patch =
            bsdiff_buffers(&previous_full_bytes, &latest_full_bytes).expect("delta patch should be created");
        let latest_delta = zstd::encode_all(latest_patch.as_slice(), 3).expect("delta should be compressed");

        let previous_full = format!("{app_id}-{previous_version}-{rid}-full.tar.zst");
        let latest_full = format!("{app_id}-{latest_version}-{rid}-full.tar.zst");
        let latest_delta_key = format!("{app_id}-{latest_version}-{rid}-delta.tar.zst");
        let mut latest_release = make_release(
            latest_version,
            "stable",
            &rid,
            &latest_full,
            &sha256_hex(&latest_full_bytes),
        );
        latest_release.set_primary_delta(Some(surge_core::releases::manifest::DeltaArtifact::bsdiff_zstd(
            "primary",
            previous_version,
            &latest_delta_key,
            latest_delta.len() as i64,
            &sha256_hex(&latest_delta),
        )));
        write_release_index(
            &store_dir,
            app_id,
            vec![
                make_release(
                    previous_version,
                    "stable",
                    &rid,
                    &previous_full,
                    &sha256_hex(&previous_full_bytes),
                ),
                latest_release,
            ],
        );
        std::fs::write(store_dir.join(&previous_full), &previous_full_bytes)
            .expect("previous full package should be written to store");
        std::fs::write(store_dir.join(&latest_delta_key), &latest_delta)
            .expect("latest delta package should be written to store");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            None,
            Some(&rid),
            None,
            &packages_dir,
            None,
            false,
        )
        .await
        .expect("installer generation should succeed");

        assert!(
            packages_dir.join(&latest_full).is_file(),
            "missing direct full package should be rebuilt from stored deltas"
        );
        assert_eq!(
            std::fs::read(packages_dir.join(&latest_full)).expect("rebuilt full package should be readable"),
            latest_full_bytes
        );
        let installers_dir = packages_dir
            .parent()
            .expect("parent should exist")
            .join("installers")
            .join(app_id)
            .join(&rid);
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let offline = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.{installer_ext}"));
        assert!(offline.exists());
    }

    #[tokio::test]
    async fn execute_installers_only_writes_package_manifest_without_downloading_or_building() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let manifest_path = tmp.path().join("surge.yml");
        let packages_dir = tmp.path().join("packages");
        let package_file = tmp.path().join("cache").join("packages.txt");
        let app_id = "installer-app";
        let rid = current_rid();
        let version = "2.2.0";

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let full_name = format!("{app_id}-{version}-{rid}-full.tar.zst");
        let full_sha256 = sha256_hex(b"package bytes for cache manifest");
        write_release_index(
            &store_dir,
            app_id,
            vec![make_release(version, "stable", &rid, &full_name, &full_sha256)],
        );
        std::fs::write(store_dir.join(&full_name), b"package bytes for cache manifest")
            .expect("full package should be written to store");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            Some(version),
            Some(&rid),
            None,
            &packages_dir,
            Some(&package_file),
            false,
        )
        .await
        .expect("package manifest generation should succeed");

        assert_eq!(
            std::fs::read_to_string(&package_file).expect("package manifest should be readable"),
            format!("{full_sha256} {full_name}\n")
        );
        assert!(
            !packages_dir.join(&full_name).exists(),
            "package manifest generation should not download the full package"
        );
        assert!(
            !packages_dir
                .parent()
                .expect("parent should exist")
                .join("installers")
                .exists(),
            "package manifest generation should not build installers"
        );
    }

    #[tokio::test]
    async fn execute_installers_only_package_manifest_includes_delta_chain_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let manifest_path = tmp.path().join("surge.yml");
        let packages_dir = tmp.path().join("packages");
        let package_file = tmp.path().join("cache").join("packages.txt");
        let app_id = "installer-app";
        let rid = current_rid();
        let previous_version = "2.1.0";
        let version = "2.2.0";

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let previous_full_bytes = b"previous full package bytes".to_vec();
        let latest_full_bytes = b"latest full package bytes".to_vec();
        let latest_patch =
            bsdiff_buffers(&previous_full_bytes, &latest_full_bytes).expect("delta patch should be created");
        let latest_delta = zstd::encode_all(latest_patch.as_slice(), 3).expect("delta should be compressed");

        let previous_full = format!("{app_id}-{previous_version}-{rid}-full.tar.zst");
        let latest_full = format!("{app_id}-{version}-{rid}-full.tar.zst");
        let latest_delta_key = format!("{app_id}-{version}-{rid}-delta.tar.zst");
        let mut latest_release = make_release(version, "stable", &rid, &latest_full, &sha256_hex(&latest_full_bytes));
        latest_release.set_primary_delta(Some(surge_core::releases::manifest::DeltaArtifact::bsdiff_zstd(
            "primary",
            previous_version,
            &latest_delta_key,
            latest_delta.len() as i64,
            &sha256_hex(&latest_delta),
        )));
        write_release_index(
            &store_dir,
            app_id,
            vec![
                make_release(
                    previous_version,
                    "stable",
                    &rid,
                    &previous_full,
                    &sha256_hex(&previous_full_bytes),
                ),
                latest_release,
            ],
        );
        std::fs::write(store_dir.join(&previous_full), &previous_full_bytes)
            .expect("previous full package should be written to store");
        std::fs::write(store_dir.join(&latest_delta_key), &latest_delta)
            .expect("latest delta package should be written to store");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            Some(version),
            Some(&rid),
            None,
            &packages_dir,
            Some(&package_file),
            false,
        )
        .await
        .expect("package manifest generation should succeed");

        assert_eq!(
            std::fs::read_to_string(&package_file).expect("package manifest should be readable"),
            format!(
                "{} {}\n{} {}\n",
                sha256_hex(&previous_full_bytes),
                previous_full,
                sha256_hex(&latest_delta),
                latest_delta_key
            )
        );
        assert!(
            !packages_dir.join(&latest_full).exists(),
            "package manifest generation should not reconstruct the full package"
        );
    }

    #[tokio::test]
    async fn execute_installers_only_uploads_installers_to_storage() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let app_id = "installer-app";
        let rid = current_rid();
        let version = "2.3.0";
        let stub = create_stub_installer_launcher(tmp.path(), &rid);
        set_installer_launcher_override(&stub);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let full_name = format!("{app_id}-{version}-{rid}-full.tar.zst");
        write_release_index(
            &store_dir,
            app_id,
            vec![make_release(
                version,
                "stable",
                &rid,
                &full_name,
                &sha256_hex(b"full package bytes"),
            )],
        );
        std::fs::write(packages_dir.join(&full_name), b"full package bytes").expect("full package should be written");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            Some(version),
            Some(&rid),
            Some(&artifacts_dir),
            &packages_dir,
            None,
            true,
        )
        .await
        .expect("installer generation and upload should succeed");

        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let online_name = format!("Setup-{rid}-{app_id}-stable-online.{installer_ext}");
        let offline_name = format!("Setup-{rid}-{app_id}-stable-offline.{installer_ext}");

        assert!(
            store_dir.join("installers").join(&online_name).is_file(),
            "online installer should be uploaded to the flat installers/ path"
        );
        assert!(
            store_dir.join("installers").join(&offline_name).is_file(),
            "offline installer should be uploaded to the flat installers/ path"
        );
    }

    #[tokio::test]
    async fn execute_pack_uses_default_dot_surge_artifacts_layout() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let manifest_path = tmp.path().join(".surge").join("surge.yml");
        let packages_dir = tmp.path().join(".surge").join("packages");
        let app_id = "installer-app";
        let rid = current_rid();
        let version = "3.0.0";
        let stub = create_stub_installer_launcher(tmp.path(), &rid);
        set_installer_launcher_override(&stub);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let artifacts_dir = default_artifacts_dir(&manifest_path, app_id, &rid, version);
        std::fs::create_dir_all(&artifacts_dir).expect("default artifacts dir should be created");
        std::fs::write(artifacts_dir.join("payload.txt"), b"payload").expect("payload should be written");
        std::fs::write(artifacts_dir.join("demoapp"), b"#!/bin/sh\necho ok\n").expect("main exe should be written");
        make_executable(&artifacts_dir.join("demoapp")).expect("main exe should be executable");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");

        execute(&manifest_path, Some(app_id), version, Some(&rid), None, &packages_dir)
            .await
            .expect("pack should succeed with default artifacts path");

        assert!(
            packages_dir
                .join(format!("{app_id}-{version}-{rid}-full.tar.zst"))
                .exists()
        );
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        assert!(
            packages_dir
                .parent()
                .expect("parent should exist")
                .join("installers")
                .join(app_id)
                .join(&rid)
                .join(format!("Setup-{rid}-{app_id}-stable-online.{installer_ext}"))
                .exists()
        );
    }

    #[test]
    fn build_installers_uses_app_scoped_prefix_in_multi_app_manifest() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let output_dir = tmp.path().join("packages");
        let app_id = "app-a";
        let rid = current_rid();
        let version = "1.2.3";
        let stub = create_stub_installer_launcher(tmp.path(), &rid);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&output_dir).expect("packages dir should be created");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");

        let full_package = output_dir.join(format!("{app_id}-{version}-{rid}-full.tar.zst"));
        std::fs::write(&full_package, b"full package bytes").expect("full package should be written");

        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {}
  prefix: releases
apps:
  - id: app-a
    main_exe: demoapp
    channels: [stable]
    target:
      rid: {rid}
      icon: icon.png
      installers: [online]
  - id: app-b
    main_exe: demoapp
    channels: [stable]
    target:
      rid: {rid}
      icon: icon.png
      installers: [online]
",
            store_dir.display(),
            rid = rid
        );
        let manifest = SurgeManifest::parse(yaml.as_bytes()).expect("manifest should parse");
        let (app, target) = manifest
            .find_app_with_target(app_id, &rid)
            .expect("app/target should exist in manifest");

        let installers = build_installers_with_launcher(
            &manifest,
            app,
            &target,
            app_id,
            &rid,
            version,
            tmp.path(),
            &artifacts_dir,
            &output_dir,
            &full_package,
            Some(&stub),
        )
        .expect("installer build should succeed");
        assert_eq!(installers.len(), 1);

        let installer_data = installer_payload(&installers[0]);
        let installer_manifest = String::from_utf8(
            surge_core::archive::extractor::read_entry(&installer_data, "installer.yml")
                .expect("installer.yml should be present"),
        )
        .expect("installer.yml should be UTF-8");
        assert!(
            installer_manifest.contains("prefix: releases/app-a"),
            "installer manifest should use app-scoped prefix in multi-app manifests"
        );
    }

    #[test]
    fn build_installers_gui_only_does_not_require_console_launcher() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let output_dir = tmp.path().join("packages");
        let app_id = "app-gui";
        let rid = current_rid();
        let version = "1.0.0";

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&output_dir).expect("packages dir should be created");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");

        let gui_stub = create_stub_gui_installer_launcher(tmp.path(), &rid);
        set_gui_installer_launcher_override(&gui_stub);

        let full_package = output_dir.join(format!("{app_id}-{version}-{rid}-full.tar.zst"));
        std::fs::write(&full_package, b"full package bytes").expect("full package should be written");

        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {}
apps:
  - id: {app_id}
    main_exe: demoapp
    channels: [stable]
    target:
      rid: {rid}
      icon: icon.png
      installers: [online-gui]
",
            store_dir.display(),
            rid = rid
        );
        let manifest = SurgeManifest::parse(yaml.as_bytes()).expect("manifest should parse");
        let (app, target) = manifest
            .find_app_with_target(app_id, &rid)
            .expect("app/target should exist in manifest");

        let missing_console_launcher = tmp.path().join("missing-surge-installer");
        let installers = build_installers_with_launcher(
            &manifest,
            app,
            &target,
            app_id,
            &rid,
            version,
            tmp.path(),
            &artifacts_dir,
            &output_dir,
            &full_package,
            Some(&missing_console_launcher),
        )
        .expect("gui-only installer build should not require console launcher");
        assert_eq!(installers.len(), 1);
    }

    #[test]
    fn build_installers_resolves_icon_relative_to_manifest_root_parent() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let manifest_root = tmp.path().join(".surge");
        let artifacts_dir = tmp.path().join("artifacts");
        let output_dir = tmp.path().join("packages");
        let app_id = "app-icon";
        let rid = current_rid();
        let version = "1.0.0";
        let stub = create_stub_installer_launcher(tmp.path(), &rid);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&manifest_root).expect("manifest root should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&output_dir).expect("packages dir should be created");

        let icon_path = manifest_root.join("youpark.svg");
        std::fs::write(&icon_path, b"<svg></svg>").expect("icon should be written");

        let full_package = output_dir.join(format!("{app_id}-{version}-{rid}-full.tar.zst"));
        std::fs::write(&full_package, b"full package bytes").expect("full package should be written");

        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {}
apps:
  - id: {app_id}
    main_exe: demoapp
    channels: [stable]
    target:
      rid: {rid}
      icon: .surge/youpark.svg
      installers: [online]
",
            store_dir.display(),
            rid = rid
        );
        let manifest = SurgeManifest::parse(yaml.as_bytes()).expect("manifest should parse");
        let (app, target) = manifest
            .find_app_with_target(app_id, &rid)
            .expect("app/target should exist in manifest");

        let installers = build_installers_with_launcher(
            &manifest,
            app,
            &target,
            app_id,
            &rid,
            version,
            &manifest_root,
            &artifacts_dir,
            &output_dir,
            &full_package,
            Some(&stub),
        )
        .expect("installer build should succeed");
        assert_eq!(installers.len(), 1);

        let installer_data = installer_payload(&installers[0]);
        let entries = surge_core::archive::extractor::list_entries_from_bytes(&installer_data)
            .expect("installer payload should be a valid archive");
        assert!(
            entries
                .iter()
                .any(|entry| entry.path.to_string_lossy() == "assets/youpark.svg"),
            "installer payload should contain icon asset copied from manifest-relative path"
        );
    }

    #[test]
    fn ensure_host_compatible_rid_rejects_cross_target_arch() {
        let host = current_rid();
        if host.ends_with("-x64") {
            let cross = host.replacen("-x64", "-arm64", 1);
            let err = ensure_host_compatible_rid(&cross).expect_err("cross rid should fail");
            assert!(
                err.to_string().contains("host-only"),
                "error should mention host-only generation"
            );
        } else if host.ends_with("-arm64") {
            let cross = host.replacen("-arm64", "-x64", 1);
            let err = ensure_host_compatible_rid(&cross).expect_err("cross rid should fail");
            assert!(
                err.to_string().contains("host-only"),
                "error should mention host-only generation"
            );
        }
    }
}
