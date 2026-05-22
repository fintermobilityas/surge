#![allow(clippy::too_many_lines)]

mod installers;
mod launchers;
mod progress;
mod resolution;
mod upload;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use self::installers::build_installers;
pub(crate) use self::launchers::{
    ensure_host_compatible_rid, find_installer_launcher_for_rid, find_surge_binary_for_rid, surge_binary_name_for_rid,
};
#[cfg(test)]
pub(crate) use self::launchers::{
    set_surge_installer_launcher_override_for_test, set_surge_installer_ui_launcher_override_for_test,
};
use self::progress::{file_size_label, pack_build_phase_message, print_stage, print_stage_done};
pub(crate) use self::resolution::{configure_context, default_artifacts_dir};
use self::resolution::{resolve_installer_package, write_package_manifest};
use self::upload::{build_installer_upload_backend, upload_installers_to_storage};
use crate::formatters::format_duration;
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::pack::builder::PackBuilder;
use surge_core::releases::artifact_cache::{CacheFetchOutcome, fetch_or_reuse_file};
use surge_core::releases::restore::{
    RestoreOptions, plan_full_archive_restore, restore_full_archive_for_version_with_options,
};
use surge_core::storage_config::build_storage_config;

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
        &self::resolution::default_channel_for_app(&manifest, app),
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
    channel: Option<&str>,
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
        resolve_installer_package(&manifest, manifest_path, app_id, version, channel, rid, artifacts_dir).await?;
    print_stage_done(
        theme,
        1,
        total_stages,
        &format!(
            "Target: {}/{} (channel: {})",
            resolved.app_id, resolved.rid, resolved.selected_channel
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
                    ..RestoreOptions::default()
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
        &resolved.selected_channel,
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
#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use std::collections::BTreeMap;

    use super::*;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::installer_bundle::read_embedded_payload;
    use surge_core::pack::builder::{PackageArtifactMetadata, package_metadata_filename};
    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;
    use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index};

    fn set_installer_launcher_override(path: &Path) {
        super::set_surge_installer_launcher_override_for_test(path);
    }

    fn set_gui_installer_launcher_override(path: &Path) {
        super::set_surge_installer_ui_launcher_override_for_test(path);
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
        write_manifest_with_channels(path, store_dir, app_id, rid, &["stable"]);
    }

    fn write_manifest_with_channels(path: &Path, store_dir: &Path, app_id: &str, rid: &str, channels: &[&str]) {
        let channels_yaml = channels.join(", ");
        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_id}
    main_exe: demoapp
    channels: [{channels_yaml}]
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
            full_compression_level: 0,
            full_zstd_workers: 0,
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
            None,
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
            None,
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
            None,
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
            None,
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
    async fn execute_installers_only_uses_requested_channel_for_selection_and_installer_manifest() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");

        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let app_id = "installer-app";
        let rid = current_rid();
        let production_version = "1.2.3";
        let test_version = "9.9.9";
        let stub = create_stub_installer_launcher(tmp.path(), &rid);
        set_installer_launcher_override(&stub);

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");
        write_manifest_with_channels(&manifest_path, &store_dir, app_id, &rid, &["test", "production"]);

        let test_full = format!("{app_id}-{test_version}-{rid}-full.tar.zst");
        let production_full = format!("{app_id}-{production_version}-{rid}-full.tar.zst");
        write_release_index(
            &store_dir,
            app_id,
            vec![
                make_release(
                    test_version,
                    "test",
                    &rid,
                    &test_full,
                    &sha256_hex(b"test package bytes"),
                ),
                make_release(
                    production_version,
                    "production",
                    &rid,
                    &production_full,
                    &sha256_hex(b"production package bytes"),
                ),
            ],
        );
        std::fs::write(store_dir.join(&test_full), b"test package bytes").expect("test package should be written");
        std::fs::write(store_dir.join(&production_full), b"production package bytes")
            .expect("production package should be written");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            None,
            Some("production"),
            Some(&rid),
            Some(&artifacts_dir),
            &packages_dir,
            None,
            false,
        )
        .await
        .expect("installer generation should succeed");

        assert!(
            packages_dir.join(&production_full).is_file(),
            "requested channel should select the production release"
        );
        assert!(
            !packages_dir.join(&test_full).is_file(),
            "requested channel should not select the manifest default channel release"
        );

        let installers_dir = packages_dir
            .parent()
            .expect("parent should exist")
            .join("installers")
            .join(app_id)
            .join(&rid);
        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let offline = installers_dir.join(format!("Setup-{rid}-{app_id}-production-offline.{installer_ext}"));
        assert!(
            offline.exists(),
            "offline installer should use the requested channel in its filename"
        );

        let payload = installer_payload(&offline);
        let installer_manifest = String::from_utf8(
            surge_core::archive::extractor::read_entry(&payload, "installer.yml")
                .expect("installer.yml should be present"),
        )
        .expect("installer.yml should be UTF-8");
        assert!(installer_manifest.contains("channel: production"));
        assert!(installer_manifest.contains("version: 1.2.3"));
    }

    #[tokio::test]
    async fn execute_installers_only_uploads_installers_to_requested_channel_key() {
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
        write_manifest_with_channels(&manifest_path, &store_dir, app_id, &rid, &["test", "production"]);

        let full_name = format!("{app_id}-{version}-{rid}-full.tar.zst");
        write_release_index(
            &store_dir,
            app_id,
            vec![make_release(
                version,
                "production",
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
            Some("production"),
            Some(&rid),
            Some(&artifacts_dir),
            &packages_dir,
            None,
            true,
        )
        .await
        .expect("installer generation and upload should succeed");

        let installer_ext = if rid.starts_with("win-") { "exe" } else { "bin" };
        let online_name = format!("Setup-{rid}-{app_id}-production-online.{installer_ext}");
        let offline_name = format!("Setup-{rid}-{app_id}-production-offline.{installer_ext}");

        assert!(
            store_dir.join("installers").join(&online_name).is_file(),
            "online installer should be uploaded under the requested channel key"
        );
        assert!(
            store_dir.join("installers").join(&offline_name).is_file(),
            "offline installer should be uploaded under the requested channel key"
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
        let full_filename = format!("{app_id}-{version}-{rid}-full.tar.zst");
        let metadata_path = packages_dir.join(package_metadata_filename(&full_filename));
        let metadata: PackageArtifactMetadata =
            serde_yaml::from_slice(&std::fs::read(&metadata_path).expect("package metadata should be readable"))
                .expect("package metadata should parse");
        assert_eq!(metadata.app_id, app_id);
        assert_eq!(metadata.version, version);
        assert_eq!(metadata.rid, rid);
        assert_eq!(metadata.archive_filename, full_filename);
        assert_eq!(metadata.full_compression_level, 3);
        assert!(metadata.full_zstd_workers >= 0);
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

        let installers = installers::build_installers_with_launcher(
            &manifest,
            app,
            &target,
            app_id,
            &rid,
            version,
            "stable",
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
        let installers = installers::build_installers_with_launcher(
            &manifest,
            app,
            &target,
            app_id,
            &rid,
            version,
            "stable",
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

        let icon_path = manifest_root.join("sample-icon.svg");
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
      icon: .surge/sample-icon.svg
      installers: [online]
",
            store_dir.display(),
            rid = rid
        );
        let manifest = SurgeManifest::parse(yaml.as_bytes()).expect("manifest should parse");
        let (app, target) = manifest
            .find_app_with_target(app_id, &rid)
            .expect("app/target should exist in manifest");

        let installers = installers::build_installers_with_launcher(
            &manifest,
            app,
            &target,
            app_id,
            &rid,
            version,
            "stable",
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
                .any(|entry| entry.path.to_string_lossy() == "assets/sample-icon.svg"),
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
