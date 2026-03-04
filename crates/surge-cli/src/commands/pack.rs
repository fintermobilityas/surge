use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::logline;
use crate::ui::UiTheme;
use serde::Serialize;
use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::{AppConfig, InstallerType, ShortcutLocation, SurgeManifest, TargetConfig};
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::pack::builder::PackBuilder;
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

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
    let ctx = Arc::new(configure_context(&manifest, &app_id)?);
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
    builder.build(None).await?;

    let mut artifact_count = 0usize;
    for artifact in builder.artifacts() {
        let dest = output_dir.join(&artifact.filename);
        if artifact.path != dest {
            std::fs::copy(&artifact.path, &dest)?;
        }
        artifact_count += 1;
        logline::subtle(&format!("  Created {}", dest.display()));
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
        artifacts_dir.as_path(),
        output_dir,
        &full_package_path,
    )?;
    let installer_count = installer_paths.len();
    for installer in installer_paths {
        logline::subtle(&format!("  Created {}", installer.display()));
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
    std::fs::create_dir_all(output_dir)?;
    let default_channel = default_channel_for_app(&manifest, app);
    print_stage_done(
        theme,
        1,
        TOTAL_STAGES,
        &format!("Target: {app_id}/{rid} (channel: {default_channel})"),
    );

    print_stage(theme, 2, TOTAL_STAGES, "Resolving release for installer build");
    let storage_config = super::build_app_scoped_storage_config(&manifest, &app_id)?;
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
    let selected_version = selected_release.version.clone();
    let full_key = selected_release.full_filename.trim();
    if full_key.is_empty() {
        return Err(SurgeError::Pack(format!(
            "Selected release {} for {}/{} does not define a full package filename",
            selected_release.version, app_id, rid
        )));
    }
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Selected release version {selected_version}"),
    );

    let artifacts_dir = artifacts_dir.map_or_else(
        || default_artifacts_dir(manifest_path, &app_id, &rid, &selected_version),
        PathBuf::from,
    );
    if !artifacts_dir.is_dir() {
        return Err(SurgeError::Pack(format!(
            "Artifacts directory does not exist: {}. Use --artifacts-dir to override.",
            artifacts_dir.display()
        )));
    }

    let local_full_name = Path::new(full_key)
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .ok_or_else(|| SurgeError::Pack(format!("Invalid full package key: {full_key}")))?;
    let full_package_path = output_dir.join(local_full_name);
    print_stage(theme, 3, TOTAL_STAGES, "Ensuring full package is available");
    if full_package_path.is_file() {
        print_stage_done(
            theme,
            3,
            TOTAL_STAGES,
            &format!("Using local package {}", full_package_path.display()),
        );
    } else {
        logline::info(&format!(
            "Full package missing locally; downloading '{}' to '{}'",
            full_key,
            full_package_path.display()
        ));
        backend.download_to_file(full_key, &full_package_path, None).await?;
        print_stage_done(
            theme,
            3,
            TOTAL_STAGES,
            &format!("Downloaded {}", full_package_path.display()),
        );
    }

    print_stage(
        theme,
        4,
        TOTAL_STAGES,
        &format!("Building installers for {app_id} v{selected_version} ({rid})"),
    );

    let installer_paths = build_installers(
        &manifest,
        app,
        &target,
        &app_id,
        &rid,
        &selected_version,
        &artifacts_dir,
        output_dir,
        &full_package_path,
    )?;
    if installer_paths.is_empty() {
        print_stage_done(
            theme,
            4,
            TOTAL_STAGES,
            &format!(
                "No installers configured for {app_id}/{rid}. Configure `installers: [web]` or `installers: [offline]` in the manifest."
            ),
        );
        return Ok(());
    }
    for installer in installer_paths {
        logline::subtle(&format!("  Created {}", installer.display()));
    }

    print_stage_done(theme, 4, TOTAL_STAGES, "Installer bundles created");
    print_stage(theme, 5, TOTAL_STAGES, "Finalize restore-installers summary");
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!("Completed in {}", format_duration(started.elapsed())),
    );

    Ok(())
}

#[derive(Debug, Serialize)]
struct InstallerManifest {
    schema: i32,
    format: &'static str,
    ui: &'static str,
    installer_type: String,
    app_id: String,
    rid: String,
    version: String,
    channel: String,
    generated_utc: String,
    headless_default_if_no_display: bool,
    release_index_key: &'static str,
    storage: InstallerStorage,
    release: InstallerRelease,
    runtime: InstallerRuntime,
}

#[derive(Debug, Serialize)]
struct InstallerStorage {
    provider: String,
    bucket: String,
    region: String,
    endpoint: String,
    prefix: String,
}

#[derive(Debug, Serialize)]
struct InstallerRelease {
    full_filename: String,
    delta_filename: String,
}

#[derive(Debug, Serialize)]
struct InstallerRuntime {
    name: String,
    main_exe: String,
    install_directory: String,
    supervisor_id: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
    persistent_assets: Vec<String>,
    installers: Vec<String>,
    environment: BTreeMap<String, String>,
}

#[allow(clippy::too_many_arguments)]
fn build_installers(
    manifest: &SurgeManifest,
    app: &AppConfig,
    target: &TargetConfig,
    app_id: &str,
    rid: &str,
    version: &str,
    artifacts_dir: &Path,
    output_dir: &Path,
    full_package_path: &Path,
) -> Result<Vec<PathBuf>> {
    let installer_types = parse_installer_types(&target.installers, app_id, rid)?;
    if installer_types.is_empty() {
        return Ok(Vec::new());
    }

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

    let icon_asset = if target.icon.trim().is_empty() {
        None
    } else {
        let source = artifacts_dir.join(&target.icon);
        if source.is_file() {
            let archive_name = source
                .file_name()
                .map(|name| format!("assets/{}", name.to_string_lossy()))
                .ok_or_else(|| SurgeError::Pack(format!("Invalid icon path in artifacts: {}", source.display())))?;
            Some((source, archive_name))
        } else {
            None
        }
    };

    let mut generated = Vec::with_capacity(installer_types.len());
    for installer_type in installer_types {
        let installer_suffix = installer_type.as_str();
        let installer_filename =
            format!("Setup-{rid}-{app_id}-{default_channel}-{installer_suffix}.surge-installer.tar.zst");
        let installer_path = installers_dir.join(&installer_filename);

        let manifest_payload = InstallerManifest {
            schema: 1,
            format: "surge-installer-v1",
            ui: "imgui",
            installer_type: installer_type.as_str().to_string(),
            app_id: app_id.to_string(),
            rid: rid.to_string(),
            version: version.to_string(),
            channel: default_channel.clone(),
            generated_utc: chrono::Utc::now().to_rfc3339(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED,
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

        let mut packer = ArchivePacker::new(DEFAULT_ZSTD_LEVEL)?;
        packer.add_buffer("installer.yml", manifest_yaml.as_bytes(), 0o644)?;
        if let Some((source, archive_name)) = &icon_asset {
            packer.add_file(source, archive_name)?;
        }
        if matches!(installer_type, InstallerType::Offline) {
            packer.add_file(full_package_path, &format!("payload/{full_filename}"))?;
        }
        packer.finalize_to_file(&installer_path)?;
        generated.push(installer_path);
    }

    Ok(generated)
}

fn parse_installer_types(installers: &[String], app_id: &str, rid: &str) -> Result<Vec<InstallerType>> {
    installers
        .iter()
        .map(|installer| {
            InstallerType::parse(installer).ok_or_else(|| {
                SurgeError::Config(format!(
                    "Unsupported installer '{installer}' for app '{app_id}' target '{rid}'. Supported values: web, offline"
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

fn default_artifacts_dir(manifest_path: &Path, app_id: &str, rid: &str, version: &str) -> PathBuf {
    manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("artifacts")
        .join(app_id)
        .join(rid)
        .join(version)
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::info(&format!("[{stage}/{total}] {text}"));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::success(&format!("[{stage}/{total}] {text}"));
}

fn configure_context(manifest: &SurgeManifest, app_id: &str) -> Result<Context> {
    super::build_app_scoped_storage_context(manifest, app_id)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use surge_core::archive::extractor::{list_entries_from_bytes, read_entry};
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::platform::detect::current_rid;
    use surge_core::platform::fs::make_executable;
    use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index};

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
      installers: [web, offline]
",
            bucket = store_dir.display()
        );
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("manifest parent should be created");
        }
        std::fs::write(path, yaml).expect("manifest write should succeed");
    }

    fn make_release(version: &str, channel: &str, rid: &str, full_filename: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec![channel.to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: true,
            full_filename: full_filename.to_string(),
            full_size: 1,
            full_sha256: String::new(),
            delta_filename: String::new(),
            delta_size: 0,
            delta_sha256: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demoapp".to_string(),
            install_directory: "demoapp".to_string(),
            supervisor_id: String::new(),
            icon: "icon.png".to_string(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: vec!["web".to_string(), "offline".to_string()],
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
    async fn execute_installers_only_creates_web_and_offline_installers() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");
        let store_dir = tmp.path().join("store");
        let artifacts_dir = tmp.path().join("artifacts");
        let packages_dir = tmp.path().join("packages");
        let manifest_path = tmp.path().join("surge.yml");
        let app_id = "installer-app";
        let rid = "linux-x64";
        let version = "2.0.0";

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&artifacts_dir).expect("artifacts dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        std::fs::write(artifacts_dir.join("icon.png"), b"icon").expect("icon should be written");
        write_manifest(&manifest_path, &store_dir, app_id, rid);

        let full_name = format!("{app_id}-{version}-{rid}-full.tar.zst");
        write_release_index(
            &store_dir,
            app_id,
            vec![make_release(version, "stable", rid, &full_name)],
        );
        std::fs::write(packages_dir.join(&full_name), b"full package bytes").expect("full package should be written");

        execute_installers_only(
            &manifest_path,
            Some(app_id),
            Some(version),
            Some(rid),
            Some(&artifacts_dir),
            &packages_dir,
        )
        .await
        .expect("installer generation should succeed");

        let installers_dir = packages_dir
            .parent()
            .expect("parent should exist")
            .join("installers")
            .join(app_id)
            .join(rid);
        let web = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-web.surge-installer.tar.zst"));
        let offline = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.surge-installer.tar.zst"));
        assert!(web.exists());
        assert!(offline.exists());

        let offline_data = std::fs::read(offline).expect("offline installer should be readable");
        let entries = list_entries_from_bytes(&offline_data).expect("offline installer should be a valid archive");
        assert!(
            entries.iter().any(|entry| entry.path.as_os_str() == "installer.yml"),
            "offline installer should include installer.yml"
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.path == Path::new("payload").join(full_name.as_str())),
            "offline installer should embed the full package"
        );
    }

    #[tokio::test]
    async fn execute_installers_only_defaults_to_latest_and_restores_missing_full_package() {
        let tmp = tempfile::tempdir().expect("temp dir should be created");
        let store_dir = tmp.path().join("store");
        let manifest_dir = tmp.path().join(".surge");
        let manifest_path = manifest_dir.join("surge.yml");
        let app_id = "installer-app";
        let rid = "linux-x64";
        let latest_version = "2.1.0";
        let previous_version = "2.0.0";
        let packages_dir = tmp.path().join("packages");

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(&packages_dir).expect("packages dir should be created");
        write_manifest(&manifest_path, &store_dir, app_id, rid);

        let default_artifacts = manifest_dir
            .join("artifacts")
            .join(app_id)
            .join(rid)
            .join(latest_version);
        std::fs::create_dir_all(&default_artifacts).expect("default artifacts dir should be created");
        std::fs::write(default_artifacts.join("icon.png"), b"icon").expect("icon should be written");

        let previous_full = format!("{app_id}-{previous_version}-{rid}-full.tar.zst");
        let latest_full = format!("{app_id}-{latest_version}-{rid}-full.tar.zst");
        write_release_index(
            &store_dir,
            app_id,
            vec![
                make_release(previous_version, "stable", rid, &previous_full),
                make_release(latest_version, "stable", rid, &latest_full),
            ],
        );
        std::fs::write(store_dir.join(&latest_full), b"latest full package bytes")
            .expect("latest full package should be written to store");

        execute_installers_only(&manifest_path, Some(app_id), None, Some(rid), None, &packages_dir)
            .await
            .expect("installer generation should succeed");

        assert!(
            packages_dir.join(&latest_full).is_file(),
            "missing full package should be restored from storage"
        );
        let installers_dir = packages_dir
            .parent()
            .expect("parent should exist")
            .join("installers")
            .join(app_id)
            .join(rid);
        let offline = installers_dir.join(format!("Setup-{rid}-{app_id}-stable-offline.surge-installer.tar.zst"));
        assert!(offline.exists());
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
        assert!(
            packages_dir
                .parent()
                .expect("parent should exist")
                .join("installers")
                .join(app_id)
                .join(&rid)
                .join(format!("Setup-{rid}-{app_id}-stable-web.surge-installer.tar.zst"))
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
        let rid = "linux-x64";
        let version = "1.2.3";

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
      rid: linux-x64
      icon: icon.png
      installers: [web]
  - id: app-b
    main_exe: demoapp
    channels: [stable]
    target:
      rid: linux-x64
      icon: icon.png
      installers: [web]
",
            store_dir.display()
        );
        let manifest = SurgeManifest::parse(yaml.as_bytes()).expect("manifest should parse");
        let (app, target) = manifest
            .find_app_with_target(app_id, rid)
            .expect("app/target should exist in manifest");

        let installers = build_installers(
            &manifest,
            app,
            &target,
            app_id,
            rid,
            version,
            &artifacts_dir,
            &output_dir,
            &full_package,
        )
        .expect("installer build should succeed");
        assert_eq!(installers.len(), 1);

        let installer_data = std::fs::read(&installers[0]).expect("installer archive should be readable");
        let installer_manifest =
            String::from_utf8(read_entry(&installer_data, "installer.yml").expect("installer.yml should be present"))
                .expect("installer.yml should be UTF-8");
        assert!(
            installer_manifest.contains("prefix: releases/app-a"),
            "installer manifest should use app-scoped prefix in multi-app manifests"
        );
    }
}
