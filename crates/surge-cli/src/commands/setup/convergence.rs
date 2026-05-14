use std::path::Path;
use std::sync::{Arc, Mutex};

use super::ProgressReporter;
use crate::logline;
use surge_core::config::installer::InstallerManifest;
use surge_core::context::{Context, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::releases::version::compare_versions;
use surge_core::storage_config::build_storage_config_from_installer_manifest;
use surge_core::update::manager::{ApplyStrategy, ProgressInfo, UpdateInfo, UpdateManager};

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstalledRuntimeManifest {
    #[serde(default)]
    id: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    bucket: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    endpoint: String,
}

pub(super) async fn converge_existing_install(
    manifest: &InstallerManifest,
    install_root: &Path,
    no_start: bool,
) -> Result<bool> {
    let active_app_dir = install_root.join("app");
    let Some(existing) = read_installed_runtime_manifest(&active_app_dir)? else {
        return Ok(false);
    };

    if existing.id.trim() != manifest.app_id.trim() || existing.version.trim().is_empty() {
        return Ok(false);
    }

    match compare_versions(existing.version.trim(), manifest.version.trim()) {
        std::cmp::Ordering::Equal => {
            if runtime_metadata_matches(&existing, manifest) {
                logline::success(&format!(
                    "'{}' v{} ({}) is already installed, skipping.",
                    manifest.app_id, manifest.version, manifest.channel
                ));
            } else {
                repair_runtime_metadata(manifest, install_root)?;
                logline::success(&format!(
                    "Repaired runtime metadata for '{}' v{} ({}).",
                    manifest.app_id, manifest.version, manifest.channel
                ));
            }
            if !no_start {
                logline::info("Existing install was already current; no restart was performed.");
            }
            Ok(true)
        }
        std::cmp::Ordering::Greater => Err(SurgeError::Update(format!(
            "Installed version {} is newer than installer target {}. Use a reinstall path to downgrade.",
            existing.version, manifest.version
        ))),
        std::cmp::Ordering::Less => update_existing_install(manifest, install_root, &existing.version).await,
    }
}

async fn update_existing_install(
    manifest: &InstallerManifest,
    install_root: &Path,
    current_version: &str,
) -> Result<bool> {
    let ctx = Arc::new(context_from_installer_manifest(manifest)?);
    let mut manager = UpdateManager::new(
        Arc::clone(&ctx),
        &manifest.app_id,
        current_version,
        &manifest.channel,
        install_root.to_str().ok_or_else(|| {
            SurgeError::Config(format!("Install root is not valid UTF-8: {}", install_root.display()))
        })?,
    )?;
    manager.set_artifact_retention_policy(manifest.effective_install_artifact_cache_policy())?;

    let Some(mut update) = manager.check_for_updates().await? else {
        repair_runtime_metadata(manifest, install_root)?;
        logline::success(&format!(
            "Repaired runtime metadata for '{}' v{} ({}).",
            manifest.app_id, manifest.version, manifest.channel
        ));
        return Ok(true);
    };

    if update.latest_version != manifest.version {
        logline::warn(&format!(
            "Release channel '{}' latest is v{}, but this installer targets v{}; falling back to exact full-package setup.",
            manifest.channel, update.latest_version, manifest.version
        ));
        return Ok(false);
    }

    logline::info(&format!(
        "Updating existing '{}' install from v{} to v{} via {} ({}).",
        manifest.app_id,
        current_version,
        manifest.version,
        update_strategy_label(update.apply_strategy),
        crate::formatters::format_bytes(u64::try_from(update.download_size.max(0)).unwrap_or(0))
    ));
    if let Some(reason) = &update.fallback_reason {
        logline::warn(&format!("Delta update unavailable; using full package: {reason}"));
    }

    apply_installer_runtime_environment(&mut update, manifest);

    let update_progress = Mutex::new(ProgressReporter::new("Applying update..."));
    manager
        .download_and_apply(
            &update,
            Some(|progress: ProgressInfo| {
                let mut reporter = update_progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(message) = reporter.observe_update(&progress) {
                    logline::subtle(&message);
                }
            }),
        )
        .await?;

    repair_runtime_metadata(manifest, install_root)?;
    logline::success(&format!(
        "Updated '{}' to v{} ({}).",
        manifest.app_id, manifest.version, manifest.channel
    ));
    Ok(true)
}

fn update_strategy_label(strategy: ApplyStrategy) -> &'static str {
    match strategy {
        ApplyStrategy::Full => "full package",
        ApplyStrategy::Delta => "delta update",
    }
}

fn read_installed_runtime_manifest(active_app_dir: &Path) -> Result<Option<InstalledRuntimeManifest>> {
    let path = active_app_dir.join(core_install::RUNTIME_MANIFEST_RELATIVE_PATH);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)?;
    let manifest = serde_yaml::from_slice(&bytes)
        .map_err(|e| SurgeError::Config(format!("Failed to parse runtime manifest '{}': {e}", path.display())))?;
    Ok(Some(manifest))
}

fn runtime_metadata_matches(existing: &InstalledRuntimeManifest, manifest: &InstallerManifest) -> bool {
    existing.channel.trim() == manifest.channel.trim()
        && existing.provider.trim() == manifest.storage.provider.trim()
        && existing.bucket.trim() == manifest.storage.bucket.trim()
        && existing.region.trim() == manifest.storage.region.trim()
        && existing.endpoint.trim() == manifest.storage.endpoint.trim()
}

fn repair_runtime_metadata(manifest: &InstallerManifest, install_root: &Path) -> Result<()> {
    let active_app_dir = install_root.join("app");
    let profile = InstallProfile::from_installer_manifest(manifest, &manifest.runtime.shortcuts);
    let runtime_manifest = core_install::RuntimeManifestMetadata::new(
        &manifest.version,
        &manifest.channel,
        &manifest.storage.provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &manifest.storage.endpoint,
    );
    core_install::write_runtime_manifest(&active_app_dir, &profile, &runtime_manifest)?;
    Ok(())
}

fn context_from_installer_manifest(manifest: &InstallerManifest) -> Result<Context> {
    let storage = build_storage_config_from_installer_manifest(manifest)?;
    let provider = storage.provider.unwrap_or(StorageProvider::Filesystem);
    let ctx = Context::new();
    ctx.set_storage(
        provider,
        &storage.bucket,
        &storage.region,
        &storage.access_key,
        &storage.secret_key,
        &storage.endpoint,
    );
    ctx.set_storage_prefix(&storage.prefix);
    Ok(ctx)
}

fn apply_installer_runtime_environment(update: &mut UpdateInfo, manifest: &InstallerManifest) {
    for release in &mut update.apply_releases {
        release.environment.extend(manifest.runtime.environment.clone());
    }
    for release in &mut update.available_releases {
        if release.version == update.latest_version {
            release.environment.extend(manifest.runtime.environment.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use surge_core::config::installer::{
        InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
    };
    use surge_core::config::manifest::CacheManifestConfig;
    use surge_core::releases::manifest::ReleaseEntry;
    use surge_core::update::manager::{ApplyStrategy, UpdateInfo};

    use super::apply_installer_runtime_environment;

    #[test]
    fn installer_runtime_environment_merges_into_update_release_environment() {
        let mut manifest = minimal_manifest();
        manifest
            .runtime
            .environment
            .insert("DISPLAY".to_string(), ":0".to_string());
        manifest
            .runtime
            .environment
            .insert("XAUTHORITY".to_string(), "/run/user/1000/gdm/Xauthority".to_string());
        manifest.runtime.environment.insert(
            "DBUS_SESSION_BUS_ADDRESS".to_string(),
            "unix:path=/run/user/1000/bus".to_string(),
        );

        let mut update = UpdateInfo {
            available_releases: vec![release("1.0.5"), release("1.1.0")],
            latest_version: "1.1.0".to_string(),
            delta_available: true,
            download_size: 42,
            apply_releases: vec![release("1.0.5"), release("1.1.0")],
            apply_strategy: ApplyStrategy::Delta,
            fallback_reason: None,
        };
        update.apply_releases[0]
            .environment
            .insert("APP_SETTING".to_string(), "base".to_string());
        update.apply_releases[1]
            .environment
            .insert("DISPLAY".to_string(), ":99".to_string());
        update.apply_releases[1]
            .environment
            .insert("APP_SETTING".to_string(), "latest".to_string());
        update.available_releases[0]
            .environment
            .insert("APP_SETTING".to_string(), "older".to_string());
        update.available_releases[1]
            .environment
            .insert("DISPLAY".to_string(), ":99".to_string());
        update.available_releases[1]
            .environment
            .insert("APP_SETTING".to_string(), "available-latest".to_string());

        apply_installer_runtime_environment(&mut update, &manifest);

        assert_eq!(
            update.apply_releases[0]
                .environment
                .get("APP_SETTING")
                .map(String::as_str),
            Some("base")
        );
        assert_eq!(
            update.apply_releases[0]
                .environment
                .get("DBUS_SESSION_BUS_ADDRESS")
                .map(String::as_str),
            Some("unix:path=/run/user/1000/bus")
        );
        assert_eq!(
            update.apply_releases[1].environment.get("DISPLAY").map(String::as_str),
            Some(":0")
        );
        assert_eq!(
            update.apply_releases[1]
                .environment
                .get("APP_SETTING")
                .map(String::as_str),
            Some("latest")
        );
        assert_eq!(
            update.apply_releases[1]
                .environment
                .get("XAUTHORITY")
                .map(String::as_str),
            Some("/run/user/1000/gdm/Xauthority")
        );
        assert_eq!(
            update.available_releases[0]
                .environment
                .get("APP_SETTING")
                .map(String::as_str),
            Some("older")
        );
        assert_eq!(update.available_releases[0].environment.get("DISPLAY"), None);
        assert_eq!(
            update.available_releases[1]
                .environment
                .get("DISPLAY")
                .map(String::as_str),
            Some(":0")
        );
        assert_eq!(
            update.available_releases[1]
                .environment
                .get("APP_SETTING")
                .map(String::as_str),
            Some("available-latest")
        );
    }

    fn minimal_manifest() -> InstallerManifest {
        InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: InstallerUi::Console,
            installer_type: "online".to_string(),
            app_id: "demo".to_string(),
            rid: "linux-x64".to_string(),
            version: "1.1.0".to_string(),
            channel: "test".to_string(),
            generated_utc: "2026-05-14T10:00:00Z".to_string(),
            headless_default_if_no_display: true,
            release_index_key: "releases.yml.zst".to_string(),
            storage: InstallerStorage {
                provider: "filesystem".to_string(),
                bucket: String::new(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release: InstallerRelease {
                full_filename: "demo-1.1.0-linux-x64-full.tar.zst".to_string(),
                full_sha256: String::new(),
                delta_filename: String::new(),
                delta_algorithm: String::new(),
                delta_patch_format: String::new(),
                delta_compression: String::new(),
            },
            runtime: InstallerRuntime {
                name: "Demo".to_string(),
                main_exe: "demo".to_string(),
                install_directory: "demo".to_string(),
                supervisor_id: "demo-supervisor".to_string(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: BTreeMap::new(),
            },
            cache: CacheManifestConfig::default(),
        }
    }

    fn release(version: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["test".to_string()],
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
            is_genesis: false,
            full_filename: format!("demo-{version}-linux-x64-full.tar.zst"),
            full_size: 1,
            full_sha256: "sha".to_string(),
            full_compression_level: 3,
            full_zstd_workers: 1,
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: "2026-05-14T10:00:00Z".to_string(),
            release_notes: String::new(),
            name: "Demo".to_string(),
            main_exe: "demo".to_string(),
            install_directory: "demo".to_string(),
            supervisor_id: "demo-supervisor".to_string(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: BTreeMap::new(),
        }
    }
}
