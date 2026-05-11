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
use surge_core::update::manager::{ApplyStrategy, ProgressInfo, UpdateManager};

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

    let Some(update) = manager.check_for_updates().await? else {
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
