use super::types::{RemoteInstallerMode, RemoteLaunchEnvironment, RemotePublishedInstallerPlan};
use super::{
    CacheFetchOutcome, InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi, Path,
    PathBuf, RELEASES_FILE_COMPRESSED, ReleaseEntry, Result, StorageBackend, SurgeError, SurgeManifest,
    cache_path_for_key, core_install, fetch_or_reuse_file, logline, pack,
};
use std::collections::BTreeMap;
use surge_core::config::manifest::CacheManifestConfig;

fn remote_installer_extension_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "exe"
    } else {
        "bin"
    }
}

pub(crate) fn plan_remote_published_installer(
    manifest: &SurgeManifest,
    app_id: &str,
    rid: &str,
    channel: &str,
    release: &ReleaseEntry,
    installer_mode: RemoteInstallerMode,
) -> Result<RemotePublishedInstallerPlan> {
    let (_app, target) = manifest
        .find_app_with_target(app_id, rid)
        .ok_or_else(|| SurgeError::Config(format!("App '{app_id}' with RID '{rid}' not found in manifest")))?;
    let declared_installers = if release.installers.is_empty() {
        &target.installers
    } else {
        &release.installers
    };
    let desired_installer = match installer_mode {
        RemoteInstallerMode::Online => "online",
        RemoteInstallerMode::Offline => "offline",
    };
    let installer_ext = remote_installer_extension_for_rid(rid);
    let candidate_key = format!("installers/Setup-{rid}-{app_id}-{channel}-{desired_installer}.{installer_ext}");

    let mut blockers = Vec::new();
    if !declared_installers
        .iter()
        .any(|installer| installer == desired_installer)
    {
        let declared = if declared_installers.is_empty() {
            "none".to_string()
        } else {
            declared_installers.join(", ")
        };
        blockers.push(format!(
            "release does not declare a '{desired_installer}' installer (declared installers: {declared})"
        ));
    }

    Ok(RemotePublishedInstallerPlan {
        candidate_keys: vec![candidate_key],
        blockers,
        cache: Some(CacheManifestConfig::from_install_artifact_cache_policy(
            manifest.effective_install_artifact_cache_policy(),
        )),
    })
}

pub(crate) fn plan_remote_published_installer_without_manifest(
    app_id: &str,
    rid: &str,
    channel: &str,
    release: &ReleaseEntry,
    installer_mode: RemoteInstallerMode,
) -> RemotePublishedInstallerPlan {
    let desired_installer = match installer_mode {
        RemoteInstallerMode::Online => "online",
        RemoteInstallerMode::Offline => "offline",
    };
    let installer_ext = remote_installer_extension_for_rid(rid);
    let candidate_key = format!("installers/Setup-{rid}-{app_id}-{channel}-{desired_installer}.{installer_ext}");
    let declared_installers = &release.installers;

    let mut blockers = Vec::new();
    if !declared_installers
        .iter()
        .any(|installer| installer == desired_installer)
    {
        let declared = if declared_installers.is_empty() {
            "none".to_string()
        } else {
            declared_installers.join(", ")
        };
        blockers.push(format!(
            "release does not declare a '{desired_installer}' installer (declared installers: {declared})"
        ));
    }

    RemotePublishedInstallerPlan {
        candidate_keys: vec![candidate_key],
        blockers,
        cache: None,
    }
}

pub(crate) fn missing_remote_installer_error(
    rid: &str,
    plan: &RemotePublishedInstallerPlan,
    installer_mode: RemoteInstallerMode,
) -> SurgeError {
    let installer_label = match installer_mode {
        RemoteInstallerMode::Online => "online",
        RemoteInstallerMode::Offline => "offline",
    };
    let attempted = if plan.candidate_keys.is_empty() {
        "none".to_string()
    } else {
        plan.candidate_keys.join(", ")
    };
    let blockers = if plan.blockers.is_empty() {
        "published installer was not found in storage".to_string()
    } else {
        plan.blockers.join("; ")
    };
    let host_rid = surge_core::platform::detect::current_rid();
    SurgeError::NotFound(format!(
        "No published {installer_label} installer is available for remote deployment of RID '{rid}'. Tried keys: {attempted}. {blockers}. Local installer build is unavailable because target RID '{rid}' does not match current host RID '{host_rid}'. Publish the installer for this target or run the install command from a matching host."
    ))
}

pub(crate) fn published_installer_public_url(
    storage_config: &surge_core::context::StorageConfig,
    key: &str,
) -> Option<String> {
    match storage_config.provider {
        Some(surge_core::context::StorageProvider::AzureBlob)
            if !storage_config.endpoint.trim().is_empty() && !storage_config.bucket.trim().is_empty() =>
        {
            Some(format!(
                "{}/{}/{}",
                storage_config.endpoint.trim_end_matches('/'),
                storage_config.bucket.trim_matches('/'),
                key.trim_start_matches('/')
            ))
        }
        _ => None,
    }
}

async fn try_download_published_installer_via_public_url(
    installer_path: &Path,
    key: &str,
    storage_config: &surge_core::context::StorageConfig,
) -> Result<bool> {
    let Some(url) = published_installer_public_url(storage_config, key) else {
        return Ok(false);
    };

    let response = reqwest::get(&url)
        .await
        .map_err(|e| SurgeError::Storage(format!("Failed to fetch published installer URL '{url}': {e}")))?;
    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(false);
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(SurgeError::Storage(format!(
            "Published installer URL '{url}' failed (HTTP {status}): {body}"
        )));
    }

    let bytes = response
        .bytes()
        .await
        .map_err(|e| SurgeError::Storage(format!("Failed to read published installer URL '{url}': {e}")))?;
    if let Some(parent) = installer_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(installer_path, &bytes)?;
    Ok(true)
}

pub(crate) async fn try_prepare_published_installer_for_tailscale(
    backend: &dyn StorageBackend,
    download_dir: &Path,
    plan: &RemotePublishedInstallerPlan,
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    installer_mode: RemoteInstallerMode,
) -> Result<Option<PathBuf>> {
    if !plan.blockers.is_empty() {
        return Ok(None);
    }

    let installer_cache_root = download_dir.join("installers");
    std::fs::create_dir_all(&installer_cache_root)?;
    for key in &plan.candidate_keys {
        let installer_path = cache_path_for_key(&installer_cache_root, key)?;
        match fetch_or_reuse_file(backend, key, &installer_path, "", None).await {
            Ok(CacheFetchOutcome::ReusedLocal) => {
                logline::info(&format!(
                    "Using cached published installer '{key}' for remote deployment."
                ));
                return Ok(Some(customize_published_installer_for_tailscale(
                    &installer_path,
                    plan.cache.as_ref(),
                    app_id,
                    release,
                    channel,
                    storage_config,
                    launch_env,
                    installer_mode,
                )?));
            }
            Ok(CacheFetchOutcome::DownloadedFresh | CacheFetchOutcome::DownloadedAfterInvalidLocal) => {
                logline::info(&format!("Fetched published installer '{key}' for remote deployment."));
                return Ok(Some(customize_published_installer_for_tailscale(
                    &installer_path,
                    plan.cache.as_ref(),
                    app_id,
                    release,
                    channel,
                    storage_config,
                    launch_env,
                    installer_mode,
                )?));
            }
            Err(SurgeError::NotFound(_)) => {
                if try_download_published_installer_via_public_url(&installer_path, key, storage_config).await? {
                    let url = published_installer_public_url(storage_config, key).unwrap_or_default();
                    logline::info(&format!(
                        "Fetched published installer from public URL '{url}' for remote deployment."
                    ));
                    return Ok(Some(customize_published_installer_for_tailscale(
                        &installer_path,
                        plan.cache.as_ref(),
                        app_id,
                        release,
                        channel,
                        storage_config,
                        launch_env,
                        installer_mode,
                    )?));
                }
            }
            Err(err) => return Err(err),
        }
    }

    Ok(None)
}

fn customize_published_installer_for_tailscale(
    published_installer_path: &Path,
    cache: Option<&CacheManifestConfig>,
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    installer_mode: RemoteInstallerMode,
) -> Result<PathBuf> {
    let payload_bytes = surge_core::installer_bundle::read_embedded_payload(published_installer_path)?;
    let launcher_bytes = surge_core::installer_bundle::read_launcher_stub(published_installer_path)?;

    let staging_dir =
        tempfile::tempdir().map_err(|e| SurgeError::Platform(format!("Failed to create staging directory: {e}")))?;
    let staging = staging_dir.path();
    surge_core::archive::extractor::extract_to(&payload_bytes, staging, None)?;
    let cache = match cache {
        Some(cache) => *cache,
        None => read_embedded_installer_cache(staging)?,
    };
    let installer_manifest = build_remote_installer_manifest(
        app_id,
        release,
        channel,
        storage_config,
        launch_env,
        installer_mode,
        cache,
    );
    let mut installer_yaml = serde_yaml::to_string(&installer_manifest)
        .map_err(|e| SurgeError::Config(format!("Failed to serialize installer manifest: {e}")))?;
    if !installer_yaml.ends_with('\n') {
        installer_yaml.push('\n');
    }
    std::fs::write(staging.join("installer.yml"), installer_yaml.as_bytes())?;

    let payload_archive = tempfile::NamedTempFile::new()
        .map_err(|e| SurgeError::Platform(format!("Failed to create payload temp file: {e}")))?;
    let mut packer =
        surge_core::archive::packer::ArchivePacker::new(surge_core::config::constants::DEFAULT_ZSTD_LEVEL)?;
    packer.add_directory(staging, "")?;
    packer.finalize_to_file(payload_archive.path())?;

    let launcher_file = tempfile::NamedTempFile::new()
        .map_err(|e| SurgeError::Platform(format!("Failed to create launcher temp file: {e}")))?;
    std::fs::write(launcher_file.path(), launcher_bytes)?;

    let installer_filename = published_installer_path.file_name().map_or_else(
        || "surge-remote-installer.bin".to_string(),
        |name| name.to_string_lossy().to_string(),
    );
    let installer_path = staging.join(installer_filename);
    surge_core::installer_bundle::write_embedded_installer(
        launcher_file.path(),
        payload_archive.path(),
        &installer_path,
    )?;
    surge_core::platform::fs::make_executable(&installer_path)?;

    std::mem::forget(staging_dir);
    Ok(installer_path)
}

fn read_embedded_installer_cache(staging: &Path) -> Result<CacheManifestConfig> {
    let installer_manifest_path = staging.join("installer.yml");
    if !installer_manifest_path.is_file() {
        return Ok(CacheManifestConfig::default());
    }

    let bytes = std::fs::read(&installer_manifest_path)?;
    let manifest: InstallerManifest = serde_yaml::from_slice(&bytes).map_err(|e| {
        SurgeError::Config(format!(
            "Failed to parse embedded installer manifest '{}': {e}",
            installer_manifest_path.display()
        ))
    })?;
    Ok(manifest.cache)
}

pub(crate) fn build_remote_runtime_environment(
    release: &ReleaseEntry,
    launch_env: &RemoteLaunchEnvironment,
) -> BTreeMap<String, String> {
    let mut environment = release.environment.clone();
    if let Some(display) = launch_env.display.as_deref().filter(|value| !value.is_empty()) {
        environment.insert("DISPLAY".to_string(), display.to_string());
    }
    if let Some(xauthority) = launch_env.xauthority.as_deref().filter(|value| !value.is_empty()) {
        environment.insert("XAUTHORITY".to_string(), xauthority.to_string());
    }
    if let Some(dbus) = launch_env
        .dbus_session_bus_address
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        environment.insert("DBUS_SESSION_BUS_ADDRESS".to_string(), dbus.to_string());
    }
    if let Some(wayland_display) = launch_env.wayland_display.as_deref().filter(|value| !value.is_empty()) {
        environment.insert("WAYLAND_DISPLAY".to_string(), wayland_display.to_string());
    }
    if let Some(xdg_runtime_dir) = launch_env.xdg_runtime_dir.as_deref().filter(|value| !value.is_empty()) {
        environment.insert("XDG_RUNTIME_DIR".to_string(), xdg_runtime_dir.to_string());
    }
    environment
}

pub(crate) fn build_remote_installer_manifest(
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    installer_mode: RemoteInstallerMode,
    cache: CacheManifestConfig,
) -> InstallerManifest {
    let environment = build_remote_runtime_environment(release, launch_env);

    InstallerManifest {
        schema: 1,
        format: "surge-installer-v1".to_string(),
        ui: InstallerUi::Console,
        installer_type: match installer_mode {
            RemoteInstallerMode::Online => "online",
            RemoteInstallerMode::Offline => "offline",
        }
        .to_string(),
        app_id: app_id.to_string(),
        rid: release.rid.clone(),
        version: release.version.clone(),
        channel: channel.to_string(),
        generated_utc: release.created_utc.clone(),
        headless_default_if_no_display: true,
        release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
        storage: InstallerStorage {
            provider: core_install::storage_provider_manifest_name(storage_config.provider).to_string(),
            bucket: storage_config.bucket.clone(),
            region: storage_config.region.clone(),
            endpoint: storage_config.endpoint.clone(),
            prefix: storage_config.prefix.clone(),
        },
        release: InstallerRelease {
            full_filename: release.full_filename.clone(),
            full_sha256: release.full_sha256.clone(),
            delta_filename: String::new(),
            delta_algorithm: String::new(),
            delta_patch_format: String::new(),
            delta_compression: String::new(),
        },
        runtime: InstallerRuntime {
            name: release.display_name(app_id).to_string(),
            main_exe: release.main_exe.clone(),
            install_directory: release.install_directory.clone(),
            supervisor_id: release.supervisor_id.clone(),
            icon: release.icon.clone(),
            shortcuts: release.shortcuts.clone(),
            persistent_assets: release.persistent_assets.clone(),
            installers: release.installers.clone(),
            environment,
        },
        cache,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_installer_for_tailscale(
    manifest: Option<&SurgeManifest>,
    app_id: &str,
    rid: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    full_package_path: Option<&Path>,
    launch_env: &RemoteLaunchEnvironment,
    installer_mode: RemoteInstallerMode,
) -> Result<std::path::PathBuf> {
    let target_icon = manifest
        .and_then(|manifest| {
            manifest
                .find_app_with_target(app_id, rid)
                .map(|(_, target)| target.icon.trim().to_string())
        })
        .filter(|icon| !icon.is_empty());

    let cache = manifest.map_or_else(CacheManifestConfig::default, |manifest| {
        CacheManifestConfig::from_install_artifact_cache_policy(manifest.effective_install_artifact_cache_policy())
    });
    let installer_manifest = build_remote_installer_manifest(
        app_id,
        release,
        channel,
        storage_config,
        launch_env,
        installer_mode,
        cache,
    );
    let installer_yaml = serde_yaml::to_string(&installer_manifest)
        .map_err(|e| SurgeError::Config(format!("Failed to serialize installer manifest: {e}")))?;

    let staging_dir =
        tempfile::tempdir().map_err(|e| SurgeError::Platform(format!("Failed to create staging directory: {e}")))?;
    let staging = staging_dir.path();

    std::fs::write(staging.join("installer.yml"), installer_yaml.as_bytes())?;

    let surge_binary = pack::find_surge_binary_for_rid(rid)?;
    let surge_name = pack::surge_binary_name_for_rid(rid);
    std::fs::copy(&surge_binary, staging.join(surge_name))?;

    if let Some(full_package_path) = full_package_path {
        let full_filename = full_package_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .ok_or_else(|| SurgeError::Config("Full package path has no filename".to_string()))?;
        let payload_dir = staging.join("payload");
        std::fs::create_dir_all(&payload_dir)?;
        std::fs::copy(full_package_path, payload_dir.join(&full_filename))?;
    }

    let icon = target_icon
        .as_deref()
        .or_else(|| (!release.icon.trim().is_empty()).then_some(release.icon.trim()))
        .unwrap_or("");
    if !icon.is_empty() {
        let icon_base_dir = full_package_path
            .and_then(Path::parent)
            .unwrap_or_else(|| Path::new("."));
        let icon_source = icon_base_dir.join(icon);
        if icon_source.is_file() {
            let assets_dir = staging.join("assets");
            std::fs::create_dir_all(&assets_dir)?;
            if let Some(filename) = icon_source.file_name() {
                std::fs::copy(&icon_source, assets_dir.join(filename))?;
            }
        }
    }

    let payload_archive = tempfile::NamedTempFile::new()
        .map_err(|e| SurgeError::Platform(format!("Failed to create payload temp file: {e}")))?;
    let mut packer =
        surge_core::archive::packer::ArchivePacker::new(surge_core::config::constants::DEFAULT_ZSTD_LEVEL)?;
    packer.add_directory(staging, "")?;
    packer.finalize_to_file(payload_archive.path())?;

    let launcher = pack::find_installer_launcher_for_rid(rid, None)?;
    let installer_path = staging_dir.path().join("surge-offline-installer");
    surge_core::installer_bundle::write_embedded_installer(&launcher, payload_archive.path(), &installer_path)?;
    surge_core::platform::fs::make_executable(&installer_path)?;

    std::mem::forget(staging_dir);

    Ok(installer_path)
}
