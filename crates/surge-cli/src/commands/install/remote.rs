#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use super::{
    ArchiveAcquisition, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BTreeMap, BufReader,
    CacheFetchOutcome, Command, InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
    Instant, Path, PathBuf, RELEASES_FILE_COMPRESSED, ReleaseEntry, ReleaseIndex, Result, Serialize, Stdio,
    StorageBackend, SurgeError, SurgeManifest, cache_path_for_key, compare_versions, core_install,
    download_release_archive, fetch_or_reuse_file, host_can_build_installer_locally, infer_os_from_rid, logline,
    make_progress_bar, make_spinner, release_install_profile, release_runtime_manifest_metadata, shell_single_quote,
};
use crate::commands::pack;
use serde::Deserialize;

pub(super) fn ensure_supported_tailscale_rid(rid: &str) -> Result<()> {
    match infer_os_from_rid(rid) {
        Some(os) if os == "linux" => Ok(()),
        Some(os) => Err(SurgeError::Config(format!(
            "Tailscale install currently supports Linux targets only. Selected RID '{rid}' targets '{os}'."
        ))),
        None => {
            logline::warn(&format!(
                "Unable to infer OS from selected RID '{rid}'. Tailscale install supports Linux targets only; verify this RID is Linux-compatible."
            ));
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct RemoteLaunchEnvironment {
    pub(super) display: Option<String>,
    pub(super) xauthority: Option<String>,
    pub(super) dbus_session_bus_address: Option<String>,
    pub(super) wayland_display: Option<String>,
    pub(super) xdg_runtime_dir: Option<String>,
}

impl RemoteLaunchEnvironment {
    pub(super) fn has_graphical_session(&self) -> bool {
        self.display.is_some() || self.wayland_display.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteInstallerMode {
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteTailscaleTransferStrategy {
    AppCopy,
    StagedInstallerCache,
    Installer { prefer_published: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteHostInstallerAvailability {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteTailscaleOperation {
    Stage,
    Install,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RemoteTailscaleCachedState {
    None,
    AppCopyPayload,
    InstallerCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VerifiedRemoteStage {
    AppCopyPayload,
    InstallerCache,
}

impl VerifiedRemoteStage {
    pub(super) fn description(self) -> &'static str {
        match self {
            Self::AppCopyPayload => "staged app payload",
            Self::InstallerCache => "staged installer cache",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RemoteTailscaleTransferInputs {
    pub(super) host_installer_availability: RemoteHostInstallerAvailability,
    pub(super) installer_mode: RemoteInstallerMode,
    pub(super) operation: RemoteTailscaleOperation,
    pub(super) cached_state: RemoteTailscaleCachedState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RemoteInstallState {
    pub(super) version: String,
    pub(super) channel: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct RemoteStagedPayloadIdentity {
    pub(super) app_id: String,
    pub(super) version: String,
    pub(super) channel: String,
    pub(super) rid: String,
    pub(super) full_filename: String,
    pub(super) full_sha256: String,
    pub(super) install_directory: String,
    pub(super) supervisor_id: String,
    pub(super) storage_provider: String,
    pub(super) storage_bucket: String,
    pub(super) storage_region: String,
    pub(super) storage_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RemotePublishedInstallerPlan {
    pub(super) candidate_keys: Vec<String>,
    pub(super) blockers: Vec<String>,
}

pub(super) fn resolve_tailscale_targets(node: &str, node_user: Option<&str>) -> Result<(String, String)> {
    let node = node.trim();
    if node.is_empty() {
        return Err(SurgeError::Config(
            "Tailscale node cannot be empty. Provide --node <node>.".to_string(),
        ));
    }

    if let Some((user_part, host_part)) = node.split_once('@') {
        if user_part.trim().is_empty() || host_part.trim().is_empty() {
            return Err(SurgeError::Config(format!(
                "Invalid --node value '{node}'. Expected '<node>' or '<user>@<node>'."
            )));
        }
        return Ok((node.to_string(), host_part.to_string()));
    }

    if let Some(user) = node_user.map(str::trim).filter(|value| !value.is_empty()) {
        Ok((format!("{user}@{node}"), node.to_string()))
    } else {
        Ok((node.to_string(), node.to_string()))
    }
}

fn remote_installer_extension_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "exe"
    } else {
        "bin"
    }
}

fn default_channel_for_remote_installer(manifest: &SurgeManifest, app_id: &str) -> Result<String> {
    let app = manifest
        .apps
        .iter()
        .find(|candidate| candidate.id == app_id)
        .ok_or_else(|| SurgeError::Config(format!("App '{app_id}' was not found in manifest")))?;
    Ok(app
        .channels
        .first()
        .cloned()
        .or_else(|| manifest.channels.first().map(|channel| channel.name.clone()))
        .unwrap_or_else(|| "stable".to_string()))
}

pub(super) fn plan_remote_published_installer(
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
    let default_channel = default_channel_for_remote_installer(manifest, app_id)?;
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
    let candidate_key =
        format!("installers/Setup-{rid}-{app_id}-{default_channel}-{desired_installer}.{installer_ext}");

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
    if channel != default_channel {
        blockers.push(format!(
            "published installers are currently bound to app default channel '{default_channel}', but install requested '{channel}'"
        ));
    }

    Ok(RemotePublishedInstallerPlan {
        candidate_keys: vec![candidate_key],
        blockers,
    })
}

pub(super) fn plan_remote_published_installer_without_manifest(
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
    }
}

pub(super) fn missing_remote_installer_error(
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

pub(super) fn published_installer_public_url(
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

pub(super) async fn try_prepare_published_installer_for_tailscale(
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
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    installer_mode: RemoteInstallerMode,
) -> Result<PathBuf> {
    let installer_manifest =
        build_remote_installer_manifest(app_id, release, channel, storage_config, launch_env, installer_mode);
    let mut installer_yaml = serde_yaml::to_string(&installer_manifest)
        .map_err(|e| SurgeError::Config(format!("Failed to serialize installer manifest: {e}")))?;
    if !installer_yaml.ends_with('\n') {
        installer_yaml.push('\n');
    }

    let payload_bytes = surge_core::installer_bundle::read_embedded_payload(published_installer_path)?;
    let launcher_bytes = surge_core::installer_bundle::read_launcher_stub(published_installer_path)?;

    let staging_dir =
        tempfile::tempdir().map_err(|e| SurgeError::Platform(format!("Failed to create staging directory: {e}")))?;
    let staging = staging_dir.path();
    surge_core::archive::extractor::extract_to(&payload_bytes, staging, None)?;
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

fn build_remote_runtime_environment(
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

pub(super) fn build_remote_installer_manifest(
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    installer_mode: RemoteInstallerMode,
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
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_installer_for_tailscale(
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

    let installer_manifest =
        build_remote_installer_manifest(app_id, release, channel, storage_config, launch_env, installer_mode);
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

pub(super) fn remote_install_root(home: &Path, app_id: &str, install_directory: &str) -> Result<PathBuf> {
    let name = if install_directory.trim().is_empty() {
        app_id.trim()
    } else {
        install_directory.trim()
    };
    if name.is_empty() {
        return Err(SurgeError::Config(
            "App id or install directory is required for remote install".to_string(),
        ));
    }

    let candidate = Path::new(name);
    if candidate.is_absolute() {
        Ok(candidate.to_path_buf())
    } else {
        Ok(home.join(".local/share").join(candidate))
    }
}

pub(super) fn remote_linux_shortcut_icon_path(
    staged_app_dir: &Path,
    remote_app_dir: &Path,
    app_id: &str,
    main_exe_name: &str,
    configured_icon: &str,
) -> PathBuf {
    let configured_icon = configured_icon.trim();
    if !configured_icon.is_empty() {
        let candidate = Path::new(configured_icon);
        return if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            remote_app_dir.join(candidate)
        };
    }

    let mut candidates = Vec::new();
    for stem in [main_exe_name.trim(), app_id.trim(), "icon", "logo"] {
        if stem.is_empty() {
            continue;
        }
        for ext in ["svg", "png", "xpm"] {
            candidates.push(PathBuf::from(format!("{stem}.{ext}")));
            candidates.push(Path::new(".surge").join(format!("{stem}.{ext}")));
        }
    }

    for candidate in candidates {
        if staged_app_dir.join(&candidate).is_file() {
            return remote_app_dir.join(candidate);
        }
    }

    remote_app_dir.join(main_exe_name)
}

fn stage_remote_linux_shortcuts(
    stage_root: &Path,
    rendered: &[surge_core::platform::shortcuts::LinuxShortcutFile],
) -> Result<()> {
    for shortcut in rendered {
        let target_dir = match shortcut.location {
            surge_core::config::manifest::ShortcutLocation::Desktop
            | surge_core::config::manifest::ShortcutLocation::StartMenu => {
                stage_root.join("shortcuts").join("applications")
            }
            surge_core::config::manifest::ShortcutLocation::Startup => stage_root.join("shortcuts").join("autostart"),
        };
        std::fs::create_dir_all(&target_dir)?;
        std::fs::write(target_dir.join(&shortcut.file_name), shortcut.content.as_bytes())?;
    }
    Ok(())
}

fn shell_export_lines(environment: &BTreeMap<String, String>) -> String {
    let mut lines = String::new();
    for (key, value) in environment {
        lines.push_str("export ");
        lines.push_str(key);
        lines.push('=');
        lines.push_str(&shell_single_quote(value));
        lines.push('\n');
    }
    lines
}

pub(super) fn build_remote_app_copy_activation_script(
    install_root: &Path,
    main_exe: &str,
    version: &str,
    environment: &BTreeMap<String, String>,
    persistent_assets: &[String],
    legacy_app_dir: Option<&Path>,
    no_start: bool,
) -> Result<String> {
    let install_root_quoted = shell_single_quote(&install_root.to_string_lossy());
    let main_exe_quoted = shell_single_quote(main_exe);
    let version_quoted = shell_single_quote(version);
    let exports = shell_export_lines(environment);
    let legacy_app_dir_quoted =
        legacy_app_dir.map_or_else(|| "''".to_string(), |path| shell_single_quote(&path.to_string_lossy()));
    let runtime_manifest_relative_path = core_install::RUNTIME_MANIFEST_RELATIVE_PATH;
    let legacy_runtime_manifest_relative_path = core_install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH;
    let persistent_asset_commands = persistent_assets
        .iter()
        .map(|asset| {
            core_install::validate_relative_persistent_asset_path(asset).map(|relative| {
                format!(
                    "  copy_persistent_asset {}\n\\\n",
                    shell_single_quote(&relative.to_string_lossy())
                )
            })
        })
        .collect::<Result<Vec<_>>>()?
        .join("");
    let persistent_asset_block = format!(
        "legacy_app_dir={legacy_app_dir_quoted}\n\\\n\
active_runtime_manifest=\"$active_app_dir/{runtime_manifest_relative_path}\"\n\\\n\
active_legacy_runtime_manifest=\"$active_app_dir/{legacy_runtime_manifest_relative_path}\"\n\\\n\
runtime_manifest_backup=\"$stage_dir/.surge-runtime-next.yml\"\n\\\n\
legacy_runtime_manifest_backup=\"$stage_dir/.surge-surge-next.yml\"\n\\\n\
\n\\\n\
copy_persistent_asset() {{\n\\\n\
  relative_path=\"$1\"\n\\\n\
  source=\"$persistent_source_dir/$relative_path\"\n\\\n\
  destination=\"$active_app_dir/$relative_path\"\n\\\n\
  if [ ! -e \"$source\" ]; then\n\\\n\
    return 0\n\\\n\
  fi\n\\\n\
  if [ -d \"$source\" ]; then\n\\\n\
    rm -rf \"$destination\"\n\\\n\
    mkdir -p \"$(dirname \"$destination\")\"\n\\\n\
    cp -a \"$source\" \"$destination\"\n\\\n\
  else\n\\\n\
    mkdir -p \"$(dirname \"$destination\")\"\n\\\n\
    if [ -d \"$destination\" ]; then\n\\\n\
      rm -rf \"$destination\"\n\\\n\
    fi\n\\\n\
    cp -a \"$source\" \"$destination\"\n\\\n\
  fi\n\\\n\
}}\n\\\n\
\n\\\n",
    );

    let mut script = format!(
        "set -eu\n\
install_root={install_root_quoted}\n\
stage_dir=\"$install_root/.surge-transfer-stage\"\n\
next_app_dir=\"$install_root/.surge-app-next\"\n\
active_app_dir=\"$install_root/app\"\n\
previous_app_dir=\"$install_root/.surge-app-prev\"\n\
applications_dir=\"$HOME/.local/share/applications\"\n\
autostart_dir=\"$HOME/.config/autostart\"\n\
main_exe={main_exe_quoted}\n\
version={version_quoted}\n\
{persistent_asset_block}\
\n\
kill_matching() {{\n\
  pattern=\"$1\"\n\
  if ! command -v pgrep >/dev/null 2>&1; then\n\
    return 0\n\
  fi\n\
  for pid in $(pgrep -u \"$(id -u)\" -f \"$pattern\" 2>/dev/null || true); do\n\
    case \"$pid\" in\n\
      \"$$\"|\"$PPID\")\n\
        continue\n\
        ;;\n\
    esac\n\
    kill \"$pid\" 2>/dev/null || true\n\
  done\n\
}}\n\
\n\
kill_matching \"$install_root/$main_exe\"\n\
kill_matching \"$install_root/app-\"\n\
kill_matching \"$install_root/app/\"\n\
rm -rf \"$next_app_dir\" \"$previous_app_dir\"\n\
if [ ! -d \"$stage_dir/app\" ]; then\n\
  echo \"Remote install stage is missing app payload\" >&2\n\
  exit 1\n\
fi\n\
mv \"$stage_dir/app\" \"$next_app_dir\"\n\
if [ -d \"$active_app_dir\" ]; then\n\
  mv \"$active_app_dir\" \"$previous_app_dir\"\n\
fi\n\
mv \"$next_app_dir\" \"$active_app_dir\"\n\
\n\
if [ -n \"${{legacy_app_dir:-}}\" ] && [ -d \"$legacy_app_dir\" ] && [ ! -d \"$previous_app_dir\" ]; then\n\
  persistent_source_dir=\"$legacy_app_dir\"\n\
elif [ -d \"$previous_app_dir\" ]; then\n\
  persistent_source_dir=\"$previous_app_dir\"\n\
else\n\
  persistent_source_dir=\"\"\n\
fi\n\
\n\
if [ -n \"${{persistent_source_dir:-}}\" ]; then\n\
  if [ -f \"$active_runtime_manifest\" ]; then\n\
    cp \"$active_runtime_manifest\" \"$runtime_manifest_backup\"\n\
  fi\n\
  if [ -f \"$active_legacy_runtime_manifest\" ]; then\n\
    cp \"$active_legacy_runtime_manifest\" \"$legacy_runtime_manifest_backup\"\n\
  fi\n\
{persistent_asset_commands}\
  if [ -f \"$runtime_manifest_backup\" ]; then\n\
    mkdir -p \"$(dirname \"$active_runtime_manifest\")\"\n\
    cp \"$runtime_manifest_backup\" \"$active_runtime_manifest\"\n\
  fi\n\
  if [ -f \"$legacy_runtime_manifest_backup\" ]; then\n\
    mkdir -p \"$(dirname \"$active_legacy_runtime_manifest\")\"\n\
    cp \"$legacy_runtime_manifest_backup\" \"$active_legacy_runtime_manifest\"\n\
  fi\n\
fi\n\
\n\
rm -rf \"$previous_app_dir\"\n\
\n\
for snapshot_dir in \"$install_root\"/app-[0-9]*; do\n\
  [ -d \"$snapshot_dir\" ] || continue\n\
  rm -rf \"$snapshot_dir\"\n\
done\n\
\n\
if [ -d \"$stage_dir/shortcuts/applications\" ]; then\n\
  mkdir -p \"$applications_dir\"\n\
  cp \"$stage_dir/shortcuts/applications/\"*.desktop \"$applications_dir/\" 2>/dev/null || true\n\
  chmod +x \"$applications_dir/\"*.desktop 2>/dev/null || true\n\
fi\n\
if [ -d \"$stage_dir/shortcuts/autostart\" ]; then\n\
  mkdir -p \"$autostart_dir\"\n\
  cp \"$stage_dir/shortcuts/autostart/\"*.desktop \"$autostart_dir/\" 2>/dev/null || true\n\
  chmod +x \"$autostart_dir/\"*.desktop 2>/dev/null || true\n\
fi\n\
rm -rf \"$stage_dir\"\n\
{exports}\
if [ ! -x \"$active_app_dir/$main_exe\" ] && [ -f \"$active_app_dir/$main_exe\" ]; then\n\
  chmod +x \"$active_app_dir/$main_exe\" || true\n\
fi\n"
    );

    if !no_start {
        script.push_str(
            "cd \"$install_root\"\n\
if [ -n \"$version\" ]; then\n\
  \"$active_app_dir/$main_exe\" --surge-installed \"$version\" >/dev/null 2>&1 || true\n\
  nohup \"$active_app_dir/$main_exe\" --surge-first-run \"$version\" >/dev/null 2>&1 &\n\
else\n\
  \"$active_app_dir/$main_exe\" --surge-installed >/dev/null 2>&1 || true\n\
  nohup \"$active_app_dir/$main_exe\" --surge-first-run >/dev/null 2>&1 &\n\
fi\n",
        );
    }

    Ok(script)
}

pub(super) fn select_latest_remote_legacy_app_dir<I, S>(install_root: &Path, entries: I) -> Option<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut best: Option<(String, PathBuf)> = None;

    for entry in entries {
        let name = entry.as_ref().trim();
        let Some(version) = remote_legacy_snapshot_version(name) else {
            continue;
        };

        if best
            .as_ref()
            .is_none_or(|(best_version, _)| compare_versions(version, best_version) == std::cmp::Ordering::Greater)
        {
            best = Some((version.to_string(), install_root.join(name)));
        }
    }

    best.map(|(_, path)| path)
}

fn remote_legacy_snapshot_version(dir_name: &str) -> Option<&str> {
    let version = dir_name.strip_prefix("app-")?;
    if version.is_empty() || !version.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(version)
}

async fn detect_remote_legacy_app_dir(ssh_node: &str, install_root: &Path) -> Result<Option<PathBuf>> {
    let probe = format!(
        "install_root={}; \
if [ -d \"$install_root\" ]; then \
  for path in \"$install_root\"/app-[0-9]*; do \
    if [ -d \"$path\" ]; then \
      basename \"$path\"; \
    fi; \
  done; \
fi",
        shell_single_quote(&install_root.to_string_lossy()),
    );
    let command = format!("sh -c {}", shell_single_quote(&probe));
    let output = run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await?;

    Ok(select_latest_remote_legacy_app_dir(
        install_root,
        output.lines().map(str::trim).filter(|line| !line.is_empty()),
    ))
}

async fn detect_remote_home_directory(ssh_node: &str) -> Result<PathBuf> {
    let command = format!("sh -c {}", shell_single_quote("printf %s \"$HOME\""));
    let output = run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await?;
    let home = output.trim();
    if home.is_empty() {
        return Err(SurgeError::Platform(format!(
            "Failed to determine HOME directory on remote node '{ssh_node}'"
        )));
    }
    Ok(PathBuf::from(home))
}

async fn stream_directory_to_tailscale_node_with_command(
    node: &str,
    local_dir: &Path,
    remote_command: &str,
) -> Result<()> {
    let ssh_command = format!("sh -lc {}", shell_single_quote(remote_command));
    let local_dir_str = local_dir.to_string_lossy().to_string();
    let mut tar_child = Command::new("tar")
        .args(["-C", local_dir_str.as_str(), "-cf", "-", "."])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to archive '{}' for transfer: {e}", local_dir.display())))?;
    let mut remote_child = Command::new("tailscale")
        .args(["ssh", node, ssh_command.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale ssh stream copy: {e}")))?;

    let mut tar_stdout = tar_child
        .stdout
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture local tar stdout".to_string()))?;
    let mut remote_stdin = remote_child
        .stdin
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale ssh stdin".to_string()))?;

    let transfer_message = format!("Streaming '{}' to '{node}'", local_dir.display());
    let transfer_spinner = make_spinner(&transfer_message);
    let transfer_result: Result<()> = async {
        let mut buffer = vec![0_u8; 128 * 1024];
        loop {
            let read_bytes = tar_stdout.read(&mut buffer).await.map_err(|e| {
                SurgeError::Platform(format!(
                    "Failed to read archived directory '{}' for transfer: {e}",
                    local_dir.display()
                ))
            })?;
            if read_bytes == 0 {
                break;
            }
            remote_stdin.write_all(&buffer[..read_bytes]).await.map_err(|e| {
                SurgeError::Platform(format!("Failed to stream '{}' to '{node}': {e}", local_dir.display()))
            })?;
            if let Some(spinner) = transfer_spinner.as_ref() {
                spinner.tick();
            }
        }
        remote_stdin.flush().await.map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to flush transfer stream to '{node}' for '{}': {e}",
                local_dir.display()
            ))
        })?;
        Ok(())
    }
    .await;
    drop(remote_stdin);

    if let Some(spinner) = &transfer_spinner {
        spinner.finish_and_clear();
    }

    if let Err(err) = transfer_result {
        let _ = tar_child.kill().await;
        let _ = remote_child.kill().await;
        return Err(err);
    }

    let tar_output = tar_child
        .wait_with_output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to wait for local tar process: {e}")))?;
    if !tar_output.status.success() {
        let stderr = String::from_utf8_lossy(&tar_output.stderr).trim().to_string();
        return Err(SurgeError::Platform(if stderr.is_empty() {
            format!("Command failed: tar -C '{}' -cf - .", local_dir.display())
        } else {
            format!("Command failed: tar -C '{}' -cf - .: {stderr}", local_dir.display())
        }));
    }

    let remote_output = remote_child
        .wait_with_output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale ssh stream copy: {e}")))?;
    if !remote_output.status.success() {
        let stderr = String::from_utf8_lossy(&remote_output.stderr).trim().to_string();
        return Err(SurgeError::Platform(if stderr.is_empty() {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>")
        } else {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>: {stderr}")
        }));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn deploy_remote_app_copy_for_tailscale(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    download_dir: &Path,
    ssh_target: &str,
    file_target: &str,
    app_id: &str,
    _rid: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    launch_env: &RemoteLaunchEnvironment,
    rid_candidates: &[String],
    full_filename: &str,
    no_start: bool,
    stage: bool,
) -> Result<()> {
    let remote_home = detect_remote_home_directory(ssh_target).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let active_app_dir = install_root.join("app");
    let runtime_environment = build_remote_runtime_environment(release, launch_env);
    let staged_payload_identity = remote_staged_payload_identity(app_id, release, channel, storage_config);
    let main_exe_name = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };

    if !stage
        && let Some(remote_staged_payload) = check_remote_staged_payload_identity(ssh_target, &install_root).await
        && remote_staged_payload == staged_payload_identity
    {
        logline::success(&format!(
            "Using pre-staged payload for '{app_id}' v{} on '{file_target}'.",
            release.version
        ));
        stop_remote_supervisor_if_running(ssh_target, &install_root, &release.supervisor_id).await?;
        let legacy_app_dir = if release.persistent_assets.is_empty() {
            None
        } else {
            detect_remote_legacy_app_dir(ssh_target, &install_root).await?
        };
        let activation_script = build_remote_app_copy_activation_script(
            &install_root,
            main_exe_name,
            &release.version,
            &runtime_environment,
            &release.persistent_assets,
            legacy_app_dir.as_deref(),
            no_start,
        )?;
        let ssh_command = format!("sh -lc {}", shell_single_quote(&activation_script));
        logline::info(&format!("Activating pre-staged install on '{file_target}'..."));
        return run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await;
    }

    std::fs::create_dir_all(download_dir)?;
    let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
    let acquisition =
        download_release_archive(backend, index, release, rid_candidates, full_filename, &local_package).await?;
    match acquisition {
        ArchiveAcquisition::ReusedLocal => {
            logline::success(&format!(
                "Using cached package '{}' at '{}'.",
                Path::new(full_filename).display(),
                local_package.display()
            ));
        }
        ArchiveAcquisition::Downloaded => {
            logline::success(&format!(
                "Downloaded '{}' to '{}'.",
                Path::new(full_filename).display(),
                local_package.display()
            ));
        }
        ArchiveAcquisition::Reconstructed => {
            logline::warn(&format!(
                "Direct full package '{}' missing in backend; reconstructed from retained release artifacts.",
                Path::new(full_filename).display()
            ));
        }
    }

    let staging_dir =
        tempfile::tempdir().map_err(|e| SurgeError::Platform(format!("Failed to create staging directory: {e}")))?;
    let stage_root = staging_dir.path().join("remote-stage");
    let stage_app_dir = stage_root.join("app");
    surge_core::archive::extractor::extract_file_to(&local_package, &stage_app_dir)?;

    let install_profile = release_install_profile(app_id, release);
    let runtime_manifest = release_runtime_manifest_metadata(release, channel, storage_config);
    core_install::write_runtime_manifest(&stage_app_dir, &install_profile, &runtime_manifest)?;
    std::fs::write(
        stage_root.join(".surge-staged-release.json"),
        serde_json::to_vec(&staged_payload_identity)
            .map_err(|e| SurgeError::Config(format!("Failed to serialize remote staged payload identity: {e}")))?,
    )?;
    let legacy_app_dir = if release.persistent_assets.is_empty() {
        None
    } else {
        detect_remote_legacy_app_dir(ssh_target, &install_root).await?
    };

    if !release.shortcuts.is_empty() {
        let icon_path =
            remote_linux_shortcut_icon_path(&stage_app_dir, &active_app_dir, app_id, main_exe_name, &release.icon);
        let rendered = surge_core::platform::shortcuts::render_linux_shortcut_files(
            app_id,
            release.display_name(app_id),
            &active_app_dir.join(main_exe_name),
            &icon_path,
            &release.supervisor_id,
            &install_root,
            &release.shortcuts,
            &runtime_environment,
        );
        stage_remote_linux_shortcuts(&stage_root, &rendered)?;
    }

    let transfer_command = format!(
        "command -v tar >/dev/null 2>&1 || {{ echo 'Remote host is missing tar' >&2; exit 1; }}; \
install_root={}; stage_dir=\"$install_root/.surge-transfer-stage\"; \
mkdir -p \"$install_root\"; rm -rf \"$stage_dir\"; mkdir -p \"$stage_dir\"; tar -C \"$stage_dir\" -xf -",
        shell_single_quote(&install_root.to_string_lossy())
    );
    logline::info(&format!(
        "Streaming extracted app payload to '{file_target}' for host-mismatch remote deployment..."
    ));
    stream_directory_to_tailscale_node_with_command(ssh_target, &stage_root, &transfer_command).await?;

    if stage {
        return Ok(());
    }

    stop_remote_supervisor_if_running(ssh_target, &install_root, &release.supervisor_id).await?;
    let activation_script = build_remote_app_copy_activation_script(
        &install_root,
        main_exe_name,
        &release.version,
        &runtime_environment,
        &release.persistent_assets,
        legacy_app_dir.as_deref(),
        no_start,
    )?;
    let ssh_command = format!("sh -lc {}", shell_single_quote(&activation_script));
    logline::info(&format!("Activating remote install on '{file_target}'..."));
    run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await
}

pub(super) fn select_remote_installer_mode(storage_config: &surge_core::context::StorageConfig) -> RemoteInstallerMode {
    match storage_config
        .provider
        .unwrap_or(surge_core::context::StorageProvider::Filesystem)
    {
        surge_core::context::StorageProvider::Filesystem => RemoteInstallerMode::Offline,
        surge_core::context::StorageProvider::S3
        | surge_core::context::StorageProvider::AzureBlob
        | surge_core::context::StorageProvider::Gcs
        | surge_core::context::StorageProvider::GitHubReleases => RemoteInstallerMode::Online,
    }
}

pub(super) fn select_remote_tailscale_transfer_strategy(
    inputs: RemoteTailscaleTransferInputs,
) -> RemoteTailscaleTransferStrategy {
    if inputs.operation == RemoteTailscaleOperation::Install
        && inputs.installer_mode == RemoteInstallerMode::Online
        && inputs.cached_state == RemoteTailscaleCachedState::InstallerCache
    {
        return RemoteTailscaleTransferStrategy::StagedInstallerCache;
    }

    if inputs.host_installer_availability == RemoteHostInstallerAvailability::Unavailable
        || matches!(inputs.installer_mode, RemoteInstallerMode::Offline)
            && (inputs.operation == RemoteTailscaleOperation::Stage
                || inputs.cached_state == RemoteTailscaleCachedState::AppCopyPayload)
    {
        RemoteTailscaleTransferStrategy::AppCopy
    } else {
        RemoteTailscaleTransferStrategy::Installer {
            prefer_published: inputs.operation == RemoteTailscaleOperation::Install,
        }
    }
}

pub(super) async fn stream_file_to_tailscale_node_with_command(
    node: &str,
    local_file: &Path,
    remote_command: &str,
) -> Result<()> {
    let ssh_command = format!("sh -lc {}", shell_single_quote(remote_command));
    let mut child = Command::new("tailscale")
        .args(["ssh", node, ssh_command.as_str()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale ssh stream copy: {e}")))?;

    let mut local_reader = tokio::fs::File::open(local_file)
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to open '{}' for transfer: {e}", local_file.display())))?;

    let transfer_total_bytes = tokio::fs::metadata(local_file).await.map_or(0, |meta| meta.len());
    let transfer_message = format!("Streaming '{}' to '{node}'", local_file.display());
    let transfer_bar = if transfer_total_bytes > 0 {
        make_progress_bar(&transfer_message, transfer_total_bytes)
    } else {
        make_spinner(&transfer_message)
    };
    let mut last_transfer_log = Instant::now();

    let mut child_stdin = child
        .stdin
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale ssh stdin".to_string()))?;

    let mut transferred_bytes = 0_u64;
    let mut buffer = vec![0_u8; 128 * 1024];
    loop {
        let read_bytes = local_reader.read(&mut buffer).await.map_err(|e| {
            SurgeError::Platform(format!("Failed to read '{}' for transfer: {e}", local_file.display()))
        })?;
        if read_bytes == 0 {
            break;
        }
        child_stdin.write_all(&buffer[..read_bytes]).await.map_err(|e| {
            SurgeError::Platform(format!("Failed to stream '{}' to '{node}': {e}", local_file.display()))
        })?;
        transferred_bytes = transferred_bytes.saturating_add(u64::try_from(read_bytes).unwrap_or(0));

        if let Some(bar) = transfer_bar.as_ref() {
            if transfer_total_bytes > 0 {
                bar.set_position(transferred_bytes);
            } else {
                bar.tick();
                bar.set_message(format!("{transfer_message} ({transferred_bytes} bytes transferred)"));
            }
        } else if last_transfer_log.elapsed() >= std::time::Duration::from_secs(5) {
            if transfer_total_bytes > 0 {
                let pct = (transferred_bytes as f64 / transfer_total_bytes as f64) * 100.0;
                logline::subtle(&format!(
                    "Streaming package to '{node}'... {transferred_bytes}/{transfer_total_bytes} bytes ({pct:.0}%)"
                ));
            } else {
                logline::subtle(&format!(
                    "Streaming package to '{node}'... {transferred_bytes} bytes transferred"
                ));
            }
            last_transfer_log = Instant::now();
        }
    }

    child_stdin.flush().await.map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to flush transfer stream to '{node}' for '{}': {e}",
            local_file.display()
        ))
    })?;
    drop(child_stdin);

    if let Some(bar) = &transfer_bar {
        bar.finish_and_clear();
    } else {
        logline::subtle(&format!(
            "Completed stream upload to '{node}' ({transferred_bytes} bytes)."
        ));
    }

    let finalize_spinner = make_spinner("Waiting for remote copy confirmation");
    if finalize_spinner.is_none() {
        logline::subtle("Waiting for remote copy confirmation...");
    }

    let output = child.wait_with_output().await;
    if let Some(spinner) = finalize_spinner {
        spinner.finish_and_clear();
    }
    let output =
        output.map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale ssh stream copy: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let msg = if stderr.is_empty() {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>")
        } else {
            format!("Command failed: tailscale ssh {node} sh -lc <stream-copy>: {stderr}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(())
}

pub(super) async fn check_remote_install_state(ssh_node: &str, install_dir: &str) -> Option<RemoteInstallState> {
    let probe = format!(
        r#"manifest="$HOME/.local/share/{}/app/.surge/runtime.yml";
if [ ! -f "$manifest" ]; then
  exit 0
fi
version="$(sed -n 's/^version:[[:space:]]*//p' "$manifest" | head -n1)"
channel="$(sed -n 's/^channel:[[:space:]]*//p' "$manifest" | head -n1)"
printf 'version=%s\nchannel=%s\n' "$version" "$channel""#,
        install_dir.replace('\'', ""),
    );
    let command = format!("sh -c {}", shell_single_quote(&probe));
    match run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await {
        Ok(output) => parse_remote_install_state(&output),
        Err(_) => None,
    }
}

pub(super) fn parse_remote_install_state(output: &str) -> Option<RemoteInstallState> {
    let mut version = None;
    let mut channel = None;

    for line in output.lines() {
        if let Some(value) = line.strip_prefix("version=") {
            let value = value.trim();
            if !value.is_empty() {
                version = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("channel=") {
            let value = value.trim();
            if !value.is_empty() {
                channel = Some(value.to_string());
            }
        }
    }

    version.map(|version| RemoteInstallState { version, channel })
}

pub(super) fn remote_staged_payload_identity(
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
) -> RemoteStagedPayloadIdentity {
    RemoteStagedPayloadIdentity {
        app_id: app_id.trim().to_string(),
        version: release.version.trim().to_string(),
        channel: channel.trim().to_string(),
        rid: release.rid.trim().to_string(),
        full_filename: release.full_filename.trim().to_string(),
        full_sha256: release.full_sha256.trim().to_string(),
        install_directory: release.install_directory.trim().to_string(),
        supervisor_id: release.supervisor_id.trim().to_string(),
        storage_provider: core_install::storage_provider_manifest_name(storage_config.provider).to_string(),
        storage_bucket: storage_config.bucket.trim().to_string(),
        storage_region: storage_config.region.trim().to_string(),
        storage_endpoint: storage_config.endpoint.trim().to_string(),
    }
}

pub(super) fn parse_remote_staged_payload_identity(output: &str) -> Option<RemoteStagedPayloadIdentity> {
    serde_json::from_str(output.trim()).ok()
}

async fn check_remote_staged_payload_identity(
    ssh_node: &str,
    install_root: &Path,
) -> Option<RemoteStagedPayloadIdentity> {
    let probe = format!(
        "cat {}/.surge-transfer-stage/.surge-staged-release.json 2>/dev/null",
        shell_single_quote(&install_root.to_string_lossy())
    );
    let command = format!("sh -c {}", shell_single_quote(&probe));
    match run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await {
        Ok(output) => parse_remote_staged_payload_identity(&output),
        Err(_) => None,
    }
}

async fn check_remote_staged_installer_identity(
    ssh_node: &str,
    install_root: &Path,
) -> Option<RemoteStagedPayloadIdentity> {
    let probe = format!(
        "cat {}/.surge-cache/staged-installer/.surge-staged-release.json 2>/dev/null",
        shell_single_quote(&install_root.to_string_lossy())
    );
    let command = format!("sh -c {}", shell_single_quote(&probe));
    match run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await {
        Ok(output) => parse_remote_staged_payload_identity(&output),
        Err(_) => None,
    }
}

pub(super) async fn remote_staged_payload_matches_release(
    ssh_node: &str,
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
) -> Result<bool> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let expected = remote_staged_payload_identity(app_id, release, channel, storage_config);
    Ok(check_remote_staged_payload_identity(ssh_node, &install_root).await == Some(expected))
}

pub(super) async fn remote_staged_installer_matches_release(
    ssh_node: &str,
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
) -> Result<bool> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let expected = remote_staged_payload_identity(app_id, release, channel, storage_config);
    Ok(check_remote_staged_installer_identity(ssh_node, &install_root).await == Some(expected))
}

pub(super) async fn verify_remote_stage_readiness(
    ssh_node: &str,
    file_target: &str,
    app_id: &str,
    selected_rid: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
) -> Result<VerifiedRemoteStage> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let expected = remote_staged_payload_identity(app_id, release, channel, storage_config);
    let app_copy_matches =
        check_remote_staged_payload_identity(ssh_node, &install_root).await == Some(expected.clone());
    let app_copy_ready = if app_copy_matches {
        remote_staged_app_copy_files_exist(ssh_node, &install_root).await?
    } else {
        false
    };
    let installer_cache_matches =
        check_remote_staged_installer_identity(ssh_node, &install_root).await == Some(expected.clone());
    let installer_cache_ready = if installer_cache_matches {
        remote_staged_installer_cache_files_exist(ssh_node, &install_root, release).await?
    } else {
        false
    };

    match select_remote_installer_mode(storage_config) {
        RemoteInstallerMode::Offline => {
            if app_copy_ready {
                return Ok(VerifiedRemoteStage::AppCopyPayload);
            }
        }
        RemoteInstallerMode::Online => {
            if installer_cache_ready {
                return Ok(VerifiedRemoteStage::InstallerCache);
            }
            if !host_can_build_installer_locally(selected_rid) && app_copy_ready {
                return Ok(VerifiedRemoteStage::AppCopyPayload);
            }
        }
    }

    let selected_rid = if selected_rid.trim().is_empty() {
        "<generic>"
    } else {
        selected_rid
    };
    if app_copy_matches || installer_cache_matches {
        return Err(SurgeError::NotFound(format!(
            "Node '{file_target}' has a staged marker for '{app_id}' v{} on channel '{channel}' (rid '{selected_rid}'), but the staged payload is incomplete or would not be reused by the next install from this host.",
            release.version
        )));
    }

    Err(SurgeError::NotFound(format!(
        "Node '{file_target}' is not staged for '{app_id}' v{} on channel '{channel}' (rid '{selected_rid}').",
        release.version
    )))
}

pub(super) async fn run_remote_staged_installer_setup(
    ssh_node: &str,
    file_target: &str,
    app_id: &str,
    release: &ReleaseEntry,
    no_start: bool,
) -> Result<()> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let setup_command = build_remote_staged_installer_setup_command(&install_root, no_start);
    let ssh_command = format!("sh -lc {}", shell_single_quote(&setup_command));
    logline::info(&format!(
        "Using pre-staged installer cache for '{app_id}' v{} on '{file_target}'.",
        release.version
    ));
    run_tailscale_streaming(&["ssh", ssh_node, ssh_command.as_str()], "remote").await
}

pub(super) async fn warn_if_remote_stage_cleanup_fails(ssh_node: &str, app_id: &str, release: &ReleaseEntry) {
    if let Err(error) = cleanup_remote_staged_payload(ssh_node, app_id, release).await {
        logline::warn(&format!("Could not remove stale remote staged payload: {error}"));
    }
}

pub(super) async fn cleanup_remote_staged_payload(ssh_node: &str, app_id: &str, release: &ReleaseEntry) -> Result<()> {
    let remote_home = detect_remote_home_directory(ssh_node).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let cleanup_command = build_remote_stage_cleanup_command(&install_root);
    let ssh_command = format!("sh -lc {}", shell_single_quote(&cleanup_command));
    run_tailscale_streaming(&["ssh", ssh_node, ssh_command.as_str()], "remote").await
}

async fn stop_remote_supervisor_if_running(ssh_node: &str, install_root: &Path, supervisor_id: &str) -> Result<()> {
    let Some(stop_command) = build_remote_stop_supervisor_command(install_root, supervisor_id) else {
        return Ok(());
    };

    logline::info(&format!(
        "Stopping remote supervisor '{}' before activation...",
        supervisor_id.trim()
    ));
    let ssh_command = format!("sh -lc {}", shell_single_quote(&stop_command));
    run_tailscale_streaming(&["ssh", ssh_node, ssh_command.as_str()], "remote").await
}

pub(super) fn build_remote_stage_cleanup_command(install_root: &Path) -> String {
    format!(
        "install_root={}; rm -rf \"$install_root/.surge-transfer-stage\"",
        shell_single_quote(&install_root.to_string_lossy())
    )
}

async fn remote_staged_app_copy_files_exist(ssh_node: &str, install_root: &Path) -> Result<bool> {
    let stage_root = install_root.join(".surge-transfer-stage");
    let marker = stage_root.join(".surge-staged-release.json");
    let app_dir = stage_root.join("app");
    remote_paths_exist(ssh_node, &[app_dir.as_path()], &[marker.as_path()]).await
}

async fn remote_staged_installer_cache_files_exist(
    ssh_node: &str,
    install_root: &Path,
    release: &ReleaseEntry,
) -> Result<bool> {
    let stage_dir = install_root.join(".surge-cache").join("staged-installer");
    let marker = stage_dir.join(".surge-staged-release.json");
    let installer_manifest = stage_dir.join("installer.yml");
    let surge_bin = stage_dir.join("surge");
    let artifact_cache_dir = install_root.join(".surge-cache").join("artifacts");
    let cached_package = cache_path_for_key(&artifact_cache_dir, release.full_filename.trim())?;
    remote_paths_exist(
        ssh_node,
        &[stage_dir.as_path()],
        &[
            marker.as_path(),
            installer_manifest.as_path(),
            surge_bin.as_path(),
            cached_package.as_path(),
        ],
    )
    .await
}

async fn remote_paths_exist(ssh_node: &str, required_dirs: &[&Path], required_files: &[&Path]) -> Result<bool> {
    let probe = build_remote_paths_exist_probe(required_dirs, required_files);
    let command = format!("sh -c {}", shell_single_quote(&probe));
    Ok(run_tailscale_capture(&["ssh", ssh_node, command.as_str()])
        .await?
        .trim()
        == "ready")
}

pub(super) fn build_remote_paths_exist_probe(required_dirs: &[&Path], required_files: &[&Path]) -> String {
    let mut checks = Vec::new();
    for path in required_dirs {
        checks.push(format!("[ -d {} ]", shell_single_quote(&path.to_string_lossy())));
    }
    for path in required_files {
        checks.push(format!("[ -f {} ]", shell_single_quote(&path.to_string_lossy())));
    }
    if checks.is_empty() {
        "printf 'ready'".to_string()
    } else {
        format!(
            "if {}; then printf 'ready'; else printf 'missing'; fi",
            checks.join(" && ")
        )
    }
}

pub(super) fn build_remote_stop_supervisor_command(install_root: &Path, supervisor_id: &str) -> Option<String> {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return None;
    }

    Some(format!(
        "install_root={}; supervisor_id={}; pid_file=\"$install_root/.surge-supervisor-$supervisor_id.pid\"; \
if [ ! -d \"$install_root\" ] || [ ! -f \"$pid_file\" ]; then exit 0; fi; \
pid=\"$(tr -d '[:space:]' < \"$pid_file\")\"; \
case \"$pid\" in ''|*[!0-9]*) echo \"Invalid PID in supervisor PID file: $pid_file\" >&2; exit 1 ;; esac; \
kill \"$pid\"; \
i=0; \
while [ -f \"$pid_file\" ]; do \
  if [ \"$i\" -ge 200 ]; then echo \"Timed out waiting for supervisor '$supervisor_id' to exit\" >&2; exit 1; fi; \
  sleep 0.1; \
  i=$((i + 1)); \
done",
        shell_single_quote(&install_root.to_string_lossy()),
        shell_single_quote(supervisor_id)
    ))
}

pub(super) fn build_remote_staged_installer_setup_command(install_root: &Path, no_start: bool) -> String {
    let no_start_flag = if no_start { " --no-start" } else { "" };
    format!(
        "install_root={}; \
stage_dir=\"$install_root/.surge-cache/staged-installer\"; \
surge_bin=\"$stage_dir/surge\"; \
if [ ! -d \"$stage_dir\" ] || [ ! -f \"$stage_dir/installer.yml\" ] || [ ! -f \"$surge_bin\" ]; then \
  echo \"Remote staged installer cache is missing required files\" >&2; \
  exit 1; \
fi; \
chmod +x \"$surge_bin\" || true; \
cd \"$stage_dir\"; \
\"$surge_bin\" setup \"$stage_dir\"{no_start_flag}",
        shell_single_quote(&install_root.to_string_lossy())
    )
}

pub(super) async fn detect_remote_launch_environment(ssh_node: &str) -> RemoteLaunchEnvironment {
    let probe = remote_launch_environment_probe();
    let command = format!("sh -c {}", shell_single_quote(probe));
    match run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await {
        Ok(output) => parse_remote_launch_environment(&output),
        Err(error) => {
            logline::warn(&format!(
                "Could not detect remote graphical session environment on '{ssh_node}': {error}"
            ));
            RemoteLaunchEnvironment::default()
        }
    }
}

pub(super) fn remote_launch_environment_probe() -> &'static str {
    r#"if command -v systemctl >/dev/null 2>&1; then
  systemctl --user show-environment 2>/dev/null || true
fi
if command -v pgrep >/dev/null 2>&1; then
  for name in gnome-shell gnome-session-binary plasmashell kwin_wayland kwin_x11 startplasma-wayland startplasma-x11 Xwayland Xorg sway weston; do
    for pid in $(pgrep -u "$(id -u)" -x "$name" 2>/dev/null); do
      tr '\0' '\n' <"/proc/$pid/environ" 2>/dev/null | grep -E '^(DISPLAY|XAUTHORITY|DBUS_SESSION_BUS_ADDRESS|WAYLAND_DISPLAY|XDG_RUNTIME_DIR)=' || true
    done
  done
fi"#
}

pub(super) fn parse_remote_launch_environment(output: &str) -> RemoteLaunchEnvironment {
    let mut launch_env = RemoteLaunchEnvironment::default();

    for line in output.lines() {
        if let Some(value) = line.strip_prefix("DISPLAY=") {
            let value = value.trim();
            if !value.is_empty() {
                launch_env.display = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("XAUTHORITY=") {
            let value = value.trim();
            if !value.is_empty() {
                launch_env.xauthority = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("DBUS_SESSION_BUS_ADDRESS=") {
            let value = value.trim();
            if !value.is_empty() {
                launch_env.dbus_session_bus_address = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("WAYLAND_DISPLAY=") {
            let value = value.trim();
            if !value.is_empty() {
                launch_env.wayland_display = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("XDG_RUNTIME_DIR=") {
            let value = value.trim();
            if !value.is_empty() {
                launch_env.xdg_runtime_dir = Some(value.to_string());
            }
        }
    }

    launch_env
}

pub(super) fn remote_install_matches(
    remote_state: Option<&RemoteInstallState>,
    expected_version: &str,
    expected_channel: &str,
) -> bool {
    remote_state.is_some_and(|state| {
        state.version.trim() == expected_version
            && state
                .channel
                .as_deref()
                .is_some_and(|channel| channel.trim() == expected_channel)
    })
}

pub(super) fn should_skip_remote_install(install_matches: bool, force: bool) -> bool {
    install_matches && !force
}

async fn run_tailscale_capture(args: &[&str]) -> Result<String> {
    let output = Command::new("tailscale")
        .args(args)
        .output()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale command: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let cmd = format!("tailscale {}", args.join(" "));
        let msg = if stderr.is_empty() {
            format!("Command failed: {cmd}")
        } else {
            format!("Command failed: {cmd}: {stderr}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub(super) async fn run_tailscale_streaming(args: &[&str], prefix: &str) -> Result<()> {
    let mut child = Command::new("tailscale")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| SurgeError::Platform(format!("Failed to run tailscale command: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale stdout".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| SurgeError::Platform("Failed to capture tailscale stderr".to_string()))?;

    let stdout_task = tokio::spawn(relay_tailscale_output(stdout, prefix.to_string()));
    let stderr_task = tokio::spawn(relay_tailscale_output(stderr, prefix.to_string()));

    let status = child
        .wait()
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to wait for tailscale command: {e}")))?;
    let stdout = stdout_task
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stdout: {e}")))?
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stdout: {e}")))?;
    let stderr = stderr_task
        .await
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stderr: {e}")))?
        .map_err(|e| SurgeError::Platform(format!("Failed to read tailscale stderr: {e}")))?;

    if !status.success() {
        let cmd = format!("tailscale {}", args.join(" "));
        let message = stderr
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .or_else(|| stdout.lines().rev().find(|line| !line.trim().is_empty()));
        let msg = if let Some(message) = message {
            format!("Command failed: {cmd}: {}", message.trim())
        } else {
            format!("Command failed: {cmd}")
        };
        return Err(SurgeError::Platform(msg));
    }

    Ok(())
}

async fn relay_tailscale_output<R>(reader: R, prefix: String) -> std::io::Result<String>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut reader = BufReader::new(reader);
    let mut buffer = Vec::new();
    let mut captured = String::new();

    loop {
        buffer.clear();
        let read = reader.read_until(b'\n', &mut buffer).await?;
        if read == 0 {
            break;
        }

        let chunk = String::from_utf8_lossy(&buffer);
        let trimmed = chunk.trim();
        if !trimmed.is_empty() {
            logline::subtle(&format!("{prefix}: {trimmed}"));
        }
        captured.push_str(&chunk);
    }

    Ok(captured)
}
