#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

mod compatibility;
mod local;
mod profile;
mod progress;
mod releases;
mod remote;
mod resolution;
mod runtime;
mod selection;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use crate::logline;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

pub(crate) use self::progress::{make_progress_bar, make_spinner, shell_single_quote};
pub(crate) use self::resolution::selected_install_manifest_path;
pub(crate) use self::runtime::{
    auto_start_after_install, host_can_build_installer_locally, install_package_locally, release_install_profile,
    release_runtime_manifest_metadata, stop_running_supervisor,
};

use self::compatibility::{CompatibilityInstallTarget, run_platform_compatibility_preflight};
use self::local::install_selected_release_locally;
use self::profile::{
    build_rid_candidates, derive_base_rid, detect_local_profile, warn_if_local_rid_looks_incompatible,
};
use self::releases::{ArchiveAcquisition, download_release_archive, fetch_release_index, select_release};
use self::remote::{
    ensure_supported_tailscale_rid, install_release_via_tailscale, resolve_tailscale_targets,
    verify_remote_stage_readiness,
};
use self::resolution::{
    build_storage_config_with_overrides, build_storage_config_without_manifest, load_install_manifest_if_available,
    resolve_install_app_id_without_manifest, resolve_tailscale_rid_without_manifest,
};
use self::selection::{
    collect_available_channels, infer_os_from_rid, prompt_install_channel, prompt_install_selection,
    require_interactive_manifest, resolve_install_channel, resolve_install_channel_without_manifest,
    should_prompt_install_selection,
};
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::installer::{
    InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install};
use surge_core::releases::artifact_cache::{CacheFetchOutcome, cache_path_for_key, fetch_or_reuse_file};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageOverrides<'a> {
    pub provider: Option<&'a str>,
    pub bucket: Option<&'a str>,
    pub region: Option<&'a str>,
    pub endpoint: Option<&'a str>,
    pub prefix: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InstallBehavior {
    pub plan_only: bool,
    pub no_start: bool,
    pub force: bool,
    pub platform_mismatch: PlatformMismatchPolicy,
    pub mode: InstallMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlatformMismatchPolicy {
    #[default]
    Reject,
    Allow,
}

impl PlatformMismatchPolicy {
    fn allows_mismatch(self) -> bool {
        matches!(self, Self::Allow)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InstallMode {
    #[default]
    Install,
    StageOnly,
    VerifyStage,
}

impl InstallMode {
    fn is_stage(self) -> bool {
        matches!(self, Self::StageOnly)
    }

    fn is_verify_stage(self) -> bool {
        matches!(self, Self::VerifyStage)
    }
}

enum InstallTarget {
    Local,
    Tailscale { ssh_target: String, file_target: String },
}

pub async fn execute(
    manifest_path: &Path,
    application_manifest_path: &Path,
    node: Option<&str>,
    node_user: Option<&str>,
    app_id: Option<&str>,
    channel: Option<&str>,
    rid: Option<&str>,
    version: Option<&str>,
    behavior: InstallBehavior,
    download_dir: &Path,
    overrides: StorageOverrides<'_>,
) -> Result<()> {
    let selected_manifest_path = selected_install_manifest_path(application_manifest_path, manifest_path);
    let manifest = load_install_manifest_if_available(selected_manifest_path)?;
    let interactive_wizard = manifest.is_some() && should_prompt_install_selection();
    let interactive_selection = if interactive_wizard {
        Some(prompt_install_selection(
            require_interactive_manifest(manifest.as_ref())?,
            app_id,
            rid,
        )?)
    } else {
        None
    };
    let selected_os = interactive_selection.as_ref().map(|selection| selection.os.clone());
    let explicit_channel = channel.map(str::trim).filter(|value| !value.is_empty());

    let install_target = match node.map(str::trim).filter(|value| !value.is_empty()) {
        Some(node) => {
            let (ssh_target, file_target) = resolve_tailscale_targets(node, node_user)?;
            InstallTarget::Tailscale {
                ssh_target,
                file_target,
            }
        }
        None => InstallTarget::Local,
    };

    let explicit_app_id = if let Some(selection) = &interactive_selection {
        Some(selection.app_id.clone())
    } else if let Some(manifest) = manifest.as_ref() {
        Some(super::resolve_app_id_with_rid_hint(manifest, app_id, rid)?)
    } else {
        app_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    };
    let selected_rid_input = interactive_selection
        .as_ref()
        .map(|selection| selection.rid.as_str())
        .or_else(|| rid.map(str::trim).filter(|value| !value.is_empty()));
    let mut storage_config = if let Some(manifest) = manifest.as_ref() {
        let app_id = explicit_app_id
            .as_deref()
            .ok_or_else(|| SurgeError::Config("Install app id could not be resolved".to_string()))?;
        build_storage_config_with_overrides(manifest, selected_manifest_path, app_id, overrides)?
    } else {
        build_storage_config_without_manifest(selected_manifest_path, explicit_app_id.as_deref(), overrides)?
    };
    let mut backend = storage::create_storage_backend(&storage_config)?;
    logline::info(&format!(
        "Fetching release index '{RELEASES_FILE_COMPRESSED}' from storage backend..."
    ));
    let index_fetch_started = Instant::now();
    let (mut index, mut index_found) = fetch_release_index(&*backend).await?;
    if manifest.is_none()
        && !index_found
        && storage_config.prefix.trim().is_empty()
        && let Some(app_id) = explicit_app_id.as_deref().filter(|value| !value.is_empty())
    {
        let mut prefixed_storage_config = storage_config.clone();
        prefixed_storage_config.prefix = app_id.to_string();
        let prefixed_backend = storage::create_storage_backend(&prefixed_storage_config)?;
        logline::info(&format!(
            "Release index was not found at storage root; retrying with derived app-scoped prefix '{app_id}'."
        ));
        let (prefixed_index, prefixed_found) = fetch_release_index(&*prefixed_backend).await?;
        if prefixed_found {
            storage_config = prefixed_storage_config;
            backend = prefixed_backend;
            index = prefixed_index;
            index_found = true;
        }
    }
    let index_fetch_elapsed_ms = index_fetch_started.elapsed().as_millis();
    if index_found {
        logline::info(&format!(
            "Fetched release index in {index_fetch_elapsed_ms}ms ({} release entries).",
            index.releases.len()
        ));
    } else {
        logline::warn(&format!(
            "Release index '{RELEASES_FILE_COMPRESSED}' was not found ({index_fetch_elapsed_ms}ms)."
        ));
    }

    let (app_id, app_id_note) = if manifest.is_some() {
        (
            explicit_app_id.ok_or_else(|| SurgeError::Config("Install app id could not be resolved".to_string()))?,
            None,
        )
    } else {
        resolve_install_app_id_without_manifest(explicit_app_id, &index)?
    };
    if let Some(note) = app_id_note {
        logline::info(&note);
    }
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }

    let (rid_candidates, profile, rid_note) = match &install_target {
        InstallTarget::Local => {
            let detected = detect_local_profile();
            if let Some(requested_rid) = selected_rid_input {
                warn_if_local_rid_looks_incompatible(requested_rid, &detected);
                (vec![requested_rid.to_string()], Some(detected), None)
            } else {
                let base_rid = derive_base_rid(&detected).ok_or_else(|| {
                    SurgeError::Platform(format!(
                        "Unable to map profile to a RID (os='{}', arch='{}'). Use --rid to override.",
                        detected.os, detected.arch
                    ))
                })?;
                (
                    build_rid_candidates(&base_rid, detected.has_nvidia_gpu()),
                    Some(detected),
                    None,
                )
            }
        }
        InstallTarget::Tailscale { .. } => {
            let (selected_rid, rid_resolution_note) = if let Some(requested_rid) = selected_rid_input {
                (requested_rid.to_string(), None)
            } else if let Some(manifest) = manifest.as_ref() {
                (super::resolve_rid(manifest, &app_id, None)?, None)
            } else {
                resolve_tailscale_rid_without_manifest(selected_rid_input, &index)?
            };
            ensure_supported_tailscale_rid(&selected_rid)?;
            (vec![selected_rid], None, rid_resolution_note)
        }
    };
    if let Some(note) = rid_note {
        logline::info(&note);
    }

    let resolved_channel = if interactive_wizard {
        prompt_install_channel(
            require_interactive_manifest(manifest.as_ref())?,
            &index,
            &app_id,
            explicit_channel,
        )?
    } else if let Some(manifest) = manifest.as_ref() {
        resolve_install_channel(manifest, &index, &app_id, explicit_channel)?
    } else {
        resolve_install_channel_without_manifest(&index, explicit_channel)?
    };
    if let Some(note) = &resolved_channel.note {
        logline::info(note);
    }
    let channel = resolved_channel.name;

    let release = select_release(
        &index.releases,
        &channel,
        version,
        &rid_candidates,
        selected_os.as_deref(),
    )
    .ok_or_else(|| {
        let version_suffix = version.map_or_else(String::new, |v| format!(" and version '{v}'"));
        let available_channels = collect_available_channels(&index.releases);
        let channel_hint = if available_channels.is_empty() {
            " Release index contains no channel metadata.".to_string()
        } else {
            format!(" Available channels: {}.", available_channels.join(", "))
        };
        let os_suffix = selected_os
            .as_ref()
            .map(|os| format!(" and OS '{os}'"))
            .unwrap_or_default();
        SurgeError::NotFound(format!(
            "No release found on channel '{channel}' for RID candidates [{}]{version_suffix}{os_suffix}.{channel_hint}",
            rid_candidates.join(", "),
        ))
    })?;

    let selected_rid = if release.rid.is_empty() {
        "<generic>".to_string()
    } else {
        release.rid.clone()
    };

    if let Some(profile) = profile {
        match &install_target {
            InstallTarget::Local => logline::info(&format!(
                "Local profile: os={}, arch={}, gpu={}",
                profile.os, profile.arch, profile.gpu
            )),
            InstallTarget::Tailscale { ssh_target, .. } => logline::info(&format!(
                "Remote profile for {ssh_target}: os={}, arch={}, gpu={}",
                profile.os, profile.arch, profile.gpu
            )),
        }
    }
    logline::info(&format!("RID candidates: {}", rid_candidates.join(", ")));
    logline::success(&format!(
        "Selected release: app={} version={} rid={} channels={} full_package={}",
        app_id,
        release.version,
        selected_rid,
        if release.channels.is_empty() {
            "-".to_string()
        } else {
            release.channels.join(",")
        },
        release.full_filename
    ));

    let full_filename = release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::NotFound(format!(
            "Release {} ({selected_rid}) has no full package filename",
            release.version
        )));
    }

    let compatibility_rid = if release.rid.trim().is_empty() {
        rid_candidates.first().map(String::as_str).unwrap_or_default()
    } else {
        release.rid.as_str()
    };
    if let Some(compatibility) = manifest
        .as_ref()
        .and_then(|manifest| manifest.find_target(&app_id, compatibility_rid))
        .and_then(|target| target.compatibility)
        .filter(|compatibility| !compatibility.is_empty())
    {
        let compatibility_target = match &install_target {
            InstallTarget::Local => CompatibilityInstallTarget::Local,
            InstallTarget::Tailscale {
                ssh_target,
                file_target,
            } => CompatibilityInstallTarget::Tailscale {
                ssh_target,
                file_target,
            },
        };
        run_platform_compatibility_preflight(
            compatibility_target,
            compatibility_rid,
            &compatibility,
            behavior.platform_mismatch.allows_mismatch(),
        )
        .await?;
    }

    if behavior.mode.is_verify_stage() {
        match &install_target {
            InstallTarget::Local => {
                return Err(SurgeError::Config(
                    "--verify-stage requires 'tailscale' install method".to_string(),
                ));
            }
            InstallTarget::Tailscale {
                ssh_target,
                file_target,
            } => {
                let verified_stage = verify_remote_stage_readiness(
                    ssh_target,
                    file_target,
                    &app_id,
                    &selected_rid,
                    release,
                    &channel,
                    &storage_config,
                )
                .await?;
                logline::success(&format!(
                    "Verified {} is ready for '{}' v{} on '{}'.",
                    verified_stage.description(),
                    app_id,
                    release.version,
                    file_target
                ));
                return Ok(());
            }
        }
    }

    if behavior.plan_only {
        match &install_target {
            InstallTarget::Local => {
                logline::warn("Plan only mode: no download performed. Remove --plan-only to fetch the package.");
                return Ok(());
            }
            InstallTarget::Tailscale {
                ssh_target,
                file_target,
            } => {
                install_release_via_tailscale(
                    manifest.as_ref(),
                    &*backend,
                    &index,
                    download_dir,
                    ssh_target,
                    file_target,
                    &app_id,
                    &selected_rid,
                    &rid_candidates,
                    release,
                    &channel,
                    &storage_config,
                    full_filename,
                    behavior,
                )
                .await?;
                logline::warn(&format!(
                    "Plan only mode: no transfer performed. Remove --plan-only to apply the selected plan to {file_target}."
                ));
                return Ok(());
            }
        }
    }

    match &install_target {
        InstallTarget::Local => {
            install_selected_release_locally(
                &*backend,
                &index,
                download_dir,
                &app_id,
                release,
                &channel,
                &rid_candidates,
                full_filename,
                &storage_config,
                behavior.no_start,
            )
            .await?;
        }
        InstallTarget::Tailscale {
            ssh_target,
            file_target,
        } => {
            install_release_via_tailscale(
                manifest.as_ref(),
                &*backend,
                &index,
                download_dir,
                ssh_target,
                file_target,
                &app_id,
                &selected_rid,
                &rid_candidates,
                release,
                &channel,
                &storage_config,
                full_filename,
                behavior,
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap, clippy::similar_names)]

    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use super::profile::{
        RuntimeProfile, build_rid_candidates, derive_base_rid, local_rid_incompatibility_warnings, parse_rid_signature,
    };
    use super::releases::{ArchiveAcquisition, download_release_archive, select_release};
    use super::remote::{
        RemoteConvergenceAction, RemoteHostInstallerAvailability, RemoteInstallState, RemoteInstallerMode,
        RemoteLaunchEnvironment, RemotePublishedInstallerPlan, RemoteTailscaleCachedState, RemoteTailscaleOperation,
        RemoteTailscaleTransferInputs, RemoteTailscaleTransferStrategy, build_remote_app_copy_activation_script,
        build_remote_installer_manifest, build_remote_paths_exist_probe, build_remote_process_verification_probe,
        build_remote_runtime_start_command, build_remote_stage_cleanup_command,
        build_remote_staged_installer_setup_command, build_remote_stop_supervisor_command,
        missing_remote_installer_error, parse_remote_install_state, parse_remote_launch_environment,
        parse_remote_staged_payload_identity, plan_remote_convergence, plan_remote_published_installer,
        plan_remote_published_installer_without_manifest, published_installer_public_url, remote_install_matches,
        remote_launch_environment_probe, remote_staged_payload_identity, select_latest_remote_legacy_app_dir,
        select_remote_installer_mode, select_remote_tailscale_transfer_strategy,
        select_remote_tailscale_transfer_strategy_for_convergence, should_skip_remote_install,
        try_prepare_published_installer_for_tailscale,
    };
    use super::resolution::{
        build_storage_config_without_manifest, resolve_install_app_id_without_manifest,
        resolve_tailscale_rid_without_manifest,
    };
    use super::selection::{
        AppInstallTargetOption, collect_available_channels, collect_install_channel_options,
        collect_target_options_for_app, format_target_option_label, infer_os_from_rid, resolve_install_channel,
        resolve_install_target_selection,
    };
    use super::*;
    use surge_core::archive::extractor::read_entry;
    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::config::manifest::{
        CacheManifestConfig, InstallArtifactCachePolicy, InstallArtifactCacheRetention, ShortcutLocation, SurgeManifest,
    };
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::installer_bundle::read_embedded_payload;
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{DeltaArtifact, ReleaseIndex, compress_release_index};
    use surge_core::storage::filesystem::FilesystemBackend;

    fn release(version: &str, channel: &str, rid: &str, full: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec![channel.to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: full.to_string(),
            full_size: 1,
            full_sha256: "x".to_string(),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: String::new(),
            install_directory: String::new(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: vec![ShortcutLocation::Desktop],
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: BTreeMap::new(),
        }
    }

    fn storage_config(bucket: &str) -> surge_core::context::StorageConfig {
        surge_core::context::StorageConfig {
            provider: Some(surge_core::context::StorageProvider::Filesystem),
            bucket: bucket.to_string(),
            ..surge_core::context::StorageConfig::default()
        }
    }

    fn remote_state(version: &str, channel: &str, storage: &surge_core::context::StorageConfig) -> RemoteInstallState {
        RemoteInstallState {
            app_id: Some("demo".to_string()),
            version: version.to_string(),
            active_executable_exists: true,
            channel: Some(channel.to_string()),
            storage_provider: Some(core_install::storage_provider_manifest_name(storage.provider).to_string()),
            storage_bucket: Some(storage.bucket.clone()),
            storage_region: Some(storage.region.clone()),
            storage_endpoint: Some(storage.endpoint.clone()),
        }
    }

    fn remote_manifest(app_id: &str, rid: &str, channels: &[&str], installers: &[&str]) -> SurgeManifest {
        let mut channels_yaml = String::new();
        for channel in channels {
            channels_yaml.push_str("      - ");
            channels_yaml.push_str(channel);
            channels_yaml.push('\n');
        }

        let mut installers_yaml = String::new();
        for installer in installers {
            installers_yaml.push_str("          - ");
            installers_yaml.push_str(installer);
            installers_yaml.push('\n');
        }

        let yaml = format!(
            "schema: 1\napps:\n  - id: {app_id}\n    channels:\n{channels_yaml}    targets:\n      - rid: {rid}\n        installers:\n{installers_yaml}"
        );
        serde_yaml::from_str(&yaml).expect("manifest should parse")
    }

    fn latest_full_cache_policy() -> CacheManifestConfig {
        CacheManifestConfig::from_install_artifact_cache_policy(InstallArtifactCachePolicy {
            retention: InstallArtifactCacheRetention::LatestFull,
            keep_full_count: 1,
        })
    }

    fn create_published_installer(dir: &Path, installer_name: &str, manifest: &InstallerManifest) -> PathBuf {
        let launcher = dir.join("surge-installer");
        std::fs::write(&launcher, b"launcher-bytes").expect("launcher should be written");

        let staging = dir.join("staging");
        std::fs::create_dir_all(&staging).expect("staging dir should be created");
        let installer_yaml = serde_yaml::to_string(manifest).expect("installer manifest should serialize");
        std::fs::write(staging.join("installer.yml"), installer_yaml).expect("installer manifest should be written");
        std::fs::write(staging.join("surge"), b"binary").expect("surge binary placeholder should be written");

        let payload_archive = dir.join("payload.tar.zst");
        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer.add_directory(&staging, "").expect("staging dir should be added");
        packer
            .finalize_to_file(&payload_archive)
            .expect("payload archive should be written");

        let installer = dir.join(installer_name);
        surge_core::installer_bundle::write_embedded_installer(&launcher, &payload_archive, &installer)
            .expect("installer should be created");
        installer
    }

    #[test]
    fn parse_remote_launch_environment_reads_graphical_session_vars() {
        let launch_env = parse_remote_launch_environment(
            "DISPLAY=:0\nXAUTHORITY=/run/user/1000/gdm/Xauthority\nDBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus\nWAYLAND_DISPLAY=wayland-0\nXDG_RUNTIME_DIR=/run/user/1000\n",
        );

        assert_eq!(launch_env.display.as_deref(), Some(":0"));
        assert_eq!(launch_env.xauthority.as_deref(), Some("/run/user/1000/gdm/Xauthority"));
        assert_eq!(
            launch_env.dbus_session_bus_address.as_deref(),
            Some("unix:path=/run/user/1000/bus")
        );
        assert_eq!(launch_env.wayland_display.as_deref(), Some("wayland-0"));
        assert_eq!(launch_env.xdg_runtime_dir.as_deref(), Some("/run/user/1000"));
        assert!(launch_env.has_graphical_session());
    }

    #[test]
    fn remote_launch_environment_probe_checks_systemd_and_session_processes() {
        let probe = remote_launch_environment_probe();

        assert!(probe.contains("systemctl --user show-environment"));
        assert!(probe.contains("gnome-shell"));
        assert!(probe.contains("Xwayland"));
        assert!(probe.contains("DISPLAY|XAUTHORITY|DBUS_SESSION_BUS_ADDRESS|WAYLAND_DISPLAY|XDG_RUNTIME_DIR"));
    }

    #[test]
    fn build_remote_installer_manifest_includes_remote_launch_environment() {
        let release = release("1.2.3", "stable", "linux-x64", "demo.tar.zst");
        let launch_env = RemoteLaunchEnvironment {
            display: Some(":0".to_string()),
            xauthority: Some("/run/user/1000/gdm/Xauthority".to_string()),
            dbus_session_bus_address: Some("unix:path=/run/user/1000/bus".to_string()),
            wayland_display: Some("wayland-0".to_string()),
            xdg_runtime_dir: Some("/run/user/1000".to_string()),
        };

        let manifest = build_remote_installer_manifest(
            "demoapp",
            &release,
            "stable",
            &storage_config("/tmp/releases"),
            &launch_env,
            RemoteInstallerMode::Online,
            CacheManifestConfig::default(),
        );

        assert_eq!(
            manifest.runtime.environment.get("DISPLAY").map(String::as_str),
            Some(":0")
        );
        assert_eq!(
            manifest.runtime.environment.get("XAUTHORITY").map(String::as_str),
            Some("/run/user/1000/gdm/Xauthority")
        );
        assert_eq!(
            manifest
                .runtime
                .environment
                .get("DBUS_SESSION_BUS_ADDRESS")
                .map(String::as_str),
            Some("unix:path=/run/user/1000/bus")
        );
        assert_eq!(
            manifest.runtime.environment.get("WAYLAND_DISPLAY").map(String::as_str),
            Some("wayland-0")
        );
        assert_eq!(
            manifest.runtime.environment.get("XDG_RUNTIME_DIR").map(String::as_str),
            Some("/run/user/1000")
        );
        assert_eq!(manifest.installer_type, "online");
    }

    #[test]
    fn build_storage_config_without_manifest_reads_generic_storage_env_overrides() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let scope = tmp.path().join(".surge").join("missing-application.yml");
        let env_path = tmp.path().join(".env.surge");
        std::fs::write(
            &env_path,
            "SURGE_STORAGE_PROVIDER=filesystem\nSURGE_STORAGE_BUCKET=/srv/releases\nSURGE_STORAGE_PREFIX=edge/demo\n",
        )
        .expect("env file should be written");
        crate::envfile::load_storage_env_files(&scope, &[env_path]).expect("env file should load");

        let config = build_storage_config_without_manifest(&scope, None, StorageOverrides::default())
            .expect("manifestless storage config should build");

        assert_eq!(config.provider, Some(surge_core::context::StorageProvider::Filesystem));
        assert_eq!(config.bucket, "/srv/releases");
        assert_eq!(config.prefix, "edge/demo");
    }

    #[test]
    fn resolve_install_app_id_without_manifest_uses_release_index_value() {
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            ..ReleaseIndex::default()
        };

        let (app_id, note) =
            resolve_install_app_id_without_manifest(None, &index).expect("app id should resolve from index");

        assert_eq!(app_id, "demo");
        assert!(
            note.as_deref()
                .is_some_and(|value| value.contains("using app id 'demo' from the release index"))
        );
    }

    #[test]
    fn resolve_tailscale_rid_without_manifest_uses_single_index_rid() {
        let index = ReleaseIndex {
            releases: vec![release("1.2.3", "test", "linux-arm64", "demo.tar.zst")],
            ..ReleaseIndex::default()
        };

        let (rid, note) = resolve_tailscale_rid_without_manifest(None, &index).expect("rid should resolve from index");

        assert_eq!(rid, "linux-arm64");
        assert!(
            note.as_deref()
                .is_some_and(|value| value.contains("only RID 'linux-arm64' advertised by the release index"))
        );
    }

    #[test]
    fn plan_remote_published_installer_without_manifest_uses_requested_channel() {
        let mut entry = release("1.2.3", "test", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];

        let plan = plan_remote_published_installer_without_manifest(
            "demo",
            "linux-arm64",
            "test",
            &entry,
            RemoteInstallerMode::Online,
        );

        assert_eq!(
            plan.candidate_keys,
            vec!["installers/Setup-linux-arm64-demo-test-online.bin".to_string()]
        );
        assert!(plan.blockers.is_empty(), "unexpected blockers: {:?}", plan.blockers);
    }

    #[test]
    fn build_remote_app_copy_activation_script_exports_env_and_lifecycle_hooks() {
        let mut environment = BTreeMap::new();
        environment.insert("DISPLAY".to_string(), ":0".to_string());
        environment.insert(
            "DBUS_SESSION_BUS_ADDRESS".to_string(),
            "unix:path=/run/user/1000/bus".to_string(),
        );

        let script = build_remote_app_copy_activation_script(
            Path::new("/home/demo/.local/share/demo"),
            "demoapp",
            "1.2.3",
            &environment,
            &[],
            None,
            false,
        )
        .expect("script should build");

        assert!(script.contains("export DISPLAY=':0'"));
        assert!(script.contains("export DBUS_SESSION_BUS_ADDRESS='unix:path=/run/user/1000/bus'"));
        assert!(script.contains("--surge-installed \"$version\""));
        assert!(script.contains("--surge-first-run \"$version\""));
        assert!(script.contains("kill_matching \"$install_root/$main_exe\""));
        assert!(script.contains("kill_matching \"$install_root/app-\""));
    }

    #[cfg(unix)]
    #[test]
    fn build_remote_app_copy_activation_script_preserves_persistent_assets_and_prunes_snapshots() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let active_app_dir = install_root.join("app");
        let stage_app_dir = install_root.join(".surge-transfer-stage").join("app");
        let stale_snapshot = install_root.join("app-1.0.0");
        let older_snapshot = install_root.join("app-0.9.0");
        let persistent_assets = vec!["settings.json".to_string(), "state".to_string()];

        std::fs::create_dir_all(active_app_dir.join("state")).expect("state dir should exist");
        std::fs::create_dir_all(&stage_app_dir).expect("stage app dir should exist");
        std::fs::create_dir_all(&stale_snapshot).expect("stale snapshot should exist");
        std::fs::create_dir_all(&older_snapshot).expect("older snapshot should exist");
        std::fs::write(active_app_dir.join("settings.json"), "persisted settings").expect("settings should exist");
        std::fs::write(active_app_dir.join("state").join("cache.bin"), "persisted cache").expect("state should exist");
        std::fs::write(active_app_dir.join("old.txt"), "remove me").expect("old file should exist");
        std::fs::write(stage_app_dir.join("demoapp"), "#!/bin/sh\nexit 0\n").expect("demoapp should exist");
        std::fs::write(stage_app_dir.join("settings.json"), "packaged settings").expect("settings should exist");
        std::fs::create_dir_all(stage_app_dir.join("state")).expect("packaged state dir should exist");
        std::fs::write(stage_app_dir.join("state").join("cache.bin"), "packaged cache")
            .expect("packaged state should exist");
        std::fs::write(stage_app_dir.join("payload.txt"), "new payload").expect("payload should exist");

        let script = build_remote_app_copy_activation_script(
            &install_root,
            "demoapp",
            "1.2.3",
            &BTreeMap::new(),
            &persistent_assets,
            None,
            true,
        )
        .expect("script should build");

        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(script)
            .env("HOME", tmp.path())
            .status()
            .expect("script should run");
        assert!(status.success(), "script should succeed");

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("settings.json")).expect("settings should exist"),
            "persisted settings"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("state").join("cache.bin")).expect("state should exist"),
            "persisted cache"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload should exist"),
            "new payload"
        );
        assert!(
            !active_app_dir.join("old.txt").exists(),
            "undeclared assets should be removed"
        );
        assert!(!stale_snapshot.exists(), "stale snapshot should be pruned");
        assert!(!older_snapshot.exists(), "older snapshot should be pruned");
        assert!(
            !install_root.join(".surge-transfer-stage").exists(),
            "transfer stage should be cleaned up"
        );
        assert!(
            !install_root.join(".surge-app-prev").exists(),
            "previous app dir should be removed"
        );
    }

    #[test]
    fn build_remote_app_copy_activation_script_restores_runtime_metadata_after_persistent_copy() {
        let script = build_remote_app_copy_activation_script(
            Path::new("/home/demo/.local/share/demo"),
            "demoapp",
            "1.2.3",
            &BTreeMap::new(),
            &["settings.json".to_string(), ".surge".to_string()],
            Some(Path::new("/home/demo/.local/share/demo/app-1.2.2")),
            true,
        )
        .expect("script should build");

        assert!(script.contains("legacy_app_dir='/home/demo/.local/share/demo/app-1.2.2'"));
        assert!(script.contains("persistent_source_dir=\"$legacy_app_dir\""));
        assert!(script.contains("copy_persistent_asset 'settings.json'"));
        assert!(script.contains("copy_persistent_asset '.surge'"));
        assert!(script.contains("runtime_manifest_backup=\"$stage_dir/.surge-runtime-next.yml\""));
        assert!(script.contains("legacy_runtime_manifest_backup=\"$stage_dir/.surge-surge-next.yml\""));
        assert!(script.contains("cp \"$runtime_manifest_backup\" \"$active_runtime_manifest\""));
        assert!(script.contains("cp \"$legacy_runtime_manifest_backup\" \"$active_legacy_runtime_manifest\""));
    }

    #[test]
    fn select_latest_remote_legacy_app_dir_uses_semver_ordering() {
        let install_root = Path::new("/home/demo/.local/share/demo");
        let selected =
            select_latest_remote_legacy_app_dir(install_root, ["app-1.0.1-alpha.1", "app-1.0.1", "app-0.9.9"])
                .expect("legacy app dir should be selected");

        assert_eq!(selected, install_root.join("app-1.0.1"));
    }

    #[test]
    fn select_latest_remote_legacy_app_dir_ignores_non_version_directories() {
        let install_root = Path::new("/home/demo/.local/share/demo");

        let selected =
            select_latest_remote_legacy_app_dir(install_root, ["app-backup", "app-staging", ".surge-app-prev"]);

        assert_eq!(selected, None);
    }

    #[test]
    fn select_remote_installer_mode_prefers_online_for_remote_storage() {
        let filesystem = storage_config("/tmp/releases");
        assert_eq!(select_remote_installer_mode(&filesystem), RemoteInstallerMode::Offline);

        let mut azure = storage_config("bucket");
        azure.provider = Some(surge_core::context::StorageProvider::AzureBlob);
        assert_eq!(select_remote_installer_mode(&azure), RemoteInstallerMode::Online);
    }

    #[test]
    fn select_remote_tailscale_transfer_strategy_uses_app_copy_when_stage_reduces_transfer() {
        assert_eq!(
            select_remote_tailscale_transfer_strategy(RemoteTailscaleTransferInputs {
                host_installer_availability: RemoteHostInstallerAvailability::Available,
                installer_mode: RemoteInstallerMode::Offline,
                operation: RemoteTailscaleOperation::Stage,
                cached_state: RemoteTailscaleCachedState::None,
            }),
            RemoteTailscaleTransferStrategy::AppCopy
        );
        assert_eq!(
            select_remote_tailscale_transfer_strategy(RemoteTailscaleTransferInputs {
                host_installer_availability: RemoteHostInstallerAvailability::Available,
                installer_mode: RemoteInstallerMode::Offline,
                operation: RemoteTailscaleOperation::Install,
                cached_state: RemoteTailscaleCachedState::AppCopyPayload,
            }),
            RemoteTailscaleTransferStrategy::AppCopy
        );
        assert_eq!(
            select_remote_tailscale_transfer_strategy(RemoteTailscaleTransferInputs {
                host_installer_availability: RemoteHostInstallerAvailability::Unavailable,
                installer_mode: RemoteInstallerMode::Online,
                operation: RemoteTailscaleOperation::Install,
                cached_state: RemoteTailscaleCachedState::None,
            }),
            RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
        );
    }

    #[test]
    fn select_remote_tailscale_transfer_strategy_disables_published_installers_for_stage_mode() {
        assert_eq!(
            select_remote_tailscale_transfer_strategy(RemoteTailscaleTransferInputs {
                host_installer_availability: RemoteHostInstallerAvailability::Available,
                installer_mode: RemoteInstallerMode::Online,
                operation: RemoteTailscaleOperation::Stage,
                cached_state: RemoteTailscaleCachedState::None,
            }),
            RemoteTailscaleTransferStrategy::Installer {
                prefer_published: false
            }
        );
        assert_eq!(
            select_remote_tailscale_transfer_strategy(RemoteTailscaleTransferInputs {
                host_installer_availability: RemoteHostInstallerAvailability::Available,
                installer_mode: RemoteInstallerMode::Online,
                operation: RemoteTailscaleOperation::Install,
                cached_state: RemoteTailscaleCachedState::None,
            }),
            RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
        );
    }

    #[test]
    fn select_remote_tailscale_transfer_strategy_prefers_staged_online_installer_cache() {
        assert_eq!(
            select_remote_tailscale_transfer_strategy(RemoteTailscaleTransferInputs {
                host_installer_availability: RemoteHostInstallerAvailability::Available,
                installer_mode: RemoteInstallerMode::Online,
                operation: RemoteTailscaleOperation::Install,
                cached_state: RemoteTailscaleCachedState::InstallerCache,
            }),
            RemoteTailscaleTransferStrategy::StagedInstallerCache
        );
    }

    #[test]
    fn select_remote_tailscale_transfer_strategy_uses_online_installer_for_online_reinstall_repairs() {
        assert_eq!(
            select_remote_tailscale_transfer_strategy_for_convergence(
                RemoteTailscaleTransferInputs {
                    host_installer_availability: RemoteHostInstallerAvailability::Available,
                    installer_mode: RemoteInstallerMode::Online,
                    operation: RemoteTailscaleOperation::Install,
                    cached_state: RemoteTailscaleCachedState::None,
                },
                RemoteConvergenceAction::Reinstall
            ),
            RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
        );
        assert_eq!(
            select_remote_tailscale_transfer_strategy_for_convergence(
                RemoteTailscaleTransferInputs {
                    host_installer_availability: RemoteHostInstallerAvailability::Available,
                    installer_mode: RemoteInstallerMode::Online,
                    operation: RemoteTailscaleOperation::Install,
                    cached_state: RemoteTailscaleCachedState::InstallerCache,
                },
                RemoteConvergenceAction::Reinstall
            ),
            RemoteTailscaleTransferStrategy::StagedInstallerCache
        );
        assert_eq!(
            select_remote_tailscale_transfer_strategy_for_convergence(
                RemoteTailscaleTransferInputs {
                    host_installer_availability: RemoteHostInstallerAvailability::Available,
                    installer_mode: RemoteInstallerMode::Offline,
                    operation: RemoteTailscaleOperation::Install,
                    cached_state: RemoteTailscaleCachedState::None,
                },
                RemoteConvergenceAction::Reinstall
            ),
            RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
        );
        assert_eq!(
            select_remote_tailscale_transfer_strategy_for_convergence(
                RemoteTailscaleTransferInputs {
                    host_installer_availability: RemoteHostInstallerAvailability::Unavailable,
                    installer_mode: RemoteInstallerMode::Online,
                    operation: RemoteTailscaleOperation::Install,
                    cached_state: RemoteTailscaleCachedState::None,
                },
                RemoteConvergenceAction::Reinstall
            ),
            RemoteTailscaleTransferStrategy::Installer { prefer_published: true }
        );
    }

    #[test]
    fn build_remote_stage_cleanup_command_quotes_install_root() {
        let command = build_remote_stage_cleanup_command(Path::new("/home/demo/apps/customer's app"));

        assert_eq!(
            command,
            "install_root='/home/demo/apps/customer'\"'\"'s app'; rm -rf \"$install_root/.surge-transfer-stage\""
        );
    }

    #[test]
    fn build_remote_staged_installer_setup_command_quotes_install_root() {
        let command = build_remote_staged_installer_setup_command(Path::new("/home/demo/apps/customer's app"), true);

        assert!(command.contains("install_root='/home/demo/apps/customer'\"'\"'s app'"));
        assert!(command.contains("stage_dir=\"$install_root/.surge-cache/staged-installer\""));
        assert!(command.contains("\"$surge_bin\" setup \"$stage_dir\" --no-start"));
    }

    #[test]
    fn build_remote_paths_exist_probe_quotes_paths() {
        let dir = Path::new("/home/demo/apps/customer's app/.surge-transfer-stage/app");
        let file = Path::new("/home/demo/apps/customer's app/.surge-transfer-stage/.surge-staged-release.json");

        let probe = build_remote_paths_exist_probe(&[dir], &[file]);

        assert!(probe.contains("[ -d '/home/demo/apps/customer'\"'\"'s app/.surge-transfer-stage/app' ]"));
        assert!(probe.contains(
            "[ -f '/home/demo/apps/customer'\"'\"'s app/.surge-transfer-stage/.surge-staged-release.json' ]"
        ));
        assert!(probe.contains("printf 'ready'"));
        assert!(probe.contains("printf 'missing'"));
    }

    #[test]
    fn remote_staged_payload_identity_changes_when_channel_or_artifact_changes() {
        let mut entry = release("1.2.3", "stable", "linux-arm64", "demo.tar.zst");
        entry.full_sha256 = "sha256-a".to_string();
        entry.install_directory = "demo".to_string();
        entry.supervisor_id = "demo-supervisor".to_string();

        let baseline = remote_staged_payload_identity("demo", &entry, "stable", &storage_config("/srv/releases"));
        let promoted = remote_staged_payload_identity("demo", &entry, "beta", &storage_config("/srv/releases"));

        let mut rebuilt_release = entry.clone();
        rebuilt_release.full_sha256 = "sha256-b".to_string();
        let rebuilt =
            remote_staged_payload_identity("demo", &rebuilt_release, "stable", &storage_config("/srv/releases"));

        assert_ne!(baseline, promoted);
        assert_ne!(baseline, rebuilt);
    }

    #[test]
    fn parse_remote_staged_payload_identity_round_trips_json() {
        let mut entry = release("1.2.3", "stable", "linux-arm64", "demo.tar.zst");
        entry.full_sha256 = "sha256-a".to_string();
        entry.install_directory = "demo".to_string();
        entry.supervisor_id = "demo-supervisor".to_string();

        let identity = remote_staged_payload_identity("demo", &entry, "stable", &storage_config("/srv/releases"));
        let encoded = serde_json::to_string(&identity).expect("staged identity should serialize");

        assert_eq!(parse_remote_staged_payload_identity(&encoded), Some(identity));
        assert_eq!(parse_remote_staged_payload_identity("not-json"), None);
    }

    #[test]
    fn build_remote_stop_supervisor_command_quotes_install_root() {
        let command =
            build_remote_stop_supervisor_command(Path::new("/home/demo/apps/customer's app"), "demo-supervisor")
                .expect("supervisor command should exist");

        assert!(command.contains("install_root='/home/demo/apps/customer'\"'\"'s app'"));
        assert!(command.contains("supervisor_id='demo-supervisor'"));
        assert!(command.contains("pid_file=\"$install_root/.surge-supervisor-$supervisor_id.pid\""));
        assert!(command.contains("clear_if_stale"));
        assert!(command.contains("rm -f \"$pid_file\""));
        assert!(command.contains("kill -KILL \"$pid\""));
    }

    #[test]
    fn build_remote_process_verification_probe_checks_app_version_and_supervisor() {
        let probe = build_remote_process_verification_probe(
            Path::new("/home/demo/apps/demo-app"),
            "demoapp",
            "demo-supervisor",
            "1.2.3",
        );

        assert!(probe.contains("active_exe=\"$install_root/app/$main_exe\""));
        assert!(probe.contains("contains_target_first_run()"));
        assert!(probe.contains("contains_target_version_arg()"));
        assert!(probe.contains("extract_watched_pid()"));
        assert!(probe.contains("*\" --surge-first-run $version \"*|*\" $version --surge-first-run \"*"));
        assert!(probe.contains("target_app_pids"));
        assert!(probe.contains("target_supervisor_seen"));
        assert!(probe.contains("surge-supervisor"));
        assert!(probe.contains("--id $supervisor_id"));
        assert!(probe.contains("app process for $active_exe was not found"));
        assert!(probe.contains("app process for $active_exe is running without target proof for $version"));
        assert!(probe.contains("stale app process for $active_exe is still running without target proof for $version"));
        assert!(probe.contains("supervisor process '$supervisor_id' is still waiting for the previous child"));
        assert!(probe.contains("supervisor process '$supervisor_id' is running with stale first-run proof"));
        assert!(probe.contains("supervisor process '$supervisor_id' was not found"));
        assert!(probe.contains("supervisor process '$supervisor_id' is not watching target app process for $version"));
    }

    #[cfg(unix)]
    #[test]
    fn build_remote_process_verification_probe_is_valid_shell_syntax() {
        use std::io::Write;

        let probe = build_remote_process_verification_probe(
            Path::new("/home/demo/apps/demo-app"),
            "demoapp",
            "demo-supervisor",
            "1.2.3",
        );
        let mut child = std::process::Command::new("sh")
            .arg("-n")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("shell syntax checker should start");
        child
            .stdin
            .as_mut()
            .expect("stdin should be piped")
            .write_all(probe.as_bytes())
            .expect("probe should be written to shell");
        let status = child.wait().expect("shell syntax checker should exit");
        assert!(status.success(), "probe failed shell syntax check with {status}");
    }

    #[test]
    fn plan_remote_published_installer_uses_requested_channel_key() {
        let manifest = remote_manifest("demo", "linux-arm64", &["test", "production"], &["online"]);
        let mut entry = release("1.2.3", "production", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];

        let plan = plan_remote_published_installer(
            &manifest,
            "demo",
            "linux-arm64",
            "production",
            &entry,
            RemoteInstallerMode::Online,
        )
        .expect("plan should resolve");

        assert_eq!(
            plan.candidate_keys,
            vec!["installers/Setup-linux-arm64-demo-production-online.bin".to_string()]
        );
        assert!(plan.blockers.is_empty(), "unexpected blockers: {:?}", plan.blockers);
    }

    #[test]
    fn plan_remote_published_installer_carries_manifest_cache_policy() {
        let yaml = br"schema: 1
cache:
  installArtifacts:
    retention: latest_full
    keepFullCount: 1
apps:
  - id: demo
    channels:
      - production
    targets:
      - rid: linux-arm64
        installers:
          - online
";
        let manifest: SurgeManifest = serde_yaml::from_slice(yaml).expect("manifest should parse");
        let mut entry = release("1.2.3", "production", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];

        let plan = plan_remote_published_installer(
            &manifest,
            "demo",
            "linux-arm64",
            "production",
            &entry,
            RemoteInstallerMode::Online,
        )
        .expect("plan should resolve");
        let policy = plan
            .cache
            .expect("manifest-backed plan should carry cache policy")
            .effective_install_artifact_cache_policy();

        assert_eq!(policy.retention, InstallArtifactCacheRetention::LatestFull);
        assert_eq!(policy.keep_full_count, 1);
    }

    #[test]
    fn plan_remote_published_installer_drops_default_channel_mismatch_blocker() {
        let manifest = remote_manifest("demo", "linux-arm64", &["test", "production"], &["online"]);
        let mut entry = release("1.2.3", "production", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];

        let plan = plan_remote_published_installer(
            &manifest,
            "demo",
            "linux-arm64",
            "production",
            &entry,
            RemoteInstallerMode::Online,
        )
        .expect("plan should resolve");

        assert_eq!(
            plan.candidate_keys,
            vec!["installers/Setup-linux-arm64-demo-production-online.bin".to_string()]
        );
        assert!(plan.blockers.is_empty(), "unexpected blockers: {:?}", plan.blockers);
    }

    #[tokio::test]
    async fn try_prepare_published_installer_for_tailscale_rewrites_manifest_for_remote_env() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let store_dir = tmp.path().join("store");
        let download_dir = tmp.path().join("downloads");
        std::fs::create_dir_all(store_dir.join("installers")).expect("installers dir should exist");

        let manifest = remote_manifest("demo", "linux-arm64", &["test", "production"], &["online"]);
        let mut entry = release("1.2.3", "test", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];
        entry.main_exe = "demoapp".to_string();
        entry.install_directory = "demo".to_string();
        entry.full_filename = "demo.tar.zst".to_string();

        let generic_installer_manifest = InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: InstallerUi::Console,
            installer_type: "online".to_string(),
            app_id: "demo".to_string(),
            rid: "linux-arm64".to_string(),
            version: "1.2.3".to_string(),
            channel: "test".to_string(),
            generated_utc: "2026-03-13T00:00:00Z".to_string(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: "filesystem".to_string(),
                bucket: store_dir.to_string_lossy().to_string(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release: InstallerRelease {
                full_filename: "demo.tar.zst".to_string(),
                full_sha256: String::new(),
                delta_filename: String::new(),
                delta_algorithm: String::new(),
                delta_patch_format: String::new(),
                delta_compression: String::new(),
            },
            runtime: InstallerRuntime {
                name: "Demo".to_string(),
                main_exe: "demoapp".to_string(),
                install_directory: "demo".to_string(),
                supervisor_id: "demo-supervisor".to_string(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: vec!["online".to_string()],
                environment: BTreeMap::new(),
            },
            cache: CacheManifestConfig::default(),
        };
        create_published_installer(
            &store_dir.join("installers"),
            "Setup-linux-arm64-demo-test-online.bin",
            &generic_installer_manifest,
        );

        let backend = FilesystemBackend::new(store_dir.to_str().expect("utf-8 path"), "");
        let launch_env = RemoteLaunchEnvironment {
            display: Some(":0".to_string()),
            xauthority: Some("/run/user/1000/gdm/Xauthority".to_string()),
            dbus_session_bus_address: Some("unix:path=/run/user/1000/bus".to_string()),
            wayland_display: None,
            xdg_runtime_dir: Some("/run/user/1000".to_string()),
        };
        let plan = plan_remote_published_installer(
            &manifest,
            "demo",
            "linux-arm64",
            "test",
            &entry,
            RemoteInstallerMode::Online,
        )
        .expect("plan should resolve");

        let customized_installer = try_prepare_published_installer_for_tailscale(
            &backend,
            &download_dir,
            &plan,
            "demo",
            &entry,
            "test",
            &storage_config(store_dir.to_str().expect("utf-8 path")),
            &launch_env,
            RemoteInstallerMode::Online,
        )
        .await
        .expect("published installer should prepare")
        .expect("customized installer should exist");

        let payload = read_embedded_payload(&customized_installer).expect("payload should be readable");
        let installer_manifest: InstallerManifest =
            serde_yaml::from_slice(&read_entry(&payload, "installer.yml").expect("installer manifest should exist"))
                .expect("installer manifest should parse");
        assert_eq!(installer_manifest.channel, "test");
        assert_eq!(
            installer_manifest
                .runtime
                .environment
                .get("DISPLAY")
                .map(String::as_str),
            Some(":0")
        );
        assert_eq!(
            installer_manifest
                .runtime
                .environment
                .get("XAUTHORITY")
                .map(String::as_str),
            Some("/run/user/1000/gdm/Xauthority")
        );
        assert_eq!(
            installer_manifest
                .runtime
                .environment
                .get("DBUS_SESSION_BUS_ADDRESS")
                .map(String::as_str),
            Some("unix:path=/run/user/1000/bus")
        );
        assert_eq!(
            installer_manifest
                .runtime
                .environment
                .get("XDG_RUNTIME_DIR")
                .map(String::as_str),
            Some("/run/user/1000")
        );
    }

    #[tokio::test]
    async fn try_prepare_published_installer_without_manifest_preserves_embedded_cache_policy() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let store_dir = tmp.path().join("store");
        let download_dir = tmp.path().join("downloads");
        std::fs::create_dir_all(store_dir.join("installers")).expect("installers dir should exist");

        let mut entry = release("1.2.3", "test", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];
        entry.main_exe = "demoapp".to_string();
        entry.install_directory = "demo".to_string();
        entry.full_filename = "demo.tar.zst".to_string();

        let generic_installer_manifest = InstallerManifest {
            schema: 1,
            format: "surge-installer-v1".to_string(),
            ui: InstallerUi::Console,
            installer_type: "online".to_string(),
            app_id: "demo".to_string(),
            rid: "linux-arm64".to_string(),
            version: "1.2.3".to_string(),
            channel: "test".to_string(),
            generated_utc: "2026-03-13T00:00:00Z".to_string(),
            headless_default_if_no_display: true,
            release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
            storage: InstallerStorage {
                provider: "filesystem".to_string(),
                bucket: store_dir.to_string_lossy().to_string(),
                region: String::new(),
                endpoint: String::new(),
                prefix: String::new(),
            },
            release: InstallerRelease {
                full_filename: "demo.tar.zst".to_string(),
                full_sha256: String::new(),
                delta_filename: String::new(),
                delta_algorithm: String::new(),
                delta_patch_format: String::new(),
                delta_compression: String::new(),
            },
            runtime: InstallerRuntime {
                name: "Demo".to_string(),
                main_exe: "demoapp".to_string(),
                install_directory: "demo".to_string(),
                supervisor_id: "demo-supervisor".to_string(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: vec!["online".to_string()],
                environment: BTreeMap::new(),
            },
            cache: latest_full_cache_policy(),
        };
        create_published_installer(
            &store_dir.join("installers"),
            "Setup-linux-arm64-demo-test-online.bin",
            &generic_installer_manifest,
        );

        let backend = FilesystemBackend::new(store_dir.to_str().expect("utf-8 path"), "");
        let plan = plan_remote_published_installer_without_manifest(
            "demo",
            "linux-arm64",
            "test",
            &entry,
            RemoteInstallerMode::Online,
        );

        let customized_installer = try_prepare_published_installer_for_tailscale(
            &backend,
            &download_dir,
            &plan,
            "demo",
            &entry,
            "test",
            &storage_config(store_dir.to_str().expect("utf-8 path")),
            &RemoteLaunchEnvironment::default(),
            RemoteInstallerMode::Online,
        )
        .await
        .expect("published installer should prepare")
        .expect("customized installer should exist");

        let payload = read_embedded_payload(&customized_installer).expect("payload should be readable");
        let installer_manifest: InstallerManifest =
            serde_yaml::from_slice(&read_entry(&payload, "installer.yml").expect("installer manifest should exist"))
                .expect("installer manifest should parse");
        let policy = installer_manifest.effective_install_artifact_cache_policy();
        assert_eq!(policy.retention, InstallArtifactCacheRetention::LatestFull);
        assert_eq!(policy.keep_full_count, 1);
    }

    #[test]
    fn missing_remote_installer_error_mentions_keys_and_host_mismatch() {
        let err = missing_remote_installer_error(
            "linux-arm64",
            &RemotePublishedInstallerPlan {
                candidate_keys: vec!["installers/Setup-linux-arm64-demo-test-online.bin".to_string()],
                blockers: vec!["published installer was not found in storage".to_string()],
                cache: None,
            },
            RemoteInstallerMode::Online,
        );

        let message = err.to_string();
        assert!(message.contains("Setup-linux-arm64-demo-test-online.bin"));
        assert!(message.contains("current host RID"));
        assert!(message.contains("matching host"));
    }

    #[test]
    fn published_installer_public_url_uses_azure_endpoint_and_bucket() {
        let config = surge_core::context::StorageConfig {
            provider: Some(surge_core::context::StorageProvider::AzureBlob),
            bucket: "sample-container".to_string(),
            endpoint: "https://example.blob.core.windows.net".to_string(),
            ..surge_core::context::StorageConfig::default()
        };

        let url = published_installer_public_url(
            &config,
            "installers/Setup-linux-arm64-sampleapp-linux-arm64-test-online.bin",
        );

        assert_eq!(
            url.as_deref(),
            Some(
                "https://example.blob.core.windows.net/sample-container/installers/Setup-linux-arm64-sampleapp-linux-arm64-test-online.bin"
            )
        );
    }

    #[tokio::test]
    async fn execute_installs_selected_release_locally_from_backend() {
        let temp_dir = tempfile::tempdir().expect("temp dir should exist");
        let store_dir = temp_dir.path().join("store");
        let install_root = temp_dir.path().join("install-root");
        let download_dir = temp_dir.path().join("download-cache");
        let application_manifest_path = temp_dir.path().join(".surge").join("application.yml");
        let fallback_manifest_path = temp_dir.path().join("fallback-surge.yml");
        let rid = current_rid();
        let full_filename = format!("demo-1.2.3-{rid}-full.tar.zst");

        std::fs::create_dir_all(&store_dir).expect("store dir should be created");
        std::fs::create_dir_all(application_manifest_path.parent().expect("app manifest parent"))
            .expect("app manifest dir should be created");

        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer("demoapp", b"#!/bin/sh\necho installed\n", 0o755)
            .expect("main executable should be added");
        packer
            .add_buffer("payload.txt", b"installed from execute", 0o644)
            .expect("payload should be added");
        let package_bytes = packer.finalize().expect("archive should be finalized");
        std::fs::write(store_dir.join(&full_filename), &package_bytes).expect("package should be written");

        let mut entry = release("1.2.3", "stable", &rid, &full_filename);
        entry.main_exe = "demoapp".to_string();
        entry.install_directory = install_root.to_string_lossy().to_string();
        entry.shortcuts = Vec::new();
        entry.full_size = i64::try_from(package_bytes.len()).expect("package length should fit i64");
        entry.full_sha256 = sha256_hex(&package_bytes);

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![entry],
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).expect("release index should compress");
        std::fs::write(store_dir.join(RELEASES_FILE_COMPRESSED), compressed).expect("release index should be written");

        let manifest_yaml = format!(
            "schema: 1\nstorage:\n  provider: filesystem\n  bucket: {}\napps:\n  - id: demo\n    channels: [stable]\n    target:\n      rid: {rid}\n",
            store_dir.display()
        );
        std::fs::write(&application_manifest_path, manifest_yaml).expect("application manifest should be written");

        execute(
            &fallback_manifest_path,
            &application_manifest_path,
            None,
            None,
            Some("demo"),
            Some("stable"),
            Some(&rid),
            Some("1.2.3"),
            InstallBehavior {
                plan_only: false,
                no_start: true,
                force: false,
                platform_mismatch: PlatformMismatchPolicy::Reject,
                mode: InstallMode::Install,
            },
            &download_dir,
            StorageOverrides::default(),
        )
        .await
        .expect("install command should succeed");

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).expect("payload file should exist"),
            "installed from execute"
        );
        assert!(
            download_dir.join(&full_filename).is_file(),
            "package should be cached locally after install"
        );

        let runtime_manifest =
            std::fs::read_to_string(active_app_dir.join(surge_core::install::RUNTIME_MANIFEST_RELATIVE_PATH))
                .expect("runtime manifest should be written");
        assert!(runtime_manifest.contains("id: demo"));
        assert!(runtime_manifest.contains("version: 1.2.3"));
        assert!(runtime_manifest.contains("channel: stable"));
        assert!(runtime_manifest.contains(&format!("bucket: {}", store_dir.display())));
    }

    #[test]
    fn ensure_supported_tailscale_rid_accepts_linux() {
        assert!(ensure_supported_tailscale_rid("linux-x64").is_ok());
        assert!(ensure_supported_tailscale_rid("linux-arm64-cuda").is_ok());
    }

    #[test]
    fn ensure_supported_tailscale_rid_rejects_windows() {
        let err = ensure_supported_tailscale_rid("win-x64").expect_err("windows should be rejected");
        assert!(
            err.to_string().contains("supports Linux targets only"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_targets_plain_node_without_user() {
        let (ssh_target, file_target) = resolve_tailscale_targets("edge-node", None).expect("targets");
        assert_eq!(ssh_target, "edge-node");
        assert_eq!(file_target, "edge-node");
    }

    #[test]
    fn resolve_targets_plain_node_with_node_user() {
        let (ssh_target, file_target) = resolve_tailscale_targets("edge-node", Some("operator")).expect("targets");
        assert_eq!(ssh_target, "operator@edge-node");
        assert_eq!(file_target, "edge-node");
    }

    #[test]
    fn resolve_targets_user_at_node_keeps_file_target_host_only() {
        let (ssh_target, file_target) = resolve_tailscale_targets("alice@edge-node", Some("ignored")).expect("targets");
        assert_eq!(ssh_target, "alice@edge-node");
        assert_eq!(file_target, "edge-node");
    }

    #[test]
    fn shell_single_quote_escapes_apostrophes() {
        assert_eq!(shell_single_quote("plain"), "'plain'");
        assert_eq!(shell_single_quote("O'Reilly"), "'O'\"'\"'Reilly'");
    }

    #[test]
    fn install_package_locally_creates_expected_app_layout() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let package_path = tmp.path().join("package.tar.zst");

        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer("sampleapp", b"#!/bin/sh\necho ok\n", 0o755)
            .expect("main executable should be added");
        packer
            .add_buffer(".surge/surge.yml", b"schema: 1\n", 0o644)
            .expect("manifest should be added");
        let package_bytes = packer.finalize().expect("archive should be finalized");
        std::fs::write(&package_path, package_bytes).expect("archive should be written");

        let mut entry = release("1.2.3", "test", "linux-x64-cuda", "sampleapp-full.tar.zst");
        entry.main_exe = "sampleapp".to_string();
        entry.install_directory = "sampleapp".to_string();
        entry.shortcuts = Vec::new();

        let profile = release_install_profile("sampleapp", &entry);
        core_install::install_package_locally_at_root(&profile, &package_path, &install_root)
            .expect("local install should succeed");

        assert!(install_root.join("app").join("sampleapp").is_file());
        assert!(install_root.join("app").join(".surge").join("surge.yml").is_file());
        assert!(!install_root.join(".surge-app-next").exists());
        assert!(!install_root.join(".surge-app-prev").exists());
    }

    #[test]
    fn install_package_locally_replaces_existing_app_directory() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let install_root = tmp.path().join("install-root");
        let existing_app_dir = install_root.join("app");
        std::fs::create_dir_all(&existing_app_dir).expect("existing app dir should exist");
        std::fs::write(existing_app_dir.join("old.txt"), b"old").expect("old file should be written");

        let package_path = tmp.path().join("package.tar.zst");
        let mut packer = ArchivePacker::new(3).expect("archive packer should be created");
        packer
            .add_buffer("new.txt", b"new", 0o644)
            .expect("new payload should be added");
        let package_bytes = packer.finalize().expect("archive should be finalized");
        std::fs::write(&package_path, package_bytes).expect("archive should be written");

        let mut entry = release("1.2.3", "test", "linux-x64-cuda", "sampleapp-full.tar.zst");
        entry.main_exe = "sampleapp".to_string();
        entry.install_directory = "sampleapp".to_string();
        entry.shortcuts = Vec::new();

        let profile = release_install_profile("sampleapp", &entry);
        core_install::install_package_locally_at_root(&profile, &package_path, &install_root)
            .expect("local install should succeed");

        assert!(install_root.join("app").join("new.txt").is_file());
        assert!(!install_root.join("app").join("old.txt").exists());
        assert!(!install_root.join(".surge-app-prev").exists());
    }

    #[tokio::test]
    async fn download_release_archive_reconstructs_missing_full_from_deltas() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let backend = FilesystemBackend::new(tmp.path().to_str().expect("temp path should be utf-8"), "");

        let full_v1 = b"payload-v1".to_vec();
        let full_v2 = b"payload-v2".to_vec();
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).expect("patch should build");
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).expect("delta should encode");

        let mut v1 = release("1.0.0", "test", "linux-x64", "demo-1.0.0-linux-x64-full.tar.zst");
        v1.full_sha256 = sha256_hex(&full_v1);
        v1.set_primary_delta(None);

        let mut v2 = release("1.1.0", "test", "linux-x64", "demo-1.1.0-linux-x64-full.tar.zst");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .expect("v1 full should upload");
        let v2_delta = v2
            .selected_delta()
            .expect("v2 should include descriptor delta")
            .filename;
        backend
            .put_object(&v2_delta, &delta_v2, "application/octet-stream")
            .await
            .expect("v2 delta should upload");

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2.clone()],
            ..ReleaseIndex::default()
        };

        let destination = tmp.path().join("downloaded-full.tar.zst");
        let rebuilt = download_release_archive(
            &backend,
            &index,
            &v2,
            &[String::from("linux-x64")],
            &v2.full_filename,
            &destination,
        )
        .await
        .expect("fallback restore should succeed");

        assert_eq!(rebuilt, ArchiveAcquisition::Reconstructed);
        assert_eq!(
            std::fs::read(destination).expect("rebuilt archive should be readable"),
            full_v2
        );
    }

    #[tokio::test]
    async fn download_release_archive_reuses_valid_cached_full_package() {
        let tmp = tempfile::tempdir().expect("temp dir should exist");
        let backend = FilesystemBackend::new(tmp.path().to_str().expect("temp path should be utf-8"), "");

        let full = b"payload-v1".to_vec();
        let mut release = release("1.0.0", "test", "linux-x64", "demo-1.0.0-linux-x64-full.tar.zst");
        release.full_sha256 = sha256_hex(&full);
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![release.clone()],
            ..ReleaseIndex::default()
        };

        let destination = tmp.path().join("cached-full.tar.zst");
        std::fs::write(&destination, &full).expect("cached full should be written");

        let acquisition = download_release_archive(
            &backend,
            &index,
            &release,
            &[String::from("linux-x64")],
            &release.full_filename,
            &destination,
        )
        .await
        .expect("reuse should succeed");

        assert_eq!(acquisition, ArchiveAcquisition::ReusedLocal);
        assert_eq!(
            std::fs::read(destination).expect("cached full should be readable"),
            full
        );
    }

    fn load_reference_manifest_bytes() -> Vec<u8> {
        br"schema: 2
channels:
  - name: test
  - name: production
apps:
  - id: quasar-ubuntu24.04-linux-x64-cpu
    channels: [test, production]
    target:
      rid: linux-x64
  - id: quasar-ubuntu24.04-linux-x64-cuda
    channels: [test, production]
    target:
      rid: linux-x64
  - id: quasar-jetpack4.6-linux-arm64
    channels: [test, production]
    target:
      rid: linux-arm64
  - id: quasar-jetpack5.0-linux-arm64
    channels: [test, production]
    target:
      rid: linux-arm64
  - id: quasar-jetpack5.1-linux-arm64
    channels: [test, production]
    target:
      rid: linux-arm64
"
        .to_vec()
    }

    #[test]
    fn parse_rid_signature_extracts_os_arch_and_gpu_hint() {
        let signature = parse_rid_signature("linux-x64-cuda").expect("rid signature should parse");
        assert_eq!(signature.os, "linux");
        assert_eq!(signature.arch, "x64");
        assert!(signature.has_gpu_hint);
    }

    #[test]
    fn local_rid_incompatibility_warnings_detect_os_arch_and_gpu_mismatch() {
        let local_profile = RuntimeProfile {
            os: "linux".to_string(),
            arch: "arm64".to_string(),
            gpu: "none".to_string(),
        };
        let warnings = local_rid_incompatibility_warnings("win-x64-cuda", &local_profile);
        assert_eq!(warnings.len(), 3);
        assert!(warnings.iter().any(|warning| warning.contains("targets OS 'win'")));
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("targets architecture 'x64'"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("implies GPU acceleration"))
        );
    }

    #[test]
    fn derive_rid_candidates_gpu() {
        let profile = RuntimeProfile {
            os: "Linux".to_string(),
            arch: "amd64".to_string(),
            gpu: "nvidia".to_string(),
        };
        let base = derive_base_rid(&profile).expect("base rid should resolve");
        let candidates = build_rid_candidates(&base, true);
        assert!(candidates.contains(&"linux-x64-nvidia".to_string()));
        assert!(candidates.contains(&"linux-x64-cuda".to_string()));
        assert!(candidates.contains(&"linux-x64-gpu".to_string()));
        assert!(candidates.contains(&"linux-x64".to_string()));
    }

    #[test]
    fn derive_rid_candidates_cover_cpu_cuda_variants() {
        let x64_cpu = build_rid_candidates("linux-x64", false);
        assert!(x64_cpu.contains(&"linux-x64".to_string()));
        assert!(x64_cpu.contains(&"linux-x64-cpu".to_string()));

        let x64_gpu = build_rid_candidates("linux-x64", true);
        assert!(x64_gpu.contains(&"linux-x64-cuda".to_string()));

        let arm64 = build_rid_candidates("linux-arm64", true);
        assert!(arm64.contains(&"linux-arm64".to_string()));
    }

    #[test]
    fn derive_rid_candidates_cover_reference_manifest_targets() {
        let manifest = SurgeManifest::parse(&load_reference_manifest_bytes()).expect("manifest should parse");
        let mut rids = manifest
            .app_ids()
            .into_iter()
            .flat_map(|app_id| manifest.target_rids(&app_id))
            .collect::<Vec<_>>();
        rids.sort();
        rids.dedup();

        assert!(rids.contains(&"linux-x64".to_string()));
        assert!(rids.contains(&"linux-arm64".to_string()));

        let cpu_candidates = build_rid_candidates("linux-x64", false);
        let gpu_candidates = build_rid_candidates("linux-x64", true);
        let arm_candidates = build_rid_candidates("linux-arm64", true);

        assert!(cpu_candidates.contains(&"linux-x64-cpu".to_string()));
        assert!(gpu_candidates.contains(&"linux-x64-cuda".to_string()));
        assert!(arm_candidates.contains(&"linux-arm64".to_string()));
    }

    #[test]
    fn select_release_prefers_first_matching_candidate() {
        let releases = vec![
            release("1.1.0", "stable", "linux-x64", "cpu-1.1.0"),
            release("1.0.0", "stable", "linux-x64-gpu", "gpu-1.0.0"),
            release("1.2.0", "stable", "", "generic-1.2.0"),
        ];

        let candidates = vec![
            "linux-x64-gpu".to_string(),
            "linux-x64".to_string(),
            "linux-x64-cpu".to_string(),
        ];

        let selected = select_release(&releases, "stable", None, &candidates, None).expect("release should resolve");
        assert_eq!(selected.full_filename, "gpu-1.0.0");
    }

    #[test]
    fn select_release_falls_back_to_generic() {
        let releases = vec![release("1.3.0", "stable", "", "generic-1.3.0")];
        let candidates = vec!["linux-arm64".to_string()];

        let selected = select_release(&releases, "stable", None, &candidates, None).expect("release should resolve");
        assert_eq!(selected.full_filename, "generic-1.3.0");
    }

    #[test]
    fn select_release_supports_cpu_cuda_variants() {
        let releases = vec![
            release("1.0.0", "production", "linux-x64-cpu", "cpu"),
            release("1.0.0", "production", "linux-x64-cuda", "cuda"),
            release("1.0.0", "production", "linux-arm64", "arm"),
        ];

        let gpu_candidates = build_rid_candidates("linux-x64", true);
        let gpu =
            select_release(&releases, "production", None, &gpu_candidates, None).expect("gpu release should resolve");
        assert_eq!(gpu.full_filename, "cuda");

        let cpu_candidates = build_rid_candidates("linux-x64", false);
        let cpu =
            select_release(&releases, "production", None, &cpu_candidates, None).expect("cpu release should resolve");
        assert_eq!(cpu.full_filename, "cpu");
    }

    #[test]
    fn select_release_honors_selected_os_filter() {
        let mut linux = release("1.0.0", "stable", "linux-x64", "linux");
        linux.os = "linux".to_string();
        let mut windows = release("1.0.0", "stable", "win-x64", "windows");
        windows.os = "windows".to_string();
        let releases = vec![linux, windows];

        let candidates = vec!["linux-x64".to_string(), "win-x64".to_string()];
        let selected =
            select_release(&releases, "stable", None, &candidates, Some("windows")).expect("release should resolve");
        assert_eq!(selected.full_filename, "windows");
    }

    #[test]
    fn collect_target_options_for_app_infers_os_from_rid() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/surge-test
apps:
  - id: demo
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let options = collect_target_options_for_app(&manifest, "demo").expect("targets should resolve");
        assert!(options.contains(&AppInstallTargetOption {
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
        }));
    }

    #[test]
    fn resolve_install_target_selection_auto_selects_single_option() {
        let selected = resolve_install_target_selection(
            &[AppInstallTargetOption {
                os: "linux".to_string(),
                rid: "linux-x64".to_string(),
            }],
            None,
        )
        .expect("single target should be selected");
        assert_eq!(selected.rid, "linux-x64");
        assert_eq!(selected.os, "linux");
    }

    #[test]
    fn resolve_install_target_selection_uses_requested_rid_when_unique() {
        let selected = resolve_install_target_selection(
            &[
                AppInstallTargetOption {
                    os: "linux".to_string(),
                    rid: "linux-x64".to_string(),
                },
                AppInstallTargetOption {
                    os: "linux".to_string(),
                    rid: "linux-arm64".to_string(),
                },
            ],
            Some("linux-arm64"),
        )
        .expect("requested rid should select target");
        assert_eq!(selected.rid, "linux-arm64");
        assert_eq!(selected.os, "linux");
    }

    #[test]
    fn format_target_option_label_uses_os_arch_format() {
        let label = format_target_option_label(&AppInstallTargetOption {
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
        });
        assert_eq!(label, "linux/x64");
    }

    #[test]
    fn format_target_option_label_single_segment_rid() {
        let label = format_target_option_label(&AppInstallTargetOption {
            os: "linux".to_string(),
            rid: "custom".to_string(),
        });
        assert_eq!(label, "custom");
    }

    #[test]
    fn infer_os_from_rid_maps_common_prefixes() {
        assert_eq!(infer_os_from_rid("linux-x64"), Some("linux".to_string()));
        assert_eq!(infer_os_from_rid("win-x64"), Some("windows".to_string()));
        assert_eq!(infer_os_from_rid("osx-arm64"), Some("macos".to_string()));
        assert_eq!(infer_os_from_rid("unknown-rid"), None);
    }

    #[test]
    fn resolve_install_channel_uses_explicit_override() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
channels:
  - name: test
  - name: production
apps:
  - id: demo
    channels: [test, production]
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex::default();
        let resolved =
            resolve_install_channel(&manifest, &index, "demo", Some("test")).expect("channel should resolve");
        assert_eq!(resolved.name, "test");
        assert!(resolved.note.is_none());
    }

    #[test]
    fn resolve_install_channel_auto_selects_single_available_channel() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
channels:
  - name: test
  - name: production
apps:
  - id: demo
    channels: [test, production]
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![release("1.0.0", "production", "linux-x64", "demo-full.tar.zst")],
            ..ReleaseIndex::default()
        };
        let resolved = resolve_install_channel(&manifest, &index, "demo", None).expect("channel should resolve");
        assert_eq!(resolved.name, "production");
        assert!(resolved.note.is_some());
    }

    #[test]
    fn resolve_install_channel_requires_explicit_choice_when_multiple_channels_exist() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
channels:
  - name: test
  - name: production
apps:
  - id: demo
    channels: [test, production]
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![
                release("1.0.0", "test", "linux-x64", "demo-test.tar.zst"),
                release("1.0.0", "production", "linux-x64", "demo-prod.tar.zst"),
            ],
            ..ReleaseIndex::default()
        };

        let err = resolve_install_channel(&manifest, &index, "demo", None).expect_err("choice should be required");
        assert!(err.to_string().contains("Multiple channels available"));
    }

    #[test]
    fn resolve_install_channel_auto_selects_single_configured_channel_when_index_is_empty() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
channels:
  - name: production
apps:
  - id: demo
    channels: [production]
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex::default();
        let resolved = resolve_install_channel(&manifest, &index, "demo", None).expect("channel should resolve");
        assert_eq!(resolved.name, "production");
        assert!(resolved.note.is_some());
    }

    #[test]
    fn collect_install_channel_options_prefers_release_index_channels() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
channels:
  - name: test
apps:
  - id: demo
    channels: [test]
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![release("1.0.0", "production", "linux-x64", "demo-full.tar.zst")],
            ..ReleaseIndex::default()
        };
        let channels = collect_install_channel_options(&manifest, &index, "demo");
        assert_eq!(channels, vec!["production".to_string()]);
    }

    #[test]
    fn collect_install_channel_options_falls_back_to_manifest_channels() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
channels:
  - name: test
  - name: production
apps:
  - id: demo
    channels: [test, production]
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex::default();
        let channels = collect_install_channel_options(&manifest, &index, "demo");
        assert_eq!(channels, vec!["test".to_string(), "production".to_string()]);
    }

    #[test]
    fn collect_install_channel_options_defaults_to_stable() {
        let manifest = SurgeManifest::parse(
            br"schema: 1
storage:
  provider: filesystem
  bucket: /tmp/releases
apps:
  - id: demo
    target:
      rid: linux-x64
",
        )
        .expect("manifest should parse");

        let index = ReleaseIndex::default();
        let channels = collect_install_channel_options(&manifest, &index, "demo");
        assert_eq!(channels, vec!["stable".to_string()]);
    }

    #[test]
    fn collect_available_channels_deduplicates_and_sorts() {
        let releases = vec![
            release("1.0.0", "test", "linux-x64", "a"),
            release("1.0.1", "production", "linux-x64", "b"),
            release("1.0.2", "test", "linux-x64", "c"),
        ];
        let channels = collect_available_channels(&releases);
        assert_eq!(channels, vec!["production".to_string(), "test".to_string()]);
    }

    #[test]
    fn parse_remote_install_state_extracts_version_and_channel() {
        let state = parse_remote_install_state(
            "id=demo\nversion=1.2.3\nactive_executable_exists=true\nchannel=production\nprovider=filesystem\nbucket=/srv/releases\nregion=\nendpoint=\n",
        )
        .expect("remote install state should parse");
        assert_eq!(state.app_id.as_deref(), Some("demo"));
        assert_eq!(state.version, "1.2.3");
        assert!(state.active_executable_exists);
        assert_eq!(state.channel.as_deref(), Some("production"));
        assert_eq!(state.storage_provider.as_deref(), Some("filesystem"));
        assert_eq!(state.storage_bucket.as_deref(), Some("/srv/releases"));
    }

    #[test]
    fn parse_remote_install_state_requires_version() {
        assert!(parse_remote_install_state("channel=test\n").is_none());
    }

    #[test]
    fn remote_install_matches_requires_matching_app_channel_and_version() {
        let production = RemoteInstallState {
            app_id: Some("demo".to_string()),
            version: "1.2.3".to_string(),
            active_executable_exists: true,
            channel: Some("production".to_string()),
            storage_provider: None,
            storage_bucket: None,
            storage_region: None,
            storage_endpoint: None,
        };
        let test = RemoteInstallState {
            app_id: Some("demo".to_string()),
            version: "1.2.3".to_string(),
            active_executable_exists: true,
            channel: Some("test".to_string()),
            storage_provider: None,
            storage_bucket: None,
            storage_region: None,
            storage_endpoint: None,
        };

        assert!(remote_install_matches(Some(&production), "demo", "1.2.3", "production"));
        assert!(!remote_install_matches(
            Some(&production),
            "other",
            "1.2.3",
            "production"
        ));
        assert!(!remote_install_matches(
            Some(&production),
            "demo",
            "1.2.4",
            "production"
        ));
        assert!(!remote_install_matches(Some(&test), "demo", "1.2.3", "production"));
        assert!(!remote_install_matches(None, "demo", "1.2.3", "production"));
    }

    #[test]
    fn force_flag_bypasses_remote_install_skip() {
        let remote_state = RemoteInstallState {
            app_id: Some("demo".to_string()),
            version: "1.2.3".to_string(),
            active_executable_exists: true,
            channel: Some("test".to_string()),
            storage_provider: None,
            storage_bucket: None,
            storage_region: None,
            storage_endpoint: None,
        };

        let install_matches = remote_install_matches(Some(&remote_state), "demo", "1.2.3", "test");

        assert!(should_skip_remote_install(install_matches, false));
        assert!(!should_skip_remote_install(install_matches, true));
    }

    #[test]
    fn remote_convergence_plan_uses_delta_for_stale_existing_install() {
        let storage = storage_config("/srv/releases");
        let mut target = release("1.2.0", "test", "linux-x64", "demo-1.2.0-linux-x64-full.tar.zst");
        target.full_size = 1_000;
        target.set_primary_delta(Some(DeltaArtifact::sparse_file_ops_zstd(
            "primary",
            "1.0.0",
            "demo-1.2.0-linux-x64-delta.tar.zst",
            123,
            "delta-sha",
        )));
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };
        let state = remote_state("1.0.0", "test", &storage);

        let plan = plan_remote_convergence(
            Some(&state),
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            false,
        )
        .expect("plan should resolve");

        assert_eq!(plan.action, RemoteConvergenceAction::Update);
        assert!(plan.reason.is_none());
        let update = plan.update_info.expect("update info should exist");
        assert_eq!(update.apply_strategy, surge_core::update::manager::ApplyStrategy::Delta);
        assert_eq!(update.download_size, 123);
    }

    #[test]
    fn remote_convergence_plan_skips_current_install_unless_forced() {
        let storage = storage_config("/srv/releases");
        let target = release("1.2.0", "test", "linux-x64", "demo-1.2.0-linux-x64-full.tar.zst");
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };
        let state = remote_state("1.2.0", "test", &storage);

        let plan = plan_remote_convergence(
            Some(&state),
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            false,
        )
        .expect("plan should resolve");
        assert_eq!(plan.action, RemoteConvergenceAction::Skip);

        let forced = plan_remote_convergence(
            Some(&state),
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            true,
        )
        .expect("forced plan should resolve");
        assert_eq!(forced.action, RemoteConvergenceAction::ConvergeRuntime);
        assert!(
            forced
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("verify remote runtime convergence"))
        );
    }

    #[test]
    fn remote_convergence_plan_reinstalls_same_version_when_app_id_differs() {
        let storage = storage_config("/srv/releases");
        let target = release("1.2.0", "test", "linux-x64", "target-1.2.0-linux-x64-full.tar.zst");
        let index = ReleaseIndex {
            app_id: "target".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };
        let mut state = remote_state("1.2.0", "test", &storage);
        state.app_id = Some("previous".to_string());

        let plan = plan_remote_convergence(
            Some(&state),
            &index,
            "target",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            true,
        )
        .expect("forced app swap plan should resolve");

        assert_eq!(plan.action, RemoteConvergenceAction::Reinstall);
        assert!(
            plan.reason
                .as_deref()
                .is_some_and(|reason| reason.contains("installed app id 'previous' differs"))
        );
    }

    #[test]
    fn remote_convergence_plan_reinstalls_current_install_when_active_executable_is_missing() {
        let storage = storage_config("/srv/releases");
        let target = release("1.2.0", "test", "linux-x64", "demo-1.2.0-linux-x64-full.tar.zst");
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };
        let mut state = remote_state("1.2.0", "test", &storage);
        state.active_executable_exists = false;

        let plan = plan_remote_convergence(
            Some(&state),
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            false,
        )
        .expect("plan should resolve");

        assert_eq!(plan.action, RemoteConvergenceAction::Reinstall);
        assert!(
            plan.reason
                .as_deref()
                .is_some_and(|reason| reason.contains("active executable is missing"))
        );
    }

    #[test]
    fn build_remote_runtime_start_command_restarts_through_supervisor() {
        let mut environment = BTreeMap::new();
        environment.insert("DISPLAY".to_string(), ":0".to_string());

        let command = build_remote_runtime_start_command(
            Path::new("/home/demo/.local/share/demo app"),
            "demo",
            "demo-supervisor",
            "1.2.0",
            &environment,
        );

        assert!(command.contains("export DISPLAY=':0'"));
        assert!(command.contains("kill_matching \"$active_exe\""));
        assert!(command.contains("kill_matching \"$install_root/app-\""));
        assert!(command.contains("kill_matching \"surge-supervisor.*--id $supervisor_id\""));
        assert!(command.contains("supervisor_bin=\"$active_app_dir/surge-supervisor\""));
        assert!(command.contains("nohup \"$supervisor_bin\" run --id \"$supervisor_id\""));
        assert!(command.contains("-- --surge-first-run \"$version\""));
        assert!(command.contains("supervisor restart confirmed"));
    }

    #[test]
    fn build_remote_runtime_start_command_restarts_direct_app_without_supervisor() {
        let command = build_remote_runtime_start_command(
            Path::new("/home/demo/.local/share/demo"),
            "demo",
            "",
            "1.2.0",
            &BTreeMap::new(),
        );

        assert!(command.contains("nohup \"$active_exe\" --surge-first-run \"$version\""));
        assert!(command.contains("application restart requested"));
        assert!(command.contains("supervisor_id=''"));
    }

    #[test]
    fn remote_convergence_plan_uses_clean_install_when_no_install_exists() {
        let storage = storage_config("/srv/releases");
        let target = release("1.2.0", "test", "linux-x64", "demo-1.2.0-linux-x64-full.tar.zst");
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };

        let plan = plan_remote_convergence(
            None,
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            false,
        )
        .expect("plan should resolve");

        assert_eq!(plan.action, RemoteConvergenceAction::CleanInstall);
    }

    #[test]
    fn remote_convergence_plan_repairs_current_install_metadata() {
        let storage = storage_config("/srv/releases");
        let target = release("1.2.0", "test", "linux-x64", "demo-1.2.0-linux-x64-full.tar.zst");
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };
        let mut state = remote_state("1.2.0", "test", &storage);
        state.storage_bucket = Some("/old/releases".to_string());

        let plan = plan_remote_convergence(
            Some(&state),
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            false,
        )
        .expect("plan should resolve");

        assert_eq!(plan.action, RemoteConvergenceAction::RepairMetadata);
    }

    #[test]
    fn remote_convergence_plan_falls_back_to_full_when_delta_is_unsupported() {
        let storage = storage_config("/srv/releases");
        let mut target = release("1.2.0", "test", "linux-x64", "demo-1.2.0-linux-x64-full.tar.zst");
        target.full_size = 2_000;
        target.deltas = vec![DeltaArtifact {
            id: "primary".to_string(),
            from_version: "1.0.0".to_string(),
            algorithm: "unsupported".to_string(),
            patch_format: "unknown".to_string(),
            compression: "zstd".to_string(),
            filename: "demo-1.2.0-linux-x64-delta.tar.zst".to_string(),
            size: 100,
            sha256: "delta-sha".to_string(),
        }];
        target.preferred_delta_id = "primary".to_string();
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![target.clone()],
            ..ReleaseIndex::default()
        };
        let state = remote_state("1.0.0", "test", &storage);

        let plan = plan_remote_convergence(
            Some(&state),
            &index,
            "demo",
            "linux-x64",
            &target,
            "test",
            &storage,
            RemoteInstallerMode::Online,
            false,
        )
        .expect("plan should resolve");

        assert_eq!(plan.action, RemoteConvergenceAction::Update);
        let update = plan.update_info.expect("update info should exist");
        assert_eq!(update.apply_strategy, surge_core::update::manager::ApplyStrategy::Full);
        assert_eq!(update.download_size, 2_000);
        assert!(
            update
                .fallback_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("unsupported descriptor"))
        );
    }
}
