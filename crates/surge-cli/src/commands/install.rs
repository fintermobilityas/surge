#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::logline;
use crate::prompts;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::installer::{
    InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{self as core_install, InstallProfile};
use surge_core::releases::artifact_cache::{CacheFetchOutcome, cache_path_for_key, fetch_or_reuse_file};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::restore::{RestoreOptions, RestoreProgress, restore_full_archive_for_version_with_options};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend, TransferProgress};

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageOverrides<'a> {
    pub provider: Option<&'a str>,
    pub bucket: Option<&'a str>,
    pub region: Option<&'a str>,
    pub endpoint: Option<&'a str>,
    pub prefix: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeProfile {
    os: String,
    arch: String,
    gpu: String,
}

impl RuntimeProfile {
    fn has_nvidia_gpu(&self) -> bool {
        let gpu = self.gpu.trim().to_ascii_lowercase();
        gpu == "nvidia" || gpu == "true" || gpu == "yes"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RidSignature {
    os: &'static str,
    arch: &'static str,
    has_gpu_hint: bool,
}

fn ensure_supported_tailscale_rid(rid: &str) -> Result<()> {
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
struct RemoteLaunchEnvironment {
    display: Option<String>,
    xauthority: Option<String>,
    dbus_session_bus_address: Option<String>,
    wayland_display: Option<String>,
    xdg_runtime_dir: Option<String>,
}

impl RemoteLaunchEnvironment {
    fn has_graphical_session(&self) -> bool {
        self.display.is_some() || self.wayland_display.is_some()
    }
}

enum InstallTarget {
    Local,
    Tailscale { ssh_target: String, file_target: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedInstallChannel {
    name: String,
    note: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveAcquisition {
    ReusedLocal,
    Downloaded,
    Reconstructed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteInstallerMode {
    Online,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InstallSelection {
    app_id: String,
    os: String,
    rid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppInstallTargetOption {
    os: String,
    rid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteInstallState {
    version: String,
    channel: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemotePublishedInstallerPlan {
    candidate_keys: Vec<String>,
    blockers: Vec<String>,
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
    plan_only: bool,
    no_start: bool,
    force: bool,
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

    if plan_only {
        match &install_target {
            InstallTarget::Local => {
                logline::warn("Plan only mode: no download performed. Remove --plan-only to fetch the package.");
            }
            InstallTarget::Tailscale { file_target, .. } => logline::warn(&format!(
                "Plan only mode: no transfer performed. Remove --plan-only to download and copy package to {file_target}."
            )),
        }
        return Ok(());
    }

    match &install_target {
        InstallTarget::Local => {
            std::fs::create_dir_all(download_dir)?;
            let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
            let acquisition = download_release_archive(
                &*backend,
                &index,
                release,
                &rid_candidates,
                full_filename,
                &local_package,
            )
            .await?;
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
            stop_running_supervisor(&app_id, release).await?;
            let install_root = install_package_locally(&app_id, release, &local_package)?;
            let active_app_dir = install_root.join("app");
            let install_profile = release_install_profile(&app_id, release);
            let runtime_manifest = release_runtime_manifest_metadata(release, &channel, &storage_config);
            core_install::write_runtime_manifest(&active_app_dir, &install_profile, &runtime_manifest)?;
            logline::success(&format!(
                "Installed '{}' to '{}' (active app: '{}').",
                app_id,
                install_root.display(),
                active_app_dir.display()
            ));

            if !no_start && !plan_only {
                let display_name = release.display_name(&app_id);
                match auto_start_after_install(release, &app_id, &install_root, &active_app_dir) {
                    Ok(pid) => {
                        logline::success(&format!("Started '{display_name}' (pid {pid})."));
                    }
                    Err(e) => {
                        logline::warn(&format!("Auto-start failed: {e}"));
                    }
                }
            }
        }
        InstallTarget::Tailscale {
            ssh_target,
            file_target,
        } => {
            let installer_mode = select_remote_installer_mode(&storage_config);
            let install_dir = if release.install_directory.trim().is_empty() {
                &app_id
            } else {
                release.install_directory.trim()
            };
            let remote_state = check_remote_install_state(ssh_target, install_dir).await;
            let install_matches = remote_install_matches(remote_state.as_ref(), &release.version, &channel);
            if should_skip_remote_install(install_matches, force) {
                logline::success(&format!(
                    "'{app_id}' v{} ({channel}) is already installed on '{file_target}', skipping.",
                    release.version
                ));
            } else {
                if install_matches {
                    logline::info(&format!(
                        "'{app_id}' v{} ({channel}) is already installed on '{file_target}'; reinstalling due to --force.",
                        release.version
                    ));
                } else if let Some(remote_state) = &remote_state
                    && remote_state.version.trim() == release.version
                {
                    logline::info(&format!(
                        "'{app_id}' v{} is installed on '{file_target}' with channel '{}'; reinstalling to switch to '{channel}'.",
                        release.version,
                        remote_state.channel.as_deref().unwrap_or("unknown")
                    ));
                }
                let launch_env = detect_remote_launch_environment(ssh_target).await;
                if let Some(display) = launch_env.display.as_deref() {
                    logline::info(&format!("Detected remote X11 session for install: DISPLAY={display}"));
                } else if let Some(wayland_display) = launch_env.wayland_display.as_deref() {
                    logline::info(&format!(
                        "Detected remote Wayland session for install: WAYLAND_DISPLAY={wayland_display}"
                    ));
                } else if launch_env.has_graphical_session() {
                    logline::info("Detected remote graphical session for install.");
                } else {
                    logline::info(
                        "No remote graphical session environment detected; install will default to headless startup.",
                    );
                }
                if !host_can_build_installer_locally(&selected_rid) {
                    deploy_remote_app_copy_for_tailscale(
                        &*backend,
                        &index,
                        download_dir,
                        ssh_target,
                        file_target,
                        &app_id,
                        &selected_rid,
                        release,
                        &channel,
                        &storage_config,
                        &launch_env,
                        &rid_candidates,
                        full_filename,
                        no_start,
                    )
                    .await?;
                    logline::success(&format!("Installed '{app_id}' on tailscale node '{file_target}'."));
                    return Ok(());
                }
                let published_installer_plan = if let Some(manifest) = manifest.as_ref() {
                    plan_remote_published_installer(
                        manifest,
                        &app_id,
                        &selected_rid,
                        &channel,
                        release,
                        installer_mode,
                    )?
                } else {
                    plan_remote_published_installer_without_manifest(
                        &app_id,
                        &selected_rid,
                        &channel,
                        release,
                        installer_mode,
                    )
                };
                let installer_path = if let Some(installer_path) = try_prepare_published_installer_for_tailscale(
                    &*backend,
                    download_dir,
                    &published_installer_plan,
                    &app_id,
                    release,
                    &channel,
                    &storage_config,
                    &launch_env,
                    installer_mode,
                )
                .await?
                {
                    installer_path
                } else if installer_mode == RemoteInstallerMode::Offline {
                    if !host_can_build_installer_locally(&selected_rid) {
                        return Err(missing_remote_installer_error(
                            &selected_rid,
                            &published_installer_plan,
                            installer_mode,
                        ));
                    }
                    std::fs::create_dir_all(download_dir)?;
                    let local_package = download_dir.join(Path::new(full_filename).file_name().unwrap_or_default());
                    let acquisition = download_release_archive(
                        &*backend,
                        &index,
                        release,
                        &rid_candidates,
                        full_filename,
                        &local_package,
                    )
                    .await?;
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
                    logline::info("Building offline installer for remote deployment...");
                    build_installer_for_tailscale(
                        manifest.as_ref(),
                        &app_id,
                        &selected_rid,
                        release,
                        &channel,
                        &storage_config,
                        Some(&local_package),
                        &launch_env,
                        installer_mode,
                    )?
                } else {
                    if !host_can_build_installer_locally(&selected_rid) {
                        return Err(missing_remote_installer_error(
                            &selected_rid,
                            &published_installer_plan,
                            installer_mode,
                        ));
                    }
                    logline::info("Building online installer for remote deployment...");
                    build_installer_for_tailscale(
                        manifest.as_ref(),
                        &app_id,
                        &selected_rid,
                        release,
                        &channel,
                        &storage_config,
                        None,
                        &launch_env,
                        installer_mode,
                    )?
                };
                let installer_size = std::fs::metadata(&installer_path).map(|m| m.len()).unwrap_or(0);
                logline::info(&format!(
                    "Transferring installer to '{file_target}' ({})...",
                    crate::formatters::format_bytes(installer_size),
                ));
                stream_file_to_tailscale_node_with_command(
                    ssh_target,
                    &installer_path,
                    "cat > /tmp/.surge-installer && chmod +x /tmp/.surge-installer",
                )
                .await?;

                let no_start_flag = if no_start { " --no-start" } else { "" };
                let run_cmd = format!("/tmp/.surge-installer{no_start_flag} && rm -f /tmp/.surge-installer");
                let ssh_command = format!("sh -lc {}", shell_single_quote(&run_cmd));
                logline::info(&format!("Running installer on '{file_target}'..."));
                run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await?;
                logline::success(&format!("Installed '{app_id}' on tailscale node '{file_target}'."));
            }
        }
    }

    Ok(())
}

fn resolve_install_channel(
    manifest: &SurgeManifest,
    index: &ReleaseIndex,
    app_id: &str,
    explicit: Option<&str>,
) -> Result<ResolvedInstallChannel> {
    if let Some(channel) = explicit {
        return Ok(ResolvedInstallChannel {
            name: channel.to_string(),
            note: None,
        });
    }

    let available_channels = collect_available_channels(&index.releases);
    if available_channels.len() == 1 {
        let selected = available_channels[0].clone();
        return Ok(ResolvedInstallChannel {
            name: selected.clone(),
            note: Some(format!(
                "No --channel provided; single available channel '{selected}' selected automatically."
            )),
        });
    }
    if available_channels.len() > 1 {
        return Err(SurgeError::Config(format!(
            "Multiple channels available for app '{app_id}': {}. Specify --channel <name> to choose.",
            available_channels.join(", ")
        )));
    }

    let configured_channels = collect_configured_channels(manifest, app_id);
    if configured_channels.len() == 1 {
        let selected = configured_channels[0].clone();
        return Ok(ResolvedInstallChannel {
            name: selected.clone(),
            note: Some(format!(
                "No --channel provided; single configured channel '{selected}' selected automatically."
            )),
        });
    }
    if configured_channels.len() > 1 {
        return Err(SurgeError::Config(format!(
            "Multiple channels configured for app '{app_id}': {}. Specify --channel <name> to choose.",
            configured_channels.join(", ")
        )));
    }

    Ok(ResolvedInstallChannel {
        name: "stable".to_string(),
        note: Some("No channel metadata found; defaulting to 'stable'.".to_string()),
    })
}

fn prompt_install_channel(
    manifest: &SurgeManifest,
    index: &ReleaseIndex,
    app_id: &str,
    requested: Option<&str>,
) -> Result<ResolvedInstallChannel> {
    let options = collect_install_channel_options(manifest, index, app_id);
    let default_index = requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|channel| options.iter().position(|option| option == channel))
        .unwrap_or(0);
    let selected_index = prompt_choice_index("Select channel", &options, default_index)?;
    let selected = options[selected_index].clone();
    Ok(ResolvedInstallChannel {
        name: selected.clone(),
        note: Some(format!("Selected channel '{selected}' via install wizard.")),
    })
}

fn require_interactive_manifest(manifest: Option<&SurgeManifest>) -> Result<&SurgeManifest> {
    manifest.ok_or_else(|| SurgeError::Config("Interactive install requires an install manifest.".to_string()))
}

fn collect_install_channel_options(manifest: &SurgeManifest, index: &ReleaseIndex, app_id: &str) -> Vec<String> {
    let mut options = collect_available_channels(&index.releases);
    if options.is_empty() {
        options = collect_configured_channels(manifest, app_id);
    }
    if options.is_empty() {
        options.push("stable".to_string());
    }
    options
}

fn collect_configured_channels(manifest: &SurgeManifest, app_id: &str) -> Vec<String> {
    let mut channels = Vec::new();

    if let Some(app) = manifest.apps.iter().find(|app| app.id == app_id) {
        for channel in &app.channels {
            let trimmed = channel.trim();
            if !trimmed.is_empty() && !channels.iter().any(|existing| existing == trimmed) {
                channels.push(trimmed.to_string());
            }
        }
    }

    if channels.is_empty() {
        for channel in &manifest.channels {
            let trimmed = channel.name.trim();
            if !trimmed.is_empty() && !channels.iter().any(|existing| existing == trimmed) {
                channels.push(trimmed.to_string());
            }
        }
    }

    channels
}

fn collect_available_channels(releases: &[ReleaseEntry]) -> Vec<String> {
    let mut channels = BTreeSet::new();
    for release in releases {
        for channel in &release.channels {
            let trimmed = channel.trim();
            if !trimmed.is_empty() {
                channels.insert(trimmed.to_string());
            }
        }
    }
    channels.into_iter().collect()
}

fn should_prompt_install_selection() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn prompt_install_selection(
    manifest: &SurgeManifest,
    requested_app_id: Option<&str>,
    requested_rid: Option<&str>,
) -> Result<InstallSelection> {
    let mut app_ids = Vec::new();
    let mut app_labels = Vec::new();
    for app in &manifest.apps {
        let app_id = app.id.trim();
        if app_id.is_empty() || app_ids.iter().any(|existing: &String| existing == app_id) {
            continue;
        }
        app_ids.push(app_id.to_string());
        app_labels.push(prompts::format_app_label(manifest, app_id));
    }

    if app_ids.is_empty() {
        return Err(SurgeError::Config(
            "Manifest has no apps. Provide --app-id explicitly.".to_string(),
        ));
    }

    logline::title("Install target selection");
    let requested_app_id = requested_app_id.map(str::trim).filter(|value| !value.is_empty());
    let default_app_index = requested_app_id
        .and_then(|app_id| app_ids.iter().position(|candidate| candidate == app_id))
        .unwrap_or(0);
    let selected_app_index = prompt_choice_index("Select app", &app_labels, default_app_index)?;
    let selected_app_id = app_ids[selected_app_index].clone();

    let target_options = collect_target_options_for_app(manifest, &selected_app_id)?;
    if target_options.is_empty() {
        return Err(SurgeError::Config(format!(
            "App '{selected_app_id}' has no targets. Add targets to the manifest before install."
        )));
    }

    let selected_target = resolve_install_target_selection(&target_options, requested_rid)?;

    Ok(InstallSelection {
        app_id: selected_app_id,
        os: selected_target.os,
        rid: selected_target.rid,
    })
}

fn prompt_choice_index(prompt: &str, options: &[String], default_index: usize) -> Result<usize> {
    prompts::select(prompt, options, default_index)
}

fn resolve_install_target_selection(
    target_options: &[AppInstallTargetOption],
    requested_rid: Option<&str>,
) -> Result<AppInstallTargetOption> {
    if target_options.is_empty() {
        return Err(SurgeError::Config(
            "App has no target options. Add at least one target to the manifest.".to_string(),
        ));
    }

    if target_options.len() == 1 {
        return Ok(target_options[0].clone());
    }

    let requested_rid = requested_rid.map(str::trim).filter(|value| !value.is_empty());
    if let Some(requested_rid) = requested_rid {
        let mut matching = target_options.iter().filter(|option| option.rid == requested_rid);
        if let (Some(selected), None) = (matching.next(), matching.next()) {
            return Ok(selected.clone());
        }
    }

    let labels = target_options
        .iter()
        .map(format_target_option_label)
        .collect::<Vec<_>>();
    let default_index = requested_rid
        .and_then(|rid| target_options.iter().position(|option| option.rid == rid))
        .unwrap_or(0);
    let selected_index = prompt_choice_index("Select target", &labels, default_index)?;
    Ok(target_options[selected_index].clone())
}

fn format_target_option_label(option: &AppInstallTargetOption) -> String {
    let rid_parts: Vec<&str> = option.rid.split('-').collect();
    if rid_parts.len() >= 2 {
        format!("{}/{}", rid_parts[0], rid_parts[1])
    } else {
        option.rid.clone()
    }
}

fn collect_target_options_for_app(manifest: &SurgeManifest, app_id: &str) -> Result<Vec<AppInstallTargetOption>> {
    let mut options = Vec::new();
    let mut app_found = false;

    for app in &manifest.apps {
        if app.id != app_id {
            continue;
        }
        app_found = true;
        for target in app.target.iter().chain(app.targets.iter()) {
            let rid = target.rid.trim();
            if rid.is_empty() {
                continue;
            }
            let os = if target.os.trim().is_empty() {
                infer_os_from_rid(rid).unwrap_or_else(|| "unknown".to_string())
            } else {
                target.os.trim().to_ascii_lowercase()
            };
            let option = AppInstallTargetOption {
                os,
                rid: rid.to_string(),
            };
            if !options
                .iter()
                .any(|existing: &AppInstallTargetOption| existing == &option)
            {
                options.push(option);
            }
        }
    }

    if !app_found {
        return Err(SurgeError::Config(format!(
            "App '{app_id}' was not found in manifest. Provide --app-id with a valid app id."
        )));
    }

    Ok(options)
}

fn infer_os_from_rid(rid: &str) -> Option<String> {
    let prefix = rid.split('-').next()?.trim().to_ascii_lowercase();
    let normalized = match prefix.as_str() {
        "linux" => "linux",
        "win" | "windows" => "windows",
        "osx" | "macos" | "darwin" => "macos",
        _ => return None,
    };
    Some(normalized.to_string())
}

fn resolve_tailscale_targets(node: &str, node_user: Option<&str>) -> Result<(String, String)> {
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

pub(crate) fn selected_install_manifest_path<'a>(
    application_manifest_path: &'a Path,
    fallback_manifest_path: &'a Path,
) -> &'a Path {
    if application_manifest_path.is_file() {
        application_manifest_path
    } else {
        fallback_manifest_path
    }
}

fn load_install_manifest_if_available(path: &Path) -> Result<Option<SurgeManifest>> {
    if path.is_file() {
        SurgeManifest::from_file(path).map(Some)
    } else {
        Ok(None)
    }
}

fn install_override_value(scope: &Path, explicit: Option<&str>, env_key: &str, app_id: Option<&str>) -> Option<String> {
    explicit
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| crate::envfile::storage_env_lookup(env_key, scope, app_id))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn build_storage_config_without_manifest(
    scope: &Path,
    app_id: Option<&str>,
    overrides: StorageOverrides<'_>,
) -> Result<surge_core::context::StorageConfig> {
    let provider =
        install_override_value(scope, overrides.provider, "SURGE_STORAGE_PROVIDER", app_id).ok_or_else(|| {
            SurgeError::Config(
                "No install manifest was found. For manifestless install, provide --provider or SURGE_STORAGE_PROVIDER."
                    .to_string(),
            )
        })?;
    let bucket = install_override_value(scope, overrides.bucket, "SURGE_STORAGE_BUCKET", app_id).ok_or_else(|| {
        SurgeError::Config(
            "No install manifest was found. For manifestless install, provide --bucket or SURGE_STORAGE_BUCKET."
                .to_string(),
        )
    })?;
    let region = install_override_value(scope, overrides.region, "SURGE_STORAGE_REGION", app_id).unwrap_or_default();
    let endpoint =
        install_override_value(scope, overrides.endpoint, "SURGE_STORAGE_ENDPOINT", app_id).unwrap_or_default();
    let prefix = install_override_value(scope, overrides.prefix, "SURGE_STORAGE_PREFIX", app_id).unwrap_or_default();

    let provider = super::parse_storage_provider(&provider)?;
    let credentials = surge_core::storage_config::storage_credentials_from_lookup(provider, |name| {
        crate::envfile::storage_env_lookup(name, scope, app_id)
    });

    Ok(surge_core::context::StorageConfig {
        provider: Some(provider),
        bucket,
        region,
        access_key: credentials.access_key,
        secret_key: credentials.secret_key,
        endpoint,
        prefix,
    })
}

fn resolve_install_app_id_without_manifest(
    explicit_app_id: Option<String>,
    index: &ReleaseIndex,
) -> Result<(String, Option<String>)> {
    if let Some(app_id) = explicit_app_id {
        return Ok((app_id, None));
    }

    let app_id = index.app_id.trim();
    if !app_id.is_empty() {
        return Ok((
            app_id.to_string(),
            Some(format!(
                "No install manifest was found; using app id '{app_id}' from the release index."
            )),
        ));
    }

    Err(SurgeError::Config(
        "No install manifest was found and the release index does not declare an app id. Provide --app-id.".to_string(),
    ))
}

fn resolve_tailscale_rid_without_manifest(
    explicit_rid: Option<&str>,
    index: &ReleaseIndex,
) -> Result<(String, Option<String>)> {
    if let Some(rid) = explicit_rid.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok((rid.to_string(), None));
    }

    let mut rids = index
        .releases
        .iter()
        .map(|release| release.rid.trim())
        .filter(|rid| !rid.is_empty())
        .collect::<Vec<_>>();
    rids.sort_unstable();
    rids.dedup();

    match rids.as_slice() {
        [rid] => Ok((
            (*rid).to_string(),
            Some(format!(
                "No install manifest was found; using the only RID '{rid}' advertised by the release index."
            )),
        )),
        [] => Err(SurgeError::Config(
            "No install manifest was found and the release index does not advertise a concrete RID. Provide --rid."
                .to_string(),
        )),
        _ => Err(SurgeError::Config(format!(
            "No install manifest was found and the release index advertises multiple RIDs ({}). Provide --rid.",
            rids.join(", ")
        ))),
    }
}

fn resolve_install_channel_without_manifest(
    index: &ReleaseIndex,
    explicit: Option<&str>,
) -> Result<ResolvedInstallChannel> {
    if let Some(channel) = explicit {
        return Ok(ResolvedInstallChannel {
            name: channel.to_string(),
            note: None,
        });
    }

    let available_channels = collect_available_channels(&index.releases);
    if available_channels.len() == 1 {
        let selected = available_channels[0].clone();
        return Ok(ResolvedInstallChannel {
            name: selected.clone(),
            note: Some(format!(
                "No --channel provided; single available channel '{selected}' selected automatically."
            )),
        });
    }
    if available_channels.len() > 1 {
        return Err(SurgeError::Config(format!(
            "Multiple channels are available in the release index: {}. Specify --channel <name> to choose.",
            available_channels.join(", ")
        )));
    }

    Ok(ResolvedInstallChannel {
        name: "stable".to_string(),
        note: Some(
            "No --channel provided and the release index has no channel metadata; defaulting to 'stable'.".to_string(),
        ),
    })
}

fn release_install_profile<'a>(app_id: &'a str, release: &'a ReleaseEntry) -> InstallProfile<'a> {
    InstallProfile::new(
        app_id,
        release.display_name(app_id),
        &release.main_exe,
        &release.install_directory,
        &release.supervisor_id,
        &release.icon,
        &release.shortcuts,
        &release.environment,
    )
}

async fn stop_running_supervisor(app_id: &str, release: &ReleaseEntry) -> Result<()> {
    let supervisor_id = release.supervisor_id.trim();
    if supervisor_id.is_empty() {
        return Ok(());
    }

    let install_root = surge_core::platform::paths::default_install_root(app_id, &release.install_directory)?;
    super::stop_supervisor(&install_root, supervisor_id).await
}

fn install_package_locally(app_id: &str, release: &ReleaseEntry, package_path: &Path) -> Result<std::path::PathBuf> {
    let profile = release_install_profile(app_id, release);
    core_install::install_package_locally(&profile, package_path)
}

fn auto_start_after_install(
    release: &ReleaseEntry,
    app_id: &str,
    install_root: &std::path::Path,
    active_app_dir: &std::path::Path,
) -> Result<u32> {
    let profile = release_install_profile(app_id, release);
    core_install::auto_start_after_install_sequence(&profile, install_root, active_app_dir, &release.version)
}

fn release_runtime_manifest_metadata<'a>(
    release: &'a ReleaseEntry,
    channel: &'a str,
    storage_config: &'a surge_core::context::StorageConfig,
) -> core_install::RuntimeManifestMetadata<'a> {
    core_install::RuntimeManifestMetadata::new(
        &release.version,
        channel,
        core_install::storage_provider_manifest_name(storage_config.provider),
        &storage_config.bucket,
        &storage_config.region,
        &storage_config.endpoint,
    )
}

fn remote_installer_extension_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "exe"
    } else {
        "bin"
    }
}

fn host_can_build_installer_locally(rid: &str) -> bool {
    super::pack::ensure_host_compatible_rid(rid).is_ok()
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

fn plan_remote_published_installer(
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

fn plan_remote_published_installer_without_manifest(
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

fn missing_remote_installer_error(
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

fn published_installer_public_url(storage_config: &surge_core::context::StorageConfig, key: &str) -> Option<String> {
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

async fn try_prepare_published_installer_for_tailscale(
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

fn build_storage_config_with_overrides(
    manifest: &SurgeManifest,
    manifest_path: &Path,
    app_id: &str,
    overrides: StorageOverrides<'_>,
) -> Result<surge_core::context::StorageConfig> {
    let mut config = super::build_app_scoped_storage_config(manifest, manifest_path, app_id)?;

    if let Some(provider) = install_override_value(
        manifest_path,
        overrides.provider,
        "SURGE_STORAGE_PROVIDER",
        Some(app_id),
    ) {
        config.provider = Some(super::parse_storage_provider(&provider)?);
    }
    if let Some(bucket) = install_override_value(manifest_path, overrides.bucket, "SURGE_STORAGE_BUCKET", Some(app_id))
    {
        config.bucket = bucket;
    }
    if let Some(region) = install_override_value(manifest_path, overrides.region, "SURGE_STORAGE_REGION", Some(app_id))
    {
        config.region = region;
    }
    if let Some(endpoint) = install_override_value(
        manifest_path,
        overrides.endpoint,
        "SURGE_STORAGE_ENDPOINT",
        Some(app_id),
    ) {
        config.endpoint = endpoint;
    }
    if let Some(prefix) = install_override_value(manifest_path, overrides.prefix, "SURGE_STORAGE_PREFIX", Some(app_id))
    {
        config.prefix = prefix;
    }

    Ok(config)
}

fn make_progress_bar(message: &str, total: u64) -> Option<ProgressBar> {
    if !std::io::stdout().is_terminal() {
        return None;
    }

    let bar = ProgressBar::new(total);
    let style = ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> ");
    bar.set_style(style);
    bar.set_message(message.to_string());
    Some(bar)
}

fn make_spinner(message: &str) -> Option<ProgressBar> {
    if !std::io::stdout().is_terminal() {
        return None;
    }

    let spinner = ProgressBar::new_spinner();
    let style = ProgressStyle::with_template("{spinner} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_chars("|/-\\ ");
    spinner.set_style(style);
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    Some(spinner)
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<(ReleaseIndex, bool)> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => Ok((decompress_release_index(&data)?, true)),
        Err(SurgeError::NotFound(_)) => Ok((ReleaseIndex::default(), false)),
        Err(e) => Err(e),
    }
}

async fn download_release_archive(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    release: &ReleaseEntry,
    rid_candidates: &[String],
    full_filename: &str,
    destination: &Path,
) -> Result<ArchiveAcquisition> {
    struct FetchProgressUi {
        verify_spinner: Option<ProgressBar>,
        transfer_bar: Option<ProgressBar>,
    }

    let expected_sha256 = release.full_sha256.trim();
    let ui_state = Arc::new(Mutex::new(FetchProgressUi {
        verify_spinner: if destination.is_file() && !expected_sha256.is_empty() {
            make_spinner("Verifying cached package integrity")
        } else {
            None
        },
        transfer_bar: None,
    }));
    let ui_state_for_progress = Arc::clone(&ui_state);
    let total_hint = u64::try_from(release.full_size.max(0)).unwrap_or(0);
    let transfer_progress: Box<TransferProgress> = Box::new(move |done: u64, total: u64| {
        let mut ui = ui_state_for_progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(spinner) = ui.verify_spinner.take() {
            spinner.finish_and_clear();
        }
        if ui.transfer_bar.is_none() {
            let initial_total = if total > 0 { total } else { total_hint };
            ui.transfer_bar = make_progress_bar("Fetching full package", initial_total);
        }
        if let Some(bar) = ui.transfer_bar.as_ref() {
            if total > 0 {
                bar.set_length(total);
            }
            bar.set_position(done);
        }
    });
    let fetch_result = fetch_or_reuse_file(
        backend,
        full_filename,
        destination,
        &release.full_sha256,
        Some(transfer_progress.as_ref()),
    )
    .await;
    let (verify_spinner, direct_fetch_bar) = {
        let mut ui = ui_state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        (ui.verify_spinner.take(), ui.transfer_bar.take())
    };
    if let Some(spinner) = verify_spinner {
        spinner.finish_and_clear();
    }
    if let Some(bar) = direct_fetch_bar {
        bar.finish_and_clear();
    }

    match fetch_result {
        Ok(CacheFetchOutcome::ReusedLocal) => Ok(ArchiveAcquisition::ReusedLocal),
        Ok(CacheFetchOutcome::DownloadedFresh | CacheFetchOutcome::DownloadedAfterInvalidLocal) => {
            Ok(ArchiveAcquisition::Downloaded)
        }
        Err(SurgeError::NotFound(_)) => {
            let restore_rid = if release.rid.trim().is_empty() {
                rid_candidates.first().map_or("", String::as_str)
            } else {
                release.rid.as_str()
            };
            let restore_bar = make_progress_bar("Rebuilding full package from release graph", 0);
            let restore_bar_for_progress = restore_bar.clone();
            let progress = |p: RestoreProgress| {
                if let Some(bar) = &restore_bar_for_progress {
                    if p.bytes_total > 0 {
                        bar.set_length(u64::try_from(p.bytes_total).unwrap_or(0));
                        bar.set_position(u64::try_from(p.bytes_done).unwrap_or(0));
                    } else if p.items_total > 0 {
                        bar.set_length(u64::try_from(p.items_total).unwrap_or(0));
                        bar.set_position(u64::try_from(p.items_done).unwrap_or(0));
                    }
                    bar.set_message(format!(
                        "Rebuilding full package from release graph ({}/{})",
                        p.items_done, p.items_total
                    ));
                } else {
                    logline::subtle(&format!(
                        "  Rebuilding full package from release graph [{}/{}] {} / {} bytes",
                        p.items_done, p.items_total, p.bytes_done, p.bytes_total
                    ));
                }
            };
            let rebuilt = restore_full_archive_for_version_with_options(
                backend,
                index,
                restore_rid,
                &release.version,
                RestoreOptions {
                    cache_dir: destination.parent(),
                    progress: Some(&progress),
                },
            )
            .await?;
            if let Some(bar) = &restore_bar {
                bar.finish_and_clear();
            }
            std::fs::write(destination, rebuilt)?;
            Ok(ArchiveAcquisition::Reconstructed)
        }
        Err(e) => Err(e),
    }
}

fn select_release<'a>(
    releases: &'a [ReleaseEntry],
    channel: &str,
    version: Option<&str>,
    rid_candidates: &[String],
    selected_os: Option<&str>,
) -> Option<&'a ReleaseEntry> {
    let mut eligible: Vec<&ReleaseEntry> = releases
        .iter()
        .filter(|release| release.channels.iter().any(|c| c == channel))
        .collect();

    if let Some(version) = version.map(str::trim).filter(|v| !v.is_empty()) {
        eligible.retain(|release| release.version == version);
    }

    if let Some(os) = selected_os.map(str::trim).filter(|value| !value.is_empty()) {
        let os = os.to_ascii_lowercase();
        eligible.retain(|release| release_os(release).is_some_and(|release_os| release_os == os));
    }

    if eligible.is_empty() {
        return None;
    }

    for rid in rid_candidates {
        let mut by_rid: Vec<&ReleaseEntry> = eligible.iter().copied().filter(|release| release.rid == *rid).collect();
        by_rid.sort_by(|a, b| compare_versions(&b.version, &a.version));
        if let Some(best) = by_rid.first() {
            return Some(*best);
        }
    }

    let mut generic: Vec<&ReleaseEntry> = eligible
        .iter()
        .copied()
        .filter(|release| release.rid.trim().is_empty())
        .collect();
    generic.sort_by(|a, b| compare_versions(&b.version, &a.version));
    generic.first().copied()
}

fn release_os(release: &ReleaseEntry) -> Option<String> {
    if let Some(os) = normalize_release_os(&release.os) {
        return Some(os.to_string());
    }
    infer_os_from_rid(&release.rid)
}

fn normalize_release_os(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "linux" => Some("linux"),
        "win" | "windows" => Some("windows"),
        "osx" | "macos" | "darwin" => Some("macos"),
        _ => None,
    }
}

fn detect_local_profile() -> RuntimeProfile {
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();
    let gpu = if has_local_nvidia_gpu() {
        "nvidia".to_string()
    } else {
        "none".to_string()
    };
    RuntimeProfile { os, arch, gpu }
}

fn has_local_nvidia_gpu() -> bool {
    std::process::Command::new("nvidia-smi")
        .arg("-L")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn warn_if_local_rid_looks_incompatible(rid: &str, profile: &RuntimeProfile) {
    for warning in local_rid_incompatibility_warnings(rid, profile) {
        logline::warn(&warning);
    }
}

fn local_rid_incompatibility_warnings(rid: &str, profile: &RuntimeProfile) -> Vec<String> {
    let Some(selected) = parse_rid_signature(rid) else {
        return Vec::new();
    };
    let Some(local_os) = normalize_os(&profile.os) else {
        return Vec::new();
    };
    let Some(local_arch) = normalize_arch(&profile.arch) else {
        return Vec::new();
    };

    let mut warnings = Vec::new();
    if selected.os != local_os {
        warnings.push(format!(
            "Selected RID '{rid}' targets OS '{}', but local host OS appears '{}'.",
            selected.os, local_os
        ));
    }
    if selected.arch != local_arch {
        warnings.push(format!(
            "Selected RID '{rid}' targets architecture '{}', but local host architecture appears '{}'.",
            selected.arch, local_arch
        ));
    }
    if selected.has_gpu_hint && !profile.has_nvidia_gpu() {
        warnings.push(format!(
            "Selected RID '{rid}' implies GPU acceleration, but no local NVIDIA GPU was detected."
        ));
    }
    warnings
}

fn parse_rid_signature(rid: &str) -> Option<RidSignature> {
    let mut parts = rid.trim().split('-');
    let raw_os = parts.next()?.trim().to_ascii_lowercase();
    let os = match raw_os.as_str() {
        "linux" => "linux",
        "win" | "windows" => "win",
        "osx" | "macos" | "darwin" => "osx",
        _ => normalize_os(raw_os.as_str())?,
    };
    let arch = normalize_arch(parts.next()?)?;
    let has_gpu_hint = parts.any(|part| {
        let part = part.trim().to_ascii_lowercase();
        part == "cuda" || part == "nvidia" || part == "gpu"
    });
    Some(RidSignature { os, arch, has_gpu_hint })
}

fn derive_base_rid(profile: &RuntimeProfile) -> Option<String> {
    let os = normalize_os(&profile.os)?;
    let arch = normalize_arch(&profile.arch)?;
    Some(format!("{os}-{arch}"))
}

fn normalize_os(raw: &str) -> Option<&'static str> {
    let os = raw.trim().to_ascii_lowercase();
    if os.contains("linux") {
        Some("linux")
    } else if os.contains("darwin") || os.contains("mac") {
        Some("osx")
    } else if os.contains("windows") || os.contains("mingw") || os.contains("msys") {
        Some("win")
    } else {
        None
    }
}

fn normalize_arch(raw: &str) -> Option<&'static str> {
    let arch = raw.trim().to_ascii_lowercase();
    if arch == "x86_64" || arch == "amd64" || arch == "x64" {
        Some("x64")
    } else if arch == "aarch64" || arch == "arm64" {
        Some("arm64")
    } else if arch == "x86" || arch == "i386" || arch == "i686" {
        Some("x86")
    } else {
        None
    }
}

fn build_rid_candidates(base_rid: &str, nvidia_gpu: bool) -> Vec<String> {
    let mut candidates: Vec<String> = Vec::new();
    let mut push_unique = |candidate: String| {
        if !candidates.iter().any(|existing| existing == &candidate) {
            candidates.push(candidate);
        }
    };

    if nvidia_gpu {
        push_unique(format!("{base_rid}-nvidia"));
        push_unique(format!("{base_rid}-cuda"));
        push_unique(format!("{base_rid}-gpu"));
    }
    push_unique(base_rid.to_string());
    if !nvidia_gpu {
        push_unique(format!("{base_rid}-cpu"));
    }

    candidates
}

fn shell_single_quote(raw: &str) -> String {
    let mut escaped = String::from("'");
    for ch in raw.chars() {
        if ch == '\'' {
            escaped.push_str("'\"'\"'");
        } else {
            escaped.push(ch);
        }
    }
    escaped.push('\'');
    escaped
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

fn build_remote_installer_manifest(
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
fn build_installer_for_tailscale(
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

    let surge_binary = super::pack::find_surge_binary_for_rid(rid)?;
    let surge_name = super::pack::surge_binary_name_for_rid(rid);
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

    let launcher = super::pack::find_installer_launcher_for_rid(rid, None)?;
    let installer_path = staging_dir.path().join("surge-offline-installer");
    surge_core::installer_bundle::write_embedded_installer(&launcher, payload_archive.path(), &installer_path)?;
    surge_core::platform::fs::make_executable(&installer_path)?;

    // Keep the tempdir alive until the process exits
    std::mem::forget(staging_dir);

    Ok(installer_path)
}

fn remote_install_root(home: &Path, app_id: &str, install_directory: &str) -> Result<PathBuf> {
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

fn remote_linux_shortcut_icon_path(
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

fn build_remote_app_copy_activation_script(
    install_root: &Path,
    main_exe: &str,
    version: &str,
    environment: &BTreeMap<String, String>,
    no_start: bool,
) -> String {
    let install_root_quoted = shell_single_quote(&install_root.to_string_lossy());
    let main_exe_quoted = shell_single_quote(main_exe);
    let version_quoted = shell_single_quote(version);
    let exports = shell_export_lines(environment);

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
rm -rf \"$previous_app_dir\"\n\
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

    script
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
async fn deploy_remote_app_copy_for_tailscale(
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
) -> Result<()> {
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

    let remote_home = detect_remote_home_directory(ssh_target).await?;
    let install_root = remote_install_root(&remote_home, app_id, &release.install_directory)?;
    let active_app_dir = install_root.join("app");
    let runtime_environment = build_remote_runtime_environment(release, launch_env);
    let main_exe_name = if release.main_exe.trim().is_empty() {
        app_id
    } else {
        release.main_exe.trim()
    };

    let staging_dir =
        tempfile::tempdir().map_err(|e| SurgeError::Platform(format!("Failed to create staging directory: {e}")))?;
    let stage_root = staging_dir.path().join("remote-stage");
    let stage_app_dir = stage_root.join("app");
    surge_core::archive::extractor::extract_file_to(&local_package, &stage_app_dir)?;

    let install_profile = release_install_profile(app_id, release);
    let runtime_manifest = release_runtime_manifest_metadata(release, channel, storage_config);
    core_install::write_runtime_manifest(&stage_app_dir, &install_profile, &runtime_manifest)?;

    if !release.shortcuts.is_empty() {
        let icon_path =
            remote_linux_shortcut_icon_path(&stage_app_dir, &active_app_dir, app_id, main_exe_name, &release.icon);
        let rendered = surge_core::platform::shortcuts::render_linux_shortcut_files(
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

    let activation_script = build_remote_app_copy_activation_script(
        &install_root,
        main_exe_name,
        &release.version,
        &runtime_environment,
        no_start,
    );
    let ssh_command = format!("sh -lc {}", shell_single_quote(&activation_script));
    logline::info(&format!("Activating remote install on '{file_target}'..."));
    run_tailscale_streaming(&["ssh", ssh_target, ssh_command.as_str()], "remote").await
}

fn select_remote_installer_mode(storage_config: &surge_core::context::StorageConfig) -> RemoteInstallerMode {
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

async fn stream_file_to_tailscale_node_with_command(node: &str, local_file: &Path, remote_command: &str) -> Result<()> {
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

async fn check_remote_install_state(ssh_node: &str, install_dir: &str) -> Option<RemoteInstallState> {
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

fn parse_remote_install_state(output: &str) -> Option<RemoteInstallState> {
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

async fn detect_remote_launch_environment(ssh_node: &str) -> RemoteLaunchEnvironment {
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

fn remote_launch_environment_probe() -> &'static str {
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

fn parse_remote_launch_environment(output: &str) -> RemoteLaunchEnvironment {
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

fn remote_install_matches(
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

fn should_skip_remote_install(install_matches: bool, force: bool) -> bool {
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

async fn run_tailscale_streaming(args: &[&str], prefix: &str) -> Result<()> {
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

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap, clippy::similar_names)]

    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use super::*;
    use surge_core::archive::extractor::read_entry;
    use surge_core::archive::packer::ArchivePacker;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::config::manifest::ShortcutLocation;
    use surge_core::config::manifest::SurgeManifest;
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
            false,
        );

        assert!(script.contains("export DISPLAY=':0'"));
        assert!(script.contains("export DBUS_SESSION_BUS_ADDRESS='unix:path=/run/user/1000/bus'"));
        assert!(script.contains("--surge-installed \"$version\""));
        assert!(script.contains("--surge-first-run \"$version\""));
        assert!(script.contains("kill_matching \"$install_root/$main_exe\""));
        assert!(script.contains("kill_matching \"$install_root/app-\""));
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
    fn plan_remote_published_installer_uses_default_channel_key() {
        let manifest = remote_manifest("demo", "linux-arm64", &["test", "production"], &["online"]);
        let mut entry = release("1.2.3", "test", "linux-arm64", "demo.tar.zst");
        entry.installers = vec!["online".to_string()];

        let plan = plan_remote_published_installer(
            &manifest,
            "demo",
            "linux-arm64",
            "test",
            &entry,
            RemoteInstallerMode::Online,
        )
        .expect("plan should resolve");

        assert_eq!(
            plan.candidate_keys,
            vec!["installers/Setup-linux-arm64-demo-test-online.bin".to_string()]
        );
        assert!(plan.blockers.is_empty(), "unexpected blockers: {:?}", plan.blockers);
    }

    #[test]
    fn plan_remote_published_installer_reports_channel_mismatch() {
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
            vec!["installers/Setup-linux-arm64-demo-test-online.bin".to_string()]
        );
        assert!(
            plan.blockers
                .iter()
                .any(|blocker| blocker.contains("default channel 'test'")),
            "missing channel mismatch blocker: {:?}",
            plan.blockers
        );
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

    #[test]
    fn missing_remote_installer_error_mentions_keys_and_host_mismatch() {
        let err = missing_remote_installer_error(
            "linux-arm64",
            &RemotePublishedInstallerPlan {
                candidate_keys: vec!["installers/Setup-linux-arm64-demo-test-online.bin".to_string()],
                blockers: vec!["published installer was not found in storage".to_string()],
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
            false,
            true,
            false,
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
        let state = parse_remote_install_state("version=1.2.3\nchannel=production\n")
            .expect("remote install state should parse");
        assert_eq!(state.version, "1.2.3");
        assert_eq!(state.channel.as_deref(), Some("production"));
    }

    #[test]
    fn parse_remote_install_state_requires_version() {
        assert!(parse_remote_install_state("channel=test\n").is_none());
    }

    #[test]
    fn remote_install_matches_requires_matching_channel_and_version() {
        let production = RemoteInstallState {
            version: "1.2.3".to_string(),
            channel: Some("production".to_string()),
        };
        let test = RemoteInstallState {
            version: "1.2.3".to_string(),
            channel: Some("test".to_string()),
        };

        assert!(remote_install_matches(Some(&production), "1.2.3", "production"));
        assert!(!remote_install_matches(Some(&production), "1.2.4", "production"));
        assert!(!remote_install_matches(Some(&test), "1.2.3", "production"));
        assert!(!remote_install_matches(None, "1.2.3", "production"));
    }

    #[test]
    fn force_flag_bypasses_remote_install_skip() {
        let remote_state = RemoteInstallState {
            version: "1.2.3".to_string(),
            channel: Some("test".to_string()),
        };

        let install_matches = remote_install_matches(Some(&remote_state), "1.2.3", "test");

        assert!(should_skip_remote_install(install_matches, false));
        assert!(!should_skip_remote_install(install_matches, true));
    }
}
