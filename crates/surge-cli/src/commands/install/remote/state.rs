use super::execution::{detect_remote_home_directory, run_tailscale_capture};
use super::staging::{
    remote_install_root, remote_staged_app_copy_files_exist, remote_staged_installer_cache_files_exist,
};
use super::types::{
    RemoteConvergenceAction, RemoteConvergencePlan, RemoteHostInstallerAvailability, RemoteInstallState,
    RemoteInstallerMode, RemoteLaunchEnvironment, RemoteStagedPayloadIdentity, RemoteTailscaleCachedState,
    RemoteTailscaleOperation, RemoteTailscaleTransferInputs, RemoteTailscaleTransferStrategy, VerifiedRemoteStage,
};
use super::{
    Path, ReleaseEntry, ReleaseIndex, Result, SurgeError, compare_versions, core_install,
    host_can_build_installer_locally, logline, shell_single_quote,
};
use surge_core::update::manager::plan_update_from_index;

pub(crate) fn select_remote_installer_mode(storage_config: &surge_core::context::StorageConfig) -> RemoteInstallerMode {
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

pub(crate) fn select_remote_tailscale_transfer_strategy(
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

pub(crate) async fn check_remote_install_state(ssh_node: &str, install_dir: &str) -> Option<RemoteInstallState> {
    let probe = format!(
        r#"manifest="$HOME/.local/share/{}/app/.surge/runtime.yml";
if [ ! -f "$manifest" ]; then
  exit 0
fi
version="$(sed -n 's/^version:[[:space:]]*//p' "$manifest" | head -n1)"
channel="$(sed -n 's/^channel:[[:space:]]*//p' "$manifest" | head -n1)"
provider="$(sed -n 's/^provider:[[:space:]]*//p' "$manifest" | head -n1)"
bucket="$(sed -n 's/^bucket:[[:space:]]*//p' "$manifest" | head -n1)"
region="$(sed -n 's/^region:[[:space:]]*//p' "$manifest" | head -n1)"
endpoint="$(sed -n 's/^endpoint:[[:space:]]*//p' "$manifest" | head -n1)"
printf 'version=%s\nchannel=%s\nprovider=%s\nbucket=%s\nregion=%s\nendpoint=%s\n' "$version" "$channel" "$provider" "$bucket" "$region" "$endpoint""#,
        install_dir.replace('\'', ""),
    );
    let command = format!("sh -c {}", shell_single_quote(&probe));
    match run_tailscale_capture(&["ssh", ssh_node, command.as_str()]).await {
        Ok(output) => parse_remote_install_state(&output),
        Err(_) => None,
    }
}

pub(crate) fn parse_remote_install_state(output: &str) -> Option<RemoteInstallState> {
    let mut version = None;
    let mut channel = None;
    let mut storage_provider = None;
    let mut storage_bucket = None;
    let mut storage_region = None;
    let mut storage_endpoint = None;

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
            continue;
        }

        if let Some(value) = line.strip_prefix("provider=") {
            let value = value.trim();
            if !value.is_empty() {
                storage_provider = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("bucket=") {
            let value = value.trim();
            if !value.is_empty() {
                storage_bucket = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("region=") {
            let value = value.trim();
            if !value.is_empty() {
                storage_region = Some(value.to_string());
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("endpoint=") {
            let value = value.trim();
            if !value.is_empty() {
                storage_endpoint = Some(value.to_string());
            }
        }
    }

    version.map(|version| RemoteInstallState {
        version,
        channel,
        storage_provider,
        storage_bucket,
        storage_region,
        storage_endpoint,
    })
}

pub(crate) fn plan_remote_convergence(
    remote_state: Option<&RemoteInstallState>,
    index: &ReleaseIndex,
    app_id: &str,
    selected_rid: &str,
    release: &ReleaseEntry,
    channel: &str,
    storage_config: &surge_core::context::StorageConfig,
    installer_mode: RemoteInstallerMode,
    force: bool,
) -> Result<RemoteConvergencePlan> {
    let Some(remote_state) = remote_state else {
        return Ok(RemoteConvergencePlan::clean_install(release));
    };

    let installed_version = Some(remote_state.version.clone());
    let metadata_matches = remote_state.metadata_matches(channel, storage_config);
    match compare_versions(&remote_state.version, &release.version) {
        std::cmp::Ordering::Equal if metadata_matches && !force => Ok(RemoteConvergencePlan {
            action: RemoteConvergenceAction::Skip,
            installed_version,
            target_version: release.version.clone(),
            update_info: None,
            reason: Some("installed version, channel, and storage metadata already match".to_string()),
        }),
        std::cmp::Ordering::Equal if !force => Ok(RemoteConvergencePlan {
            action: RemoteConvergenceAction::RepairMetadata,
            installed_version,
            target_version: release.version.clone(),
            update_info: None,
            reason: Some("installed version matches but runtime channel or storage metadata is stale".to_string()),
        }),
        std::cmp::Ordering::Greater if !force => Err(SurgeError::Update(format!(
            "Remote install is already on newer version {} than selected target {}. Use --force to reinstall the selected version.",
            remote_state.version, release.version
        ))),
        std::cmp::Ordering::Less if installer_mode == RemoteInstallerMode::Online && !force => {
            let update_info = plan_update_from_index(
                index,
                app_id,
                &remote_state.version,
                channel,
                Some(&release.version),
                selected_rid,
            )?;
            let reason = update_info.as_ref().and_then(|info| info.fallback_reason.clone());
            Ok(RemoteConvergencePlan {
                action: RemoteConvergenceAction::Update,
                installed_version,
                target_version: release.version.clone(),
                reason: if update_info.is_some() {
                    reason
                } else {
                    Some("release graph did not produce an exact update plan".to_string())
                },
                update_info,
            })
        }
        _ => Ok(RemoteConvergencePlan {
            action: RemoteConvergenceAction::Reinstall,
            installed_version,
            target_version: release.version.clone(),
            update_info: None,
            reason: if force {
                Some("--force requested reinstall".to_string())
            } else {
                Some("remote update requires online storage; using full install transfer".to_string())
            },
        }),
    }
}

pub(crate) fn remote_staged_payload_identity(
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

pub(crate) fn parse_remote_staged_payload_identity(output: &str) -> Option<RemoteStagedPayloadIdentity> {
    serde_json::from_str(output.trim()).ok()
}

pub(crate) async fn remote_staged_payload_matches_release(
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

pub(crate) async fn remote_staged_installer_matches_release(
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

pub(crate) async fn verify_remote_stage_readiness(
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

pub(crate) async fn detect_remote_launch_environment(ssh_node: &str) -> RemoteLaunchEnvironment {
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

pub(crate) fn remote_launch_environment_probe() -> &'static str {
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

pub(crate) fn parse_remote_launch_environment(output: &str) -> RemoteLaunchEnvironment {
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

pub(crate) fn remote_install_matches(
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

#[cfg(test)]
pub(crate) fn should_skip_remote_install(install_matches: bool, force: bool) -> bool {
    install_matches && !force
}

pub(crate) async fn check_remote_staged_payload_identity(
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

pub(crate) async fn check_remote_staged_installer_identity(
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
