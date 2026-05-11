use super::{Deserialize, ReleaseEntry, Result, Serialize, SurgeError, infer_os_from_rid, logline};
use surge_core::config::manifest::CacheManifestConfig;
use surge_core::context::StorageConfig;
use surge_core::install::storage_provider_manifest_name;
use surge_core::update::manager::UpdateInfo;

pub(crate) fn ensure_supported_tailscale_rid(rid: &str) -> Result<()> {
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
pub(crate) struct RemoteLaunchEnvironment {
    pub(crate) display: Option<String>,
    pub(crate) xauthority: Option<String>,
    pub(crate) dbus_session_bus_address: Option<String>,
    pub(crate) wayland_display: Option<String>,
    pub(crate) xdg_runtime_dir: Option<String>,
}

impl RemoteLaunchEnvironment {
    pub(crate) fn has_graphical_session(&self) -> bool {
        self.display.is_some() || self.wayland_display.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteInstallerMode {
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteTailscaleTransferStrategy {
    AppCopy,
    StagedInstallerCache,
    Installer { prefer_published: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteHostInstallerAvailability {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteTailscaleOperation {
    Stage,
    Install,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteTailscaleCachedState {
    None,
    AppCopyPayload,
    InstallerCache,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifiedRemoteStage {
    AppCopyPayload,
    InstallerCache,
}

impl VerifiedRemoteStage {
    pub(crate) fn description(self) -> &'static str {
        match self {
            Self::AppCopyPayload => "staged app payload",
            Self::InstallerCache => "staged installer cache",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RemoteTailscaleTransferInputs {
    pub(crate) host_installer_availability: RemoteHostInstallerAvailability,
    pub(crate) installer_mode: RemoteInstallerMode,
    pub(crate) operation: RemoteTailscaleOperation,
    pub(crate) cached_state: RemoteTailscaleCachedState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteInstallState {
    pub(crate) version: String,
    pub(crate) channel: Option<String>,
    pub(crate) storage_provider: Option<String>,
    pub(crate) storage_bucket: Option<String>,
    pub(crate) storage_region: Option<String>,
    pub(crate) storage_endpoint: Option<String>,
}

impl RemoteInstallState {
    pub(crate) fn metadata_matches(&self, expected_channel: &str, storage_config: &StorageConfig) -> bool {
        self.channel
            .as_deref()
            .is_some_and(|value| value.trim() == expected_channel)
            && self
                .storage_provider
                .as_deref()
                .is_some_and(|value| value.trim() == storage_provider_manifest_name(storage_config.provider))
            && self
                .storage_bucket
                .as_deref()
                .is_some_and(|value| value.trim() == storage_config.bucket.trim())
            && self.storage_region.as_deref().unwrap_or("").trim() == storage_config.region.trim()
            && self.storage_endpoint.as_deref().unwrap_or("").trim() == storage_config.endpoint.trim()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteConvergenceAction {
    CleanInstall,
    Update,
    RepairMetadata,
    Reinstall,
    Skip,
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteConvergencePlan {
    pub(crate) action: RemoteConvergenceAction,
    pub(crate) installed_version: Option<String>,
    pub(crate) target_version: String,
    pub(crate) update_info: Option<UpdateInfo>,
    pub(crate) reason: Option<String>,
}

impl RemoteConvergencePlan {
    pub(crate) fn clean_install(release: &ReleaseEntry) -> Self {
        Self {
            action: RemoteConvergenceAction::CleanInstall,
            installed_version: None,
            target_version: release.version.clone(),
            update_info: None,
            reason: Some("no existing install was detected".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RemoteStagedPayloadIdentity {
    pub(crate) app_id: String,
    pub(crate) version: String,
    pub(crate) channel: String,
    pub(crate) rid: String,
    pub(crate) full_filename: String,
    pub(crate) full_sha256: String,
    pub(crate) install_directory: String,
    pub(crate) supervisor_id: String,
    pub(crate) storage_provider: String,
    pub(crate) storage_bucket: String,
    pub(crate) storage_region: String,
    pub(crate) storage_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemotePublishedInstallerPlan {
    pub(crate) candidate_keys: Vec<String>,
    pub(crate) blockers: Vec<String>,
    pub(crate) cache: Option<CacheManifestConfig>,
}
