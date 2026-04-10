#![allow(clippy::cast_precision_loss, clippy::too_many_lines)]

mod activation;
mod execution;
mod published_installer;
mod staging;
mod state;
mod types;

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

pub(crate) use self::execution::{
    resolve_tailscale_targets, run_tailscale_streaming, stream_file_to_tailscale_node_with_command,
};
pub(crate) use self::published_installer::{
    build_installer_for_tailscale, missing_remote_installer_error, plan_remote_published_installer,
    plan_remote_published_installer_without_manifest, try_prepare_published_installer_for_tailscale,
};
pub(crate) use self::staging::{
    deploy_remote_app_copy_for_tailscale, run_remote_staged_installer_setup, warn_if_remote_stage_cleanup_fails,
};
pub(crate) use self::state::{
    check_remote_install_state, detect_remote_launch_environment, remote_install_matches,
    remote_staged_installer_matches_release, remote_staged_payload_matches_release, select_remote_installer_mode,
    select_remote_tailscale_transfer_strategy, should_skip_remote_install, verify_remote_stage_readiness,
};
pub(crate) use self::types::{
    RemoteHostInstallerAvailability, RemoteInstallerMode, RemoteTailscaleCachedState, RemoteTailscaleOperation,
    RemoteTailscaleTransferInputs, RemoteTailscaleTransferStrategy, ensure_supported_tailscale_rid,
};

#[cfg(test)]
#[cfg(test)]
pub(crate) use self::activation::build_remote_app_copy_activation_script;
#[cfg(test)]
pub(crate) use self::published_installer::{build_remote_installer_manifest, published_installer_public_url};
#[cfg(test)]
pub(crate) use self::staging::{
    build_remote_paths_exist_probe, build_remote_stage_cleanup_command, build_remote_staged_installer_setup_command,
    build_remote_stop_supervisor_command, select_latest_remote_legacy_app_dir,
};
#[cfg(test)]
pub(crate) use self::state::{
    parse_remote_install_state, parse_remote_launch_environment, parse_remote_staged_payload_identity,
    remote_launch_environment_probe, remote_staged_payload_identity,
};
#[cfg(test)]
pub(crate) use self::types::{RemoteInstallState, RemoteLaunchEnvironment, RemotePublishedInstallerPlan};
