use std::path::Path;

use super::{ReleaseEntry, Result, core_install};
use surge_core::install::InstallProfile;

pub(crate) fn release_install_profile<'a>(app_id: &'a str, release: &'a ReleaseEntry) -> InstallProfile<'a> {
    InstallProfile::new(
        app_id,
        release.display_name(app_id),
        &release.main_exe,
        &release.install_directory,
        &release.supervisor_id,
        &release.icon,
        &release.shortcuts,
        &release.persistent_assets,
        &release.environment,
    )
}

pub(crate) async fn stop_running_supervisor(app_id: &str, release: &ReleaseEntry) -> Result<()> {
    let supervisor_id = release.supervisor_id.trim();
    if supervisor_id.is_empty() {
        return Ok(());
    }

    let install_root = surge_core::platform::paths::default_install_root(app_id, &release.install_directory)?;
    super::super::stop_supervisor(&install_root, supervisor_id).await
}

pub(crate) fn install_package_locally(
    app_id: &str,
    release: &ReleaseEntry,
    package_path: &Path,
) -> Result<std::path::PathBuf> {
    let profile = release_install_profile(app_id, release);
    core_install::install_package_locally(&profile, package_path)
}

pub(crate) fn auto_start_after_install(
    release: &ReleaseEntry,
    app_id: &str,
    install_root: &Path,
    active_app_dir: &Path,
) -> Result<u32> {
    let profile = release_install_profile(app_id, release);
    core_install::auto_start_after_install_sequence(&profile, install_root, active_app_dir, &release.version)
}

pub(crate) fn release_runtime_manifest_metadata<'a>(
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

pub(crate) fn host_can_build_installer_locally(rid: &str) -> bool {
    super::super::pack::ensure_host_compatible_rid(rid).is_ok()
}
