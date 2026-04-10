use std::path::Path;

use super::{
    ArchiveAcquisition, ReleaseEntry, ReleaseIndex, Result, StorageBackend, auto_start_after_install, core_install,
    download_release_archive, install_package_locally, logline, release_install_profile,
    release_runtime_manifest_metadata, stop_running_supervisor,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn install_selected_release_locally(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    download_dir: &Path,
    app_id: &str,
    release: &ReleaseEntry,
    channel: &str,
    rid_candidates: &[String],
    full_filename: &str,
    storage_config: &surge_core::context::StorageConfig,
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

    stop_running_supervisor(app_id, release).await?;
    let install_root = install_package_locally(app_id, release, &local_package)?;
    let active_app_dir = install_root.join("app");
    let install_profile = release_install_profile(app_id, release);
    let runtime_manifest = release_runtime_manifest_metadata(release, channel, storage_config);
    core_install::write_runtime_manifest(&active_app_dir, &install_profile, &runtime_manifest)?;
    logline::success(&format!(
        "Installed '{}' to '{}' (active app: '{}').",
        app_id,
        install_root.display(),
        active_app_dir.display()
    ));

    if !no_start {
        let display_name = release.display_name(app_id);
        match auto_start_after_install(release, app_id, &install_root, &active_app_dir) {
            Ok(pid) => {
                logline::success(&format!("Started '{display_name}' (pid {pid})."));
            }
            Err(error) => {
                logline::warn(&format!("Auto-start failed: {error}"));
            }
        }
    }

    Ok(())
}
