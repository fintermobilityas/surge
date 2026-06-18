use std::path::{Path, PathBuf};

use tracing::warn;

use crate::archive::extractor::extract_file_to_with_progress;
use crate::error::{Result, SurgeError};
use crate::platform::fs::list_directories;
use crate::platform::paths::default_install_root;
use crate::platform::shortcuts::install_shortcuts;
use crate::releases::version::compare_versions;
use crate::supervisor::stub::find_latest_app_dir;

use super::persistent_assets::copy_persistent_assets;
use super::runtime_manifest::RuntimeManifestSnapshot;
use super::{InstallProfile, InstallProgress, InstallProgressCallback, InstallProgressStage, emit_install_progress};

/// Resolve the install root and install the package there.
pub fn install_package_locally(profile: &InstallProfile<'_>, package_path: &Path) -> Result<PathBuf> {
    let install_root = default_install_root(profile.app_id, profile.install_directory)?;
    install_package_locally_at_root(profile, package_path, &install_root)?;
    Ok(install_root)
}

/// Extract a package into `install_root/app` with atomic swap, then create shortcuts.
pub fn install_package_locally_at_root(
    profile: &InstallProfile<'_>,
    package_path: &Path,
    install_root: &Path,
) -> Result<()> {
    install_package_locally_at_root_with_progress(profile, package_path, install_root, None)
}

/// Extract a package into `install_root/app` with atomic swap, then create shortcuts.
pub fn install_package_locally_at_root_with_progress(
    profile: &InstallProfile<'_>,
    package_path: &Path,
    install_root: &Path,
    progress: Option<&InstallProgressCallback<'_>>,
) -> Result<()> {
    std::fs::create_dir_all(install_root)?;

    let active_app_dir = install_root.join("app");
    let next_app_dir = install_root.join(".surge-app-next");
    let previous_app_dir = install_root.join(".surge-app-prev");
    let fallback_previous_app_dir = if active_app_dir.is_dir() {
        None
    } else {
        find_latest_app_dir(install_root).ok()
    };

    if next_app_dir.is_dir() {
        std::fs::remove_dir_all(&next_app_dir)?;
    }
    if previous_app_dir.is_dir() {
        std::fs::remove_dir_all(&previous_app_dir)?;
    }

    emit_install_progress(
        progress,
        InstallProgress {
            stage: InstallProgressStage::Extract,
            phase_percent: 0,
            bytes_done: 0,
            bytes_total: 0,
            items_done: 0,
            items_total: 0,
        },
    );
    let extract_progress = |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
        let phase_percent = if bytes_total > 0 {
            bytes_done
                .saturating_mul(100)
                .checked_div(bytes_total)
                .map_or(0, |percent| percent.clamp(0, 100) as i32)
        } else if items_total > 0 {
            items_done
                .saturating_mul(100)
                .checked_div(items_total)
                .map_or(0, |percent| percent.clamp(0, 100) as i32)
        } else {
            0
        };
        emit_install_progress(
            progress,
            InstallProgress {
                stage: InstallProgressStage::Extract,
                phase_percent,
                bytes_done: i64::try_from(bytes_done).unwrap_or(i64::MAX),
                bytes_total: i64::try_from(bytes_total).unwrap_or(i64::MAX),
                items_done: i64::try_from(items_done).unwrap_or(i64::MAX),
                items_total: i64::try_from(items_total).unwrap_or(i64::MAX),
            },
        );
    };
    extract_file_to_with_progress(package_path, &next_app_dir, Some(&extract_progress))?;
    emit_install_progress(
        progress,
        InstallProgress {
            stage: InstallProgressStage::Extract,
            phase_percent: 100,
            bytes_done: 0,
            bytes_total: 0,
            items_done: 0,
            items_total: 0,
        },
    );

    emit_install_progress(
        progress,
        InstallProgress {
            stage: InstallProgressStage::Activate,
            phase_percent: 0,
            bytes_done: 0,
            bytes_total: 0,
            items_done: 0,
            items_total: 0,
        },
    );
    if active_app_dir.is_dir() {
        std::fs::rename(&active_app_dir, &previous_app_dir)?;
    }

    if let Err(rename_err) = std::fs::rename(&next_app_dir, &active_app_dir) {
        if previous_app_dir.is_dir() && !active_app_dir.exists() {
            let _ = std::fs::rename(&previous_app_dir, &active_app_dir);
        }
        return Err(SurgeError::Io(rename_err));
    }
    emit_install_progress(
        progress,
        InstallProgress {
            stage: InstallProgressStage::Activate,
            phase_percent: 100,
            bytes_done: 0,
            bytes_total: 0,
            items_done: 0,
            items_total: 0,
        },
    );

    let runtime_manifest_snapshot = RuntimeManifestSnapshot::capture(&active_app_dir)?;
    let install_result = (|| -> Result<()> {
        let previous_app_dir_for_assets = if previous_app_dir.is_dir() {
            Some(previous_app_dir.as_path())
        } else {
            fallback_previous_app_dir.as_deref()
        };
        if !profile.persistent_assets.is_empty()
            && let Some(previous) = previous_app_dir_for_assets
        {
            copy_persistent_assets(previous, &active_app_dir, profile.persistent_assets)?;
        }

        ensure_main_executable_mode(profile, &active_app_dir)?;

        if !profile.shortcuts.is_empty() {
            emit_install_progress(
                progress,
                InstallProgress {
                    stage: InstallProgressStage::Shortcuts,
                    phase_percent: 0,
                    bytes_done: 0,
                    bytes_total: 0,
                    items_done: 0,
                    items_total: 0,
                },
            );
            let main_exe = profile.main_exe.trim();
            if main_exe.is_empty() {
                return Err(SurgeError::Config(format!(
                    "App '{}' has shortcuts configured but no main executable metadata",
                    profile.app_id
                )));
            }
            install_shortcuts(
                profile.app_id,
                profile.display_name,
                &active_app_dir,
                main_exe,
                profile.supervisor_id,
                profile.icon,
                profile.shortcuts,
                profile.environment,
            )?;
            emit_install_progress(
                progress,
                InstallProgress {
                    stage: InstallProgressStage::Shortcuts,
                    phase_percent: 100,
                    bytes_done: 0,
                    bytes_total: 0,
                    items_done: i64::try_from(profile.shortcuts.len()).unwrap_or(i64::MAX),
                    items_total: i64::try_from(profile.shortcuts.len()).unwrap_or(i64::MAX),
                },
            );
        }

        if previous_app_dir.is_dir() {
            std::fs::remove_dir_all(&previous_app_dir)?;
        }

        Ok(())
    })();
    if let Err(err) = install_result {
        if let Err(restore_err) = runtime_manifest_snapshot.restore(&active_app_dir) {
            return Err(SurgeError::Platform(format!(
                "Failed to restore runtime manifests after install error '{err}': {restore_err}"
            )));
        }
        return Err(err);
    }

    if let Err(err) = prune_version_snapshots(install_root, 0) {
        warn!(error = %err, "Failed to prune installed app version snapshots after install");
    }

    Ok(())
}

#[cfg(unix)]
fn ensure_main_executable_mode(profile: &InstallProfile<'_>, active_app_dir: &Path) -> Result<()> {
    let main_exe = profile.main_exe.trim();
    if main_exe.is_empty() {
        return Ok(());
    }

    let exe_path = active_app_dir.join(main_exe);
    if exe_path.is_file() {
        crate::platform::fs::make_executable(&exe_path)?;
    }

    Ok(())
}

#[cfg(not(unix))]
fn ensure_main_executable_mode(_profile: &InstallProfile<'_>, _active_app_dir: &Path) -> Result<()> {
    Ok(())
}

fn app_snapshot_version(dir_name: &str) -> Option<&str> {
    let version = dir_name.strip_prefix("app-")?;
    if version.is_empty() || !version.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(version)
}

pub fn prune_version_snapshots(install_dir: &Path, keep_latest: usize) -> Result<usize> {
    let mut snapshots: Vec<(String, PathBuf)> = list_directories(install_dir)?
        .into_iter()
        .filter_map(|dir_name| {
            let version = app_snapshot_version(&dir_name)?.to_string();
            let path = install_dir.join(&dir_name);
            Some((version, path))
        })
        .collect();

    snapshots.sort_by(|(left, _), (right, _)| compare_versions(right, left));

    let mut pruned = 0usize;
    for (_, path) in snapshots.into_iter().skip(keep_latest) {
        std::fs::remove_dir_all(path)?;
        pruned = pruned.saturating_add(1);
    }

    Ok(pruned)
}
