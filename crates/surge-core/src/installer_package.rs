use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::config::installer::InstallerManifest;
use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{cache_path_for_key, cached_artifact_matches, prune_cached_artifacts};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use crate::releases::restore::{
    RestoreOptions, RestoreProgressCallback, restore_full_archive_for_version_with_options,
    retained_artifacts_for_cache_policy, retained_artifacts_for_cache_policy_without_index,
};
use crate::storage::{self, StorageBackend, TransferProgress};
use crate::storage_config::build_storage_config_from_installer_manifest;

pub type InstallerPackageStageCallback<'a> = dyn Fn(InstallerPackageStage) + Send + Sync + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallerPackageStage {
    UsingBundledPayload,
    UsingCachedPackage,
    PreparingPackage,
    DownloadingPackage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallerPackageAcquisition {
    BundledPayload,
    ArtifactCache,
    PreparedArtifactCache,
    ArtifactCacheFallback,
    Downloaded,
}

#[derive(Debug)]
pub struct ResolvedInstallerPackage {
    path: PathBuf,
    pub retained_artifacts: Option<BTreeSet<String>>,
    pub acquisition: InstallerPackageAcquisition,
}

impl ResolvedInstallerPackage {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Default)]
pub struct ResolveInstallerPackageOptions<'a> {
    pub download_progress: Option<&'a TransferProgress<'a>>,
    pub restore_progress: Option<&'a RestoreProgressCallback<'a>>,
    pub stage: Option<&'a InstallerPackageStageCallback<'a>>,
}

pub async fn resolve_installer_package(
    staging_dir: &Path,
    manifest: &InstallerManifest,
    install_root: &Path,
    options: ResolveInstallerPackageOptions<'_>,
) -> Result<ResolvedInstallerPackage> {
    let full_filename = manifest.release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::Config(
            "Installer manifest has no full_filename in release section".to_string(),
        ));
    }

    let payload_path = staging_dir.join("payload").join(full_filename);
    if payload_path.is_file() {
        notify_stage(options.stage, InstallerPackageStage::UsingBundledPayload);
        return Ok(ResolvedInstallerPackage {
            path: payload_path,
            retained_artifacts: None,
            acquisition: InstallerPackageAcquisition::BundledPayload,
        });
    }

    let artifact_cache_dir = install_artifact_cache_dir(install_root);
    std::fs::create_dir_all(&artifact_cache_dir)?;
    let cached_package_path = cache_path_for_key(&artifact_cache_dir, full_filename)?;
    let storage_config = build_storage_config_from_installer_manifest(manifest)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    let index = match fetch_release_index(&*backend, manifest).await {
        Ok(index) => index,
        Err(error) if cached_package_path.is_file() => {
            notify_stage(options.stage, InstallerPackageStage::UsingCachedPackage);
            tracing::warn!(
                "Could not fetch release index; using cached package '{}' without verification: {}",
                cached_package_path.display(),
                error
            );
            return Ok(ResolvedInstallerPackage {
                path: cached_package_path,
                retained_artifacts: None,
                acquisition: InstallerPackageAcquisition::ArtifactCacheFallback,
            });
        }
        Err(error) => return Err(error),
    };
    let retained_artifacts = index
        .as_ref()
        .map(|index| retained_artifacts_for_install_cache(index, manifest));

    if let Some(index) = index.as_ref()
        && let Some(release) = find_release_for_installer(index, manifest)
    {
        if cached_artifact_matches(&cached_package_path, &release.full_sha256)? {
            notify_stage(options.stage, InstallerPackageStage::UsingCachedPackage);
            return Ok(ResolvedInstallerPackage {
                path: cached_package_path,
                retained_artifacts,
                acquisition: InstallerPackageAcquisition::ArtifactCache,
            });
        }

        notify_stage(options.stage, InstallerPackageStage::PreparingPackage);
        let restored = restore_full_archive_for_version_with_options(
            &*backend,
            index,
            &manifest.rid,
            &manifest.version,
            RestoreOptions {
                cache_dir: Some(&artifact_cache_dir),
                progress: options.restore_progress,
            },
        )
        .await?;
        std::fs::write(&cached_package_path, restored)?;
        return Ok(ResolvedInstallerPackage {
            path: cached_package_path,
            retained_artifacts,
            acquisition: InstallerPackageAcquisition::PreparedArtifactCache,
        });
    }

    if cached_package_path.is_file() {
        notify_stage(options.stage, InstallerPackageStage::UsingCachedPackage);
        tracing::warn!(
            "Release metadata for '{}' was not found; using cached package '{}'.",
            full_filename,
            cached_package_path.display()
        );
        return Ok(ResolvedInstallerPackage {
            path: cached_package_path,
            retained_artifacts,
            acquisition: InstallerPackageAcquisition::ArtifactCacheFallback,
        });
    }

    notify_stage(options.stage, InstallerPackageStage::DownloadingPackage);
    backend
        .download_to_file(full_filename, &cached_package_path, options.download_progress)
        .await?;

    Ok(ResolvedInstallerPackage {
        path: cached_package_path,
        retained_artifacts,
        acquisition: InstallerPackageAcquisition::Downloaded,
    })
}

pub fn install_artifact_cache_dir(install_root: &Path) -> PathBuf {
    install_root.join(".surge-cache").join("artifacts")
}

pub fn prune_install_artifact_cache(install_root: &Path, retained_artifacts: &BTreeSet<String>) -> Result<usize> {
    prune_cached_artifacts(&install_artifact_cache_dir(install_root), retained_artifacts)
}

#[must_use]
pub fn retained_artifacts_for_install_cache_without_index(manifest: &InstallerManifest) -> Option<BTreeSet<String>> {
    retained_artifacts_for_cache_policy_without_index(
        manifest.effective_install_artifact_cache_policy(),
        &manifest.release.full_filename,
    )
}

fn retained_artifacts_for_install_cache(index: &ReleaseIndex, manifest: &InstallerManifest) -> BTreeSet<String> {
    let policy = manifest.effective_install_artifact_cache_policy();
    retained_artifacts_for_cache_policy(index, policy, &manifest.release.full_filename, 0)
}

fn notify_stage(stage: Option<&InstallerPackageStageCallback<'_>>, value: InstallerPackageStage) {
    if let Some(stage) = stage {
        stage(value);
    }
}

async fn fetch_release_index(
    backend: &dyn StorageBackend,
    manifest: &InstallerManifest,
) -> Result<Option<ReleaseIndex>> {
    let key = manifest.release_index_key.trim();
    let key = if key.is_empty() { RELEASES_FILE_COMPRESSED } else { key };
    match backend.get_object(key).await {
        Ok(bytes) => {
            let index = decompress_release_index(&bytes)?;
            if !index.app_id.is_empty() && index.app_id != manifest.app_id {
                return Err(SurgeError::NotFound(format!(
                    "Release index belongs to app '{}' not '{}'",
                    index.app_id, manifest.app_id
                )));
            }
            Ok(Some(index))
        }
        Err(SurgeError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn find_release_for_installer<'a>(index: &'a ReleaseIndex, manifest: &InstallerManifest) -> Option<&'a ReleaseEntry> {
    index.releases.iter().find(|release| {
        release.version == manifest.version
            && release.full_filename.trim() == manifest.release.full_filename.trim()
            && (release.rid.is_empty() || manifest.rid.is_empty() || release.rid == manifest.rid)
    })
}
