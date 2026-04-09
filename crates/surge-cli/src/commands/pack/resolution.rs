use std::path::{Path, PathBuf};

use surge_core::config::constants::PACK_DEFAULT_MAX_MEMORY_BYTES;
use surge_core::config::manifest::{AppConfig, SurgeManifest};
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

#[derive(Debug, Clone)]
pub(super) struct ResolvedInstallerPackage {
    pub(super) app_id: String,
    pub(super) rid: String,
    pub(super) default_channel: String,
    pub(super) selected_version: String,
    pub(super) full_key: String,
    pub(super) full_sha256: String,
    pub(super) local_full_name: String,
    pub(super) artifacts_dir: PathBuf,
}

pub(super) async fn resolve_installer_package(
    manifest: &SurgeManifest,
    manifest_path: &Path,
    app_id: Option<&str>,
    version: Option<&str>,
    rid: Option<&str>,
    artifacts_dir: Option<&Path>,
) -> Result<(Box<dyn StorageBackend>, ReleaseIndex, ResolvedInstallerPackage)> {
    let app_id = super::super::resolve_app_id_with_rid_hint(manifest, app_id, rid)?;
    let rid = super::super::resolve_rid(manifest, &app_id, rid)?;
    let (app, _) = manifest
        .find_app_with_target(&app_id, &rid)
        .ok_or_else(|| SurgeError::Config(format!("No target {rid} found for app {app_id}")))?;
    let default_channel = default_channel_for_app(manifest, app);
    let storage_config = super::super::build_app_scoped_storage_config(manifest, manifest_path, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    let index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    let selected_release =
        select_release_for_installers(&index.releases, &default_channel, version, &rid).ok_or_else(|| {
            SurgeError::NotFound(format!(
                "No release found for app '{}' rid '{}' on channel '{}'{}",
                app_id,
                rid,
                default_channel,
                version.map_or_else(String::new, |v| format!(" and version '{v}'"))
            ))
        })?;
    let full_key = selected_release.full_filename.trim();
    if full_key.is_empty() {
        return Err(SurgeError::Pack(format!(
            "Selected release {} for {}/{} does not define a full package filename",
            selected_release.version, app_id, rid
        )));
    }
    let local_full_name = Path::new(full_key)
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or_else(|| SurgeError::Pack(format!("Invalid full package key: {full_key}")))?
        .to_string();
    let artifacts_dir = artifacts_dir.map_or_else(
        || default_artifacts_dir(manifest_path, &app_id, &rid, &selected_release.version),
        PathBuf::from,
    );

    Ok((
        backend,
        index,
        ResolvedInstallerPackage {
            app_id,
            rid,
            default_channel,
            selected_version: selected_release.version.clone(),
            full_key: full_key.to_string(),
            full_sha256: selected_release.full_sha256.clone(),
            local_full_name,
            artifacts_dir,
        },
    ))
}

pub(super) fn write_package_manifest(
    path: &Path,
    specs: &[surge_core::releases::restore::RestoreArtifactSpec],
) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut manifest = String::new();
    for spec in specs {
        manifest.push_str(spec.sha256.trim());
        manifest.push(' ');
        manifest.push_str(spec.key.trim());
        manifest.push('\n');
    }
    std::fs::write(path, manifest)?;
    Ok(())
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<ReleaseIndex> {
    match backend
        .get_object(surge_core::config::constants::RELEASES_FILE_COMPRESSED)
        .await
    {
        Ok(data) => decompress_release_index(&data),
        Err(SurgeError::NotFound(_)) => Ok(ReleaseIndex::default()),
        Err(e) => Err(e),
    }
}

fn select_release_for_installers(
    releases: &[ReleaseEntry],
    channel: &str,
    version: Option<&str>,
    rid: &str,
) -> Option<ReleaseEntry> {
    let mut eligible: Vec<&ReleaseEntry> = releases
        .iter()
        .filter(|release| release.channels.iter().any(|c| c == channel))
        .collect();

    if let Some(requested) = version.map(str::trim).filter(|value| !value.is_empty()) {
        eligible.retain(|release| release.version == requested);
    }

    if eligible.is_empty() {
        return None;
    }

    let mut by_rid: Vec<&ReleaseEntry> = eligible.iter().copied().filter(|release| release.rid == rid).collect();
    by_rid.sort_by(|a, b| compare_versions(&b.version, &a.version));
    if let Some(release) = by_rid.first() {
        return Some((*release).clone());
    }

    let mut generic: Vec<&ReleaseEntry> = eligible
        .iter()
        .copied()
        .filter(|release| release.rid.trim().is_empty())
        .collect();
    generic.sort_by(|a, b| compare_versions(&b.version, &a.version));
    generic.first().map(|release| (*release).clone())
}

pub(super) fn default_channel_for_app(manifest: &SurgeManifest, app: &AppConfig) -> String {
    app.channels
        .first()
        .cloned()
        .or_else(|| manifest.channels.first().map(|channel| channel.name.clone()))
        .unwrap_or_else(|| "stable".to_string())
}

pub(super) fn installer_storage_prefix(manifest: &SurgeManifest, app_id: &str) -> String {
    if manifest.apps.len() > 1 {
        super::super::append_prefix(&manifest.storage.prefix, app_id)
    } else {
        manifest.storage.prefix.clone()
    }
}

pub(crate) fn default_artifacts_dir(manifest_path: &Path, app_id: &str, rid: &str, version: &str) -> PathBuf {
    manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("artifacts")
        .join(app_id)
        .join(rid)
        .join(version)
}

pub(crate) fn configure_context(manifest_path: &Path, manifest: &SurgeManifest, app_id: &str) -> Result<Context> {
    let ctx = super::super::build_app_scoped_storage_context(manifest, manifest_path, app_id)?;
    let pack_policy = manifest.effective_pack_policy();
    let mut budget = ctx.resource_budget();
    let available_threads = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);

    budget.max_threads = i32::try_from(available_threads).unwrap_or(i32::MAX);
    budget.max_memory_bytes = PACK_DEFAULT_MAX_MEMORY_BYTES;
    ctx.set_resource_budget({
        budget.zstd_compression_level = pack_policy.compression_level;
        budget
    });
    Ok(ctx)
}
