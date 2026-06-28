use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{debug, warn};

use crate::context::Context;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::pack::builder::build_canonical_archive_from_directory;
use crate::platform::fs::write_file_atomic;
use crate::releases::artifact_cache::cache_path_for_key;
use crate::releases::manifest::ReleaseEntry;
use crate::supervisor::stub::find_latest_app_dir;

pub(in crate::update::manager) fn synthesize_current_full_archive_from_installed_app(
    install_dir: &Path,
    current_version: &str,
    current_release: &ReleaseEntry,
    artifact_cache_dir: &Path,
    ctx: &Arc<Context>,
) -> Result<Vec<u8>> {
    let app_dir = find_previous_app_dir(install_dir, current_version).ok_or_else(|| {
        SurgeError::NotFound(format!(
            "No active installed app directory was found for current version {current_version}"
        ))
    })?;

    let mut excluded_relative_paths = BTreeSet::new();
    excluded_relative_paths.insert(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH.to_string());
    excluded_relative_paths.insert(crate::install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH.to_string());
    if runtime_state_dir_contains_only_manifests(&app_dir)? {
        excluded_relative_paths.insert(".surge".to_string());
    }

    let budget = ctx.resource_budget();
    let archive = build_canonical_archive_from_directory(
        &app_dir,
        budget.zstd_compression_level,
        budget.effective_zstd_workers(),
        &excluded_relative_paths,
    )?;

    let mut cache_path = None;
    if !current_release.full_sha256.trim().is_empty() {
        let actual_sha256 = sha256_hex(&archive);
        if actual_sha256 == current_release.full_sha256 {
            cache_path = Some(cache_path_for_key(artifact_cache_dir, &current_release.full_filename)?);
        } else {
            warn!(
                version = %current_release.version,
                expected_sha256 = %current_release.full_sha256,
                actual_sha256 = %actual_sha256,
                "Installed app content reproduced the current package payload but not the original compressed full archive bytes; using synthesized archive for in-flight delta application without caching it"
            );
        }
    }

    if let Some(cache_path) = cache_path {
        write_file_atomic(&cache_path, &archive)?;
        debug!(
            version = %current_release.version,
            app_dir = %app_dir.display(),
            cache_path = %cache_path.display(),
            "Rebuilt current full archive from installed app content"
        );
    }
    Ok(archive)
}

pub(in crate::update::manager) fn find_previous_app_dir(install_dir: &Path, current_version: &str) -> Option<PathBuf> {
    let active = install_dir.join("app");
    if active.is_dir() {
        return Some(active);
    }

    let explicit = install_dir.join(format!("app-{current_version}"));
    if explicit.is_dir() {
        return Some(explicit);
    }

    find_latest_app_dir(install_dir).ok()
}

fn runtime_state_dir_contains_only_manifests(app_dir: &Path) -> Result<bool> {
    let surge_dir = app_dir.join(".surge");
    if !surge_dir.exists() {
        return Ok(false);
    }
    if !surge_dir.is_dir() {
        return Ok(false);
    }

    let allowed = BTreeSet::from([
        crate::install::RUNTIME_MANIFEST_RELATIVE_PATH.to_string(),
        crate::install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH.to_string(),
    ]);
    let mut stack = vec![surge_dir];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)?.collect::<std::result::Result<Vec<_>, std::io::Error>>()?;
        for entry in entries {
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                stack.push(path);
                continue;
            }

            let relative = path
                .strip_prefix(app_dir)
                .map_err(|e| SurgeError::Update(format!("Failed to relativize installed app path: {e}")))?;
            let relative = relative.to_string_lossy().replace('\\', "/");
            if !allowed.contains(&relative) {
                return Ok(false);
            }
        }
    }

    Ok(true)
}
