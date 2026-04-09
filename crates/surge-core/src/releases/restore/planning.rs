use crate::error::{Result, SurgeError};
use crate::releases::delta::is_supported_delta;
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex};
use crate::releases::version::compare_versions;
use crate::storage::StorageBackend;

use super::RestoreArtifactSpec;

#[must_use]
pub fn sorted_releases_for_rid<'a>(index: &'a ReleaseIndex, rid: &str) -> Vec<&'a ReleaseEntry> {
    let mut releases: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|release| release.rid == rid || release.rid.is_empty())
        .collect();
    releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
    releases
}

#[must_use]
pub fn find_release_for_version_rid<'a>(index: &'a ReleaseIndex, rid: &str, version: &str) -> Option<&'a ReleaseEntry> {
    sorted_releases_for_rid(index, rid)
        .into_iter()
        .find(|release| release.version == version)
}

#[must_use]
pub fn find_previous_release_for_rid<'a>(
    index: &'a ReleaseIndex,
    rid: &str,
    version: &str,
) -> Option<&'a ReleaseEntry> {
    let mut previous: Option<&ReleaseEntry> = None;
    for release in sorted_releases_for_rid(index, rid) {
        if compare_versions(&release.version, version) != std::cmp::Ordering::Less {
            continue;
        }
        previous = Some(release);
    }
    previous
}

pub async fn plan_full_archive_restore(
    storage: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
    version: &str,
) -> Result<Vec<RestoreArtifactSpec>> {
    let releases = sorted_releases_for_rid(index, rid);
    let target_idx = releases
        .iter()
        .position(|release| release.version == version)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} ({rid}) not found in index")))?;

    for base_idx in (0..=target_idx).rev() {
        let Some(specs) = build_restore_plan_specs(&releases, target_idx, base_idx) else {
            continue;
        };
        if restore_artifacts_exist(storage, &specs).await? {
            return Ok(specs);
        }
    }

    Err(SurgeError::NotFound(format!(
        "No reconstructable full archive found for {version} ({rid})"
    )))
}

fn build_restore_plan_specs(
    releases: &[&ReleaseEntry],
    target_idx: usize,
    base_idx: usize,
) -> Option<Vec<RestoreArtifactSpec>> {
    let base_release = releases.get(base_idx)?;
    let full_key = base_release.full_filename.trim();
    if full_key.is_empty() {
        return None;
    }

    let mut specs = vec![RestoreArtifactSpec {
        key: full_key.to_string(),
        sha256: base_release.full_sha256.clone(),
        size: base_release.full_size,
    }];

    for release in releases.iter().take(target_idx + 1).skip(base_idx + 1) {
        let delta = release.selected_delta()?;
        if !is_supported_delta(&delta) {
            return None;
        }
        specs.push(RestoreArtifactSpec {
            key: delta.filename.clone(),
            sha256: delta.sha256.clone(),
            size: delta.size,
        });
    }

    Some(specs)
}

async fn restore_artifacts_exist(storage: &dyn StorageBackend, specs: &[RestoreArtifactSpec]) -> Result<bool> {
    for spec in specs {
        match storage.head_object(&spec.key).await {
            Ok(_) => {}
            Err(SurgeError::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        }
    }

    Ok(true)
}
