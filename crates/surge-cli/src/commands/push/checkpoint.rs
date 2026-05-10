use std::cmp::Ordering as VersionOrdering;
use std::collections::{BTreeMap, BTreeSet};

use surge_core::config::manifest::PackPolicy;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex};
use surge_core::releases::restore::sorted_releases_for_rid;
use surge_core::releases::version::compare_versions;
use surge_core::storage::StorageBackend;

pub(super) async fn rid_has_uploaded_full_artifact(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
) -> Result<bool> {
    let mut candidates: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|release| (release.rid == rid || release.rid.is_empty()) && !release.full_filename.trim().is_empty())
        .collect();
    candidates.sort_by(|a, b| compare_versions(&a.version, &b.version));

    for release in candidates {
        match backend.head_object(release.full_filename.trim()).await {
            Ok(_) => return Ok(true),
            Err(SurgeError::NotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    Ok(false)
}

pub(super) async fn should_upload_full_artifact(
    backend: &dyn StorageBackend,
    existing_index: Option<&ReleaseIndex>,
    rid: &str,
    version: &str,
    delta_available: bool,
    has_existing_full_for_rid: bool,
    pack_policy: PackPolicy,
) -> Result<bool> {
    if !delta_available || !has_existing_full_for_rid {
        return Ok(true);
    }

    let Some(index) = existing_index else {
        return Ok(true);
    };

    let Some(delta_steps) = delta_steps_since_latest_uploaded_full(backend, index, rid, version).await? else {
        return Ok(true);
    };

    Ok(delta_steps >= pack_policy.max_chain_length || delta_steps.saturating_add(1) >= pack_policy.checkpoint_every)
}

async fn delta_steps_since_latest_uploaded_full(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    rid: &str,
    version: &str,
) -> Result<Option<u32>> {
    let mut delta_steps = 0u32;
    let releases = sorted_releases_for_rid(index, rid);
    for release in releases.iter().rev() {
        if compare_versions(&release.version, version) != VersionOrdering::Less {
            continue;
        }

        let full = release.full_filename.trim();
        if !full.is_empty() {
            match backend.head_object(full).await {
                Ok(_) => return Ok(Some(delta_steps)),
                Err(SurgeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }

        if release.selected_delta().is_some() {
            delta_steps = delta_steps.saturating_add(1);
        } else {
            return Ok(None);
        }
    }

    Ok(None)
}

pub(super) async fn retained_uploaded_full_artifacts_for_index(
    backend: &dyn StorageBackend,
    index: &ReleaseIndex,
    per_rid_limit: u32,
) -> Result<BTreeSet<String>> {
    let limit = usize::try_from(per_rid_limit).unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(BTreeSet::new());
    }

    let mut by_rid: BTreeMap<&str, Vec<&ReleaseEntry>> = BTreeMap::new();
    for release in &index.releases {
        by_rid.entry(release.rid.as_str()).or_default().push(release);
    }

    let mut retained = BTreeSet::new();
    for releases in by_rid.values_mut() {
        releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
        let mut retained_for_rid = 0usize;
        for release in releases.iter().rev() {
            if retained_for_rid >= limit {
                break;
            }

            let full = release.full_filename.trim();
            if full.is_empty() {
                continue;
            }

            match backend.head_object(full).await {
                Ok(_) => {
                    retained.insert(full.to_string());
                    retained_for_rid = retained_for_rid.saturating_add(1);
                }
                Err(SurgeError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }

    Ok(retained)
}
