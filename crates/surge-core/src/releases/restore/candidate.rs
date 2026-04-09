use std::collections::BTreeMap;
use std::path::Path;

use crate::error::{Result, SurgeError};
use crate::releases::artifact_cache::{cache_path_for_key, cached_artifact_matches};
use crate::releases::delta::is_supported_delta;
use crate::releases::manifest::{DeltaArtifact, ReleaseEntry};
use crate::storage::StorageBackend;

use super::RestoreArtifactSpec;

pub(super) struct RestoreCandidate<'a> {
    pub(super) base_release: &'a ReleaseEntry,
    pub(super) chain_releases: Vec<&'a ReleaseEntry>,
    pub(super) chain_deltas: Vec<DeltaArtifact>,
    pub(super) total_items: i64,
    pub(super) total_bytes: i64,
}

pub(super) async fn select_restore_candidate<'a>(
    storage: &dyn StorageBackend,
    releases: &[&'a ReleaseEntry],
    target_idx: usize,
    cache_dir: Option<&Path>,
) -> Result<RestoreCandidate<'a>> {
    let mut best: Option<(RestoreCandidate<'a>, i64, i64)> = None;
    let mut cache_state = BTreeMap::new();
    let mut storage_state = BTreeMap::new();

    for base_idx in (0..=target_idx).rev() {
        let Some(candidate) = build_restore_candidate(releases, target_idx, base_idx) else {
            continue;
        };
        let Some((missing_items, missing_bytes)) =
            assess_restore_candidate(storage, &candidate, cache_dir, &mut cache_state, &mut storage_state).await?
        else {
            continue;
        };
        if cache_dir.is_none() {
            return Ok(candidate);
        }
        if best.as_ref().is_none_or(|(current, current_items, current_bytes)| {
            restore_candidate_is_better(
                &candidate,
                missing_items,
                missing_bytes,
                current,
                *current_items,
                *current_bytes,
            )
        }) {
            best = Some((candidate, missing_items, missing_bytes));
        }
    }

    best.map(|(candidate, _, _)| candidate).ok_or_else(|| {
        let version = releases
            .get(target_idx)
            .map_or_else(|| "<unknown>".to_string(), |release| release.version.clone());
        let rid = releases
            .get(target_idx)
            .map_or_else(|| "<unknown>".to_string(), |release| release.rid.clone());
        SurgeError::NotFound(format!("No reconstructable full archive found for {version} ({rid})"))
    })
}

fn build_restore_candidate<'a>(
    releases: &[&'a ReleaseEntry],
    target_idx: usize,
    base_idx: usize,
) -> Option<RestoreCandidate<'a>> {
    let base_release = *releases.get(base_idx)?;
    if base_release.full_filename.trim().is_empty() {
        return None;
    }

    let chain_releases: Vec<&ReleaseEntry> = releases
        .iter()
        .take(target_idx + 1)
        .skip(base_idx + 1)
        .copied()
        .collect();
    let mut chain_deltas = Vec::with_capacity(chain_releases.len());
    let mut total_bytes = base_release.full_size.max(0);
    for release in &chain_releases {
        let delta = release.selected_delta()?;
        if !is_supported_delta(&delta) {
            return None;
        }
        total_bytes = total_bytes.saturating_add(delta.size.max(0));
        chain_deltas.push(delta);
    }

    let total_items = i64::try_from(chain_deltas.len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .unwrap_or(i64::MAX);

    Some(RestoreCandidate {
        base_release,
        chain_releases,
        chain_deltas,
        total_items,
        total_bytes,
    })
}

async fn assess_restore_candidate(
    storage: &dyn StorageBackend,
    candidate: &RestoreCandidate<'_>,
    cache_dir: Option<&Path>,
    cache_state: &mut BTreeMap<String, bool>,
    storage_state: &mut BTreeMap<String, bool>,
) -> Result<Option<(i64, i64)>> {
    let mut missing_items = 0i64;
    let mut missing_bytes = 0i64;

    let mut specs = Vec::with_capacity(candidate.chain_deltas.len().saturating_add(1));
    specs.push(RestoreArtifactSpec {
        key: candidate.base_release.full_filename.clone(),
        sha256: candidate.base_release.full_sha256.clone(),
        size: candidate.base_release.full_size,
    });
    for delta in &candidate.chain_deltas {
        specs.push(RestoreArtifactSpec {
            key: delta.filename.clone(),
            sha256: delta.sha256.clone(),
            size: delta.size,
        });
    }

    for spec in specs {
        if let Some(cache_root) = cache_dir
            && cached_artifact_available(cache_root, &spec, cache_state)?
        {
            continue;
        }
        if !storage_artifact_available(storage, &spec.key, storage_state).await? {
            return Ok(None);
        }
        missing_items = missing_items.saturating_add(1);
        missing_bytes = missing_bytes.saturating_add(spec.size.max(0));
    }

    Ok(Some((missing_items, missing_bytes)))
}

fn cached_artifact_available(
    cache_root: &Path,
    spec: &RestoreArtifactSpec,
    cache_state: &mut BTreeMap<String, bool>,
) -> Result<bool> {
    if let Some(cached) = cache_state.get(&spec.key) {
        return Ok(*cached);
    }

    let cache_path = cache_path_for_key(cache_root, &spec.key)?;
    let cached = cached_artifact_matches(&cache_path, &spec.sha256)?;
    cache_state.insert(spec.key.clone(), cached);
    Ok(cached)
}

async fn storage_artifact_available(
    storage: &dyn StorageBackend,
    key: &str,
    storage_state: &mut BTreeMap<String, bool>,
) -> Result<bool> {
    if let Some(cached) = storage_state.get(key) {
        return Ok(*cached);
    }

    let available = match storage.head_object(key).await {
        Ok(_) => true,
        Err(SurgeError::NotFound(_)) => false,
        Err(e) => return Err(e),
    };
    storage_state.insert(key.to_string(), available);
    Ok(available)
}

fn restore_candidate_is_better(
    candidate: &RestoreCandidate<'_>,
    missing_items: i64,
    missing_bytes: i64,
    current: &RestoreCandidate<'_>,
    current_items: i64,
    current_bytes: i64,
) -> bool {
    (missing_bytes, missing_items, candidate.chain_deltas.len())
        < (current_bytes, current_items, current.chain_deltas.len())
}
