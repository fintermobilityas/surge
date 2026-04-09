use std::collections::{BTreeMap, BTreeSet};

use crate::releases::manifest::{ReleaseEntry, ReleaseIndex};
use crate::releases::version::compare_versions;

#[must_use]
pub fn required_artifacts_for_index(index: &ReleaseIndex) -> BTreeSet<String> {
    let mut by_rid: BTreeMap<&str, Vec<&ReleaseEntry>> = BTreeMap::new();
    for release in &index.releases {
        by_rid.entry(release.rid.as_str()).or_default().push(release);
    }

    let mut required = BTreeSet::new();
    for releases in by_rid.values_mut() {
        releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
        extend_required_artifacts_for_sorted_releases(releases, &mut required);
    }
    required
}

#[must_use]
pub fn local_checkpoint_artifacts_for_index(index: &ReleaseIndex, per_rid_limit: usize) -> BTreeSet<String> {
    let mut by_rid: BTreeMap<&str, Vec<&ReleaseEntry>> = BTreeMap::new();
    for release in &index.releases {
        by_rid.entry(release.rid.as_str()).or_default().push(release);
    }

    let mut retained = BTreeSet::new();
    for releases in by_rid.values_mut() {
        releases.sort_by(|a, b| compare_versions(&a.version, &b.version));
        for release in releases.iter().rev().take(per_rid_limit) {
            let key = release.full_filename.trim();
            if !key.is_empty() {
                retained.insert(key.to_string());
            }
        }
    }

    retained
}

fn extend_required_artifacts_for_sorted_releases(releases: &[&ReleaseEntry], required: &mut BTreeSet<String>) {
    if releases.is_empty() {
        return;
    }

    let mut required_full_indices = Vec::new();
    if let Some(first_full_idx) = releases
        .iter()
        .position(|release| !release.full_filename.trim().is_empty())
    {
        required_full_indices.push(first_full_idx);
    }

    for (idx, release) in releases.iter().enumerate().skip(1) {
        if release.selected_delta().is_none() && !release.full_filename.trim().is_empty() {
            required_full_indices.push(idx);
        }
    }

    if required_full_indices.is_empty() {
        return;
    }

    required_full_indices.sort_unstable();
    required_full_indices.dedup();

    for idx in &required_full_indices {
        let full = releases[*idx].full_filename.trim();
        if !full.is_empty() {
            required.insert(full.to_string());
        }
    }

    let first_required_full = required_full_indices[0];
    for release in releases.iter().skip(first_required_full + 1) {
        for delta in release.all_deltas() {
            let key = delta.filename.trim();
            if !key.is_empty() {
                required.insert(key.to_string());
            }
        }
    }
}
