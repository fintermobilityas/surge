use std::collections::{BTreeMap, BTreeSet};

use crate::config::manifest::{InstallArtifactCachePolicy, InstallArtifactCacheRetention};
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

#[must_use]
pub fn retained_artifacts_for_cache_policy(
    index: &ReleaseIndex,
    policy: InstallArtifactCachePolicy,
    warm_full_filename: &str,
    release_graph_checkpoint_fulls: usize,
) -> BTreeSet<String> {
    let mut retained = match policy.retention {
        InstallArtifactCacheRetention::ReleaseGraph => {
            let mut retained = required_artifacts_for_index(index);
            retained.extend(local_checkpoint_artifacts_for_index(
                index,
                release_graph_checkpoint_fulls,
            ));
            retained
        }
        InstallArtifactCacheRetention::LatestFull => {
            let keep_full_count = usize::try_from(policy.keep_full_count.max(1)).unwrap_or(usize::MAX);
            local_checkpoint_artifacts_for_index(index, keep_full_count)
        }
        InstallArtifactCacheRetention::JustInstalled | InstallArtifactCacheRetention::None => BTreeSet::new(),
    };

    if !matches!(policy.retention, InstallArtifactCacheRetention::None) {
        insert_warm_full(&mut retained, warm_full_filename);
    }

    retained
}

#[must_use]
pub fn retained_artifacts_for_cache_policy_without_index(
    policy: InstallArtifactCachePolicy,
    warm_full_filename: &str,
) -> Option<BTreeSet<String>> {
    match policy.retention {
        InstallArtifactCacheRetention::ReleaseGraph => None,
        InstallArtifactCacheRetention::LatestFull | InstallArtifactCacheRetention::JustInstalled => {
            let mut retained = BTreeSet::new();
            insert_warm_full(&mut retained, warm_full_filename);
            Some(retained)
        }
        InstallArtifactCacheRetention::None => Some(BTreeSet::new()),
    }
}

fn insert_warm_full(retained: &mut BTreeSet<String>, warm_full_filename: &str) {
    let warm_full_filename = warm_full_filename.trim();
    if !warm_full_filename.is_empty() {
        retained.insert(warm_full_filename.to_string());
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn release(version: &str, rid: &str, full: &str, delta: Option<&str>) -> ReleaseEntry {
        let mut release = ReleaseEntry {
            version: version.to_string(),
            channels: vec!["stable".to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: full.to_string(),
            full_size: 1,
            full_sha256: String::new(),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demo".to_string(),
            install_directory: "demo".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: BTreeMap::new(),
        };
        if let Some(delta) = delta {
            release.set_primary_delta(Some(crate::releases::manifest::DeltaArtifact::bsdiff_zstd(
                "primary", "", delta, 1, "",
            )));
        }
        release
    }

    fn index() -> ReleaseIndex {
        ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![
                release("1.0.0", "linux-x64", "v1-full.tar.zst", None),
                release("1.1.0", "linux-x64", "v2-full.tar.zst", Some("v2-delta.tar.zst")),
                release("1.2.0", "linux-x64", "v3-full.tar.zst", Some("v3-delta.tar.zst")),
            ],
            ..ReleaseIndex::default()
        }
    }

    #[test]
    fn cache_policy_release_graph_keeps_graph_checkpoints_and_warm_full() {
        let retained = retained_artifacts_for_cache_policy(
            &index(),
            InstallArtifactCachePolicy::default(),
            "warm-full.tar.zst",
            1,
        );

        assert!(retained.contains("v1-full.tar.zst"));
        assert!(retained.contains("v2-delta.tar.zst"));
        assert!(retained.contains("v3-delta.tar.zst"));
        assert!(retained.contains("v3-full.tar.zst"));
        assert!(retained.contains("warm-full.tar.zst"));
    }

    #[test]
    fn cache_policy_latest_full_keeps_newest_fulls_and_warm_full() {
        let retained = retained_artifacts_for_cache_policy(
            &index(),
            InstallArtifactCachePolicy {
                retention: InstallArtifactCacheRetention::LatestFull,
                keep_full_count: 2,
            },
            "warm-full.tar.zst",
            3,
        );

        assert!(!retained.contains("v1-full.tar.zst"));
        assert!(retained.contains("v2-full.tar.zst"));
        assert!(retained.contains("v3-full.tar.zst"));
        assert!(retained.contains("warm-full.tar.zst"));
        assert!(retained.iter().all(|key| !key.contains("delta")));
    }

    #[test]
    fn cache_policy_just_installed_keeps_only_warm_full() {
        let retained = retained_artifacts_for_cache_policy(
            &index(),
            InstallArtifactCachePolicy {
                retention: InstallArtifactCacheRetention::JustInstalled,
                keep_full_count: 1,
            },
            "warm-full.tar.zst",
            3,
        );

        assert_eq!(retained, BTreeSet::from(["warm-full.tar.zst".to_string()]));
    }

    #[test]
    fn cache_policy_none_keeps_no_artifacts() {
        let retained = retained_artifacts_for_cache_policy(
            &index(),
            InstallArtifactCachePolicy {
                retention: InstallArtifactCacheRetention::None,
                keep_full_count: 1,
            },
            "warm-full.tar.zst",
            3,
        );

        assert!(retained.is_empty());
    }
}
