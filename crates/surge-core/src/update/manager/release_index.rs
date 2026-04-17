use std::cmp::Ordering;

use tracing::{debug, info};

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::error::{Result, SurgeError};
use crate::platform::detect::current_rid;
use crate::releases::delta::is_supported_delta;
use crate::releases::manifest::{
    ReleaseEntry, ReleaseIndex, decompress_release_index, get_delta_chain, get_releases_newer_than,
};
use crate::releases::version::compare_versions;
use crate::storage::create_storage_backend;
use crate::storage_config::append_prefix;

use super::{ApplyStrategy, UpdateInfo, UpdateManager};

pub(super) async fn load_release_index(manager: &mut UpdateManager) -> Result<ReleaseIndex> {
    let base_prefix = manager.ctx.storage_config().prefix;
    let scoped_prefix = app_scoped_prefix(&base_prefix, &manager.app_id);

    if let Some(scoped_prefix) = scoped_prefix {
        debug!(
            app_id = %manager.app_id,
            base_prefix = %base_prefix,
            scoped_prefix = %scoped_prefix,
            "Requiring app-scoped release index derived from configured prefix"
        );

        let mut scoped_config = manager.ctx.storage_config();
        scoped_config.prefix = scoped_prefix.clone();
        let scoped_backend = create_storage_backend(&scoped_config)?;

        match scoped_backend.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(data) => {
                info!(
                    app_id = %manager.app_id,
                    scoped_prefix = %scoped_prefix,
                    "Using app-scoped storage prefix for update checks"
                );
                manager.ctx.set_storage_prefix(&scoped_prefix);
                manager.storage = scoped_backend;
                return decompress_release_index(&data);
            }
            Err(SurgeError::NotFound(_)) => {
                return Err(SurgeError::NotFound(format!(
                    "Release index '{RELEASES_FILE_COMPRESSED}' not found on required app-scoped prefix"
                )));
            }
            Err(e) => return Err(e),
        }
    }

    match manager.storage.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data),
        Err(SurgeError::NotFound(_)) => Err(SurgeError::NotFound(format!(
            "Release index '{RELEASES_FILE_COMPRESSED}' not found"
        ))),
        Err(e) => Err(e),
    }
}

fn app_scoped_prefix(base_prefix: &str, app_id: &str) -> Option<String> {
    let app_id = app_id.trim().trim_matches('/');
    if app_id.is_empty() {
        return None;
    }

    let already_scoped = base_prefix
        .trim()
        .trim_matches('/')
        .rsplit('/')
        .next()
        .is_some_and(|segment| segment == app_id);
    if already_scoped {
        return None;
    }

    let scoped_prefix = append_prefix(base_prefix, app_id);
    (scoped_prefix != base_prefix).then_some(scoped_prefix)
}

pub(super) fn resolve_update_info(manager: &mut UpdateManager, index: ReleaseIndex) -> Result<Option<UpdateInfo>> {
    let current_rid = current_rid();
    let current_os = normalize_os_label(current_rid.split('-').next().unwrap_or_default());

    if !index.app_id.is_empty() && index.app_id != manager.app_id {
        return Err(SurgeError::Update(format!(
            "Release index app_id '{}' does not match requested app '{}'",
            index.app_id, manager.app_id
        )));
    }

    let mut compatible_index = index.clone();
    compatible_index.releases.retain(|release| {
        release.channels.iter().any(|channel| channel == &manager.channel)
            && compare_versions(&release.version, &manager.current_version) == Ordering::Greater
            && release_matches_rid(release, &current_rid)
            && release_matches_os(release, &current_os)
    });

    let newer = get_releases_newer_than(&compatible_index, &manager.current_version, &manager.channel);
    if newer.is_empty() {
        debug!("No updates available");
        manager.cached_index = Some(index);
        return Ok(None);
    }

    let latest = newer
        .last()
        .map(|release| (*release).clone())
        .ok_or_else(|| SurgeError::Update("No latest release found".to_string()))?;
    let latest_version = latest.version.clone();
    let delta_chain = get_delta_chain(
        &compatible_index,
        &manager.current_version,
        &latest_version,
        &manager.channel,
    );

    let available_releases: Vec<ReleaseEntry> = newer.into_iter().cloned().collect();
    let resolved_delta_chain = delta_chain
        .and_then(|chain| resolve_delta_chain_for_current_install(chain.as_slice(), &manager.current_version));
    let supported_delta_chain = resolved_delta_chain.filter(|chain| {
        chain
            .iter()
            .all(|release| release.selected_delta().is_some_and(|delta| is_supported_delta(&delta)))
    });

    let (apply_releases, apply_strategy, download_size) = if let Some(chain) = supported_delta_chain {
        let size = chain
            .iter()
            .filter_map(ReleaseEntry::selected_delta)
            .map(|delta| delta.size)
            .sum();
        (chain, ApplyStrategy::Delta, size)
    } else {
        (vec![latest.clone()], ApplyStrategy::Full, latest.full_size)
    };
    let delta_available = matches!(apply_strategy, ApplyStrategy::Delta);

    info!(
        latest_version = %latest_version,
        delta_available,
        download_size,
        releases_count = available_releases.len(),
        "Updates available"
    );

    manager.cached_index = Some(index);

    Ok(Some(UpdateInfo {
        available_releases,
        latest_version,
        delta_available,
        download_size,
        apply_releases,
        apply_strategy,
    }))
}

/// For each release in the chain, pick the delta whose `from_version` matches
/// the previous step's version (or `current_version` for the first step) and
/// rewrite the release entry to expose only that delta.
///
/// Returns `None` if any step lacks a usable delta. Without this lookup, a
/// delta built against one source version can be applied to a different
/// installed version, causing sparse-file basis hash mismatches at apply time.
///
/// Releases pushed before the index recorded `from_version` carry a single
/// legacy delta with an empty `from_version`. We accept that delta as a
/// fallback because legacy `surge pack` always built the delta from the
/// immediately preceding release; the apply path's basis hash check is the
/// final guard against an incompatible legacy chain.
fn resolve_delta_chain_for_current_install(
    chain: &[&ReleaseEntry],
    current_version: &str,
) -> Option<Vec<ReleaseEntry>> {
    let mut resolved = Vec::with_capacity(chain.len());
    let mut prev_version = current_version.to_string();
    for release in chain {
        let delta = release.delta_from_source(&prev_version).or_else(|| {
            release
                .deltas
                .iter()
                .find(|delta| !delta.filename.trim().is_empty() && delta.from_version.trim().is_empty())
                .cloned()
        })?;
        let mut entry = (*release).clone();
        entry.preferred_delta_id.clone_from(&delta.id);
        entry.deltas = vec![delta];
        prev_version.clone_from(&entry.version);
        resolved.push(entry);
    }
    Some(resolved)
}

pub(super) fn release_matches_rid(release: &ReleaseEntry, current_rid: &str) -> bool {
    release.rid.is_empty() || release.rid == current_rid
}

pub(super) fn release_matches_os(release: &ReleaseEntry, current_os: &str) -> bool {
    release.os.is_empty() || normalize_os_label(&release.os) == current_os
}

pub(super) fn normalize_os_label(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "windows" | "win" => "win".to_string(),
        "macos" | "osx" | "darwin" => "osx".to_string(),
        "linux" => "linux".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_delta_chain_for_current_install;
    use crate::releases::manifest::{DeltaArtifact, ReleaseEntry};

    fn release_with_deltas(version: &str, deltas: Vec<DeltaArtifact>) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["production".to_string()],
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
            is_genesis: false,
            full_filename: format!("demo-{version}-linux-x64-full.tar.zst"),
            full_size: 1,
            full_sha256: "hash".to_string(),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas,
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
            environment: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn resolver_picks_delta_matching_current_install_version() {
        let release = release_with_deltas(
            "1.2.0",
            vec![
                DeltaArtifact::sparse_file_ops_zstd("primary", "1.1.0", "delta-from-110.tar.zst", 100, "sha-110"),
                DeltaArtifact::sparse_file_ops_zstd("from-100", "1.0.0", "delta-from-100.tar.zst", 200, "sha-100"),
            ],
        );

        let resolved = resolve_delta_chain_for_current_install(&[&release], "1.0.0").expect("chain resolves");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].deltas.len(), 1);
        assert_eq!(resolved[0].deltas[0].from_version, "1.0.0");
        assert_eq!(resolved[0].preferred_delta_id, "from-100");
    }

    #[test]
    fn resolver_falls_back_to_legacy_delta_with_empty_from_version() {
        // Releases pushed before from_version was tracked carry one delta with
        // an empty from_version. The resolver must still accept those so legacy
        // chains keep updating.
        let release = release_with_deltas(
            "1.1.0",
            vec![DeltaArtifact::sparse_file_ops_zstd(
                "primary",
                "",
                "delta-legacy.tar.zst",
                100,
                "sha-legacy",
            )],
        );

        let resolved = resolve_delta_chain_for_current_install(&[&release], "1.0.0").expect("legacy chain resolves");
        assert_eq!(resolved[0].deltas.len(), 1);
        assert!(resolved[0].deltas[0].from_version.is_empty());
    }

    #[test]
    fn resolver_returns_none_when_no_delta_matches_or_is_legacy() {
        // A delta with a wrong from_version and no legacy fallback must reject
        // the chain so the apply path falls back to a full install instead of
        // attempting an incompatible patch.
        let release = release_with_deltas(
            "1.2.0",
            vec![DeltaArtifact::sparse_file_ops_zstd(
                "primary",
                "1.1.0",
                "delta-from-110.tar.zst",
                100,
                "sha-110",
            )],
        );

        assert!(resolve_delta_chain_for_current_install(&[&release], "1.0.0").is_none());
    }
}
