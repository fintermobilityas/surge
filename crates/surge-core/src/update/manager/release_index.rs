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
    let supported_delta_chain = delta_chain.filter(|chain| {
        chain
            .iter()
            .all(|release| release.selected_delta().is_some_and(|delta| is_supported_delta(&delta)))
    });

    let (apply_releases, apply_strategy, download_size) = if let Some(chain) = supported_delta_chain {
        let selected: Vec<ReleaseEntry> = chain.into_iter().cloned().collect();
        let size = selected
            .iter()
            .filter_map(ReleaseEntry::selected_delta)
            .map(|delta| delta.size)
            .sum();
        (selected, ApplyStrategy::Delta, size)
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
