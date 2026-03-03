use std::collections::BTreeSet;
use std::path::Path;

use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{compress_release_index, decompress_release_index};
use surge_core::storage;

/// Migrate release data from one storage backend to another.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    rid: Option<&str>,
    dest_manifest_path: &Path,
) -> Result<()> {
    let src_manifest = SurgeManifest::from_file(manifest_path)?;
    let (source_app_id, rid) = resolve_source_app_and_rid(&src_manifest, app_id, rid)?;
    let canonical_app_id = canonicalize_app_id(&source_app_id, &rid);
    let dest_manifest = SurgeManifest::from_file(dest_manifest_path)?;

    let src_config = build_storage_config(&src_manifest, &source_app_id)?;
    let dest_config = build_storage_config(&dest_manifest, &canonical_app_id)?;

    let src_backend = storage::create_storage_backend(&src_config)?;
    let dest_backend = storage::create_storage_backend(&dest_config)?;

    tracing::info!(
        "Migrating {source_app_id}/{rid} -> {canonical_app_id}/{rid} from {} to {}",
        src_manifest.storage.provider,
        dest_manifest.storage.provider
    );

    let releases_data = src_backend.get_object(RELEASES_FILE_COMPRESSED).await?;
    let release_index = decompress_release_index(&releases_data)?;
    if !release_index.app_id.is_empty()
        && release_index.app_id != source_app_id
        && canonicalize_app_id(&release_index.app_id, &rid) != canonical_app_id
    {
        return Err(SurgeError::Config(format!(
            "Source release index belongs to '{}' not '{}'",
            release_index.app_id, source_app_id
        )));
    }

    let mut migrated_index = release_index.clone();
    migrated_index.app_id = canonical_app_id.clone();
    migrated_index
        .releases
        .retain(|release| release.rid.is_empty() || release.rid == rid);

    let mut copy_operations: BTreeSet<(String, String)> = BTreeSet::new();
    for release in &mut migrated_index.releases {
        if !release.full_filename.is_empty() {
            let old_key = release.full_filename.clone();
            let new_key = canonicalize_artifact_key(&old_key, &source_app_id, &canonical_app_id);
            release.full_filename.clone_from(&new_key);
            copy_operations.insert((old_key, new_key));
        }
        if !release.delta_filename.is_empty() {
            let old_key = release.delta_filename.clone();
            let new_key = canonicalize_artifact_key(&old_key, &source_app_id, &canonical_app_id);
            release.delta_filename.clone_from(&new_key);
            copy_operations.insert((old_key, new_key));
        }
    }

    if copy_operations.is_empty() {
        tracing::warn!("No release files found in releases index for {source_app_id}/{rid}; trying legacy key layout");
        let mut marker: Option<String> = None;
        let prefix = format!("{source_app_id}/{rid}/");
        loop {
            let listing = src_backend.list_objects(&prefix, marker.as_deref(), 100).await?;
            for entry in &listing.entries {
                let destination_key = canonicalize_artifact_key(&entry.key, &source_app_id, &canonical_app_id);
                copy_operations.insert((entry.key.clone(), destination_key));
            }

            if listing.is_truncated {
                marker = listing.next_marker;
            } else {
                break;
            }
        }
    }

    let mut migrated = 0u64;
    for (source_key, destination_key) in &copy_operations {
        tracing::debug!("Migrating: {source_key} -> {destination_key}");
        let data = src_backend.get_object(source_key).await?;
        dest_backend
            .put_object(destination_key, &data, "application/octet-stream")
            .await?;
        migrated += 1;
    }

    let rewritten_releases_data = compress_release_index(&migrated_index, DEFAULT_ZSTD_LEVEL)?;
    dest_backend
        .put_object(
            RELEASES_FILE_COMPRESSED,
            &rewritten_releases_data,
            "application/octet-stream",
        )
        .await?;
    tracing::debug!("Migrated {}", RELEASES_FILE_COMPRESSED);

    tracing::info!("Migration complete: {migrated} object(s) migrated");
    Ok(())
}

fn resolve_source_app_and_rid(
    manifest: &SurgeManifest,
    requested_app_id: Option<&str>,
    requested_rid: Option<&str>,
) -> Result<(String, String)> {
    let requested_rid = requested_rid
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);

    if let Some(app_id) = requested_app_id.map(str::trim).filter(|value| !value.is_empty()) {
        if manifest.find_app(app_id).is_some() {
            let rid = super::resolve_rid(manifest, app_id, requested_rid.as_deref())?;
            return Ok((app_id.to_string(), rid));
        }

        let mut candidates: Vec<(String, String)> = manifest
            .apps
            .iter()
            .flat_map(|app| {
                manifest
                    .target_rids(&app.id)
                    .into_iter()
                    .map(move |target_rid| (app.id.clone(), target_rid))
            })
            .filter(|(source_id, target_rid)| {
                canonicalize_app_id(source_id, target_rid) == app_id
                    && requested_rid.as_ref().is_none_or(|requested| requested == target_rid)
            })
            .collect();
        candidates.sort();
        candidates.dedup();

        return match candidates.as_slice() {
            [(source_id, rid)] => Ok((source_id.clone(), rid.clone())),
            [] => Err(SurgeError::Config(format!(
                "No app found matching '{app_id}' for migration"
            ))),
            _ => Err(SurgeError::Config(format!(
                "Ambiguous migration app id '{app_id}'. Provide explicit --rid and/or full --app-id."
            ))),
        };
    }

    let source_app_id = super::resolve_app_id(manifest, None)?;
    let rid = super::resolve_rid(manifest, &source_app_id, requested_rid.as_deref())?;
    Ok((source_app_id, rid))
}

fn canonicalize_app_id(app_id: &str, rid: &str) -> String {
    let rid_suffix = format!("-{rid}");
    app_id.strip_suffix(&rid_suffix).unwrap_or(app_id).to_string()
}

fn canonicalize_artifact_key(key: &str, source_app_id: &str, canonical_app_id: &str) -> String {
    if source_app_id == canonical_app_id {
        return key.to_string();
    }

    let source_prefix = format!("{source_app_id}-");
    if let Some(rest) = key.strip_prefix(&source_prefix) {
        format!("{canonical_app_id}-{rest}")
    } else {
        key.to_string()
    }
}

fn build_storage_config(manifest: &SurgeManifest, app_id: &str) -> Result<surge_core::context::StorageConfig> {
    if manifest.storage.provider.trim().is_empty() {
        return Err(SurgeError::Config(
            "Storage provider is required for migration manifests".to_string(),
        ));
    }
    if manifest.storage.bucket.trim().is_empty() {
        return Err(SurgeError::Config(
            "Storage bucket/root is required for migration manifests".to_string(),
        ));
    }
    super::build_app_scoped_storage_config(manifest, app_id)
}
