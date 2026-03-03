use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use crate::formatters::{format_byte_progress, format_bytes, format_duration};
use crate::ui::UiTheme;
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
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, "Resolving source and destination manifests");
    let src_manifest = SurgeManifest::from_file(manifest_path)?;
    let (source_app_id, rid) = resolve_source_app_and_rid(&src_manifest, app_id, rid)?;
    let canonical_app_id = canonicalize_app_id(&source_app_id, &rid);
    let dest_manifest = SurgeManifest::from_file(dest_manifest_path)?;

    let src_config = build_storage_config(&src_manifest, &source_app_id)?;
    let dest_config = build_storage_config(&dest_manifest, &canonical_app_id)?;

    let src_backend = storage::create_storage_backend(&src_config)?;
    let dest_backend = storage::create_storage_backend(&dest_config)?;
    print_stage_done(
        theme,
        1,
        TOTAL_STAGES,
        &format!("Migrating {source_app_id}/{rid} -> {canonical_app_id}/{rid}"),
    );

    print_stage(theme, 2, TOTAL_STAGES, "Loading and filtering release index");

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
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!(
            "Retained {} release entry(ies) for migration",
            migrated_index.releases.len()
        ),
    );

    print_stage(theme, 3, TOTAL_STAGES, "Planning artifact copy operations");
    let mut copy_operations: BTreeSet<CopyOperation> = BTreeSet::new();
    for release in &mut migrated_index.releases {
        if !release.full_filename.is_empty() {
            let old_key = release.full_filename.clone();
            let new_key = canonicalize_artifact_key(&old_key, &source_app_id, &canonical_app_id);
            release.full_filename.clone_from(&new_key);
            copy_operations.insert(CopyOperation {
                source_key: old_key,
                destination_key: new_key,
                size_hint: release.full_size.max(0) as u64,
            });
        }
        if !release.delta_filename.is_empty() {
            let old_key = release.delta_filename.clone();
            let new_key = canonicalize_artifact_key(&old_key, &source_app_id, &canonical_app_id);
            release.delta_filename.clone_from(&new_key);
            copy_operations.insert(CopyOperation {
                source_key: old_key,
                destination_key: new_key,
                size_hint: release.delta_size.max(0) as u64,
            });
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
                copy_operations.insert(CopyOperation {
                    source_key: entry.key.clone(),
                    destination_key,
                    size_hint: entry.size.max(0) as u64,
                });
            }

            if listing.is_truncated {
                marker = listing.next_marker;
            } else {
                break;
            }
        }
    }
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        &format!("Planned {} artifact copy operation(s)", copy_operations.len()),
    );

    print_stage(theme, 4, TOTAL_STAGES, "Copying artifacts and writing release index");
    let total_planned_bytes: u64 = copy_operations.iter().map(|op| op.size_hint).sum();
    let mut migrated = 0u64;
    let mut migrated_bytes = 0u64;
    for operation in &copy_operations {
        let source_key = &operation.source_key;
        let destination_key = &operation.destination_key;
        tracing::debug!("Migrating: {source_key} -> {destination_key}");
        let data = src_backend.get_object(source_key).await?;
        dest_backend
            .put_object(destination_key, &data, "application/octet-stream")
            .await?;
        migrated += 1;
        migrated_bytes = migrated_bytes.saturating_add(data.len() as u64);
        let progress = if total_planned_bytes > 0 {
            format_byte_progress(migrated_bytes, total_planned_bytes, "copied")
        } else {
            format!("copied {migrated}/{} artifact(s)", copy_operations.len())
        };
        println!("{}", theme.subtle(&format!("      {progress}")));
    }

    let rewritten_releases_data = compress_release_index(&migrated_index, DEFAULT_ZSTD_LEVEL)?;
    dest_backend
        .put_object(
            RELEASES_FILE_COMPRESSED,
            &rewritten_releases_data,
            "application/octet-stream",
        )
        .await?;
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!(
            "Copied {migrated} artifact(s) and wrote {RELEASES_FILE_COMPRESSED} ({})",
            if total_planned_bytes > 0 {
                format_byte_progress(migrated_bytes, total_planned_bytes, "copied")
            } else {
                format_bytes(migrated_bytes)
            }
        ),
    );

    print_stage(theme, 5, TOTAL_STAGES, "Finalize migration summary");
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Completed in {} (source provider: {}, destination provider: {})",
            format_duration(started.elapsed()),
            src_manifest.storage.provider,
            dest_manifest.storage.provider
        ),
    );
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

    let source_app_id = super::resolve_app_id_with_rid_hint(manifest, None, requested_rid.as_deref())?;
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CopyOperation {
    source_key: String,
    destination_key: String,
    size_hint: u64,
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

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.info(&format!("[{stage}/{total}] {text}")));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.success(&format!("[{stage}/{total}] {text}")));
}
