use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use crate::formatters::{format_byte_progress, format_duration};
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};
use surge_core::crypto::sha256::sha256_hex_file;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index};
use surge_core::releases::restore::required_artifacts_for_index;
use surge_core::storage::{self, StorageBackend};

/// Push built packages to cloud storage.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    channel: &str,
    packages_dir: &Path,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();
    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and target");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let (app, target) = manifest
        .find_app_with_target(&app_id, &rid)
        .ok_or_else(|| SurgeError::Config(format!("Target '{rid}' not found for app '{app_id}'")))?;
    let main_exe = app.effective_main_exe();
    let install_directory = app.effective_install_directory();
    let supervisor_id = app.supervisor_id.clone();
    let icon = target.icon.clone();
    let shortcuts = target.shortcuts.clone();
    let persistent_assets = target.persistent_assets.clone();
    let installers = target.installers.clone();
    let environment = target.environment.clone();

    let storage_config = super::build_app_scoped_storage_config(&manifest, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    print_stage_done(theme, 1, TOTAL_STAGES, &format!("Target: {app_id}/{rid}"));

    print_stage(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Validating package inputs at {}", packages_dir.display()),
    );
    if !packages_dir.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Packages directory does not exist: {}",
            packages_dir.display()
        )));
    }

    let full_filename = format!("{app_id}-{version}-{rid}-full.tar.zst");
    let full_archive = packages_dir.join(&full_filename);
    if !full_archive.is_file() {
        return Err(SurgeError::Storage(format!(
            "Full archive not found: {}",
            full_archive.display()
        )));
    }
    print_stage_done(
        theme,
        2,
        TOTAL_STAGES,
        &format!("Found full package {}", full_archive.display()),
    );

    print_stage(theme, 3, TOTAL_STAGES, "Uploading release artifacts");
    let full_size = std::fs::metadata(&full_archive)?.len() as i64;
    let delta_filename = format!("{app_id}-{version}-{rid}-delta.tar.zst");
    let delta_archive = packages_dir.join(&delta_filename);
    let delta_size_hint = if delta_archive.is_file() {
        std::fs::metadata(&delta_archive)?.len() as i64
    } else {
        0
    };
    let total_upload_bytes = full_size.saturating_add(delta_size_hint).max(0) as u64;

    backend.upload_from_file(&full_filename, &full_archive, None).await?;
    let mut uploaded_bytes_progress = full_size.max(0) as u64;
    println!(
        "{}",
        theme.subtle(&format!(
            "      {}",
            format_byte_progress(uploaded_bytes_progress, total_upload_bytes, "uploaded")
        ))
    );

    let full_sha256 = sha256_hex_file(&full_archive)?;
    let (delta_filename, delta_size, delta_sha256, delta_uploaded) = if delta_archive.is_file() {
        backend.upload_from_file(&delta_filename, &delta_archive, None).await?;
        let delta_size = std::fs::metadata(&delta_archive)?.len() as i64;
        uploaded_bytes_progress = uploaded_bytes_progress.saturating_add(delta_size.max(0) as u64);
        println!(
            "{}",
            theme.subtle(&format!(
                "      {}",
                format_byte_progress(uploaded_bytes_progress, total_upload_bytes, "uploaded")
            ))
        );
        (delta_filename, delta_size, sha256_hex_file(&delta_archive)?, true)
    } else {
        (String::new(), 0, String::new(), false)
    };
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        if delta_uploaded {
            "Uploaded full and delta artifacts"
        } else {
            "Uploaded full artifact (no delta package found)"
        },
    );

    print_stage(theme, 4, TOTAL_STAGES, "Updating release index");
    let pruned = update_release_index(
        &*backend,
        &app_id,
        version,
        &rid,
        channel,
        full_filename,
        full_size,
        full_sha256,
        delta_filename,
        delta_size,
        delta_sha256,
        main_exe,
        install_directory,
        supervisor_id,
        icon,
        shortcuts,
        persistent_assets,
        installers,
        environment,
    )
    .await?;
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!("Updated {RELEASES_FILE_COMPRESSED} (pruned {pruned} stale artifact(s))"),
    );

    print_stage(theme, 5, TOTAL_STAGES, "Finalize push summary");
    let uploaded_count = if delta_uploaded { 2 } else { 1 };
    let uploaded_bytes_total = if delta_uploaded {
        full_size + delta_size
    } else {
        full_size
    };
    let uploaded_bytes_u64 = uploaded_bytes_total.max(0) as u64;
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Published {app_id} v{version} ({rid}) -> {channel} in {} (objects: {uploaded_count}, {})",
            format_duration(started.elapsed()),
            format_byte_progress(uploaded_bytes_u64, total_upload_bytes, "uploaded")
        ),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn update_release_index(
    backend: &dyn StorageBackend,
    app_id: &str,
    version: &str,
    rid: &str,
    channel: &str,
    full_filename: String,
    full_size: i64,
    full_sha256: String,
    delta_filename: String,
    delta_size: i64,
    delta_sha256: String,
    main_exe: String,
    install_directory: String,
    supervisor_id: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
    persistent_assets: Vec<String>,
    installers: Vec<String>,
    environment: BTreeMap<String, String>,
) -> Result<usize> {
    let mut index = match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data)?,
        Err(SurgeError::NotFound(_)) => ReleaseIndex {
            schema: SCHEMA_VERSION,
            app_id: app_id.to_string(),
            ..ReleaseIndex::default()
        },
        Err(e) => return Err(e),
    };

    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::Storage(format!(
            "Release index belongs to '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    if index.app_id.is_empty() {
        index.app_id = app_id.to_string();
    }

    let mut channels = BTreeSet::new();
    channels.insert(channel.to_string());

    for existing in &index.releases {
        if existing.version == version && existing.rid == rid {
            for existing_channel in &existing.channels {
                channels.insert(existing_channel.clone());
            }
        }
    }

    let is_genesis_for_rid = !index
        .releases
        .iter()
        .any(|release| release.rid == rid || release.rid.is_empty());

    index
        .releases
        .retain(|release| !(release.version == version && release.rid == rid));

    index.releases.push(ReleaseEntry {
        version: version.to_string(),
        channels: channels.into_iter().collect(),
        os: detect_os_from_rid(rid),
        rid: rid.to_string(),
        is_genesis: is_genesis_for_rid,
        full_filename,
        full_size,
        full_sha256,
        delta_filename,
        delta_size,
        delta_sha256,
        created_utc: chrono::Utc::now().to_rfc3339(),
        release_notes: String::new(),
        main_exe,
        install_directory,
        supervisor_id,
        icon,
        shortcuts,
        persistent_assets,
        installers,
        environment,
    });

    index.last_write_utc = chrono::Utc::now().to_rfc3339();

    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;
    let pruned = prune_redundant_artifacts(backend, &index).await?;

    Ok(pruned)
}

async fn prune_redundant_artifacts(backend: &dyn StorageBackend, index: &ReleaseIndex) -> Result<usize> {
    let required = required_artifacts_for_index(index);

    let mut candidates = BTreeSet::new();
    for release in &index.releases {
        let full = release.full_filename.trim();
        if !full.is_empty() {
            candidates.insert(full.to_string());
        }
        let delta = release.delta_filename.trim();
        if !delta.is_empty() {
            candidates.insert(delta.to_string());
        }
    }

    let mut pruned = 0usize;
    for key in candidates {
        if required.contains(&key) {
            continue;
        }

        match backend.delete_object(&key).await {
            Ok(()) | Err(SurgeError::NotFound(_)) => {
                pruned += 1;
            }
            Err(e) => return Err(e),
        }
    }

    if pruned > 0 {
        tracing::info!(pruned, retained = required.len(), "Pruned redundant release artifacts");
    }
    Ok(pruned)
}

fn detect_os_from_rid(rid: &str) -> String {
    rid.split('-').next().unwrap_or("unknown").to_string()
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.info(&format!("[{stage}/{total}] {text}")));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    println!("{}", theme.success(&format!("[{stage}/{total}] {text}")));
}
