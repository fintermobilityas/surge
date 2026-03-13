#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::manual_let_else,
    clippy::needless_continue,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::formatters::{format_byte_progress, format_bytes, format_duration};
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use surge_core::config::manifest::{ShortcutLocation, SurgeManifest};
use surge_core::crypto::sha256::sha256_hex_file;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::delta::patch_format_from_magic_prefix;
use surge_core::releases::manifest::{
    DeltaArtifact, PATCH_FORMAT_BSDIFF4, PATCH_FORMAT_BSDIFF4_ARCHIVE_V2, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2,
    PATCH_FORMAT_CHUNKED_BSDIFF_V1, ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index,
};
use surge_core::releases::restore::required_artifacts_for_index;
use surge_core::releases::version::compare_versions;
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
    let name = app.effective_name();
    let main_exe = app.effective_main_exe();
    let install_directory = app.effective_install_directory();
    let supervisor_id = app.supervisor_id.clone();
    let icon = target.icon.clone();
    let shortcuts = target.shortcuts.clone();
    let persistent_assets = target.persistent_assets.clone();
    let installers = target.installers.clone();
    let environment = target.environment.clone();

    let storage_config = super::build_app_scoped_storage_config(&manifest, manifest_path, &app_id)?;
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
    let full_sha256 = sha256_hex_file(&full_archive)?;
    let delta_filename = format!("{app_id}-{version}-{rid}-delta.tar.zst");
    let delta_archive = packages_dir.join(&delta_filename);
    let delta_available = delta_archive.is_file();
    let delta_size_hint = if delta_available {
        std::fs::metadata(&delta_archive)?.len() as i64
    } else {
        0
    };

    let existing_index = fetch_existing_release_index(&*backend).await?;
    let has_existing_full_for_rid = if let Some(index) = existing_index.as_ref() {
        rid_has_uploaded_full_artifact(&*backend, index, &rid).await?
    } else {
        false
    };
    let full_uploaded = !has_existing_full_for_rid || !delta_available;
    let total_upload_bytes = (if full_uploaded { full_size } else { 0 })
        .saturating_add(delta_size_hint)
        .max(0) as u64;
    let mut uploaded_bytes_progress = 0u64;

    if full_uploaded {
        uploaded_bytes_progress = uploaded_bytes_progress
            .saturating_add(upload_artifact_with_feedback(&*backend, "full", &full_filename, &full_archive).await?);
        logline::subtle(&format!(
            "      {}",
            format_byte_progress(uploaded_bytes_progress, total_upload_bytes, "uploaded")
        ));
    } else {
        logline::info("Skipping full package upload: existing full baseline found for RID; publishing delta only.");
    }

    let (delta_filename, delta_size, delta_sha256, delta_patch_format, delta_uploaded) = if delta_available {
        let delta_size = std::fs::metadata(&delta_archive)?.len() as i64;
        upload_artifact_with_feedback(&*backend, "delta", &delta_filename, &delta_archive).await?;
        let delta_sha256 = sha256_hex_file(&delta_archive)?;
        let delta_patch_format = infer_delta_patch_format(&delta_archive)?;
        uploaded_bytes_progress = uploaded_bytes_progress.saturating_add(delta_size.max(0) as u64);
        logline::subtle(&format!(
            "      {}",
            format_byte_progress(uploaded_bytes_progress, total_upload_bytes, "uploaded")
        ));
        (delta_filename, delta_size, delta_sha256, delta_patch_format, true)
    } else {
        (String::new(), 0, String::new(), String::new(), false)
    };
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        if full_uploaded && delta_uploaded {
            "Uploaded full and delta artifacts"
        } else if full_uploaded {
            "Uploaded full artifact (no delta package found)"
        } else {
            "Uploaded delta artifact (existing full baseline retained)"
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
        delta_patch_format,
        name,
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
    let uploaded_count = usize::from(full_uploaded) + usize::from(delta_uploaded);
    let uploaded_bytes_total = (if full_uploaded { full_size } else { 0 }) + delta_size;
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

async fn fetch_existing_release_index(backend: &dyn StorageBackend) -> Result<Option<ReleaseIndex>> {
    match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => Ok(Some(decompress_release_index(&data)?)),
        Err(SurgeError::NotFound(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

async fn rid_has_uploaded_full_artifact(backend: &dyn StorageBackend, index: &ReleaseIndex, rid: &str) -> Result<bool> {
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
    delta_patch_format: String,
    name: String,
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

    let mut entry = ReleaseEntry {
        version: version.to_string(),
        channels: channels.into_iter().collect(),
        os: detect_os_from_rid(rid),
        rid: rid.to_string(),
        is_genesis: is_genesis_for_rid,
        full_filename,
        full_size,
        full_sha256,
        deltas: Vec::new(),
        preferred_delta_id: String::new(),
        created_utc: chrono::Utc::now().to_rfc3339(),
        release_notes: String::new(),
        name,
        main_exe,
        install_directory,
        supervisor_id,
        icon,
        shortcuts,
        persistent_assets,
        installers,
        environment,
    };
    let primary_delta = if delta_filename.trim().is_empty() {
        None
    } else if delta_patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V2) {
        Some(DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "",
            &delta_filename,
            delta_size,
            &delta_sha256,
        ))
    } else if delta_patch_format.eq_ignore_ascii_case(PATCH_FORMAT_BSDIFF4_ARCHIVE_V2) {
        Some(DeltaArtifact::bsdiff_archive_zstd(
            "primary",
            "",
            &delta_filename,
            delta_size,
            &delta_sha256,
        ))
    } else if delta_patch_format.eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1) {
        Some(DeltaArtifact::chunked_bsdiff_zstd(
            "primary",
            "",
            &delta_filename,
            delta_size,
            &delta_sha256,
        ))
    } else {
        Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "",
            &delta_filename,
            delta_size,
            &delta_sha256,
        ))
    };
    entry.set_primary_delta(primary_delta);
    index.releases.push(entry);

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
        for delta in release.all_deltas() {
            let key = delta.filename.trim();
            if !key.is_empty() {
                candidates.insert(key.to_string());
            }
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

fn infer_delta_patch_format(delta_archive: &Path) -> Result<String> {
    let file = File::open(delta_archive)?;
    let mut decoder = match zstd::stream::read::Decoder::new(file) {
        Ok(decoder) => decoder,
        Err(_) => return Ok(PATCH_FORMAT_BSDIFF4.to_string()),
    };
    let mut prefix = [0u8; 4];
    let bytes_read = match decoder.read(&mut prefix) {
        Ok(bytes_read) => bytes_read,
        Err(_) => return Ok(PATCH_FORMAT_BSDIFF4.to_string()),
    };
    if bytes_read == prefix.len()
        && let Some(patch_format) = patch_format_from_magic_prefix(&prefix)
    {
        return Ok(patch_format.to_string());
    }
    Ok(PATCH_FORMAT_BSDIFF4.to_string())
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::info(&format!("[{stage}/{total}] {text}"));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::success(&format!("[{stage}/{total}] {text}"));
}

async fn upload_artifact_with_feedback(
    backend: &dyn StorageBackend,
    artifact_kind: &str,
    key: &str,
    source_path: &Path,
) -> Result<u64> {
    let total_bytes = std::fs::metadata(source_path)?.len();
    logline::subtle(&format!(
        "  Uploading {artifact_kind} artifact {key} ({})",
        format_bytes(total_bytes)
    ));

    let started = Instant::now();
    let upload_running = Arc::new(AtomicBool::new(true));
    let bytes_done = Arc::new(AtomicU64::new(0));

    let upload_running_for_heartbeat = Arc::clone(&upload_running);
    let bytes_done_for_heartbeat = Arc::clone(&bytes_done);
    let key_for_heartbeat = key.to_string();
    let heartbeat = thread::spawn(move || {
        while upload_running_for_heartbeat.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_secs(5));
            if !upload_running_for_heartbeat.load(Ordering::Relaxed) {
                break;
            }

            let uploaded = bytes_done_for_heartbeat.load(Ordering::Relaxed).min(total_bytes);
            let progress = if uploaded == 0 {
                format!(
                    "uploaded 0 B / {} (elapsed {})",
                    format_bytes(total_bytes),
                    format_duration(started.elapsed())
                )
            } else {
                format!(
                    "{} (elapsed {})",
                    format_byte_progress(uploaded, total_bytes, "uploaded"),
                    format_duration(started.elapsed())
                )
            };
            logline::subtle(&format!("      {key_for_heartbeat}: {progress}"));
        }
    });

    let bytes_done_for_progress = Arc::clone(&bytes_done);
    let progress = move |done: u64, _total: u64| {
        bytes_done_for_progress.store(done.min(total_bytes), Ordering::Relaxed);
    };

    let upload_result = backend.upload_from_file(key, source_path, Some(&progress)).await;
    bytes_done.store(total_bytes, Ordering::Relaxed);
    upload_running.store(false, Ordering::Relaxed);
    let _ = heartbeat.join();
    upload_result?;

    logline::subtle(&format!(
        "      {key}: {} in {}",
        format_byte_progress(total_bytes, total_bytes, "uploaded"),
        format_duration(started.elapsed())
    ));

    Ok(total_bytes)
}
