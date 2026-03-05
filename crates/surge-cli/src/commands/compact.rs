use std::collections::BTreeSet;
use std::path::Path;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{compress_release_index, decompress_release_index};
use surge_core::releases::restore::{required_artifacts_for_index, restore_full_archive_for_version};
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

/// Compact a channel to a single latest full release and prune stale artifacts.
///
/// When `app_id` and `rid` are omitted, iterates over every app and target in the manifest.
pub async fn execute(manifest_path: &Path, app_id: Option<&str>, rid: Option<&str>, channel: &str) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let targets: Vec<(String, String)> = if let Some(app_id) = app_id {
        let app_id = app_id.to_string();
        let rid = super::resolve_rid(&manifest, &app_id, rid)?;
        vec![(app_id, rid)]
    } else {
        manifest
            .app_ids()
            .into_iter()
            .flat_map(|app_id| {
                manifest
                    .target_rids(&app_id)
                    .into_iter()
                    .map(move |rid| (app_id.clone(), rid))
            })
            .collect()
    };

    let total_targets = targets.len();
    logline::info(&format!("Compacting {total_targets} target(s) on channel '{channel}'"));
    logline::plain("");

    let mut errors = Vec::new();
    for (app_id, rid) in &targets {
        if let Err(e) = compact_single(&manifest, app_id, rid, channel).await {
            logline::warn(&format!("  Failed {app_id}/{rid}: {e}"));
            errors.push(format!("{app_id}/{rid}: {e}"));
        }
        logline::plain("");
    }

    if errors.is_empty() {
        logline::success(&format!("All {total_targets} target(s) compacted successfully."));
        Ok(())
    } else {
        Err(SurgeError::Storage(format!(
            "{} target(s) failed: {}",
            errors.len(),
            errors.join("; ")
        )))
    }
}

async fn compact_single(manifest: &SurgeManifest, app_id: &str, rid: &str, channel: &str) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, &format!("{app_id}/{rid}"));
    let storage_config = super::build_app_scoped_storage_config(manifest, app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;

    let mut index = match backend.get_object(RELEASES_FILE_COMPRESSED).await {
        Ok(data) => decompress_release_index(&data)?,
        Err(SurgeError::NotFound(_)) => {
            print_stage_done(theme, 1, TOTAL_STAGES, "No release index, skipped");
            return Ok(());
        }
        Err(e) => return Err(e),
    };
    let total_before = index.releases.len();

    print_stage(theme, 2, TOTAL_STAGES, "Finding latest release");
    let channel_name = channel.to_string();
    let latest_version = index
        .releases
        .iter()
        .filter(|r| r.rid == rid && r.channels.contains(&channel_name))
        .max_by(|a, b| compare_versions(&a.version, &b.version))
        .map(|r| r.version.clone());

    let latest_version = match latest_version {
        Some(v) => v,
        None => {
            print_stage_done(theme, 2, TOTAL_STAGES, &format!("No releases on '{channel}', skipped"));
            return Ok(());
        }
    };
    print_stage_done(theme, 2, TOTAL_STAGES, &format!("v{latest_version}"));

    print_stage(theme, 3, TOTAL_STAGES, "Ensuring latest full artifact exists");
    let full_materialized = ensure_release_full_artifact(&*backend, &index, rid, &latest_version).await?;
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        if full_materialized {
            "Rebuilt and uploaded latest full artifact"
        } else {
            "Latest full artifact already present"
        },
    );

    print_stage(
        theme,
        4,
        TOTAL_STAGES,
        "Pruning compacted channel history and stale artifacts",
    );
    let stale_filenames = referenced_artifacts(&index);
    let releases_on_channel_before = index
        .releases
        .iter()
        .filter(|release| release.rid == rid && release.channels.iter().any(|existing| existing == channel))
        .count();

    for release in &mut index.releases {
        if release.rid != rid || !release.channels.iter().any(|existing| existing == channel) {
            continue;
        }

        if release.version == latest_version {
            release.set_primary_delta(None);
        } else {
            release.channels.retain(|existing| existing != channel);
        }
    }
    index
        .releases
        .retain(|release| release.rid != rid || !release.channels.is_empty());

    let required = required_artifacts_for_index(&index);

    let mut deleted = 0usize;
    for key in &stale_filenames {
        if required.contains(key) {
            continue;
        }

        match backend.delete_object(key).await {
            Ok(()) | Err(SurgeError::NotFound(_)) => {
                deleted += 1;
            }
            Err(e) => {
                tracing::warn!("Failed to delete {key}: {e}");
            }
        }
    }

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;

    let removed = total_before - index.releases.len();
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!(
            "Pruned {older} older '{channel}' release(s), removed {removed} release row(s), deleted {deleted} artifact(s)",
            older = releases_on_channel_before.saturating_sub(1)
        ),
    );

    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Compacted to v{latest_version} (full only) in {}",
            format_duration(started.elapsed())
        ),
    );
    Ok(())
}

fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::info(&format!("[{stage}/{total}] {text}"));
}

fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::success(&format!("[{stage}/{total}] {text}"));
}

fn referenced_artifacts(index: &surge_core::releases::manifest::ReleaseIndex) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for release in &index.releases {
        let full = release.full_filename.trim();
        if !full.is_empty() {
            keys.insert(full.to_string());
        }
        for delta in release.all_deltas() {
            let key = delta.filename.trim();
            if !key.is_empty() {
                keys.insert(key.to_string());
            }
        }
    }
    keys
}

async fn ensure_release_full_artifact(
    backend: &dyn StorageBackend,
    index: &surge_core::releases::manifest::ReleaseIndex,
    rid: &str,
    version: &str,
) -> Result<bool> {
    let release = index
        .releases
        .iter()
        .find(|release| release.rid == rid && release.version == version)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} ({rid}) not found in index")))?;
    let full_filename = release.full_filename.trim();
    if full_filename.is_empty() {
        return Err(SurgeError::Storage(format!(
            "Release {version} ({rid}) has no full artifact descriptor"
        )));
    }

    match backend.head_object(full_filename).await {
        Ok(_) => Ok(false),
        Err(SurgeError::NotFound(_)) => {
            let archive = restore_full_archive_for_version(backend, index, rid, version).await?;
            backend
                .put_object(full_filename, &archive, "application/octet-stream")
                .await?;
            Ok(true)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex};

    fn write_manifest(path: &Path, store_dir: &Path, app_id: &str, rid: &str) {
        let yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_id}
    name: Compact Test App
    main_exe: demoapp
    targets:
      - rid: {rid}
",
            bucket = store_dir.display()
        );
        std::fs::write(path, yaml).unwrap();
    }

    fn make_release(version: &str, rid: &str, full_bytes: &[u8]) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["stable".to_string()],
            os: rid.split('-').next().unwrap_or("unknown").to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: format!("compact-app-{version}-{rid}-full.tar.zst"),
            full_size: i64::try_from(full_bytes.len()).unwrap(),
            full_sha256: sha256_hex(full_bytes),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: "Compact Test App".to_string(),
            main_exe: "demoapp".to_string(),
            install_directory: "demoapp".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    fn write_index(path: &Path, releases: Vec<ReleaseEntry>) {
        let index = ReleaseIndex {
            app_id: "compact-app".to_string(),
            releases,
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(path, compressed).unwrap();
    }

    #[tokio::test]
    async fn compact_materializes_latest_delta_only_release_before_pruning_history() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_id = "compact-app";
        std::fs::create_dir_all(&store_dir).unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2-hello".to_vec();
        let full_v3 = b"full-v3-hello-world".to_vec();
        let delta_v2 = zstd::encode_all(bsdiff_buffers(&full_v1, &full_v2).unwrap().as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(bsdiff_buffers(&full_v2, &full_v3).unwrap().as_slice(), 3).unwrap();

        let mut v1 = make_release("1.0.0", &rid, &full_v1);
        v1.set_primary_delta(None);

        let mut v2 = make_release("1.1.0", &rid, &full_v2);
        let v2_delta_key = format!("{app_id}-1.1.0-{rid}-delta.tar.zst");
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            &v2_delta_key,
            i64::try_from(delta_v2.len()).unwrap(),
            &sha256_hex(&delta_v2),
        )));

        let mut v3 = make_release("1.2.0", &rid, &full_v3);
        let v3_delta_key = format!("{app_id}-1.2.0-{rid}-delta.tar.zst");
        v3.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            &v3_delta_key,
            i64::try_from(delta_v3.len()).unwrap(),
            &sha256_hex(&delta_v3),
        )));

        std::fs::write(store_dir.join(&v1.full_filename), &full_v1).unwrap();
        std::fs::write(store_dir.join(&v2_delta_key), &delta_v2).unwrap();
        std::fs::write(store_dir.join(&v3_delta_key), &delta_v3).unwrap();
        write_index(
            &store_dir.join(RELEASES_FILE_COMPRESSED),
            vec![v1.clone(), v2, v3.clone()],
        );

        execute(&manifest_path, Some(app_id), Some(&rid), "stable")
            .await
            .unwrap();

        let compacted =
            decompress_release_index(&std::fs::read(store_dir.join(RELEASES_FILE_COMPRESSED)).unwrap()).unwrap();
        assert_eq!(compacted.releases.len(), 1);
        let latest = &compacted.releases[0];
        assert_eq!(latest.version, "1.2.0");
        assert!(latest.selected_delta().is_none());
        assert_eq!(std::fs::read(store_dir.join(&v3.full_filename)).unwrap(), full_v3);
        assert!(!store_dir.join(&v1.full_filename).exists());
        assert!(!store_dir.join(&v2_delta_key).exists());
        assert!(!store_dir.join(&v3_delta_key).exists());
    }

    #[tokio::test]
    async fn compact_prunes_mixed_full_and_delta_history_to_latest_full_only() {
        let tmp = tempfile::tempdir().unwrap();
        let store_dir = tmp.path().join("store");
        let manifest_path = tmp.path().join("surge.yml");
        let rid = current_rid();
        let app_id = "compact-app";
        std::fs::create_dir_all(&store_dir).unwrap();
        write_manifest(&manifest_path, &store_dir, app_id, &rid);

        let full_v1 = b"release-1".to_vec();
        let full_v2 = b"release-2-delta".to_vec();
        let full_v3 = b"release-3-delta".to_vec();
        let full_v4 = b"release-4-full".to_vec();
        let full_v5 = b"release-5-delta".to_vec();
        let full_v6 = b"release-6-full".to_vec();
        let delta_v2 = zstd::encode_all(bsdiff_buffers(&full_v1, &full_v2).unwrap().as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(bsdiff_buffers(&full_v2, &full_v3).unwrap().as_slice(), 3).unwrap();
        let delta_v5 = zstd::encode_all(bsdiff_buffers(&full_v4, &full_v5).unwrap().as_slice(), 3).unwrap();

        let mut v1 = make_release("1.0.0", &rid, &full_v1);
        v1.set_primary_delta(None);

        let mut v2 = make_release("1.1.0", &rid, &full_v2);
        let v2_delta_key = format!("{app_id}-1.1.0-{rid}-delta.tar.zst");
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            &v2_delta_key,
            i64::try_from(delta_v2.len()).unwrap(),
            &sha256_hex(&delta_v2),
        )));

        let mut v3 = make_release("1.2.0", &rid, &full_v3);
        let v3_delta_key = format!("{app_id}-1.2.0-{rid}-delta.tar.zst");
        v3.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            &v3_delta_key,
            i64::try_from(delta_v3.len()).unwrap(),
            &sha256_hex(&delta_v3),
        )));

        let mut v4 = make_release("1.3.0", &rid, &full_v4);
        v4.set_primary_delta(None);

        let mut v5 = make_release("1.4.0", &rid, &full_v5);
        let v5_delta_key = format!("{app_id}-1.4.0-{rid}-delta.tar.zst");
        v5.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.3.0",
            &v5_delta_key,
            i64::try_from(delta_v5.len()).unwrap(),
            &sha256_hex(&delta_v5),
        )));

        let mut v6 = make_release("1.5.0", &rid, &full_v6);
        v6.set_primary_delta(None);

        std::fs::write(store_dir.join(&v1.full_filename), &full_v1).unwrap();
        std::fs::write(store_dir.join(&v2_delta_key), &delta_v2).unwrap();
        std::fs::write(store_dir.join(&v3_delta_key), &delta_v3).unwrap();
        std::fs::write(store_dir.join(&v4.full_filename), &full_v4).unwrap();
        std::fs::write(store_dir.join(&v5_delta_key), &delta_v5).unwrap();
        std::fs::write(store_dir.join(&v6.full_filename), &full_v6).unwrap();
        write_index(
            &store_dir.join(RELEASES_FILE_COMPRESSED),
            vec![v1.clone(), v2, v3, v4.clone(), v5, v6.clone()],
        );

        execute(&manifest_path, Some(app_id), Some(&rid), "stable")
            .await
            .unwrap();

        let compacted =
            decompress_release_index(&std::fs::read(store_dir.join(RELEASES_FILE_COMPRESSED)).unwrap()).unwrap();
        assert_eq!(compacted.releases.len(), 1);
        let latest = &compacted.releases[0];
        assert_eq!(latest.version, "1.5.0");
        assert!(latest.selected_delta().is_none());
        assert_eq!(std::fs::read(store_dir.join(&v6.full_filename)).unwrap(), full_v6);
        assert!(!store_dir.join(&v1.full_filename).exists());
        assert!(!store_dir.join(&v2_delta_key).exists());
        assert!(!store_dir.join(&v3_delta_key).exists());
        assert!(!store_dir.join(&v4.full_filename).exists());
        assert!(!store_dir.join(&v5_delta_key).exists());
    }
}
