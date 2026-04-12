use std::path::Path;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::crypto::sha256::sha256_hex;
use surge_core::diff::chunked::ChunkedDiffOptions;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::delta::build_sparse_file_patch;
use surge_core::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, compress_release_index};
use surge_core::releases::restore::restore_full_archive_for_version;
use surge_core::releases::version::compare_versions;
use surge_core::storage::{self, StorageBackend};

/// Promote a release version to a target channel.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and target release");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let storage_config = super::build_app_scoped_storage_config(&manifest, manifest_path, &app_id)?;
    super::ensure_mutating_storage_access(&storage_config, "promote release")?;
    let backend = storage::create_storage_backend(&storage_config)?;
    print_stage_done(theme, 1, TOTAL_STAGES, &format!("Target: {app_id}/{rid} v{version}"));

    print_stage(theme, 2, TOTAL_STAGES, "Loading release index");
    let mut index = super::fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    print_stage_done(theme, 2, TOTAL_STAGES, "Release index loaded");

    let release_idx = index
        .releases
        .iter()
        .position(|release| release.version == version && release.rid == rid)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} not found for {app_id}/{rid}")))?;

    let already_on_channel = index.releases[release_idx]
        .channels
        .iter()
        .any(|existing| existing == channel);
    let previous_on_channel = previous_release_on_channel(&index, &rid, channel, version);
    let needs_channel_delta = previous_on_channel
        .as_deref()
        .is_some_and(|prev| index.releases[release_idx].delta_from_source(prev).is_none());

    if already_on_channel && !needs_channel_delta {
        print_stage(theme, 3, TOTAL_STAGES, "Updating channel membership");
        print_stage_done(
            theme,
            3,
            TOTAL_STAGES,
            &format!("Release already on channel '{channel}'; no changes applied"),
        );
        print_stage(theme, 4, TOTAL_STAGES, "Finalize promote summary");
        print_stage_done(
            theme,
            4,
            TOTAL_STAGES,
            &format!("Completed in {} (no-op)", format_duration(started.elapsed())),
        );
        return Ok(());
    }

    print_stage(theme, 3, TOTAL_STAGES, "Ensuring release full artifact exists");
    let full_materialized = super::ensure_release_full_artifact(&*backend, &index, &rid, version).await?;
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        if full_materialized {
            "Rebuilt and uploaded missing full artifact"
        } else {
            "Release full artifact already present"
        },
    );

    print_stage(theme, 4, TOTAL_STAGES, "Updating channel membership");
    if !already_on_channel {
        let release = &mut index.releases[release_idx];
        release.channels.push(channel.to_string());
        release.channels.sort();
        release.channels.dedup();
    }
    // Even when the release is already on the channel, re-check that a
    // production-compatible delta exists. This is what recovers a release that
    // was previously promoted before this fix shipped: rerun `surge promote`
    // and the missing channel delta is built in place without a demote/repromote
    // cycle.
    let channel_delta_summary = match previous_on_channel {
        Some(prev_version) => {
            ensure_channel_delta(&*backend, &mut index, &app_id, &rid, version, &prev_version).await?
        }
        None => "no previous release on channel; skipped delta rebuild".to_string(),
    };

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!("Added channel '{channel}' to release ({channel_delta_summary})"),
    );

    print_stage(theme, 5, TOTAL_STAGES, "Finalize promote summary");
    print_stage_done(
        theme,
        5,
        TOTAL_STAGES,
        &format!(
            "Promoted {app_id} v{version} ({rid}) -> {channel} in {}",
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

/// Find the latest release strictly before `version` for the given `rid` that
/// will be on `channel` after the in-flight promotion. Returns `None` if no
/// such predecessor exists, in which case the new release is the channel's
/// genesis and no cross-version delta needs rebuilding.
fn previous_release_on_channel(index: &ReleaseIndex, rid: &str, channel: &str, version: &str) -> Option<String> {
    let mut candidates: Vec<&ReleaseEntry> = index
        .releases
        .iter()
        .filter(|release| {
            release.rid == rid
                && release.version != version
                && release.channels.iter().any(|existing| existing == channel)
        })
        .collect();
    candidates.sort_by(|left, right| compare_versions(&left.version, &right.version));
    candidates
        .into_iter()
        .rev()
        .find(|release| compare_versions(&release.version, version) == std::cmp::Ordering::Less)
        .map(|release| release.version.clone())
}

/// Make sure the release at `version` carries a sparse delta whose basis is the
/// previous release on the target channel. Builds and uploads the delta when
/// missing so production nodes can transition from `from_version` to `version`
/// without hitting a basis hash mismatch on files that changed across an
/// in-between test-only release.
///
/// Returns a one-line summary suitable for inclusion in the promote stage log.
async fn ensure_channel_delta(
    backend: &dyn StorageBackend,
    index: &mut ReleaseIndex,
    app_id: &str,
    rid: &str,
    version: &str,
    from_version: &str,
) -> Result<String> {
    let release_idx = index
        .releases
        .iter()
        .position(|release| release.version == version && release.rid == rid)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} not found for {app_id}/{rid}")))?;

    if index.releases[release_idx].delta_from_source(from_version).is_some() {
        return Ok(format!("delta from v{from_version} already present"));
    }

    let prev_archive = restore_full_archive_for_version(backend, index, rid, from_version).await?;
    let new_archive = restore_full_archive_for_version(backend, index, rid, version).await?;

    let diff_options = ChunkedDiffOptions::default();
    let patch = build_sparse_file_patch(&prev_archive, &new_archive, DEFAULT_ZSTD_LEVEL, 0, &diff_options)?;
    let compressed = zstd::encode_all(patch.as_slice(), DEFAULT_ZSTD_LEVEL)
        .map_err(|err| SurgeError::Archive(format!("Failed to compress channel delta: {err}")))?;

    let from_version_slug = sanitize_version_for_filename(from_version);
    let delta_filename = format!("{app_id}-{version}-{rid}-from-{from_version_slug}-delta.tar.zst");
    backend
        .put_object(&delta_filename, &compressed, "application/octet-stream")
        .await?;

    let delta_size = i64::try_from(compressed.len())
        .map_err(|_| SurgeError::Archive(format!("Channel delta is too large: {} bytes", compressed.len())))?;
    let delta_sha256 = sha256_hex(&compressed);
    let delta_id = format!("from-{from_version_slug}");

    let delta =
        DeltaArtifact::sparse_file_ops_zstd(&delta_id, from_version, &delta_filename, delta_size, &delta_sha256);
    index.releases[release_idx].upsert_delta(delta);

    Ok(format!(
        "rebuilt delta from v{from_version} ({delta_size} bytes) and stored as {delta_filename}"
    ))
}

fn sanitize_version_for_filename(version: &str) -> String {
    version
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use surge_core::config::constants::DEFAULT_ZSTD_LEVEL;
    use surge_core::crypto::sha256::sha256_hex;
    use surge_core::diff::wrapper::bsdiff_buffers;
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, decompress_release_index};

    fn write_manifest(path: &Path, store_dir: &Path, app_id: &str, rid: &str) {
        let yaml = format!(
            "schema: 1\nstorage:\n  provider: filesystem\n  bucket: {}\napps:\n  - id: {app_id}\n    target:\n      rid: {rid}\n",
            store_dir.display()
        );
        std::fs::write(path, yaml).expect("manifest should be written");
    }

    fn write_index(path: &Path, index: &ReleaseIndex) {
        let compressed = compress_release_index(index, DEFAULT_ZSTD_LEVEL).expect("compressed index");
        std::fs::write(path.join(RELEASES_FILE_COMPRESSED), compressed).expect("release index should be written");
    }

    fn read_index(path: &Path) -> ReleaseIndex {
        let data = std::fs::read(path.join(RELEASES_FILE_COMPRESSED)).expect("release index should exist");
        decompress_release_index(&data).expect("release index should parse")
    }

    fn release(version: &str, rid: &str, channels: &[&str]) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: channels.iter().map(|channel| (*channel).to_string()).collect(),
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: true,
            full_filename: format!("demo-{version}-{rid}-full.tar.zst"),
            full_size: 1,
            full_sha256: "hash".to_string(),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            name: String::new(),
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

    #[tokio::test]
    async fn execute_adds_requested_channel_and_sorts_membership() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);
        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                releases: vec![release("1.2.3", &rid, &["stable"])],
                ..ReleaseIndex::default()
            },
        );
        std::fs::write(store_dir.join(format!("demo-1.2.3-{rid}-full.tar.zst")), b"payload")
            .expect("full artifact should be present");

        execute(&manifest_path, Some("demo"), "1.2.3", Some(&rid), "beta")
            .await
            .expect("promote should succeed");

        let index = read_index(&store_dir);
        assert_eq!(
            index.releases[0].channels,
            vec!["beta".to_string(), "stable".to_string()]
        );
        assert!(!index.last_write_utc.is_empty());
    }

    #[tokio::test]
    async fn execute_is_noop_when_release_already_on_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);
        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                last_write_utc: "unchanged".to_string(),
                releases: vec![release("1.2.3", &rid, &["beta", "stable"])],
                ..ReleaseIndex::default()
            },
        );

        execute(&manifest_path, Some("demo"), "1.2.3", Some(&rid), "beta")
            .await
            .expect("promote should no-op successfully");

        let index = read_index(&store_dir);
        assert_eq!(
            index.releases[0].channels,
            vec!["beta".to_string(), "stable".to_string()]
        );
        assert_eq!(index.last_write_utc, "unchanged");
    }

    #[tokio::test]
    async fn execute_materializes_missing_full_before_adding_channel() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);

        let v1 = b"demo-v1-full".to_vec();
        let v2 = b"demo-v2-full".to_vec();
        let delta = zstd::encode_all(bsdiff_buffers(&v1, &v2).unwrap().as_slice(), 3).unwrap();

        let v1_full_key = format!("demo-1.0.0-{rid}-full.tar.zst");
        let v2_full_key = format!("demo-1.1.0-{rid}-full.tar.zst");
        let v2_delta_key = format!("demo-1.1.0-{rid}-delta.tar.zst");

        std::fs::write(store_dir.join(&v1_full_key), &v1).expect("base full should be present");
        std::fs::write(store_dir.join(&v2_delta_key), &delta).expect("delta artifact should be present");

        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                releases: vec![
                    ReleaseEntry {
                        version: "1.0.0".to_string(),
                        channels: vec!["stable".to_string()],
                        os: "linux".to_string(),
                        rid: rid.clone(),
                        is_genesis: true,
                        full_filename: v1_full_key.clone(),
                        full_size: i64::try_from(v1.len()).unwrap(),
                        full_sha256: sha256_hex(&v1),
                        deltas: Vec::new(),
                        preferred_delta_id: String::new(),
                        created_utc: chrono::Utc::now().to_rfc3339(),
                        release_notes: String::new(),
                        name: String::new(),
                        main_exe: "demoapp".to_string(),
                        install_directory: "demoapp".to_string(),
                        supervisor_id: String::new(),
                        icon: String::new(),
                        shortcuts: Vec::new(),
                        persistent_assets: Vec::new(),
                        installers: Vec::new(),
                        environment: std::collections::BTreeMap::new(),
                    },
                    ReleaseEntry {
                        version: "1.1.0".to_string(),
                        channels: vec!["stable".to_string()],
                        os: "linux".to_string(),
                        rid: rid.clone(),
                        is_genesis: false,
                        full_filename: v2_full_key.clone(),
                        full_size: i64::try_from(v2.len()).unwrap(),
                        full_sha256: sha256_hex(&v2),
                        deltas: vec![DeltaArtifact::bsdiff_zstd(
                            "primary",
                            "1.0.0",
                            &v2_delta_key,
                            i64::try_from(delta.len()).unwrap(),
                            &sha256_hex(&delta),
                        )],
                        preferred_delta_id: "primary".to_string(),
                        created_utc: chrono::Utc::now().to_rfc3339(),
                        release_notes: String::new(),
                        name: String::new(),
                        main_exe: "demoapp".to_string(),
                        install_directory: "demoapp".to_string(),
                        supervisor_id: String::new(),
                        icon: String::new(),
                        shortcuts: Vec::new(),
                        persistent_assets: Vec::new(),
                        installers: Vec::new(),
                        environment: std::collections::BTreeMap::new(),
                    },
                ],
                ..ReleaseIndex::default()
            },
        );

        execute(&manifest_path, Some("demo"), "1.1.0", Some(&rid), "beta")
            .await
            .expect("promote should materialize and succeed");

        let index = read_index(&store_dir);
        let promoted = index
            .releases
            .iter()
            .find(|release| release.version == "1.1.0" && release.rid == rid)
            .expect("promoted release should exist");
        assert_eq!(promoted.channels, vec!["beta".to_string(), "stable".to_string()]);
        assert_eq!(std::fs::read(store_dir.join(v2_full_key)).unwrap(), v2);
    }

    #[tokio::test]
    async fn execute_rebuilds_channel_delta_when_promoting_across_skipped_versions() {
        use surge_core::archive::packer::ArchivePacker;
        use surge_core::releases::delta::{apply_delta_patch, decode_delta_patch};

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);

        let stage = temp_dir.path();
        let v100_dir = stage.join("v100");
        let v110_dir = stage.join("v110");
        let v120_dir = stage.join("v120");
        for dir in [&v100_dir, &v110_dir, &v120_dir] {
            std::fs::create_dir_all(dir).unwrap();
        }
        // A non-trivial file that changes its content (and therefore its hash)
        // at every release — this is the analog of `camera-tuner.deps.json`
        // that triggered the production-promotion mismatches in the field.
        std::fs::write(v100_dir.join("camera-tuner.deps.json"), br#"{"version":"1.0.0"}"#).unwrap();
        std::fs::write(v110_dir.join("camera-tuner.deps.json"), br#"{"version":"1.1.0"}"#).unwrap();
        std::fs::write(v120_dir.join("camera-tuner.deps.json"), br#"{"version":"1.2.0"}"#).unwrap();
        // A second file that is identical across releases, exercising the
        // sparse-file delta path that copies unchanged files by hash reference.
        for dir in [&v100_dir, &v110_dir, &v120_dir] {
            std::fs::write(dir.join("readme.txt"), b"shared content\n").unwrap();
        }

        let pack_dir = |dir: &Path| -> Vec<u8> {
            let mut packer = ArchivePacker::new(3).unwrap();
            packer.add_directory(dir, "").unwrap();
            packer.finalize().unwrap()
        };

        let v100_full = pack_dir(&v100_dir);
        let v110_full = pack_dir(&v110_dir);
        let v120_full = pack_dir(&v120_dir);

        // Build the v1.1.0 → v1.2.0 sparse delta the same way `surge pack` would,
        // basing it on the immediate previous overall version (v1.1.0). This is
        // exactly what production nodes cannot apply when v1.0.0 is their installed
        // version because the basis-hash check on `camera-tuner.deps.json` fails.
        let raw_v120_patch =
            build_sparse_file_patch(&v110_full, &v120_full, 3, 0, &ChunkedDiffOptions::default()).unwrap();
        let v120_delta_bytes = zstd::encode_all(raw_v120_patch.as_slice(), 3).unwrap();

        let v100_full_key = format!("demo-1.0.0-{rid}-full.tar.zst");
        let v110_full_key = format!("demo-1.1.0-{rid}-full.tar.zst");
        let v120_full_key = format!("demo-1.2.0-{rid}-full.tar.zst");
        let v120_delta_key = format!("demo-1.2.0-{rid}-delta.tar.zst");

        std::fs::write(store_dir.join(&v100_full_key), &v100_full).unwrap();
        std::fs::write(store_dir.join(&v110_full_key), &v110_full).unwrap();
        std::fs::write(store_dir.join(&v120_full_key), &v120_full).unwrap();
        std::fs::write(store_dir.join(&v120_delta_key), &v120_delta_bytes).unwrap();

        let make_release = |version: &str, channels: &[&str], full_key: &str, full_bytes: &[u8]| ReleaseEntry {
            version: version.to_string(),
            channels: channels.iter().map(|c| (*c).to_string()).collect(),
            os: "linux".to_string(),
            rid: rid.clone(),
            is_genesis: version == "1.0.0",
            full_filename: full_key.to_string(),
            full_size: i64::try_from(full_bytes.len()).unwrap(),
            full_sha256: sha256_hex(full_bytes),
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demoapp".to_string(),
            install_directory: "demoapp".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        };

        let mut v120 = make_release("1.2.0", &["test"], &v120_full_key, &v120_full);
        v120.deltas = vec![DeltaArtifact::sparse_file_ops_zstd(
            "primary",
            "1.1.0",
            &v120_delta_key,
            i64::try_from(v120_delta_bytes.len()).unwrap(),
            &sha256_hex(&v120_delta_bytes),
        )];
        v120.preferred_delta_id = "primary".to_string();

        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                releases: vec![
                    make_release("1.0.0", &["production", "test"], &v100_full_key, &v100_full),
                    make_release("1.1.0", &["test"], &v110_full_key, &v110_full),
                    v120.clone(),
                ],
                ..ReleaseIndex::default()
            },
        );

        execute(&manifest_path, Some("demo"), "1.2.0", Some(&rid), "production")
            .await
            .expect("promote should succeed");

        let index = read_index(&store_dir);
        let promoted = index
            .releases
            .iter()
            .find(|release| release.version == "1.2.0" && release.rid == rid)
            .expect("promoted release should exist");
        assert!(promoted.channels.iter().any(|channel| channel == "production"));

        let production_delta = promoted
            .delta_from_source("1.0.0")
            .expect("delta from previous-on-channel release should exist after promote");
        assert_eq!(production_delta.from_version, "1.0.0");

        // Original test-channel delta from v1.1.0 must still be present so test
        // nodes can keep updating without regression.
        let test_delta = promoted
            .delta_from_source("1.1.0")
            .expect("original test-channel delta should be preserved");
        assert_eq!(test_delta.id, "primary");

        let production_delta_path = store_dir.join(&production_delta.filename);
        let production_delta_bytes = std::fs::read(&production_delta_path)
            .expect("rebuilt production delta artifact should be uploaded to storage");
        assert_eq!(sha256_hex(&production_delta_bytes), production_delta.sha256);

        // Verify the new delta actually transforms a v1.0.0 archive into v1.2.0
        // when applied — this is the apply path that previously crashed with
        // "Sparse delta file hash mismatch for camera-tuner.deps.json".
        let decoded = decode_delta_patch(&production_delta_bytes, &production_delta).unwrap();
        let rebuilt = apply_delta_patch(&v100_full, &decoded, &production_delta).unwrap();
        let working_dir = tempfile::tempdir().unwrap();
        surge_core::archive::extractor::extract_to(&rebuilt, working_dir.path(), None).unwrap();
        assert_eq!(
            std::fs::read(working_dir.path().join("camera-tuner.deps.json")).unwrap(),
            br#"{"version":"1.2.0"}"#,
        );
    }

    #[tokio::test]
    async fn execute_rejects_promotion_when_missing_full_cannot_be_restored() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);
        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                last_write_utc: "unchanged".to_string(),
                releases: vec![release("1.2.3", &rid, &["stable"])],
                ..ReleaseIndex::default()
            },
        );

        let err = execute(&manifest_path, Some("demo"), "1.2.3", Some(&rid), "beta")
            .await
            .expect_err("promote should fail when full cannot be restored");
        let err = err.to_string();
        assert!(
            err.contains("No reconstructable full archive found"),
            "unexpected error: {err}"
        );

        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["stable".to_string()]);
        assert_eq!(index.last_write_utc, "unchanged");
        assert!(!store_dir.join(format!("demo-1.2.3-{rid}-full.tar.zst")).exists());
    }
}
