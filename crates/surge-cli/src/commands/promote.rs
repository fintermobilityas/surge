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
use surge_core::releases::delta::{
    apply_delta_patch, build_sparse_file_patch, decode_delta_patch, delta_target_archive_encoding,
};
use surge_core::releases::manifest::{
    DeltaArtifact, ReleaseEntry, ReleaseIndex, UNRECORDED_COMPRESSION_LEVEL, UNRECORDED_ZSTD_WORKERS,
    compress_release_index,
};
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

/// Make sure the release at `version` carries a delta whose basis is the
/// previous release on the target channel. Builds and uploads the delta when
/// missing so production nodes can transition from `from_version` to `version`
/// even when the target release's primary delta was built against a different
/// in-between test-only version.
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
    let target = &index.releases[release_idx];
    let (archive_compression_level, archive_zstd_workers) = resolve_target_archive_encoding(backend, target).await?;

    let patch = build_sparse_file_patch(
        &prev_archive,
        &new_archive,
        archive_compression_level,
        archive_zstd_workers,
        &ChunkedDiffOptions::default(),
    )?;

    // Self-verify: apply the freshly built delta to `prev_archive` and require
    // the rebuilt archive's SHA-256 to match `target.full_sha256`. This is the
    // exact invariant every node enforces in
    // `update/manager/apply.rs::materialize_delta_payload`, so we must not
    // upload a delta that would fail it. Without this check, any stale or
    // missing encoding metadata (older release entries, FFI-overridden
    // ResourceBudget, etc.) would silently produce a non-applicable delta and
    // the entire promoted release would be poison on the fleet.
    let rebuilt = apply_delta_patch(
        &prev_archive,
        &patch,
        &DeltaArtifact::sparse_file_ops_zstd("self-verify", from_version, "", 0, ""),
    )?;
    let rebuilt_sha256 = sha256_hex(&rebuilt);
    if rebuilt_sha256 != target.full_sha256 {
        return Err(SurgeError::Pack(format!(
            "Refusing to upload channel delta for {version} ({rid}): rebuilt full archive SHA-256 \
             {rebuilt_sha256} does not match release manifest full_sha256 {} \
             (tried compression_level={archive_compression_level}, zstd_workers={archive_zstd_workers}). \
             Re-pack the release with a recorded full_compression_level/full_zstd_workers so promote \
             can reproduce the original encoding, or rerun `surge pack` to refresh the full artifact.",
            target.full_sha256
        )));
    }

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

/// Resolve the `(compression_level, zstd_workers)` to use when re-packing
/// `target`'s full archive from a freshly-built sparse-file-ops delta.
///
/// Order of preference:
/// 1. `target.full_compression_level` / `target.full_zstd_workers` — these are
///    recorded by `pack/builder/full.rs` and are the only source of truth
///    that survives subsequent manifest changes or FFI budget overrides.
/// 2. The `target.selected_delta()` manifest — older release entries that
///    predate the recorded fields still have a primary delta with the right
///    settings baked into its `SparseFileDeltaManifest`.
/// 3. No fallback: if both sources are unavailable we return an error so the
///    caller can refuse to upload a delta whose rebuild SHA nobody can predict.
async fn resolve_target_archive_encoding(backend: &dyn StorageBackend, release: &ReleaseEntry) -> Result<(i32, u32)> {
    if release.full_compression_level != UNRECORDED_COMPRESSION_LEVEL
        && release.full_zstd_workers != UNRECORDED_ZSTD_WORKERS
    {
        let workers = u32::try_from(release.full_zstd_workers.max(0)).unwrap_or(0);
        return Ok((release.full_compression_level, workers));
    }

    if let Some(delta) = release.selected_delta() {
        match backend.get_object(&delta.filename).await {
            Ok(delta_bytes) => {
                let patch = decode_delta_patch(&delta_bytes, &delta)?;
                if let Some((compression_level, zstd_workers)) = delta_target_archive_encoding(&patch, &delta)? {
                    return Ok((compression_level, zstd_workers));
                }
            }
            Err(SurgeError::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
    }

    Err(SurgeError::Pack(format!(
        "Cannot determine original pack encoding for release {} ({}): no recorded \
         full_compression_level/full_zstd_workers on the release entry and no readable \
         selected_delta to infer them from. Re-run `surge pack` for this release so the new \
         encoding metadata is written into the release index.",
        release.version, release.rid
    )))
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
            full_compression_level: 0,
            full_zstd_workers: 0,
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
                        full_compression_level: 0,
                        full_zstd_workers: 0,
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
                        full_compression_level: 0,
                        full_zstd_workers: 0,
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
        use surge_core::releases::delta::{apply_delta_patch, build_sparse_file_patch, decode_delta_patch};
        const FULL_ARCHIVE_ZSTD_LEVEL: i32 = 7;
        const SYNTH_ARCHIVE_ZSTD_LEVEL: i32 = 9;
        const SYNTH_ARCHIVE_ZSTD_WORKERS: u32 = 4;

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
            let mut packer = ArchivePacker::new(FULL_ARCHIVE_ZSTD_LEVEL).unwrap();
            packer.add_directory(dir, "").unwrap();
            packer.finalize().unwrap()
        };

        let v100_full = pack_dir(&v100_dir);
        let v110_full = pack_dir(&v110_dir);
        let v120_full = pack_dir(&v120_dir);
        let v100_synth = {
            let synth_extract = tempfile::tempdir().unwrap();
            surge_core::archive::extractor::extract_to(&v100_full, synth_extract.path(), None).unwrap();
            let mut synth_packer =
                ArchivePacker::with_threads(SYNTH_ARCHIVE_ZSTD_LEVEL, SYNTH_ARCHIVE_ZSTD_WORKERS).unwrap();
            synth_packer.add_directory(synth_extract.path(), "").unwrap();
            synth_packer.finalize().unwrap()
        };
        assert_ne!(v100_synth, v100_full);

        // Build the v1.1.0 → v1.2.0 sparse delta the same way `surge pack` would,
        // basing it on the immediate previous overall version (v1.1.0). This is
        // exactly what production nodes cannot apply when v1.0.0 is their installed
        // version because the basis-hash check on `camera-tuner.deps.json` fails.
        let raw_v120_patch = build_sparse_file_patch(
            &v110_full,
            &v120_full,
            FULL_ARCHIVE_ZSTD_LEVEL,
            0,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let v120_delta_bytes = zstd::encode_all(raw_v120_patch.as_slice(), FULL_ARCHIVE_ZSTD_LEVEL).unwrap();

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
            full_compression_level: FULL_ARCHIVE_ZSTD_LEVEL,
            full_zstd_workers: 0,
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
        assert_eq!(
            production_delta.patch_format,
            surge_core::releases::manifest::PATCH_FORMAT_SPARSE_FILE_OPS_V1
        );

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
        // "Sparse delta file hash mismatch for camera-tuner.deps.json". It also
        // must reproduce the exact target archive bytes when the release was
        // packed with non-default compression settings, even if the updater had
        // to synthesize a tree-equivalent base archive with different zstd
        // settings from the installed app contents.
        let decoded = decode_delta_patch(&production_delta_bytes, &production_delta).unwrap();
        let rebuilt = apply_delta_patch(&v100_synth, &decoded, &production_delta).unwrap();
        assert_eq!(rebuilt, v120_full);
        let working_dir = tempfile::tempdir().unwrap();
        surge_core::archive::extractor::extract_to(&rebuilt, working_dir.path(), None).unwrap();
        assert_eq!(
            std::fs::read(working_dir.path().join("camera-tuner.deps.json")).unwrap(),
            br#"{"version":"1.2.0"}"#,
        );
    }

    // Reproduces the production crashloop where nodes reject the promoted
    // 2996.0.0 release with "SHA-256 mismatch for rebuilt full archive".
    //
    // Root cause: 2996.0.0 was a **checkpoint full** — the primary delta chain
    // hit `pack_policy.max_chain_length` (default 8) so pack skipped the
    // primary delta and only uploaded a full archive. When `ensure_channel_delta`
    // then runs during promote, `resolve_target_archive_encoding` finds no
    // selected_delta on the target release and falls back to
    // `ResourceBudget::default()` — but that default has
    // `zstd_compression_level = 9`, while the full archive was actually packed
    // with `PackPolicy::default().compression_level = 3`.
    //
    // The fallback therefore builds the channel-aware delta at level 9, a node
    // on the previous production release applies it, and the rebuilt archive
    // bytes no longer match the level-3 `full_sha256` recorded at pack time.
    #[tokio::test]
    async fn execute_channel_delta_preserves_full_sha256_when_target_is_checkpoint_full() {
        use surge_core::archive::packer::ArchivePacker;
        use surge_core::releases::delta::{apply_delta_patch, decode_delta_patch};
        // PackPolicy::default().compression_level
        const PACK_ZSTD_LEVEL: i32 = 3;
        // Whatever pack happened to use on the build runner (capped by CPU count).
        const PACK_ZSTD_WORKERS: u32 = 4;

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);

        let stage = temp_dir.path();
        let v100_dir = stage.join("v100");
        let v120_dir = stage.join("v120");
        for dir in [&v100_dir, &v120_dir] {
            std::fs::create_dir_all(dir).unwrap();
        }
        // Realistic file mix: a small config that changes (PatchFile op) and a
        // larger payload that also changes (WriteFile/PatchFile). The specific
        // content doesn't matter; the point is that the resulting repack is
        // sensitive to the zstd compression level.
        std::fs::write(v100_dir.join("app.config"), br#"{"version":"1.0.0"}"#).unwrap();
        std::fs::write(v120_dir.join("app.config"), br#"{"version":"1.2.0"}"#).unwrap();
        std::fs::write(v100_dir.join("payload.bin"), vec![b'A'; 256 * 1024]).unwrap();
        std::fs::write(v120_dir.join("payload.bin"), {
            let mut bytes = vec![b'A'; 256 * 1024];
            bytes[128] = b'B';
            bytes
        })
        .unwrap();

        let pack_dir = |dir: &Path| -> Vec<u8> {
            let mut packer = ArchivePacker::with_threads(PACK_ZSTD_LEVEL, PACK_ZSTD_WORKERS).unwrap();
            packer.add_directory(dir, "").unwrap();
            packer.finalize().unwrap()
        };

        let v100_full = pack_dir(&v100_dir);
        let v120_full = pack_dir(&v120_dir);

        let v100_full_key = format!("demo-1.0.0-{rid}-full.tar.zst");
        let v120_full_key = format!("demo-1.2.0-{rid}-full.tar.zst");
        std::fs::write(store_dir.join(&v100_full_key), &v100_full).unwrap();
        std::fs::write(store_dir.join(&v120_full_key), &v120_full).unwrap();

        let make_release = |version: &str,
                            channels: &[&str],
                            full_key: &str,
                            full_bytes: &[u8],
                            full_compression_level: i32,
                            full_zstd_workers: i32|
         -> ReleaseEntry {
            ReleaseEntry {
                version: version.to_string(),
                channels: channels.iter().map(|c| (*c).to_string()).collect(),
                os: "linux".to_string(),
                rid: rid.clone(),
                is_genesis: version == "1.0.0",
                full_filename: full_key.to_string(),
                full_size: i64::try_from(full_bytes.len()).unwrap(),
                full_sha256: sha256_hex(full_bytes),
                full_compression_level,
                full_zstd_workers,
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
        };

        // v1.2.0 is a checkpoint full — no primary delta. This is what
        // `should_publish_checkpoint_full` in `pack/builder/delta.rs` produces
        // when `deltas_since_checkpoint >= max_chain_length`. The new pack
        // records the real (level, workers) on the release entry, so promote
        // can rebuild a channel delta whose apply produces matching bytes.
        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                releases: vec![
                    make_release(
                        "1.0.0",
                        &["production", "test"],
                        &v100_full_key,
                        &v100_full,
                        PACK_ZSTD_LEVEL,
                        i32::try_from(PACK_ZSTD_WORKERS).unwrap(),
                    ),
                    make_release(
                        "1.2.0",
                        &["test"],
                        &v120_full_key,
                        &v120_full,
                        PACK_ZSTD_LEVEL,
                        i32::try_from(PACK_ZSTD_WORKERS).unwrap(),
                    ),
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
        let production_delta = promoted
            .delta_from_source("1.0.0")
            .expect("production-channel delta from 1.0.0 must be produced by promote");

        let production_delta_path = store_dir.join(&production_delta.filename);
        let production_delta_bytes = std::fs::read(&production_delta_path).expect("production delta artifact uploaded");

        let decoded = decode_delta_patch(&production_delta_bytes, &production_delta).unwrap();
        let rebuilt = apply_delta_patch(&v100_full, &decoded, &production_delta).unwrap();
        let rebuilt_sha256 = sha256_hex(&rebuilt);
        assert_eq!(
            rebuilt_sha256, promoted.full_sha256,
            "rebuilt full archive SHA must match manifest `full_sha256`"
        );
    }

    // Guard rail for the reviewer's concern: if a release pre-dates the
    // `full_compression_level`/`full_zstd_workers` fields AND its primary
    // delta was pruned (e.g. by a previous `surge compact`), promote has no
    // way to reproduce the exact pack encoding. Rather than silently uploading
    // a delta that every node will reject, promote must refuse and surface a
    // clear error so the operator can re-pack the target release.
    #[tokio::test]
    async fn execute_channel_delta_refuses_when_encoding_is_unknowable() {
        use surge_core::archive::packer::ArchivePacker;
        use surge_core::releases::manifest::{UNRECORDED_COMPRESSION_LEVEL, UNRECORDED_ZSTD_WORKERS};

        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store_dir = temp_dir.path().join("store");
        let manifest_path = temp_dir.path().join("surge.yml");
        let rid = current_rid();

        std::fs::create_dir_all(&store_dir).expect("store dir");
        write_manifest(&manifest_path, &store_dir, "demo", &rid);

        let stage = temp_dir.path();
        let v100_dir = stage.join("v100");
        let v120_dir = stage.join("v120");
        for dir in [&v100_dir, &v120_dir] {
            std::fs::create_dir_all(dir).unwrap();
        }
        std::fs::write(v100_dir.join("app.config"), br#"{"v":"1.0"}"#).unwrap();
        std::fs::write(v120_dir.join("app.config"), br#"{"v":"1.2"}"#).unwrap();

        let pack_dir = |dir: &Path| -> Vec<u8> {
            let mut packer = ArchivePacker::with_threads(3, 4).unwrap();
            packer.add_directory(dir, "").unwrap();
            packer.finalize().unwrap()
        };
        let v100_full = pack_dir(&v100_dir);
        let v120_full = pack_dir(&v120_dir);

        let v100_full_key = format!("demo-1.0.0-{rid}-full.tar.zst");
        let v120_full_key = format!("demo-1.2.0-{rid}-full.tar.zst");
        std::fs::write(store_dir.join(&v100_full_key), &v100_full).unwrap();
        std::fs::write(store_dir.join(&v120_full_key), &v120_full).unwrap();

        let make_legacy_release = |version: &str, channels: &[&str], full_key: &str, full_bytes: &[u8]| ReleaseEntry {
            version: version.to_string(),
            channels: channels.iter().map(|c| (*c).to_string()).collect(),
            os: "linux".to_string(),
            rid: rid.clone(),
            is_genesis: version == "1.0.0",
            full_filename: full_key.to_string(),
            full_size: i64::try_from(full_bytes.len()).unwrap(),
            full_sha256: sha256_hex(full_bytes),
            // Simulate a legacy release entry packed before the fields existed.
            full_compression_level: UNRECORDED_COMPRESSION_LEVEL,
            full_zstd_workers: UNRECORDED_ZSTD_WORKERS,
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

        write_index(
            &store_dir,
            &ReleaseIndex {
                app_id: "demo".to_string(),
                releases: vec![
                    make_legacy_release("1.0.0", &["production", "test"], &v100_full_key, &v100_full),
                    make_legacy_release("1.2.0", &["test"], &v120_full_key, &v120_full),
                ],
                ..ReleaseIndex::default()
            },
        );

        let err = execute(&manifest_path, Some("demo"), "1.2.0", Some(&rid), "production")
            .await
            .expect_err("promote should refuse when encoding is unknowable");
        let msg = err.to_string();
        assert!(
            msg.contains("Cannot determine original pack encoding"),
            "unexpected error: {msg}"
        );
    }

    // End-to-end reproduction of the production crash where nodes reject the
    // promoted 2996.0.0 release with:
    //   "SHA-256 mismatch for rebuilt full archive 2996.0.0: expected ..., got ..."
    //
    // Mirrors the real production flow:
    //   * full archives and primary deltas are packed with MULTITHREADED zstd
    //     (workers = 4), matching `ResourceBudget::default()` in CI.
    //   * v1.0.0 is on `production`; v1.1.0 and v1.2.0 live only on `test`.
    //   * `surge promote 1.2.0 production` must build a sparse-file-ops delta
    //     directly from v1.0.0's full to v1.2.0's full (skipping v1.1.0).
    //   * a node sitting at v1.0.0 fetches that new production-channel delta
    //     and expects the rebuilt archive to hash to v1.2.0's full_sha256.
    #[tokio::test]
    async fn execute_rebuilds_channel_delta_with_multithreaded_pack_preserves_full_sha256() {
        use surge_core::archive::packer::ArchivePacker;
        use surge_core::releases::delta::{apply_delta_patch, build_sparse_file_patch, decode_delta_patch};
        const ZSTD_LEVEL: i32 = 9;
        const ZSTD_WORKERS: u32 = 4;

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
            std::fs::create_dir_all(dir.join("bin")).unwrap();
            std::fs::create_dir_all(dir.join("assets")).unwrap();
        }
        // Non-trivial changing files — mix of small text and a larger binary
        // so the delta exercises both WriteFile and PatchFile ops.
        std::fs::write(v100_dir.join("bin/app.config"), br#"{"version":"1.0.0"}"#).unwrap();
        std::fs::write(v110_dir.join("bin/app.config"), br#"{"version":"1.1.0"}"#).unwrap();
        std::fs::write(v120_dir.join("bin/app.config"), br#"{"version":"1.2.0"}"#).unwrap();
        std::fs::write(v100_dir.join("bin/runtime.bin"), vec![b'A'; 1024 * 1024]).unwrap();
        std::fs::write(v110_dir.join("bin/runtime.bin"), {
            let mut bytes = vec![b'A'; 1024 * 1024];
            bytes[512] = b'B';
            bytes
        })
        .unwrap();
        std::fs::write(v120_dir.join("bin/runtime.bin"), {
            let mut bytes = vec![b'A'; 1024 * 1024];
            bytes[512] = b'B';
            bytes[1024] = b'C';
            bytes
        })
        .unwrap();
        // Unchanged file — should be omitted from the sparse-file-ops delta.
        for dir in [&v100_dir, &v110_dir, &v120_dir] {
            std::fs::write(dir.join("assets/shared.bin"), vec![b'Z'; 256 * 1024]).unwrap();
        }

        let pack_dir = |dir: &Path| -> Vec<u8> {
            let mut packer = ArchivePacker::with_threads(ZSTD_LEVEL, ZSTD_WORKERS).unwrap();
            packer.add_directory(dir, "").unwrap();
            packer.finalize().unwrap()
        };

        let v100_full = pack_dir(&v100_dir);
        let v110_full = pack_dir(&v110_dir);
        let v120_full = pack_dir(&v120_dir);

        // Primary delta is built the same way `surge pack` builds it in CI: with
        // the production ResourceBudget (level 9, workers 4).
        let raw_v110_patch = build_sparse_file_patch(
            &v100_full,
            &v110_full,
            ZSTD_LEVEL,
            ZSTD_WORKERS,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let v110_delta_bytes = zstd::encode_all(raw_v110_patch.as_slice(), DEFAULT_ZSTD_LEVEL).unwrap();
        let raw_v120_patch = build_sparse_file_patch(
            &v110_full,
            &v120_full,
            ZSTD_LEVEL,
            ZSTD_WORKERS,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let v120_delta_bytes = zstd::encode_all(raw_v120_patch.as_slice(), DEFAULT_ZSTD_LEVEL).unwrap();

        let v100_full_key = format!("demo-1.0.0-{rid}-full.tar.zst");
        let v110_full_key = format!("demo-1.1.0-{rid}-full.tar.zst");
        let v120_full_key = format!("demo-1.2.0-{rid}-full.tar.zst");
        let v110_delta_key = format!("demo-1.1.0-{rid}-delta.tar.zst");
        let v120_delta_key = format!("demo-1.2.0-{rid}-delta.tar.zst");

        std::fs::write(store_dir.join(&v100_full_key), &v100_full).unwrap();
        std::fs::write(store_dir.join(&v110_full_key), &v110_full).unwrap();
        std::fs::write(store_dir.join(&v120_full_key), &v120_full).unwrap();
        std::fs::write(store_dir.join(&v110_delta_key), &v110_delta_bytes).unwrap();
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
            full_compression_level: ZSTD_LEVEL,
            full_zstd_workers: i32::try_from(ZSTD_WORKERS).unwrap(),
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

        let mut v110 = make_release("1.1.0", &["test"], &v110_full_key, &v110_full);
        v110.deltas = vec![DeltaArtifact::sparse_file_ops_zstd(
            "primary",
            "1.0.0",
            &v110_delta_key,
            i64::try_from(v110_delta_bytes.len()).unwrap(),
            &sha256_hex(&v110_delta_bytes),
        )];
        v110.preferred_delta_id = "primary".to_string();
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
                    v110,
                    v120,
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
        let production_delta = promoted
            .delta_from_source("1.0.0")
            .expect("production-channel delta from 1.0.0 must be produced by promote");

        let production_delta_path = store_dir.join(&production_delta.filename);
        let production_delta_bytes = std::fs::read(&production_delta_path).expect("production delta artifact uploaded");

        // Simulate a node sitting on v1.0.0 applying the new production-channel
        // delta. This is the exact invariant the node checks at
        // update/manager/apply.rs after `apply_delta_patch`.
        let decoded = decode_delta_patch(&production_delta_bytes, &production_delta).unwrap();
        let rebuilt = apply_delta_patch(&v100_full, &decoded, &production_delta).unwrap();
        let rebuilt_sha256 = sha256_hex(&rebuilt);
        assert_eq!(
            rebuilt_sha256, promoted.full_sha256,
            "rebuilt full archive SHA must match manifest `full_sha256` (prod regression: nodes reject the update otherwise)"
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
