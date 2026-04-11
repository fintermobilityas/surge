use std::path::Path;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::compress_release_index;
use surge_core::releases::version::canonicalize_version;
use surge_core::storage;

/// Promote a release version to a target channel.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    const TOTAL_STAGES: usize = 5;
    let version = canonicalize_version(version, "release version")?;

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

    if index.releases[release_idx]
        .channels
        .iter()
        .any(|existing| existing == channel)
    {
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
    let full_materialized = super::ensure_release_full_artifact(&*backend, &index, &rid, &version).await?;
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
    let release = &mut index.releases[release_idx];
    release.channels.push(channel.to_string());
    release.channels.sort();
    release.channels.dedup();

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;
    print_stage_done(theme, 4, TOTAL_STAGES, &format!("Added channel '{channel}' to release"));

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
