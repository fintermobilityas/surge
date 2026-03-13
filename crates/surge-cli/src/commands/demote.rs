use std::path::Path;
use std::time::Instant;

use crate::formatters::format_duration;
use crate::logline;
use crate::ui::UiTheme;
use surge_core::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{compress_release_index, decompress_release_index};
use surge_core::storage::{self, StorageBackend};

/// Demote (remove) a release version from a channel.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    version: &str,
    rid: Option<&str>,
    channel: &str,
) -> Result<()> {
    const TOTAL_STAGES: usize = 4;

    let theme = UiTheme::global();
    let started = Instant::now();

    print_stage(theme, 1, TOTAL_STAGES, "Resolving manifest and target release");
    let manifest = SurgeManifest::from_file(manifest_path)?;
    let app_id = super::resolve_app_id_with_rid_hint(&manifest, app_id, rid)?;
    let rid = super::resolve_rid(&manifest, &app_id, rid)?;
    let storage_config = super::build_app_scoped_storage_config(&manifest, manifest_path, &app_id)?;
    let backend = storage::create_storage_backend(&storage_config)?;
    print_stage_done(theme, 1, TOTAL_STAGES, &format!("Target: {app_id}/{rid} v{version}"));

    print_stage(theme, 2, TOTAL_STAGES, "Loading release index");
    let mut index = fetch_release_index(&*backend).await?;
    if !index.app_id.is_empty() && index.app_id != app_id {
        return Err(SurgeError::NotFound(format!(
            "Release index belongs to app '{}' not '{}'",
            index.app_id, app_id
        )));
    }
    print_stage_done(theme, 2, TOTAL_STAGES, "Release index loaded");

    print_stage(theme, 3, TOTAL_STAGES, "Removing channel membership");
    let release = index
        .releases
        .iter_mut()
        .find(|release| release.version == version && release.rid == rid)
        .ok_or_else(|| SurgeError::NotFound(format!("Release {version} not found for {app_id}/{rid}")))?;

    let before_len = release.channels.len();
    release.channels.retain(|existing| existing != channel);
    if release.channels.len() == before_len {
        return Err(SurgeError::NotFound(format!(
            "Release {version} is not on channel '{channel}'"
        )));
    }

    index.last_write_utc = chrono::Utc::now().to_rfc3339();
    let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL)?;
    backend
        .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
        .await?;
    print_stage_done(
        theme,
        3,
        TOTAL_STAGES,
        &format!("Removed channel '{channel}' from release"),
    );

    print_stage(theme, 4, TOTAL_STAGES, "Finalize demote summary");
    print_stage_done(
        theme,
        4,
        TOTAL_STAGES,
        &format!(
            "Demoted {app_id} v{version} ({rid}) from channel '{channel}' in {}",
            format_duration(started.elapsed())
        ),
    );
    Ok(())
}

async fn fetch_release_index(backend: &dyn StorageBackend) -> Result<surge_core::releases::manifest::ReleaseIndex> {
    let data = backend.get_object(RELEASES_FILE_COMPRESSED).await?;
    decompress_release_index(&data)
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
    use surge_core::platform::detect::current_rid;
    use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex};

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
    async fn execute_removes_requested_channel_and_preserves_others() {
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
                releases: vec![release("1.2.3", &rid, &["beta", "stable"])],
                ..ReleaseIndex::default()
            },
        );

        execute(&manifest_path, Some("demo"), "1.2.3", Some(&rid), "beta")
            .await
            .expect("demote should succeed");

        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["stable".to_string()]);
        assert!(!index.last_write_utc.is_empty());
    }

    #[tokio::test]
    async fn execute_errors_when_release_is_not_on_requested_channel() {
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
            .expect_err("demote should fail for missing channel");
        assert!(err.to_string().contains("is not on channel 'beta'"));

        let index = read_index(&store_dir);
        assert_eq!(index.releases[0].channels, vec!["stable".to_string()]);
        assert_eq!(index.last_write_utc, "unchanged");
    }
}
