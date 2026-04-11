//! Update manager: check for updates, download, verify, and apply them.

mod apply;
mod artifacts;
mod lifecycle;
mod progress;
mod release_index;

use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::context::Context;
use crate::error::{Result, SurgeError};
use crate::install::{
    InstallProfile, RuntimeManifestMetadata, copy_persistent_assets, prune_version_snapshots,
    storage_provider_manifest_name, write_runtime_manifest,
};
use crate::platform::fs::atomic_rename;
use crate::platform::shortcuts::install_shortcuts;
use crate::releases::artifact_cache::prune_cached_artifacts;
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, decompress_release_index};
use crate::releases::restore::{local_checkpoint_artifacts_for_index, required_artifacts_for_index};
use crate::storage::{StorageBackend, create_storage_backend};
use crate::supervisor::state::supervisor_pid_file;

use self::apply::materialize_update_payload;
use self::artifacts::prepare_update_artifacts;
pub use self::progress::ProgressInfo;
use self::progress::emit_progress;
use self::release_index::{load_release_index as load_release_index_impl, resolve_update_info};

/// Strategy used when applying an update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStrategy {
    Full,
    Delta,
}

/// Information about available updates.
#[derive(Debug, Clone)]
pub struct UpdateInfo {
    /// List of releases newer than the current version.
    pub available_releases: Vec<ReleaseEntry>,
    /// The latest available version string.
    pub latest_version: String,
    /// Whether a delta update path is available.
    pub delta_available: bool,
    /// Total download size in bytes (for the chosen update strategy).
    pub download_size: i64,
    /// Release sequence that will actually be downloaded and applied.
    pub apply_releases: Vec<ReleaseEntry>,
    /// Which strategy is used for this update.
    pub apply_strategy: ApplyStrategy,
}

const DEFAULT_RELEASE_RETENTION_LIMIT: usize = 1;

/// Manages checking for and applying application updates.
pub struct UpdateManager {
    ctx: Arc<Context>,
    app_id: String,
    current_version: String,
    channel: String,
    release_retention_limit: usize,
    install_dir: PathBuf,
    storage: Box<dyn StorageBackend>,
    cached_index: Option<ReleaseIndex>,
}

impl UpdateManager {
    async fn load_release_index(&mut self) -> Result<ReleaseIndex> {
        load_release_index_impl(self).await
    }

    /// Create a new update manager.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The Surge context with storage and resource configuration
    /// * `app_id` - Application identifier
    /// * `current_version` - Currently installed version string
    /// * `channel` - Release channel to check (e.g., "stable", "beta")
    /// * `install_dir` - Path to the application install directory
    pub fn new(
        ctx: Arc<Context>,
        app_id: &str,
        current_version: &str,
        channel: &str,
        install_dir: &str,
    ) -> Result<Self> {
        let storage_cfg = ctx.storage_config();
        if !storage_cfg.access_key.trim().is_empty() || !storage_cfg.secret_key.trim().is_empty() {
            return Err(SurgeError::Config(
                "Client update checks must not embed storage credentials; use a publicly readable store".to_string(),
            ));
        }
        let storage = create_storage_backend(&storage_cfg)?;

        Ok(Self {
            ctx,
            app_id: app_id.to_string(),
            current_version: current_version.to_string(),
            channel: channel.to_string(),
            release_retention_limit: DEFAULT_RELEASE_RETENTION_LIMIT,
            install_dir: PathBuf::from(install_dir),
            storage,
            cached_index: None,
        })
    }

    /// Return the currently selected update channel.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Switch the update channel at runtime.
    ///
    /// The next `check_for_updates` call uses the new channel immediately.
    pub fn set_channel(&mut self, channel: &str) -> Result<()> {
        let normalized = channel.trim();
        if normalized.is_empty() {
            return Err(SurgeError::Config("Update channel cannot be empty".to_string()));
        }
        self.channel = normalized.to_string();
        self.cached_index = None;
        Ok(())
    }

    /// Return the currently installed version tracked by this manager.
    #[must_use]
    pub fn current_version(&self) -> &str {
        &self.current_version
    }

    /// Return the number of versioned app snapshots retained after updates.
    #[must_use]
    pub fn release_retention_limit(&self) -> usize {
        self.release_retention_limit
    }

    /// Update the local version baseline used for update checks.
    pub fn set_current_version(&mut self, version: &str) -> Result<()> {
        let normalized = version.trim();
        if normalized.is_empty() {
            return Err(SurgeError::Config("Current version cannot be empty".to_string()));
        }
        self.current_version = normalized.to_string();
        self.cached_index = None;
        Ok(())
    }

    /// Update the number of old app snapshots retained after successful updates.
    pub fn set_release_retention_limit(&mut self, limit: usize) {
        self.release_retention_limit = limit;
    }

    /// Check for available updates on the configured channel.
    ///
    /// Downloads the release index from storage and compares versions.
    /// Returns `None` if no updates are available, or `Some(UpdateInfo)`
    /// with details about available releases.
    pub async fn check_for_updates(&mut self) -> Result<Option<UpdateInfo>> {
        self.ctx.check_cancelled()?;

        info!(
            app_id = %self.app_id,
            current_version = %self.current_version,
            channel = %self.channel,
            "Checking for updates"
        );

        let index = self.load_release_index().await?;
        resolve_update_info(self, index)
    }

    /// Download and apply an update.
    ///
    /// Executes a 6-phase pipeline:
    /// 1. Check - validate update info and prepare
    /// 2. Download - fetch update packages from storage
    /// 3. Verify - verify SHA-256 hashes of downloaded files
    /// 4. Extract - extract the final archive into the install tree
    /// 5. Apply delta - rebuild the final archive when using delta updates
    /// 6. Finalize - move files into place, clean up
    pub async fn download_and_apply<F>(&self, info: &UpdateInfo, progress: Option<F>) -> Result<()>
    where
        F: Fn(ProgressInfo) + Send + Sync,
    {
        self.ctx.check_cancelled()?;
        let progress = progress.map(Arc::new);

        // Phase 1: Check
        info!(version = %info.latest_version, "Starting update");
        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 1,
                ..ProgressInfo::default()
            },
        );

        if info.apply_releases.is_empty() {
            return Err(SurgeError::Update("No releases to apply".to_string()));
        }

        let staging_dir = self.install_dir.join(".surge-staging");
        tokio::fs::create_dir_all(&staging_dir).await?;

        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 1,
                phase_percent: 100,
                total_percent: 5,
                ..ProgressInfo::default()
            },
        );

        // Phase 2: Download
        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 2,
                total_percent: 10,
                bytes_total: info.download_size.max(0),
                items_total: i64::try_from(info.apply_releases.len()).unwrap_or(i64::MAX),
                ..ProgressInfo::default()
            },
        );

        let artifact_cache_dir = self.install_dir.join(".surge-cache").join("artifacts");
        tokio::fs::create_dir_all(&artifact_cache_dir).await?;
        prepare_update_artifacts(self, info, &staging_dir, &artifact_cache_dir, progress.as_ref()).await?;

        let extract_dir = staging_dir.join("extracted");
        tokio::fs::create_dir_all(&extract_dir).await?;
        let extracted_final_dir = materialize_update_payload(
            self,
            info,
            &staging_dir,
            &artifact_cache_dir,
            &extract_dir,
            progress.as_ref(),
        )
        .await?;

        // Phase 6: Finalize
        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 6,
                total_percent: 90,
                ..ProgressInfo::default()
            },
        );
        let latest = info
            .apply_releases
            .last()
            .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
        let active_app_dir = self.install_dir.join("app");
        let next_app_dir = self.install_dir.join(".surge-app-next");
        let previous_swap_dir = self.install_dir.join(".surge-app-prev");
        let supervisor_was_running = !latest.supervisor_id.trim().is_empty()
            && supervisor_pid_file(&self.install_dir, &latest.supervisor_id).is_file();

        lifecycle::request_supervisor_shutdown(&self.install_dir, &latest.supervisor_id).await?;

        if next_app_dir.exists() {
            tokio::fs::remove_dir_all(&next_app_dir).await?;
        }
        if previous_swap_dir.exists() {
            tokio::fs::remove_dir_all(&previous_swap_dir).await?;
        }

        // Legacy installs may still be on `app-{version}` layout.
        let fallback_previous_app_dir = if active_app_dir.is_dir() {
            None
        } else {
            apply::find_previous_app_dir(&self.install_dir, &self.current_version)
        };

        atomic_rename(&extracted_final_dir, &next_app_dir)?;

        if active_app_dir.is_dir() {
            atomic_rename(&active_app_dir, &previous_swap_dir)?;
        }
        if let Err(err) = atomic_rename(&next_app_dir, &active_app_dir) {
            // Best effort rollback to previous active content.
            if previous_swap_dir.is_dir() && !active_app_dir.exists() {
                let _ = atomic_rename(&previous_swap_dir, &active_app_dir);
            }
            return Err(err);
        }

        let previous_app_dir_for_assets = if previous_swap_dir.is_dir() {
            Some(previous_swap_dir.as_path())
        } else {
            fallback_previous_app_dir.as_deref()
        };

        if !latest.persistent_assets.is_empty() {
            if let Some(previous) = previous_app_dir_for_assets {
                copy_persistent_assets(previous, &active_app_dir, &latest.persistent_assets)?;
            } else {
                debug!(
                    version = %latest.version,
                    "No previous app directory found; skipping persistent asset carry-over"
                );
            }
        }

        let storage_cfg = self.ctx.storage_config();
        let runtime_manifest_profile = InstallProfile::new(
            &self.app_id,
            latest.display_name(&self.app_id),
            &latest.main_exe,
            &latest.install_directory,
            &latest.supervisor_id,
            &latest.icon,
            &latest.shortcuts,
            &latest.persistent_assets,
            &latest.environment,
        );
        let runtime_manifest_metadata = RuntimeManifestMetadata::new(
            &latest.version,
            &self.channel,
            storage_provider_manifest_name(storage_cfg.provider),
            &storage_cfg.bucket,
            &storage_cfg.region,
            &storage_cfg.endpoint,
        );
        write_runtime_manifest(&active_app_dir, &runtime_manifest_profile, &runtime_manifest_metadata)?;

        if !latest.shortcuts.is_empty() {
            match install_shortcuts(
                &self.app_id,
                latest.display_name(&self.app_id),
                &active_app_dir,
                &latest.main_exe,
                &latest.supervisor_id,
                &latest.icon,
                &latest.shortcuts,
                &latest.environment,
            ) {
                Ok(()) => {
                    debug!(version = %latest.version, "Installed shortcuts");
                }
                Err(e) => {
                    warn!(
                        version = %latest.version,
                        error = %e,
                        "Failed to install shortcuts (continuing)"
                    );
                }
            }
        }

        if previous_swap_dir.is_dir() {
            let previous_version_dir = self.install_dir.join(format!("app-{}", self.current_version));
            if !self.current_version.trim().is_empty()
                && previous_version_dir != active_app_dir
                && !previous_version_dir.exists()
            {
                if let Err(e) = atomic_rename(&previous_swap_dir, &previous_version_dir) {
                    warn!(
                        previous = %previous_swap_dir.display(),
                        target = %previous_version_dir.display(),
                        error = %e,
                        "Failed to preserve previous active directory snapshot"
                    );
                    let _ = tokio::fs::remove_dir_all(&previous_swap_dir).await;
                }
            } else {
                let _ = tokio::fs::remove_dir_all(&previous_swap_dir).await;
            }
        }
        match prune_version_snapshots(&self.install_dir, self.release_retention_limit) {
            Ok(0) => {}
            Ok(pruned) => {
                debug!(
                    pruned,
                    retained = self.release_retention_limit,
                    "Pruned stale installed app version snapshots"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to prune installed app version snapshots");
            }
        }

        // Clean up staging directory
        if staging_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        }

        let prune_index = if let Some(cached) = &self.cached_index {
            Some(cached.clone())
        } else {
            match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
                Ok(data) => Some(decompress_release_index(&data)?),
                Err(SurgeError::NotFound(_)) => None,
                Err(e) => return Err(e),
            }
        };
        if let Some(index) = prune_index {
            let mut retained_artifacts = required_artifacts_for_index(&index);
            retained_artifacts.extend(local_checkpoint_artifacts_for_index(&index, 3));
            let warm_full_filename = latest.full_filename.trim();
            if !warm_full_filename.is_empty() {
                retained_artifacts.insert(warm_full_filename.to_string());
            }
            match prune_cached_artifacts(&artifact_cache_dir, &retained_artifacts) {
                Ok(0) => {}
                Ok(pruned) => {
                    debug!(
                        pruned,
                        retained = retained_artifacts.len(),
                        "Pruned stale local artifact cache entries"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Failed to prune local artifact cache");
                }
            }
        }

        lifecycle::invoke_post_update_hook(&self.install_dir, &active_app_dir, latest);

        if supervisor_was_running {
            lifecycle::restart_supervisor_after_update(&self.install_dir, &active_app_dir, latest);
        }

        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 6,
                phase_percent: 100,
                total_percent: 100,
                ..ProgressInfo::default()
            },
        );

        info!(
            version = %info.latest_version,
            "Update applied successfully"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::config::constants::DEFAULT_ZSTD_LEVEL;
    #[cfg(target_os = "linux")]
    use crate::config::manifest::ShortcutLocation;
    use crate::context::StorageProvider;
    use crate::crypto::sha256::{sha256_hex, sha256_hex_file};
    use crate::diff::chunked::ChunkedDiffOptions;
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::platform::detect::current_rid;
    #[cfg(target_os = "linux")]
    use crate::platform::shortcuts::{
        clear_test_shortcut_paths_override, lock_test_shortcut_environment_async, set_test_shortcut_paths_override,
    };
    use crate::releases::delta::{apply_delta_patch, build_sparse_file_patch, decode_delta_patch};
    use crate::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, compress_release_index};
    use crate::releases::restore::find_release_for_version_rid;

    #[cfg(target_os = "linux")]
    struct ShortcutPathsOverrideGuard;

    #[cfg(target_os = "linux")]
    impl Drop for ShortcutPathsOverrideGuard {
        fn drop(&mut self) {
            clear_test_shortcut_paths_override();
        }
    }

    fn make_entry(version: &str, channel: &str, os: &str, rid: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec![channel.to_string()],
            os: os.to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: format!("{version}-full.tar.zst"),
            full_size: 1000,
            full_sha256: String::new(),
            deltas: vec![DeltaArtifact::bsdiff_zstd(
                "primary",
                "",
                &format!("{version}-delta.tar.zst"),
                100,
                "",
            )],
            preferred_delta_id: "primary".to_string(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "test-app".to_string(),
            install_directory: "test-app".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    fn current_os_label_for_tests() -> String {
        let rid = current_rid();
        let raw = rid.split('-').next().unwrap_or_default();
        release_index::normalize_os_label(raw)
    }

    fn app_scoped_store_root(store_root: &Path, app_id: &str) -> PathBuf {
        let app_store = store_root.join(app_id);
        std::fs::create_dir_all(&app_store).unwrap();
        app_store
    }

    fn write_app_scoped_release_index(store_root: &Path, app_id: &str, index: &ReleaseIndex) -> PathBuf {
        let app_store = app_scoped_store_root(store_root, app_id);
        let compressed = compress_release_index(index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(app_store.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();
        app_store
    }

    fn pseudo_random_bytes(len: usize) -> Vec<u8> {
        let mut state = 0x1234_5678_9abc_def0_u64;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push((state & 0xff) as u8);
        }
        bytes
    }

    #[test]
    fn test_progress_info_default() {
        let p = ProgressInfo::default();
        assert_eq!(p.phase, 0);
        assert_eq!(p.total_percent, 0);
        assert!(p.speed_bytes_per_sec.abs() < f64::EPSILON);
    }

    #[test]
    fn test_update_info_creation() {
        let info = UpdateInfo {
            available_releases: vec![],
            latest_version: "2.0.0".to_string(),
            delta_available: false,
            download_size: 1024,
            apply_releases: vec![],
            apply_strategy: ApplyStrategy::Full,
        };
        assert_eq!(info.latest_version, "2.0.0");
        assert!(!info.delta_available);
    }

    #[test]
    fn test_update_manager_no_storage() {
        let ctx = Arc::new(Context::new());
        let result = UpdateManager::new(ctx, "app", "1.0.0", "stable", "/tmp/app");
        assert!(result.is_err());
    }

    #[test]
    fn test_update_manager_rejects_embedded_credentials() {
        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::S3,
            "bucket",
            "region",
            "embedded-key",
            "embedded-secret",
            "",
        );

        let err = UpdateManager::new(ctx, "app", "1.0.0", "stable", "/tmp/app")
            .err()
            .expect("expected UpdateManager::new to fail");
        assert!(err.to_string().contains("must not embed storage credentials"));
    }

    #[test]
    fn test_set_channel_and_version_validate_input() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, "app", "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();
        assert_eq!(manager.channel(), "stable");
        assert_eq!(manager.current_version(), "1.0.0");
        assert_eq!(manager.release_retention_limit(), 1);

        manager.set_channel("test").unwrap();
        manager.set_current_version("1.1.0").unwrap();
        manager.set_release_retention_limit(0);
        assert_eq!(manager.channel(), "test");
        assert_eq!(manager.current_version(), "1.1.0");
        assert_eq!(manager.release_retention_limit(), 0);

        let err = manager.set_channel("  ").unwrap_err();
        assert!(err.to_string().contains("channel cannot be empty"));
        let err = manager.set_current_version("").unwrap_err();
        assert!(err.to_string().contains("Current version cannot be empty"));
    }

    #[test]
    fn test_os_normalization() {
        assert_eq!(release_index::normalize_os_label("windows"), "win");
        assert_eq!(release_index::normalize_os_label("win"), "win");
        assert_eq!(release_index::normalize_os_label("macos"), "osx");
        assert_eq!(release_index::normalize_os_label("linux"), "linux");
    }

    #[test]
    fn test_release_rid_filter() {
        let release = make_entry("1.0.0", "stable", "linux", "linux-x64");
        assert!(release_index::release_matches_rid(&release, "linux-x64"));
        assert!(!release_index::release_matches_rid(&release, "win-x64"));
    }

    #[tokio::test]
    async fn test_request_supervisor_shutdown_noop_when_supervisor_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        lifecycle::request_supervisor_shutdown(tmp.path(), "").await.unwrap();
        lifecycle::request_supervisor_shutdown(tmp.path(), "missing")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_request_supervisor_shutdown_waits_for_pid_file_to_disappear() {
        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path();
        let supervisor_id = "test-supervisor";
        let pid_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.pid"));
        let stop_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.stop"));
        std::fs::write(&pid_file, "123").unwrap();

        let pid_file_for_task = pid_file.clone();
        let stop_file_for_task = stop_file.clone();
        let waiter = tokio::spawn(async move {
            for _ in 0..100 {
                if stop_file_for_task.exists() {
                    std::fs::remove_file(pid_file_for_task).unwrap();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for stop file to be created");
        });

        lifecycle::request_supervisor_shutdown_with_timeout(
            install_dir,
            supervisor_id,
            Duration::from_secs(2),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        waiter.await.unwrap();
        assert!(!stop_file.exists());
    }

    #[tokio::test]
    async fn test_request_supervisor_shutdown_times_out_when_supervisor_does_not_exit() {
        let tmp = tempfile::tempdir().unwrap();
        let install_dir = tmp.path();
        let supervisor_id = "test-supervisor";
        let pid_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.pid"));
        let stop_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.stop"));
        std::fs::write(&pid_file, "123").unwrap();

        let err = lifecycle::request_supervisor_shutdown_with_timeout(
            install_dir,
            supervisor_id,
            Duration::from_millis(50),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("Timed out waiting for supervisor"));
        assert!(stop_file.exists());
    }

    #[tokio::test]
    async fn test_check_for_updates_rejects_mismatched_app_id() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let index = ReleaseIndex {
            app_id: "other-app".to_string(),
            releases: vec![make_entry("1.1.0", "stable", "", "")],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let err = manager.check_for_updates().await.unwrap_err();
        assert!(err.to_string().contains("does not match requested app"));
    }

    #[tokio::test]
    async fn test_check_for_updates_loads_required_app_scoped_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![make_entry(
                "1.1.0",
                "stable",
                &current_os_label_for_tests(),
                &current_rid(),
            )],
            ..ReleaseIndex::default()
        };
        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let mut manager =
            UpdateManager::new(ctx.clone(), app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let info = manager
            .check_for_updates()
            .await
            .expect("update check should succeed")
            .expect("update should be available");
        assert_eq!(info.latest_version, "1.1.0");
        assert_eq!(ctx.storage_config().prefix, app_id);
    }

    #[tokio::test]
    async fn test_check_for_updates_prefers_app_scoped_prefix_when_root_index_is_mismatched() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let root_index = ReleaseIndex {
            app_id: "other-app".to_string(),
            releases: vec![make_entry(
                "9.9.9",
                "stable",
                &current_os_label_for_tests(),
                &current_rid(),
            )],
            ..ReleaseIndex::default()
        };
        let root_compressed = compress_release_index(&root_index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), root_compressed).unwrap();

        let scoped_index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![make_entry(
                "1.1.0",
                "stable",
                &current_os_label_for_tests(),
                &current_rid(),
            )],
            ..ReleaseIndex::default()
        };
        write_app_scoped_release_index(&store_root, app_id, &scoped_index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let mut manager =
            UpdateManager::new(ctx.clone(), app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let info = manager
            .check_for_updates()
            .await
            .expect("update check should succeed")
            .expect("update should be available");
        assert_eq!(info.latest_version, "1.1.0");
        assert_eq!(ctx.storage_config().prefix, app_id);
    }

    #[tokio::test]
    async fn test_check_for_updates_requires_app_scoped_prefix_when_it_is_derivable() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let root_index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![make_entry(
                "1.1.0",
                "stable",
                &current_os_label_for_tests(),
                &current_rid(),
            )],
            ..ReleaseIndex::default()
        };
        let root_compressed = compress_release_index(&root_index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), root_compressed).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let err = manager.check_for_updates().await.unwrap_err();
        assert!(err.to_string().contains("not found on required app-scoped prefix"));
    }

    #[tokio::test]
    async fn test_check_for_updates_genesis_without_delta_uses_full_strategy() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let mut release = make_entry("1.1.0", "stable", &current_os_label_for_tests(), &current_rid());
        release.is_genesis = true;
        release.set_primary_delta(None);

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert!(!info.delta_available);
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);
        assert_eq!(info.apply_releases.len(), 1);
        assert_eq!(info.apply_releases[0].full_filename, "1.1.0-full.tar.zst");
    }

    #[tokio::test]
    async fn test_check_for_updates_treats_stable_as_newer_than_matching_prerelease() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let rid = current_rid();
        let os = current_os_label_for_tests();

        let mut prerelease = make_entry("2859.0.0-prerelease.56", "test", &os, &rid);
        prerelease.full_filename = format!("test-app-2859.0.0-prerelease.56-{rid}-full.tar.zst");
        prerelease.set_primary_delta(None);
        prerelease.is_genesis = true;

        let mut stable = make_entry("2859.0.0", "test", &os, &rid);
        stable.full_filename = format!("test-app-2859.0.0-{rid}-full.tar.zst");
        stable.set_primary_delta(None);

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![prerelease, stable],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(
            ctx,
            app_id,
            "2859.0.0-prerelease.56",
            "test",
            tmp.path().to_str().unwrap(),
        )
        .unwrap();

        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.latest_version, "2859.0.0");
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);
        assert_eq!(info.apply_releases.len(), 1);
        assert_eq!(info.apply_releases[0].version, "2859.0.0");
    }

    #[tokio::test]
    async fn test_check_for_updates_uses_descriptor_delta_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let rid = current_rid();
        let os = current_os_label_for_tests();
        let mut release = make_entry("1.1.0", "stable", &os, &rid);
        release.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "1.1.0-descriptor-delta.tar.zst",
            99,
            "",
        )));

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };
        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert!(info.delta_available);
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);
        assert_eq!(info.download_size, 99);
    }

    #[tokio::test]
    async fn test_check_for_updates_falls_back_to_full_for_unsupported_descriptor() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();

        let rid = current_rid();
        let os = current_os_label_for_tests();
        let mut release = make_entry("1.1.0", "stable", &os, &rid);
        release.deltas = vec![DeltaArtifact {
            id: "primary".to_string(),
            from_version: "1.0.0".to_string(),
            algorithm: "qbsdiff_bsdiff4".to_string(),
            patch_format: "bsdiff4".to_string(),
            compression: "zstd".to_string(),
            filename: "1.1.0-unsupported-delta.tar.zst".to_string(),
            size: 99,
            sha256: String::new(),
        }];
        release.preferred_delta_id = "primary".to_string();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };
        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert!(!info.delta_available);
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);
    }

    #[tokio::test]
    async fn test_check_for_updates_after_channel_switch() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        let rid = current_rid();
        let os = current_os_label_for_tests();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![
                make_entry("1.1.0", "stable", &os, &rid),
                make_entry("1.2.0", "test", &os, &rid),
            ],
            ..ReleaseIndex::default()
        };
        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();
        let stable_update = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(stable_update.latest_version, "1.1.0");

        manager.set_channel("test").unwrap();
        let test_update = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(test_update.latest_version, "1.2.0");
    }

    #[tokio::test]
    async fn test_download_and_apply_full_installs_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"installed payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: app_id.to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();

        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);

        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let installed_file = install_root.join("app").join("payload.txt");
        assert!(installed_file.exists());
        assert_eq!(std::fs::read_to_string(installed_file).unwrap(), "installed payload");
        let runtime_manifest = install_root
            .join("app")
            .join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH);
        assert!(runtime_manifest.is_file());
        let runtime_manifest_raw = std::fs::read_to_string(&runtime_manifest).unwrap();
        assert!(runtime_manifest_raw.contains("id: test-app"));
        assert!(runtime_manifest_raw.contains("version: 1.1.0"));
        assert!(runtime_manifest_raw.contains("channel: stable"));

        std::fs::remove_file(app_store.join(&full_filename)).unwrap();
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();
        let installed_file = install_root.join("app").join("payload.txt");
        assert!(installed_file.exists());
        assert_eq!(std::fs::read_to_string(installed_file).unwrap(), "installed payload");
        assert!(runtime_manifest.is_file());
    }

    #[tokio::test]
    async fn test_download_and_apply_full_removes_non_persistent_files_and_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let current_app_dir = install_root.join("app");
        std::fs::create_dir_all(current_app_dir.join("state")).unwrap();
        std::fs::create_dir_all(current_app_dir.join("temp")).unwrap();
        std::fs::write(current_app_dir.join("settings.json"), "persisted settings").unwrap();
        std::fs::write(current_app_dir.join("state").join("user.db"), "persisted state").unwrap();
        std::fs::write(current_app_dir.join("old-token.txt"), "remove me").unwrap();
        std::fs::write(current_app_dir.join("temp").join("old.log"), "remove dir").unwrap();

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"new payload", 0o644).unwrap();
        packer.add_buffer("settings.json", b"packaged settings", 0o644).unwrap();
        packer.add_buffer("state/default.db", b"packaged state", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: app_id.to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: vec!["settings.json".to_string(), "state".to_string()],
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);

        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let active_app_dir = install_root.join("app");
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("payload.txt")).unwrap(),
            "new payload"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("settings.json")).unwrap(),
            "persisted settings"
        );
        assert_eq!(
            std::fs::read_to_string(active_app_dir.join("state").join("user.db")).unwrap(),
            "persisted state"
        );
        assert!(!active_app_dir.join("state").join("default.db").exists());
        assert!(!active_app_dir.join("old-token.txt").exists());
        assert!(!active_app_dir.join("temp").exists());
    }

    #[tokio::test]
    async fn test_download_and_apply_reports_incremental_progress_for_full_update() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        for index in 0..3 {
            let payload = pseudo_random_bytes(196_608 + (index * 4_096));
            packer
                .add_buffer(&format!("payload-{index}.bin"), &payload, 0o644)
                .unwrap();
        }
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: app_id.to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();

        let observed = Arc::new(Mutex::new(Vec::<ProgressInfo>::new()));
        let observed_for_progress = Arc::clone(&observed);

        manager
            .download_and_apply(
                &info,
                Some(move |progress: ProgressInfo| {
                    observed_for_progress
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(progress);
                }),
            )
            .await
            .unwrap();

        let observed = observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        let initial_download = observed
            .iter()
            .find(|progress| progress.phase == 2 && progress.total_percent == 10)
            .expect("expected initial download progress");
        assert_eq!(initial_download.bytes_total, full_size);

        assert!(observed.iter().any(|progress| {
            progress.phase == 2 && progress.phase_percent > 0 && progress.phase_percent < 100 && progress.bytes_done > 0
        }));
        assert!(
            observed
                .iter()
                .any(|progress| progress.phase == 2 && progress.speed_bytes_per_sec > 0.0)
        );
        assert!(observed.iter().any(|progress| {
            progress.phase == 4
                && progress.phase_percent > 0
                && progress.phase_percent < 100
                && progress.items_done > 0
                && progress.items_total >= 3
        }));

        let final_progress = observed.last().expect("expected final progress");
        assert_eq!(final_progress.phase, 6);
        assert_eq!(final_progress.total_percent, 100);
    }

    #[tokio::test]
    async fn test_download_and_apply_reports_incremental_progress_for_delta_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let rid = current_rid();
        let os = current_os_label_for_tests();

        let mut packer_v1 = ArchivePacker::new(3).unwrap();
        packer_v1.add_buffer("payload.txt", b"v1 payload", 0o644).unwrap();
        let full_v1 = packer_v1.finalize().unwrap();

        let mut packer_v2 = ArchivePacker::new(3).unwrap();
        packer_v2.add_buffer("payload.txt", b"v2 payload", 0o644).unwrap();
        let full_v2 = packer_v2.finalize().unwrap();

        let mut packer_v3 = ArchivePacker::new(3).unwrap();
        packer_v3.add_buffer("payload.txt", b"v3 payload", 0o644).unwrap();
        let full_v3 = packer_v3.finalize().unwrap();

        let mut packer_v4 = ArchivePacker::new(3).unwrap();
        packer_v4.add_buffer("payload.txt", b"v4 payload", 0o644).unwrap();
        let full_v4 = packer_v4.finalize().unwrap();

        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let patch_v3 = bsdiff_buffers(&full_v2, &full_v3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();
        let patch_v4 = bsdiff_buffers(&full_v3, &full_v4).unwrap();
        let delta_v4 = zstd::encode_all(patch_v4.as_slice(), 3).unwrap();

        let full_v1_name = format!("{app_id}-1.0.0-{rid}-full.tar.zst");
        let full_v2_name = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_v3_name = format!("{app_id}-1.2.0-{rid}-full.tar.zst");
        let full_v4_name = format!("{app_id}-1.3.0-{rid}-full.tar.zst");
        let delta_v2_name = format!("{app_id}-1.1.0-{rid}-delta.tar.zst");
        let delta_v3_name = format!("{app_id}-1.2.0-{rid}-delta.tar.zst");
        let delta_v4_name = format!("{app_id}-1.3.0-{rid}-delta.tar.zst");

        std::fs::write(app_store.join(&full_v1_name), &full_v1).unwrap();
        std::fs::write(app_store.join(&delta_v2_name), &delta_v2).unwrap();
        std::fs::write(app_store.join(&delta_v3_name), &delta_v3).unwrap();
        std::fs::write(app_store.join(&delta_v4_name), &delta_v4).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![
                ReleaseEntry {
                    version: "1.0.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: true,
                    full_filename: full_v1_name.clone(),
                    full_size: full_v1.len() as i64,
                    full_sha256: sha256_hex(&full_v1),
                    deltas: Vec::new(),
                    preferred_delta_id: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
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
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v2_name.clone(),
                    full_size: full_v2.len() as i64,
                    full_sha256: sha256_hex(&full_v2),
                    deltas: vec![DeltaArtifact::bsdiff_zstd(
                        "primary",
                        "1.0.0",
                        &delta_v2_name,
                        delta_v2.len() as i64,
                        &sha256_hex(&delta_v2),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
                ReleaseEntry {
                    version: "1.2.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v3_name.clone(),
                    full_size: full_v3.len() as i64,
                    full_sha256: sha256_hex(&full_v3),
                    deltas: vec![DeltaArtifact::bsdiff_zstd(
                        "primary",
                        "1.1.0",
                        &delta_v3_name,
                        delta_v3.len() as i64,
                        &sha256_hex(&delta_v3),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
                ReleaseEntry {
                    version: "1.3.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os,
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v4_name,
                    full_size: full_v4.len() as i64,
                    full_sha256: sha256_hex(&full_v4),
                    deltas: vec![DeltaArtifact::bsdiff_zstd(
                        "primary",
                        "1.2.0",
                        &delta_v4_name,
                        delta_v4.len() as i64,
                        &sha256_hex(&delta_v4),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
            ],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);

        let observed = Arc::new(Mutex::new(Vec::<ProgressInfo>::new()));
        let observed_for_progress = Arc::clone(&observed);
        manager
            .download_and_apply(
                &info,
                Some(move |progress: ProgressInfo| {
                    observed_for_progress
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(progress);
                }),
            )
            .await
            .unwrap();

        let observed = observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        assert!(observed.iter().any(|progress| {
            progress.phase == 5
                && progress.phase_percent > 0
                && progress.phase_percent < 100
                && progress.items_done > 0
                && progress.items_done < progress.items_total
        }));

        let installed = std::fs::read_to_string(install_root.join("app").join("payload.txt")).unwrap();
        assert_eq!(installed, "v4 payload");
    }

    #[tokio::test]
    async fn test_download_and_apply_delta_restores_missing_base_full() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let rid = current_rid();
        let os = current_os_label_for_tests();

        let mut packer_v1 = ArchivePacker::new(3).unwrap();
        packer_v1.add_buffer("payload.txt", b"v1 payload", 0o644).unwrap();
        let full_v1 = packer_v1.finalize().unwrap();

        let mut packer_v2 = ArchivePacker::new(3).unwrap();
        packer_v2.add_buffer("payload.txt", b"v2 payload", 0o644).unwrap();
        let full_v2 = packer_v2.finalize().unwrap();

        let mut packer_v3 = ArchivePacker::new(3).unwrap();
        packer_v3.add_buffer("payload.txt", b"v3 payload", 0o644).unwrap();
        let full_v3 = packer_v3.finalize().unwrap();

        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let patch_v3 = bsdiff_buffers(&full_v2, &full_v3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let full_v1_name = format!("{app_id}-1.0.0-{rid}-full.tar.zst");
        let full_v2_name = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_v3_name = format!("{app_id}-1.2.0-{rid}-full.tar.zst");
        let delta_v2_name = format!("{app_id}-1.1.0-{rid}-delta.tar.zst");
        let delta_v3_name = format!("{app_id}-1.2.0-{rid}-delta.tar.zst");

        std::fs::write(app_store.join(&full_v1_name), &full_v1).unwrap();
        std::fs::write(app_store.join(&delta_v2_name), &delta_v2).unwrap();
        std::fs::write(app_store.join(&delta_v3_name), &delta_v3).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![
                ReleaseEntry {
                    version: "1.0.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: true,
                    full_filename: full_v1_name.clone(),
                    full_size: full_v1.len() as i64,
                    full_sha256: sha256_hex(&full_v1),
                    deltas: Vec::new(),
                    preferred_delta_id: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
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
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v2_name.clone(),
                    full_size: full_v2.len() as i64,
                    full_sha256: sha256_hex(&full_v2),
                    deltas: vec![DeltaArtifact::bsdiff_zstd(
                        "primary",
                        "1.0.0",
                        &delta_v2_name,
                        delta_v2.len() as i64,
                        &sha256_hex(&delta_v2),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
                ReleaseEntry {
                    version: "1.2.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os,
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v3_name.clone(),
                    full_size: full_v3.len() as i64,
                    full_sha256: sha256_hex(&full_v3),
                    deltas: vec![DeltaArtifact::bsdiff_zstd(
                        "primary",
                        "1.1.0",
                        &delta_v3_name,
                        delta_v3.len() as i64,
                        &sha256_hex(&delta_v3),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
            ],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.1.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let installed = std::fs::read_to_string(install_root.join("app").join("payload.txt")).unwrap();
        assert_eq!(installed, "v3 payload");
    }

    #[tokio::test]
    async fn test_download_and_apply_delta_rebuilds_current_full_from_installed_app_when_cache_chain_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let rid = current_rid();
        let os = current_os_label_for_tests();

        let source_v2 = tmp.path().join("source-v2");
        let source_v3 = tmp.path().join("source-v3");
        std::fs::create_dir_all(&source_v2).unwrap();
        std::fs::create_dir_all(&source_v3).unwrap();
        std::fs::write(source_v2.join("payload.txt"), "v2 payload").unwrap();
        std::fs::write(source_v3.join("payload.txt"), "v3 payload").unwrap();

        let mut packer_v2 = ArchivePacker::new(3).unwrap();
        packer_v2.add_directory(&source_v2, "").unwrap();
        let full_v2 = packer_v2.finalize().unwrap();

        let mut packer_v3 = ArchivePacker::new(3).unwrap();
        packer_v3.add_directory(&source_v3, "").unwrap();
        let full_v3 = packer_v3.finalize().unwrap();

        let patch_v3 = build_sparse_file_patch(&full_v2, &full_v3, 3, 0, &ChunkedDiffOptions::default()).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let full_v2_name = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_v3_name = format!("{app_id}-1.2.0-{rid}-full.tar.zst");
        let delta_v3_name = format!("{app_id}-1.2.0-{rid}-delta.tar.zst");

        std::fs::write(app_store.join(&delta_v3_name), &delta_v3).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![
                ReleaseEntry {
                    version: "1.1.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: true,
                    full_filename: full_v2_name.clone(),
                    full_size: full_v2.len() as i64,
                    full_sha256: sha256_hex(&full_v2),
                    deltas: Vec::new(),
                    preferred_delta_id: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
                ReleaseEntry {
                    version: "1.2.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os,
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v3_name.clone(),
                    full_size: full_v3.len() as i64,
                    full_sha256: sha256_hex(&full_v3),
                    deltas: vec![DeltaArtifact::sparse_file_ops_zstd(
                        "primary",
                        "1.1.0",
                        &delta_v3_name,
                        delta_v3.len() as i64,
                        &sha256_hex(&delta_v3),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
            ],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let active_app_dir = install_root.join("app");
        std::fs::create_dir_all(active_app_dir.join(".surge")).unwrap();
        std::fs::write(active_app_dir.join("payload.txt"), "v2 payload").unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            format!("id: {app_id}\nversion: 1.1.0\n"),
        )
        .unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH),
            format!("id: {app_id}\nversion: 1.1.0\n"),
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let current_release = find_release_for_version_rid(&index, &rid, "1.1.0").unwrap();
        let artifact_cache_dir = install_root.join(".surge-cache").join("artifacts");
        let synthesized = apply::synthesize_current_full_archive_from_installed_app(
            &install_root,
            "1.1.0",
            current_release,
            &artifact_cache_dir,
            &ctx,
        )
        .unwrap();

        let full_extract = tempfile::tempdir().unwrap();
        crate::archive::extractor::extract_to(&full_v2, full_extract.path(), None).unwrap();
        let synth_extract = tempfile::tempdir().unwrap();
        crate::archive::extractor::extract_to(&synthesized, synth_extract.path(), None).unwrap();
        let mut repacked_full = ArchivePacker::new(3).unwrap();
        repacked_full.add_directory(full_extract.path(), "").unwrap();
        let repacked_full = repacked_full.finalize().unwrap();
        let mut repacked_synth = ArchivePacker::new(3).unwrap();
        repacked_synth.add_directory(synth_extract.path(), "").unwrap();
        let repacked_synth = repacked_synth.finalize().unwrap();
        assert_eq!(repacked_synth, repacked_full);

        let delta = index.releases[1].selected_delta().unwrap();
        let decoded = decode_delta_patch(&delta_v3, &delta).unwrap();
        let rebuilt = apply_delta_patch(&synthesized, &decoded, &delta).unwrap();
        assert_eq!(rebuilt, full_v3);

        let mut manager = UpdateManager::new(ctx, app_id, "1.1.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);
        assert_eq!(info.apply_releases.len(), 1);
        assert_eq!(info.apply_releases[0].version, "1.2.0");
        assert_eq!(info.apply_releases[0].full_sha256, sha256_hex(&full_v3));
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let installed = std::fs::read_to_string(install_root.join("app").join("payload.txt")).unwrap();
        assert_eq!(installed, "v3 payload");

        let cached_current_full = install_root.join(".surge-cache").join("artifacts").join(&full_v2_name);
        assert!(!cached_current_full.exists());
        let cached_latest_full = install_root.join(".surge-cache").join("artifacts").join(&full_v3_name);
        assert!(cached_latest_full.exists());
    }

    #[tokio::test]
    async fn test_download_and_apply_delta_prefers_app_scoped_release_index_lineage() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        let app_scoped_store = store_root.join(app_id);
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&app_scoped_store).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let rid = current_rid();
        let os = current_os_label_for_tests();

        let source_v2_good = tmp.path().join("source-v2-good");
        let source_v2_bad = tmp.path().join("source-v2-bad");
        let source_v3 = tmp.path().join("source-v3");
        std::fs::create_dir_all(&source_v2_good).unwrap();
        std::fs::create_dir_all(&source_v2_bad).unwrap();
        std::fs::create_dir_all(&source_v3).unwrap();
        std::fs::write(source_v2_good.join("payload.txt"), "v2 payload").unwrap();
        std::fs::write(source_v2_bad.join("payload.txt"), "v2 payload").unwrap();
        std::fs::write(source_v3.join("payload.txt"), "v3 payload").unwrap();
        std::fs::write(
            source_v2_good.join("camera-tuner.deps.json"),
            "{\"deps\":\"good-v2\"}\n",
        )
        .unwrap();
        std::fs::write(source_v2_bad.join("camera-tuner.deps.json"), "{\"deps\":\"bad-v2\"}\n").unwrap();
        std::fs::write(source_v3.join("camera-tuner.deps.json"), "{\"deps\":\"v3\"}\n").unwrap();

        let mut good_v2_packer = ArchivePacker::new(3).unwrap();
        good_v2_packer.add_directory(&source_v2_good, "").unwrap();
        let full_v2_good = good_v2_packer.finalize().unwrap();

        let mut bad_v2_packer = ArchivePacker::new(3).unwrap();
        bad_v2_packer.add_directory(&source_v2_bad, "").unwrap();
        let full_v2_bad = bad_v2_packer.finalize().unwrap();

        let mut v3_packer = ArchivePacker::new(3).unwrap();
        v3_packer.add_directory(&source_v3, "").unwrap();
        let full_v3 = v3_packer.finalize().unwrap();

        let patch_v3 = build_sparse_file_patch(&full_v2_good, &full_v3, 3, 0, &ChunkedDiffOptions::default()).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let full_v2_name = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_v3_name = format!("{app_id}-1.2.0-{rid}-full.tar.zst");
        let delta_v3_name = format!("{app_id}-1.2.0-{rid}-delta.tar.zst");

        std::fs::write(store_root.join(&full_v2_name), &full_v2_bad).unwrap();
        std::fs::write(store_root.join(&full_v3_name), &full_v3).unwrap();
        std::fs::write(store_root.join(&delta_v3_name), &delta_v3).unwrap();

        std::fs::write(app_scoped_store.join(&full_v2_name), &full_v2_good).unwrap();
        std::fs::write(app_scoped_store.join(&full_v3_name), &full_v3).unwrap();
        std::fs::write(app_scoped_store.join(&delta_v3_name), &delta_v3).unwrap();

        let make_index = |full_v2: &[u8]| ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![
                ReleaseEntry {
                    version: "1.1.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: true,
                    full_filename: full_v2_name.clone(),
                    full_size: full_v2.len() as i64,
                    full_sha256: sha256_hex(full_v2),
                    deltas: Vec::new(),
                    preferred_delta_id: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
                ReleaseEntry {
                    version: "1.2.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v3_name.clone(),
                    full_size: full_v3.len() as i64,
                    full_sha256: sha256_hex(&full_v3),
                    deltas: vec![DeltaArtifact::sparse_file_ops_zstd(
                        "primary",
                        "1.1.0",
                        &delta_v3_name,
                        delta_v3.len() as i64,
                        &sha256_hex(&delta_v3),
                    )],
                    preferred_delta_id: "primary".to_string(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    name: String::new(),
                    main_exe: app_id.to_string(),
                    install_directory: app_id.to_string(),
                    supervisor_id: String::new(),
                    icon: String::new(),
                    shortcuts: Vec::new(),
                    persistent_assets: Vec::new(),
                    installers: Vec::new(),
                    environment: std::collections::BTreeMap::new(),
                },
            ],
            ..ReleaseIndex::default()
        };

        let root_compressed = compress_release_index(&make_index(&full_v2_bad), DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), root_compressed).unwrap();
        let scoped_compressed = compress_release_index(&make_index(&full_v2_good), DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(app_scoped_store.join(RELEASES_FILE_COMPRESSED), scoped_compressed).unwrap();

        let active_app_dir = install_root.join("app");
        std::fs::create_dir_all(active_app_dir.join(".surge")).unwrap();
        std::fs::write(active_app_dir.join("payload.txt"), "v2 payload").unwrap();
        std::fs::write(
            active_app_dir.join("camera-tuner.deps.json"),
            "{\"deps\":\"good-v2\"}\n",
        )
        .unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH),
            format!("id: {app_id}\nversion: 1.1.0\n"),
        )
        .unwrap();
        std::fs::write(
            active_app_dir.join(crate::install::LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH),
            format!("id: {app_id}\nversion: 1.1.0\n"),
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager =
            UpdateManager::new(ctx.clone(), app_id, "1.1.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(ctx.storage_config().prefix, app_id);
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let installed_payload = std::fs::read_to_string(install_root.join("app").join("payload.txt")).unwrap();
        assert_eq!(installed_payload, "v3 payload");
        let installed_deps = std::fs::read_to_string(install_root.join("app").join("camera-tuner.deps.json")).unwrap();
        assert_eq!(installed_deps, "{\"deps\":\"v3\"}\n");
    }

    #[tokio::test]
    async fn test_download_and_apply_moves_previous_active_into_version_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let current_app_dir = install_root.join("app");
        std::fs::create_dir_all(&current_app_dir).unwrap();
        std::fs::write(current_app_dir.join("payload.txt"), "old payload").unwrap();

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"new payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: app_id.to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let current_payload = std::fs::read_to_string(install_root.join("app").join("payload.txt")).unwrap();
        assert_eq!(current_payload, "new payload");

        let previous_payload = std::fs::read_to_string(install_root.join("app-1.0.0").join("payload.txt")).unwrap();
        assert_eq!(previous_payload, "old payload");

        assert!(!install_root.join(".surge-app-next").exists());
        assert!(!install_root.join(".surge-app-prev").exists());
    }

    #[tokio::test]
    async fn test_download_and_apply_prunes_old_version_snapshots_to_retention_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let current_app_dir = install_root.join("app");
        std::fs::create_dir_all(&current_app_dir).unwrap();
        std::fs::write(current_app_dir.join("payload.txt"), "old payload").unwrap();
        std::fs::create_dir_all(install_root.join("app-0.9.0")).unwrap();
        std::fs::create_dir_all(install_root.join("app-0.8.0")).unwrap();
        std::fs::create_dir_all(install_root.join("app-backup")).unwrap();

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"new payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: app_id.to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        manager.set_release_retention_limit(1);

        let info = manager.check_for_updates().await.unwrap().unwrap();
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        assert!(install_root.join("app").is_dir());
        assert!(install_root.join("app-1.0.0").is_dir());
        assert!(!install_root.join("app-0.9.0").exists());
        assert!(!install_root.join("app-0.8.0").exists());
        assert!(install_root.join("app-backup").is_dir());
    }

    #[tokio::test]
    async fn test_download_and_apply_with_zero_retention_removes_version_snapshots() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let current_app_dir = install_root.join("app");
        std::fs::create_dir_all(&current_app_dir).unwrap();
        std::fs::write(current_app_dir.join("payload.txt"), "old payload").unwrap();
        std::fs::create_dir_all(install_root.join("app-0.9.0")).unwrap();

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"new payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: app_id.to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        manager.set_release_retention_limit(0);

        let info = manager.check_for_updates().await.unwrap().unwrap();
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        assert!(install_root.join("app").is_dir());
        assert!(!install_root.join("app-1.0.0").exists());
        assert!(!install_root.join("app-0.9.0").exists());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_download_and_apply_full_installs_shortcuts() {
        let _shortcut_env_lock = lock_test_shortcut_environment_async().await;
        let tmp = tempfile::tempdir().unwrap();
        let applications_dir = tmp
            .path()
            .join("shortcut-home")
            .join(".local")
            .join("share")
            .join("applications");
        let autostart_dir = tmp.path().join("shortcut-home").join(".config").join("autostart");
        set_test_shortcut_paths_override(applications_dir.clone(), autostart_dir.clone());
        let _override_guard = ShortcutPathsOverrideGuard;

        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        let app_id = "test-app";
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();
        let app_store = app_scoped_store_root(&store_root, app_id);

        let rid = current_rid();
        let full_filename = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let full_path = app_store.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("demoapp", b"#!/bin/sh\necho demo\n", 0o755).unwrap();
        packer.add_buffer("icon.png", b"png", 0o644).unwrap();
        packer.add_buffer("payload.txt", b"installed payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: "linux".to_string(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: "demoapp".to_string(),
                install_directory: app_id.to_string(),
                supervisor_id: String::new(),
                icon: "icon.png".to_string(),
                shortcuts: vec![ShortcutLocation::Desktop, ShortcutLocation::Startup],
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        write_app_scoped_release_index(&store_root, app_id, &index);

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, app_id, "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);
        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let installed_file = install_root.join("app").join("payload.txt");
        assert!(installed_file.exists());

        // Linux desktop matching follows the executable name / window class, not the scoped app id.
        let desktop_file = applications_dir.join("demoapp.desktop");
        let startup_file = autostart_dir.join("demoapp.desktop");
        assert!(desktop_file.exists());
        assert!(startup_file.exists());

        let desktop_content = std::fs::read_to_string(desktop_file).unwrap();
        assert!(desktop_content.contains("Icon="));
        assert!(desktop_content.contains("Name=demoapp"));
        assert!(desktop_content.contains("StartupWMClass=demoapp"));
        let stable_exe_path = install_root.join("app").join("demoapp");
        assert!(desktop_content.contains(stable_exe_path.to_string_lossy().as_ref()));
    }
}
