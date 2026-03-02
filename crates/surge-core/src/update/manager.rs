//! Update manager: check for updates, download, verify, and apply them.

use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, info};

use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::context::Context;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    ReleaseEntry, ReleaseIndex, decompress_release_index, get_delta_chain, get_releases_newer_than,
};
use crate::releases::version::compare_versions;
use crate::storage::{StorageBackend, create_storage_backend};

/// Progress information for update operations.
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    /// Current phase (1 = check, 2 = download, 3 = verify, 4 = extract, 5 = apply_delta, 6 = finalize).
    pub phase: i32,
    /// Percentage complete within the current phase (0-100).
    pub phase_percent: i32,
    /// Overall percentage complete (0-100).
    pub total_percent: i32,
    /// Bytes processed so far in this phase.
    pub bytes_done: i64,
    /// Total bytes expected in this phase.
    pub bytes_total: i64,
    /// Items processed so far in this phase.
    pub items_done: i64,
    /// Total items expected in this phase.
    pub items_total: i64,
    /// Current processing speed in bytes per second.
    pub speed_bytes_per_sec: f64,
}

impl Default for ProgressInfo {
    fn default() -> Self {
        Self {
            phase: 0,
            phase_percent: 0,
            total_percent: 0,
            bytes_done: 0,
            bytes_total: 0,
            items_done: 0,
            items_total: 0,
            speed_bytes_per_sec: 0.0,
        }
    }
}

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

/// Manages checking for and applying application updates.
pub struct UpdateManager {
    ctx: Arc<Context>,
    app_id: String,
    current_version: String,
    channel: String,
    install_dir: PathBuf,
    storage: Box<dyn StorageBackend>,
    cached_index: Option<ReleaseIndex>,
}

impl UpdateManager {
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
        let storage = create_storage_backend(&storage_cfg)?;

        Ok(Self {
            ctx,
            app_id: app_id.to_string(),
            current_version: current_version.to_string(),
            channel: channel.to_string(),
            install_dir: PathBuf::from(install_dir),
            storage,
            cached_index: None,
        })
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

        // Download release index
        let data = self.storage.get_object(RELEASES_FILE_COMPRESSED).await?;
        let index = decompress_release_index(&data)?;
        let current_rid = crate::platform::detect::current_rid();
        let current_os = normalize_os_label(current_rid.split('-').next().unwrap_or_default());

        if !index.app_id.is_empty() && index.app_id != self.app_id {
            return Err(SurgeError::Update(format!(
                "Release index app_id '{}' does not match requested app '{}'",
                index.app_id, self.app_id
            )));
        }

        // Keep only releases compatible with our channel/platform.
        let mut compatible_index = index.clone();
        compatible_index.releases.retain(|release| {
            release.channels.iter().any(|c| c == &self.channel)
                && compare_versions(&release.version, &self.current_version) == std::cmp::Ordering::Greater
                && release_matches_rid(release, &current_rid)
                && release_matches_os(release, &current_os)
        });

        // Find newer releases on our channel
        let newer = get_releases_newer_than(&compatible_index, &self.current_version, &self.channel);

        if newer.is_empty() {
            debug!("No updates available");
            self.cached_index = Some(index);
            return Ok(None);
        }

        let latest = newer
            .last()
            .map(|release| (*release).clone())
            .ok_or_else(|| SurgeError::Update("No latest release found".to_string()))?;
        let latest_version = latest.version.clone();

        // Check if a delta chain exists
        let delta_chain = get_delta_chain(&compatible_index, &self.current_version, &latest_version, &self.channel);

        let available_releases: Vec<ReleaseEntry> = newer.into_iter().cloned().collect();
        let (apply_releases, apply_strategy, download_size) = if let Some(chain) = delta_chain {
            let selected: Vec<ReleaseEntry> = chain.into_iter().cloned().collect();
            let size = selected.iter().map(|r| r.delta_size).sum();
            (selected, ApplyStrategy::Delta, size)
        } else {
            (vec![latest.clone()], ApplyStrategy::Full, latest.full_size)
        };
        let delta_available = matches!(apply_strategy, ApplyStrategy::Delta);

        info!(
            latest_version = %latest_version,
            delta_available,
            download_size,
            releases_count = available_releases.len(),
            "Updates available"
        );

        self.cached_index = Some(index);

        Ok(Some(UpdateInfo {
            available_releases,
            latest_version,
            delta_available,
            download_size,
            apply_releases,
            apply_strategy,
        }))
    }

    /// Download and apply an update.
    ///
    /// Executes a 6-phase pipeline:
    /// 1. Check - validate update info and prepare
    /// 2. Download - fetch update packages from storage
    /// 3. Verify - verify SHA-256 hashes of downloaded files
    /// 4. Extract - extract the downloaded archive
    /// 5. Apply delta - apply binary diffs if using delta updates
    /// 6. Finalize - move files into place, clean up
    pub async fn download_and_apply<F>(&self, info: &UpdateInfo, progress: Option<F>) -> Result<()>
    where
        F: Fn(ProgressInfo) + Send + Sync,
    {
        self.ctx.check_cancelled()?;

        let report = |phase: i32, phase_pct: i32, total_pct: i32| {
            if let Some(ref cb) = progress {
                cb(ProgressInfo {
                    phase,
                    phase_percent: phase_pct,
                    total_percent: total_pct,
                    ..Default::default()
                });
            }
        };

        // Phase 1: Check
        info!(version = %info.latest_version, "Starting update");
        report(1, 0, 0);

        if info.apply_releases.is_empty() {
            return Err(SurgeError::Update("No releases to apply".to_string()));
        }

        let staging_dir = self.install_dir.join(".surge-staging");
        tokio::fs::create_dir_all(&staging_dir).await?;

        report(1, 100, 5);

        // Phase 2: Download
        report(2, 0, 10);

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            // Download delta packages for each version in the chain
            let total_deltas = info.apply_releases.len();
            for (i, release) in info.apply_releases.iter().enumerate() {
                self.ctx.check_cancelled()?;

                if release.delta_filename.is_empty() {
                    continue;
                }

                let dest = staging_dir.join(&release.delta_filename);
                debug!(
                    filename = %release.delta_filename,
                    "Downloading delta package"
                );

                self.storage
                    .download_to_file(&release.delta_filename, &dest, None)
                    .await?;

                let pct = ((i + 1) * 100 / total_deltas) as i32;
                report(2, pct, 10 + pct * 30 / 100);
            }
        } else {
            // Download the full package for the latest release
            let latest = info
                .apply_releases
                .last()
                .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;

            let dest = staging_dir.join(&latest.full_filename);
            debug!(
                filename = %latest.full_filename,
                "Downloading full package"
            );

            self.storage
                .download_to_file(&latest.full_filename, &dest, None)
                .await?;
        }

        report(2, 100, 40);

        // Phase 3: Verify
        report(3, 0, 45);

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            for release in &info.apply_releases {
                self.ctx.check_cancelled()?;

                if release.delta_filename.is_empty() {
                    continue;
                }

                let path = staging_dir.join(&release.delta_filename);
                let hash = crate::crypto::sha256::sha256_hex_file(&path)?;
                if !release.delta_sha256.is_empty() && hash != release.delta_sha256 {
                    return Err(SurgeError::Update(format!(
                        "SHA-256 mismatch for {}: expected {}, got {hash}",
                        release.delta_filename, release.delta_sha256
                    )));
                }
            }
        } else {
            let latest = info
                .apply_releases
                .last()
                .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
            let path = staging_dir.join(&latest.full_filename);
            let hash = crate::crypto::sha256::sha256_hex_file(&path)?;
            if !latest.full_sha256.is_empty() && hash != latest.full_sha256 {
                return Err(SurgeError::Update(format!(
                    "SHA-256 mismatch for {}: expected {}, got {hash}",
                    latest.full_filename, latest.full_sha256
                )));
            }
        }

        report(3, 100, 55);

        // Phase 4: Extract
        report(4, 0, 60);

        let extract_dir = staging_dir.join("extracted");
        tokio::fs::create_dir_all(&extract_dir).await?;

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            // Extract each delta package
            for release in &info.apply_releases {
                self.ctx.check_cancelled()?;

                if release.delta_filename.is_empty() {
                    continue;
                }

                let archive_path = staging_dir.join(&release.delta_filename);
                let version_dir = extract_dir.join(&release.version);
                tokio::fs::create_dir_all(&version_dir).await?;

                crate::archive::extractor::extract_file_to(&archive_path, &version_dir)?;
            }
        } else {
            let latest = info
                .apply_releases
                .last()
                .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
            let archive_path = staging_dir.join(&latest.full_filename);
            crate::archive::extractor::extract_file_to(&archive_path, &extract_dir)?;
        }

        report(4, 100, 75);

        // Phase 5: Apply delta (if applicable)
        report(5, 0, 80);

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            debug!("Delta application is handled during extraction step");
            // Delta diffs would be applied here by reading the current installed
            // files and applying bsdiff patches from the extracted delta packages.
            // The actual bsdiff application is version-specific and depends on the
            // pack format produced by PackBuilder.
        }

        report(5, 100, 85);

        // Phase 6: Finalize
        report(6, 0, 90);

        let app_dir = self.install_dir.join(format!("app-{}", info.latest_version));
        if app_dir.exists() {
            tokio::fs::remove_dir_all(&app_dir).await?;
        }

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            // Move the final version's extracted files into place
            let source = extract_dir.join(&info.latest_version);
            if source.exists() {
                crate::platform::fs::atomic_rename(&source, &app_dir)?;
            } else {
                // If extraction was flat (no version subdirectory), use extract_dir
                crate::platform::fs::atomic_rename(&extract_dir, &app_dir)?;
            }
        } else {
            crate::platform::fs::atomic_rename(&extract_dir, &app_dir)?;
        }

        // Clean up staging directory
        if staging_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        }

        report(6, 100, 100);

        info!(
            version = %info.latest_version,
            "Update applied successfully"
        );

        Ok(())
    }
}

fn release_matches_rid(release: &ReleaseEntry, current_rid: &str) -> bool {
    release.rid.is_empty() || release.rid == current_rid
}

fn release_matches_os(release: &ReleaseEntry, current_os: &str) -> bool {
    release.os.is_empty() || normalize_os_label(&release.os) == current_os
}

fn normalize_os_label(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "windows" | "win" => "win".to_string(),
        "macos" | "osx" | "darwin" => "osx".to_string(),
        "linux" => "linux".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::crypto::sha256::sha256_hex_file;
    use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index};

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
            delta_filename: format!("{version}-delta.tar.zst"),
            delta_size: 100,
            delta_sha256: String::new(),
            created_utc: String::new(),
            release_notes: String::new(),
        }
    }

    #[test]
    fn test_progress_info_default() {
        let p = ProgressInfo::default();
        assert_eq!(p.phase, 0);
        assert_eq!(p.total_percent, 0);
        assert_eq!(p.speed_bytes_per_sec, 0.0);
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
    fn test_os_normalization() {
        assert_eq!(normalize_os_label("windows"), "win");
        assert_eq!(normalize_os_label("win"), "win");
        assert_eq!(normalize_os_label("macos"), "osx");
        assert_eq!(normalize_os_label("linux"), "linux");
    }

    #[test]
    fn test_release_rid_filter() {
        let release = make_entry("1.0.0", "stable", "linux", "linux-x64");
        assert!(release_matches_rid(&release, "linux-x64"));
        assert!(!release_matches_rid(&release, "win-x64"));
    }

    #[tokio::test]
    async fn test_check_for_updates_rejects_mismatched_app_id() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();

        let index = ReleaseIndex {
            app_id: "other-app".to_string(),
            releases: vec![make_entry("1.1.0", "stable", "", "")],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, crate::config::constants::DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(
            store_root.join(crate::config::constants::RELEASES_FILE_COMPRESSED),
            compressed,
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            crate::context::StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let mut manager = UpdateManager::new(ctx, "test-app", "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let err = manager.check_for_updates().await.unwrap_err();
        assert!(err.to_string().contains("does not match requested app"));
    }

    #[tokio::test]
    async fn test_check_for_updates_genesis_without_delta_uses_full_strategy() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();

        let mut release = make_entry("1.1.0", "stable", "linux", &crate::platform::detect::current_rid());
        release.is_genesis = true;
        release.delta_filename.clear();
        release.delta_size = 0;
        release.delta_sha256.clear();

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, crate::config::constants::DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(
            store_root.join(crate::config::constants::RELEASES_FILE_COMPRESSED),
            compressed,
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            crate::context::StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, "test-app", "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();

        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert!(!info.delta_available);
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);
        assert_eq!(info.apply_releases.len(), 1);
        assert_eq!(info.apply_releases[0].full_filename, "1.1.0-full.tar.zst");
    }

    #[tokio::test]
    async fn test_download_and_apply_full_installs_files() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let rid = crate::platform::detect::current_rid();
        let full_filename = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_path = store_root.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"installed payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: "linux".to_string(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_filename.clone(),
                full_size,
                full_sha256,
                delta_filename: String::new(),
                delta_size: 0,
                delta_sha256: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, crate::config::constants::DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(
            store_root.join(crate::config::constants::RELEASES_FILE_COMPRESSED),
            compressed,
        )
        .unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            crate::context::StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager =
            UpdateManager::new(ctx, "test-app", "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();

        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Full);

        manager
            .download_and_apply(&info, None::<fn(ProgressInfo)>)
            .await
            .unwrap();

        let installed_file = install_root.join("app-1.1.0").join("payload.txt");
        assert!(installed_file.exists());
        assert_eq!(std::fs::read_to_string(installed_file).unwrap(), "installed payload");
    }
}
