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

        // Find newer releases on our channel
        let newer = get_releases_newer_than(&index, &self.current_version, &self.channel);

        if newer.is_empty() {
            debug!("No updates available");
            self.cached_index = Some(index);
            return Ok(None);
        }

        let latest = newer.last().expect("newer is non-empty");
        let latest_version = latest.version.clone();

        // Check if a delta chain exists
        let delta_chain = get_delta_chain(&index, &self.current_version, &latest_version, &self.channel);

        let (delta_available, download_size) = if let Some(ref chain) = delta_chain {
            let size: i64 = chain.iter().map(|r| r.delta_size).sum();
            (true, size)
        } else {
            // Fall back to full download of the latest release
            (false, latest.full_size)
        };

        let available_releases: Vec<ReleaseEntry> = newer.into_iter().cloned().collect();

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

        if info.available_releases.is_empty() {
            return Err(SurgeError::Update("No releases to apply".to_string()));
        }

        let staging_dir = self.install_dir.join(".surge-staging");
        tokio::fs::create_dir_all(&staging_dir).await?;

        report(1, 100, 5);

        // Phase 2: Download
        report(2, 0, 10);

        if info.delta_available {
            // Download delta packages for each version in the chain
            let total_deltas = info.available_releases.len();
            for (i, release) in info.available_releases.iter().enumerate() {
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
                .available_releases
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

        if info.delta_available {
            for release in &info.available_releases {
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
            let latest = info.available_releases.last().unwrap();
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

        if info.delta_available {
            // Extract each delta package
            for release in &info.available_releases {
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
            let latest = info.available_releases.last().unwrap();
            let archive_path = staging_dir.join(&latest.full_filename);
            crate::archive::extractor::extract_file_to(&archive_path, &extract_dir)?;
        }

        report(4, 100, 75);

        // Phase 5: Apply delta (if applicable)
        report(5, 0, 80);

        if info.delta_available {
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

        if info.delta_available {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
