//! Update manager: check for updates, download, verify, and apply them.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::context::Context;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{
    ReleaseEntry, ReleaseIndex, decompress_release_index, get_delta_chain, get_releases_newer_than,
};
use crate::releases::version::compare_versions;
use crate::storage::{StorageBackend, create_storage_backend};
use crate::config::constants::RELEASES_FILE_COMPRESSED;

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
            // Restore the current full archive (direct or reconstructed from
            // earlier full + deltas), then apply the downloaded delta chain.
            let index = if let Some(cached) = &self.cached_index {
                cached.clone()
            } else {
                let data = self.storage.get_object(RELEASES_FILE_COMPRESSED).await?;
                decompress_release_index(&data)?
            };
            let rid = crate::platform::detect::current_rid();
            let mut rebuilt_archive = crate::releases::restore::restore_full_archive_for_version(
                self.storage.as_ref(),
                &index,
                &rid,
                &self.current_version,
            )
            .await
            .map_err(|e| {
                SurgeError::Update(format!(
                    "Failed to restore base full archive for {}: {e}",
                    self.current_version
                ))
            })?;

            for release in &info.apply_releases {
                self.ctx.check_cancelled()?;

                if release.delta_filename.is_empty() {
                    return Err(SurgeError::Update(format!(
                        "Delta update path is missing delta filename for {}",
                        release.version
                    )));
                }

                let delta_path = staging_dir.join(&release.delta_filename);
                let delta_compressed = tokio::fs::read(&delta_path).await?;
                let patch = zstd::decode_all(delta_compressed.as_slice()).map_err(|e| {
                    SurgeError::Archive(format!("Failed to decompress delta {}: {e}", release.delta_filename))
                })?;
                rebuilt_archive = crate::diff::wrapper::bspatch_buffers(&rebuilt_archive, &patch).map_err(|e| {
                    SurgeError::Update(format!("Failed to apply delta {}: {e}", release.delta_filename))
                })?;

                if !release.full_sha256.is_empty() {
                    let hash = crate::crypto::sha256::sha256_hex(&rebuilt_archive);
                    if hash != release.full_sha256 {
                        return Err(SurgeError::Update(format!(
                            "SHA-256 mismatch for rebuilt full archive {}: expected {}, got {hash}",
                            release.version, release.full_sha256
                        )));
                    }
                }
            }

            let rebuilt_archive_path = staging_dir.join("rebuilt-full.tar.zst");
            tokio::fs::write(&rebuilt_archive_path, &rebuilt_archive).await?;
            crate::archive::extractor::extract_file_to(&rebuilt_archive_path, &extract_dir)?;
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
            debug!("Delta chain was restored and applied during extraction");
        }

        report(5, 100, 85);

        // Phase 6: Finalize
        report(6, 0, 90);
        let latest = info
            .apply_releases
            .last()
            .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
        let active_app_dir = self.install_dir.join("app");
        let next_app_dir = self.install_dir.join(".surge-app-next");
        let previous_swap_dir = self.install_dir.join(".surge-app-prev");

        request_supervisor_shutdown(&self.install_dir, &latest.supervisor_id).await?;

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
            find_previous_app_dir(&self.install_dir, &self.current_version)
        };

        let extracted_final_dir = if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            let source = extract_dir.join(&info.latest_version);
            if source.exists() { source } else { extract_dir.clone() }
        } else {
            extract_dir.clone()
        };
        crate::platform::fs::atomic_rename(&extracted_final_dir, &next_app_dir)?;

        if active_app_dir.is_dir() {
            crate::platform::fs::atomic_rename(&active_app_dir, &previous_swap_dir)?;
        }
        if let Err(err) = crate::platform::fs::atomic_rename(&next_app_dir, &active_app_dir) {
            // Best effort rollback to previous active content.
            if previous_swap_dir.is_dir() && !active_app_dir.exists() {
                let _ = crate::platform::fs::atomic_rename(&previous_swap_dir, &active_app_dir);
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

        if !latest.shortcuts.is_empty() {
            match crate::platform::shortcuts::install_shortcuts(
                &self.app_id,
                &active_app_dir,
                &latest.main_exe,
                &latest.icon,
                &latest.shortcuts,
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
                if let Err(e) = crate::platform::fs::atomic_rename(&previous_swap_dir, &previous_version_dir) {
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

fn find_previous_app_dir(install_dir: &Path, current_version: &str) -> Option<PathBuf> {
    let active = install_dir.join("app");
    if active.is_dir() {
        return Some(active);
    }

    let explicit = install_dir.join(format!("app-{current_version}"));
    if explicit.is_dir() {
        return Some(explicit);
    }

    crate::supervisor::stub::find_latest_app_dir(install_dir).ok()
}

async fn request_supervisor_shutdown(install_dir: &Path, supervisor_id: &str) -> Result<()> {
    request_supervisor_shutdown_with_timeout(
        install_dir,
        supervisor_id,
        Duration::from_secs(20),
        Duration::from_millis(100),
    )
    .await
}

async fn request_supervisor_shutdown_with_timeout(
    install_dir: &Path,
    supervisor_id: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<()> {
    let supervisor_id = supervisor_id.trim();
    if supervisor_id.is_empty() {
        return Ok(());
    }

    let pid_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.pid"));
    if !pid_file.is_file() {
        return Ok(());
    }

    let stop_file = install_dir.join(format!(".surge-supervisor-{supervisor_id}.stop"));
    tokio::fs::write(&stop_file, b"surge-update").await?;

    let deadline = tokio::time::Instant::now() + timeout;
    while pid_file.exists() {
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "Timed out waiting for supervisor '{supervisor_id}' to stop before applying update"
            )));
        }
        tokio::time::sleep(poll_interval).await;
    }

    let _ = tokio::fs::remove_file(&stop_file).await;
    Ok(())
}

fn copy_persistent_assets(previous_app_dir: &Path, new_app_dir: &Path, assets: &[String]) -> Result<()> {
    for asset in assets {
        let relative = validate_relative_persistent_asset_path(asset)?;
        let source = previous_app_dir.join(&relative);
        if !source.exists() {
            continue;
        }

        let destination = new_app_dir.join(&relative);
        if source.is_dir() {
            if destination.exists() {
                if destination.is_dir() {
                    std::fs::remove_dir_all(&destination)?;
                } else {
                    std::fs::remove_file(&destination)?;
                }
            }
            crate::platform::fs::copy_directory(&source, &destination)?;
        } else {
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if destination.exists() && destination.is_dir() {
                std::fs::remove_dir_all(&destination)?;
            }
            std::fs::copy(&source, &destination)?;
        }
    }

    Ok(())
}

fn validate_relative_persistent_asset_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        return Err(SurgeError::Update("Persistent asset path cannot be empty".to_string()));
    }

    let candidate = Path::new(raw);
    if candidate.is_absolute() {
        return Err(SurgeError::Update(format!(
            "Persistent asset path must be relative: {raw}"
        )));
    }

    let first_component = candidate
        .components()
        .next()
        .and_then(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .unwrap_or_default();
    if first_component.to_ascii_lowercase().starts_with("app-") {
        return Err(SurgeError::Update(format!(
            "Persistent asset path cannot start with 'app-': {raw}"
        )));
    }

    for component in candidate.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            return Err(SurgeError::Update(format!(
                "Persistent asset path cannot contain parent/root traversal: {raw}"
            )));
        }
    }

    Ok(candidate.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::config::constants::DEFAULT_ZSTD_LEVEL;
    #[cfg(target_os = "linux")]
    use crate::config::manifest::ShortcutLocation;
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
        let rid = crate::platform::detect::current_rid();
        let raw = rid.split('-').next().unwrap_or_default();
        normalize_os_label(raw)
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
    fn test_update_manager_rejects_embedded_credentials() {
        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            crate::context::StorageProvider::S3,
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
            crate::context::StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, "app", "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();
        assert_eq!(manager.channel(), "stable");
        assert_eq!(manager.current_version(), "1.0.0");

        manager.set_channel("test").unwrap();
        manager.set_current_version("1.1.0").unwrap();
        assert_eq!(manager.channel(), "test");
        assert_eq!(manager.current_version(), "1.1.0");

        let err = manager.set_channel("  ").unwrap_err();
        assert!(err.to_string().contains("channel cannot be empty"));
        let err = manager.set_current_version("").unwrap_err();
        assert!(err.to_string().contains("Current version cannot be empty"));
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
    async fn test_request_supervisor_shutdown_noop_when_supervisor_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        request_supervisor_shutdown(tmp.path(), "").await.unwrap();
        request_supervisor_shutdown(tmp.path(), "missing").await.unwrap();
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

        request_supervisor_shutdown_with_timeout(
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

        let err = request_supervisor_shutdown_with_timeout(
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
        std::fs::create_dir_all(&store_root).unwrap();

        let index = ReleaseIndex {
            app_id: "other-app".to_string(),
            releases: vec![make_entry("1.1.0", "stable", "", "")],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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

        let mut release = make_entry(
            "1.1.0",
            "stable",
            &current_os_label_for_tests(),
            &crate::platform::detect::current_rid(),
        );
        release.is_genesis = true;
        release.delta_filename.clear();
        release.delta_size = 0;
        release.delta_sha256.clear();

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
    async fn test_check_for_updates_after_channel_switch() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();
        let rid = crate::platform::detect::current_rid();
        let os = current_os_label_for_tests();

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![
                make_entry("1.1.0", "stable", &os, &rid),
                make_entry("1.2.0", "test", &os, &rid),
            ],
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
                os: current_os_label_for_tests(),
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
                main_exe: "test-app".to_string(),
                install_directory: "test-app".to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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

        let installed_file = install_root.join("app").join("payload.txt");
        assert!(installed_file.exists());
        assert_eq!(std::fs::read_to_string(installed_file).unwrap(), "installed payload");
    }

    #[tokio::test]
    async fn test_download_and_apply_delta_restores_missing_base_full() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let rid = crate::platform::detect::current_rid();
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

        let patch_v2 = crate::diff::wrapper::bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let patch_v3 = crate::diff::wrapper::bsdiff_buffers(&full_v2, &full_v3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let full_v1_name = format!("test-app-1.0.0-{rid}-full.tar.zst");
        let full_v2_name = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_v3_name = format!("test-app-1.2.0-{rid}-full.tar.zst");
        let delta_v2_name = format!("test-app-1.1.0-{rid}-delta.tar.zst");
        let delta_v3_name = format!("test-app-1.2.0-{rid}-delta.tar.zst");

        std::fs::write(store_root.join(&full_v1_name), &full_v1).unwrap();
        std::fs::write(store_root.join(&delta_v2_name), &delta_v2).unwrap();
        std::fs::write(store_root.join(&delta_v3_name), &delta_v3).unwrap();

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![
                ReleaseEntry {
                    version: "1.0.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: os.clone(),
                    rid: rid.clone(),
                    is_genesis: true,
                    full_filename: full_v1_name.clone(),
                    full_size: full_v1.len() as i64,
                    full_sha256: crate::crypto::sha256::sha256_hex(&full_v1),
                    delta_filename: String::new(),
                    delta_size: 0,
                    delta_sha256: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    main_exe: "test-app".to_string(),
                    install_directory: "test-app".to_string(),
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
                    full_sha256: crate::crypto::sha256::sha256_hex(&full_v2),
                    delta_filename: delta_v2_name.clone(),
                    delta_size: delta_v2.len() as i64,
                    delta_sha256: crate::crypto::sha256::sha256_hex(&delta_v2),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    main_exe: "test-app".to_string(),
                    install_directory: "test-app".to_string(),
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
                    full_filename: full_v3_name,
                    full_size: full_v3.len() as i64,
                    full_sha256: crate::crypto::sha256::sha256_hex(&full_v3),
                    delta_filename: delta_v3_name,
                    delta_size: delta_v3.len() as i64,
                    delta_sha256: crate::crypto::sha256::sha256_hex(&delta_v3),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
                    main_exe: "test-app".to_string(),
                    install_directory: "test-app".to_string(),
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

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
            UpdateManager::new(ctx, "test-app", "1.1.0", "stable", install_root.to_str().unwrap()).unwrap();
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
    async fn test_download_and_apply_moves_previous_active_into_version_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let current_app_dir = install_root.join("app");
        std::fs::create_dir_all(&current_app_dir).unwrap();
        std::fs::write(current_app_dir.join("payload.txt"), "old payload").unwrap();

        let rid = crate::platform::detect::current_rid();
        let full_filename = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_path = store_root.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"new payload", 0o644).unwrap();
        packer.finalize_to_file(&full_path).unwrap();

        let full_size = std::fs::metadata(&full_path).unwrap().len() as i64;
        let full_sha256 = sha256_hex_file(&full_path).unwrap();

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![ReleaseEntry {
                version: "1.1.0".to_string(),
                channels: vec!["stable".to_string()],
                os: current_os_label_for_tests(),
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
                main_exe: "test-app".to_string(),
                install_directory: "test-app".to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_download_and_apply_full_installs_shortcuts() {
        struct ShortcutPathsOverrideGuard;

        impl Drop for ShortcutPathsOverrideGuard {
            fn drop(&mut self) {
                crate::platform::shortcuts::clear_test_shortcut_paths_override();
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        let applications_dir = tmp
            .path()
            .join("shortcut-home")
            .join(".local")
            .join("share")
            .join("applications");
        let autostart_dir = tmp.path().join("shortcut-home").join(".config").join("autostart");
        crate::platform::shortcuts::set_test_shortcut_paths_override(applications_dir.clone(), autostart_dir.clone());
        let _override_guard = ShortcutPathsOverrideGuard;

        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let rid = crate::platform::detect::current_rid();
        let full_filename = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_path = store_root.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("demoapp", b"#!/bin/sh\necho demo\n", 0o755).unwrap();
        packer.add_buffer("icon.png", b"png", 0o644).unwrap();
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
                main_exe: "demoapp".to_string(),
                install_directory: "test-app".to_string(),
                supervisor_id: String::new(),
                icon: "icon.png".to_string(),
                shortcuts: vec![ShortcutLocation::Desktop, ShortcutLocation::Startup],
                persistent_assets: Vec::new(),
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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

        let installed_file = install_root.join("app").join("payload.txt");
        assert!(installed_file.exists());

        let desktop_file = applications_dir.join("test-app.desktop");
        let startup_file = autostart_dir.join("test-app.desktop");
        assert!(desktop_file.exists());
        assert!(startup_file.exists());

        let desktop_content = std::fs::read_to_string(desktop_file).unwrap();
        assert!(desktop_content.contains("Icon="));
        let stable_exe_path = install_root.join("app").join("demoapp");
        assert!(desktop_content.contains(stable_exe_path.to_string_lossy().as_ref()));
    }
}
