//! Update manager: check for updates, download, verify, and apply them.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use crate::archive::extractor::extract_file_to_with_progress;
use crate::config::constants::RELEASES_FILE_COMPRESSED;
use crate::context::Context;
use crate::crypto::sha256::{sha256_hex, sha256_hex_file};
use crate::error::{Result, SurgeError};
use crate::install::{InstallProfile, RuntimeManifestMetadata, storage_provider_manifest_name, write_runtime_manifest};
use crate::platform::detect::current_rid;
use crate::platform::fs::{atomic_rename, copy_directory};
use crate::platform::process::{current_pid, spawn_detached, spawn_process, supervisor_binary_name};
use crate::platform::shortcuts::install_shortcuts;
use crate::releases::artifact_cache::{
    CacheFetchOutcome, cache_path_for_key, fetch_or_reuse_file, prune_cached_artifacts,
};
use crate::releases::delta::{apply_delta_patch, decode_delta_patch, is_supported_delta};
use crate::releases::manifest::{
    ReleaseEntry, ReleaseIndex, decompress_release_index, get_delta_chain, get_releases_newer_than,
};
use crate::releases::restore::{
    RestoreOptions, local_checkpoint_artifacts_for_index, required_artifacts_for_index,
    restore_full_archive_for_version_with_options,
};
use crate::releases::version::compare_versions;
use crate::storage::{StorageBackend, create_storage_backend};
use crate::storage_config::append_prefix;
use crate::supervisor::state::{read_restart_args, supervisor_pid_file, supervisor_stop_file};
use crate::supervisor::stub::find_latest_app_dir;

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

fn emit_progress<F>(progress: Option<&Arc<F>>, progress_info: ProgressInfo)
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    if let Some(cb) = progress {
        cb(progress_info);
    }
}

fn clamp_progress_percent(done: i64, total: i64) -> i32 {
    if total > 0 {
        ((done.saturating_mul(100)) / total).clamp(0, 100) as i32
    } else {
        0
    }
}

fn clamp_progress_percent_u64(done: u64, total: u64) -> i32 {
    if total > 0 {
        ((done.saturating_mul(100)) / total).clamp(0, 100) as i32
    } else {
        0
    }
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn phase_total_percent(phase_start: i32, phase_span: i32, phase_percent: i32) -> i32 {
    phase_start + phase_percent.clamp(0, 100) * phase_span / 100
}

#[allow(clippy::cast_precision_loss)]
fn average_speed_bytes_per_sec(bytes_done: u64, started_at: Instant) -> f64 {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed > 0.0 {
        bytes_done as f64 / elapsed
    } else {
        0.0
    }
}

#[derive(Debug, Clone)]
struct ArtifactDownload {
    key: String,
    sha256: String,
    size: i64,
}

#[derive(Debug)]
struct DownloadProgressState {
    started_at: Instant,
    bytes_by_artifact: BTreeMap<String, u64>,
    bytes_done: u64,
    items_done: i64,
}

impl DownloadProgressState {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            bytes_by_artifact: BTreeMap::new(),
            bytes_done: 0,
            items_done: 0,
        }
    }

    fn observe_artifact_bytes(&mut self, key: &str, done: u64) {
        let previous = self.bytes_by_artifact.insert(key.to_string(), done).unwrap_or(0);
        self.bytes_done = self.bytes_done.saturating_add(done.saturating_sub(previous));
    }

    fn finish_artifact(&mut self, key: &str, total: u64) {
        self.observe_artifact_bytes(key, total);
        self.items_done = self.items_done.saturating_add(1);
    }

    fn snapshot(&self, total_bytes: u64, total_items: i64) -> ProgressInfo {
        let phase_percent = if total_bytes > 0 {
            clamp_progress_percent_u64(self.bytes_done, total_bytes)
        } else {
            clamp_progress_percent(self.items_done, total_items.max(1))
        };
        ProgressInfo {
            phase: 2,
            phase_percent,
            total_percent: 10 + phase_percent * 30 / 100,
            bytes_done: saturating_i64_from_u64(self.bytes_done),
            bytes_total: saturating_i64_from_u64(total_bytes),
            items_done: self.items_done,
            items_total: total_items,
            speed_bytes_per_sec: average_speed_bytes_per_sec(self.bytes_done, self.started_at),
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
    async fn load_release_index(&mut self) -> Result<ReleaseIndex> {
        match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(data) => decompress_release_index(&data),
            Err(SurgeError::NotFound(_)) => {
                let base_prefix = self.ctx.storage_config().prefix;
                let scoped_prefix = append_prefix(&base_prefix, &self.app_id);
                if scoped_prefix == base_prefix {
                    return Err(SurgeError::NotFound(format!(
                        "Release index '{RELEASES_FILE_COMPRESSED}' not found"
                    )));
                }

                debug!(
                    app_id = %self.app_id,
                    base_prefix = %base_prefix,
                    scoped_prefix = %scoped_prefix,
                    "Release index not found on configured prefix; trying app-scoped prefix"
                );

                let mut scoped_config = self.ctx.storage_config();
                scoped_config.prefix = scoped_prefix.clone();
                let scoped_backend = create_storage_backend(&scoped_config)?;

                match scoped_backend.get_object(RELEASES_FILE_COMPRESSED).await {
                    Ok(data) => {
                        info!(
                            app_id = %self.app_id,
                            scoped_prefix = %scoped_prefix,
                            "Using app-scoped storage prefix for update checks"
                        );
                        self.ctx.set_storage_prefix(&scoped_prefix);
                        self.storage = scoped_backend;
                        decompress_release_index(&data)
                    }
                    Err(SurgeError::NotFound(_)) => Err(SurgeError::NotFound(format!(
                        "Release index '{RELEASES_FILE_COMPRESSED}' not found on configured or app-scoped prefix"
                    ))),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
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
        let index = self.load_release_index().await?;
        let current_rid = current_rid();
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
        let supported_delta_chain = delta_chain.filter(|chain| {
            chain
                .iter()
                .all(|release| release.selected_delta().is_some_and(|delta| is_supported_delta(&delta)))
        });

        let (apply_releases, apply_strategy, download_size) = if let Some(chain) = supported_delta_chain {
            let selected: Vec<ReleaseEntry> = chain.into_iter().cloned().collect();
            let size = selected
                .iter()
                .filter_map(ReleaseEntry::selected_delta)
                .map(|delta| delta.size)
                .sum();
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
    /// 4. Extract - extract the final archive into the install tree
    /// 5. Apply delta - rebuild the final archive when using delta updates
    /// 6. Finalize - move files into place, clean up
    pub async fn download_and_apply<F>(&self, info: &UpdateInfo, progress: Option<F>) -> Result<()>
    where
        F: Fn(ProgressInfo) + Send + Sync,
    {
        const DOWNLOAD_CONCURRENCY: usize = 4;

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

        let artifacts: Vec<ArtifactDownload> = if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            info.apply_releases
                .iter()
                .filter_map(ReleaseEntry::selected_delta)
                .map(|delta| ArtifactDownload {
                    key: delta.filename.clone(),
                    sha256: delta.sha256.clone(),
                    size: delta.size,
                })
                .collect()
        } else {
            let latest = info
                .apply_releases
                .last()
                .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
            vec![ArtifactDownload {
                key: latest.full_filename.clone(),
                sha256: latest.full_sha256.clone(),
                size: latest.full_size,
            }]
        };
        if artifacts.is_empty() {
            return Err(SurgeError::Update("No artifacts selected for download".to_string()));
        }

        let total_items = i64::try_from(artifacts.len()).unwrap_or(i64::MAX);
        let total_bytes = artifacts
            .iter()
            .fold(0i64, |acc, artifact| acc.saturating_add(artifact.size.max(0)));
        let total_bytes_u64 = u64::try_from(total_bytes).unwrap_or(u64::MAX);

        let storage = self.storage.as_ref();
        let staging_dir_ref = &staging_dir;
        let cache_dir_ref = &artifact_cache_dir;

        let download_progress_state = Arc::new(Mutex::new(DownloadProgressState::new()));
        let mut download_stream = stream::iter(artifacts.into_iter())
            .map(|artifact| {
                let download_progress_state = Arc::clone(&download_progress_state);
                let progress = progress.clone();
                async move {
                    let cache_path = cache_path_for_key(cache_dir_ref, &artifact.key)?;
                    let artifact_key_for_progress = artifact.key.clone();
                    let progress_callback = move |done: u64, _total: u64| {
                        let snapshot = {
                            let mut state = download_progress_state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            state.observe_artifact_bytes(&artifact_key_for_progress, done);
                            state.snapshot(total_bytes_u64, total_items)
                        };
                        emit_progress(progress.as_ref(), snapshot);
                    };
                    let outcome = fetch_or_reuse_file(
                        storage,
                        &artifact.key,
                        &cache_path,
                        &artifact.sha256,
                        Some(&progress_callback),
                    )
                    .await?;

                    let stage_path = staging_dir_ref.join(&artifact.key);
                    if let Some(parent) = stage_path.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    tokio::fs::copy(&cache_path, &stage_path).await?;

                    Ok::<(ArtifactDownload, CacheFetchOutcome), SurgeError>((artifact, outcome))
                }
            })
            .buffer_unordered(DOWNLOAD_CONCURRENCY);

        while let Some(result) = download_stream.next().await {
            self.ctx.check_cancelled()?;
            let (artifact, outcome) = result?;

            debug!(key = %artifact.key, ?outcome, "Prepared artifact for update application");

            let snapshot = {
                let mut state = download_progress_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.finish_artifact(&artifact.key, u64::try_from(artifact.size.max(0)).unwrap_or(u64::MAX));
                state.snapshot(total_bytes_u64, total_items)
            };
            emit_progress(progress.as_ref(), snapshot);
        }

        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 2,
                phase_percent: 100,
                total_percent: 40,
                bytes_done: total_bytes,
                bytes_total: total_bytes,
                items_done: total_items,
                items_total: total_items,
                speed_bytes_per_sec: {
                    let state = download_progress_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    average_speed_bytes_per_sec(state.bytes_done, state.started_at)
                },
            },
        );

        // Phase 3: Verify
        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 3,
                total_percent: 45,
                items_total: total_items,
                ..ProgressInfo::default()
            },
        );

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            for release in &info.apply_releases {
                self.ctx.check_cancelled()?;

                let Some(delta) = release.selected_delta() else {
                    continue;
                };

                let path = staging_dir.join(&delta.filename);
                let hash = sha256_hex_file(&path)?;
                if !delta.sha256.is_empty() && hash != delta.sha256 {
                    return Err(SurgeError::Update(format!(
                        "SHA-256 mismatch for {}: expected {}, got {hash}",
                        delta.filename, delta.sha256
                    )));
                }
            }
        } else {
            let latest = info
                .apply_releases
                .last()
                .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
            let path = staging_dir.join(&latest.full_filename);
            let hash = sha256_hex_file(&path)?;
            if !latest.full_sha256.is_empty() && hash != latest.full_sha256 {
                return Err(SurgeError::Update(format!(
                    "SHA-256 mismatch for {}: expected {}, got {hash}",
                    latest.full_filename, latest.full_sha256
                )));
            }
        }

        emit_progress(
            progress.as_ref(),
            ProgressInfo {
                phase: 3,
                phase_percent: 100,
                total_percent: 55,
                items_done: total_items,
                items_total: total_items,
                ..ProgressInfo::default()
            },
        );

        let extract_dir = staging_dir.join("extracted");
        tokio::fs::create_dir_all(&extract_dir).await?;

        if matches!(info.apply_strategy, ApplyStrategy::Delta) {
            // Phase 5: Apply delta
            let apply_delta_started_at = Instant::now();
            let apply_delta_total_items = i64::try_from(info.apply_releases.len()).unwrap_or(i64::MAX);
            let apply_delta_total_bytes = info
                .apply_releases
                .iter()
                .filter_map(ReleaseEntry::selected_delta)
                .fold(0i64, |acc, delta| acc.saturating_add(delta.size.max(0)));

            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 5,
                    total_percent: 60,
                    bytes_total: apply_delta_total_bytes,
                    items_total: apply_delta_total_items,
                    ..ProgressInfo::default()
                },
            );

            // Restore the current full archive (direct or reconstructed from
            // earlier full + deltas), then apply the downloaded delta chain.
            let index = if let Some(cached) = &self.cached_index {
                cached.clone()
            } else {
                let data = self.storage.get_object(RELEASES_FILE_COMPRESSED).await?;
                decompress_release_index(&data)?
            };
            let rid = current_rid();
            let mut rebuilt_archive = restore_full_archive_for_version_with_options(
                self.storage.as_ref(),
                &index,
                &rid,
                &self.current_version,
                RestoreOptions {
                    cache_dir: Some(&artifact_cache_dir),
                    progress: None,
                },
            )
            .await
            .map_err(|e| {
                SurgeError::Update(format!(
                    "Failed to restore base full archive for {}: {e}",
                    self.current_version
                ))
            })?;

            let mut apply_delta_items_done = 0i64;
            let mut apply_delta_bytes_done = 0i64;
            for release in &info.apply_releases {
                self.ctx.check_cancelled()?;

                let Some(delta) = release.selected_delta() else {
                    return Err(SurgeError::Update(format!(
                        "Delta update path is missing delta filename for {}",
                        release.version
                    )));
                };

                if !is_supported_delta(&delta) {
                    return Err(SurgeError::Update(format!(
                        "Delta {} for {} uses unsupported descriptor (algorithm='{}', format='{}', compression='{}')",
                        delta.filename, release.version, delta.algorithm, delta.patch_format, delta.compression
                    )));
                }

                let delta_path = staging_dir.join(&delta.filename);
                let delta_compressed = tokio::fs::read(&delta_path).await?;
                let patch = decode_delta_patch(delta_compressed.as_slice(), &delta)
                    .map_err(|e| SurgeError::Archive(format!("Failed to decompress delta {}: {e}", delta.filename)))?;
                rebuilt_archive = apply_delta_patch(&rebuilt_archive, &patch, &delta)
                    .map_err(|e| SurgeError::Update(format!("Failed to apply delta {}: {e}", delta.filename)))?;

                if !release.full_sha256.is_empty() {
                    let hash = sha256_hex(&rebuilt_archive);
                    if hash != release.full_sha256 {
                        return Err(SurgeError::Update(format!(
                            "SHA-256 mismatch for rebuilt full archive {}: expected {}, got {hash}",
                            release.version, release.full_sha256
                        )));
                    }
                }

                apply_delta_items_done = apply_delta_items_done.saturating_add(1);
                apply_delta_bytes_done = apply_delta_bytes_done.saturating_add(delta.size.max(0));
                let phase_percent = clamp_progress_percent(apply_delta_items_done, apply_delta_total_items.max(1));
                emit_progress(
                    progress.as_ref(),
                    ProgressInfo {
                        phase: 5,
                        phase_percent,
                        total_percent: phase_total_percent(60, 20, phase_percent),
                        bytes_done: apply_delta_bytes_done,
                        bytes_total: apply_delta_total_bytes,
                        items_done: apply_delta_items_done,
                        items_total: apply_delta_total_items,
                        speed_bytes_per_sec: average_speed_bytes_per_sec(
                            u64::try_from(apply_delta_bytes_done.max(0)).unwrap_or(u64::MAX),
                            apply_delta_started_at,
                        ),
                    },
                );
            }

            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 5,
                    phase_percent: 100,
                    total_percent: 80,
                    bytes_done: apply_delta_total_bytes,
                    bytes_total: apply_delta_total_bytes,
                    items_done: apply_delta_total_items,
                    items_total: apply_delta_total_items,
                    speed_bytes_per_sec: average_speed_bytes_per_sec(
                        u64::try_from(apply_delta_total_bytes.max(0)).unwrap_or(u64::MAX),
                        apply_delta_started_at,
                    ),
                },
            );

            let rebuilt_archive_path = staging_dir.join("rebuilt-full.tar.zst");
            tokio::fs::write(&rebuilt_archive_path, &rebuilt_archive).await?;
            // Phase 4: Extract the rebuilt archive into place.
            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 4,
                    total_percent: 80,
                    ..ProgressInfo::default()
                },
            );
            let extract_started_at = Instant::now();
            let progress_for_extract = progress.clone();
            let extract_progress = move |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
                let phase_percent = if bytes_total > 0 {
                    clamp_progress_percent_u64(bytes_done, bytes_total)
                } else {
                    clamp_progress_percent_u64(items_done, items_total)
                };
                emit_progress(
                    progress_for_extract.as_ref(),
                    ProgressInfo {
                        phase: 4,
                        phase_percent,
                        total_percent: phase_total_percent(80, 10, phase_percent),
                        bytes_done: saturating_i64_from_u64(bytes_done),
                        bytes_total: saturating_i64_from_u64(bytes_total),
                        items_done: saturating_i64_from_u64(items_done),
                        items_total: saturating_i64_from_u64(items_total),
                        speed_bytes_per_sec: average_speed_bytes_per_sec(bytes_done, extract_started_at),
                    },
                );
            };
            extract_file_to_with_progress(&rebuilt_archive_path, &extract_dir, Some(&extract_progress))?;
            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 4,
                    phase_percent: 100,
                    total_percent: 90,
                    ..ProgressInfo::default()
                },
            );
        } else {
            // Phase 4: Extract
            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 4,
                    total_percent: 60,
                    ..ProgressInfo::default()
                },
            );
            let extract_started_at = Instant::now();
            let progress_for_extract = progress.clone();
            let extract_progress = move |items_done: u64, items_total: u64, bytes_done: u64, bytes_total: u64| {
                let phase_percent = if bytes_total > 0 {
                    clamp_progress_percent_u64(bytes_done, bytes_total)
                } else {
                    clamp_progress_percent_u64(items_done, items_total)
                };
                emit_progress(
                    progress_for_extract.as_ref(),
                    ProgressInfo {
                        phase: 4,
                        phase_percent,
                        total_percent: phase_total_percent(60, 15, phase_percent),
                        bytes_done: saturating_i64_from_u64(bytes_done),
                        bytes_total: saturating_i64_from_u64(bytes_total),
                        items_done: saturating_i64_from_u64(items_done),
                        items_total: saturating_i64_from_u64(items_total),
                        speed_bytes_per_sec: average_speed_bytes_per_sec(bytes_done, extract_started_at),
                    },
                );
            };
            let latest = info
                .apply_releases
                .last()
                .ok_or_else(|| SurgeError::Update("No latest release".to_string()))?;
            let archive_path = staging_dir.join(&latest.full_filename);
            extract_file_to_with_progress(&archive_path, &extract_dir, Some(&extract_progress))?;
            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 4,
                    phase_percent: 100,
                    total_percent: 75,
                    ..ProgressInfo::default()
                },
            );

            // Phase 5: Apply delta (if applicable)
            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 5,
                    total_percent: 80,
                    ..ProgressInfo::default()
                },
            );
            emit_progress(
                progress.as_ref(),
                ProgressInfo {
                    phase: 5,
                    phase_percent: 100,
                    total_percent: 85,
                    ..ProgressInfo::default()
                },
            );
        }

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

        invoke_post_update_hook(&self.install_dir, &active_app_dir, latest);

        if supervisor_was_running {
            restart_supervisor_after_update(&self.install_dir, &active_app_dir, latest);
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

    find_latest_app_dir(install_dir).ok()
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

    let pid_file = supervisor_pid_file(install_dir, supervisor_id);
    if !pid_file.is_file() {
        return Ok(());
    }

    let stop_file = supervisor_stop_file(install_dir, supervisor_id);
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

fn invoke_post_update_hook(install_dir: &Path, active_app_dir: &Path, latest: &ReleaseEntry) {
    let main_exe = latest.main_exe.trim();
    if main_exe.is_empty() {
        return;
    }

    let exe_path = active_app_dir.join(main_exe);
    if !exe_path.is_file() {
        warn!(
            exe = %exe_path.display(),
            version = %latest.version,
            "Skipping post-update lifecycle hook because the executable is missing"
        );
        return;
    }

    let lifecycle_args = [String::from("--surge-updated"), latest.version.clone()];
    let lifecycle_args_refs: Vec<&str> = lifecycle_args.iter().map(String::as_str).collect();

    match spawn_process(&exe_path, &lifecycle_args_refs, Some(install_dir), &latest.environment) {
        Ok(mut handle) => wait_for_post_update_hook(&mut handle, &exe_path),
        Err(e) => {
            warn!(
                exe = %exe_path.display(),
                version = %latest.version,
                error = %e,
                "Failed to invoke post-update lifecycle hook (continuing)"
            );
        }
    }
}

fn wait_for_post_update_hook(handle: &mut crate::platform::process::ProcessHandle, exe_path: &Path) {
    let check_interval = Duration::from_millis(100);
    let deadline = std::time::Instant::now() + Duration::from_secs(15);

    while std::time::Instant::now() < deadline {
        if !handle.poll_running() {
            match handle.wait() {
                Ok(result) if result.exit_code == 0 => {
                    debug!(exe = %exe_path.display(), "Post-update lifecycle hook completed successfully");
                }
                Ok(result) => {
                    warn!(
                        exe = %exe_path.display(),
                        exit_code = result.exit_code,
                        "Post-update lifecycle hook exited non-zero (continuing)"
                    );
                }
                Err(e) => {
                    warn!(
                        exe = %exe_path.display(),
                        error = %e,
                        "Failed waiting for post-update lifecycle hook (continuing)"
                    );
                }
            }
            return;
        }

        std::thread::sleep(check_interval);
    }

    warn!(
        exe = %exe_path.display(),
        "Post-update lifecycle hook exceeded timeout, terminating it (continuing)"
    );
    let _ = handle.kill();
    let _ = handle.wait();
}

fn restart_supervisor_after_update(install_dir: &Path, active_app_dir: &Path, latest: &ReleaseEntry) {
    let supervisor_id = latest.supervisor_id.trim();
    if supervisor_id.is_empty() {
        return;
    }

    let supervisor_path = active_app_dir.join(supervisor_binary_name());
    if !supervisor_path.is_file() {
        warn!(
            supervisor = %supervisor_path.display(),
            "Cannot restart supervisor after update because the bundled binary is missing"
        );
        return;
    }

    let exe_path = active_app_dir.join(&latest.main_exe);
    if !exe_path.is_file() {
        warn!(
            exe = %exe_path.display(),
            "Cannot restart supervisor after update because the application executable is missing"
        );
        return;
    }

    let restart_args = match read_restart_args(install_dir, supervisor_id) {
        Ok(args) => args,
        Err(e) => {
            warn!(
                supervisor_id,
                error = %e,
                "Failed reading stored supervisor restart arguments; restarting with no extra args"
            );
            Vec::new()
        }
    };

    let install_dir_str = install_dir.to_string_lossy();
    let pid_str = current_pid().to_string();
    let exe_path_str = exe_path.to_string_lossy();
    let mut args: Vec<&str> = vec![
        "watch",
        "--id",
        supervisor_id,
        "--dir",
        &install_dir_str,
        "--pid",
        &pid_str,
        "--exe",
        &exe_path_str,
    ];
    if !restart_args.is_empty() {
        args.push("--");
        args.extend(restart_args.iter().map(String::as_str));
    }

    match spawn_detached(&supervisor_path, &args, Some(install_dir), &latest.environment) {
        Ok(handle) => {
            info!(pid = handle.pid(), supervisor_id, "Restarted supervisor after update");
        }
        Err(e) => {
            warn!(
                supervisor_id,
                error = %e,
                "Failed to restart supervisor after update (continuing)"
            );
        }
    }
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
            copy_directory(&source, &destination)?;
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
    #![allow(clippy::cast_possible_wrap)]

    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::config::constants::DEFAULT_ZSTD_LEVEL;
    #[cfg(target_os = "linux")]
    use crate::config::manifest::ShortcutLocation;
    use crate::context::StorageProvider;
    use crate::diff::wrapper::bsdiff_buffers;
    #[cfg(target_os = "linux")]
    use crate::platform::shortcuts::{clear_test_shortcut_paths_override, set_test_shortcut_paths_override};
    use crate::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex, compress_release_index};

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
        normalize_os_label(raw)
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
            StorageProvider::Filesystem,
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
    async fn test_check_for_updates_falls_back_to_app_scoped_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let app_id = "test-app";
        let app_scoped_store = store_root.join(app_id);
        std::fs::create_dir_all(&app_scoped_store).unwrap();

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
        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(app_scoped_store.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
    async fn test_check_for_updates_genesis_without_delta_uses_full_strategy() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();

        let mut release = make_entry("1.1.0", "stable", &current_os_label_for_tests(), &current_rid());
        release.is_genesis = true;
        release.set_primary_delta(None);

        let index = ReleaseIndex {
            app_id: "test-app".to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
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
    async fn test_check_for_updates_treats_stable_as_newer_than_matching_prerelease() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
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
            app_id: "test-app".to_string(),
            releases: vec![prerelease, stable],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
            "test-app",
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
            app_id: "test-app".to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager = UpdateManager::new(ctx, "test-app", "1.0.0", "stable", tmp.path().to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert!(info.delta_available);
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);
        assert_eq!(info.download_size, 99);
    }

    #[tokio::test]
    async fn test_check_for_updates_falls_back_to_full_for_unsupported_descriptor() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
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
            app_id: "test-app".to_string(),
            releases: vec![release],
            ..ReleaseIndex::default()
        };
        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
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
    }

    #[tokio::test]
    async fn test_check_for_updates_after_channel_switch() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();
        let rid = current_rid();
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
            StorageProvider::Filesystem,
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

        let rid = current_rid();
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
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
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
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
        let runtime_manifest = install_root
            .join("app")
            .join(crate::install::RUNTIME_MANIFEST_RELATIVE_PATH);
        assert!(runtime_manifest.is_file());
        let runtime_manifest_raw = std::fs::read_to_string(&runtime_manifest).unwrap();
        assert!(runtime_manifest_raw.contains("id: test-app"));
        assert!(runtime_manifest_raw.contains("version: 1.1.0"));
        assert!(runtime_manifest_raw.contains("channel: stable"));

        std::fs::remove_file(store_root.join(&full_filename)).unwrap();
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
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let current_app_dir = install_root.join("app");
        std::fs::create_dir_all(current_app_dir.join("state")).unwrap();
        std::fs::create_dir_all(current_app_dir.join("temp")).unwrap();
        std::fs::write(current_app_dir.join("settings.json"), "persisted settings").unwrap();
        std::fs::write(current_app_dir.join("state").join("user.db"), "persisted state").unwrap();
        std::fs::write(current_app_dir.join("old-token.txt"), "remove me").unwrap();
        std::fs::write(current_app_dir.join("temp").join("old.log"), "remove dir").unwrap();

        let rid = current_rid();
        let full_filename = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_path = store_root.join(&full_filename);

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_buffer("payload.txt", b"new payload", 0o644).unwrap();
        packer.add_buffer("settings.json", b"packaged settings", 0o644).unwrap();
        packer.add_buffer("state/default.db", b"packaged state", 0o644).unwrap();
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
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
                main_exe: "test-app".to_string(),
                install_directory: "test-app".to_string(),
                supervisor_id: String::new(),
                icon: String::new(),
                shortcuts: Vec::new(),
                persistent_assets: vec!["settings.json".to_string(), "state".to_string()],
                installers: Vec::new(),
                environment: std::collections::BTreeMap::new(),
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
            UpdateManager::new(ctx, "test-app", "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
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
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let rid = current_rid();
        let full_filename = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_path = store_root.join(&full_filename);

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
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
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
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
            UpdateManager::new(ctx, "test-app", "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();

        let observed = Arc::new(Mutex::new(Vec::new()));
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
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

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

        let full_v1_name = format!("test-app-1.0.0-{rid}-full.tar.zst");
        let full_v2_name = format!("test-app-1.1.0-{rid}-full.tar.zst");
        let full_v3_name = format!("test-app-1.2.0-{rid}-full.tar.zst");
        let full_v4_name = format!("test-app-1.3.0-{rid}-full.tar.zst");
        let delta_v2_name = format!("test-app-1.1.0-{rid}-delta.tar.zst");
        let delta_v3_name = format!("test-app-1.2.0-{rid}-delta.tar.zst");
        let delta_v4_name = format!("test-app-1.3.0-{rid}-delta.tar.zst");

        std::fs::write(store_root.join(&full_v1_name), &full_v1).unwrap();
        std::fs::write(store_root.join(&delta_v2_name), &delta_v2).unwrap();
        std::fs::write(store_root.join(&delta_v3_name), &delta_v3).unwrap();
        std::fs::write(store_root.join(&delta_v4_name), &delta_v4).unwrap();

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
                    full_sha256: sha256_hex(&full_v1),
                    deltas: Vec::new(),
                    preferred_delta_id: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
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
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );

        let mut manager =
            UpdateManager::new(ctx, "test-app", "1.0.0", "stable", install_root.to_str().unwrap()).unwrap();
        let info = manager.check_for_updates().await.unwrap().unwrap();
        assert_eq!(info.apply_strategy, ApplyStrategy::Delta);

        let observed = Arc::new(Mutex::new(Vec::new()));
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
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

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
                    full_sha256: sha256_hex(&full_v1),
                    deltas: Vec::new(),
                    preferred_delta_id: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
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
            StorageProvider::Filesystem,
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

        let rid = current_rid();
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
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
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
            }],
            ..ReleaseIndex::default()
        };

        let compressed = compress_release_index(&index, DEFAULT_ZSTD_LEVEL).unwrap();
        std::fs::write(store_root.join(RELEASES_FILE_COMPRESSED), compressed).unwrap();

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
                clear_test_shortcut_paths_override();
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
        set_test_shortcut_paths_override(applications_dir.clone(), autostart_dir.clone());
        let _override_guard = ShortcutPathsOverrideGuard;

        let store_root = tmp.path().join("store");
        let install_root = tmp.path().join("install");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&install_root).unwrap();

        let rid = current_rid();
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
                deltas: Vec::new(),
                preferred_delta_id: String::new(),
                created_utc: chrono::Utc::now().to_rfc3339(),
                release_notes: String::new(),
                name: String::new(),
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
            StorageProvider::Filesystem,
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

        // name is empty so display_name falls back to main_exe ("demoapp")
        let desktop_file = applications_dir.join("demoapp.desktop");
        let startup_file = autostart_dir.join("demoapp.desktop");
        assert!(desktop_file.exists());
        assert!(startup_file.exists());

        let desktop_content = std::fs::read_to_string(desktop_file).unwrap();
        assert!(desktop_content.contains("Icon="));
        assert!(desktop_content.contains("Name=demoapp"));
        let stable_exe_path = install_root.join("app").join("demoapp");
        assert!(desktop_content.contains(stable_exe_path.to_string_lossy().as_ref()));
    }
}
