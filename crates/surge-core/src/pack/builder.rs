//! Pack builder: create full and delta packages, upload to storage.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::archive::packer::ArchivePacker;
use crate::config::constants::{RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use crate::config::manifest::{ShortcutLocation, SurgeManifest};
use crate::context::Context;
use crate::crypto::sha256::sha256_hex_file;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index};
use crate::storage::{StorageBackend, create_storage_backend};

/// A built package artifact ready for upload.
#[derive(Debug, Clone)]
pub struct PackageArtifact {
    /// Local path to the artifact file.
    pub path: PathBuf,
    /// Filename used in storage.
    pub filename: String,
    /// File size in bytes.
    pub size: i64,
    /// SHA-256 hash (lowercase hex).
    pub sha256: String,
    /// Whether this is a delta package.
    pub is_delta: bool,
    /// Source version for delta packages, empty for full packages.
    pub from_version: String,
}

/// Builds full and delta release packages from application artifacts.
#[allow(dead_code)]
pub struct PackBuilder {
    ctx: Arc<Context>,
    app_id: String,
    rid: String,
    version: String,
    main_exe: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
    artifacts_dir: PathBuf,
    storage: Box<dyn StorageBackend>,
    artifacts: Vec<PackageArtifact>,
}

impl PackBuilder {
    /// Create a new pack builder.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The Surge context
    /// * `manifest_path` - Path to the surge.yml manifest
    /// * `app_id` - Application identifier
    /// * `rid` - Runtime identifier (e.g., "linux-x64")
    /// * `version` - Version being packaged
    /// * `artifacts_dir` - Directory containing the built application files
    pub fn new(
        ctx: Arc<Context>,
        manifest_path: &str,
        app_id: &str,
        rid: &str,
        version: &str,
        artifacts_dir: &str,
    ) -> Result<Self> {
        let manifest = SurgeManifest::from_file(Path::new(manifest_path))?;

        let app = manifest
            .find_app(app_id)
            .ok_or_else(|| SurgeError::Config(format!("App '{app_id}' not found in manifest")))?;
        let target = app
            .targets
            .iter()
            .find(|t| t.rid == rid)
            .ok_or_else(|| SurgeError::Config(format!("Target '{rid}' not found for app '{app_id}'")))?;
        let main_exe = if app.main_exe.is_empty() {
            app.id.clone()
        } else {
            app.main_exe.clone()
        };

        let storage_cfg = ctx.storage_config();
        let storage = create_storage_backend(&storage_cfg)?;

        let artifacts_path = PathBuf::from(artifacts_dir);
        if !artifacts_path.exists() {
            return Err(SurgeError::Pack(format!(
                "Artifacts directory does not exist: {artifacts_dir}"
            )));
        }

        if !target.shortcuts.is_empty() {
            let exe_path = artifacts_path.join(&main_exe);
            if !exe_path.is_file() {
                return Err(SurgeError::Pack(format!(
                    "Configured main executable '{}' not found in artifacts: {}",
                    main_exe,
                    exe_path.display()
                )));
            }
        }

        if !target.icon.is_empty() {
            let icon_path = artifacts_path.join(&target.icon);
            if !icon_path.is_file() {
                return Err(SurgeError::Pack(format!(
                    "Configured icon '{}' not found in artifacts: {}",
                    target.icon,
                    icon_path.display()
                )));
            }
        }

        Ok(Self {
            ctx,
            app_id: app_id.to_string(),
            rid: rid.to_string(),
            version: version.to_string(),
            main_exe,
            icon: target.icon.clone(),
            shortcuts: target.shortcuts.clone(),
            artifacts_dir: artifacts_path,
            storage,
            artifacts: Vec::new(),
        })
    }

    /// Build the full and delta packages.
    ///
    /// Creates a tar.zst archive of the artifacts directory as the full package.
    /// If a previous version exists in storage, also creates a delta package
    /// using bsdiff.
    ///
    /// The optional `progress` callback receives `(items_done, items_total)`.
    pub async fn build(&mut self, progress: Option<&dyn Fn(i32, i32)>) -> Result<()> {
        self.ctx.check_cancelled()?;

        let total_steps = 2; // full + potential delta
        let report = |step: i32| {
            if let Some(cb) = progress {
                cb(step, total_steps);
            }
        };

        info!(
            app_id = %self.app_id,
            version = %self.version,
            rid = %self.rid,
            "Building packages"
        );

        // Step 1: Build full package
        report(0);
        let full_artifact = self.build_full_package().await?;
        self.artifacts.push(full_artifact);
        report(1);

        // Step 2: Attempt delta package (non-fatal if it fails)
        match self.build_delta_package().await {
            Ok(Some(delta_artifact)) => {
                self.artifacts.push(delta_artifact);
                debug!("Delta package built successfully");
            }
            Ok(None) => {
                debug!("No previous version for delta, skipping");
            }
            Err(e) => {
                warn!("Delta package build failed (non-fatal): {e}");
            }
        }
        report(2);

        info!(artifact_count = self.artifacts.len(), "Package build complete");

        Ok(())
    }

    /// Upload built packages to storage and update the release index.
    ///
    /// The optional `progress` callback receives `(items_done, items_total)`.
    pub async fn push(&self, channel: &str, progress: Option<&dyn Fn(i32, i32)>) -> Result<()> {
        self.ctx.check_cancelled()?;

        if self.artifacts.is_empty() {
            return Err(SurgeError::Pack("No artifacts to push. Run build() first.".to_string()));
        }

        let total = self.artifacts.len() as i32 + 1; // artifacts + index update
        let report = |step: i32| {
            if let Some(cb) = progress {
                cb(step, total);
            }
        };

        info!(channel, artifact_count = self.artifacts.len(), "Uploading packages");

        // Upload each artifact
        for (i, artifact) in self.artifacts.iter().enumerate() {
            self.ctx.check_cancelled()?;

            debug!(filename = %artifact.filename, "Uploading artifact");
            self.storage
                .upload_from_file(&artifact.filename, &artifact.path, None)
                .await?;

            report(i as i32 + 1);
        }

        // Update the release index
        self.update_release_index(channel).await?;
        report(total);

        info!("Push complete");
        Ok(())
    }

    /// Get the list of built artifacts.
    #[must_use]
    pub fn artifacts(&self) -> &[PackageArtifact] {
        &self.artifacts
    }

    /// Build the full tar.zst package.
    async fn build_full_package(&self) -> Result<PackageArtifact> {
        let budget = self.ctx.resource_budget();
        let filename = format!("{}-{}-{}-full.tar.zst", self.app_id, self.version, self.rid);

        let output_dir = self.artifacts_dir.parent().unwrap_or(Path::new("."));
        let output_path = output_dir.join(&filename);

        let mut packer = ArchivePacker::new(budget.zstd_compression_level)?;
        packer.add_directory(&self.artifacts_dir, "")?;
        packer.finalize_to_file(&output_path)?;

        let sha256 = sha256_hex_file(&output_path)?;
        let size = std::fs::metadata(&output_path)?.len() as i64;

        Ok(PackageArtifact {
            path: output_path,
            filename,
            size,
            sha256,
            is_delta: false,
            from_version: String::new(),
        })
    }

    /// Attempt to build a delta package from the previous version.
    ///
    /// Returns `Ok(None)` if no previous version exists.
    async fn build_delta_package(&self) -> Result<Option<PackageArtifact>> {
        // Try to download the previous full package to compute a delta
        let prev_filename = self.find_previous_full_package().await?;
        let prev_filename = match prev_filename {
            Some(f) => f,
            None => return Ok(None),
        };

        // Download the previous package
        let staging_dir = crate::platform::fs::create_temp_dir()?;
        let prev_path = staging_dir.join(&prev_filename);
        self.storage.download_to_file(&prev_filename, &prev_path, None).await?;

        // Read both packages
        let prev_data = std::fs::read(&prev_path)?;
        let full_artifact = self
            .artifacts
            .iter()
            .find(|a| !a.is_delta)
            .ok_or_else(|| SurgeError::Pack("Full package not yet built".to_string()))?;
        let new_data = std::fs::read(&full_artifact.path)?;

        // Compute bsdiff
        let patch = crate::diff::wrapper::bsdiff_buffers(&prev_data, &new_data)?;

        let delta_filename = format!("{}-{}-{}-delta.tar.zst", self.app_id, self.version, self.rid);
        let output_dir = self.artifacts_dir.parent().unwrap_or(Path::new("."));
        let delta_path = output_dir.join(&delta_filename);

        // Compress the delta with zstd
        let budget = self.ctx.resource_budget();
        let compressed = zstd::encode_all(patch.as_slice(), budget.zstd_compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to compress delta: {e}")))?;
        std::fs::write(&delta_path, &compressed)?;

        let sha256 = sha256_hex_file(&delta_path)?;
        let size = std::fs::metadata(&delta_path)?.len() as i64;

        // Clean up temp
        let _ = std::fs::remove_dir_all(&staging_dir);

        Ok(Some(PackageArtifact {
            path: delta_path,
            filename: delta_filename,
            size,
            sha256,
            is_delta: true,
            from_version: String::new(), // Could be inferred from index
        }))
    }

    /// Find the previous version's full package filename in the release index.
    async fn find_previous_full_package(&self) -> Result<Option<String>> {
        let data = match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(d) => d,
            Err(SurgeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };

        let index = decompress_release_index(&data)?;

        // Find the most recent release before this version
        let mut previous: Option<&ReleaseEntry> = None;
        for release in &index.releases {
            if !release.rid.is_empty() && release.rid != self.rid {
                continue;
            }
            if crate::releases::version::compare_versions(&release.version, &self.version) == std::cmp::Ordering::Less {
                if let Some(prev) = previous {
                    if crate::releases::version::compare_versions(&release.version, &prev.version)
                        == std::cmp::Ordering::Greater
                    {
                        previous = Some(release);
                    }
                } else {
                    previous = Some(release);
                }
            }
        }

        Ok(previous.map(|r| r.full_filename.clone()))
    }

    /// Update the release index in storage with the new release entry.
    async fn update_release_index(&self, channel: &str) -> Result<()> {
        // Try to fetch existing index, create new one if not found
        let mut index = match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(data) => decompress_release_index(&data)?,
            Err(SurgeError::NotFound(_)) => ReleaseIndex {
                schema: SCHEMA_VERSION,
                app_id: self.app_id.clone(),
                pack_id: String::new(),
                last_write_utc: String::new(),
                releases: Vec::new(),
            },
            Err(e) => return Err(e),
        };

        // Find the full and delta artifacts
        let full = self.artifacts.iter().find(|a| !a.is_delta);
        let delta = self.artifacts.iter().find(|a| a.is_delta);

        let entry = ReleaseEntry {
            version: self.version.clone(),
            channels: vec![channel.to_string()],
            os: detect_os_from_rid(&self.rid),
            rid: self.rid.clone(),
            is_genesis: index.releases.is_empty(),
            full_filename: full.map_or(String::new(), |a| a.filename.clone()),
            full_size: full.map_or(0, |a| a.size),
            full_sha256: full.map_or(String::new(), |a| a.sha256.clone()),
            delta_filename: delta.map_or(String::new(), |a| a.filename.clone()),
            delta_size: delta.map_or(0, |a| a.size),
            delta_sha256: delta.map_or(String::new(), |a| a.sha256.clone()),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            main_exe: self.main_exe.clone(),
            icon: self.icon.clone(),
            shortcuts: self.shortcuts.clone(),
        };

        // Remove any existing entry for this version/RID pair and add the new one.
        index
            .releases
            .retain(|r| !(r.version == self.version && r.rid == self.rid));
        index.releases.push(entry);

        index.last_write_utc = chrono::Utc::now().to_rfc3339();

        let budget = self.ctx.resource_budget();
        let compressed = compress_release_index(&index, budget.zstd_compression_level)?;
        self.storage
            .put_object(RELEASES_FILE_COMPRESSED, &compressed, "application/octet-stream")
            .await?;

        Ok(())
    }
}

/// Extract OS name from a RID string (e.g., "linux-x64" -> "linux").
fn detect_os_from_rid(rid: &str) -> String {
    rid.split('-').next().unwrap_or("unknown").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_os_from_rid() {
        assert_eq!(detect_os_from_rid("linux-x64"), "linux");
        assert_eq!(detect_os_from_rid("win-arm64"), "win");
        assert_eq!(detect_os_from_rid("osx-x64"), "osx");
        assert_eq!(detect_os_from_rid("unknown"), "unknown");
    }

    #[test]
    fn test_package_artifact_creation() {
        let artifact = PackageArtifact {
            path: PathBuf::from("/tmp/test.tar.zst"),
            filename: "test.tar.zst".to_string(),
            size: 1024,
            sha256: "abc123".to_string(),
            is_delta: false,
            from_version: String::new(),
        };
        assert!(!artifact.is_delta);
        assert_eq!(artifact.size, 1024);
    }
}
