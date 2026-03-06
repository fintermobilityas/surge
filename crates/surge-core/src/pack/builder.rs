//! Pack builder: create full and delta packages, upload to storage.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::archive::packer::ArchivePacker;
use crate::config::constants::{RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use crate::config::manifest::{PackDeltaStrategy, ShortcutLocation, SurgeManifest};
use crate::context::{Context, ResourceBudget};
use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::{ChunkedDiffOptions, DEFAULT_CHUNK_SIZE, chunked_bsdiff};
use crate::diff::wrapper::bsdiff_buffers;
use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::releases::manifest::{
    DeltaArtifact, PATCH_FORMAT_BSDIFF4, PATCH_FORMAT_CHUNKED_BSDIFF_V1, ReleaseEntry, ReleaseIndex,
    compress_release_index, decompress_release_index,
};
use crate::releases::restore::{find_previous_release_for_rid, restore_full_archive_for_version};
use crate::storage::{StorageBackend, create_storage_backend};

/// A built package artifact ready for upload.
#[derive(Debug, Clone)]
pub struct PackageArtifact {
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
    /// Delta patch format identifier, empty for full packages.
    pub patch_format: String,
    bytes: Vec<u8>,
}

impl PackageArtifact {
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Builds full and delta release packages from application artifacts.
#[allow(dead_code)]
pub struct PackBuilder {
    ctx: Arc<Context>,
    app_id: String,
    rid: String,
    version: String,
    name: String,
    main_exe: String,
    install_directory: String,
    supervisor_id: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
    persistent_assets: Vec<String>,
    installers: Vec<String>,
    environment: BTreeMap<String, String>,
    artifacts_dir: PathBuf,
    delta_strategy: PackDeltaStrategy,
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
        let pack_policy = manifest.effective_pack_policy();
        let (app, target) = manifest
            .find_app_with_target(app_id, rid)
            .ok_or_else(|| SurgeError::Config(format!("Target '{rid}' not found for app '{app_id}'")))?;
        let main_exe = app.effective_main_exe();

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
            name: app.effective_name(),
            main_exe,
            install_directory: app.effective_install_directory(),
            supervisor_id: app.supervisor_id.clone(),
            icon: target.icon.clone(),
            shortcuts: target.shortcuts.clone(),
            persistent_assets: target.persistent_assets.clone(),
            installers: target.installers.clone(),
            environment: target.environment.clone(),
            artifacts_dir: artifacts_path,
            delta_strategy: pack_policy.delta_strategy,
            storage,
            artifacts: Vec::new(),
        })
    }

    /// Build the full and delta packages.
    ///
    /// Creates a tar.zst archive of the artifacts directory as the full package.
    /// If a previous version exists in storage, also creates a delta package
    /// using chunked bsdiff.
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
        let full_artifact = self.build_full_package()?;
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

        let total = i32::try_from(self.artifacts.len()).unwrap_or(i32::MAX - 1) + 1; // artifacts + index update
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
                .put_object(&artifact.filename, artifact.bytes(), "application/octet-stream")
                .await?;

            report(i32::try_from(i).unwrap_or(i32::MAX - 1) + 1);
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

    /// Write built artifacts to `output_dir`.
    pub fn write_artifacts_to(&self, output_dir: &Path) -> Result<Vec<PathBuf>> {
        std::fs::create_dir_all(output_dir)?;

        self.artifacts
            .iter()
            .map(|artifact| {
                let path = output_dir.join(&artifact.filename);
                write_file_atomic(&path, artifact.bytes())?;
                Ok(path)
            })
            .collect()
    }

    /// Build the full tar.zst package.
    fn build_full_package(&mut self) -> Result<PackageArtifact> {
        let budget = self.ctx.resource_budget();
        let filename = format!("{}-{}-{}-full.tar.zst", self.app_id, self.version, self.rid);

        let mut packer = ArchivePacker::new(budget.zstd_compression_level)?;
        packer.add_directory(&self.artifacts_dir, "")?;

        // Bundle surge-supervisor if supervisor_id is configured and not already in artifacts.
        if !self.supervisor_id.trim().is_empty() {
            let supervisor_name = supervisor_binary_name_for_rid(&self.rid);
            if !self.artifacts_dir.join(supervisor_name).is_file() {
                let supervisor_source = find_supervisor_binary(supervisor_name)?;
                packer.add_file(&supervisor_source, supervisor_name)?;
            }
        }

        validate_surge_dotnet_native_runtime_bundle(&self.artifacts_dir, &self.rid)?;

        let archive_bytes = packer.finalize()?;
        let sha256 = sha256_hex(&archive_bytes);
        let size = i64::try_from(archive_bytes.len())
            .map_err(|_| SurgeError::Archive(format!("Archive is too large: {} bytes", archive_bytes.len())))?;

        Ok(PackageArtifact {
            filename,
            size,
            sha256,
            is_delta: false,
            from_version: String::new(),
            patch_format: String::new(),
            bytes: archive_bytes,
        })
    }

    /// Attempt to build a delta package from the previous version.
    ///
    /// Returns `Ok(None)` if no previous version exists.
    async fn build_delta_package(&self) -> Result<Option<PackageArtifact>> {
        let data = match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(d) => d,
            Err(SurgeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let index = decompress_release_index(&data)?;

        let Some(previous_release) = find_previous_release_for_rid(&index, &self.rid, &self.version) else {
            return Ok(None);
        };

        let prev_data =
            restore_full_archive_for_version(self.storage.as_ref(), &index, &self.rid, &previous_release.version)
                .await?;
        let new_data = self
            .artifacts
            .iter()
            .find(|a| !a.is_delta)
            .map(PackageArtifact::bytes)
            .ok_or_else(|| SurgeError::Pack("Full package not yet built".to_string()))?;

        let budget = self.ctx.resource_budget();
        let (patch, patch_format) = match self.delta_strategy {
            PackDeltaStrategy::ArchiveChunkedBsdiff => {
                let diff_options = chunked_diff_options(&budget, prev_data.len(), new_data.len());
                (
                    chunked_bsdiff(&prev_data, new_data, &diff_options)?,
                    PATCH_FORMAT_CHUNKED_BSDIFF_V1.to_string(),
                )
            }
            PackDeltaStrategy::ArchiveBsdiff => {
                (bsdiff_buffers(&prev_data, new_data)?, PATCH_FORMAT_BSDIFF4.to_string())
            }
        };

        let delta_filename = format!("{}-{}-{}-delta.tar.zst", self.app_id, self.version, self.rid);
        let compressed = zstd::encode_all(patch.as_slice(), budget.zstd_compression_level)
            .map_err(|e| SurgeError::Archive(format!("Failed to compress delta: {e}")))?;

        let sha256 = sha256_hex(&compressed);
        let size = i64::try_from(compressed.len())
            .map_err(|_| SurgeError::Archive(format!("Delta is too large: {} bytes", compressed.len())))?;

        Ok(Some(PackageArtifact {
            filename: delta_filename,
            size,
            sha256,
            is_delta: true,
            from_version: previous_release.version.clone(),
            patch_format,
            bytes: compressed,
        }))
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
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            name: self.name.clone(),
            main_exe: self.main_exe.clone(),
            install_directory: self.install_directory.clone(),
            supervisor_id: self.supervisor_id.clone(),
            icon: self.icon.clone(),
            shortcuts: self.shortcuts.clone(),
            persistent_assets: self.persistent_assets.clone(),
            installers: self.installers.clone(),
            environment: self.environment.clone(),
        };
        let mut entry = entry;
        let primary_delta = delta.map(|artifact| {
            if artifact
                .patch_format
                .eq_ignore_ascii_case(PATCH_FORMAT_CHUNKED_BSDIFF_V1)
            {
                DeltaArtifact::chunked_bsdiff_zstd(
                    "primary",
                    &artifact.from_version,
                    &artifact.filename,
                    artifact.size,
                    &artifact.sha256,
                )
            } else {
                DeltaArtifact::bsdiff_zstd(
                    "primary",
                    &artifact.from_version,
                    &artifact.filename,
                    artifact.size,
                    &artifact.sha256,
                )
            }
        });
        entry.set_primary_delta(primary_delta);

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

fn chunked_diff_options(budget: &ResourceBudget, older_len: usize, newer_len: usize) -> ChunkedDiffOptions {
    const MIN_CHUNK_SIZE: usize = 4 * 1024 * 1024;
    const BYTES_PER_THREAD_FACTOR: usize = 12;

    let requested_threads = usize::try_from(budget.max_threads).ok().unwrap_or(0);
    let planning_threads = if requested_threads == 0 {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
    } else {
        requested_threads
    };
    let archive_len = older_len.max(newer_len).max(1);

    let mut chunk_size = DEFAULT_CHUNK_SIZE.min(archive_len);
    if let Ok(memory_budget) = usize::try_from(budget.max_memory_bytes)
        && memory_budget > 0
    {
        let per_thread_budget = memory_budget / planning_threads.max(1);
        let budget_chunk_size = per_thread_budget / BYTES_PER_THREAD_FACTOR;
        chunk_size = chunk_size.min(budget_chunk_size.max(MIN_CHUNK_SIZE));
    }
    chunk_size = chunk_size.clamp(1, archive_len.max(MIN_CHUNK_SIZE));

    let chunk_count = archive_len.div_ceil(chunk_size).max(1);

    ChunkedDiffOptions {
        chunk_size,
        max_threads: if requested_threads == 0 {
            0
        } else {
            requested_threads.min(chunk_count)
        },
    }
}

fn validate_surge_dotnet_native_runtime_bundle(artifacts_path: &Path, rid: &str) -> Result<()> {
    if !artifacts_path.join("Surge.NET.dll").is_file() {
        return Ok(());
    }

    let candidates = native_library_candidates_for_rid(rid);
    if candidates.iter().any(|name| artifacts_path.join(name).is_file()) {
        return Ok(());
    }

    Err(SurgeError::Pack(format!(
        "Surge.NET.dll found in artifacts, but no native Surge runtime library for RID '{rid}' was found in the artifacts being packaged. Expected one of: {}. Bundle the native runtime into the build output so pack stays hermetic and does not depend on the host toolchain.",
        candidates.join(", ")
    )))
}

fn native_library_candidates_for_rid(rid: &str) -> Vec<&'static str> {
    let os = rid.split('-').next().unwrap_or_default();
    match os {
        "linux" => vec!["libsurge.so", "surge.so"],
        "osx" | "macos" => vec!["libsurge.dylib", "surge.dylib"],
        "win" | "windows" => vec!["surge.dll", "libsurge.dll"],
        _ => vec![
            "libsurge.so",
            "surge.so",
            "libsurge.dylib",
            "surge.dylib",
            "surge.dll",
            "libsurge.dll",
        ],
    }
}

/// Extract OS name from a RID string (e.g., "linux-x64" -> "linux").
fn detect_os_from_rid(rid: &str) -> String {
    rid.split('-').next().unwrap_or("unknown").to_string()
}

fn supervisor_binary_name() -> &'static str {
    crate::platform::process::supervisor_binary_name()
}

fn supervisor_binary_name_for_rid(rid: &str) -> &'static str {
    match rid.split('-').next().unwrap_or_default() {
        "win" | "windows" => "surge-supervisor.exe",
        "linux" | "osx" | "macos" => "surge-supervisor",
        _ => supervisor_binary_name(),
    }
}

fn find_supervisor_binary(name: &str) -> Result<PathBuf> {
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let candidate = parent.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(SurgeError::Pack(format!(
        "Supervisor binary '{name}' is required (supervisor_id is configured) but was not found next to the surge binary. Use the official Surge release bundle for this platform or place '{name}' next to surge."
    )))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
    use crate::context::StorageProvider;
    use crate::crypto::sha256::sha256_hex;
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::platform::detect::current_rid;
    use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index};

    #[test]
    fn test_detect_os_from_rid() {
        assert_eq!(detect_os_from_rid("linux-x64"), "linux");
        assert_eq!(detect_os_from_rid("win-arm64"), "win");
        assert_eq!(detect_os_from_rid("osx-x64"), "osx");
        assert_eq!(detect_os_from_rid("unknown"), "unknown");
    }

    #[test]
    fn test_supervisor_binary_name_follows_target_rid() {
        assert_eq!(supervisor_binary_name_for_rid("linux-x64"), "surge-supervisor");
        assert_eq!(supervisor_binary_name_for_rid("osx-arm64"), "surge-supervisor");
        assert_eq!(supervisor_binary_name_for_rid("win-x64"), "surge-supervisor.exe");
    }

    #[test]
    fn test_package_artifact_creation() {
        let artifact = PackageArtifact {
            filename: "test.tar.zst".to_string(),
            size: 1024,
            sha256: "abc123".to_string(),
            is_delta: false,
            from_version: String::new(),
            patch_format: String::new(),
            bytes: b"test".to_vec(),
        };
        assert!(!artifact.is_delta);
        assert_eq!(artifact.size, 1024);
        assert_eq!(artifact.bytes(), b"test");
    }

    #[test]
    fn test_native_library_candidates_for_known_rids() {
        assert_eq!(
            native_library_candidates_for_rid("linux-x64"),
            vec!["libsurge.so", "surge.so"]
        );
        assert_eq!(
            native_library_candidates_for_rid("osx-arm64"),
            vec!["libsurge.dylib", "surge.dylib"]
        );
        assert_eq!(
            native_library_candidates_for_rid("win-x64"),
            vec!["surge.dll", "libsurge.dll"]
        );
    }

    #[test]
    fn test_validate_surge_dotnet_native_runtime_bundle_requires_matching_native_lib() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path();
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");

        let err = validate_surge_dotnet_native_runtime_bundle(artifacts, "linux-x64")
            .expect_err("validation should fail without native library");
        assert!(
            err.to_string().contains("Surge.NET.dll found in artifacts"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_validate_surge_dotnet_native_runtime_bundle_accepts_matching_native_lib() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path();
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");
        std::fs::write(artifacts.join("libsurge.so"), b"native").expect("native lib should be written");

        validate_surge_dotnet_native_runtime_bundle(artifacts, "linux-x64")
            .expect("validation should pass with native library");
    }

    #[test]
    fn test_validate_surge_dotnet_native_runtime_bundle_rejects_toolchain_fallback() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path().join("artifacts");
        std::fs::create_dir_all(&artifacts).expect("artifacts dir should be created");
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");

        let err = validate_surge_dotnet_native_runtime_bundle(&artifacts, "linux-x64")
            .expect_err("validation should require the runtime in artifacts");
        assert!(err.to_string().contains("pack stays hermetic"));
    }

    #[tokio::test]
    async fn test_build_delta_restores_previous_full_from_delta_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let artifacts_root = tmp.path().join("artifacts");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&artifacts_root).unwrap();
        std::fs::write(artifacts_root.join("payload.txt"), b"v3 payload").unwrap();

        let app_id = "demo";
        let rid = current_rid();
        let manifest_path = tmp.path().join("surge.yml");
        let manifest_yaml = format!(
            r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
apps:
  - id: {app_id}
    target:
      rid: {rid}
",
            bucket = store_root.display()
        );
        std::fs::write(&manifest_path, manifest_yaml).unwrap();

        let mut packer_v1 = ArchivePacker::new(3).unwrap();
        packer_v1.add_buffer("payload.txt", b"v1 payload", 0o644).unwrap();
        let full_v1 = packer_v1.finalize().unwrap();

        let mut packer_v2 = ArchivePacker::new(3).unwrap();
        packer_v2.add_buffer("payload.txt", b"v2 payload", 0o644).unwrap();
        let full_v2 = packer_v2.finalize().unwrap();

        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();

        let full_v1_name = format!("{app_id}-1.0.0-{rid}-full.tar.zst");
        let full_v2_name = format!("{app_id}-1.1.0-{rid}-full.tar.zst");
        let delta_v2_name = format!("{app_id}-1.1.0-{rid}-delta.tar.zst");

        std::fs::write(store_root.join(&full_v1_name), &full_v1).unwrap();
        std::fs::write(store_root.join(&delta_v2_name), &delta_v2).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![
                ReleaseEntry {
                    version: "1.0.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: "linux".to_string(),
                    rid: rid.clone(),
                    is_genesis: true,
                    full_filename: full_v1_name,
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
                    environment: BTreeMap::new(),
                },
                ReleaseEntry {
                    version: "1.1.0".to_string(),
                    channels: vec!["stable".to_string()],
                    os: "linux".to_string(),
                    rid: rid.clone(),
                    is_genesis: false,
                    full_filename: full_v2_name,
                    full_size: full_v2.len() as i64,
                    full_sha256: sha256_hex(&full_v2),
                    deltas: vec![DeltaArtifact::bsdiff_zstd(
                        "primary",
                        "1.0.0",
                        &format!("{app_id}-1.1.0-{rid}-delta.tar.zst"),
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
                    environment: BTreeMap::new(),
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

        let mut builder = PackBuilder::new(
            ctx,
            manifest_path.to_str().unwrap(),
            app_id,
            &rid,
            "1.2.0",
            artifacts_root.to_str().unwrap(),
        )
        .unwrap();

        builder.build(None).await.unwrap();
        let delta = builder
            .artifacts()
            .iter()
            .find(|artifact| artifact.is_delta)
            .expect("delta artifact should be produced");
        assert_eq!(delta.from_version, "1.1.0");
    }
}
