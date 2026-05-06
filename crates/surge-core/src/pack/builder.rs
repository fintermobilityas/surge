//! Pack builder: create full and delta packages, upload to storage.

mod delta;
mod full;
mod release_index;
mod staging;
mod toolchain;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::config::manifest::{PackPolicy, ShortcutLocation, SurgeManifest};
use crate::context::Context;
use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;
use crate::releases::manifest::UNRECORDED_ZSTD_WORKERS;
use crate::storage::{StorageBackend, create_storage_backend};

pub(crate) use self::staging::build_canonical_archive_from_directory;

pub const PACKAGE_METADATA_SCHEMA_VERSION: u32 = 1;
pub const PACKAGE_METADATA_SUFFIX: &str = ".metadata.yml";

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
    /// zstd compression level that was actually used to build this artifact.
    /// Recorded on full-archive artifacts so the release index can persist it;
    /// future promotes read this to rebuild channel-aware deltas without
    /// guessing what the original `pack` configuration was.
    pub zstd_compression_level: i32,
    /// zstd worker count that was actually used to build this artifact
    /// (0 = single-threaded). Recorded for the same reason as
    /// `zstd_compression_level`.
    pub zstd_workers: u32,
    bytes: Vec<u8>,
}

/// Metadata that `surge pack` writes next to a full package so a later
/// `surge push` can preserve the original archive encoding in the release
/// index.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PackageArtifactMetadata {
    pub schema: u32,
    pub app_id: String,
    pub version: String,
    pub rid: String,
    pub archive_filename: String,
    pub archive_size: i64,
    pub archive_sha256: String,
    pub full_compression_level: i32,
    pub full_zstd_workers: i32,
}

#[derive(Debug, Clone)]
pub struct TimedArtifact {
    pub filename: String,
    pub size_bytes: u64,
    pub is_delta: bool,
    pub duration: Duration,
}

#[derive(Debug, Clone)]
pub struct BuildBreakdown {
    pub full: TimedArtifact,
    pub delta: Option<TimedArtifact>,
}

#[derive(Debug, Clone)]
pub struct PushBreakdown {
    pub artifacts: Vec<TimedArtifact>,
    pub release_index_update: Duration,
    pub total: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BundledArtifact {
    source: PathBuf,
    archive_name: String,
}

impl PackageArtifact {
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl PackageArtifactMetadata {
    #[must_use]
    pub fn for_full_artifact(app_id: &str, version: &str, rid: &str, artifact: &PackageArtifact) -> Self {
        Self {
            schema: PACKAGE_METADATA_SCHEMA_VERSION,
            app_id: app_id.to_string(),
            version: version.to_string(),
            rid: rid.to_string(),
            archive_filename: artifact.filename.clone(),
            archive_size: artifact.size,
            archive_sha256: artifact.sha256.clone(),
            full_compression_level: artifact.zstd_compression_level,
            full_zstd_workers: i32::try_from(artifact.zstd_workers).unwrap_or(UNRECORDED_ZSTD_WORKERS),
        }
    }
}

#[must_use]
pub fn package_metadata_filename(archive_filename: &str) -> String {
    format!("{archive_filename}{PACKAGE_METADATA_SUFFIX}")
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
    pack_policy: PackPolicy,
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
            pack_policy,
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
        self.build_with_breakdown(progress).await.map(|_| ())
    }

    /// Build the full and delta packages and return per-artifact timings.
    ///
    /// The optional `progress` callback receives `(items_done, items_total)`.
    pub async fn build_with_breakdown(&mut self, progress: Option<&dyn Fn(i32, i32)>) -> Result<BuildBreakdown> {
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
        let full_started = Instant::now();
        let full_artifact = self.build_full_package()?;
        let full_duration = full_started.elapsed();
        let full_timing = TimedArtifact {
            filename: full_artifact.filename.clone(),
            size_bytes: u64::try_from(full_artifact.size).unwrap_or(0),
            is_delta: false,
            duration: full_duration,
        };
        self.artifacts.push(full_artifact);
        report(1);

        // Step 2: Attempt delta package (non-fatal if it fails)
        let delta_started = Instant::now();
        let delta = match self.build_delta_package().await {
            Ok(Some(delta_artifact)) => {
                let timing = TimedArtifact {
                    filename: delta_artifact.filename.clone(),
                    size_bytes: u64::try_from(delta_artifact.size).unwrap_or(0),
                    is_delta: true,
                    duration: delta_started.elapsed(),
                };
                self.artifacts.push(delta_artifact);
                debug!("Delta package built successfully");
                Some(timing)
            }
            Ok(None) => {
                debug!("No previous version for delta, skipping");
                None
            }
            Err(e) if matches!(e, SurgeError::Integrity(_)) => {
                self.artifacts.clear();
                return Err(e);
            }
            Err(e) => {
                warn!("Delta package build failed (non-fatal): {e}");
                None
            }
        };
        report(2);

        info!(artifact_count = self.artifacts.len(), "Package build complete");

        Ok(BuildBreakdown {
            full: full_timing,
            delta,
        })
    }

    /// Upload built packages to storage and update the release index.
    ///
    /// The optional `progress` callback receives `(items_done, items_total)`.
    pub async fn push(&self, channel: &str, progress: Option<&dyn Fn(i32, i32)>) -> Result<()> {
        self.push_with_breakdown(channel, progress).await.map(|_| ())
    }

    /// Upload built packages to storage and return per-artifact upload timings.
    ///
    /// The optional `progress` callback receives `(items_done, items_total)`.
    pub async fn push_with_breakdown(
        &self,
        channel: &str,
        progress: Option<&dyn Fn(i32, i32)>,
    ) -> Result<PushBreakdown> {
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
        let push_started = Instant::now();
        let mut artifact_timings = Vec::with_capacity(self.artifacts.len());

        // Upload each artifact
        for (i, artifact) in self.artifacts.iter().enumerate() {
            self.ctx.check_cancelled()?;

            debug!(filename = %artifact.filename, "Uploading artifact");
            let upload_started = Instant::now();
            self.storage
                .put_object(&artifact.filename, artifact.bytes(), "application/octet-stream")
                .await?;
            artifact_timings.push(TimedArtifact {
                filename: artifact.filename.clone(),
                size_bytes: u64::try_from(artifact.size).unwrap_or(0),
                is_delta: artifact.is_delta,
                duration: upload_started.elapsed(),
            });

            report(i32::try_from(i).unwrap_or(i32::MAX - 1) + 1);
        }

        // Update the release index
        let release_index_started = Instant::now();
        self.update_release_index(channel).await?;
        let release_index_update = release_index_started.elapsed();
        report(total);

        info!("Push complete");
        Ok(PushBreakdown {
            artifacts: artifact_timings,
            release_index_update,
            total: push_started.elapsed(),
        })
    }

    /// Get the list of built artifacts.
    #[must_use]
    pub fn artifacts(&self) -> &[PackageArtifact] {
        &self.artifacts
    }

    /// Write built artifacts to `output_dir`.
    pub fn write_artifacts_to(&self, output_dir: &Path) -> Result<Vec<PathBuf>> {
        std::fs::create_dir_all(output_dir)?;

        let artifact_paths = self
            .artifacts
            .iter()
            .map(|artifact| {
                let path = output_dir.join(&artifact.filename);
                write_file_atomic(&path, artifact.bytes())?;
                Ok(path)
            })
            .collect::<Result<Vec<_>>>()?;

        for artifact in self.artifacts.iter().filter(|artifact| !artifact.is_delta) {
            let metadata = PackageArtifactMetadata::for_full_artifact(&self.app_id, &self.version, &self.rid, artifact);
            let metadata_yaml = serde_yaml::to_string(&metadata)?;
            let metadata_path = output_dir.join(package_metadata_filename(&artifact.filename));
            write_file_atomic(&metadata_path, metadata_yaml.as_bytes())?;
        }

        Ok(artifact_paths)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use super::*;
    use crate::archive::extractor::extract_to;
    use crate::archive::packer::ArchivePacker;
    use crate::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
    use crate::context::StorageProvider;
    use crate::crypto::sha256::sha256_hex;
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::platform::detect::current_rid;
    use crate::releases::artifact_cache::cache_path_for_key;
    use crate::releases::manifest::{
        DeltaArtifact, PATCH_FORMAT_SPARSE_FILE_OPS_V1, ReleaseEntry, ReleaseIndex, compress_release_index,
        decompress_release_index,
    };
    use crate::releases::restore::{
        RestoreOptions, restore_full_archive_for_version, restore_full_archive_for_version_with_options,
    };

    #[test]
    fn test_detect_os_from_rid() {
        assert_eq!(release_index::detect_os_from_rid("linux-x64"), "linux");
        assert_eq!(release_index::detect_os_from_rid("win-arm64"), "win");
        assert_eq!(release_index::detect_os_from_rid("osx-x64"), "osx");
        assert_eq!(release_index::detect_os_from_rid("unknown"), "unknown");
    }

    #[test]
    fn test_supervisor_binary_name_follows_target_rid() {
        assert_eq!(
            toolchain::supervisor_binary_name_for_rid("linux-x64"),
            "surge-supervisor"
        );
        assert_eq!(
            toolchain::supervisor_binary_name_for_rid("osx-arm64"),
            "surge-supervisor"
        );
        assert_eq!(
            toolchain::supervisor_binary_name_for_rid("win-x64"),
            "surge-supervisor.exe"
        );
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
            zstd_compression_level: 0,
            zstd_workers: 0,
            bytes: b"test".to_vec(),
        };
        assert!(!artifact.is_delta);
        assert_eq!(artifact.size, 1024);
        assert_eq!(artifact.bytes(), b"test");
    }

    #[test]
    fn test_native_library_candidates_for_known_rids() {
        assert_eq!(
            toolchain::native_library_candidates_for_rid("linux-x64"),
            vec!["libsurge.so", "surge.so"]
        );
        assert_eq!(
            toolchain::native_library_candidates_for_rid("osx-arm64"),
            vec!["libsurge.dylib", "surge.dylib"]
        );
        assert_eq!(
            toolchain::native_library_candidates_for_rid("win-x64"),
            vec!["surge.dll", "libsurge.dll"]
        );
    }

    #[test]
    fn test_resolve_surge_dotnet_native_runtime_bundle_requires_matching_native_lib() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path();
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");

        let err =
            toolchain::resolve_surge_dotnet_native_runtime_bundle_with_host(artifacts, "linux-x64", "linux-x64", &[])
                .expect_err("validation should fail without native library");
        assert!(
            err.to_string().contains("Surge.NET.dll found in artifacts"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_resolve_surge_dotnet_native_runtime_bundle_accepts_matching_native_lib() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path();
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");
        std::fs::write(artifacts.join("libsurge.so"), b"native").expect("native lib should be written");

        let bundled =
            toolchain::resolve_surge_dotnet_native_runtime_bundle_with_host(artifacts, "linux-x64", "linux-x64", &[])
                .expect("validation should pass with native library");
        assert!(bundled.is_none(), "existing artifact should not be rebundled");
    }

    #[test]
    fn test_resolve_surge_dotnet_native_runtime_bundle_uses_toolchain_runtime_when_missing_from_artifacts() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path().join("artifacts");
        let toolchain = tmp.path().join("toolchain");
        std::fs::create_dir_all(&artifacts).expect("artifacts dir should be created");
        std::fs::create_dir_all(&toolchain).expect("toolchain dir should be created");
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");
        let bundled_path = toolchain.join("libsurge.so");
        std::fs::write(&bundled_path, b"native").expect("native lib should be written");

        let bundled = toolchain::resolve_surge_dotnet_native_runtime_bundle_with_host(
            &artifacts,
            "linux-x64",
            "linux-x64",
            &[toolchain],
        )
        .expect("toolchain runtime should be accepted")
        .expect("missing runtime should be bundled");
        assert_eq!(bundled.source, bundled_path);
        assert_eq!(bundled.archive_name, "libsurge.so");
    }

    #[test]
    fn test_resolve_surge_dotnet_native_runtime_bundle_rejects_cross_host_toolchain_fallback() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path().join("artifacts");
        std::fs::create_dir_all(&artifacts).expect("artifacts dir should be created");
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");

        let err =
            toolchain::resolve_surge_dotnet_native_runtime_bundle_with_host(&artifacts, "win-x64", "linux-x64", &[])
                .expect_err("cross-host runtime fallback should fail");
        assert!(err.to_string().contains("host-only"));
    }

    fn deterministic_payload(size: usize, version_marker: u8) -> Vec<u8> {
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut payload = Vec::with_capacity(size);
        for _ in 0..size {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            payload.push((state & 0xff) as u8);
        }
        if let Some(first) = payload.first_mut() {
            *first = version_marker;
        }
        if payload.len() > 1 {
            payload[1] = version_marker.wrapping_mul(3);
        }
        payload
    }

    fn write_demoapp_fixture_artifacts(source_root: &Path, dest: &Path, version_label: &str, marker: u8) {
        std::fs::create_dir_all(dest).unwrap();
        let mut program = std::fs::read_to_string(source_root.join("Program.cs")).unwrap();
        program.push_str("\n// delta-version: ");
        program.push_str(version_label);
        program.push('\n');
        std::fs::write(dest.join("Program.cs"), program).unwrap();
        std::fs::copy(source_root.join("demoapp.csproj"), dest.join("demoapp.csproj")).unwrap();
        std::fs::write(
            dest.join("payload.bin"),
            deterministic_payload(16 * 1024 * 1024, marker),
        )
        .unwrap();
    }

    #[test]
    fn test_materialized_pack_root_with_bundled_artifacts_roundtrips_deterministically() {
        let tmp = tempfile::tempdir().unwrap();
        let artifacts = tmp.path().join("artifacts");
        let bundled_root = tmp.path().join("bundled");
        std::fs::create_dir_all(artifacts.join("nested")).unwrap();
        std::fs::create_dir_all(&bundled_root).unwrap();
        std::fs::write(artifacts.join("app.dll"), b"managed").unwrap();
        std::fs::write(artifacts.join("nested").join("payload.bin"), b"payload").unwrap();
        let supervisor = bundled_root.join("surge-supervisor");
        let native = bundled_root.join("libsurge.so");
        std::fs::write(&supervisor, b"supervisor").unwrap();
        std::fs::write(&native, b"native").unwrap();

        let staging = staging::materialize_canonical_pack_root(
            &artifacts,
            &[
                BundledArtifact {
                    source: supervisor,
                    archive_name: "surge-supervisor".to_string(),
                },
                BundledArtifact {
                    source: native,
                    archive_name: "libsurge.so".to_string(),
                },
            ],
        )
        .unwrap();

        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_directory(staging.path(), "").unwrap();
        let archive = packer.finalize().unwrap();

        let extracted = tmp.path().join("extracted");
        extract_to(&archive, &extracted, None).unwrap();

        let mut roundtrip_packer = ArchivePacker::new(3).unwrap();
        roundtrip_packer.add_directory(&extracted, "").unwrap();
        let roundtrip_archive = roundtrip_packer.finalize().unwrap();

        assert_eq!(roundtrip_archive, archive);
    }

    #[test]
    fn test_legacy_appended_bundled_pack_differs_from_canonical_staged_pack() {
        let tmp = tempfile::tempdir().unwrap();
        let artifacts = tmp.path().join("artifacts");
        let bundled_root = tmp.path().join("bundled");
        std::fs::create_dir_all(artifacts.join("nested")).unwrap();
        std::fs::create_dir_all(&bundled_root).unwrap();
        std::fs::write(artifacts.join("app.dll"), b"managed").unwrap();
        std::fs::write(artifacts.join("nested").join("payload.bin"), b"payload").unwrap();
        let supervisor = bundled_root.join("surge-supervisor");
        let native = bundled_root.join("libsurge.so");
        std::fs::write(&supervisor, b"supervisor").unwrap();
        std::fs::write(&native, b"native").unwrap();

        let mut legacy = ArchivePacker::new(3).unwrap();
        legacy.add_directory(&artifacts, "").unwrap();
        legacy.add_file(&supervisor, "surge-supervisor").unwrap();
        legacy.add_file(&native, "libsurge.so").unwrap();
        let legacy_archive = legacy.finalize().unwrap();

        let staging = staging::materialize_canonical_pack_root(
            &artifacts,
            &[
                BundledArtifact {
                    source: supervisor,
                    archive_name: "surge-supervisor".to_string(),
                },
                BundledArtifact {
                    source: native,
                    archive_name: "libsurge.so".to_string(),
                },
            ],
        )
        .unwrap();
        let mut canonical = ArchivePacker::new(3).unwrap();
        canonical.add_directory(staging.path(), "").unwrap();
        let canonical_archive = canonical.finalize().unwrap();

        assert_ne!(legacy_archive, canonical_archive);
    }

    #[tokio::test]
    async fn test_build_delta_restores_previous_full_from_delta_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let artifacts_root = tmp.path().join("artifacts");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&artifacts_root).unwrap();
        std::fs::write(artifacts_root.join("payload.txt"), b"v3 payload").unwrap();
        std::fs::write(
            artifacts_root.join("stable.bin"),
            deterministic_payload(2 * 1024 * 1024, 77),
        )
        .unwrap();

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
                    full_compression_level: 0,
                    full_zstd_workers: 0,
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
                    full_compression_level: 0,
                    full_zstd_workers: 0,
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

    #[tokio::test]
    async fn test_update_release_index_rejects_existing_index_for_other_app() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let artifacts_root = tmp.path().join("artifacts");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&artifacts_root).unwrap();
        std::fs::write(artifacts_root.join("payload.txt"), b"payload").unwrap();

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

        let index = ReleaseIndex {
            app_id: "other-app".to_string(),
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
            "1.0.0",
            artifacts_root.to_str().unwrap(),
        )
        .unwrap();

        builder.build(None).await.unwrap();
        let err = builder.update_release_index("stable").await.unwrap_err();
        assert!(
            err.to_string()
                .contains("Release index app_id 'other-app' does not match pack app 'demo'")
        );
    }

    #[tokio::test]
    async fn test_build_delta_rejects_inconsistent_base_sha256() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let artifacts_root = tmp.path().join("artifacts");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&artifacts_root).unwrap();
        std::fs::write(artifacts_root.join("payload.txt"), b"v2 payload").unwrap();

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

        let full_v1_name = format!("{app_id}-1.0.0-{rid}-full.tar.zst");
        std::fs::write(store_root.join(&full_v1_name), &full_v1).unwrap();

        let index = ReleaseIndex {
            app_id: app_id.to_string(),
            releases: vec![ReleaseEntry {
                version: "1.0.0".to_string(),
                channels: vec!["stable".to_string()],
                os: "linux".to_string(),
                rid: rid.clone(),
                is_genesis: true,
                full_filename: full_v1_name,
                full_size: full_v1.len() as i64,
                full_sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
                full_compression_level: 0,
                full_zstd_workers: 0,
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

        let mut builder = PackBuilder::new(
            ctx,
            manifest_path.to_str().unwrap(),
            app_id,
            &rid,
            "1.1.0",
            artifacts_root.to_str().unwrap(),
        )
        .unwrap();

        let err = builder.build(None).await.unwrap_err();
        assert!(
            err.to_string().contains("SHA-256 mismatch"),
            "expected SHA-256 mismatch error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_build_delta_fails_on_corrupt_direct_base_even_if_cached_chain_can_reconstruct_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        let artifacts_root = tmp.path().join("artifacts");
        let cache_root = tmp.path().join("cache");
        std::fs::create_dir_all(&store_root).unwrap();
        std::fs::create_dir_all(&artifacts_root).unwrap();
        std::fs::create_dir_all(&cache_root).unwrap();
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

        let mut corrupted_full_v2 = full_v2.clone();
        corrupted_full_v2[0] ^= 0xff;

        std::fs::write(store_root.join(&full_v1_name), &full_v1).unwrap();
        std::fs::write(store_root.join(&full_v2_name), &corrupted_full_v2).unwrap();
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
                    full_filename: full_v1_name.clone(),
                    full_size: full_v1.len() as i64,
                    full_sha256: sha256_hex(&full_v1),
                    full_compression_level: 0,
                    full_zstd_workers: 0,
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
                    full_filename: full_v2_name.clone(),
                    full_size: full_v2.len() as i64,
                    full_sha256: sha256_hex(&full_v2),
                    full_compression_level: 0,
                    full_zstd_workers: 0,
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
                    environment: BTreeMap::new(),
                },
            ],
            ..ReleaseIndex::default()
        };

        let cached_full_v1 = cache_path_for_key(&cache_root, &full_v1_name).unwrap();
        std::fs::create_dir_all(cached_full_v1.parent().unwrap()).unwrap();
        std::fs::write(&cached_full_v1, &full_v1).unwrap();
        let cached_delta_v2 = cache_path_for_key(&cache_root, &delta_v2_name).unwrap();
        std::fs::create_dir_all(cached_delta_v2.parent().unwrap()).unwrap();
        std::fs::write(&cached_delta_v2, &delta_v2).unwrap();

        let backend = crate::storage::filesystem::FilesystemBackend::new(store_root.to_str().unwrap(), "");
        let restored = restore_full_archive_for_version_with_options(
            &backend,
            &index,
            &rid,
            "1.1.0",
            RestoreOptions {
                cache_dir: Some(&cache_root),
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(restored, full_v2);

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

        let err = builder.build(None).await.unwrap_err();
        assert!(
            err.to_string().contains("SHA-256 mismatch"),
            "expected SHA-256 mismatch error, got: {err}"
        );
        assert!(
            builder.artifacts().is_empty(),
            "integrity failures should clear staged artifacts"
        );
    }

    #[tokio::test]
    async fn test_build_and_push_breakdown_reports_full_and_delta_timings() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let store_root = tmp.path().join("store");
        let artifacts_root = tmp.path().join("artifacts");
        std::fs::create_dir_all(&store_root).expect("store dir should exist");
        std::fs::create_dir_all(&artifacts_root).expect("artifacts dir should exist");

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
        std::fs::write(&manifest_path, manifest_yaml).expect("manifest should be written");

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().expect("store root utf8"),
            "",
            "",
            "",
            "",
        );

        std::fs::write(artifacts_root.join("payload.txt"), b"v1 payload").expect("v1 payload should be written");
        std::fs::write(
            artifacts_root.join("stable.bin"),
            deterministic_payload(2 * 1024 * 1024, 77),
        )
        .expect("stable payload should be written");
        let mut builder_v1 = PackBuilder::new(
            Arc::clone(&ctx),
            manifest_path.to_str().expect("manifest path utf8"),
            app_id,
            &rid,
            "1.0.0",
            artifacts_root.to_str().expect("artifacts path utf8"),
        )
        .expect("builder v1");
        let build_v1 = builder_v1
            .build_with_breakdown(None)
            .await
            .expect("first build breakdown should succeed");
        assert!(!build_v1.full.is_delta);
        assert!(
            build_v1.delta.is_none(),
            "first publish should not have a delta artifact"
        );
        assert!(build_v1.full.size_bytes > 0);

        let push_v1 = builder_v1
            .push_with_breakdown("stable", None)
            .await
            .expect("first push breakdown should succeed");
        assert_eq!(push_v1.artifacts.len(), 1);
        assert!(!push_v1.artifacts[0].is_delta);

        std::fs::write(artifacts_root.join("payload.txt"), b"v2 payload").expect("v2 payload should be written");
        let mut builder_v2 = PackBuilder::new(
            Arc::clone(&ctx),
            manifest_path.to_str().expect("manifest path utf8"),
            app_id,
            &rid,
            "1.1.0",
            artifacts_root.to_str().expect("artifacts path utf8"),
        )
        .expect("builder v2");
        let build_v2 = builder_v2
            .build_with_breakdown(None)
            .await
            .expect("second build breakdown should succeed");
        let delta_timing = build_v2.delta.expect("second publish should include a delta artifact");
        assert!(delta_timing.is_delta);
        assert!(delta_timing.size_bytes > 0);

        let push_v2 = builder_v2
            .push_with_breakdown("stable", None)
            .await
            .expect("second push breakdown should succeed");
        assert_eq!(push_v2.artifacts.len(), 2);
        assert!(push_v2.artifacts.iter().any(|artifact| artifact.is_delta));
        assert!(push_v2.release_index_update <= push_v2.total);
    }

    #[tokio::test]
    async fn test_demoapp_fixture_multi_release_delta_stays_small_and_restorable() {
        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();

        let demoapp_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .join("demoapp");
        let artifacts_v1 = tmp.path().join("demoapp-v1");
        let artifacts_v2 = tmp.path().join("demoapp-v2");
        let artifacts_v3 = tmp.path().join("demoapp-v3");
        write_demoapp_fixture_artifacts(&demoapp_root, &artifacts_v1, "1.0.0", 1);
        write_demoapp_fixture_artifacts(&demoapp_root, &artifacts_v2, "1.1.0", 2);
        write_demoapp_fixture_artifacts(&demoapp_root, &artifacts_v3, "1.2.0", 3);

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
    name: Demo App
    main_exe: demoapp
    targets:
      - rid: {rid}
",
            bucket = store_root.display()
        );
        std::fs::write(&manifest_path, manifest_yaml).unwrap();

        let ctx = Arc::new(Context::new());
        ctx.set_storage(
            StorageProvider::Filesystem,
            store_root.to_str().unwrap(),
            "",
            "",
            "",
            "",
        );
        let mut budget = ctx.resource_budget();
        budget.zstd_compression_level = 7;
        ctx.set_resource_budget(budget.clone());

        let mut builder_v1 = PackBuilder::new(
            Arc::clone(&ctx),
            manifest_path.to_str().unwrap(),
            app_id,
            &rid,
            "1.0.0",
            artifacts_v1.to_str().unwrap(),
        )
        .unwrap();
        builder_v1.build(None).await.unwrap();
        let _full_v1 = builder_v1
            .artifacts()
            .iter()
            .find(|artifact| !artifact.is_delta)
            .unwrap()
            .clone();
        builder_v1.push("stable", None).await.unwrap();

        let mut builder_v2 = PackBuilder::new(
            Arc::clone(&ctx),
            manifest_path.to_str().unwrap(),
            app_id,
            &rid,
            "1.1.0",
            artifacts_v2.to_str().unwrap(),
        )
        .unwrap();
        builder_v2.build(None).await.unwrap();
        let full_v2 = builder_v2
            .artifacts()
            .iter()
            .find(|artifact| !artifact.is_delta)
            .unwrap()
            .clone();
        let delta_v2 = builder_v2
            .artifacts()
            .iter()
            .find(|artifact| artifact.is_delta)
            .unwrap()
            .clone();
        assert_eq!(delta_v2.patch_format, PATCH_FORMAT_SPARSE_FILE_OPS_V1);
        assert!(delta_v2.bytes().len() * 100 < full_v2.bytes().len());
        builder_v2.push("stable", None).await.unwrap();

        let mut builder_v3 = PackBuilder::new(
            Arc::clone(&ctx),
            manifest_path.to_str().unwrap(),
            app_id,
            &rid,
            "1.2.0",
            artifacts_v3.to_str().unwrap(),
        )
        .unwrap();
        builder_v3.build(None).await.unwrap();
        let full_v3 = builder_v3
            .artifacts()
            .iter()
            .find(|artifact| !artifact.is_delta)
            .unwrap()
            .clone();
        let delta_v3 = builder_v3
            .artifacts()
            .iter()
            .find(|artifact| artifact.is_delta)
            .unwrap()
            .clone();
        assert_eq!(delta_v3.patch_format, PATCH_FORMAT_SPARSE_FILE_OPS_V1);
        assert!(delta_v3.bytes().len() * 100 < full_v3.bytes().len());
        builder_v3.push("stable", None).await.unwrap();

        let index_bytes = std::fs::read(store_root.join(RELEASES_FILE_COMPRESSED)).unwrap();
        let index = decompress_release_index(&index_bytes).unwrap();
        let backend = crate::storage::filesystem::FilesystemBackend::new(store_root.to_str().unwrap(), "");
        let restored = restore_full_archive_for_version(&backend, &index, &rid, "1.2.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v3.bytes());
    }
}
