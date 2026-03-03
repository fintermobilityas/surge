//! Pack builder: create full and delta packages, upload to storage.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::archive::packer::ArchivePacker;
use crate::config::constants::{RELEASES_FILE_COMPRESSED, SCHEMA_VERSION};
use crate::config::manifest::{ShortcutLocation, SurgeManifest};
use crate::context::Context;
use crate::crypto::sha256::sha256_hex_file;
use crate::diff::wrapper::bsdiff_buffers;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex, compress_release_index, decompress_release_index};
use crate::releases::restore::{find_previous_release_for_rid, restore_full_archive_for_version};
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
    install_directory: String,
    supervisor_id: String,
    icon: String,
    shortcuts: Vec<ShortcutLocation>,
    persistent_assets: Vec<String>,
    installers: Vec<String>,
    environment: BTreeMap<String, String>,
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

        validate_surge_dotnet_native_dependency(&artifacts_path, rid)?;

        Ok(Self {
            ctx,
            app_id: app_id.to_string(),
            rid: rid.to_string(),
            version: version.to_string(),
            main_exe,
            install_directory: app.effective_install_directory(),
            supervisor_id: app.supervisor_id.clone(),
            icon: target.icon.clone(),
            shortcuts: target.shortcuts.clone(),
            persistent_assets: target.persistent_assets.clone(),
            installers: target.installers.clone(),
            environment: target.environment.clone(),
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
        let data = match self.storage.get_object(RELEASES_FILE_COMPRESSED).await {
            Ok(d) => d,
            Err(SurgeError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let index = decompress_release_index(&data)?;

        let previous_release = match find_previous_release_for_rid(&index, &self.rid, &self.version) {
            Some(release) => release,
            None => return Ok(None),
        };

        let prev_data =
            restore_full_archive_for_version(self.storage.as_ref(), &index, &self.rid, &previous_release.version)
                .await?;
        let full_artifact = self
            .artifacts
            .iter()
            .find(|a| !a.is_delta)
            .ok_or_else(|| SurgeError::Pack("Full package not yet built".to_string()))?;
        let new_data = std::fs::read(&full_artifact.path)?;

        // Compute bsdiff
        let patch = bsdiff_buffers(&prev_data, &new_data)?;

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

        Ok(Some(PackageArtifact {
            path: delta_path,
            filename: delta_filename,
            size,
            sha256,
            is_delta: true,
            from_version: previous_release.version.clone(),
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
            delta_filename: delta.map_or(String::new(), |a| a.filename.clone()),
            delta_size: delta.map_or(0, |a| a.size),
            delta_sha256: delta.map_or(String::new(), |a| a.sha256.clone()),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            main_exe: self.main_exe.clone(),
            install_directory: self.install_directory.clone(),
            supervisor_id: self.supervisor_id.clone(),
            icon: self.icon.clone(),
            shortcuts: self.shortcuts.clone(),
            persistent_assets: self.persistent_assets.clone(),
            installers: self.installers.clone(),
            environment: self.environment.clone(),
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

fn validate_surge_dotnet_native_dependency(artifacts_path: &Path, rid: &str) -> Result<()> {
    if !artifacts_path.join("Surge.NET.dll").is_file() {
        return Ok(());
    }

    let candidates = native_library_candidates_for_rid(rid);
    if candidates.iter().any(|name| artifacts_path.join(name).is_file()) {
        return Ok(());
    }

    Err(SurgeError::Pack(format!(
        "Surge.NET.dll found in artifacts, but no native Surge runtime library for RID '{rid}'. Expected one of: {}",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::config::constants::{DEFAULT_ZSTD_LEVEL, RELEASES_FILE_COMPRESSED};
    use crate::context::StorageProvider;
    use crate::crypto::sha256::sha256_hex;
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
    fn test_validate_surge_dotnet_native_dependency_requires_matching_native_lib() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path();
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");

        let err = validate_surge_dotnet_native_dependency(artifacts, "linux-x64")
            .expect_err("validation should fail without native library");
        assert!(
            err.to_string().contains("Surge.NET.dll found in artifacts"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_validate_surge_dotnet_native_dependency_accepts_matching_native_lib() {
        let tmp = tempfile::tempdir().expect("tempdir should be created");
        let artifacts = tmp.path();
        std::fs::write(artifacts.join("Surge.NET.dll"), b"managed").expect("managed dll should be written");
        std::fs::write(artifacts.join("libsurge.so"), b"native").expect("native lib should be written");

        validate_surge_dotnet_native_dependency(artifacts, "linux-x64")
            .expect("validation should pass with native library");
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
                    delta_filename: String::new(),
                    delta_size: 0,
                    delta_sha256: String::new(),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
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
                    delta_filename: delta_v2_name,
                    delta_size: delta_v2.len() as i64,
                    delta_sha256: sha256_hex(&delta_v2),
                    created_utc: chrono::Utc::now().to_rfc3339(),
                    release_notes: String::new(),
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
