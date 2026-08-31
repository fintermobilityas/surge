use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::context::StorageProvider;
use crate::error::{Result, SurgeError};

use super::InstallProfile;

pub const RUNTIME_MANIFEST_RELATIVE_PATH: &str = ".surge/runtime.yml";
pub const LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH: &str = ".surge/surge.yml";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeManifestMetadata<'a> {
    pub version: &'a str,
    pub channel: &'a str,
    pub storage_provider: &'a str,
    pub storage_bucket: &'a str,
    pub storage_region: &'a str,
    pub storage_endpoint: &'a str,
}

impl<'a> RuntimeManifestMetadata<'a> {
    #[must_use]
    pub fn new(
        version: &'a str,
        channel: &'a str,
        storage_provider: &'a str,
        storage_bucket: &'a str,
        storage_region: &'a str,
        storage_endpoint: &'a str,
    ) -> Self {
        Self {
            version,
            channel,
            storage_provider,
            storage_bucket,
            storage_region,
            storage_endpoint,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeManifestFile<'a> {
    id: &'a str,
    version: &'a str,
    channel: &'a str,
    #[serde(rename = "installDirectory")]
    install_directory: &'a str,
    #[serde(rename = "mainExe", skip_serializing_if = "str::is_empty")]
    main_exe: &'a str,
    #[serde(rename = "supervisorId", skip_serializing_if = "str::is_empty")]
    supervisor_id: &'a str,
    provider: &'a str,
    bucket: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    region: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    endpoint: &'a str,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RuntimeManifestIdentity {
    #[serde(rename = "id", default)]
    pub(crate) app_id: String,
    #[serde(default)]
    pub(crate) version: String,
    #[serde(rename = "mainExe", default)]
    pub(crate) main_exe: String,
    #[serde(rename = "supervisorId", default)]
    pub(crate) supervisor_id: String,
}

#[derive(Debug, Clone, Default)]
pub(super) struct RuntimeManifestSnapshot {
    runtime_manifest: Option<Vec<u8>>,
    legacy_runtime_manifest: Option<Vec<u8>>,
}

impl RuntimeManifestSnapshot {
    pub(super) fn capture(active_app_dir: &Path) -> Result<Self> {
        Ok(Self {
            runtime_manifest: read_optional_file(&active_app_dir.join(RUNTIME_MANIFEST_RELATIVE_PATH))?,
            legacy_runtime_manifest: read_optional_file(&active_app_dir.join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH))?,
        })
    }

    pub(super) fn restore(&self, active_app_dir: &Path) -> Result<()> {
        restore_optional_file(
            &active_app_dir.join(RUNTIME_MANIFEST_RELATIVE_PATH),
            self.runtime_manifest.as_deref(),
        )?;
        restore_optional_file(
            &active_app_dir.join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH),
            self.legacy_runtime_manifest.as_deref(),
        )
    }
}

fn read_optional_file(path: &Path) -> Result<Option<Vec<u8>>> {
    if path.is_file() {
        Ok(Some(std::fs::read(path)?))
    } else {
        Ok(None)
    }
}

fn restore_optional_file(path: &Path, contents: Option<&[u8]>) -> Result<()> {
    if let Some(contents) = contents {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, contents)?;
    } else if path.exists() {
        if path.is_dir() {
            std::fs::remove_dir_all(path)?;
        } else {
            std::fs::remove_file(path)?;
        }
    }

    Ok(())
}

#[must_use]
pub fn storage_provider_manifest_name(provider: Option<StorageProvider>) -> &'static str {
    match provider.unwrap_or(StorageProvider::Filesystem) {
        StorageProvider::S3 => "s3",
        StorageProvider::AzureBlob => "azure",
        StorageProvider::Gcs => "gcs",
        StorageProvider::Filesystem => "filesystem",
        StorageProvider::GitHubReleases => "github_releases",
    }
}

pub fn write_runtime_manifest(
    active_app_dir: &Path,
    profile: &InstallProfile<'_>,
    metadata: &RuntimeManifestMetadata<'_>,
) -> Result<PathBuf> {
    let manifest = RuntimeManifestFile {
        id: profile.app_id.trim(),
        version: metadata.version.trim(),
        channel: metadata.channel.trim(),
        install_directory: profile.install_directory.trim(),
        main_exe: profile.main_exe.trim(),
        supervisor_id: profile.supervisor_id.trim(),
        provider: metadata.storage_provider.trim(),
        bucket: metadata.storage_bucket.trim(),
        region: metadata.storage_region.trim(),
        endpoint: metadata.storage_endpoint.trim(),
    };

    if manifest.id.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: app id is empty".to_string(),
        ));
    }
    if manifest.version.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: version is empty".to_string(),
        ));
    }
    if manifest.channel.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: channel is empty".to_string(),
        ));
    }
    if manifest.provider.is_empty() {
        return Err(SurgeError::Config(
            "Cannot write runtime manifest: storage provider is empty".to_string(),
        ));
    }

    let runtime_manifest_path = active_app_dir.join(RUNTIME_MANIFEST_RELATIVE_PATH);
    if let Some(parent) = runtime_manifest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let legacy_manifest_path = active_app_dir.join(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH);
    if let Some(parent) = legacy_manifest_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut yaml = serde_yaml::to_string(&manifest)
        .map_err(|e| SurgeError::Config(format!("Failed to serialize runtime manifest: {e}")))?;
    if !yaml.ends_with('\n') {
        yaml.push('\n');
    }
    std::fs::write(&runtime_manifest_path, &yaml)?;
    std::fs::write(&legacy_manifest_path, yaml)?;
    Ok(runtime_manifest_path)
}

pub fn read_runtime_manifest_version(active_app_dir: &Path) -> Result<Option<String>> {
    Ok(read_runtime_manifest_identity(active_app_dir)?.map(|identity| identity.version))
}

pub(crate) fn read_runtime_manifest_identity(active_app_dir: &Path) -> Result<Option<RuntimeManifestIdentity>> {
    for relative_path in [RUNTIME_MANIFEST_RELATIVE_PATH, LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH] {
        let path = active_app_dir.join(relative_path);
        if !path.is_file() {
            continue;
        }

        let raw = std::fs::read(&path)?;
        let mut manifest: RuntimeManifestIdentity = serde_yaml::from_slice(&raw)
            .map_err(|e| SurgeError::Config(format!("Failed to parse runtime manifest '{}': {e}", path.display())))?;
        if !manifest.version.trim().is_empty() {
            manifest.app_id = manifest.app_id.trim().to_string();
            manifest.version = manifest.version.trim().to_string();
            manifest.main_exe = manifest.main_exe.trim().to_string();
            manifest.supervisor_id = manifest.supervisor_id.trim().to_string();
            return Ok(Some(manifest));
        }
    }

    Ok(None)
}
