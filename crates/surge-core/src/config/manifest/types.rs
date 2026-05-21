use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::constants::{
    PACK_DEFAULT_CHECKPOINT_EVERY, PACK_DEFAULT_COMPRESSION_FORMAT, PACK_DEFAULT_DELTA_STRATEGY,
    PACK_DEFAULT_KEEP_LATEST_FULLS, PACK_DEFAULT_MAX_CHAIN_LENGTH, PACK_DEFAULT_ZSTD_LEVEL,
};
use crate::releases::manifest::COMPRESSION_ZSTD;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageManifestConfig {
    pub provider: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LockManifestConfig {
    #[serde(default)]
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackManifestConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<PackDeltaManifestConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<PackCompressionManifestConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<PackRetentionManifestConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackDeltaManifestConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chain_length: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackCompressionManifestConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackRetentionManifestConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_latest_fulls: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_every: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CacheManifestConfig {
    #[serde(
        default,
        rename = "installArtifacts",
        alias = "install_artifacts",
        skip_serializing_if = "Option::is_none"
    )]
    pub install_artifacts: Option<InstallArtifactCacheManifestConfig>,
}

impl CacheManifestConfig {
    #[must_use]
    pub fn from_install_artifact_cache_policy(policy: InstallArtifactCachePolicy) -> Self {
        Self {
            install_artifacts: Some(InstallArtifactCacheManifestConfig {
                retention: Some(policy.retention),
                keep_full_count: Some(policy.keep_full_count),
            }),
        }
    }

    #[must_use]
    pub fn effective_install_artifact_cache_policy(&self) -> InstallArtifactCachePolicy {
        let mut policy = InstallArtifactCachePolicy::default();
        if let Some(install_artifacts) = self.install_artifacts {
            if let Some(retention) = install_artifacts.retention {
                policy.retention = retention;
            }
            if let Some(keep_full_count) = install_artifacts.keep_full_count {
                policy.keep_full_count = keep_full_count;
            }
        }
        policy
    }

    #[must_use]
    pub fn is_default(&self) -> bool {
        self.install_artifacts.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct InstallArtifactCacheManifestConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<InstallArtifactCacheRetention>,
    #[serde(
        default,
        rename = "keepFullCount",
        alias = "keep_full_count",
        skip_serializing_if = "Option::is_none"
    )]
    pub keep_full_count: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum InstallArtifactCacheRetention {
    #[default]
    ReleaseGraph,
    LatestFull,
    JustInstalled,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallArtifactCachePolicy {
    #[serde(default)]
    pub retention: InstallArtifactCacheRetention,
    #[serde(
        default = "default_install_artifact_cache_keep_full_count",
        rename = "keepFullCount",
        alias = "keep_full_count"
    )]
    pub keep_full_count: u32,
}

impl Default for InstallArtifactCachePolicy {
    fn default() -> Self {
        Self {
            retention: InstallArtifactCacheRetention::ReleaseGraph,
            keep_full_count: default_install_artifact_cache_keep_full_count(),
        }
    }
}

const fn default_install_artifact_cache_keep_full_count() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetConfig {
    pub rid: String,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub distro: String,
    #[serde(default)]
    pub variant: String,
    #[serde(default)]
    pub artifacts_dir: String,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub icon: String,
    #[serde(default)]
    pub shortcuts: Vec<ShortcutLocation>,
    #[serde(default, alias = "persistentAssets")]
    pub persistent_assets: Vec<String>,
    #[serde(default)]
    pub installers: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<TargetCompatibilityConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TargetCompatibilityConfig {
    #[serde(
        default,
        rename = "os-release",
        alias = "os_release",
        skip_serializing_if = "Option::is_none"
    )]
    pub os_release: Option<OsReleaseCompatibilityConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuCompatibilityConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub packages: BTreeMap<String, String>,
}

impl TargetCompatibilityConfig {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.os_release.is_none() && self.gpu.is_none() && self.files.is_empty() && self.packages.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OsReleaseCompatibilityConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(
        default,
        rename = "version-id",
        alias = "version_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub version_id: Option<String>,
    #[serde(
        default,
        rename = "id-like",
        alias = "id_like",
        skip_serializing_if = "Option::is_none"
    )]
    pub id_like: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GpuCompatibilityConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, alias = "main")]
    pub main_exe: String,
    #[serde(default, alias = "installDirectory")]
    pub install_directory: String,
    #[serde(default, alias = "supervisorid")]
    pub supervisor_id: String,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub icon: String,
    #[serde(default)]
    pub shortcuts: Vec<ShortcutLocation>,
    #[serde(default, alias = "persistentAssets")]
    pub persistent_assets: Vec<String>,
    #[serde(default)]
    pub installers: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub targets: Vec<TargetConfig>,
    #[serde(default, alias = "target", skip_serializing_if = "Option::is_none")]
    pub target: Option<TargetConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShortcutLocation {
    StartMenu,
    Desktop,
    Startup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstallerType {
    Online,
    Offline,
    OnlineGui,
    OfflineGui,
}

impl InstallerType {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "online" => Some(Self::Online),
            "offline" => Some(Self::Offline),
            "online-gui" => Some(Self::OnlineGui),
            "offline-gui" => Some(Self::OfflineGui),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Offline => "offline",
            Self::OnlineGui => "online-gui",
            Self::OfflineGui => "offline-gui",
        }
    }

    #[must_use]
    pub fn is_gui(self) -> bool {
        matches!(self, Self::OnlineGui | Self::OfflineGui)
    }

    #[must_use]
    pub fn is_offline(self) -> bool {
        matches!(self, Self::Offline | Self::OfflineGui)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackDeltaStrategy {
    SparseFileOps,
    ArchiveChunkedBsdiff,
    ArchiveBsdiff,
}

impl PackDeltaStrategy {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            PACK_DEFAULT_DELTA_STRATEGY => Some(Self::SparseFileOps),
            "archive-chunked-bsdiff" => Some(Self::ArchiveChunkedBsdiff),
            "archive-bsdiff" => Some(Self::ArchiveBsdiff),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SparseFileOps => "sparse-file-ops",
            Self::ArchiveChunkedBsdiff => "archive-chunked-bsdiff",
            Self::ArchiveBsdiff => "archive-bsdiff",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackCompressionFormat {
    Zstd,
}

impl PackCompressionFormat {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            PACK_DEFAULT_COMPRESSION_FORMAT => Some(Self::Zstd),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zstd => COMPRESSION_ZSTD,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackPolicy {
    pub delta_strategy: PackDeltaStrategy,
    pub compression_format: PackCompressionFormat,
    pub compression_level: i32,
    pub max_chain_length: u32,
    pub keep_latest_fulls: u32,
    pub checkpoint_every: u32,
}

impl Default for PackPolicy {
    fn default() -> Self {
        Self {
            delta_strategy: PackDeltaStrategy::SparseFileOps,
            compression_format: PackCompressionFormat::Zstd,
            compression_level: PACK_DEFAULT_ZSTD_LEVEL,
            max_chain_length: PACK_DEFAULT_MAX_CHAIN_LENGTH,
            keep_latest_fulls: PACK_DEFAULT_KEEP_LATEST_FULLS,
            checkpoint_every: PACK_DEFAULT_CHECKPOINT_EVERY,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChannelManifestConfig {
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurgeManifest {
    #[serde(default = "default_schema")]
    pub schema: i32,
    #[serde(default)]
    pub storage: StorageManifestConfig,
    #[serde(default)]
    pub lock: Option<LockManifestConfig>,
    #[serde(default)]
    pub channels: Vec<ChannelManifestConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack: Option<PackManifestConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheManifestConfig>,
    #[serde(default)]
    pub apps: Vec<AppConfig>,
}

fn default_schema() -> i32 {
    crate::config::constants::SCHEMA_VERSION
}
