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
    #[serde(default)]
    pub apps: Vec<AppConfig>,
}

fn default_schema() -> i32 {
    crate::config::constants::SCHEMA_VERSION
}
