use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::manifest::ShortcutLocation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallerManifest {
    pub schema: i32,
    pub format: String,
    pub ui: String,
    pub installer_type: String,
    pub app_id: String,
    pub rid: String,
    pub version: String,
    pub channel: String,
    pub generated_utc: String,
    #[serde(default)]
    pub headless_default_if_no_display: bool,
    pub release_index_key: String,
    pub storage: InstallerStorage,
    pub release: InstallerRelease,
    pub runtime: InstallerRuntime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallerStorage {
    pub provider: String,
    pub bucket: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallerRelease {
    pub full_filename: String,
    #[serde(default)]
    pub delta_filename: String,
    #[serde(default)]
    pub delta_algorithm: String,
    #[serde(default)]
    pub delta_patch_format: String,
    #[serde(default)]
    pub delta_compression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallerRuntime {
    pub name: String,
    pub main_exe: String,
    #[serde(default)]
    pub install_directory: String,
    #[serde(default)]
    pub supervisor_id: String,
    #[serde(default)]
    pub icon: String,
    #[serde(default)]
    pub shortcuts: Vec<ShortcutLocation>,
    #[serde(default)]
    pub persistent_assets: Vec<String>,
    #[serde(default)]
    pub installers: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}
