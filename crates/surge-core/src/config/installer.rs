use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::manifest::{CacheManifestConfig, InstallArtifactCachePolicy, ShortcutLocation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstallerUi {
    Console,
    Egui,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallerManifest {
    pub schema: i32,
    pub format: String,
    pub ui: InstallerUi,
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
    #[serde(default, skip_serializing_if = "CacheManifestConfig::is_default")]
    pub cache: CacheManifestConfig,
}

impl InstallerManifest {
    #[must_use]
    pub fn effective_install_artifact_cache_policy(&self) -> InstallArtifactCachePolicy {
        self.cache.effective_install_artifact_cache_policy()
    }
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
    pub full_sha256: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::manifest::InstallArtifactCacheRetention;

    #[test]
    fn installer_manifest_defaults_cache_policy_for_older_manifests() {
        let yaml = br#"schema: 1
format: surge-installer-v1
ui: console
installer_type: online
app_id: demo
rid: linux-x64
version: "1.2.3"
channel: stable
generated_utc: "2026-04-28T00:00:00Z"
headless_default_if_no_display: true
release_index_key: releases.zstd
storage:
  provider: filesystem
  bucket: /tmp/store
release:
  full_filename: demo-full.tar.zst
runtime:
  name: Demo
  main_exe: demo
"#;

        let manifest: InstallerManifest = serde_yaml::from_slice(yaml).expect("installer manifest should parse");
        let policy = manifest.effective_install_artifact_cache_policy();

        assert_eq!(policy.retention, InstallArtifactCacheRetention::ReleaseGraph);
        assert_eq!(policy.keep_full_count, 1);
    }
}
