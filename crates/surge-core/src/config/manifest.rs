use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

use crate::error::{Result, SurgeError};

/// Storage configuration within the manifest.
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

/// Lock server configuration within the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LockManifestConfig {
    #[serde(default)]
    pub url: String,
}

/// Per-target (RID) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    pub rid: String,
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
}

/// Per-app configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub main_exe: String,
    #[serde(default)]
    pub targets: Vec<TargetConfig>,
}

/// Supported shortcut locations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShortcutLocation {
    StartMenu,
    Desktop,
    Startup,
}

/// Top-level surge.yml manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurgeManifest {
    #[serde(default = "default_schema")]
    pub schema: i32,
    #[serde(default)]
    pub storage: StorageManifestConfig,
    #[serde(default)]
    pub lock: Option<LockManifestConfig>,
    #[serde(default)]
    pub apps: Vec<AppConfig>,
}

fn default_schema() -> i32 {
    crate::config::constants::SCHEMA_VERSION
}

impl SurgeManifest {
    /// Parse a manifest from YAML bytes.
    pub fn parse(data: &[u8]) -> Result<Self> {
        let manifest: Self = serde_yaml::from_slice(data)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Parse a manifest from a file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        Self::parse(&data)
    }

    /// Serialize the manifest to YAML bytes.
    pub fn to_yaml(&self) -> Result<Vec<u8>> {
        let s = serde_yaml::to_string(self)?;
        Ok(s.into_bytes())
    }

    /// Validate the manifest.
    pub fn validate(&self) -> Result<()> {
        if self.schema != crate::config::constants::SCHEMA_VERSION {
            return Err(SurgeError::Config(format!(
                "Unsupported schema version: {} (expected {})",
                self.schema,
                crate::config::constants::SCHEMA_VERSION,
            )));
        }

        let provider = self.storage.provider.to_lowercase();
        if !["s3", "azure", "gcs", "filesystem"].contains(&provider.as_str()) {
            return Err(SurgeError::Config(format!(
                "Unknown storage provider: {}",
                self.storage.provider
            )));
        }

        if provider != "filesystem" && self.storage.bucket.is_empty() {
            return Err(SurgeError::Config(
                "Storage bucket is required for cloud providers".to_string(),
            ));
        }

        for app in &self.apps {
            if app.id.is_empty() {
                return Err(SurgeError::Config("App id is required".to_string()));
            }

            for target in &app.targets {
                if target.rid.is_empty() {
                    return Err(SurgeError::Config(format!(
                        "Target rid is required for app '{}'",
                        app.id
                    )));
                }

                let mut seen = HashSet::new();
                for shortcut in &target.shortcuts {
                    if !seen.insert(shortcut) {
                        return Err(SurgeError::Config(format!(
                            "Duplicate shortcut location '{shortcut:?}' for app '{}' target '{}'",
                            app.id, target.rid
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Find an app config by ID.
    pub fn find_app(&self, app_id: &str) -> Option<&AppConfig> {
        self.apps.iter().find(|a| a.id == app_id)
    }

    /// Find a target config for an app.
    pub fn find_target(&self, app_id: &str, rid: &str) -> Option<&TargetConfig> {
        self.find_app(app_id)
            .and_then(|app| app.targets.iter().find(|t| t.rid == rid))
    }
}
