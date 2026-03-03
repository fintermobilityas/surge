use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path};

use crate::error::{Result, SurgeError};

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
    Web,
    Offline,
}

impl InstallerType {
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "web" => Some(Self::Web),
            "offline" => Some(Self::Offline),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Web => "web",
            Self::Offline => "offline",
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
    #[serde(default)]
    pub apps: Vec<AppConfig>,
}

fn default_schema() -> i32 {
    crate::config::constants::SCHEMA_VERSION
}

impl SurgeManifest {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let raw: Value = serde_yaml::from_slice(data)?;
        reject_embedded_storage_credentials(&raw)?;
        let mut manifest: Self = serde_yaml::from_value(raw)?;
        manifest.normalize();
        manifest.validate()?;
        Ok(manifest)
    }

    /// Expands multi-target apps with no explicit ID into separate single-target
    /// apps with IDs derived as `{main}-{distro}-{rid}[-{variant}]`.
    /// Also collects channels from apps when top-level channels are empty.
    fn normalize(&mut self) {
        // Expand apps: split multi-target apps without explicit IDs.
        let mut expanded = Vec::new();
        for app in std::mem::take(&mut self.apps) {
            if !app.id.is_empty() {
                expanded.push(app);
                continue;
            }

            let targets: Vec<TargetConfig> = app.iter_targets().cloned().collect();
            if targets.is_empty() {
                expanded.push(app);
                continue;
            }

            let main = if app.main_exe.is_empty() {
                &app.name
            } else {
                &app.main_exe
            };
            for target in targets {
                let mut derived_id = format!("{}-{}-{}", main, target.distro, target.rid);
                if !target.variant.is_empty() {
                    derived_id.push('-');
                    derived_id.push_str(&target.variant);
                }

                expanded.push(AppConfig {
                    id: derived_id,
                    name: app.name.clone(),
                    main_exe: app.main_exe.clone(),
                    install_directory: app.install_directory.clone(),
                    supervisor_id: app.supervisor_id.clone(),
                    channels: app.channels.clone(),
                    os: app.os.clone(),
                    icon: app.icon.clone(),
                    shortcuts: app.shortcuts.clone(),
                    persistent_assets: app.persistent_assets.clone(),
                    installers: app.installers.clone(),
                    environment: app.environment.clone(),
                    targets: vec![target],
                    target: None,
                });
            }
        }
        self.apps = expanded;

        // Collect channels from apps when top-level channels are empty.
        if self.channels.is_empty() {
            let mut seen = HashSet::new();
            for app in &self.apps {
                for channel in &app.channels {
                    if seen.insert(channel.clone()) {
                        self.channels.push(ChannelManifestConfig { name: channel.clone() });
                    }
                }
            }
        }
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        Self::parse(&data)
    }

    pub fn to_yaml(&self) -> Result<Vec<u8>> {
        let s = serde_yaml::to_string(self)?;
        Ok(s.into_bytes())
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema != crate::config::constants::SCHEMA_VERSION && self.schema != 2 {
            return Err(SurgeError::Config(format!(
                "Unsupported schema version: {} (expected {} or 2)",
                self.schema,
                crate::config::constants::SCHEMA_VERSION,
            )));
        }

        let provider = normalize_provider(&self.storage.provider);
        if provider.is_empty() {
            if self.channels.is_empty() {
                return Err(SurgeError::Config(
                    "Storage provider is required unless top-level channels are configured".to_string(),
                ));
            }
        } else {
            if !["s3", "azure", "gcs", "filesystem", "github_releases"].contains(&provider.as_str()) {
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
        }

        let mut global_channels = HashSet::new();
        for channel in &self.channels {
            let name = channel.name.trim();
            if name.is_empty() {
                return Err(SurgeError::Config("Channel name cannot be empty".to_string()));
            }
            if !global_channels.insert(name.to_string()) {
                return Err(SurgeError::Config(format!(
                    "Duplicate top-level channel '{}'",
                    channel.name
                )));
            }
        }

        for app in &self.apps {
            if app.id.is_empty() {
                return Err(SurgeError::Config(
                    "App id is required (set 'id' explicitly or provide 'main'/'distro'/'rid' for auto-derivation)"
                        .to_string(),
                ));
            }

            if app.iter_targets().next().is_none() {
                return Err(SurgeError::Config(format!(
                    "At least one target is required for app '{}'",
                    app.id
                )));
            }

            let mut app_channels = HashSet::new();
            for channel in &app.channels {
                if channel.trim().is_empty() {
                    return Err(SurgeError::Config(format!(
                        "App '{}' has an empty channel entry",
                        app.id
                    )));
                }
                if !app_channels.insert(channel.clone()) {
                    return Err(SurgeError::Config(format!(
                        "Duplicate channel '{}' for app '{}'",
                        channel, app.id
                    )));
                }
                if !global_channels.is_empty() && !global_channels.contains(channel) {
                    return Err(SurgeError::Config(format!(
                        "App '{}' references unknown channel '{}'",
                        app.id, channel
                    )));
                }
            }

            for target in app.iter_targets() {
                if target.rid.is_empty() {
                    return Err(SurgeError::Config(format!(
                        "Target rid is required for app '{}'",
                        app.id
                    )));
                }

                let resolved_target = app.resolve_target(target);
                let mut seen = HashSet::new();
                for shortcut in &resolved_target.shortcuts {
                    if !seen.insert(shortcut) {
                        return Err(SurgeError::Config(format!(
                            "Duplicate shortcut location '{shortcut:?}' for app '{}' target '{}'",
                            app.id, target.rid
                        )));
                    }
                }

                let mut installers_seen = HashSet::new();
                for installer in &resolved_target.installers {
                    if installer.trim().is_empty() {
                        return Err(SurgeError::Config(format!(
                            "Empty installer entry for app '{}' target '{}'",
                            app.id, target.rid
                        )));
                    }
                    let installer_type = InstallerType::parse(installer).ok_or_else(|| {
                        SurgeError::Config(format!(
                            "Unsupported installer '{}' for app '{}' target '{}'. Supported values: web, offline",
                            installer, app.id, target.rid
                        ))
                    })?;

                    if !installers_seen.insert(installer_type) {
                        return Err(SurgeError::Config(format!(
                            "Duplicate installer '{}' for app '{}' target '{}'",
                            installer, app.id, target.rid
                        )));
                    }
                }

                let mut persistent_seen = HashSet::new();
                for asset in &resolved_target.persistent_assets {
                    validate_persistent_asset(asset, &app.id, &target.rid)?;
                    if !persistent_seen.insert(asset.clone()) {
                        return Err(SurgeError::Config(format!(
                            "Duplicate persistent asset '{}' for app '{}' target '{}'",
                            asset, app.id, target.rid
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn find_app(&self, app_id: &str) -> Option<&AppConfig> {
        self.apps.iter().find(|a| a.id == app_id)
    }

    pub fn find_app_with_target(&self, app_id: &str, rid: &str) -> Option<(&AppConfig, TargetConfig)> {
        self.apps
            .iter()
            .filter(|app| app.id == app_id)
            .find_map(|app| app.find_target(rid).map(|target| (app, app.resolve_target(target))))
    }

    pub fn find_target(&self, app_id: &str, rid: &str) -> Option<TargetConfig> {
        self.find_app_with_target(app_id, rid).map(|(_, target)| target)
    }

    #[must_use]
    pub fn app_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.apps.iter().map(|app| app.id.clone()).collect();
        ids.sort();
        ids.dedup();
        ids
    }

    #[must_use]
    pub fn target_rids(&self, app_id: &str) -> Vec<String> {
        let mut rids: Vec<String> = self
            .apps
            .iter()
            .filter(|app| app.id == app_id)
            .flat_map(|app| app.iter_targets().map(|target| target.rid.clone()))
            .filter(|rid| !rid.is_empty())
            .collect();
        rids.sort();
        rids.dedup();
        rids
    }
}

impl AppConfig {
    fn iter_targets(&self) -> impl Iterator<Item = &TargetConfig> {
        self.target.iter().chain(self.targets.iter())
    }

    fn find_target(&self, rid: &str) -> Option<&TargetConfig> {
        self.iter_targets().find(|target| target.rid == rid)
    }

    #[must_use]
    pub fn effective_main_exe(&self) -> String {
        if self.main_exe.trim().is_empty() {
            self.id.clone()
        } else {
            self.main_exe.clone()
        }
    }

    #[must_use]
    pub fn effective_install_directory(&self) -> String {
        if self.install_directory.trim().is_empty() {
            self.id.clone()
        } else {
            self.install_directory.clone()
        }
    }

    /// Inherits app-level defaults into the target where fields are empty.
    #[must_use]
    pub fn resolve_target(&self, target: &TargetConfig) -> TargetConfig {
        let mut resolved = target.clone();
        if resolved.os.is_empty() {
            resolved.os.clone_from(&self.os);
        }
        if resolved.icon.is_empty() {
            resolved.icon.clone_from(&self.icon);
        }
        if resolved.shortcuts.is_empty() {
            resolved.shortcuts.clone_from(&self.shortcuts);
        }
        if resolved.persistent_assets.is_empty() {
            resolved.persistent_assets.clone_from(&self.persistent_assets);
        }
        if resolved.installers.is_empty() {
            resolved.installers.clone_from(&self.installers);
        }
        resolved.installers = canonicalize_installers(&resolved.installers);
        if !self.environment.is_empty() {
            let mut merged = self.environment.clone();
            for (key, value) in &resolved.environment {
                merged.insert(key.clone(), value.clone());
            }
            resolved.environment = merged;
        }
        resolved
    }
}

fn normalize_provider(raw: &str) -> String {
    raw.trim().to_ascii_lowercase().replace('-', "_")
}

fn reject_embedded_storage_credentials(root: &Value) -> Result<()> {
    fn walk(value: &Value, path: &mut Vec<String>, storage_scope: bool) -> Result<()> {
        match value {
            Value::Mapping(mapping) => {
                for (key, child_value) in mapping {
                    let raw_key = key.as_str().unwrap_or_default();
                    let key_normalized = normalize_yaml_key(raw_key);
                    let child_scope = storage_scope
                        || key_normalized == "storage"
                        || key_normalized == "pushfeed"
                        || key_normalized == "updatefeed";

                    if child_scope && is_forbidden_credential_key(&key_normalized) {
                        let mut full_path = path.clone();
                        full_path.push(raw_key.to_string());
                        return Err(SurgeError::Config(format!(
                            "Credentials are not allowed in manifests. Remove '{}'; runtime stores must be publicly readable.",
                            full_path.join(".")
                        )));
                    }

                    path.push(raw_key.to_string());
                    walk(child_value, path, child_scope)?;
                    path.pop();
                }
            }
            Value::Sequence(sequence) => {
                for (index, child_value) in sequence.iter().enumerate() {
                    path.push(format!("[{index}]"));
                    walk(child_value, path, storage_scope)?;
                    path.pop();
                }
            }
            _ => {}
        }

        Ok(())
    }

    let mut path = Vec::new();
    walk(root, &mut path, false)
}

fn normalize_yaml_key(raw: &str) -> String {
    raw.chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase()
}

fn is_forbidden_credential_key(key: &str) -> bool {
    matches!(
        key,
        "accesskey"
            | "secretkey"
            | "apikey"
            | "token"
            | "password"
            | "username"
            | "clientsecret"
            | "privkey"
            | "privatekey"
            | "sastoken"
    )
}

fn canonicalize_installers(installers: &[String]) -> Vec<String> {
    installers
        .iter()
        .map(|installer| {
            InstallerType::parse(installer).map_or_else(
                || installer.trim().to_string(),
                |installer_type| installer_type.as_str().to_string(),
            )
        })
        .collect()
}

fn validate_persistent_asset(path: &str, app_id: &str, rid: &str) -> Result<()> {
    if path.trim().is_empty() {
        return Err(SurgeError::Config(format!(
            "Persistent asset path cannot be empty for app '{app_id}' target '{rid}'"
        )));
    }

    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return Err(SurgeError::Config(format!(
            "Persistent asset path must be relative for app '{app_id}' target '{rid}': {path}"
        )));
    }

    let first_component = candidate
        .components()
        .next()
        .and_then(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .unwrap_or_default();
    if first_component.to_ascii_lowercase().starts_with("app-") {
        return Err(SurgeError::Config(format!(
            "Persistent asset path cannot start with 'app-' for app '{app_id}' target '{rid}': {path}"
        )));
    }

    for component in candidate.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            return Err(SurgeError::Config(format!(
                "Persistent asset path cannot traverse parent/root for app '{app_id}' target '{rid}': {path}"
            )));
        }
    }

    Ok(())
}
