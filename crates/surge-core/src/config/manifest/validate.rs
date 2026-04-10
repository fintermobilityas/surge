use std::collections::HashSet;
use std::path::{Component, Path};

use serde_yaml::Value;

use crate::error::{Result, SurgeError};
use crate::releases::manifest::COMPRESSION_ZSTD;

use super::types::{InstallerType, PackCompressionFormat, PackDeltaStrategy, SurgeManifest};

impl SurgeManifest {
    pub(super) fn validate(&self) -> Result<()> {
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
                    "Duplicate top-level channel '{name}'",
                    name = channel.name
                )));
            }
        }

        if let Some(pack) = &self.pack {
            if let Some(delta) = &pack.delta {
                if let Some(strategy) = &delta.strategy
                    && PackDeltaStrategy::parse(strategy).is_none()
                {
                    return Err(SurgeError::Config(format!(
                        "Unsupported pack delta strategy '{strategy}'. Supported values: sparse-file-ops, archive-chunked-bsdiff, archive-bsdiff"
                    )));
                }
                if delta.max_chain_length == Some(0) {
                    return Err(SurgeError::Config(
                        "pack.delta.max_chain_length must be greater than zero".to_string(),
                    ));
                }
            }

            if let Some(compression) = &pack.compression {
                if let Some(format) = &compression.format
                    && PackCompressionFormat::parse(format).is_none()
                {
                    return Err(SurgeError::Config(format!(
                        "Unsupported pack compression format '{format}'. Supported values: {COMPRESSION_ZSTD}"
                    )));
                }
                if let Some(level) = compression.level
                    && !(1..=22).contains(&level)
                {
                    return Err(SurgeError::Config(format!(
                        "pack.compression.level must be between 1 and 22, got {level}"
                    )));
                }
            }

            if let Some(retention) = &pack.retention {
                if retention.keep_latest_fulls == Some(0) {
                    return Err(SurgeError::Config(
                        "pack.retention.keep_latest_fulls must be greater than zero".to_string(),
                    ));
                }
                if retention.checkpoint_every == Some(0) {
                    return Err(SurgeError::Config(
                        "pack.retention.checkpoint_every must be greater than zero".to_string(),
                    ));
                }
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
                            "Unsupported installer '{}' for app '{}' target '{}'. Supported values: online, offline, online-gui, offline-gui",
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
}

fn normalize_provider(raw: &str) -> String {
    raw.trim().to_ascii_lowercase().replace('-', "_")
}

pub(super) fn reject_embedded_storage_credentials(root: &Value) -> Result<()> {
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

pub(super) fn canonicalize_installers(installers: &[String]) -> Vec<String> {
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
