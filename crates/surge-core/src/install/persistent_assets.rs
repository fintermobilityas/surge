use std::path::{Component, Path, PathBuf};

use crate::error::{Result, SurgeError};
use crate::platform::fs::copy_directory;

pub fn copy_persistent_assets(previous_app_dir: &Path, new_app_dir: &Path, assets: &[String]) -> Result<()> {
    for asset in assets {
        let relative = validate_relative_persistent_asset_path(asset)?;
        let source = previous_app_dir.join(&relative);
        if !source.exists() {
            continue;
        }

        let destination = new_app_dir.join(&relative);
        if source.is_dir() {
            if destination.exists() {
                if destination.is_dir() {
                    std::fs::remove_dir_all(&destination)?;
                } else {
                    std::fs::remove_file(&destination)?;
                }
            }
            copy_directory(&source, &destination)?;
        } else {
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if destination.exists() && destination.is_dir() {
                std::fs::remove_dir_all(&destination)?;
            }
            std::fs::copy(&source, &destination)?;
        }
    }

    Ok(())
}

pub fn validate_relative_persistent_asset_path(raw: &str) -> Result<PathBuf> {
    if raw.trim().is_empty() {
        return Err(SurgeError::Update("Persistent asset path cannot be empty".to_string()));
    }

    let candidate = Path::new(raw);
    if candidate.is_absolute() {
        return Err(SurgeError::Update(format!(
            "Persistent asset path must be relative: {raw}"
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
        return Err(SurgeError::Update(format!(
            "Persistent asset path cannot start with 'app-': {raw}"
        )));
    }

    for component in candidate.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            return Err(SurgeError::Update(format!(
                "Persistent asset path cannot contain parent/root traversal: {raw}"
            )));
        }
    }

    Ok(candidate.to_path_buf())
}
