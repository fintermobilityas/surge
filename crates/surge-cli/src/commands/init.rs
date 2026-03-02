use std::path::Path;

use surge_core::config::manifest::{AppConfig, StorageManifestConfig, SurgeManifest, TargetConfig};
use surge_core::error::Result;
use surge_core::platform::detect::current_rid;

/// Initialize a new surge.yml manifest file.
pub async fn execute(
    manifest_path: &Path,
    app_id: &str,
    name: Option<&str>,
    provider: &str,
    bucket: &str,
) -> Result<()> {
    if manifest_path.exists() {
        tracing::warn!("Manifest already exists at {}", manifest_path.display());
        return Err(surge_core::error::SurgeError::Config(format!(
            "Manifest already exists at {}",
            manifest_path.display()
        )));
    }

    let rid = current_rid();
    let manifest = SurgeManifest {
        schema: surge_core::config::constants::SCHEMA_VERSION,
        storage: StorageManifestConfig {
            provider: provider.to_string(),
            bucket: bucket.to_string(),
            ..Default::default()
        },
        lock: None,
        apps: vec![AppConfig {
            id: app_id.to_string(),
            name: name.unwrap_or(app_id).to_string(),
            targets: vec![TargetConfig {
                rid,
                artifacts_dir: String::new(),
                include: vec![],
                exclude: vec![],
            }],
        }],
    };

    let yaml = manifest.to_yaml()?;
    std::fs::write(manifest_path, &yaml)?;

    tracing::info!("Created manifest at {}", manifest_path.display());
    Ok(())
}
