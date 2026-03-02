use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};

/// Acquire a distributed lock.
pub async fn acquire(manifest_path: &Path, name: &str, timeout: u32) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let lock_config = manifest
        .lock
        .as_ref()
        .ok_or_else(|| SurgeError::Config("No lock server configured in manifest".to_string()))?;

    if lock_config.url.is_empty() {
        return Err(SurgeError::Config("Lock server URL is empty in manifest".to_string()));
    }

    tracing::info!("Acquiring lock '{name}' (timeout: {timeout}s) via {}", lock_config.url);

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/locks/{name}/acquire", lock_config.url))
        .json(&serde_json::json!({ "timeout_seconds": timeout }))
        .send()
        .await
        .map_err(|e| SurgeError::Lock(format!("Failed to contact lock server: {e}")))?;

    if !response.status().is_success() {
        return Err(SurgeError::Lock(format!(
            "Lock acquire failed with status: {}",
            response.status()
        )));
    }

    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|e| SurgeError::Lock(format!("Failed to parse lock response: {e}")))?;

    let challenge = body.get("challenge").and_then(|v| v.as_str()).unwrap_or("");

    println!("{challenge}");
    tracing::info!("Lock '{name}' acquired");

    Ok(())
}

/// Release a distributed lock.
pub async fn release(manifest_path: &Path, name: &str, challenge: &str) -> Result<()> {
    let manifest = SurgeManifest::from_file(manifest_path)?;

    let lock_config = manifest
        .lock
        .as_ref()
        .ok_or_else(|| SurgeError::Config("No lock server configured in manifest".to_string()))?;

    if lock_config.url.is_empty() {
        return Err(SurgeError::Config("Lock server URL is empty in manifest".to_string()));
    }

    tracing::info!("Releasing lock '{name}' via {}", lock_config.url);

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/locks/{name}/release", lock_config.url))
        .json(&serde_json::json!({ "challenge": challenge }))
        .send()
        .await
        .map_err(|e| SurgeError::Lock(format!("Failed to contact lock server: {e}")))?;

    if !response.status().is_success() {
        return Err(SurgeError::Lock(format!(
            "Lock release failed with status: {}",
            response.status()
        )));
    }

    tracing::info!("Lock '{name}' released");
    Ok(())
}
