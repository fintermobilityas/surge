use std::path::Path;
use std::sync::Arc;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::Context;
use surge_core::error::{Result, SurgeError};
use surge_core::lock::mutex::DistributedMutex;

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

    let ctx = Arc::new(Context::new());
    ctx.set_lock_server(&lock_config.url);

    let mut mutex = DistributedMutex::new(ctx, name);
    let acquired = mutex.try_acquire(timeout as i32).await?;
    if !acquired {
        return Err(SurgeError::Lock(format!("Lock '{name}' is held by another process")));
    }

    let challenge = mutex.challenge().unwrap_or("");
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

    let ctx = Arc::new(Context::new());
    ctx.set_lock_server(&lock_config.url);

    let mut mutex = DistributedMutex::new(ctx, name);
    mutex.set_challenge(challenge.to_string());
    mutex.try_release().await?;

    tracing::info!("Lock '{name}' released");
    Ok(())
}
