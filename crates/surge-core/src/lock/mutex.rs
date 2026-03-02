//! Distributed mutex via HTTP lock server.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::context::Context;
use crate::error::{Result, SurgeError};

/// Request body for acquiring a lock.
#[derive(Debug, Serialize)]
struct AcquireRequest {
    name: String,
    timeout_seconds: i32,
}

/// Response from a lock acquisition attempt.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AcquireResponse {
    #[serde(default)]
    acquired: bool,
    #[serde(default)]
    challenge: String,
    #[serde(default)]
    message: String,
}

/// Request body for releasing a lock.
#[derive(Debug, Serialize)]
struct ReleaseRequest {
    name: String,
    challenge: String,
}

/// A distributed mutex backed by an HTTP lock server.
///
/// The lock server protocol uses:
/// - `POST /lock` with `AcquireRequest` to acquire
/// - `DELETE /lock` with `ReleaseRequest` to release
///
/// The server returns a challenge token on successful acquisition that must
/// be presented when releasing.
pub struct DistributedMutex {
    ctx: Arc<Context>,
    name: String,
    challenge: Option<String>,
    client: reqwest::Client,
}

impl DistributedMutex {
    /// Create a new distributed mutex with the given name.
    pub fn new(ctx: Arc<Context>, name: &str) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(format!("surge/{}", crate::config::constants::VERSION))
            .build()
            .unwrap_or_default();

        Self {
            ctx,
            name: name.to_string(),
            challenge: None,
            client,
        }
    }

    /// Try to acquire the distributed lock.
    ///
    /// Returns `true` if the lock was successfully acquired, `false` if
    /// it could not be acquired (e.g., held by another process).
    /// The `timeout_seconds` parameter tells the lock server how long
    /// to hold the lock before auto-releasing.
    pub async fn try_acquire(&mut self, timeout_seconds: i32) -> Result<bool> {
        self.ctx.check_cancelled()?;

        let lock_cfg = self.ctx.lock_config();
        if lock_cfg.server_url.is_empty() {
            return Err(SurgeError::Lock("Lock server URL not configured".to_string()));
        }

        let url = format!("{}/lock", lock_cfg.server_url.trim_end_matches('/'));

        let body = AcquireRequest {
            name: self.name.clone(),
            timeout_seconds,
        };

        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| SurgeError::Lock(format!("Failed to contact lock server: {e}")))?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            // 409 Conflict typically means the lock is held by another process
            if status == 409 {
                return Ok(false);
            }
            let body_text = response.text().await.unwrap_or_default();
            return Err(SurgeError::Lock(format!(
                "Lock server returned HTTP {status}: {body_text}"
            )));
        }

        let resp: AcquireResponse = response
            .json()
            .await
            .map_err(|e| SurgeError::Lock(format!("Invalid lock server response: {e}")))?;

        if resp.acquired {
            self.challenge = Some(resp.challenge);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Try to release the distributed lock.
    ///
    /// Requires that the lock was previously acquired (i.e., a challenge
    /// token is available). Returns an error if the lock was not held.
    pub async fn try_release(&mut self) -> Result<()> {
        self.ctx.check_cancelled()?;

        let challenge = self
            .challenge
            .take()
            .ok_or_else(|| SurgeError::Lock("Cannot release: lock is not held".to_string()))?;

        let lock_cfg = self.ctx.lock_config();
        if lock_cfg.server_url.is_empty() {
            return Err(SurgeError::Lock("Lock server URL not configured".to_string()));
        }

        let url = format!("{}/lock", lock_cfg.server_url.trim_end_matches('/'));

        let body = ReleaseRequest {
            name: self.name.clone(),
            challenge,
        };

        let response = self
            .client
            .delete(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| SurgeError::Lock(format!("Failed to contact lock server: {e}")))?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body_text = response.text().await.unwrap_or_default();
            return Err(SurgeError::Lock(format!(
                "Lock release failed with HTTP {status}: {body_text}"
            )));
        }

        Ok(())
    }

    /// Set the challenge token externally.
    ///
    /// Used by the FFI layer for `surge_lock_release`, where the challenge
    /// string is passed in from the C caller rather than obtained from a
    /// prior `try_acquire` call on this instance.
    pub fn set_challenge(&mut self, challenge: String) {
        self.challenge = Some(challenge);
    }

    /// Check if the lock is currently held by this instance.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.challenge.is_some()
    }

    /// Get the challenge token if the lock is held.
    #[must_use]
    pub fn challenge(&self) -> Option<&str> {
        self.challenge.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mutex_initial_state() {
        let ctx = Arc::new(Context::new());
        let mutex = DistributedMutex::new(ctx, "test-lock");
        assert!(!mutex.is_locked());
        assert!(mutex.challenge().is_none());
    }

    #[tokio::test]
    async fn test_mutex_no_server_configured() {
        let ctx = Arc::new(Context::new());
        let mut mutex = DistributedMutex::new(ctx, "test-lock");
        let result = mutex.try_acquire(30).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not configured"));
    }

    #[tokio::test]
    async fn test_mutex_release_without_acquire() {
        let ctx = Arc::new(Context::new());
        let mut mutex = DistributedMutex::new(ctx, "test-lock");
        let result = mutex.try_release().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not held"));
    }
}
