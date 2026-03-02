//! Distributed mutex via the snapx.dev lock server.
//!
//! Protocol:
//! - `POST /lock`   — acquire with `{ "name", "duration" }`; returns challenge as plain text
//! - `DELETE /unlock` — release with `{ "name", "challenge", "breakPeriod" }`

use std::sync::Arc;

use serde::Serialize;

use crate::context::Context;
use crate::error::{Result, SurgeError};

/// Default lock server URL.
const DEFAULT_LOCK_SERVER: &str = "https://snapx.dev";

/// Request body for acquiring a lock (matches snapx.dev `Lock` record).
#[derive(Debug, Serialize)]
struct AcquireRequest {
    name: String,
    /// Duration as a .NET `TimeSpan` string, e.g. `"00:05:00"` for 5 minutes.
    duration: String,
}

/// Request body for releasing a lock (matches snapx.dev `Unlock` record).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseRequest {
    name: String,
    challenge: String,
    break_period: Option<String>,
}

/// Format seconds as a .NET `TimeSpan` string (`HH:MM:SS`).
fn format_duration(seconds: i32) -> String {
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    let s = seconds % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

/// A distributed mutex backed by the snapx.dev lock server.
///
/// The lock server is a simple HTTP service:
/// - `POST /lock` with a JSON `{ "name", "duration" }` body acquires the lock
///   and returns the challenge token as plain text.
/// - `DELETE /unlock` with a JSON `{ "name", "challenge", "breakPeriod" }` body
///   releases the lock.
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

    /// Resolve the lock server base URL.
    ///
    /// Uses the configured URL if set, otherwise falls back to
    /// `https://snapx.dev`.
    fn server_url(&self) -> String {
        let cfg = self.ctx.lock_config();
        if cfg.server_url.is_empty() {
            DEFAULT_LOCK_SERVER.to_string()
        } else {
            cfg.server_url.clone()
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

        let base = self.server_url();
        let url = format!("{}/lock", base.trim_end_matches('/'));

        let body = AcquireRequest {
            name: self.name.clone(),
            duration: format_duration(timeout_seconds),
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

        // snapx.dev returns the challenge token as plain text.
        let challenge = response
            .text()
            .await
            .map_err(|e| SurgeError::Lock(format!("Invalid lock server response: {e}")))?;

        let challenge = challenge.trim().to_string();
        if challenge.is_empty() {
            return Ok(false);
        }

        self.challenge = Some(challenge);
        Ok(true)
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

        let base = self.server_url();
        let url = format!("{}/unlock", base.trim_end_matches('/'));

        let body = ReleaseRequest {
            name: self.name.clone(),
            challenge,
            break_period: None,
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

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "00:00:00");
        assert_eq!(format_duration(30), "00:00:30");
        assert_eq!(format_duration(300), "00:05:00");
        assert_eq!(format_duration(3661), "01:01:01");
        assert_eq!(format_duration(86400), "24:00:00");
    }

    #[test]
    fn test_server_url_default() {
        let ctx = Arc::new(Context::new());
        let mutex = DistributedMutex::new(ctx, "test-lock");
        assert_eq!(mutex.server_url(), "https://snapx.dev");
    }

    #[test]
    fn test_server_url_custom() {
        let ctx = Arc::new(Context::new());
        ctx.set_lock_server("https://custom.lock.server");
        let mutex = DistributedMutex::new(ctx, "test-lock");
        assert_eq!(mutex.server_url(), "https://custom.lock.server");
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
