use std::ffi::CString;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::{ErrorCode, SurgeError};

/// Cloud/local storage provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum StorageProvider {
    S3 = 0,
    AzureBlob = 1,
    Gcs = 2,
    Filesystem = 3,
    GitHubReleases = 4,
}

impl StorageProvider {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::S3),
            1 => Some(Self::AzureBlob),
            2 => Some(Self::Gcs),
            3 => Some(Self::Filesystem),
            4 => Some(Self::GitHubReleases),
            _ => None,
        }
    }
}

/// Storage backend configuration.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    pub provider: Option<StorageProvider>,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
    pub prefix: String,
}

/// Lock server configuration.
#[derive(Debug, Clone, Default)]
pub struct LockConfig {
    pub server_url: String,
}

/// Resource budget limits (matches `surge_resource_budget` in surge_api.h).
#[derive(Debug, Clone)]
#[repr(C)]
pub struct ResourceBudget {
    pub max_memory_bytes: i64,
    pub max_threads: i32,
    pub max_concurrent_downloads: i32,
    pub max_download_speed_bps: i64,
    pub zstd_compression_level: i32,
}

impl Default for ResourceBudget {
    fn default() -> Self {
        Self {
            max_memory_bytes: 512 * 1024 * 1024,
            max_threads: 4,
            max_concurrent_downloads: 4,
            max_download_speed_bps: 0,
            zstd_compression_level: 9,
        }
    }
}

/// Internal last-error state.
#[allow(dead_code)]
struct LastError {
    code: i32,
    message: String,
    /// Cached CString for FFI returns.
    c_message: Option<CString>,
}

/// The main Surge context. Thread-safe (`Send + Sync`).
pub struct Context {
    pub storage: Mutex<StorageConfig>,
    pub lock_config: Mutex<LockConfig>,
    pub resource_budget: Mutex<ResourceBudget>,
    cancelled: AtomicBool,
    last_error: Mutex<Option<LastError>>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            storage: Mutex::new(StorageConfig::default()),
            lock_config: Mutex::new(LockConfig::default()),
            resource_budget: Mutex::new(ResourceBudget::default()),
            cancelled: AtomicBool::new(false),
            last_error: Mutex::new(None),
        }
    }

    /// Set storage configuration.
    pub fn set_storage(
        &self,
        provider: StorageProvider,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        endpoint: &str,
    ) {
        let mut cfg = self.storage.lock().unwrap();
        cfg.provider = Some(provider);
        cfg.bucket = bucket.to_string();
        cfg.region = region.to_string();
        cfg.access_key = access_key.to_string();
        cfg.secret_key = secret_key.to_string();
        cfg.endpoint = endpoint.to_string();
    }

    /// Set lock server URL.
    pub fn set_lock_server(&self, url: &str) {
        let mut cfg = self.lock_config.lock().unwrap();
        cfg.server_url = url.to_string();
    }

    /// Set resource budget.
    pub fn set_resource_budget(&self, budget: ResourceBudget) {
        let mut b = self.resource_budget.lock().unwrap();
        *b = budget;
    }

    /// Request cancellation of in-progress operations.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Reset the cancellation flag.
    pub fn reset_cancel(&self) {
        self.cancelled.store(false, Ordering::Release);
    }

    /// Check cancellation and return error if cancelled.
    pub fn check_cancelled(&self) -> crate::error::Result<()> {
        if self.is_cancelled() {
            Err(SurgeError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Set the last error.
    pub fn set_last_error(&self, code: ErrorCode, message: &str) {
        let mut err = self.last_error.lock().unwrap();
        *err = Some(LastError {
            code: code as i32,
            message: message.to_string(),
            c_message: CString::new(message).ok(),
        });
    }

    /// Set the last error from a `SurgeError`.
    pub fn set_error(&self, e: &SurgeError) {
        self.set_last_error(e.error_code(), &e.to_string());
    }

    /// Get the last error code and message pointer (for FFI).
    /// Returns `None` if no error has been set.
    pub fn last_error(&self) -> Option<(i32, *const std::ffi::c_char)> {
        let err = self.last_error.lock().unwrap();
        err.as_ref().map(|e| {
            let ptr = e.c_message.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
            (e.code, ptr)
        })
    }

    /// Clear the last error.
    pub fn clear_error(&self) {
        let mut err = self.last_error.lock().unwrap();
        *err = None;
    }

    /// Get a snapshot of the storage config.
    pub fn storage_config(&self) -> StorageConfig {
        self.storage.lock().unwrap().clone()
    }

    /// Get a snapshot of the lock config.
    pub fn lock_config(&self) -> LockConfig {
        self.lock_config.lock().unwrap().clone()
    }

    /// Get a snapshot of the resource budget.
    pub fn resource_budget(&self) -> ResourceBudget {
        self.resource_budget.lock().unwrap().clone()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}
