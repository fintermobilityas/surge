use std::ffi::CString;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use crate::error::{ErrorCode, SurgeError};

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

#[derive(Debug, Clone, Default)]
pub struct LockConfig {
    pub server_url: String,
}

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

#[allow(dead_code)]
struct LastError {
    code: i32,
    message: String,
    c_message: Option<CString>,
}

pub struct Context {
    pub storage: Mutex<StorageConfig>,
    pub lock_config: Mutex<LockConfig>,
    pub resource_budget: Mutex<ResourceBudget>,
    cancelled: AtomicBool,
    last_error: Mutex<Option<LastError>>,
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
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

    pub fn set_storage(
        &self,
        provider: StorageProvider,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        endpoint: &str,
    ) {
        let mut cfg = lock_recover(&self.storage);
        cfg.provider = Some(provider);
        cfg.bucket = bucket.to_string();
        cfg.region = region.to_string();
        cfg.access_key = access_key.to_string();
        cfg.secret_key = secret_key.to_string();
        cfg.endpoint = endpoint.to_string();
    }

    pub fn set_storage_prefix(&self, prefix: &str) {
        let mut cfg = lock_recover(&self.storage);
        cfg.prefix = prefix.to_string();
    }

    pub fn set_lock_server(&self, url: &str) {
        let mut cfg = lock_recover(&self.lock_config);
        cfg.server_url = url.to_string();
    }

    pub fn set_resource_budget(&self, budget: ResourceBudget) {
        let mut b = lock_recover(&self.resource_budget);
        *b = budget;
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn reset_cancel(&self) {
        self.cancelled.store(false, Ordering::Release);
    }

    pub fn check_cancelled(&self) -> crate::error::Result<()> {
        if self.is_cancelled() {
            Err(SurgeError::Cancelled)
        } else {
            Ok(())
        }
    }

    pub fn set_last_error(&self, code: ErrorCode, message: &str) {
        let mut err = lock_recover(&self.last_error);
        *err = Some(LastError {
            code: code as i32,
            message: message.to_string(),
            c_message: CString::new(message).ok(),
        });
    }

    pub fn set_error(&self, e: &SurgeError) {
        self.set_last_error(e.error_code(), &e.to_string());
    }

    /// Returns the last error code and message pointer for FFI consumers.
    /// The pointer is valid until the next error-mutating call.
    pub fn last_error(&self) -> Option<(i32, *const std::ffi::c_char)> {
        let err = lock_recover(&self.last_error);
        err.as_ref().map(|e| {
            let ptr = e.c_message.as_ref().map_or(std::ptr::null(), |c| c.as_ptr());
            (e.code, ptr)
        })
    }

    pub fn clear_error(&self) {
        let mut err = lock_recover(&self.last_error);
        *err = None;
    }

    pub fn storage_config(&self) -> StorageConfig {
        lock_recover(&self.storage).clone()
    }

    pub fn lock_config(&self) -> LockConfig {
        lock_recover(&self.lock_config).clone()
    }

    pub fn resource_budget(&self) -> ResourceBudget {
        lock_recover(&self.resource_budget).clone()
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}
