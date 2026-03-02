pub mod azure;
pub mod filesystem;
pub mod gcs;
pub mod github_releases;
pub mod s3;

use crate::context::{StorageConfig, StorageProvider};
use crate::error::{Result, SurgeError};
use async_trait::async_trait;

/// Metadata about a stored object.
#[derive(Debug, Clone, Default)]
pub struct ObjectInfo {
    pub size: i64,
    pub etag: String,
    pub content_type: String,
}

/// A single item in a list-objects response.
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub size: i64,
}

/// Result of a list-objects operation.
#[derive(Debug, Clone, Default)]
pub struct ListResult {
    pub entries: Vec<ListEntry>,
    pub next_marker: Option<String>,
    pub is_truncated: bool,
}

/// Progress callback for upload/download: (bytes_done, bytes_total).
pub type TransferProgress = dyn Fn(u64, u64) + Send + Sync;

/// Abstract storage backend interface.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Upload an object from bytes.
    async fn put_object(&self, key: &str, data: &[u8], content_type: &str) -> Result<()>;

    /// Download an object to bytes.
    async fn get_object(&self, key: &str) -> Result<Vec<u8>>;

    /// Get object metadata without downloading.
    async fn head_object(&self, key: &str) -> Result<ObjectInfo>;

    /// Delete an object.
    async fn delete_object(&self, key: &str) -> Result<()>;

    /// List objects with a prefix.
    async fn list_objects(&self, prefix: &str, marker: Option<&str>, max_keys: i32) -> Result<ListResult>;

    /// Download an object directly to a file.
    async fn download_to_file(
        &self,
        key: &str,
        dest: &std::path::Path,
        progress: Option<&TransferProgress>,
    ) -> Result<()>;

    /// Upload a file to storage.
    async fn upload_from_file(
        &self,
        key: &str,
        src: &std::path::Path,
        progress: Option<&TransferProgress>,
    ) -> Result<()>;
}

/// Create a storage backend from configuration.
pub fn create_storage_backend(config: &StorageConfig) -> Result<Box<dyn StorageBackend>> {
    match config.provider {
        Some(StorageProvider::Filesystem) => Ok(Box::new(filesystem::FilesystemBackend::new(
            &config.bucket,
            &config.prefix,
        ))),
        Some(StorageProvider::S3) => Ok(Box::new(s3::S3Backend::new(config)?)),
        Some(StorageProvider::AzureBlob) => Ok(Box::new(azure::AzureBlobBackend::new(config)?)),
        Some(StorageProvider::Gcs) => Ok(Box::new(gcs::GcsBackend::new(config)?)),
        Some(StorageProvider::GitHubReleases) => Ok(Box::new(github_releases::GitHubReleasesBackend::new(config)?)),
        None => Err(SurgeError::Config("No storage provider configured".to_string())),
    }
}
