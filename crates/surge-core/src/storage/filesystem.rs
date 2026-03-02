//! Filesystem storage backend for local/testing deployments.

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult, ObjectInfo, StorageBackend, TransferProgress};

/// Local filesystem storage backend.
pub struct FilesystemBackend {
    root: PathBuf,
    prefix: String,
}

impl FilesystemBackend {
    pub fn new(root: &str, prefix: &str) -> Self {
        Self {
            root: PathBuf::from(root),
            prefix: prefix.to_string(),
        }
    }

    fn resolve_key(&self, key: &str) -> PathBuf {
        if self.prefix.is_empty() {
            self.root.join(key)
        } else {
            self.root.join(&self.prefix).join(key)
        }
    }
}

#[async_trait]
impl StorageBackend for FilesystemBackend {
    async fn put_object(&self, key: &str, data: &[u8], _content_type: &str) -> Result<()> {
        let path = self.resolve_key(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let path = self.resolve_key(key);
        if !path.exists() {
            return Err(SurgeError::NotFound(format!("Object not found: {key}")));
        }
        Ok(tokio::fs::read(&path).await?)
    }

    async fn head_object(&self, key: &str) -> Result<ObjectInfo> {
        let path = self.resolve_key(key);
        if !path.exists() {
            return Err(SurgeError::NotFound(format!("Object not found: {key}")));
        }
        let meta = tokio::fs::metadata(&path).await?;
        Ok(ObjectInfo {
            size: meta.len() as i64,
            etag: String::new(),
            content_type: String::new(),
        })
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let path = self.resolve_key(key);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn list_objects(&self, prefix: &str, _marker: Option<&str>, max_keys: i32) -> Result<ListResult> {
        let dir = self.resolve_key(prefix);
        let mut entries = Vec::new();

        if !dir.exists() {
            return Ok(ListResult::default());
        }

        let mut read_dir = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            if entries.len() >= max_keys as usize {
                return Ok(ListResult {
                    entries,
                    next_marker: None,
                    is_truncated: true,
                });
            }
            let meta = entry.metadata().await?;
            if meta.is_file() {
                let key = format!("{}/{}", prefix, entry.file_name().to_string_lossy());
                entries.push(ListEntry {
                    key,
                    size: meta.len() as i64,
                });
            }
        }

        Ok(ListResult {
            entries,
            next_marker: None,
            is_truncated: false,
        })
    }

    async fn download_to_file(&self, key: &str, dest: &Path, progress: Option<&TransferProgress>) -> Result<()> {
        let data = self.get_object(key).await?;
        let total = data.len() as u64;
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(dest, &data).await?;
        if let Some(cb) = progress {
            cb(total, total);
        }
        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress>) -> Result<()> {
        let data = tokio::fs::read(src).await?;
        let total = data.len() as u64;
        self.put_object(key, &data, "application/octet-stream").await?;
        if let Some(cb) = progress {
            cb(total, total);
        }
        Ok(())
    }
}
