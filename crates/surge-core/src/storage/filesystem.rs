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

    fn base_dir(&self) -> PathBuf {
        if self.prefix.is_empty() {
            self.root.clone()
        } else {
            self.root.join(&self.prefix)
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
        let marker = _marker;
        let base = self.base_dir();
        let prefix_path = if prefix.is_empty() {
            base.clone()
        } else {
            base.join(prefix)
        };

        if !prefix_path.exists() {
            return Ok(ListResult::default());
        }

        let mut all_entries = Vec::new();
        collect_entries_recursive(&base, &prefix_path, &mut all_entries)?;
        all_entries.sort_by(|a, b| a.key.cmp(&b.key));

        let start_idx = marker
            .and_then(|m| all_entries.iter().position(|entry| entry.key.as_str() > m))
            .unwrap_or_else(|| if marker.is_some() { all_entries.len() } else { 0 });

        let max = max_keys.max(0) as usize;
        let entries: Vec<ListEntry> = all_entries.iter().skip(start_idx).take(max).cloned().collect();
        let is_truncated = start_idx + entries.len() < all_entries.len();
        let next_marker = if is_truncated {
            entries.last().map(|entry| entry.key.clone())
        } else {
            None
        };

        Ok(ListResult {
            entries,
            next_marker,
            is_truncated,
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

fn collect_entries_recursive(base: &Path, path: &Path, out: &mut Vec<ListEntry>) -> Result<()> {
    let metadata = std::fs::metadata(path)?;
    if metadata.is_file() {
        let rel = path
            .strip_prefix(base)
            .map_err(|e| SurgeError::Storage(format!("Failed to relativize key path: {e}")))?;
        let key = rel.to_string_lossy().replace('\\', "/");
        out.push(ListEntry {
            key,
            size: metadata.len() as i64,
        });
        return Ok(());
    }

    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        collect_entries_recursive(base, &entry.path(), out)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_list_objects_recursive() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");
        backend
            .put_object(
                "app/linux-x64/stable/1.0.0/full.tar.zst",
                b"full",
                "application/octet-stream",
            )
            .await
            .unwrap();
        backend
            .put_object(
                "app/linux-x64/stable/1.0.0/delta.tar.zst",
                b"delta",
                "application/octet-stream",
            )
            .await
            .unwrap();

        let listed = backend.list_objects("app/linux-x64/", None, 100).await.unwrap();
        assert_eq!(listed.entries.len(), 2);
        assert_eq!(listed.entries[0].key, "app/linux-x64/stable/1.0.0/delta.tar.zst");
        assert_eq!(listed.entries[1].key, "app/linux-x64/stable/1.0.0/full.tar.zst");
        assert!(!listed.is_truncated);
    }

    #[tokio::test]
    async fn test_list_objects_marker_pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");
        backend
            .put_object("a/1.bin", b"1", "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object("a/2.bin", b"2", "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object("a/3.bin", b"3", "application/octet-stream")
            .await
            .unwrap();

        let first = backend.list_objects("a/", None, 2).await.unwrap();
        assert_eq!(first.entries.len(), 2);
        assert!(first.is_truncated);
        let marker = first.next_marker.clone().unwrap();
        assert_eq!(marker, "a/2.bin");

        let second = backend.list_objects("a/", Some(&marker), 2).await.unwrap();
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].key, "a/3.bin");
        assert!(!second.is_truncated);
    }

    #[tokio::test]
    async fn test_list_objects_missing_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");
        let listed = backend.list_objects("missing/", None, 10).await.unwrap();
        assert!(listed.entries.is_empty());
    }
}
