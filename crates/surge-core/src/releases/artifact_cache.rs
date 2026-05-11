use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::path::{Component, Path, PathBuf};

use crate::crypto::sha256::sha256_hex_file;
use crate::error::{Result, SurgeError};
use crate::platform::fs::atomic_rename;
use crate::storage::{StorageBackend, TransferProgress};
use fs2::FileExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFetchOutcome {
    ReusedLocal,
    DownloadedFresh,
    DownloadedAfterInvalidLocal,
}

pub fn cache_path_for_key(cache_root: &Path, key: &str) -> Result<PathBuf> {
    let mut path = PathBuf::from(cache_root);
    let key_path = Path::new(key);
    for component in key_path.components() {
        match component {
            Component::Normal(segment) => path.push(segment),
            _ => {
                return Err(SurgeError::Storage(format!(
                    "Invalid artifact cache key '{key}'; only relative normal path segments are supported"
                )));
            }
        }
    }
    if path == cache_root {
        return Err(SurgeError::Storage(format!(
            "Invalid artifact cache key '{key}'; key produced an empty path"
        )));
    }
    Ok(path)
}

pub fn sha256_matches_file(path: &Path, expected_sha256: &str) -> Result<bool> {
    let expected = expected_sha256.trim();
    if expected.is_empty() || !path.is_file() {
        return Ok(false);
    }
    let actual = sha256_hex_file(path)?;
    Ok(actual.eq_ignore_ascii_case(expected))
}

pub fn cached_artifact_matches(path: &Path, expected_sha256: &str) -> Result<bool> {
    let expected = expected_sha256.trim();
    if expected.is_empty() {
        return Ok(path.is_file());
    }
    sha256_matches_file(path, expected)
}

pub async fn fetch_or_reuse_file(
    storage: &dyn StorageBackend,
    key: &str,
    destination: &Path,
    expected_sha256: &str,
    progress: Option<&TransferProgress<'_>>,
) -> Result<CacheFetchOutcome> {
    let expected = expected_sha256.trim();
    let _lock = CacheFileLock::acquire(destination).await?;
    let had_local = destination.is_file();
    if !expected.is_empty() && had_local && sha256_matches_file(destination, expected)? {
        return Ok(CacheFetchOutcome::ReusedLocal);
    }

    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp_path = temporary_download_path(destination)?;
    let download_result = storage.download_to_file(key, &tmp_path, progress).await;
    if let Err(error) = download_result {
        remove_file_if_exists(&tmp_path);
        return Err(error);
    }

    if !expected.is_empty() && !sha256_matches_file(&tmp_path, expected)? {
        remove_file_if_exists(&tmp_path);
        return Err(SurgeError::Storage(format!(
            "SHA-256 mismatch for '{key}' after download"
        )));
    }

    atomic_rename(&tmp_path, destination)?;

    if had_local && !expected.is_empty() {
        Ok(CacheFetchOutcome::DownloadedAfterInvalidLocal)
    } else {
        Ok(CacheFetchOutcome::DownloadedFresh)
    }
}

struct CacheFileLock {
    #[allow(dead_code)]
    file: File,
}

impl CacheFileLock {
    async fn acquire(destination: &Path) -> Result<Self> {
        let parent = destination.parent().ok_or_else(|| {
            SurgeError::Storage(format!(
                "Cannot lock cache path without parent directory: {}",
                destination.display()
            ))
        })?;
        std::fs::create_dir_all(parent)?;
        let lock_path = cache_lock_path(destination);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|e| SurgeError::Storage(format!("Failed to open cache lock '{}': {e}", lock_path.display())))?;
        let file = tokio::task::spawn_blocking(move || {
            lock_cache_file(&file, &lock_path)?;
            Ok::<File, SurgeError>(file)
        })
        .await
        .map_err(|e| SurgeError::Storage(format!("Failed to join cache lock task: {e}")))??;
        Ok(Self { file })
    }
}

fn lock_cache_file(file: &File, lock_path: &Path) -> Result<()> {
    file.lock_exclusive()
        .map_err(|e| SurgeError::Storage(format!("Failed to lock cache file '{}': {e}", lock_path.display())))
}

fn cache_lock_path(destination: &Path) -> PathBuf {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    destination.with_file_name(format!(".{file_name}.lock"))
}

fn temporary_download_path(destination: &Path) -> Result<PathBuf> {
    let parent = destination.parent().ok_or_else(|| {
        SurgeError::Storage(format!(
            "Cannot create temporary cache path without parent directory: {}",
            destination.display()
        ))
    })?;
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    let temp = tempfile::Builder::new()
        .prefix(&format!(".{file_name}."))
        .suffix(".tmp")
        .tempfile_in(parent)?;
    let path = temp.path().to_path_buf();
    drop(temp);
    Ok(path)
}

fn remove_file_if_exists(path: &Path) {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
}

pub fn prune_cached_artifacts(cache_root: &Path, required_keys: &BTreeSet<String>) -> Result<usize> {
    if !cache_root.exists() {
        return Ok(0);
    }
    if !cache_root.is_dir() {
        return Err(SurgeError::Storage(format!(
            "Artifact cache path is not a directory: {}",
            cache_root.display()
        )));
    }

    let mut pruned = 0usize;
    prune_cached_artifacts_recursive(cache_root, cache_root, required_keys, &mut pruned)?;
    prune_empty_directories(cache_root, cache_root)?;
    Ok(pruned)
}

fn prune_cached_artifacts_recursive(
    cache_root: &Path,
    dir: &Path,
    required_keys: &BTreeSet<String>,
    pruned: &mut usize,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            prune_cached_artifacts_recursive(cache_root, &path, required_keys, pruned)?;
            continue;
        }
        if !path.is_file() {
            continue;
        }

        let rel = path
            .strip_prefix(cache_root)
            .map_err(|e| SurgeError::Storage(format!("Invalid cache entry '{}': {e}", path.display())))?;
        let key = rel.to_string_lossy().replace('\\', "/");
        if required_keys.contains(&key) {
            continue;
        }

        std::fs::remove_file(&path)?;
        *pruned = pruned.saturating_add(1);
    }

    Ok(())
}

fn prune_empty_directories(dir: &Path, root: &Path) -> Result<bool> {
    let mut is_empty = true;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if !prune_empty_directories(&path, root)? {
                is_empty = false;
            }
            continue;
        }
        is_empty = false;
    }

    if is_empty && dir != root {
        std::fs::remove_dir(dir)?;
        return Ok(true);
    }

    Ok(is_empty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::sha256::sha256_hex;
    use crate::storage::{ListResult, ObjectInfo, TransferProgress, filesystem::FilesystemBackend};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    struct SlowBackend {
        payload: Vec<u8>,
        downloads: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StorageBackend for SlowBackend {
        async fn put_object(&self, _key: &str, _data: &[u8], _content_type: &str) -> Result<()> {
            unimplemented!("test backend is read-only")
        }

        async fn get_object(&self, _key: &str) -> Result<Vec<u8>> {
            unimplemented!("test backend only supports file downloads")
        }

        async fn head_object(&self, _key: &str) -> Result<ObjectInfo> {
            Ok(ObjectInfo {
                size: i64::try_from(self.payload.len()).expect("payload length should fit i64"),
                ..ObjectInfo::default()
            })
        }

        async fn delete_object(&self, _key: &str) -> Result<()> {
            unimplemented!("test backend is read-only")
        }

        async fn list_objects(&self, _prefix: &str, _marker: Option<&str>, _max_keys: i32) -> Result<ListResult> {
            Ok(ListResult::default())
        }

        async fn download_to_file(
            &self,
            _key: &str,
            dest: &Path,
            progress: Option<&TransferProgress<'_>>,
        ) -> Result<()> {
            self.downloads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            tokio::fs::write(dest, &self.payload).await?;
            if let Some(progress) = progress {
                let len = self.payload.len() as u64;
                progress(len, len);
            }
            Ok(())
        }

        async fn upload_from_file(
            &self,
            _key: &str,
            _src: &Path,
            _progress: Option<&TransferProgress<'_>>,
        ) -> Result<()> {
            unimplemented!("test backend is read-only")
        }
    }

    #[test]
    fn cache_path_for_key_rejects_parent_traversal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let err = cache_path_for_key(tmp.path(), "../outside").expect_err("parent traversal must fail");
        assert!(err.to_string().contains("Invalid artifact cache key"));
    }

    #[test]
    fn sha256_matches_file_reports_expected_result() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("artifact.bin");
        std::fs::write(&path, b"payload").expect("write");
        assert!(sha256_matches_file(&path, &sha256_hex(b"payload")).expect("hash check"));
        assert!(!sha256_matches_file(&path, &sha256_hex(b"other")).expect("hash check"));
        assert!(!sha256_matches_file(&path, "").expect("empty hash should not match"));
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_reuses_valid_local_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).expect("mkdir backend");
        let backend = FilesystemBackend::new(backend_root.to_str().expect("utf-8"), "");

        backend
            .put_object("artifact.bin", b"remote-payload", "application/octet-stream")
            .await
            .expect("upload");

        let local = tmp.path().join("artifact.bin");
        std::fs::write(&local, b"remote-payload").expect("write local");
        let outcome = fetch_or_reuse_file(&backend, "artifact.bin", &local, &sha256_hex(b"remote-payload"), None)
            .await
            .expect("fetch/reuse");

        assert_eq!(outcome, CacheFetchOutcome::ReusedLocal);
        assert_eq!(std::fs::read(&local).expect("read local"), b"remote-payload");
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_replaces_invalid_local_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).expect("mkdir backend");
        let backend = FilesystemBackend::new(backend_root.to_str().expect("utf-8"), "");

        backend
            .put_object("artifact.bin", b"remote-payload", "application/octet-stream")
            .await
            .expect("upload");

        let local = tmp.path().join("artifact.bin");
        std::fs::write(&local, b"stale-payload").expect("write local");
        let outcome = fetch_or_reuse_file(&backend, "artifact.bin", &local, &sha256_hex(b"remote-payload"), None)
            .await
            .expect("fetch/reuse");

        assert_eq!(outcome, CacheFetchOutcome::DownloadedAfterInvalidLocal);
        assert_eq!(std::fs::read(&local).expect("read local"), b"remote-payload");
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_downloads_when_hash_is_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).expect("mkdir backend");
        let backend = FilesystemBackend::new(backend_root.to_str().expect("utf-8"), "");

        backend
            .put_object("artifact.bin", b"remote-payload", "application/octet-stream")
            .await
            .expect("upload");

        let local = tmp.path().join("artifact.bin");
        std::fs::write(&local, b"stale-payload").expect("write local");
        let outcome = fetch_or_reuse_file(&backend, "artifact.bin", &local, "", None)
            .await
            .expect("fetch/reuse");

        assert_eq!(outcome, CacheFetchOutcome::DownloadedFresh);
        assert_eq!(std::fs::read(&local).expect("read local"), b"remote-payload");
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_serializes_concurrent_writes_to_same_cache_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let destination = tmp.path().join("artifact.bin");
        let payload = b"remote-payload".to_vec();
        let expected_sha = sha256_hex(&payload);
        let backend = Arc::new(SlowBackend {
            payload,
            downloads: AtomicUsize::new(0),
        });

        let first_backend = Arc::clone(&backend);
        let first_destination = destination.clone();
        let first_sha = expected_sha.clone();
        let first = tokio::spawn(async move {
            fetch_or_reuse_file(
                first_backend.as_ref(),
                "artifact.bin",
                &first_destination,
                &first_sha,
                None,
            )
            .await
        });

        let second_backend = Arc::clone(&backend);
        let second_destination = destination.clone();
        let second_sha = expected_sha.clone();
        let second = tokio::spawn(async move {
            fetch_or_reuse_file(
                second_backend.as_ref(),
                "artifact.bin",
                &second_destination,
                &second_sha,
                None,
            )
            .await
        });

        let outcomes = (first.await.expect("first task"), second.await.expect("second task"));
        assert!(matches!(
            outcomes,
            (
                Ok(CacheFetchOutcome::DownloadedFresh),
                Ok(CacheFetchOutcome::ReusedLocal)
            ) | (
                Ok(CacheFetchOutcome::ReusedLocal),
                Ok(CacheFetchOutcome::DownloadedFresh)
            )
        ));
        assert_eq!(backend.downloads.load(Ordering::SeqCst), 1);
        assert_eq!(
            std::fs::read(destination).expect("cache file should exist"),
            b"remote-payload"
        );
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_does_not_expose_partial_download_at_final_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let destination = tmp.path().join("artifact.bin");
        let payload = b"remote-payload".to_vec();
        let expected_sha = sha256_hex(&payload);
        let backend = SlowBackend {
            payload,
            downloads: AtomicUsize::new(0),
        };
        let observed_final_path_during_download = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&observed_final_path_during_download);
        let watched_destination = destination.clone();
        let progress = move |_done: u64, _total: u64| {
            if watched_destination.exists() {
                observed.store(true, Ordering::SeqCst);
            }
        };

        let outcome = fetch_or_reuse_file(&backend, "artifact.bin", &destination, &expected_sha, Some(&progress))
            .await
            .expect("fetch should succeed");

        assert_eq!(outcome, CacheFetchOutcome::DownloadedFresh);
        assert!(!observed_final_path_during_download.load(Ordering::SeqCst));
        assert_eq!(
            std::fs::read(destination).expect("cache file should exist"),
            b"remote-payload"
        );
    }

    #[test]
    fn cached_artifact_matches_accepts_existing_file_without_hash() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("artifact.bin");
        std::fs::write(&path, b"payload").expect("write");
        assert!(cached_artifact_matches(&path, "").expect("match check"));
    }

    #[test]
    fn prune_cached_artifacts_removes_stale_files_and_empty_directories() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache_root = tmp.path().join("cache");
        let required = cache_root.join("required.bin");
        let stale = cache_root.join("nested").join("stale.bin");
        std::fs::create_dir_all(stale.parent().expect("nested dir")).expect("mkdir");
        std::fs::write(&required, b"required").expect("write required");
        std::fs::write(&stale, b"stale").expect("write stale");

        let required_keys = BTreeSet::from([String::from("required.bin")]);
        let pruned = prune_cached_artifacts(&cache_root, &required_keys).expect("prune cache");

        assert_eq!(pruned, 1);
        assert!(required.is_file());
        assert!(!stale.exists());
        assert!(!cache_root.join("nested").exists());
    }
}
