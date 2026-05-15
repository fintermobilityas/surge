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
    let resumable = !expected.is_empty() && storage.supports_resumable_downloads();
    let (download_path, resume_offset, keep_partial_on_error) = if resumable {
        let partial_path = partial_download_path(destination)?;
        let resume_offset = prepare_resumable_download(storage, key, &partial_path, expected).await?;
        (partial_path, resume_offset, true)
    } else {
        let tmp_path = temporary_download_path(destination)?;
        remove_file_if_exists(&partial_download_path(destination)?);
        (tmp_path, 0, false)
    };
    if resume_offset > 0 && sha256_matches_file(&download_path, expected)? {
        atomic_rename(&download_path, destination)?;
        return if had_local && !expected.is_empty() {
            Ok(CacheFetchOutcome::DownloadedAfterInvalidLocal)
        } else {
            Ok(CacheFetchOutcome::DownloadedFresh)
        };
    }

    let download_result = if resume_offset > 0 {
        storage
            .download_to_file_from_offset(key, &download_path, resume_offset, progress)
            .await
    } else {
        storage.download_to_file(key, &download_path, progress).await
    };
    if let Err(error) = download_result {
        if !keep_partial_on_error {
            remove_file_if_exists(&download_path);
        }
        return Err(error);
    }

    if !expected.is_empty() && !sha256_matches_file(&download_path, expected)? {
        remove_file_if_exists(&download_path);
        return Err(SurgeError::Storage(format!(
            "SHA-256 mismatch for '{key}' after download"
        )));
    }

    atomic_rename(&download_path, destination)?;

    if had_local && !expected.is_empty() {
        Ok(CacheFetchOutcome::DownloadedAfterInvalidLocal)
    } else {
        Ok(CacheFetchOutcome::DownloadedFresh)
    }
}

async fn prepare_resumable_download(
    storage: &dyn StorageBackend,
    key: &str,
    partial_path: &Path,
    expected_sha256: &str,
) -> Result<u64> {
    let partial_size = std::fs::metadata(partial_path).map_or(0, |metadata| metadata.len());
    if partial_size == 0 {
        return Ok(0);
    }

    let remote_size = match storage.head_object(key).await {
        Ok(info) => u64::try_from(info.size).unwrap_or(0),
        Err(_) => 0,
    };
    if remote_size > 0 && partial_size > remote_size {
        remove_file_if_exists(partial_path);
        return Ok(0);
    }
    if remote_size > 0 && partial_size == remote_size {
        if sha256_matches_file(partial_path, expected_sha256)? {
            return Ok(partial_size);
        }
        remove_file_if_exists(partial_path);
        return Ok(0);
    }

    Ok(partial_size)
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

fn partial_download_path(destination: &Path) -> Result<PathBuf> {
    let file_name = destination.file_name().and_then(|name| name.to_str()).ok_or_else(|| {
        SurgeError::Storage(format!(
            "Cannot create partial cache path for destination without file name: {}",
            destination.display()
        ))
    })?;
    Ok(destination.with_file_name(format!(".{file_name}.partial")))
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
    use std::io::Write;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::time::Duration;

    struct SlowBackend {
        payload: Vec<u8>,
        downloads: AtomicUsize,
        range_downloads: AtomicUsize,
        resumable: bool,
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

        fn supports_resumable_downloads(&self) -> bool {
            self.resumable
        }

        async fn download_to_file_from_offset(
            &self,
            _key: &str,
            dest: &Path,
            offset: u64,
            progress: Option<&TransferProgress<'_>>,
        ) -> Result<()> {
            self.range_downloads.fetch_add(1, Ordering::SeqCst);
            let start =
                usize::try_from(offset).map_err(|e| SurgeError::Storage(format!("invalid test offset: {e}")))?;
            let mut file = OpenOptions::new().append(true).open(dest)?;
            file.write_all(&self.payload[start..])?;
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
            range_downloads: AtomicUsize::new(0),
            resumable: false,
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
            range_downloads: AtomicUsize::new(0),
            resumable: false,
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

    #[tokio::test]
    async fn fetch_or_reuse_file_resumes_existing_partial_download() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let destination = tmp.path().join("artifact.bin");
        let payload = b"remote-payload".to_vec();
        let partial_path = partial_download_path(&destination).expect("partial path");
        std::fs::write(&partial_path, b"remote-").expect("write partial");

        let backend = SlowBackend {
            payload: payload.clone(),
            downloads: AtomicUsize::new(0),
            range_downloads: AtomicUsize::new(0),
            resumable: true,
        };

        let outcome = fetch_or_reuse_file(&backend, "artifact.bin", &destination, &sha256_hex(&payload), None)
            .await
            .expect("fetch should succeed");

        assert_eq!(outcome, CacheFetchOutcome::DownloadedFresh);
        assert_eq!(backend.downloads.load(Ordering::SeqCst), 0);
        assert_eq!(backend.range_downloads.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(&destination).expect("cache file should exist"), payload);
        assert!(!partial_path.exists());
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_accepts_resumable_backend_restart_after_range_http_200() {
        struct RestartingResumeBackend {
            payload: Vec<u8>,
            range_downloads: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl StorageBackend for RestartingResumeBackend {
            fn supports_resumable_downloads(&self) -> bool {
                true
            }

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
                _progress: Option<&TransferProgress<'_>>,
            ) -> Result<()> {
                tokio::fs::write(dest, &self.payload).await?;
                Ok(())
            }

            async fn download_to_file_from_offset(
                &self,
                _key: &str,
                dest: &Path,
                _offset: u64,
                _progress: Option<&TransferProgress<'_>>,
            ) -> Result<()> {
                self.range_downloads.fetch_add(1, Ordering::SeqCst);
                tokio::fs::write(dest, &self.payload).await?;
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

        let tmp = tempfile::tempdir().expect("tempdir");
        let destination = tmp.path().join("artifact.bin");
        let payload = b"remote-payload".to_vec();
        let partial_path = partial_download_path(&destination).expect("partial path");
        std::fs::write(&partial_path, b"remote-").expect("write partial");

        let backend = RestartingResumeBackend {
            payload: payload.clone(),
            range_downloads: AtomicUsize::new(0),
        };

        let outcome = fetch_or_reuse_file(&backend, "artifact.bin", &destination, &sha256_hex(&payload), None)
            .await
            .expect("fetch should accept restarted full response");

        assert_eq!(outcome, CacheFetchOutcome::DownloadedFresh);
        assert_eq!(backend.range_downloads.load(Ordering::SeqCst), 1);
        assert_eq!(std::fs::read(&destination).expect("cache file should exist"), payload);
        assert!(!partial_path.exists());
    }

    #[tokio::test]
    async fn fetch_or_reuse_file_keeps_resumable_partial_after_download_error() {
        struct FailingResumeBackend;

        #[async_trait::async_trait]
        impl StorageBackend for FailingResumeBackend {
            fn supports_resumable_downloads(&self) -> bool {
                true
            }

            async fn put_object(&self, _key: &str, _data: &[u8], _content_type: &str) -> Result<()> {
                unimplemented!("test backend is read-only")
            }

            async fn get_object(&self, _key: &str) -> Result<Vec<u8>> {
                unimplemented!("test backend only supports file downloads")
            }

            async fn head_object(&self, _key: &str) -> Result<ObjectInfo> {
                Ok(ObjectInfo {
                    size: 14,
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
                _dest: &Path,
                _progress: Option<&TransferProgress<'_>>,
            ) -> Result<()> {
                Err(SurgeError::Storage("simulated transfer failure".to_string()))
            }

            async fn download_to_file_from_offset(
                &self,
                _key: &str,
                _dest: &Path,
                _offset: u64,
                _progress: Option<&TransferProgress<'_>>,
            ) -> Result<()> {
                Err(SurgeError::Storage("simulated transfer failure".to_string()))
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

        let tmp = tempfile::tempdir().expect("tempdir");
        let destination = tmp.path().join("artifact.bin");
        let partial_path = partial_download_path(&destination).expect("partial path");
        std::fs::write(&partial_path, b"remote-").expect("write partial");

        let result = fetch_or_reuse_file(
            &FailingResumeBackend,
            "artifact.bin",
            &destination,
            &sha256_hex(b"remote-payload"),
            None,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(std::fs::read(&partial_path).expect("partial should remain"), b"remote-");
        assert!(!destination.exists());
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
