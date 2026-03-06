use std::path::{Component, Path, PathBuf};

use crate::crypto::sha256::sha256_hex_file;
use crate::error::{Result, SurgeError};
use crate::storage::{StorageBackend, TransferProgress};

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

pub async fn fetch_or_reuse_file(
    storage: &dyn StorageBackend,
    key: &str,
    destination: &Path,
    expected_sha256: &str,
    progress: Option<&TransferProgress<'_>>,
) -> Result<CacheFetchOutcome> {
    let expected = expected_sha256.trim();
    let had_local = destination.is_file();
    if !expected.is_empty() && had_local && sha256_matches_file(destination, expected)? {
        return Ok(CacheFetchOutcome::ReusedLocal);
    }

    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    storage.download_to_file(key, destination, progress).await?;

    if !expected.is_empty() && !sha256_matches_file(destination, expected)? {
        return Err(SurgeError::Storage(format!(
            "SHA-256 mismatch for '{key}' after download"
        )));
    }

    if had_local && !expected.is_empty() {
        Ok(CacheFetchOutcome::DownloadedAfterInvalidLocal)
    } else {
        Ok(CacheFetchOutcome::DownloadedFresh)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::sha256::sha256_hex;
    use crate::storage::filesystem::FilesystemBackend;

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
}
