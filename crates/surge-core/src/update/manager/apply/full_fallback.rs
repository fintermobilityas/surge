//! Rebuild a release's full package from the release graph when the publisher never uploaded it.

use std::path::Path;

use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::releases::manifest::{ReleaseEntry, ReleaseIndex};
use crate::releases::restore::restore_full_archive_for_version;
use crate::storage::StorageBackend;

use super::UpdateManager;

/// Restore `release`'s full archive through the newest available full plus the delta chain, verify
/// it against the release's recorded SHA-256 and place it at `cache_path`.
pub(in crate::update::manager) async fn restore_full_into_cache(
    manager: &UpdateManager,
    release: &ReleaseEntry,
    cache_path: &Path,
) -> Result<()> {
    let index = manager
        .cached_index
        .as_ref()
        .ok_or_else(|| SurgeError::Update("release index is not loaded".to_string()))?;
    let archive = restore_verified_full_archive(manager.storage.as_ref(), index, release).await?;
    write_atomically(cache_path, &archive).await
}

/// Restore and verify the full archive for `release` without touching the cache.
pub(in crate::update::manager) async fn restore_verified_full_archive(
    storage: &dyn StorageBackend,
    index: &ReleaseIndex,
    release: &ReleaseEntry,
) -> Result<Vec<u8>> {
    let archive = restore_full_archive_for_version(storage, index, &release.rid, &release.version).await?;
    let expected = release.full_sha256.trim();
    if !expected.is_empty() {
        let actual = sha256_hex(&archive);
        if actual != expected {
            return Err(SurgeError::Integrity(format!(
                "Rebuilt full archive for {} ({}) has SHA-256 {actual}, release manifest expects {expected}",
                release.version, release.rid
            )));
        }
    }
    Ok(archive)
}

async fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temp = path.with_extension("rebuilding");
    tokio::fs::write(&temp, bytes).await?;
    tokio::fs::rename(&temp, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::releases::manifest::DeltaArtifact;
    use crate::storage::filesystem::FilesystemBackend;

    fn release(version: &str, rid: &str, full: &[u8]) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["production".to_string()],
            os: "linux".to_string(),
            rid: rid.to_string(),
            is_genesis: false,
            full_filename: format!("demo-{version}-{rid}-full.tar.zst"),
            full_size: i64::try_from(full.len()).unwrap(),
            full_sha256: sha256_hex(full),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas: Vec::new(),
            preferred_delta_id: String::new(),
            created_utc: chrono::Utc::now().to_rfc3339(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demoapp".to_string(),
            install_directory: "demoapp".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: Default::default(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn rebuilds_a_missing_full_from_the_previous_full_and_the_delta_and_verifies_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();
        let rid = "linux-x64";

        let full_v1 = b"release-1-full".to_vec();
        let full_v2 = b"release-2-content".to_vec();
        let delta_v2 = zstd::encode_all(bsdiff_buffers(&full_v1, &full_v2).unwrap().as_slice(), 3).unwrap();

        let v1 = release("1.1.0", rid, &full_v1);
        let mut v2 = release("1.2.0", rid, &full_v2);
        let delta_key = format!("demo-1.2.0-{rid}-delta.tar.zst");
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            &delta_key,
            i64::try_from(delta_v2.len()).unwrap(),
            &sha256_hex(&delta_v2),
        )));
        // The publisher skipped v2's full upload: only v1's full and v2's delta exist.
        std::fs::write(store.join(&v1.full_filename), &full_v1).unwrap();
        std::fs::write(store.join(&delta_key), &delta_v2).unwrap();
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2.clone()],
            ..ReleaseIndex::default()
        };
        let backend = FilesystemBackend::new(store.to_str().unwrap(), "");

        let restored = restore_verified_full_archive(&backend, &index, &v2).await.unwrap();

        assert_eq!(restored, full_v2);
    }

    #[tokio::test]
    async fn rejects_a_rebuilt_full_whose_hash_does_not_match_the_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = tmp.path().join("store");
        std::fs::create_dir_all(&store).unwrap();
        let rid = "linux-x64";
        let full_v1 = b"release-1-full".to_vec();
        let mut v1 = release("1.1.0", rid, &full_v1);
        v1.full_sha256 = "deadbeef".to_string();
        std::fs::write(store.join(&v1.full_filename), &full_v1).unwrap();
        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone()],
            ..ReleaseIndex::default()
        };
        let backend = FilesystemBackend::new(store.to_str().unwrap(), "");

        let err = restore_verified_full_archive(&backend, &index, &v1).await.unwrap_err();

        assert!(matches!(err, SurgeError::Integrity(_)), "{err}");
    }
}
