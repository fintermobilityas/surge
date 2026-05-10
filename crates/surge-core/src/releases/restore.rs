//! Release artifact graph helpers for restore/reconstruction and pruning.

mod candidate;
mod planning;
mod recovery;
mod retention;

use std::path::Path;

pub use self::planning::{
    find_previous_release_for_rid, find_release_for_version_rid, plan_full_archive_restore, sorted_releases_for_rid,
};
pub use self::recovery::{restore_full_archive_for_version, restore_full_archive_for_version_with_options};
pub use self::retention::{
    local_checkpoint_artifacts_for_index, required_artifacts_for_index, retained_artifacts_for_cache_policy,
    retained_artifacts_for_cache_policy_without_index,
};

pub type RestoreProgressCallback<'a> = dyn Fn(RestoreProgress) + Send + Sync + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreProgress {
    pub items_done: i64,
    pub items_total: i64,
    pub bytes_done: i64,
    pub bytes_total: i64,
}

#[derive(Default)]
pub struct RestoreOptions<'a> {
    pub cache_dir: Option<&'a Path>,
    pub progress: Option<&'a RestoreProgressCallback<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreArtifactSpec {
    pub key: String,
    pub sha256: String,
    pub size: i64,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap)]

    use super::*;
    use crate::archive::packer::ArchivePacker;
    use crate::crypto::sha256::sha256_hex;
    use crate::diff::chunked::{ChunkedDiffOptions, chunked_bsdiff};
    use crate::diff::wrapper::bsdiff_buffers;
    use crate::releases::artifact_cache::cache_path_for_key;
    use crate::releases::manifest::{DeltaArtifact, ReleaseEntry, ReleaseIndex};
    use crate::storage::StorageBackend;
    use crate::storage::filesystem::FilesystemBackend;

    fn make_entry(version: &str) -> ReleaseEntry {
        ReleaseEntry {
            version: version.to_string(),
            channels: vec!["stable".to_string()],
            os: "linux".to_string(),
            rid: "linux-x64".to_string(),
            is_genesis: false,
            full_filename: format!("demo-{version}-linux-x64-full.tar.zst"),
            full_size: 0,
            full_sha256: String::new(),
            full_compression_level: 0,
            full_zstd_workers: 0,
            deltas: vec![DeltaArtifact::bsdiff_zstd(
                "primary",
                "",
                &format!("demo-{version}-linux-x64-delta.tar.zst"),
                0,
                "",
            )],
            preferred_delta_id: "primary".to_string(),
            created_utc: String::new(),
            release_notes: String::new(),
            name: String::new(),
            main_exe: "demo".to_string(),
            install_directory: "demo".to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: Vec::new(),
            environment: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn test_required_artifacts_prunes_redundant_fulls_and_deltas() {
        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);

        let mut v2 = make_entry("1.1.0");
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta",
            0,
            "",
        )));

        let mut v3 = make_entry("1.2.0");
        v3.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-delta",
            0,
            "",
        )));

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2.clone(), v3.clone()],
            ..ReleaseIndex::default()
        };

        let v2_delta = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        let v3_delta = v3.selected_delta().expect("v3 should have delta descriptor").filename;
        let required = required_artifacts_for_index(&index);
        assert!(required.contains(&v1.full_filename));
        assert!(required.contains(&v2_delta));
        assert!(required.contains(&v3_delta));
        assert!(!required.contains(&v2.full_filename));
        assert!(!required.contains(&v3.full_filename));
    }

    #[tokio::test]
    async fn test_restore_full_archive_rebuilds_from_deltas_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();
        let full_v3 = b"full-v3".to_vec();

        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let patch_v3 = bsdiff_buffers(&full_v2, &full_v3).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        let mut v3 = make_entry("1.2.0");
        v3.full_sha256 = sha256_hex(&full_v3);
        v3.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-linux-x64-delta.tar.zst",
            delta_v3.len() as i64,
            &sha256_hex(&delta_v3),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let v2_delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        let v3_delta_key = v3.selected_delta().expect("v3 should have delta descriptor").filename;
        backend
            .put_object(&v2_delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v3_delta_key, &delta_v3, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2, v3],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version(&backend, &index, "linux-x64", "1.2.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v3);
    }

    #[tokio::test]
    async fn test_restore_full_archive_rebuilds_from_chunked_deltas_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2-with-extra-data".to_vec();
        let full_v3 = b"full-v3-with-even-more-extra-data".to_vec();

        let patch_v2 = chunked_bsdiff(&full_v1, &full_v2, &ChunkedDiffOptions::default()).unwrap();
        let patch_v3 = chunked_bsdiff(&full_v2, &full_v3, &ChunkedDiffOptions::default()).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        let mut v3 = make_entry("1.2.0");
        v3.full_sha256 = sha256_hex(&full_v3);
        v3.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-linux-x64-delta.tar.zst",
            delta_v3.len() as i64,
            &sha256_hex(&delta_v3),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let v2_delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        let v3_delta_key = v3.selected_delta().expect("v3 should have delta descriptor").filename;
        backend
            .put_object(&v2_delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v3_delta_key, &delta_v3, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2, v3],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version(&backend, &index, "linux-x64", "1.2.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v3);
    }

    #[tokio::test]
    async fn test_restore_full_archive_rebuilds_from_archive_chunked_deltas_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let mut packer_v1 = ArchivePacker::new(7).unwrap();
        packer_v1
            .add_buffer("Program.cs", b"Console.WriteLine(\"v1\");\n", 0o644)
            .unwrap();
        packer_v1
            .add_buffer("payload.bin", &vec![b'A'; 1024 * 1024], 0o644)
            .unwrap();
        let full_v1 = packer_v1.finalize().unwrap();

        let mut packer_v2 = ArchivePacker::new(7).unwrap();
        packer_v2
            .add_buffer("Program.cs", b"Console.WriteLine(\"v2\");\n", 0o644)
            .unwrap();
        packer_v2
            .add_buffer("payload.bin", &vec![b'A'; 1024 * 1024], 0o644)
            .unwrap();
        let full_v2 = packer_v2.finalize().unwrap();

        let mut packer_v3 = ArchivePacker::new(7).unwrap();
        packer_v3
            .add_buffer("Program.cs", b"Console.WriteLine(\"v3\");\n", 0o644)
            .unwrap();
        packer_v3
            .add_buffer("payload.bin", &vec![b'A'; 1024 * 1024], 0o644)
            .unwrap();
        let full_v3 = packer_v3.finalize().unwrap();

        let patch_v2 = crate::releases::delta::build_archive_chunked_patch(
            &full_v1,
            &full_v2,
            7,
            0,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let patch_v3 = crate::releases::delta::build_archive_chunked_patch(
            &full_v2,
            &full_v3,
            7,
            0,
            &ChunkedDiffOptions::default(),
        )
        .unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();
        let delta_v3 = zstd::encode_all(patch_v3.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = crate::crypto::sha256::sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = crate::crypto::sha256::sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &crate::crypto::sha256::sha256_hex(&delta_v2),
        )));

        let mut v3 = make_entry("1.2.0");
        v3.full_sha256 = crate::crypto::sha256::sha256_hex(&full_v3);
        v3.set_primary_delta(Some(DeltaArtifact::chunked_bsdiff_archive_zstd(
            "primary",
            "1.1.0",
            "demo-1.2.0-linux-x64-delta.tar.zst",
            delta_v3.len() as i64,
            &crate::crypto::sha256::sha256_hex(&delta_v3),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let v2_delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        let v3_delta_key = v3.selected_delta().expect("v3 should have delta descriptor").filename;
        backend
            .put_object(&v2_delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v3_delta_key, &delta_v3, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2, v3],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version(&backend, &index, "linux-x64", "1.2.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v3);
    }

    #[tokio::test]
    async fn test_restore_full_archive_prefers_direct_full_when_available() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-delta",
            13,
            &sha256_hex(b"invalid-delta"),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v2.full_filename, &full_v2, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version(&backend, &index, "linux-x64", "1.1.0")
            .await
            .unwrap();
        assert_eq!(restored, full_v2);
    }

    #[tokio::test]
    async fn test_restore_full_archive_uses_local_cache_when_backend_artifacts_are_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).unwrap();
        let cache_root = tmp.path().join("cache");
        std::fs::create_dir_all(&cache_root).unwrap();
        let backend = FilesystemBackend::new(backend_root.to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let v2_delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        backend
            .put_object(&v2_delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1, v2],
            ..ReleaseIndex::default()
        };

        let first = restore_full_archive_for_version_with_options(
            &backend,
            &index,
            "linux-x64",
            "1.1.0",
            RestoreOptions {
                cache_dir: Some(&cache_root),
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(first, full_v2);

        std::fs::remove_dir_all(&backend_root).unwrap();
        std::fs::create_dir_all(&backend_root).unwrap();

        let second = restore_full_archive_for_version_with_options(
            &backend,
            &index,
            "linux-x64",
            "1.1.0",
            RestoreOptions {
                cache_dir: Some(&cache_root),
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(second, full_v2);
    }

    #[tokio::test]
    async fn test_restore_full_archive_prefers_cached_graph_over_direct_full_download() {
        let tmp = tempfile::tempdir().unwrap();
        let backend_root = tmp.path().join("backend");
        std::fs::create_dir_all(&backend_root).unwrap();
        let cache_root = tmp.path().join("cache");
        std::fs::create_dir_all(&cache_root).unwrap();
        let backend = FilesystemBackend::new(backend_root.to_str().unwrap(), "");

        let full_v1 = vec![b'a'; 4096];
        let mut full_v2 = full_v1.clone();
        full_v2[0] = b'b';
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_size = i64::try_from(full_v1.len()).unwrap();
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_size = i64::try_from(full_v2.len()).unwrap();
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            i64::try_from(delta_v2.len()).unwrap(),
            &sha256_hex(&delta_v2),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        backend
            .put_object(&v2.full_filename, &full_v2, "application/octet-stream")
            .await
            .unwrap();
        let delta_key = v2.selected_delta().unwrap().filename.clone();
        backend
            .put_object(&delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();

        let cached_v1 = cache_path_for_key(&cache_root, &v1.full_filename).unwrap();
        let cached_delta = cache_path_for_key(&cache_root, &delta_key).unwrap();
        std::fs::create_dir_all(cached_v1.parent().unwrap()).unwrap();
        std::fs::write(&cached_v1, &full_v1).unwrap();
        std::fs::write(&cached_delta, &delta_v2).unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2.clone()],
            ..ReleaseIndex::default()
        };

        let restored = restore_full_archive_for_version_with_options(
            &backend,
            &index,
            "linux-x64",
            "1.1.0",
            RestoreOptions {
                cache_dir: Some(&cache_root),
                progress: None,
            },
        )
        .await
        .unwrap();

        assert_eq!(restored, full_v2);
        assert!(
            cache_path_for_key(&cache_root, &v2.full_filename).unwrap().exists(),
            "reconstructed full should be retained as a local checkpoint"
        );
    }

    #[tokio::test]
    async fn test_plan_full_archive_restore_reports_delta_chain_when_direct_full_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(tmp.path().to_str().unwrap(), "");

        let full_v1 = b"full-v1".to_vec();
        let full_v2 = b"full-v2".to_vec();
        let patch_v2 = bsdiff_buffers(&full_v1, &full_v2).unwrap();
        let delta_v2 = zstd::encode_all(patch_v2.as_slice(), 3).unwrap();

        let mut v1 = make_entry("1.0.0");
        v1.set_primary_delta(None);
        v1.full_sha256 = sha256_hex(&full_v1);

        let mut v2 = make_entry("1.1.0");
        v2.full_sha256 = sha256_hex(&full_v2);
        v2.set_primary_delta(Some(DeltaArtifact::bsdiff_zstd(
            "primary",
            "1.0.0",
            "demo-1.1.0-linux-x64-delta.tar.zst",
            delta_v2.len() as i64,
            &sha256_hex(&delta_v2),
        )));

        backend
            .put_object(&v1.full_filename, &full_v1, "application/octet-stream")
            .await
            .unwrap();
        let delta_key = v2.selected_delta().expect("v2 should have delta descriptor").filename;
        backend
            .put_object(&delta_key, &delta_v2, "application/octet-stream")
            .await
            .unwrap();

        let index = ReleaseIndex {
            app_id: "demo".to_string(),
            releases: vec![v1.clone(), v2],
            ..ReleaseIndex::default()
        };

        let specs = plan_full_archive_restore(&backend, &index, "linux-x64", "1.1.0")
            .await
            .unwrap();

        assert_eq!(
            specs,
            vec![
                RestoreArtifactSpec {
                    key: v1.full_filename,
                    sha256: v1.full_sha256,
                    size: v1.full_size,
                },
                RestoreArtifactSpec {
                    key: delta_key,
                    sha256: sha256_hex(&delta_v2),
                    size: delta_v2.len() as i64,
                },
            ]
        );
    }
}
