use super::archive::{
    ARCHIVE_BSDIFF_MAGIC, ARCHIVE_CHUNKED_MAGIC, LEGACY_ARCHIVE_BSDIFF_MAGIC, LEGACY_ARCHIVE_CHUNKED_MAGIC,
    decode_archive_patch_payload,
};
use super::sparse_ops::SPARSE_FILE_OPS_MAGIC;
use super::*;
use crate::archive::packer::ArchivePacker;
use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::ChunkedDiffOptions;
use crate::releases::manifest::{
    DeltaArtifact, PATCH_FORMAT_BSDIFF4_ARCHIVE_V3, PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3,
};

fn make_archive(version: &str, compression_level: i32, zstd_workers: u32) -> Vec<u8> {
    let mut packer = if zstd_workers > 1 {
        ArchivePacker::with_threads(compression_level, zstd_workers).unwrap()
    } else {
        ArchivePacker::new(compression_level).unwrap()
    };
    let banner = format!("console write for {version}\n");
    packer.add_buffer("Program.cs", banner.as_bytes(), 0o644).unwrap();
    packer
        .add_buffer("demoapp.csproj", b"<Project Sdk=\"Microsoft.NET.Sdk\" />\n", 0o644)
        .unwrap();
    packer
        .add_buffer("assets/payload.bin", &vec![b'Z'; 8 * 1024 * 1024], 0o644)
        .unwrap();
    packer
        .add_buffer("assets/aux.bin", &vec![b'Q'; 4 * 1024 * 1024], 0o644)
        .unwrap();
    packer.finalize().unwrap()
}

#[test]
fn test_patch_format_from_magic_prefix_detects_archive_formats() {
    assert_eq!(
        patch_format_from_magic_prefix(SPARSE_FILE_OPS_MAGIC),
        Some(PATCH_FORMAT_SPARSE_FILE_OPS_V1)
    );
    assert_eq!(
        patch_format_from_magic_prefix(LEGACY_ARCHIVE_BSDIFF_MAGIC),
        Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3)
    );
    assert_eq!(
        patch_format_from_magic_prefix(ARCHIVE_BSDIFF_MAGIC),
        Some(PATCH_FORMAT_BSDIFF4_ARCHIVE_V3)
    );
    assert_eq!(
        patch_format_from_magic_prefix(LEGACY_ARCHIVE_CHUNKED_MAGIC),
        Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3)
    );
    assert_eq!(
        patch_format_from_magic_prefix(ARCHIVE_CHUNKED_MAGIC),
        Some(PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3)
    );
}

#[test]
fn test_archive_bsdiff_patch_roundtrip_rebuilds_full_archive_bytes() {
    let zstd_workers = 4;
    let full_v1 = make_archive("1.0.0", 7, zstd_workers);
    let full_v2 = make_archive("1.1.0", 7, zstd_workers);
    let patch = build_archive_bsdiff_patch(&full_v1, &full_v2, 7, zstd_workers).unwrap();
    let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
    let delta = DeltaArtifact::bsdiff_archive_zstd(
        "primary",
        "1.0.0",
        "demo-1.1.0-delta.tar.zst",
        i64::try_from(delta_bytes.len()).unwrap(),
        &sha256_hex(&delta_bytes),
    );

    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
    assert_eq!(rebuilt, full_v2);
}

#[test]
fn test_legacy_archive_bsdiff_patch_magic_roundtrip_rebuilds_full_archive_bytes() {
    let zstd_workers = 4;
    let full_v1 = make_archive("1.0.0", 7, zstd_workers);
    let full_v2 = make_archive("1.1.0", 7, zstd_workers);
    let mut patch = build_archive_bsdiff_patch(&full_v1, &full_v2, 7, zstd_workers).unwrap();
    patch[..LEGACY_ARCHIVE_BSDIFF_MAGIC.len()].copy_from_slice(LEGACY_ARCHIVE_BSDIFF_MAGIC);
    let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
    let delta = DeltaArtifact::with_patch_format(
        "primary",
        "1.0.0",
        PATCH_FORMAT_BSDIFF4_ARCHIVE_V3,
        "demo-1.1.0-delta.tar.zst",
        i64::try_from(delta_bytes.len()).unwrap(),
        &sha256_hex(&delta_bytes),
    );

    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
    assert_eq!(rebuilt, full_v2);
}

#[test]
fn test_archive_chunked_patch_roundtrip_rebuilds_full_archive_bytes() {
    let zstd_workers = 4;
    let full_v1 = make_archive("1.0.0", 11, zstd_workers);
    let full_v2 = make_archive("1.1.0", 11, zstd_workers);
    let patch =
        build_archive_chunked_patch(&full_v1, &full_v2, 11, zstd_workers, &ChunkedDiffOptions::default()).unwrap();
    let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
    let delta = DeltaArtifact::chunked_bsdiff_archive_zstd(
        "primary",
        "1.0.0",
        "demo-1.1.0-delta.tar.zst",
        i64::try_from(delta_bytes.len()).unwrap(),
        &sha256_hex(&delta_bytes),
    );

    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
    assert_eq!(rebuilt, full_v2);
}

#[test]
fn test_legacy_archive_chunked_patch_magic_roundtrip_rebuilds_full_archive_bytes() {
    let zstd_workers = 4;
    let full_v1 = make_archive("1.0.0", 11, zstd_workers);
    let full_v2 = make_archive("1.1.0", 11, zstd_workers);
    let mut patch =
        build_archive_chunked_patch(&full_v1, &full_v2, 11, zstd_workers, &ChunkedDiffOptions::default()).unwrap();
    patch[..LEGACY_ARCHIVE_CHUNKED_MAGIC.len()].copy_from_slice(LEGACY_ARCHIVE_CHUNKED_MAGIC);
    let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
    let delta = DeltaArtifact::with_patch_format(
        "primary",
        "1.0.0",
        PATCH_FORMAT_CHUNKED_BSDIFF_ARCHIVE_V3,
        "demo-1.1.0-delta.tar.zst",
        i64::try_from(delta_bytes.len()).unwrap(),
        &sha256_hex(&delta_bytes),
    );

    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
    assert_eq!(rebuilt, full_v2);
}

#[test]
fn test_archive_patch_payload_rejects_invalid_magic() {
    let err = decode_archive_patch_payload(b"BAD!\x03\0\0\0payload", *ARCHIVE_BSDIFF_MAGIC, None, None).unwrap_err();
    assert!(err.to_string().contains("magic"));
}

#[test]
fn test_sparse_file_patch_roundtrip_rebuilds_full_archive_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let old_dir = dir.path().join("old");
    let new_dir = dir.path().join("new");
    std::fs::create_dir_all(old_dir.join("bin")).unwrap();
    std::fs::create_dir_all(new_dir.join("bin")).unwrap();
    std::fs::create_dir_all(new_dir.join("models")).unwrap();
    std::fs::write(old_dir.join("bin").join("runtime.bin"), vec![b'A'; 512 * 1024]).unwrap();
    std::fs::write(old_dir.join("config.json"), br#"{"version":1}"#).unwrap();
    std::fs::write(new_dir.join("bin").join("runtime.bin"), {
        let mut bytes = vec![b'A'; 512 * 1024];
        bytes[1234] = b'B';
        bytes
    })
    .unwrap();
    std::fs::write(new_dir.join("config.json"), br#"{"version":2}"#).unwrap();
    std::fs::write(new_dir.join("models").join("model-v2.bin"), vec![b'Z'; 512 * 1024]).unwrap();

    let mut old_packer = ArchivePacker::new(7).unwrap();
    old_packer.add_directory(&old_dir, "").unwrap();
    let full_v1 = old_packer.finalize().unwrap();

    let mut new_packer = ArchivePacker::new(7).unwrap();
    new_packer.add_directory(&new_dir, "").unwrap();
    let full_v2 = new_packer.finalize().unwrap();

    let patch = build_sparse_file_patch(
        &full_v1,
        &full_v2,
        7,
        0,
        &ChunkedDiffOptions {
            chunk_size: 128 * 1024,
            max_threads: 1,
        },
    )
    .unwrap();
    let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
    let delta = DeltaArtifact::sparse_file_ops_zstd(
        "primary",
        "1.0.0",
        "demo-1.1.0-delta.tar.zst",
        i64::try_from(delta_bytes.len()).unwrap(),
        &sha256_hex(&delta_bytes),
    );

    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let rebuilt = apply_delta_patch(&full_v1, &decoded, &delta).unwrap();
    assert_eq!(rebuilt, full_v2);
}

#[test]
fn test_sparse_file_patch_can_apply_directly_to_directory() {
    let dir = tempfile::tempdir().unwrap();
    let old_dir = dir.path().join("old");
    let new_dir = dir.path().join("new");
    std::fs::create_dir_all(old_dir.join("bin")).unwrap();
    std::fs::create_dir_all(new_dir.join("bin")).unwrap();
    std::fs::create_dir_all(new_dir.join("models")).unwrap();
    std::fs::write(old_dir.join("bin").join("runtime.bin"), vec![b'A'; 256 * 1024]).unwrap();
    std::fs::write(old_dir.join("config.json"), br#"{"version":1}"#).unwrap();
    std::fs::write(new_dir.join("bin").join("runtime.bin"), {
        let mut bytes = vec![b'A'; 256 * 1024];
        bytes[2048] = b'B';
        bytes
    })
    .unwrap();
    std::fs::write(new_dir.join("config.json"), br#"{"version":2}"#).unwrap();
    std::fs::write(new_dir.join("models").join("model-v2.bin"), vec![b'Z'; 128 * 1024]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(old_dir.join("bin"), std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(new_dir.join("bin"), std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    let mut old_packer = ArchivePacker::new(7).unwrap();
    old_packer.add_directory(&old_dir, "").unwrap();
    let full_v1 = old_packer.finalize().unwrap();

    let mut new_packer = ArchivePacker::new(7).unwrap();
    new_packer.add_directory(&new_dir, "").unwrap();
    let full_v2 = new_packer.finalize().unwrap();

    let patch = build_sparse_file_patch(
        &full_v1,
        &full_v2,
        7,
        0,
        &ChunkedDiffOptions {
            chunk_size: 64 * 1024,
            max_threads: 1,
        },
    )
    .unwrap();
    let delta_bytes = zstd::encode_all(patch.as_slice(), 3).unwrap();
    let delta = DeltaArtifact::sparse_file_ops_zstd(
        "primary",
        "1.0.0",
        "demo-1.1.0-delta.tar.zst",
        i64::try_from(delta_bytes.len()).unwrap(),
        &sha256_hex(&delta_bytes),
    );
    assert!(is_sparse_file_ops_delta(&delta));

    let working_dir = tempfile::tempdir().unwrap();
    crate::archive::extractor::extract_to(&full_v1, working_dir.path(), None).unwrap();

    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let archive_settings = apply_sparse_file_patch_to_directory(working_dir.path(), &decoded).unwrap();
    assert_eq!(archive_settings, (7, 0));

    let mut rebuilt_packer = ArchivePacker::new(7).unwrap();
    rebuilt_packer.add_directory(working_dir.path(), "").unwrap();
    let rebuilt = rebuilt_packer.finalize().unwrap();

    assert_eq!(rebuilt, full_v2);
    assert_eq!(
        std::fs::read_to_string(working_dir.path().join("config.json")).unwrap(),
        r#"{"version":2}"#
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(working_dir.path().join("bin"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700);
    }
}
