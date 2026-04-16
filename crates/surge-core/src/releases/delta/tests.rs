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
fn test_delta_target_archive_encoding_reads_sparse_file_patch_settings() {
    let full_v1 = make_archive("1.0.0", 7, 0);
    let full_v2 = make_archive("1.1.0", 7, 4);
    let patch = build_sparse_file_patch(&full_v1, &full_v2, 7, 4, &ChunkedDiffOptions::default()).unwrap();
    let delta = DeltaArtifact::sparse_file_ops_zstd("primary", "1.0.0", "demo-1.1.0-delta.tar.zst", 1, "sha");

    let encoding = delta_target_archive_encoding(&patch, &delta).unwrap();
    assert_eq!(encoding, Some((7, 4)));
}

#[test]
fn test_delta_target_archive_encoding_reads_archive_chunked_settings() {
    let full_v1 = make_archive("1.0.0", 11, 0);
    let full_v2 = make_archive("1.1.0", 11, 4);
    let patch = build_archive_chunked_patch(&full_v1, &full_v2, 11, 4, &ChunkedDiffOptions::default()).unwrap();
    let delta = DeltaArtifact::chunked_bsdiff_archive_zstd("primary", "1.0.0", "demo-1.1.0-delta.tar.zst", 1, "sha");

    let encoding = delta_target_archive_encoding(&patch, &delta).unwrap();
    assert_eq!(encoding, Some((11, 4)));
}
