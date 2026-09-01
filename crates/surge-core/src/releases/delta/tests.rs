use super::archive::{
    ARCHIVE_BSDIFF_MAGIC, ARCHIVE_CHUNKED_MAGIC, LEGACY_ARCHIVE_BSDIFF_MAGIC, LEGACY_ARCHIVE_CHUNKED_MAGIC,
    decode_archive_patch_payload,
};
use super::sparse_diff::build_sparse_file_patch_in_memory;
use super::sparse_ops::{
    SPARSE_FILE_OPS_MAGIC, apply_sparse_file_patch_with_progress, apply_sparse_step_in_place,
    build_sparse_file_patch_via_disk,
};
use super::*;
use crate::archive::extractor::extract_to;
use crate::archive::packer::ArchivePacker;
use crate::crypto::sha256::sha256_hex;
use crate::diff::chunked::{ChunkedDiffOptions, ChunkedPatchFormat};
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
            format: ChunkedPatchFormat::Legacy,
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
fn test_sparse_file_patch_reports_incremental_apply_progress() {
    let dir = tempfile::tempdir().unwrap();
    let old_dir = dir.path().join("old");
    let new_dir = dir.path().join("new");
    std::fs::create_dir_all(old_dir.join("bin")).unwrap();
    std::fs::create_dir_all(new_dir.join("bin")).unwrap();
    std::fs::create_dir_all(new_dir.join("models")).unwrap();
    std::fs::write(old_dir.join("bin").join("runtime.bin"), vec![b'A'; 512 * 1024]).unwrap();
    std::fs::write(new_dir.join("bin").join("runtime.bin"), {
        let mut bytes = vec![b'A'; 512 * 1024];
        bytes[4096] = b'B';
        bytes
    })
    .unwrap();
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
            format: ChunkedPatchFormat::Legacy,
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

    let observed = std::sync::Mutex::new(Vec::new());
    let decoded = decode_delta_patch(&delta_bytes, &delta).unwrap();
    let rebuilt = apply_delta_patch_with_progress(
        &full_v1,
        &decoded,
        &delta,
        Some(&|progress| {
            observed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(progress);
        }),
    )
    .unwrap();

    assert_eq!(rebuilt, full_v2);
    let observed = observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        observed
            .iter()
            .any(|progress| progress.units_done > 0 && progress.units_done < progress.units_total),
        "expected at least one intermediate sparse delta apply progress event"
    );
    let final_progress = observed.last().expect("expected sparse delta apply progress");
    assert_eq!(final_progress.units_done, final_progress.units_total);
}

// Multithreaded-zstd variant of the roundtrip test. Mirrors what production
// pack actually does: full archive and sparse delta are both built with the
// same (level, workers) as provided by ResourceBudget (by default 4 workers).
// The rebuilt-via-delta archive must be byte-identical to the original full.
#[test]
fn test_sparse_file_patch_roundtrip_rebuilds_full_archive_bytes_multithreaded() {
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

    let mut old_packer = ArchivePacker::with_threads(9, 4).unwrap();
    old_packer.add_directory(&old_dir, "").unwrap();
    let full_v1 = old_packer.finalize().unwrap();

    let mut new_packer = ArchivePacker::with_threads(9, 4).unwrap();
    new_packer.add_directory(&new_dir, "").unwrap();
    let full_v2 = new_packer.finalize().unwrap();

    let patch = build_sparse_file_patch(&full_v1, &full_v2, 9, 4, &ChunkedDiffOptions::default()).unwrap();
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
    assert_eq!(
        sha256_hex(&rebuilt),
        sha256_hex(&full_v2),
        "sparse-file-ops rebuild must match the multithreaded-packed full archive bit-for-bit"
    );
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

#[test]
fn test_in_place_chain_steps_match_single_shot_apply() {
    let dir = tempfile::tempdir().unwrap();

    // v1: base; v2: config change; v3: model change.
    let v1_dir = dir.path().join("v1");
    let v2_dir = dir.path().join("v2");
    let v3_dir = dir.path().join("v3");
    for v in [&v1_dir, &v2_dir, &v3_dir] {
        std::fs::create_dir_all(v.join("bin")).unwrap();
        std::fs::create_dir_all(v.join("models")).unwrap();
    }
    for v in [&v1_dir, &v2_dir, &v3_dir] {
        std::fs::write(v.join("bin").join("runtime.bin"), vec![b'A'; 256 * 1024]).unwrap();
    }
    std::fs::write(v1_dir.join("config.json"), br#"{"version":1}"#).unwrap();
    std::fs::write(v2_dir.join("config.json"), br#"{"version":2}"#).unwrap();
    std::fs::write(v3_dir.join("config.json"), br#"{"version":3}"#).unwrap();
    std::fs::write(v3_dir.join("models").join("model.bin"), vec![b'M'; 256 * 1024]).unwrap();

    let pack = |d: &std::path::Path| -> Vec<u8> {
        let mut packer = ArchivePacker::new(7).unwrap();
        packer.add_directory(d, "").unwrap();
        packer.finalize().unwrap()
    };
    let full_v1 = pack(&v1_dir);
    let full_v2 = pack(&v2_dir);
    let full_v3 = pack(&v3_dir);

    let opts = ChunkedDiffOptions {
        chunk_size: 128 * 1024,
        max_threads: 1,
        format: ChunkedPatchFormat::Legacy,
    };
    let patch_12 = build_sparse_file_patch(&full_v1, &full_v2, 7, 0, &opts).unwrap();
    let patch_23 = build_sparse_file_patch(&full_v2, &full_v3, 7, 0, &opts).unwrap();

    // Single-shot reference: each delta applied to the previous archive.
    let single_step1 = apply_sparse_file_patch_with_progress(&full_v1, &patch_12, None).unwrap();
    assert_eq!(single_step1, full_v2);
    let single_step2 = apply_sparse_file_patch_with_progress(&single_step1, &patch_23, None).unwrap();
    assert_eq!(single_step2, full_v3);

    // In-place chain: extract once, carry the tree across both steps.
    let chain_dir = dir.path().join("chain");
    std::fs::create_dir_all(&chain_dir).unwrap();
    extract_to(&full_v1, &chain_dir, None).unwrap();
    // The shared verified-hash cache is what production chain walks use:
    // step 2's basis check for config.json is answered from step 1's
    // target verification instead of a full re-hash.
    let mut verified = VerifiedFileHashes::new();
    let chain_step1 = apply_sparse_step_in_place(&chain_dir, &patch_12, 0, None, &mut verified).unwrap();
    assert_eq!(chain_step1, single_step1);
    let chain_step2 = apply_sparse_step_in_place(&chain_dir, &patch_23, 0, None, &mut verified).unwrap();
    assert_eq!(chain_step2, single_step2);
}

#[test]
fn test_in_place_chain_with_verified_cache_detects_external_modification() {
    let dir = tempfile::tempdir().unwrap();

    let v1_dir = dir.path().join("v1");
    let v2_dir = dir.path().join("v2");
    let v3_dir = dir.path().join("v3");
    for v in [&v1_dir, &v2_dir, &v3_dir] {
        std::fs::create_dir_all(v).unwrap();
        let payload = (0..(256 * 1024)).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        std::fs::write(v.join("payload.bin"), &payload).unwrap();
    }
    for (v, tag) in [
        (v1_dir.as_path(), 1u8),
        (v2_dir.as_path(), 2u8),
        (v3_dir.as_path(), 3u8),
    ] {
        let mut p = std::fs::read(v.join("payload.bin")).unwrap();
        p[0] = tag;
        std::fs::write(v.join("payload.bin"), p).unwrap();
    }

    let pack = |d: &std::path::Path| -> Vec<u8> {
        let mut packer = ArchivePacker::new(7).unwrap();
        packer.add_directory(d, "").unwrap();
        packer.finalize().unwrap()
    };
    let full_v1 = pack(&v1_dir);
    let full_v2 = pack(&v2_dir);
    let full_v3 = pack(&v3_dir);

    let opts = ChunkedDiffOptions {
        chunk_size: 128 * 1024,
        max_threads: 1,
        format: ChunkedPatchFormat::Legacy,
    };
    let patch_12 = build_sparse_file_patch(&full_v1, &full_v2, 7, 0, &opts).unwrap();
    let patch_23 = build_sparse_file_patch(&full_v2, &full_v3, 7, 0, &opts).unwrap();

    let chain_dir = dir.path().join("chain");
    extract_to(&full_v1, &chain_dir, None).unwrap();
    let mut verified = VerifiedFileHashes::new();
    let step1 = apply_sparse_step_in_place(&chain_dir, &patch_12, 0, None, &mut verified).unwrap();
    assert_eq!(step1, full_v2);
    // The cache now believes payload.bin holds the v2 content.

    // Simulate external corruption between steps (bypasses the cache).
    let mut corrupted = std::fs::read(chain_dir.join("payload.bin")).unwrap();
    corrupted[1] = 0xFF;
    std::fs::write(chain_dir.join("payload.bin"), corrupted).unwrap();

    // Step 2 skips the basis re-hash via the cache, but the post-patch
    // target hash must still catch the corrupted source.
    let err = apply_sparse_step_in_place(&chain_dir, &patch_23, 0, None, &mut verified).unwrap_err();
    assert!(
        err.to_string().contains("hash mismatch"),
        "expected a hash mismatch, got: {err}"
    );
}

/// Shared body for the in-memory vs disk equivalence checks.
fn sparse_equivalence_check(mode_old: u32, mode_new: u32) {
    let opts = ChunkedDiffOptions {
        chunk_size: 256 * 1024,
        max_threads: 1,
        format: ChunkedPatchFormat::IdentityChunks,
    };

    let big = vec![0xA5u8; 1_200_000];
    let mut packer = ArchivePacker::new(3).unwrap();
    packer.add_buffer("big.bin", &big, 0o644).unwrap();
    packer.add_buffer("same.bin", b"unchanged\n", 0o644).unwrap();
    packer.add_buffer("mode_only.bin", b"mode\n", mode_old).unwrap();
    packer.add_buffer("gone.bin", b"bye\n", 0o644).unwrap();
    let old_archive = packer.finalize().unwrap();

    let mut next_big = big;
    next_big[300_000] = 0x5A; // one page inside the second 256 KiB chunk
    let mut packer = ArchivePacker::new(3).unwrap();
    packer.add_buffer("big.bin", &next_big, 0o644).unwrap();
    packer.add_buffer("same.bin", b"unchanged\n", 0o644).unwrap();
    packer.add_buffer("mode_only.bin", b"mode\n", mode_new).unwrap();
    packer.add_buffer("added.bin", b"new file\n", 0o644).unwrap();
    let new_archive = packer.finalize().unwrap();

    let mem_patch =
        build_sparse_file_patch_in_memory(&old_archive, &new_archive, 3, 1, &opts).expect("in-memory sparse build");
    let disk_patch =
        build_sparse_file_patch_via_disk(&old_archive, &new_archive, 3, 1, &opts).expect("disk sparse build");
    assert_eq!(
        mem_patch, disk_patch,
        "in-memory and disk sparse builds must be byte-identical"
    );

    // Round trip: applying the in-memory patch yields a tree that matches the
    // next version exactly (the rebuilt archive may reorder entries relative to
    // the add_buffer insertion order above, so compare the extracted tree).
    let rebuilt = apply_sparse_file_patch_with_progress(&old_archive, &mem_patch, None).expect("apply in-memory patch");
    let check_dir = tempfile::tempdir().unwrap();
    extract_to(&rebuilt, check_dir.path(), None).unwrap();

    let big_on_disk = std::fs::read(check_dir.path().join("big.bin")).unwrap();
    assert_eq!(big_on_disk, next_big, "patched file must match the next version");
    assert_eq!(
        std::fs::read(check_dir.path().join("same.bin")).unwrap(),
        b"unchanged\n"
    );
    assert_eq!(
        std::fs::read(check_dir.path().join("mode_only.bin")).unwrap(),
        b"mode\n"
    );
    assert_eq!(
        std::fs::read(check_dir.path().join("added.bin")).unwrap(),
        b"new file\n"
    );
    assert!(!check_dir.path().join("gone.bin").exists(), "deleted file must be gone");
}

#[test]
fn in_memory_sparse_patch_is_byte_identical_to_disk_build() {
    // Non-unix filesystems do not report the tar header modes after
    // extraction, so the disk reference build cannot observe a mode change;
    // the mode-free core is the cross-platform invariant.
    sparse_equivalence_check(0o644, 0o644);
}

#[cfg(unix)]
#[test]
fn in_memory_sparse_patch_mode_changes_match_disk_build() {
    sparse_equivalence_check(0o600, 0o640);
}
