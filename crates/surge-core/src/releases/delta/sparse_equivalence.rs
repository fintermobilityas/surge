//! Equivalence checks between the sparse delta build paths: the historical
//! disk extract+compare builder, the in-memory builder, and the
//! packed-directory newer side must all produce identical patches.

use std::fs;

use super::sparse_diff::build_sparse_file_patch_in_memory;
use super::sparse_ops::{apply_sparse_file_patch_with_progress, build_sparse_file_patch_via_disk};
use crate::archive::extractor::extract_to;
use crate::archive::packer::ArchivePacker;
use crate::diff::chunked::{ChunkedDiffOptions, ChunkedPatchFormat};
use crate::releases::delta::{build_sparse_file_patch, build_sparse_file_patch_with_tree_from_directory};

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

#[test]
fn directory_newer_side_matches_archive_build() {
    let opts = ChunkedDiffOptions {
        chunk_size: 256 * 1024,
        max_threads: 1,
        format: ChunkedPatchFormat::IdentityChunks,
    };

    let dir_v1 = tempfile::tempdir().unwrap();
    let dir_v2 = tempfile::tempdir().unwrap();
    let big = vec![0x42u8; 300_000];
    let mut next_big = big.clone();
    next_big[123_456] = 0x99;

    fs::write(dir_v1.path().join("big.bin"), &big).unwrap();
    fs::write(dir_v1.path().join("same.bin"), b"steady\n").unwrap();
    fs::write(dir_v1.path().join("gone.bin"), b"bye\n").unwrap();
    fs::write(dir_v1.path().join("tool"), b"tool v1\n").unwrap();
    fs::write(dir_v2.path().join("big.bin"), &next_big).unwrap();
    fs::write(dir_v2.path().join("same.bin"), b"steady\n").unwrap();
    fs::write(dir_v2.path().join("tool"), b"tool v2\n").unwrap();
    fs::write(dir_v2.path().join("added.bin"), b"new file\n").unwrap();

    let overrides: std::collections::BTreeSet<String> = ["tool".to_string()].into();
    let mut pack_v1 = ArchivePacker::new(3).unwrap();
    pack_v1
        .add_directory_with_executable_overrides(dir_v1.path(), "", &overrides)
        .unwrap();
    let old_archive = pack_v1.finalize().unwrap();
    let mut pack_v2 = ArchivePacker::new(3).unwrap();
    pack_v2
        .add_directory_with_executable_overrides(dir_v2.path(), "", &overrides)
        .unwrap();
    let new_archive = pack_v2.finalize().unwrap();

    let (from_dir, _) = build_sparse_file_patch_with_tree_from_directory(
        None,
        &old_archive,
        "",
        dir_v2.path(),
        &overrides,
        "",
        3,
        1,
        &opts,
    )
    .unwrap();
    let from_archive = build_sparse_file_patch(&old_archive, &new_archive, 3, 1, &opts).unwrap();
    assert_eq!(
        from_dir, from_archive,
        "directory newer side must produce the identical patch to the archive-based build"
    );
    // The directory walk applies the executable override the same way the
    // packer does: without it the ops list would disagree on the mode.
    let no_override: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let (no_override_patch, _) = build_sparse_file_patch_with_tree_from_directory(
        None,
        &old_archive,
        "",
        dir_v2.path(),
        &no_override,
        "",
        3,
        1,
        &opts,
    )
    .unwrap();
    assert_ne!(
        no_override_patch, from_archive,
        "without the executable override the modes (and patch) must differ"
    );
}

#[test]
fn parallel_file_passes_produce_identical_payloads() {
    // Many changed files: the publisher runs their hash+diff pipelines on a
    // bounded worker pool. The ops list and payload bytes must not depend
    // on how many pipelines run concurrently (per-chunk diff results are
    // serialized by chunk index, hashes are order-independent).
    let dir_v1 = tempfile::tempdir().unwrap();
    let dir_v2 = tempfile::tempdir().unwrap();
    let mut seed: u64 = 0x5eed;
    let mut next = || -> u64 {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        seed
    };
    for i in 0..20usize {
        let base: Vec<u8> = (0..(1024 * 1024)).map(|j| (next() >> (j % 61)) as u8).collect();
        let mut changed = base.clone();
        let at = (i * 13) % changed.len();
        changed[at] = 0xA0;
        let name = format!("file_{i:02}.bin");
        std::fs::write(dir_v1.path().join(&name), &base).unwrap();
        std::fs::write(dir_v2.path().join(&name), &changed).unwrap();
    }
    // Plus an unchanged file and a new file to exercise the other paths.
    std::fs::write(dir_v1.path().join("same.bin"), b"steady\n").unwrap();
    std::fs::write(dir_v2.path().join("same.bin"), b"steady\n").unwrap();
    std::fs::write(dir_v2.path().join("added.bin"), b"brand new\n").unwrap();

    let pack = |d: &std::path::Path| -> Vec<u8> {
        let mut packer = ArchivePacker::new(3).unwrap();
        packer.add_directory(d, "").unwrap();
        packer.finalize().unwrap()
    };
    let old_archive = pack(dir_v1.path());
    let new_archive = pack(dir_v2.path());

    let opts = ChunkedDiffOptions {
        chunk_size: 256 * 1024,
        max_threads: 0,
        format: ChunkedPatchFormat::IdentityChunks,
    };
    // 8 workers => up to 8 parallel file pipelines; 1 worker => sequential.
    let patch_wide = build_sparse_file_patch(&old_archive, &new_archive, 3, 8, &opts).unwrap();
    let patch_narrow = build_sparse_file_patch(&old_archive, &new_archive, 3, 1, &opts).unwrap();

    let (manifest_wide, payloads_wide) = super::sparse_ops::decode_sparse_file_ops_payload(&patch_wide).unwrap();
    let (manifest_narrow, payloads_narrow) = super::sparse_ops::decode_sparse_file_ops_payload(&patch_narrow).unwrap();
    assert_eq!(
        format!("{:?}", manifest_wide.ops),
        format!("{:?}", manifest_narrow.ops),
        "ops must be parallelism-independent"
    );
    assert_eq!(
        payloads_wide, payloads_narrow,
        "payload bytes must be parallelism-independent"
    );
}
