use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::time::Instant;

use surge_core::archive::extractor;
use surge_core::archive::packer::ArchivePacker;
use surge_core::crypto::sha256;
use surge_core::diff::chunked::{self, ChunkedDiffOptions};
use surge_core::diff::wrapper;

use crate::report::BenchmarkResult;

fn time<F, T>(f: F) -> (T, std::time::Duration)
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

pub fn run_archive_create(v1_dir: &Path, levels: &[i32]) -> Vec<(BenchmarkResult, Vec<u8>)> {
    let input_size = dir_size(v1_dir);
    let mut results = Vec::new();

    for &level in levels {
        let dir = v1_dir.to_path_buf();
        let (archive_data, duration) = time(|| {
            let mut packer = ArchivePacker::new(level).expect("Failed to create packer");
            packer.add_directory(&dir, "").expect("Failed to add directory");
            packer.finalize().expect("Failed to finalize archive")
        });

        let output_size = archive_data.len() as u64;
        results.push((
            BenchmarkResult {
                name: format!("Archive create (zstd={level})"),
                duration,
                input_size,
                output_size,
            },
            archive_data,
        ));
    }

    results
}

pub fn run_archive_extract(archive_data: &[u8], work_dir: &Path) -> BenchmarkResult {
    let extract_dir = work_dir.join("extract_test");
    let input_size = archive_data.len() as u64;

    let ((), duration) = time(|| {
        extractor::extract_to(archive_data, &extract_dir, None).expect("Failed to extract archive");
    });

    let output_size = dir_size_recursive(&extract_dir);
    let _ = fs::remove_dir_all(&extract_dir);

    BenchmarkResult {
        name: "Archive extract".to_string(),
        duration,
        input_size,
        output_size,
    }
}

pub fn run_sha256_memory(archive_data: &[u8]) -> BenchmarkResult {
    let input_size = archive_data.len() as u64;

    let (_hash, duration) = time(|| sha256::sha256_hex(archive_data));

    BenchmarkResult {
        name: "SHA-256 (in-memory)".to_string(),
        duration,
        input_size,
        output_size: 32,
    }
}

pub fn run_sha256_file(file_path: &Path) -> BenchmarkResult {
    let input_size = fs::metadata(file_path).map(|m| m.len()).unwrap_or(0);

    let (_hash, duration) = time(|| {
        sha256::sha256_hex_file(file_path).expect("Failed to hash file");
    });

    BenchmarkResult {
        name: "SHA-256 (file)".to_string(),
        duration,
        input_size,
        output_size: 32,
    }
}

pub fn run_zstd_compress(data: &[u8], levels: &[i32]) -> Vec<(BenchmarkResult, Vec<u8>)> {
    let input_size = data.len() as u64;
    let mut results = Vec::new();

    for &level in levels {
        let (compressed, duration) = time(|| zstd::encode_all(Cursor::new(data), level).expect("Failed to compress"));

        let output_size = compressed.len() as u64;
        results.push((
            BenchmarkResult {
                name: format!("Zstd compress (level={level})"),
                duration,
                input_size,
                output_size,
            },
            compressed,
        ));
    }

    results
}

pub fn run_zstd_decompress(compressed: &[u8], original_size: u64) -> BenchmarkResult {
    let input_size = compressed.len() as u64;

    let (_decompressed, duration) = time(|| zstd::decode_all(Cursor::new(compressed)).expect("Failed to decompress"));

    BenchmarkResult {
        name: "Zstd decompress".to_string(),
        duration,
        input_size,
        output_size: original_size,
    }
}

pub fn run_bsdiff(v1_data: &[u8], v2_data: &[u8]) -> (BenchmarkResult, Vec<u8>) {
    let input_size = (v1_data.len() + v2_data.len()) as u64;

    let (patch, duration) = time(|| wrapper::bsdiff_buffers(v1_data, v2_data).expect("bsdiff failed"));

    let output_size = patch.len() as u64;
    (
        BenchmarkResult {
            name: "bsdiff".to_string(),
            duration,
            input_size,
            output_size,
        },
        patch,
    )
}

pub fn run_bspatch(v1_data: &[u8], patch: &[u8], expected_size: u64) -> BenchmarkResult {
    let input_size = (v1_data.len() + patch.len()) as u64;

    let (_reconstructed, duration) = time(|| wrapper::bspatch_buffers(v1_data, patch).expect("bspatch failed"));

    BenchmarkResult {
        name: "bspatch".to_string(),
        duration,
        input_size,
        output_size: expected_size,
    }
}

pub fn run_installer_online(v1_dir: &Path) -> BenchmarkResult {
    let input_size = dir_size(v1_dir);

    let (archive_data, duration) = time(|| {
        // Online installer: manifest buffer only (small metadata archive)
        let mut packer = ArchivePacker::new(3).expect("packer");
        // Add a small manifest buffer simulating installer metadata
        let manifest = br#"{"version":"1.0.0","files":[]}"#;
        packer
            .add_buffer("manifest.json", manifest, 0o644)
            .expect("add manifest");
        packer.finalize().expect("finalize")
    });

    let output_size = archive_data.len() as u64;
    BenchmarkResult {
        name: "Installer (online)".to_string(),
        duration,
        input_size,
        output_size,
    }
}

pub fn run_installer_offline(v1_dir: &Path) -> BenchmarkResult {
    let input_size = dir_size(v1_dir);

    let (archive_data, duration) = time(|| {
        // Offline installer: manifest + full archive payload
        let mut inner_packer = ArchivePacker::new(3).expect("inner packer");
        inner_packer.add_directory(v1_dir, "").expect("add dir");
        let payload = inner_packer.finalize().expect("finalize inner");

        let mut packer = ArchivePacker::new(1).expect("outer packer");
        let manifest = br#"{"version":"1.0.0","files":[]}"#;
        packer
            .add_buffer("manifest.json", manifest, 0o644)
            .expect("add manifest");
        packer
            .add_buffer("payload.tar.zst", &payload, 0o644)
            .expect("add payload");
        packer.finalize().expect("finalize outer")
    });

    let output_size = archive_data.len() as u64;
    BenchmarkResult {
        name: "Installer (offline)".to_string(),
        duration,
        input_size,
        output_size,
    }
}

pub fn run_chunked_bsdiff(v1_data: &[u8], v2_data: &[u8]) -> (BenchmarkResult, Vec<u8>) {
    let input_size = (v1_data.len() + v2_data.len()) as u64;
    let opts = ChunkedDiffOptions::default();

    let (patch, duration) = time(|| chunked::chunked_bsdiff(v1_data, v2_data, &opts).expect("chunked bsdiff failed"));

    let output_size = patch.len() as u64;
    (
        BenchmarkResult {
            name: "chunked bsdiff".to_string(),
            duration,
            input_size,
            output_size,
        },
        patch,
    )
}

pub fn run_chunked_bspatch(v1_data: &[u8], patch: &[u8], expected_size: u64) -> BenchmarkResult {
    let input_size = (v1_data.len() + patch.len()) as u64;
    let opts = ChunkedDiffOptions::default();

    let (_reconstructed, duration) =
        time(|| chunked::chunked_bspatch(v1_data, patch, &opts).expect("chunked bspatch failed"));

    BenchmarkResult {
        name: "chunked bspatch".to_string(),
        duration,
        input_size,
        output_size: expected_size,
    }
}

/// Simulates a real-world update scenario:
/// - Build a full package (archive of v1)
/// - Build a delta package (chunked diff between v1 and v2 archives)
/// - Apply N sequential deltas to reconstruct the final version
///
/// Returns results for: full package build, delta package build, and applying N deltas.
pub fn run_update_scenario(v1_dir: &Path, v2_dir: &Path, num_deltas: usize) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();

    // 1. Build full package (archive v1)
    let (full_pkg, full_duration) = time(|| {
        let mut packer = ArchivePacker::new(3).expect("packer");
        packer.add_directory(v1_dir, "").expect("add dir");
        packer.finalize().expect("finalize")
    });
    results.push(BenchmarkResult {
        name: "Full package build".to_string(),
        duration: full_duration,
        input_size: dir_size(v1_dir),
        output_size: full_pkg.len() as u64,
    });

    // 2. Build delta package (chunked diff between archives)
    let archive_v2 = {
        let mut packer = ArchivePacker::new(3).expect("packer v2");
        packer.add_directory(v2_dir, "").expect("add v2 dir");
        packer.finalize().expect("finalize v2")
    };

    let opts = ChunkedDiffOptions::default();
    let (delta_pkg, delta_duration) =
        time(|| chunked::chunked_bsdiff(&full_pkg, &archive_v2, &opts).expect("chunked bsdiff"));
    results.push(BenchmarkResult {
        name: "Delta package build".to_string(),
        duration: delta_duration,
        input_size: (full_pkg.len() + archive_v2.len()) as u64,
        output_size: delta_pkg.len() as u64,
    });

    // 3. Apply single delta
    let (reconstructed, single_apply_duration) =
        time(|| chunked::chunked_bspatch(&full_pkg, &delta_pkg, &opts).expect("chunked bspatch"));
    assert_eq!(
        sha256::sha256_hex(&reconstructed),
        sha256::sha256_hex(&archive_v2),
        "single delta verification failed"
    );
    results.push(BenchmarkResult {
        name: "Apply 1 delta".to_string(),
        duration: single_apply_duration,
        input_size: (full_pkg.len() + delta_pkg.len()) as u64,
        output_size: reconstructed.len() as u64,
    });

    // 4. Apply N deltas independently (simulates catching up N versions behind
    // by applying each delta from the same base — measures total patching throughput)
    if num_deltas > 1 {
        let ((), chain_duration) = time(|| {
            for _ in 0..num_deltas {
                let _ = chunked::chunked_bspatch(&full_pkg, &delta_pkg, &opts).expect("chain bspatch");
            }
        });
        results.push(BenchmarkResult {
            name: format!("Apply {num_deltas}x deltas"),
            duration: chain_duration,
            input_size: (full_pkg.len() as u64 + delta_pkg.len() as u64) * num_deltas as u64,
            output_size: reconstructed.len() as u64 * num_deltas as u64,
        });
    }

    results
}

fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata()
                && meta.is_file()
            {
                total += meta.len();
            }
        }
    }
    total
}

fn dir_size_recursive(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                } else if meta.is_dir() {
                    total += dir_size_recursive(&entry.path());
                }
            }
        }
    }
    total
}
