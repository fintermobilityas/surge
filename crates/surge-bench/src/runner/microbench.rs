use std::fs;
use std::io::Cursor;
use std::path::Path;

use surge_core::archive::extractor;
use surge_core::archive::packer::ArchivePacker;
use surge_core::crypto::sha256;
use surge_core::diff::chunked::{self, ChunkedDiffOptions};
use surge_core::diff::wrapper;

use crate::report::BenchmarkResult;

use super::fs_compare::{dir_size, dir_size_recursive};
use super::time;

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
        let mut packer = ArchivePacker::new(3).expect("packer");
        let manifest = br#"{"version":"1.0.0","files":[]}"#;
        packer
            .add_buffer("manifest.json", manifest, 0o644)
            .expect("add manifest");
        packer.finalize().expect("finalize")
    });

    let output_size = archive_data.len() as u64;
    BenchmarkResult {
        name: "Synthetic installer bundle (online)".to_string(),
        duration,
        input_size,
        output_size,
    }
}

pub fn run_installer_offline(v1_dir: &Path) -> BenchmarkResult {
    let input_size = dir_size(v1_dir);

    let (archive_data, duration) = time(|| {
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
        name: "Synthetic installer bundle (offline)".to_string(),
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
