#![allow(clippy::cast_sign_loss, clippy::too_many_lines)]

use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use surge_core::archive::extractor;
use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::context::{Context, StorageProvider};
use surge_core::crypto::sha256;
use surge_core::diff::chunked::{self, ChunkedDiffOptions};
use surge_core::diff::wrapper;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH, RUNTIME_MANIFEST_RELATIVE_PATH};
use surge_core::pack::builder::PackBuilder;
use surge_core::platform::detect::current_rid;
use surge_core::update::manager::{ApplyStrategy, ProgressInfo, UpdateManager};

use crate::payload::{PayloadTemplate, ScenarioProfile};
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

/// Publish a real release chain and benchmark the native update manager path.
pub async fn run_update_scenario(
    work_dir: &Path,
    scale: f64,
    seed: u64,
    scenario: ScenarioProfile,
    num_deltas: usize,
    pack_zstd_level: i32,
    pack_max_threads: Option<usize>,
    pack_memory_mb: u64,
) -> Result<Vec<BenchmarkResult>> {
    let mut results = Vec::new();
    let version_count = num_deltas.max(1) + 1;
    let app_id = "bench-app";
    let rid = current_rid();
    let store_dir = work_dir.join("update-store");
    let install_dir = work_dir.join("update-install");
    let artifacts_dir = work_dir.join("update-artifacts");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&install_dir)?;

    let manifest_path = work_dir.join("update-bench.surge.yml");
    write_bench_manifest(&manifest_path, &store_dir, app_id, &rid, pack_zstd_level)?;

    let ctx = Arc::new(Context::new());
    ctx.set_storage(
        StorageProvider::Filesystem,
        store_dir
            .to_str()
            .ok_or_else(|| SurgeError::Config(format!("Storage path is not valid UTF-8: {}", store_dir.display())))?,
        "",
        "",
        "",
        "",
    );
    let mut budget = ctx.resource_budget();
    let available_threads = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);
    let requested_threads = pack_max_threads.unwrap_or(available_threads).max(1);
    budget.max_threads = i32::try_from(requested_threads).unwrap_or(i32::MAX);
    budget.max_memory_bytes = i64::try_from(pack_memory_mb.saturating_mul(1024 * 1024)).unwrap_or(i64::MAX);
    budget.zstd_compression_level = pack_zstd_level;
    ctx.set_resource_budget(budget);

    let template = PayloadTemplate::new(scale, seed);
    let mut total_input_bytes = template.write_base(&artifacts_dir, seed)?;
    let publish_started = Instant::now();

    for version_index in 1..=version_count {
        if version_index > 1 {
            total_input_bytes = total_input_bytes.saturating_add(template.mutate_version(
                &artifacts_dir,
                seed,
                version_index,
                scenario,
            )?);
        }

        let version = version_label(version_index);
        let mut builder = PackBuilder::new(
            Arc::clone(&ctx),
            manifest_path.to_str().ok_or_else(|| {
                SurgeError::Config(format!("Manifest path is not valid UTF-8: {}", manifest_path.display()))
            })?,
            app_id,
            &rid,
            &version,
            artifacts_dir.to_str().ok_or_else(|| {
                SurgeError::Config(format!(
                    "Artifacts path is not valid UTF-8: {}",
                    artifacts_dir.display()
                ))
            })?,
        )?;
        builder.build(None).await?;
        builder.push("stable", None).await?;
    }

    results.push(BenchmarkResult {
        name: format!("Publish {version_count} releases"),
        duration: publish_started.elapsed(),
        input_size: total_input_bytes,
        output_size: dir_size_recursive(&store_dir),
    });

    let baseline_version = version_label(1);
    let baseline_full = store_dir.join(format!("{app_id}-{baseline_version}-{rid}-full.tar.zst"));
    let baseline_bytes = fs::read(&baseline_full)?;
    let baseline_app_dir = install_dir.join("app");
    extractor::extract_to(&baseline_bytes, &baseline_app_dir, None)?;

    let mut update_manager = UpdateManager::new(
        Arc::clone(&ctx),
        app_id,
        &baseline_version,
        "stable",
        install_dir
            .to_str()
            .ok_or_else(|| SurgeError::Config(format!("Install path is not valid UTF-8: {}", install_dir.display())))?,
    )?;

    let releases_index_path = store_dir.join(RELEASES_FILE_COMPRESSED);
    let releases_index_size = fs::metadata(&releases_index_path).map(|meta| meta.len()).unwrap_or(0);
    let check_started = Instant::now();
    let info = update_manager
        .check_for_updates()
        .await?
        .ok_or_else(|| SurgeError::Update("Expected update chain to be available".to_string()))?;
    let check_duration = check_started.elapsed();
    if version_count > 1 && !matches!(info.apply_strategy, ApplyStrategy::Delta) {
        return Err(SurgeError::Update(format!(
            "Expected delta update strategy for {version_count} published versions, got {:?}",
            info.apply_strategy
        )));
    }
    results.push(BenchmarkResult {
        name: format!("Update check ({num_deltas} deltas)"),
        duration: check_duration,
        input_size: releases_index_size,
        output_size: info.download_size.max(0) as u64,
    });

    let apply_started = Instant::now();
    update_manager
        .download_and_apply(&info, None::<fn(ProgressInfo)>)
        .await?;
    let apply_duration = apply_started.elapsed();
    assert_directories_match(&install_dir.join("app"), &artifacts_dir)?;
    results.push(BenchmarkResult {
        name: format!("Update apply ({num_deltas} deltas)"),
        duration: apply_duration,
        input_size: info.download_size.max(0) as u64,
        output_size: dir_size_recursive(&install_dir.join("app")),
    });

    Ok(results)
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

fn write_bench_manifest(path: &Path, store_dir: &Path, app_id: &str, rid: &str, pack_zstd_level: i32) -> Result<()> {
    let manifest = format!(
        r"schema: 1
storage:
  provider: filesystem
  bucket: {bucket}
pack:
  delta:
    strategy: archive-chunked-bsdiff
  compression:
    format: zstd
    level: {pack_zstd_level}
apps:
  - id: {app_id}
    name: Benchmark App
    main: app.main.dll
    channels:
      - stable
    target:
      rid: {rid}
",
        bucket = store_dir.display()
    );
    fs::write(path, manifest)?;
    Ok(())
}

fn version_label(index: usize) -> String {
    format!("1.0.{}", index.saturating_sub(1))
}

fn assert_directories_match(actual: &Path, expected: &Path) -> Result<()> {
    let mut actual_files = collect_relative_files(actual, actual)?;
    let mut expected_files = collect_relative_files(expected, expected)?;
    actual_files.sort();
    expected_files.sort();

    if actual_files != expected_files {
        return Err(SurgeError::Update(
            "Installed files do not match the expected payload".to_string(),
        ));
    }

    for relative in actual_files {
        let actual_hash = sha256::sha256_hex_file(&actual.join(&relative))?;
        let expected_hash = sha256::sha256_hex_file(&expected.join(&relative))?;
        if actual_hash != expected_hash {
            return Err(SurgeError::Update(format!(
                "Installed file differs from expected payload: {}",
                relative.display()
            )));
        }
    }

    Ok(())
}

fn collect_relative_files(root: &Path, current: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            files.extend(collect_relative_files(root, &path)?);
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|e| SurgeError::Update(format!("Failed to collect file list: {e}")))?;
            if relative != Path::new(RUNTIME_MANIFEST_RELATIVE_PATH)
                && relative != Path::new(LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH)
            {
                files.push(relative.to_path_buf());
            }
        }
    }
    Ok(files)
}
