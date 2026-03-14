#![allow(clippy::cast_sign_loss, clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use surge_core::archive::extractor;
use surge_core::archive::packer::ArchivePacker;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::config::installer::{
    InstallerManifest, InstallerRelease, InstallerRuntime, InstallerStorage, InstallerUi,
};
use surge_core::context::{Context, StorageProvider};
use surge_core::crypto::sha256;
use surge_core::diff::chunked::{self, ChunkedDiffOptions};
use surge_core::diff::wrapper;
use surge_core::error::{Result, SurgeError};
use surge_core::install::{LEGACY_RUNTIME_MANIFEST_RELATIVE_PATH, RUNTIME_MANIFEST_RELATIVE_PATH};
use surge_core::installer_bundle;
use surge_core::pack::builder::{PackBuilder, TimedArtifact};
use surge_core::platform::detect::current_rid;
use surge_core::platform::fs::make_executable;
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

const BENCH_APP_ID: &str = "bench-app";
const BENCH_APP_NAME: &str = "Benchmark App";

#[derive(Clone)]
struct ReleasePublication {
    full_build: TimedArtifact,
    delta_build: Option<TimedArtifact>,
    full_upload: TimedArtifact,
    delta_upload: Option<TimedArtifact>,
    release_index_update: Duration,
    total_push: Duration,
}

fn average_duration(values: &[Duration]) -> Duration {
    if values.is_empty() {
        return Duration::ZERO;
    }

    let total_micros: u128 = values.iter().map(Duration::as_micros).sum();
    let average_micros = total_micros / u128::try_from(values.len()).unwrap_or(1);
    Duration::from_micros(u64::try_from(average_micros).unwrap_or(u64::MAX))
}

fn average_u64(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }

    let total: u128 = values.iter().copied().map(u128::from).sum();
    let average = total / u128::try_from(values.len()).unwrap_or(1);
    u64::try_from(average).unwrap_or(u64::MAX)
}

fn configure_benchmark_context(
    store_dir: &Path,
    pack_zstd_level: i32,
    pack_max_threads: Option<usize>,
    pack_memory_mb: u64,
) -> Result<Arc<Context>> {
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

    Ok(ctx)
}

async fn publish_release(
    ctx: Arc<Context>,
    manifest_path: &Path,
    app_id: &str,
    rid: &str,
    version: &str,
    artifacts_dir: &Path,
) -> Result<ReleasePublication> {
    let mut builder = PackBuilder::new(
        ctx,
        manifest_path.to_str().ok_or_else(|| {
            SurgeError::Config(format!("Manifest path is not valid UTF-8: {}", manifest_path.display()))
        })?,
        app_id,
        rid,
        version,
        artifacts_dir.to_str().ok_or_else(|| {
            SurgeError::Config(format!(
                "Artifacts path is not valid UTF-8: {}",
                artifacts_dir.display()
            ))
        })?,
    )?;
    let build = builder.build_with_breakdown(None).await?;
    let push = builder.push_with_breakdown("stable", None).await?;
    let full_upload = push
        .artifacts
        .iter()
        .find(|artifact| !artifact.is_delta)
        .cloned()
        .ok_or_else(|| SurgeError::Pack("Full artifact upload timing was not recorded".to_string()))?;
    let delta_upload = push.artifacts.iter().find(|artifact| artifact.is_delta).cloned();

    Ok(ReleasePublication {
        full_build: build.full,
        delta_build: build.delta,
        full_upload,
        delta_upload,
        release_index_update: push.release_index_update,
        total_push: push.total,
    })
}

fn surge_binary_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge.exe"
    } else {
        "surge"
    }
}

fn installer_launcher_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge-installer.exe"
    } else {
        "surge-installer"
    }
}

fn resolve_tool_binary(env_var: &str, binary_name: &str) -> Result<PathBuf> {
    if let Ok(path) = std::env::var(env_var) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        return Err(SurgeError::Pack(format!(
            "{env_var} points to '{}' which does not exist",
            candidate.display()
        )));
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let candidate = parent.join(binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    if let Some(path_env) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_env) {
            let candidate = dir.join(binary_name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }

    Err(SurgeError::Pack(format!(
        "Required benchmark helper '{binary_name}' was not found. Put it next to surge-bench, add it to PATH, or set {env_var}."
    )))
}

fn installer_manifest(
    store_dir: &Path,
    app_id: &str,
    rid: &str,
    version: &str,
    full_filename: &str,
    installer_type: &str,
    install_root: &Path,
) -> InstallerManifest {
    InstallerManifest {
        schema: 1,
        format: "surge-installer-v1".to_string(),
        ui: InstallerUi::Console,
        installer_type: installer_type.to_string(),
        app_id: app_id.to_string(),
        rid: rid.to_string(),
        version: version.to_string(),
        channel: "stable".to_string(),
        generated_utc: "1970-01-01T00:00:00Z".to_string(),
        headless_default_if_no_display: true,
        release_index_key: RELEASES_FILE_COMPRESSED.to_string(),
        storage: InstallerStorage {
            provider: "filesystem".to_string(),
            bucket: store_dir.to_string_lossy().to_string(),
            region: String::new(),
            endpoint: String::new(),
            prefix: String::new(),
        },
        release: InstallerRelease {
            full_filename: full_filename.to_string(),
            delta_filename: String::new(),
            delta_algorithm: String::new(),
            delta_patch_format: String::new(),
            delta_compression: String::new(),
        },
        runtime: InstallerRuntime {
            name: BENCH_APP_NAME.to_string(),
            main_exe: "app.main.dll".to_string(),
            install_directory: install_root.to_string_lossy().to_string(),
            supervisor_id: String::new(),
            icon: String::new(),
            shortcuts: Vec::new(),
            persistent_assets: Vec::new(),
            installers: vec![installer_type.to_string()],
            environment: BTreeMap::new(),
        },
    }
}

fn build_console_installer(
    output_dir: &Path,
    store_dir: &Path,
    app_id: &str,
    rid: &str,
    version: &str,
    full_package_path: &Path,
    installer_type: &str,
    install_root: &Path,
    surge_binary: &Path,
    launcher: &Path,
    compression_level: i32,
) -> Result<(PathBuf, BenchmarkResult)> {
    let full_filename = full_package_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| {
            SurgeError::Pack(format!(
                "Invalid full package path (missing filename): {}",
                full_package_path.display()
            ))
        })?;
    fs::create_dir_all(output_dir)?;

    let installer_ext = if rid.starts_with("win-") || rid.starts_with("windows-") {
        "exe"
    } else {
        "bin"
    };
    let installer_path = output_dir.join(format!("Setup-{rid}-{installer_type}.{installer_ext}"));
    let staged_surge_name = surge_binary_name_for_rid(rid);
    let input_size = fs::metadata(full_package_path).map(|meta| meta.len()).unwrap_or(0);

    let (build_result, duration) = time(|| -> Result<()> {
        let staging_dir = tempfile::tempdir()
            .map_err(|e| SurgeError::Pack(format!("Failed to create installer staging directory: {e}")))?;
        let staging = staging_dir.path();
        let manifest = installer_manifest(
            store_dir,
            app_id,
            rid,
            version,
            &full_filename,
            installer_type,
            install_root,
        );
        let manifest_yaml = serde_yaml::to_string(&manifest)
            .map_err(|e| SurgeError::Pack(format!("Failed to serialize installer manifest: {e}")))?;
        fs::write(staging.join("installer.yml"), manifest_yaml.as_bytes())?;

        let staged_surge = staging.join(staged_surge_name);
        fs::copy(surge_binary, &staged_surge).map_err(|e| {
            SurgeError::Pack(format!(
                "Failed to copy surge binary '{}' into installer staging: {e}",
                surge_binary.display()
            ))
        })?;
        make_executable(&staged_surge)?;

        if installer_type == "offline" {
            let payload_dir = staging.join("payload");
            fs::create_dir_all(&payload_dir)?;
            fs::copy(full_package_path, payload_dir.join(&full_filename)).map_err(|e| {
                SurgeError::Pack(format!(
                    "Failed to copy full package '{}' into offline installer staging: {e}",
                    full_package_path.display()
                ))
            })?;
        }

        let payload_archive = tempfile::NamedTempFile::new()
            .map_err(|e| SurgeError::Pack(format!("Failed to create installer payload archive temp file: {e}")))?;
        let mut payload_packer = ArchivePacker::new(compression_level)?;
        payload_packer.add_directory(staging, "")?;
        payload_packer.finalize_to_file(payload_archive.path())?;

        installer_bundle::write_embedded_installer(launcher, payload_archive.path(), &installer_path)?;
        make_executable(&installer_path)?;
        Ok(())
    });
    build_result?;
    let output_size = fs::metadata(&installer_path).map(|meta| meta.len()).unwrap_or(0);

    Ok((
        installer_path,
        BenchmarkResult {
            name: format!("Installer create ({installer_type})"),
            duration,
            input_size,
            output_size,
        },
    ))
}

fn run_console_installer(
    installer_path: &Path,
    install_root: &Path,
    expected_payload_dir: &Path,
    installer_type: &str,
    home_dir: &Path,
) -> Result<BenchmarkResult> {
    if install_root.exists() {
        fs::remove_dir_all(install_root)?;
    }
    fs::create_dir_all(home_dir)?;
    let input_size = fs::metadata(installer_path).map(|meta| meta.len()).unwrap_or(0);
    let (status, duration) = time(|| {
        Command::new(installer_path)
            .arg("--no-start")
            .env("HOME", home_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
    });
    let status = status.map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to execute installer '{}': {e}",
            installer_path.display()
        ))
    })?;
    if !status.success() {
        return Err(SurgeError::Pack(format!(
            "Installer '{}' exited with status {status}",
            installer_path.display()
        )));
    }

    let installed_app_dir = install_root.join("app");
    assert_directories_match(&installed_app_dir, expected_payload_dir)?;

    Ok(BenchmarkResult {
        name: format!("Installer run ({installer_type})"),
        duration,
        input_size,
        output_size: dir_size_recursive(&installed_app_dir),
    })
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
        name: "Synthetic installer bundle (online)".to_string(),
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
    let app_id = BENCH_APP_ID;
    let rid = current_rid();
    let store_dir = work_dir.join("update-store");
    let install_dir = work_dir.join("update-install");
    let artifacts_dir = work_dir.join("update-artifacts");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&install_dir)?;

    let manifest_path = work_dir.join("update-bench.surge.yml");
    write_bench_manifest(&manifest_path, &store_dir, app_id, &rid, pack_zstd_level)?;

    let ctx = configure_benchmark_context(&store_dir, pack_zstd_level, pack_max_threads, pack_memory_mb)?;

    let template = PayloadTemplate::new(scale, seed);
    let base_input_bytes = template.write_base(&artifacts_dir, seed)?;
    let mut total_input_bytes = base_input_bytes;
    let publish_started = Instant::now();
    let mut baseline_publication = None;
    let mut incremental_full_builds = Vec::new();
    let mut incremental_full_build_sizes = Vec::new();
    let mut incremental_full_build_inputs = Vec::new();
    let mut delta_builds = Vec::new();
    let mut delta_build_sizes = Vec::new();
    let mut delta_build_inputs = Vec::new();
    let mut baseline_full_upload = None;
    let mut incremental_full_uploads = Vec::new();
    let mut incremental_full_upload_sizes = Vec::new();
    let mut delta_uploads = Vec::new();
    let mut delta_upload_sizes = Vec::new();
    let mut release_index_updates = Vec::new();
    let mut release_index_sizes = Vec::new();
    let mut incremental_release_publish = Vec::new();

    for version_index in 1..=version_count {
        if version_index > 1 {
            total_input_bytes = total_input_bytes.saturating_add(template.mutate_version(
                &artifacts_dir,
                seed,
                version_index,
                scenario,
            )?);
        }
        let current_payload_bytes = dir_size_recursive(&artifacts_dir);

        let version = version_label(version_index);
        let publication =
            publish_release(Arc::clone(&ctx), &manifest_path, app_id, &rid, &version, &artifacts_dir).await?;
        let release_index_size = fs::metadata(store_dir.join(RELEASES_FILE_COMPRESSED))
            .map(|meta| meta.len())
            .unwrap_or(0);
        release_index_updates.push(publication.release_index_update);
        release_index_sizes.push(release_index_size);

        if version_index == 1 {
            baseline_full_upload = Some(publication.full_upload.clone());
            baseline_publication = Some(publication);
        } else {
            incremental_full_builds.push(publication.full_build.duration);
            incremental_full_build_sizes.push(publication.full_build.size_bytes);
            incremental_full_build_inputs.push(current_payload_bytes);
            incremental_full_uploads.push(publication.full_upload.duration);
            incremental_full_upload_sizes.push(publication.full_upload.size_bytes);

            if let Some(delta_build) = publication.delta_build {
                delta_builds.push(delta_build.duration);
                delta_build_sizes.push(delta_build.size_bytes);
                delta_build_inputs.push(current_payload_bytes);
            }
            if let Some(delta_upload) = publication.delta_upload {
                delta_uploads.push(delta_upload.duration);
                delta_upload_sizes.push(delta_upload.size_bytes);
            }
            incremental_release_publish.push(publication.total_push);
        }
    }

    let baseline_publication = baseline_publication
        .ok_or_else(|| SurgeError::Update("Expected at least one published release".to_string()))?;
    let baseline_full_upload = baseline_full_upload
        .ok_or_else(|| SurgeError::Update("Expected baseline full artifact upload timing".to_string()))?;

    results.push(BenchmarkResult {
        name: "Full pack build (baseline)".to_string(),
        duration: baseline_publication.full_build.duration,
        input_size: base_input_bytes,
        output_size: baseline_publication.full_build.size_bytes,
    });
    results.push(BenchmarkResult {
        name: "Full artifact upload (baseline)".to_string(),
        duration: baseline_full_upload.duration,
        input_size: baseline_full_upload.size_bytes,
        output_size: baseline_full_upload.size_bytes,
    });
    if !incremental_full_builds.is_empty() {
        results.push(BenchmarkResult {
            name: "Full pack build (incremental avg)".to_string(),
            duration: average_duration(&incremental_full_builds),
            input_size: average_u64(&incremental_full_build_inputs),
            output_size: average_u64(&incremental_full_build_sizes),
        });
    }
    if !delta_builds.is_empty() {
        results.push(BenchmarkResult {
            name: "Delta pack build (avg)".to_string(),
            duration: average_duration(&delta_builds),
            input_size: average_u64(&delta_build_inputs),
            output_size: average_u64(&delta_build_sizes),
        });
    }
    if !incremental_full_uploads.is_empty() {
        results.push(BenchmarkResult {
            name: "Full artifact upload (incremental avg)".to_string(),
            duration: average_duration(&incremental_full_uploads),
            input_size: average_u64(&incremental_full_upload_sizes),
            output_size: average_u64(&incremental_full_upload_sizes),
        });
    }
    if !delta_uploads.is_empty() {
        results.push(BenchmarkResult {
            name: "Delta artifact upload (avg)".to_string(),
            duration: average_duration(&delta_uploads),
            input_size: average_u64(&delta_upload_sizes),
            output_size: average_u64(&delta_upload_sizes),
        });
    }
    if !release_index_updates.is_empty() {
        results.push(BenchmarkResult {
            name: "Release index update (avg)".to_string(),
            duration: average_duration(&release_index_updates),
            input_size: average_u64(&release_index_sizes),
            output_size: average_u64(&release_index_sizes),
        });
    }
    if !incremental_release_publish.is_empty() {
        results.push(BenchmarkResult {
            name: "Release publish (incremental avg)".to_string(),
            duration: average_duration(&incremental_release_publish),
            input_size: average_u64(&incremental_full_build_inputs),
            output_size: average_u64(&release_index_sizes),
        });
    }

    results.push(BenchmarkResult {
        name: format!("Publish {version_count} releases"),
        duration: publish_started.elapsed(),
        input_size: total_input_bytes,
        output_size: dir_size_recursive(&store_dir),
    });

    let baseline_version = version_label(1);
    let baseline_full = store_dir.join(&baseline_publication.full_build.filename);
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

pub async fn run_installer_scenario(
    work_dir: &Path,
    scale: f64,
    seed: u64,
    pack_zstd_level: i32,
    pack_max_threads: Option<usize>,
    pack_memory_mb: u64,
) -> Result<Vec<BenchmarkResult>> {
    let app_id = BENCH_APP_ID;
    let rid = current_rid();
    let version = version_label(1);
    let store_dir = work_dir.join("installer-store");
    let artifacts_dir = work_dir.join("installer-artifacts");
    let installers_dir = work_dir.join("installer-bundles");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&artifacts_dir)?;
    fs::create_dir_all(&installers_dir)?;

    let manifest_path = work_dir.join("installer-bench.surge.yml");
    write_bench_manifest(&manifest_path, &store_dir, app_id, &rid, pack_zstd_level)?;
    let ctx = configure_benchmark_context(&store_dir, pack_zstd_level, pack_max_threads, pack_memory_mb)?;

    let template = PayloadTemplate::new(scale, seed);
    template.write_base(&artifacts_dir, seed)?;
    let publication = publish_release(Arc::clone(&ctx), &manifest_path, app_id, &rid, &version, &artifacts_dir).await?;
    let full_package_path = store_dir.join(&publication.full_build.filename);

    let surge_binary = resolve_tool_binary("SURGE_INSTALLER_BINARY", surge_binary_name_for_rid(&rid))?;
    let installer_launcher = resolve_tool_binary("SURGE_INSTALLER_LAUNCHER", installer_launcher_name_for_rid(&rid))?;

    let online_install_root = work_dir.join("online-install-root");
    let offline_install_root = work_dir.join("offline-install-root");
    let online_home = work_dir.join("online-installer-home");
    let offline_home = work_dir.join("offline-installer-home");

    let (online_installer, online_create) = build_console_installer(
        &installers_dir,
        &store_dir,
        app_id,
        &rid,
        &version,
        &full_package_path,
        "online",
        &online_install_root,
        &surge_binary,
        &installer_launcher,
        pack_zstd_level,
    )?;
    let online_run = run_console_installer(
        &online_installer,
        &online_install_root,
        &artifacts_dir,
        "online",
        &online_home,
    )?;

    let (offline_installer, offline_create) = build_console_installer(
        &installers_dir,
        &store_dir,
        app_id,
        &rid,
        &version,
        &full_package_path,
        "offline",
        &offline_install_root,
        &surge_binary,
        &installer_launcher,
        pack_zstd_level,
    )?;
    let offline_run = run_console_installer(
        &offline_installer,
        &offline_install_root,
        &artifacts_dir,
        "offline",
        &offline_home,
    )?;

    Ok(vec![online_create, online_run, offline_create, offline_run])
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
