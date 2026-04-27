use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use surge_core::archive::extractor;
use surge_core::config::constants::RELEASES_FILE_COMPRESSED;
use surge_core::context::{Context, StorageProvider};
use surge_core::error::{Result, SurgeError};
use surge_core::pack::builder::{PackBuilder, TimedArtifact};
use surge_core::platform::detect::current_rid;
use surge_core::update::manager::{ApplyStrategy, ProgressInfo, UpdateManager};

use crate::payload::{PayloadTemplate, ScenarioProfile};
use crate::report::BenchmarkResult;

use super::BENCH_APP_ID;
use super::fs_compare::{assert_directories_match, dir_size_recursive};
use super::manifest::{version_label, write_bench_manifest};

#[derive(Clone)]
pub(super) struct ReleasePublication {
    pub(super) full_build: TimedArtifact,
    pub(super) delta_build: Option<TimedArtifact>,
    pub(super) full_upload: TimedArtifact,
    pub(super) delta_upload: Option<TimedArtifact>,
    pub(super) release_index_update: Duration,
    pub(super) total_push: Duration,
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

pub(super) fn configure_benchmark_context(
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
    let available_threads = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let requested_threads = pack_max_threads.unwrap_or(available_threads).max(1);
    budget.max_threads = i32::try_from(requested_threads).unwrap_or(i32::MAX);
    budget.max_memory_bytes = i64::try_from(pack_memory_mb.saturating_mul(1024 * 1024)).unwrap_or(i64::MAX);
    budget.zstd_compression_level = pack_zstd_level;
    ctx.set_resource_budget(budget);

    Ok(ctx)
}

pub(super) async fn publish_release(
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
        let release_index_size = fs::metadata(store_dir.join(RELEASES_FILE_COMPRESSED)).map_or(0, |meta| meta.len());
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
    let releases_index_size = fs::metadata(&releases_index_path).map_or(0, |meta| meta.len());
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
