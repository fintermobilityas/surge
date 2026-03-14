#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::struct_excessive_bools, clippy::too_many_lines)]

mod payload;
mod report;
mod runner;

use std::fs;
use std::time::Instant;

use clap::{Parser, ValueEnum};

use crate::report::{BenchmarkReport, format_size};

/// Print to stdout only when not in JSON mode.
macro_rules! log {
    ($json:expr, $($arg:tt)*) => {
        if !$json {
            println!($($arg)*);
        }
    };
}

#[derive(Parser)]
#[command(name = "surge-bench", about = "Benchmark tool for the Surge update framework")]
struct Args {
    /// Payload size multiplier (0.01–1.0)
    #[arg(long, default_value = "0.1")]
    scale: f64,

    /// Mutation profile used for the generated follow-up release(s)
    #[arg(long, value_enum, default_value_t = ScenarioArg::FullRelease)]
    scenario: ScenarioArg,

    /// Comma-separated zstd compression levels to benchmark
    #[arg(long, default_value = "1,3,9,19", value_delimiter = ',')]
    zstd_levels: Vec<i32>,

    /// Skip bsdiff/bspatch benchmarks (slow at high scale)
    #[arg(long)]
    skip_diff: bool,

    /// Skip classic bsdiff/bspatch while still running chunked diff benchmarks
    #[arg(long)]
    skip_classic_diff: bool,

    /// Skip installer benchmarks
    #[arg(long)]
    skip_installers: bool,

    /// Skip the real publish/update scenario while still running the microbench sections
    #[arg(long)]
    skip_update_scenario: bool,

    /// Run only the real publish/update scenario and skip the microbench sections
    #[arg(long)]
    update_only: bool,

    /// Run only the real installer scenario (build and execute console installers)
    #[arg(long)]
    installers_only: bool,

    /// Number of sequential deltas to apply in the update scenario
    #[arg(long, default_value = "10")]
    num_deltas: usize,

    /// Pack zstd level used for the real publish/update scenario
    #[arg(long, default_value_t = surge_core::config::constants::PACK_DEFAULT_ZSTD_LEVEL)]
    pack_zstd_level: i32,

    /// Maximum pack threads used for the real publish/update scenario (defaults to all visible cores)
    #[arg(long)]
    pack_max_threads: Option<usize>,

    /// Pack memory budget in MiB used for the real publish/update scenario
    #[arg(long, default_value = "256")]
    pack_memory_mb: u64,

    /// PRNG seed for reproducible payloads
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Emit JSON output instead of human-readable text
    #[arg(long)]
    json: bool,

    /// Use github-action-benchmark compatible format (implies --json)
    #[arg(long)]
    benchmark_format: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ScenarioArg {
    FullRelease,
    SdkOnly,
}

impl ScenarioArg {
    fn into_profile(self) -> payload::ScenarioProfile {
        match self {
            Self::FullRelease => payload::ScenarioProfile::FullRelease,
            Self::SdkOnly => payload::ScenarioProfile::SdkOnly,
        }
    }
}

fn main() {
    let args = Args::parse();
    let json_mode = args.json || args.benchmark_format;
    let scenario = args.scenario.into_profile();

    // Validate scale
    if args.scale < 0.01 || args.scale > 1.0 {
        eprintln!("Error: --scale must be between 0.01 and 1.0");
        std::process::exit(1);
    }
    if args.update_only && args.skip_diff {
        eprintln!("Error: --update-only requires diff-enabled runs (do not pass --skip-diff)");
        std::process::exit(1);
    }
    if args.update_only && args.installers_only {
        eprintln!("Error: --update-only cannot be combined with --installers-only");
        std::process::exit(1);
    }
    if args.update_only && args.skip_update_scenario {
        eprintln!("Error: --update-only cannot be combined with --skip-update-scenario");
        std::process::exit(1);
    }
    if args.installers_only && args.skip_installers {
        eprintln!("Error: --installers-only cannot be combined with --skip-installers");
        std::process::exit(1);
    }

    log!(json_mode, "Surge Benchmark");
    log!(json_mode, "===============");
    log!(
        json_mode,
        "Scale: {:.2} | Seed: {} | Scenario: {} | Zstd levels: {:?}",
        args.scale,
        args.seed,
        scenario.as_str(),
        args.zstd_levels
    );
    if args.skip_diff {
        log!(json_mode, "  (bsdiff/bspatch: SKIPPED)");
    } else if args.skip_classic_diff {
        log!(json_mode, "  (classic bsdiff/bspatch: SKIPPED)");
    }
    if args.skip_installers {
        log!(json_mode, "  (installers: SKIPPED)");
    } else if args.installers_only {
        log!(json_mode, "  (installers: real installer scenario only)");
    }

    if !args.skip_diff && !args.skip_classic_diff && args.scale > 0.3 {
        log!(json_mode, "");
        log!(json_mode, "WARNING: bsdiff at scale > 0.3 may require ~15 GB of RAM.");
        log!(json_mode, "         Use --skip-diff to skip if memory is limited.");
    }

    // Create temp directory
    let tmp = tempfile::tempdir().expect("Failed to create temp directory");
    let work_dir = tmp.path();

    // Generate payloads
    log!(json_mode, "");
    log!(json_mode, "Generating payloads...");
    let gen_start = Instant::now();
    let generated = payload::generate(work_dir, args.scale, args.seed, scenario).expect("Failed to generate payloads");
    let gen_duration = gen_start.elapsed();
    log!(
        json_mode,
        "  Generated {} files in {:.2}s",
        generated.total_files,
        gen_duration.as_secs_f64()
    );
    log!(
        json_mode,
        "  v1: {} | v2: {}",
        format_size(generated.total_size_v1),
        format_size(generated.total_size_v2)
    );

    // Collect results
    let mut results = Vec::new();

    log!(json_mode, "");
    log!(json_mode, "Running benchmarks...");

    if !args.update_only && !args.installers_only {
        // 1. Archive create (per zstd level)
        let archive_results = runner::run_archive_create(&generated.v1_dir, &args.zstd_levels);

        // Save the first archive for subsequent benchmarks
        let first_archive: Vec<u8> = if let Some((_, data)) = archive_results.first() {
            data.clone()
        } else {
            Vec::new()
        };

        for (result, _) in &archive_results {
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(result.duration),
                result.name
            );
        }
        results.extend(archive_results.into_iter().map(|(r, _)| r));

        // 2. Archive extract (using first archive)
        if !first_archive.is_empty() {
            let result = runner::run_archive_extract(&first_archive, work_dir);
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(result.duration),
                result.name
            );
            results.push(result);
        }

        // 3. SHA-256 in-memory (on first archive)
        if !first_archive.is_empty() {
            let result = runner::run_sha256_memory(&first_archive);
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(result.duration),
                result.name
            );
            results.push(result);
        }

        // 4. SHA-256 file
        // Write archive to a temp file for file-based hashing
        let archive_file = work_dir.join("test_archive.tar.zst");
        if !first_archive.is_empty() {
            fs::write(&archive_file, &first_archive).expect("Failed to write archive file");
            let result = runner::run_sha256_file(&archive_file);
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(result.duration),
                result.name
            );
            results.push(result);
            let _ = fs::remove_file(&archive_file);
        }

        // 5-6. Zstd compress/decompress (on raw payload data — read nativesdk.so or first large file)
        let raw_data = find_largest_file(&generated.v1_dir);
        if !raw_data.is_empty() {
            let zstd_results = runner::run_zstd_compress(&raw_data, &args.zstd_levels);

            // Save first compressed result for decompression benchmark
            let first_compressed = if let Some((_, data)) = zstd_results.first() {
                data.clone()
            } else {
                Vec::new()
            };

            for (result, _) in &zstd_results {
                log!(
                    json_mode,
                    "  {} {}",
                    report::format_duration(result.duration),
                    result.name
                );
            }
            results.extend(zstd_results.into_iter().map(|(r, _)| r));

            if !first_compressed.is_empty() {
                let result = runner::run_zstd_decompress(&first_compressed, raw_data.len() as u64);
                log!(
                    json_mode,
                    "  {} {}",
                    report::format_duration(result.duration),
                    result.name
                );
                results.push(result);
            }
        }

        // 7-8. bsdiff/bspatch (classic + chunked)
        if !args.skip_diff {
            // Use the archives for diffing (more realistic than raw files)
            let mut packer_v2 = surge_core::archive::packer::ArchivePacker::new(3).expect("packer v2");
            packer_v2.add_directory(&generated.v2_dir, "").expect("add v2 dir");
            let archive_v2 = packer_v2.finalize().expect("finalize v2");

            if !args.skip_classic_diff {
                let (diff_result, patch) = runner::run_bsdiff(&first_archive, &archive_v2);
                log!(
                    json_mode,
                    "  {} {}",
                    report::format_duration(diff_result.duration),
                    diff_result.name
                );
                results.push(diff_result);

                let patch_result = runner::run_bspatch(&first_archive, &patch, archive_v2.len() as u64);
                log!(
                    json_mode,
                    "  {} {}",
                    report::format_duration(patch_result.duration),
                    patch_result.name
                );
                results.push(patch_result);
                drop(patch);
            }

            // Chunked bsdiff
            let (chunked_diff_result, chunked_patch) = runner::run_chunked_bsdiff(&first_archive, &archive_v2);
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(chunked_diff_result.duration),
                chunked_diff_result.name
            );
            results.push(chunked_diff_result);

            let chunked_patch_result =
                runner::run_chunked_bspatch(&first_archive, &chunked_patch, archive_v2.len() as u64);
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(chunked_patch_result.duration),
                chunked_patch_result.name
            );
            results.push(chunked_patch_result);
        }
    }

    // 9. Real-world update scenario
    if !args.installers_only && !args.skip_diff && !args.skip_update_scenario {
        log!(json_mode, "");
        log!(json_mode, "Update scenario (real update manager chain)...");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let scenario_results = rt
            .block_on(runner::run_update_scenario(
                work_dir,
                args.scale,
                args.seed,
                scenario,
                args.num_deltas,
                args.pack_zstd_level,
                args.pack_max_threads,
                args.pack_memory_mb,
            ))
            .expect("update scenario");
        for result in &scenario_results {
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(result.duration),
                result.name
            );
        }
        results.extend(scenario_results);
    }

    // 10-11. Installers
    if args.installers_only {
        log!(json_mode, "");
        log!(json_mode, "Installer scenario (real console installer build + run)...");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let installer_results = rt
            .block_on(runner::run_installer_scenario(
                work_dir,
                args.scale,
                args.seed,
                args.pack_zstd_level,
                args.pack_max_threads,
                args.pack_memory_mb,
            ))
            .expect("installer scenario");
        for result in &installer_results {
            log!(
                json_mode,
                "  {} {}",
                report::format_duration(result.duration),
                result.name
            );
        }
        results.extend(installer_results);
    } else if !args.update_only && !args.skip_installers {
        let online_result = runner::run_installer_online(&generated.v1_dir);
        log!(
            json_mode,
            "  {} {}",
            report::format_duration(online_result.duration),
            online_result.name
        );
        results.push(online_result);

        let offline_result = runner::run_installer_offline(&generated.v1_dir);
        log!(
            json_mode,
            "  {} {}",
            report::format_duration(offline_result.duration),
            offline_result.name
        );
        results.push(offline_result);
    }

    let benchmark_report = BenchmarkReport {
        scale: args.scale,
        seed: args.seed,
        total_files: generated.total_files,
        payload_size: generated.total_size_v1,
        results,
    };

    if args.benchmark_format {
        report::print_benchmark_json(&benchmark_report);
    } else if args.json {
        report::print_json(&benchmark_report);
    } else {
        report::print_results(&benchmark_report);
    }

    // TempDir auto-cleanup on drop
}

fn find_largest_file(dir: &std::path::Path) -> Vec<u8> {
    let mut largest_path = None;
    let mut largest_size = 0u64;

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata()
                && meta.is_file()
                && meta.len() > largest_size
            {
                largest_size = meta.len();
                largest_path = Some(entry.path());
            }
        }
    }

    largest_path
        .map(|p| fs::read(p).unwrap_or_default())
        .unwrap_or_default()
}
