#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Benchmark comparing classic bsdiff vs chunked bsdiff on real files.
//!
//! Usage: cargo run --release -p surge-bench --example diff_bench -- <old_file> <new_file>

use std::fs;
use std::time::Instant;

use surge_core::diff::chunked::{self, ChunkedDiffOptions};
use surge_core::diff::wrapper;

fn peak_rss_mb() -> f64 {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if line.starts_with("VmPeak:") {
            let kb: f64 = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

fn current_rss_mb() -> f64 {
    let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let kb: f64 = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

fn reset_peak_rss() {
    // Writing to /proc/self/clear_refs can reset some counters but VmPeak
    // cannot be reset. We'll just note the current peak before each test.
}

fn format_size(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: diff_bench <old_file> <new_file> [--skip-classic]");
        std::process::exit(1);
    }

    let old_path = &args[1];
    let new_path = &args[2];
    let skip_classic = args.iter().any(|a| a == "--skip-classic");

    println!("Loading files...");
    let t0 = Instant::now();
    let old_data = fs::read(old_path).expect("Failed to read old file");
    let new_data = fs::read(new_path).expect("Failed to read new file");
    println!(
        "  Loaded in {:.2}s | old: {} | new: {}",
        t0.elapsed().as_secs_f64(),
        format_size(old_data.len()),
        format_size(new_data.len()),
    );
    let rss_after_load = current_rss_mb();
    println!("  RSS after load: {rss_after_load:.0} MB");
    println!();

    // --- Classic bsdiff ---
    if !skip_classic {
        println!("=== Classic bsdiff ===");
        reset_peak_rss();
        let peak_before = peak_rss_mb();

        let t1 = Instant::now();
        let patch = wrapper::bsdiff_buffers(&old_data, &new_data).expect("bsdiff failed");
        let diff_time = t1.elapsed();

        let peak_after = peak_rss_mb();
        println!("  Time:       {:.2}s", diff_time.as_secs_f64());
        println!("  Patch size: {}", format_size(patch.len()));
        println!(
            "  Peak RSS:   {:.0} MB (delta: +{:.0} MB)",
            peak_after,
            peak_after - peak_before
        );

        // Verify with bspatch
        let t2 = Instant::now();
        let reconstructed = wrapper::bspatch_buffers(&old_data, &patch).expect("bspatch failed");
        let patch_time = t2.elapsed();
        println!("  Patch time: {:.2}s", patch_time.as_secs_f64());
        println!("  Verified:   {}", reconstructed == new_data);
        println!();

        // Free classic patch memory before chunked test
        drop(patch);
        drop(reconstructed);
    }

    // --- Chunked bsdiff ---
    let chunk_sizes: &[usize] = &[32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024];
    let thread_counts: &[usize] = &[1, 4, 0]; // 0 = auto

    for &chunk_size in chunk_sizes {
        for &threads in thread_counts {
            let opts = ChunkedDiffOptions {
                chunk_size,
                max_threads: threads,
            };
            let thread_label = if threads == 0 {
                "auto".to_string()
            } else {
                threads.to_string()
            };
            let chunk_mb = chunk_size / (1024 * 1024);

            println!("=== Chunked bsdiff (chunk={chunk_mb}MB, threads={thread_label}) ===");
            reset_peak_rss();
            let peak_before = peak_rss_mb();

            let t1 = Instant::now();
            let patch = chunked::chunked_bsdiff(&old_data, &new_data, &opts).expect("chunked bsdiff failed");
            let diff_time = t1.elapsed();

            let peak_after = peak_rss_mb();
            println!("  Time:       {:.2}s", diff_time.as_secs_f64());
            println!("  Patch size: {}", format_size(patch.len()));
            println!(
                "  Peak RSS:   {:.0} MB (delta: +{:.0} MB)",
                peak_after,
                peak_after - peak_before
            );

            // Verify with chunked bspatch
            let t2 = Instant::now();
            let reconstructed = chunked::chunked_bspatch(&old_data, &patch, &opts).expect("chunked bspatch failed");
            let patch_time = t2.elapsed();
            println!("  Patch time: {:.2}s", patch_time.as_secs_f64());
            println!("  Verified:   {}", reconstructed == new_data);
            println!();

            drop(patch);
            drop(reconstructed);
        }
    }
}
