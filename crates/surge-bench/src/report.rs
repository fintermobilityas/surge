#![allow(clippy::cast_precision_loss)]

use std::time::Duration;

use serde::Serialize;

#[derive(Serialize)]
pub struct BenchmarkReport {
    pub scale: f64,
    pub seed: u64,
    pub total_files: usize,
    pub payload_size: u64,
    pub results: Vec<BenchmarkResult>,
}

#[derive(Serialize)]
pub struct BenchmarkResult {
    pub name: String,
    #[serde(serialize_with = "serialize_duration")]
    pub duration: Duration,
    pub input_size: u64,
    pub output_size: u64,
}

fn serialize_duration<S: serde::Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_f64(d.as_secs_f64() * 1000.0)
}

#[derive(Serialize)]
struct BenchmarkEntry {
    name: String,
    unit: String,
    value: f64,
}

fn should_export_size_metric(name: &str) -> bool {
    name.starts_with("Full pack build")
        || name.starts_with("Delta pack build")
        || name.starts_with("Full artifact upload")
        || name.starts_with("Delta artifact upload")
        || name.starts_with("Release index update")
        || name.starts_with("Update check")
        || name.starts_with("Installer create")
}

pub fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs >= 60.0 {
        let mins = secs / 60.0;
        format!("{mins:.1}m")
    } else if secs >= 1.0 {
        format!("{secs:.2}s")
    } else {
        format!("{:.0}ms", secs * 1000.0)
    }
}

pub fn format_throughput(bytes: u64, d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs == 0.0 {
        return "N/A".to_string();
    }
    let mb_per_sec = (bytes as f64 / (1024.0 * 1024.0)) / secs;
    format!("{mb_per_sec:.1} MB/s")
}

pub fn print_results(report: &BenchmarkReport) {
    println!();
    println!("Surge Benchmark Results");
    println!("=======================");
    println!(
        "Scale: {:.2} | Seed: {} | Files: {} | Payload: {}",
        report.scale,
        report.seed,
        report.total_files,
        format_size(report.payload_size)
    );
    println!();
    println!(
        "{:<36} {:>10} {:>12} {:>12} {:>12}",
        "Operation", "Duration", "Input", "Output", "Throughput"
    );
    println!("{}", "\u{2500}".repeat(86));

    for r in &report.results {
        println!(
            "{:<36} {:>10} {:>12} {:>12} {:>12}",
            r.name,
            format_duration(r.duration),
            format_size(r.input_size),
            format_size(r.output_size),
            format_throughput(r.input_size, r.duration),
        );
    }
    println!();
}

pub fn print_json(report: &BenchmarkReport) {
    let json = serde_json::to_string_pretty(report).expect("Failed to serialize report");
    println!("{json}");
}

pub fn print_benchmark_json(report: &BenchmarkReport) {
    let entries: Vec<BenchmarkEntry> = report
        .results
        .iter()
        .flat_map(|r| {
            let mut entries = vec![BenchmarkEntry {
                name: r.name.clone(),
                unit: "ms".to_string(),
                value: r.duration.as_secs_f64() * 1000.0,
            }];
            if should_export_size_metric(&r.name) && r.output_size > 0 {
                entries.push(BenchmarkEntry {
                    name: format!("{} size", r.name),
                    unit: "MiB".to_string(),
                    value: r.output_size as f64 / (1024.0 * 1024.0),
                });
            }
            entries
        })
        .collect();
    let json = serde_json::to_string_pretty(&entries).expect("Failed to serialize benchmark entries");
    println!("{json}");
}
