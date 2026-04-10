#![allow(clippy::cast_sign_loss, clippy::too_many_lines)]

use std::time::{Duration, Instant};

mod fs_compare;
mod installer;
mod manifest;
mod microbench;
mod update;

pub use installer::run_installer_scenario;
pub use microbench::{
    run_archive_create, run_archive_extract, run_bsdiff, run_bspatch, run_chunked_bsdiff, run_chunked_bspatch,
    run_installer_offline, run_installer_online, run_sha256_file, run_sha256_memory, run_zstd_compress,
    run_zstd_decompress,
};
pub use update::run_update_scenario;

pub(super) const BENCH_APP_ID: &str = "bench-app";
pub(super) const BENCH_APP_NAME: &str = "Benchmark App";

pub(super) fn time<F, T>(f: F) -> (T, Duration)
where
    F: FnOnce() -> T,
{
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}
