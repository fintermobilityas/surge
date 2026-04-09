use std::path::Path;
use std::time::Duration;

use crate::formatters::{format_bytes, format_duration};
use crate::logline;
use crate::ui::UiTheme;

pub(super) fn file_size_label(path: &Path) -> String {
    match std::fs::metadata(path) {
        Ok(meta) => format_bytes(meta.len()),
        Err(_) => "unknown size".to_string(),
    }
}

pub(super) fn print_stage(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::info(&format!("[{stage}/{total}] {text}"));
}

pub(super) fn print_stage_done(theme: UiTheme, stage: usize, total: usize, text: &str) {
    let _ = theme;
    logline::success(&format!("[{stage}/{total}] {text}"));
}

pub(super) fn pack_build_phase_message(step_done: i32, step_count: i32) -> String {
    if step_done <= 0 {
        return format!("Packaging files (step 1/{step_count}: full archive)");
    }
    if step_done < step_count {
        return format!("Packaging files (step {}/{}: delta package)", step_done + 1, step_count);
    }
    "Finalizing package artifacts".to_string()
}

pub(super) fn upload_progress_message(uploaded: u64, total_bytes: u64, started: std::time::Instant) -> String {
    if uploaded == 0 {
        format!(
            "uploaded 0 B / {} (elapsed {})",
            format_bytes(total_bytes),
            format_duration(started.elapsed())
        )
    } else {
        format!(
            "{} (elapsed {})",
            crate::formatters::format_byte_progress(uploaded, total_bytes, "uploaded"),
            format_duration(started.elapsed())
        )
    }
}

pub(super) fn upload_heartbeat_interval() -> Duration {
    Duration::from_secs(5)
}
