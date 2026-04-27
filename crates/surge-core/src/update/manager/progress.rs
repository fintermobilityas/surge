use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

/// Progress information for update operations.
#[derive(Debug, Clone)]
pub struct ProgressInfo {
    /// Current phase (1 = check, 2 = download, 3 = verify, 4 = extract, 5 = apply_delta, 6 = finalize).
    pub phase: i32,
    /// Percentage complete within the current phase (0-100).
    pub phase_percent: i32,
    /// Overall percentage complete (0-100).
    pub total_percent: i32,
    /// Bytes processed so far in this phase.
    pub bytes_done: i64,
    /// Total bytes expected in this phase.
    pub bytes_total: i64,
    /// Items processed so far in this phase.
    pub items_done: i64,
    /// Total items expected in this phase.
    pub items_total: i64,
    /// Current processing speed in bytes per second.
    pub speed_bytes_per_sec: f64,
}

impl Default for ProgressInfo {
    fn default() -> Self {
        Self {
            phase: 0,
            phase_percent: 0,
            total_percent: 0,
            bytes_done: 0,
            bytes_total: 0,
            items_done: 0,
            items_total: 0,
            speed_bytes_per_sec: 0.0,
        }
    }
}

pub(super) fn emit_progress<F>(progress: Option<&Arc<F>>, progress_info: ProgressInfo)
where
    F: Fn(ProgressInfo) + Send + Sync,
{
    if let Some(cb) = progress {
        cb(progress_info);
    }
}

pub(super) fn clamp_progress_percent(done: i64, total: i64) -> i32 {
    if total > 0 {
        done.saturating_mul(100)
            .checked_div(total)
            .map_or(0, |percent| percent.clamp(0, 100) as i32)
    } else {
        0
    }
}

pub(super) fn clamp_progress_percent_u64(done: u64, total: u64) -> i32 {
    if total > 0 {
        done.saturating_mul(100)
            .checked_div(total)
            .map_or(0, |percent| percent.clamp(0, 100) as i32)
    } else {
        0
    }
}

pub(super) fn saturating_i64_from_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(super) fn phase_total_percent(phase_start: i32, phase_span: i32, phase_percent: i32) -> i32 {
    phase_start + phase_percent.clamp(0, 100) * phase_span / 100
}

#[allow(clippy::cast_precision_loss)]
pub(super) fn average_speed_bytes_per_sec(bytes_done: u64, started_at: Instant) -> f64 {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed > 0.0 {
        bytes_done as f64 / elapsed
    } else {
        0.0
    }
}

#[derive(Debug, Clone)]
pub(super) struct ArtifactDownload {
    pub key: String,
    pub sha256: String,
    pub size: i64,
}

#[derive(Debug)]
pub(super) struct DownloadProgressState {
    started_at: Instant,
    bytes_by_artifact: BTreeMap<String, u64>,
    bytes_done: u64,
    items_done: i64,
}

impl DownloadProgressState {
    pub(super) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            bytes_by_artifact: BTreeMap::new(),
            bytes_done: 0,
            items_done: 0,
        }
    }

    pub(super) fn started_at(&self) -> Instant {
        self.started_at
    }

    pub(super) fn bytes_done(&self) -> u64 {
        self.bytes_done
    }

    pub(super) fn observe_artifact_bytes(&mut self, key: &str, done: u64) {
        let previous = self.bytes_by_artifact.insert(key.to_string(), done).unwrap_or(0);
        self.bytes_done = self.bytes_done.saturating_add(done.saturating_sub(previous));
    }

    pub(super) fn finish_artifact(&mut self, key: &str, total: u64) {
        self.observe_artifact_bytes(key, total);
        self.items_done = self.items_done.saturating_add(1);
    }

    pub(super) fn snapshot(&self, total_bytes: u64, total_items: i64) -> ProgressInfo {
        let phase_percent = if total_bytes > 0 {
            clamp_progress_percent_u64(self.bytes_done, total_bytes)
        } else {
            clamp_progress_percent(self.items_done, total_items.max(1))
        };
        ProgressInfo {
            phase: 2,
            phase_percent,
            total_percent: 10 + phase_percent * 30 / 100,
            bytes_done: saturating_i64_from_u64(self.bytes_done),
            bytes_total: saturating_i64_from_u64(total_bytes),
            items_done: self.items_done,
            items_total: total_items,
            speed_bytes_per_sec: average_speed_bytes_per_sec(self.bytes_done, self.started_at),
        }
    }
}
