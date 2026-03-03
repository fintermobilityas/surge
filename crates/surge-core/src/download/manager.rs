//! Download manager with parallel downloads, SHA-256 verification, and progress tracking.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;

use crate::context::Context;
use crate::error::{Result, SurgeError};

#[derive(Debug, Clone)]
pub struct DownloadRequest {
    pub url: String,
    pub dest_path: PathBuf,
    /// Lowercase hex. Empty to skip verification.
    pub expected_sha256: String,
    /// 0 to skip size verification.
    pub expected_size: u64,
}

#[derive(Debug, Clone)]
pub struct DownloadResult {
    pub index: usize,
    pub success: bool,
    pub http_status: u16,
    pub bytes_downloaded: u64,
    pub sha256: String,
    pub error_message: String,
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub total_bytes_done: u64,
    pub total_bytes_total: u64,
    pub files_done: u64,
    pub files_total: u64,
    pub speed_bytes_per_sec: f64,
}

pub struct DownloadManager {
    ctx: Arc<Context>,
    client: reqwest::Client,
}

impl DownloadManager {
    pub fn new(ctx: Arc<Context>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent(format!("surge/{}", crate::config::constants::VERSION))
            .build()
            .unwrap_or_default();

        Self { ctx, client }
    }

    /// Downloads concurrently up to `max_concurrent_downloads` from the resource budget.
    pub async fn download<F>(&self, requests: Vec<DownloadRequest>, progress: Option<F>) -> Result<Vec<DownloadResult>>
    where
        F: Fn(DownloadProgress) + Send + Sync + 'static,
    {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        self.ctx.check_cancelled()?;

        let budget = self.ctx.resource_budget();
        let max_concurrent = budget.max_concurrent_downloads.max(1) as usize;
        let semaphore = Arc::new(Semaphore::new(max_concurrent));

        let files_total = requests.len() as u64;
        let total_bytes_total: u64 = requests.iter().map(|r| r.expected_size).sum();
        let total_bytes_done = Arc::new(AtomicU64::new(0));
        let files_done = Arc::new(AtomicU64::new(0));
        let start_time = Instant::now();

        let progress: Option<Arc<dyn Fn(DownloadProgress) + Send + Sync>> =
            progress.map(|f| Arc::new(f) as Arc<dyn Fn(DownloadProgress) + Send + Sync>);

        let mut handles = Vec::with_capacity(requests.len());

        for (index, request) in requests.into_iter().enumerate() {
            let client = self.client.clone();
            let ctx = self.ctx.clone();
            let sem = semaphore.clone();
            let bytes_done = total_bytes_done.clone();
            let f_done = files_done.clone();
            let progress = progress.clone();

            let handle = tokio::spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|e| SurgeError::Other(format!("Semaphore error: {e}")))?;

                let result = download_single_file(
                    &client,
                    &ctx,
                    index,
                    &request,
                    &bytes_done,
                    &f_done,
                    files_total,
                    total_bytes_total,
                    start_time,
                    progress.as_ref(),
                )
                .await;

                Ok::<DownloadResult, SurgeError>(result)
            });

            handles.push(handle);
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            let result = handle
                .await
                .map_err(|e| SurgeError::Other(format!("Task join error: {e}")))?;
            results.push(result?);
        }

        results.sort_by_key(|r| r.index);
        Ok(results)
    }
}

async fn download_single_file(
    client: &reqwest::Client,
    ctx: &Context,
    index: usize,
    request: &DownloadRequest,
    total_bytes_done: &AtomicU64,
    files_done: &AtomicU64,
    files_total: u64,
    total_bytes_total: u64,
    start_time: Instant,
    progress: Option<&Arc<dyn Fn(DownloadProgress) + Send + Sync>>,
) -> DownloadResult {
    let make_error = |status: u16, msg: String| DownloadResult {
        index,
        success: false,
        http_status: status,
        bytes_downloaded: 0,
        sha256: String::new(),
        error_message: msg,
    };

    if ctx.is_cancelled() {
        return make_error(0, "Cancelled".to_string());
    }

    let response = match client.get(&request.url).send().await {
        Ok(resp) => resp,
        Err(e) => return make_error(0, format!("HTTP request failed: {e}")),
    };

    let http_status = response.status().as_u16();
    if !response.status().is_success() {
        return make_error(http_status, format!("HTTP {http_status} for {}", request.url));
    }

    if let Some(parent) = request.dest_path.parent()
        && let Err(e) = tokio::fs::create_dir_all(parent).await
    {
        return make_error(http_status, format!("Failed to create directory: {e}"));
    }

    let body = match response.bytes().await {
        Ok(b) => b,
        Err(e) => return make_error(http_status, format!("Failed to read body: {e}")),
    };

    let bytes_downloaded = body.len() as u64;

    let mut hasher = Sha256::new();
    hasher.update(&body);
    let sha256 = hex::encode(hasher.finalize());

    if let Err(e) = tokio::fs::write(&request.dest_path, &body).await {
        return make_error(http_status, format!("Failed to write file: {e}"));
    }

    if !request.expected_sha256.is_empty() && sha256 != request.expected_sha256 {
        let _ = tokio::fs::remove_file(&request.dest_path).await;
        return make_error(
            http_status,
            format!("SHA-256 mismatch: expected {}, got {sha256}", request.expected_sha256),
        );
    }

    if request.expected_size > 0 && bytes_downloaded != request.expected_size {
        let _ = tokio::fs::remove_file(&request.dest_path).await;
        return make_error(
            http_status,
            format!(
                "Size mismatch: expected {}, got {bytes_downloaded}",
                request.expected_size
            ),
        );
    }

    total_bytes_done.fetch_add(bytes_downloaded, Ordering::Relaxed);
    files_done.fetch_add(1, Ordering::Relaxed);

    if let Some(cb) = progress {
        let elapsed = start_time.elapsed().as_secs_f64();
        let done = total_bytes_done.load(Ordering::Relaxed);
        let speed = if elapsed > 0.0 { done as f64 / elapsed } else { 0.0 };

        cb(DownloadProgress {
            total_bytes_done: done,
            total_bytes_total,
            files_done: files_done.load(Ordering::Relaxed),
            files_total,
            speed_bytes_per_sec: speed,
        });
    }

    DownloadResult {
        index,
        success: true,
        http_status,
        bytes_downloaded,
        sha256,
        error_message: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_download_request_creation() {
        let req = DownloadRequest {
            url: "https://example.com/file.tar.zst".to_string(),
            dest_path: PathBuf::from("/tmp/file.tar.zst"),
            expected_sha256: "abc123".to_string(),
            expected_size: 1024,
        };
        assert_eq!(req.url, "https://example.com/file.tar.zst");
        assert_eq!(req.expected_size, 1024);
    }

    #[test]
    fn test_download_progress_default() {
        let progress = DownloadProgress {
            total_bytes_done: 0,
            total_bytes_total: 0,
            files_done: 0,
            files_total: 0,
            speed_bytes_per_sec: 0.0,
        };
        assert_eq!(progress.files_done, 0);
    }

    #[test]
    fn test_download_manager_creation() {
        let ctx = Arc::new(Context::new());
        let _manager = DownloadManager::new(ctx);
    }
}
