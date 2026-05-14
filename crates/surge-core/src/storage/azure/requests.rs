use std::path::Path;

use async_trait::async_trait;
use reqwest::Client;
use tracing::debug;

use crate::context::StorageConfig;
use crate::error::{Result, SurgeError};
use crate::storage::{
    ListResult, ObjectInfo, StorageBackend, TransferProgress, download_response_to_file,
    download_response_to_file_from_offset,
};

use super::AzureBlobBackend;
use super::auth::decode_account_key;
use super::listing::parse_azure_list_blobs_xml;

impl AzureBlobBackend {
    /// Create a new Azure Blob Storage backend from configuration.
    ///
    /// Expects:
    /// - `config.bucket` as the container name
    /// - `config.access_key` as the storage account name
    /// - `config.secret_key` as the base64-encoded account key
    pub fn new(config: &StorageConfig) -> Result<Self> {
        let account_name = if config.access_key.is_empty() {
            std::env::var("AZURE_STORAGE_ACCOUNT_NAME")
                .or_else(|_| std::env::var("AZURE_STORAGE_ACCOUNT"))
                .or_else(|_| std::env::var("SURGE_AZURE_ACCESS_KEY"))
                .unwrap_or_default()
        } else {
            config.access_key.clone()
        };
        let account_key = if config.secret_key.is_empty() {
            std::env::var("AZURE_STORAGE_ACCOUNT_KEY")
                .or_else(|_| std::env::var("AZURE_STORAGE_KEY"))
                .or_else(|_| std::env::var("SURGE_AZURE_SECRET_KEY"))
                .unwrap_or_default()
        } else {
            config.secret_key.clone()
        };

        if config.bucket.is_empty() {
            return Err(SurgeError::Config(
                "Azure Blob storage requires a container name (bucket)".to_string(),
            ));
        }

        let endpoint = if config.endpoint.is_empty() {
            format!("https://{account_name}.blob.core.windows.net")
        } else {
            config.endpoint.clone()
        };

        let client = Client::builder()
            .build()
            .map_err(|e| SurgeError::Storage(format!("Failed to build HTTP client: {e}")))?;

        debug!(
            account = %account_name,
            container = %config.bucket,
            endpoint = %endpoint,
            "Azure Blob backend initialized"
        );

        Ok(Self {
            client,
            account: account_name,
            key_bytes: decode_account_key(&account_key)?,
            container: config.bucket.clone(),
            endpoint,
            prefix: config.prefix.clone(),
        })
    }

    /// Build the full blob name, prepending the configured prefix.
    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), key)
        }
    }

    /// Build the URL for a given blob.
    fn blob_url(&self, blob_name: &str) -> String {
        format!(
            "{}/{}/{}",
            self.endpoint.trim_end_matches('/'),
            self.container,
            blob_name
        )
    }

    /// Build the container-level URL (for list operations).
    fn container_url(&self) -> String {
        format!("{}/{}", self.endpoint.trim_end_matches('/'), self.container)
    }

    /// Map an HTTP response status to a `SurgeError`.
    fn check_response_status(status: reqwest::StatusCode, key: &str, body: &str) -> Result<()> {
        if status.is_success() {
            return Ok(());
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(SurgeError::NotFound(format!("Azure blob not found: {key}")));
        }
        Err(SurgeError::Storage(format!(
            "Azure request failed (HTTP {status}): {body}"
        )))
    }
}

#[async_trait]
impl StorageBackend for AzureBlobBackend {
    async fn put_object(&self, key: &str, data: &[u8], content_type: &str) -> Result<()> {
        self.require_credentials("PUT")?;
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);
        let resource_path = format!("/{}/{}", self.container, full_key);

        let extra_headers = vec![("x-ms-blob-type".to_string(), "BlockBlob".to_string())];

        let headers = self.sign_request(
            "PUT",
            &resource_path,
            &[],
            Some(data.len()),
            content_type,
            None,
            &extra_headers,
        );

        let mut req = self.client.put(&url).body(data.to_vec());
        req = req.header("Content-Length", data.len().to_string());
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Self::check_response_status(status, &full_key, &body)?;

        debug!(key = %full_key, "Azure PUT completed");
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);

        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let resource_path = format!("/{}/{}", self.container, full_key);
            let headers = self.sign_request("GET", &resource_path, &[], None, "", None, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).to_string();
            Self::check_response_status(status, &full_key, &body)?;
        }

        debug!(key = %full_key, size = bytes.len(), "Azure GET completed");
        Ok(bytes.to_vec())
    }

    async fn head_object(&self, key: &str) -> Result<ObjectInfo> {
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);

        let mut req = self.client.head(&url);
        if self.has_credentials() {
            let resource_path = format!("/{}/{}", self.container, full_key);
            let headers = self.sign_request("HEAD", &resource_path, &[], None, "", None, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(SurgeError::NotFound(format!("Azure blob not found: {full_key}")));
            }
            return Err(SurgeError::Storage(format!("Azure HEAD failed (HTTP {status})")));
        }

        let size = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .trim_matches('"')
            .to_string();

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();

        debug!(key = %full_key, size, "Azure HEAD completed");
        Ok(ObjectInfo {
            size,
            etag,
            content_type,
        })
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.require_credentials("DELETE")?;
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);
        let resource_path = format!("/{}/{}", self.container, full_key);

        let headers = self.sign_request("DELETE", &resource_path, &[], None, "", None, &[]);

        let mut req = self.client.delete(&url);
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() && status != reqwest::StatusCode::ACCEPTED && status != reqwest::StatusCode::NOT_FOUND {
            let body = resp.text().await.unwrap_or_default();
            Self::check_response_status(status, &full_key, &body)?;
        }

        debug!(key = %full_key, "Azure DELETE completed");
        Ok(())
    }

    async fn list_objects(&self, prefix: &str, marker: Option<&str>, max_keys: i32) -> Result<ListResult> {
        let full_prefix = self.full_key(prefix);
        let container_url = self.container_url();

        let mut query_params: Vec<(String, String)> = vec![
            ("comp".to_string(), "list".to_string()),
            ("restype".to_string(), "container".to_string()),
            ("maxresults".to_string(), max_keys.to_string()),
            ("prefix".to_string(), full_prefix.clone()),
        ];
        if let Some(m) = marker {
            query_params.push(("marker".to_string(), m.to_string()));
        }

        let querystring = query_params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let url = format!("{container_url}?{querystring}");
        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let resource_path = format!("/{}", self.container);
            let headers = self.sign_request("GET", &resource_path, &query_params, None, "", None, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Self::check_response_status(status, &full_prefix, &body)?;

        parse_azure_list_blobs_xml(&body)
    }

    async fn download_to_file(&self, key: &str, dest: &Path, progress: Option<&TransferProgress<'_>>) -> Result<()> {
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);

        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let resource_path = format!("/{}/{}", self.container, full_key);
            let headers = self.sign_request("GET", &resource_path, &[], None, "", None, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Self::check_response_status(status, &full_key, &body);
        }

        download_response_to_file(resp, dest, progress).await?;

        debug!(key = %full_key, dest = %dest.display(), "Azure download completed");
        Ok(())
    }

    fn supports_resumable_downloads(&self) -> bool {
        true
    }

    async fn download_to_file_from_offset(
        &self,
        key: &str,
        dest: &Path,
        offset: u64,
        progress: Option<&TransferProgress<'_>>,
    ) -> Result<()> {
        if offset == 0 {
            return self.download_to_file(key, dest, progress).await;
        }

        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);
        let range_header = format!("bytes={offset}-");

        let mut req = self
            .client
            .get(&url)
            .header(reqwest::header::RANGE, range_header.as_str());
        if self.has_credentials() {
            let resource_path = format!("/{}/{}", self.container, full_key);
            let headers = self.sign_request("GET", &resource_path, &[], None, "", Some(&range_header), &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            let body = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                return Self::check_response_status(status, &full_key, &body);
            }
            return Err(SurgeError::Storage(format!(
                "Azure blob '{full_key}' did not honor resumable range request from byte {offset} (HTTP {status})"
            )));
        }

        download_response_to_file_from_offset(resp, dest, offset, progress).await?;

        debug!(
            key = %full_key,
            dest = %dest.display(),
            offset,
            "Azure resumable download completed"
        );
        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress<'_>>) -> Result<()> {
        self.require_credentials("upload")?;
        let data = tokio::fs::read(src).await?;
        let total = data.len() as u64;
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);
        let resource_path = format!("/{}/{}", self.container, full_key);

        let extra_headers = vec![("x-ms-blob-type".to_string(), "BlockBlob".to_string())];
        let headers = self.sign_request(
            "PUT",
            &resource_path,
            &[],
            Some(data.len()),
            "application/octet-stream",
            None,
            &extra_headers,
        );

        let mut req = self.client.put(&url).body(data);
        req = req.header("Content-Length", total.to_string());
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Self::check_response_status(status, &full_key, &body)?;

        if let Some(cb) = progress {
            cb(total, total);
        }
        Ok(())
    }
}
