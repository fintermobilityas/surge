//! Azure Blob Storage backend with SharedKey authentication.

use async_trait::async_trait;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use reqwest::Client;
use tracing::{debug, trace};

use crate::context::StorageConfig;
use crate::crypto::hmac_sha256::hmac_sha256;
use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult, ObjectInfo, StorageBackend, TransferProgress};

/// Azure Blob Storage REST API version.
const AZURE_API_VERSION: &str = "2024-08-04";

/// Azure Blob Storage backend using SharedKey authentication.
pub struct AzureBlobBackend {
    client: Client,
    account: String,
    /// Base64-decoded account key (raw bytes) for HMAC signing.
    key_bytes: Vec<u8>,
    container: String,
    endpoint: String,
    prefix: String,
}

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

        let key_bytes = if account_key.is_empty() {
            Vec::new()
        } else {
            BASE64
                .decode(&account_key)
                .map_err(|e| SurgeError::Config(format!("Azure account key is not valid base64: {e}")))?
        };

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
            key_bytes,
            container: config.bucket.clone(),
            endpoint,
            prefix: config.prefix.clone(),
        })
    }

    /// Returns `true` when credentials are available for signing requests.
    fn has_credentials(&self) -> bool {
        !self.key_bytes.is_empty()
    }

    /// Return an error when a write is attempted without credentials.
    fn require_credentials(&self, operation: &str) -> Result<()> {
        if self.has_credentials() {
            Ok(())
        } else {
            Err(SurgeError::Config(format!(
                "Azure Blob {operation} requires account credentials"
            )))
        }
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

    /// Produce an RFC 1123 date string (e.g. `Mon, 02 Mar 2026 12:34:56 GMT`).
    fn rfc1123_date(now: &chrono::DateTime<Utc>) -> String {
        now.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
    }

    /// Sign a request using SharedKey authentication and return the headers
    /// that must be attached to the request.
    ///
    /// The string-to-sign format for Blob service:
    /// ```text
    /// VERB\n
    /// Content-Encoding\n
    /// Content-Language\n
    /// Content-Length\n
    /// Content-MD5\n
    /// Content-Type\n
    /// Date\n
    /// If-Modified-Since\n
    /// If-Match\n
    /// If-None-Match\n
    /// If-Unmodified-Since\n
    /// Range\n
    /// CanonicalizedHeaders\n
    /// CanonicalizedResource
    /// ```
    #[allow(clippy::too_many_arguments)]
    fn sign_request(
        &self,
        method: &str,
        resource_path: &str,
        query_params: &[(String, String)],
        content_length: Option<usize>,
        content_type: &str,
        extra_headers: &[(String, String)],
    ) -> Vec<(String, String)> {
        let now = Utc::now();
        let date = Self::rfc1123_date(&now);

        // Collect all x-ms-* headers (sorted).
        let mut ms_headers: Vec<(String, String)> = vec![
            ("x-ms-date".to_string(), date.clone()),
            ("x-ms-version".to_string(), AZURE_API_VERSION.to_string()),
        ];
        for (k, v) in extra_headers {
            if k.starts_with("x-ms-") {
                ms_headers.push((k.clone(), v.clone()));
            }
        }
        ms_headers.sort_by(|a, b| a.0.cmp(&b.0));
        // Deduplicate on key, keeping later values.
        ms_headers.dedup_by(|a, b| {
            if a.0 == b.0 {
                // Keep value from `b` (the earlier one after sort is stable).
                true
            } else {
                false
            }
        });

        let canonicalized_headers = ms_headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join("\n");

        // Canonicalized resource: /account/container/blob?\ncomp:list\n...
        let mut canonicalized_resource = format!("/{}{}", self.account, resource_path);
        if !query_params.is_empty() {
            let mut sorted_params = query_params.to_vec();
            sorted_params.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in &sorted_params {
                canonicalized_resource.push_str(&format!("\n{k}:{v}"));
            }
        }

        // Content-Length: omit for 0 or absent.
        let content_length_str = match content_length {
            Some(n) if n > 0 => n.to_string(),
            _ => String::new(),
        };

        // String to sign.
        let string_to_sign = format!(
            "{method}\n\
             \n\
             \n\
             {content_length_str}\n\
             \n\
             {content_type}\n\
             \n\
             \n\
             \n\
             \n\
             \n\
             \n\
             {canonicalized_headers}\n\
             {canonicalized_resource}"
        );

        trace!(string_to_sign = %string_to_sign, "Azure string to sign");

        // HMAC-SHA256 with the account key.
        let signature = BASE64.encode(hmac_sha256(&self.key_bytes, string_to_sign.as_bytes()));

        let authorization = format!("SharedKey {}:{signature}", self.account);

        let mut headers: Vec<(String, String)> = ms_headers;
        headers.push(("Authorization".to_string(), authorization));
        if !content_type.is_empty() {
            headers.push(("Content-Type".to_string(), content_type.to_string()));
        }

        headers
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
            let headers = self.sign_request("GET", &resource_path, &[], None, "", &[]);
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
            let headers = self.sign_request("HEAD", &resource_path, &[], None, "", &[]);
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

        let headers = self.sign_request("DELETE", &resource_path, &[], None, "", &[]);

        let mut req = self.client.delete(&url);
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        // Azure returns 202 Accepted for deletes, and 404 if already gone.
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

        // Query parameters for List Blobs.
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
            let headers = self.sign_request("GET", &resource_path, &query_params, None, "", &[]);
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

    async fn download_to_file(&self, key: &str, dest: &Path, progress: Option<&TransferProgress>) -> Result<()> {
        let full_key = self.full_key(key);
        let url = self.blob_url(&full_key);

        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let resource_path = format!("/{}/{}", self.container, full_key);
            let headers = self.sign_request("GET", &resource_path, &[], None, "", &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        }

        let resp = req.send().await?;
        let status = resp.status();
        let total = resp.content_length().unwrap_or(0);
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).to_string();
            Self::check_response_status(status, &full_key, &body)?;
        }

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(dest, &bytes).await?;

        if let Some(cb) = progress {
            cb(bytes.len() as u64, total.max(bytes.len() as u64));
        }

        debug!(key = %full_key, dest = %dest.display(), "Azure download completed");
        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress>) -> Result<()> {
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

// ---------------------------------------------------------------------------
// XML parsing for Azure List Blobs response
// ---------------------------------------------------------------------------

/// Parse an Azure List Blobs XML response into a `ListResult`.
///
/// Azure response structure:
/// ```xml
/// <EnumerationResults>
///   <Blobs>
///     <Blob>
///       <Name>key</Name>
///       <Properties>
///         <Content-Length>123</Content-Length>
///       </Properties>
///     </Blob>
///   </Blobs>
///   <NextMarker>...</NextMarker>
/// </EnumerationResults>
/// ```
fn parse_azure_list_blobs_xml(xml: &str) -> Result<ListResult> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    let mut next_marker: Option<String> = None;

    let mut in_blob = false;
    let mut in_properties = false;
    let mut current_name: Option<String> = None;
    let mut current_size: Option<i64> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match tag.as_str() {
                    "Blob" => {
                        in_blob = true;
                        current_name = None;
                        current_size = None;
                    }
                    "Properties" if in_blob => {
                        in_properties = true;
                    }
                    _ => {}
                }
                current_tag = tag;
            }
            Ok(Event::End(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match tag.as_str() {
                    "Blob" => {
                        if let Some(name) = current_name.take() {
                            entries.push(ListEntry {
                                key: name,
                                size: current_size.unwrap_or(0),
                            });
                        }
                        in_blob = false;
                        in_properties = false;
                    }
                    "Properties" => {
                        in_properties = false;
                    }
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).to_string();
                if in_blob && !in_properties && current_tag == "Name" {
                    current_name = Some(text);
                } else if in_properties && current_tag == "Content-Length" {
                    current_size = text.parse::<i64>().ok();
                } else if !in_blob && current_tag == "NextMarker" && !text.is_empty() {
                    next_marker = Some(text);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(SurgeError::Storage(format!("Failed to parse Azure list response: {e}")));
            }
            _ => {}
        }
        buf.clear();
    }

    let is_truncated = next_marker.is_some();
    debug!(count = entries.len(), is_truncated, "Azure LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}
