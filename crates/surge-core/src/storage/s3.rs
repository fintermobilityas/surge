//! Amazon S3 storage backend with AWS Signature V4 authentication.

use async_trait::async_trait;
use std::path::Path;

use chrono::Utc;
use percent_encoding::utf8_percent_encode;
use reqwest::Client;
use tracing::{debug, trace};

use super::s3_wire::{self, URI_ENCODE_PATH_SET};
use crate::context::StorageConfig;
use crate::crypto::hmac_sha256::hmac_sha256;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::storage::{
    ListResult, ObjectInfo, StorageBackend, TransferProgress, download_response_to_file,
    download_response_to_file_from_offset,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumableDownloadResponseAction {
    AppendFromOffset,
    RestartFromZero,
    Error,
}

fn resumable_download_response_action(status: reqwest::StatusCode) -> ResumableDownloadResponseAction {
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        ResumableDownloadResponseAction::AppendFromOffset
    } else if status.is_success() {
        ResumableDownloadResponseAction::RestartFromZero
    } else {
        ResumableDownloadResponseAction::Error
    }
}

/// S3-compatible storage backend.
pub struct S3Backend {
    client: Client,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
    prefix: String,
    /// Use path-style URLs (`endpoint/bucket/key`) instead of virtual-hosted
    /// (`bucket.endpoint/key`). Enabled automatically for custom endpoints.
    path_style: bool,
}

impl S3Backend {
    /// Create a new S3 backend from configuration.
    pub fn new(config: &StorageConfig) -> Result<Self> {
        if config.bucket.is_empty() {
            return Err(SurgeError::Config("S3 storage requires a bucket name".to_string()));
        }

        let region = if config.region.is_empty() {
            "us-east-1".to_string()
        } else {
            config.region.clone()
        };

        let endpoint = if config.endpoint.is_empty() {
            format!("https://s3.{region}.amazonaws.com")
        } else {
            config.endpoint.clone()
        };

        // Custom endpoints (MinIO, LocalStack, etc.) use path-style addressing.
        let path_style = !config.endpoint.is_empty();

        let client = crate::net::http_client()?;

        debug!(
            bucket = %config.bucket,
            region = %region,
            endpoint = %endpoint,
            path_style = path_style,
            "S3 backend initialized"
        );

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            region,
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
            endpoint,
            prefix: config.prefix.clone(),
            path_style,
        })
    }

    /// Returns `true` when credentials are available for signing requests.
    fn has_credentials(&self) -> bool {
        !self.access_key.is_empty() && !self.secret_key.is_empty()
    }

    /// Return an error when a write is attempted without credentials.
    fn require_credentials(&self, operation: &str) -> Result<()> {
        if self.has_credentials() {
            Ok(())
        } else {
            Err(SurgeError::Config(format!(
                "S3 {operation} requires access_key and secret_key"
            )))
        }
    }

    /// Build the full object key, prepending the configured prefix.
    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), key)
        }
    }

    /// Build the URL for a given object key.
    fn object_url(&self, key: &str) -> String {
        let encoded_key = utf8_percent_encode(key, URI_ENCODE_PATH_SET).to_string();
        if self.path_style {
            format!(
                "{}/{}/{}",
                self.endpoint.trim_end_matches('/'),
                self.bucket,
                encoded_key
            )
        } else {
            format!(
                "https://{}.s3.{}.amazonaws.com/{}",
                self.bucket, self.region, encoded_key
            )
        }
    }

    /// Build the bucket-level URL (for list operations).
    fn bucket_url(&self) -> String {
        if self.path_style {
            format!("{}/{}", self.endpoint.trim_end_matches('/'), self.bucket)
        } else {
            format!("https://{}.s3.{}.amazonaws.com", self.bucket, self.region)
        }
    }

    /// Return the `Host` header value for the request.
    fn host_header(&self) -> String {
        if self.path_style {
            // Extract host from endpoint URL.
            self.endpoint
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_string()
        } else {
            format!("{}.s3.{}.amazonaws.com", self.bucket, self.region)
        }
    }

    /// Derive the AWS Signature V4 signing key.
    ///
    /// ```text
    /// kDate    = HMAC("AWS4" + secret, date)
    /// kRegion  = HMAC(kDate, region)
    /// kService = HMAC(kRegion, "s3")
    /// kSigning = HMAC(kService, "aws4_request")
    /// ```
    fn signing_key(&self, date_stamp: &str) -> Vec<u8> {
        let k_date = hmac_sha256(format!("AWS4{}", self.secret_key).as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }

    /// Sign a request using AWS Signature Version 4 and return the
    /// `Authorization` header value together with the headers that must be
    /// added to the request.
    #[allow(clippy::too_many_arguments)]
    fn sign_request(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_querystring: &str,
        payload_hash: &str,
        now: &chrono::DateTime<Utc>,
        extra_headers: &[(String, String)],
    ) -> Vec<(String, String)> {
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();
        let host = self.host_header();

        // Canonical headers (must be sorted by lowercase header name).
        let mut header_lines = vec![format!("host:{host}")];
        let mut signed_header_names: Vec<String> = vec!["host".to_string()];
        for (name, value) in extra_headers {
            let lower = name.to_ascii_lowercase();
            header_lines.push(format!("{lower}:{value}"));
            signed_header_names.push(lower);
        }
        header_lines.push(format!("x-amz-content-sha256:{payload_hash}"));
        signed_header_names.push("x-amz-content-sha256".to_string());
        header_lines.push(format!("x-amz-date:{amz_date}"));
        signed_header_names.push("x-amz-date".to_string());
        header_lines.sort();
        signed_header_names.sort();
        let canonical_headers = format!("{}\n", header_lines.join("\n"));
        let signed_headers = signed_header_names.join(";");

        // Canonical request.
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        trace!(canonical_request = %canonical_request, "S3 canonical request");

        let credential_scope = format!("{date_stamp}/{}/s3/aws4_request", self.region);

        // String to sign.
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        // Signature.
        let signing_key = self.signing_key(&date_stamp);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        vec![
            ("Host".to_string(), host),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date),
            ("Authorization".to_string(), authorization),
        ]
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn put_object(&self, key: &str, data: &[u8], content_type: &str) -> Result<()> {
        self.require_credentials("PUT")?;
        let full_key = self.full_key(key);
        let url = self.object_url(&full_key);
        let payload_hash = sha256_hex(data);
        let now = Utc::now();

        let canonical_uri = if self.path_style {
            format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
        } else {
            format!("/{}", s3_wire::encode_uri_path(&full_key))
        };

        let headers = self.sign_request("PUT", &canonical_uri, "", &payload_hash, &now, &[]);

        let mut req = self.client.put(&url).body(data.to_vec());
        req = req.header("Content-Type", content_type);
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        s3_wire::check_response_status(status, &full_key, &body)?;

        debug!(key = %full_key, "S3 PUT completed");
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);
        let url = self.object_url(&full_key);

        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let payload_hash = sha256_hex(b"");
            let now = Utc::now();
            let canonical_uri = if self.path_style {
                format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
            } else {
                format!("/{}", s3_wire::encode_uri_path(&full_key))
            };
            let headers = self.sign_request("GET", &canonical_uri, "", &payload_hash, &now, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        } else {
            req = req.header("Host", self.host_header());
        }

        let resp = req.send().await?;
        let status = resp.status();
        let bytes = resp.bytes().await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).to_string();
            s3_wire::check_response_status(status, &full_key, &body)?;
        }

        debug!(key = %full_key, size = bytes.len(), "S3 GET completed");
        Ok(bytes.to_vec())
    }

    async fn head_object(&self, key: &str) -> Result<ObjectInfo> {
        let full_key = self.full_key(key);
        let url = self.object_url(&full_key);

        let mut req = self.client.head(&url);
        if self.has_credentials() {
            let payload_hash = sha256_hex(b"");
            let now = Utc::now();
            let canonical_uri = if self.path_style {
                format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
            } else {
                format!("/{}", s3_wire::encode_uri_path(&full_key))
            };
            let headers = self.sign_request("HEAD", &canonical_uri, "", &payload_hash, &now, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        } else {
            req = req.header("Host", self.host_header());
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(SurgeError::NotFound(format!("S3 object not found: {full_key}")));
            }
            return Err(SurgeError::Storage(format!("S3 HEAD failed (HTTP {status})")));
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

        debug!(key = %full_key, size, "S3 HEAD completed");
        Ok(ObjectInfo {
            size,
            etag,
            content_type,
        })
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.require_credentials("DELETE")?;
        let full_key = self.full_key(key);
        let url = self.object_url(&full_key);
        let payload_hash = sha256_hex(b"");
        let now = Utc::now();

        let canonical_uri = if self.path_style {
            format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
        } else {
            format!("/{}", s3_wire::encode_uri_path(&full_key))
        };

        let headers = self.sign_request("DELETE", &canonical_uri, "", &payload_hash, &now, &[]);

        let mut req = self.client.delete(&url);
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        // S3 returns 204 for successful DELETE, and does not error on missing keys.
        if !status.is_success() && status != reqwest::StatusCode::NO_CONTENT {
            let body = resp.text().await.unwrap_or_default();
            s3_wire::check_response_status(status, &full_key, &body)?;
        }

        debug!(key = %full_key, "S3 DELETE completed");
        Ok(())
    }

    async fn list_objects(&self, prefix: &str, marker: Option<&str>, max_keys: i32) -> Result<ListResult> {
        let full_prefix = self.full_key(prefix);
        let bucket_url = self.bucket_url();

        // Build query string (parameters must be sorted alphabetically).
        let mut query_parts: Vec<(String, String)> = vec![
            ("list-type".to_string(), "2".to_string()),
            ("max-keys".to_string(), max_keys.to_string()),
            ("prefix".to_string(), full_prefix.clone()),
        ];
        if let Some(m) = marker {
            query_parts.push(("continuation-token".to_string(), m.to_string()));
        }
        query_parts.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_querystring = query_parts
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    s3_wire::encode_uri_component(k),
                    s3_wire::encode_uri_component(v)
                )
            })
            .collect::<Vec<_>>()
            .join("&");

        let url = format!("{bucket_url}?{canonical_querystring}");
        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let payload_hash = sha256_hex(b"");
            let now = Utc::now();
            let canonical_uri = if self.path_style {
                format!("/{}", self.bucket)
            } else {
                "/".to_string()
            };
            let headers = self.sign_request("GET", &canonical_uri, &canonical_querystring, &payload_hash, &now, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        } else {
            req = req.header("Host", self.host_header());
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        s3_wire::check_response_status(status, &full_prefix, &body)?;

        // Parse S3 ListObjectsV2 XML response.
        s3_wire::parse_list_objects_v2_xml(&body)
    }

    async fn download_to_file(&self, key: &str, dest: &Path, progress: Option<&TransferProgress<'_>>) -> Result<()> {
        let full_key = self.full_key(key);
        let url = self.object_url(&full_key);

        let mut req = self.client.get(&url);
        if self.has_credentials() {
            let payload_hash = sha256_hex(b"");
            let now = Utc::now();
            let canonical_uri = if self.path_style {
                format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
            } else {
                format!("/{}", s3_wire::encode_uri_path(&full_key))
            };
            let headers = self.sign_request("GET", &canonical_uri, "", &payload_hash, &now, &[]);
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        } else {
            req = req.header("Host", self.host_header());
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return s3_wire::check_response_status(status, &full_key, &body);
        }

        download_response_to_file(resp, dest, progress).await?;

        debug!(key = %full_key, dest = %dest.display(), "S3 download completed");
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
        let url = self.object_url(&full_key);
        let range_header = format!("bytes={offset}-");

        let mut req = self
            .client
            .get(&url)
            .header(reqwest::header::RANGE, range_header.as_str());
        if self.has_credentials() {
            let payload_hash = sha256_hex(b"");
            let now = Utc::now();
            let canonical_uri = if self.path_style {
                format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
            } else {
                format!("/{}/", s3_wire::encode_uri_path(&full_key))
            };
            let headers = self.sign_request(
                "GET",
                &canonical_uri,
                "",
                &payload_hash,
                &now,
                &[("range".to_string(), range_header.clone())],
            );
            for (name, value) in &headers {
                req = req.header(name.as_str(), value.as_str());
            }
        } else {
            req = req.header("Host", self.host_header());
        }

        let resp = req.send().await?;
        let status = resp.status();
        match resumable_download_response_action(status) {
            ResumableDownloadResponseAction::AppendFromOffset => {
                download_response_to_file_from_offset(resp, dest, offset, progress).await?;
            }
            ResumableDownloadResponseAction::RestartFromZero => {
                debug!(
                    key = %full_key,
                    dest = %dest.display(),
                    offset,
                    status = %status,
                    "S3 endpoint ignored resumable range request; restarting download from byte zero"
                );
                download_response_to_file(resp, dest, progress).await?;
            }
            ResumableDownloadResponseAction::Error => {
                let body = resp.text().await.unwrap_or_default();
                return s3_wire::check_response_status(status, &full_key, &body);
            }
        }

        debug!(key = %full_key, dest = %dest.display(), offset, "S3 resumable download completed");
        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress<'_>>) -> Result<()> {
        self.require_credentials("upload")?;
        let data = tokio::fs::read(src).await?;
        let total = data.len() as u64;

        let full_key = self.full_key(key);
        let url = self.object_url(&full_key);
        let payload_hash = sha256_hex(&data);
        let now = Utc::now();

        let canonical_uri = if self.path_style {
            format!("/{}/{}", self.bucket, s3_wire::encode_uri_path(&full_key))
        } else {
            format!("/{}", s3_wire::encode_uri_path(&full_key))
        };

        let headers = self.sign_request("PUT", &canonical_uri, "", &payload_hash, &now, &[]);

        let mut req = self.client.put(&url).body(data);
        req = req.header("Content-Type", "application/octet-stream");
        for (name, value) in &headers {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        s3_wire::check_response_status(status, &full_key, &body)?;

        if let Some(cb) = progress {
            cb(total, total);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resumable_download_response_action_maps_statuses() {
        assert_eq!(
            resumable_download_response_action(reqwest::StatusCode::PARTIAL_CONTENT),
            ResumableDownloadResponseAction::AppendFromOffset
        );
        assert_eq!(
            resumable_download_response_action(reqwest::StatusCode::OK),
            ResumableDownloadResponseAction::RestartFromZero
        );
        assert_eq!(
            resumable_download_response_action(reqwest::StatusCode::FORBIDDEN),
            ResumableDownloadResponseAction::Error
        );
        assert_eq!(
            resumable_download_response_action(reqwest::StatusCode::NOT_FOUND),
            ResumableDownloadResponseAction::Error
        );
    }
}
