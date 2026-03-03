//! Google Cloud Storage backend with HMAC (S3-interop XML API) and OAuth2 bearer token auth.

use async_trait::async_trait;
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Client;
use tokio::sync::RwLock;
use tracing::{debug, trace};

use crate::context::StorageConfig;
use crate::crypto::hmac_sha256::hmac_sha256;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult, ObjectInfo, StorageBackend, TransferProgress};

/// Characters that must NOT be percent-encoded in URI paths.
const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// Same set but preserves '/'.
const URI_ENCODE_PATH_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// GCS XML API endpoint (S3-interop).
const GCS_XML_ENDPOINT: &str = "https://storage.googleapis.com";

/// GCS JSON API endpoint (used for OAuth2 bearer-token auth).
const GCS_JSON_ENDPOINT: &str = "https://storage.googleapis.com/storage/v1";

/// GCS JSON API upload endpoint.
const GCS_UPLOAD_ENDPOINT: &str = "https://storage.googleapis.com/upload/storage/v1";

/// Authentication mode for GCS.
#[derive(Debug, Clone)]
enum AuthMode {
    /// No credentials — only anonymous (public) reads are possible.
    Anonymous,
    /// HMAC-based authentication using the S3-interop XML API.
    Hmac { access_key: String, secret_key: String },
    /// OAuth2 bearer token authentication using the JSON API.
    OAuth2 {
        /// Current bearer token. Wrapped in `Arc<RwLock>` for interior
        /// mutability across `&self` calls and thread safety.
        token: Arc<RwLock<String>>,
    },
}

/// Google Cloud Storage backend.
pub struct GcsBackend {
    client: Client,
    bucket: String,
    prefix: String,
    endpoint: String,
    auth: AuthMode,
}

impl GcsBackend {
    /// Create a new GCS backend from configuration.
    ///
    /// Authentication mode is inferred:
    /// - If `access_key` and `secret_key` are both set, HMAC mode is used
    ///   (S3-interop XML API).
    /// - If only `secret_key` is set, it is treated as an OAuth2 bearer token
    ///   (JSON API). The token can also be sourced from the `GOOGLE_ACCESS_TOKEN`
    ///   environment variable.
    pub fn new(config: &StorageConfig) -> Result<Self> {
        if config.bucket.is_empty() {
            return Err(SurgeError::Config("GCS storage requires a bucket name".to_string()));
        }

        let auth = if !config.access_key.is_empty() && !config.secret_key.is_empty() {
            AuthMode::Hmac {
                access_key: config.access_key.clone(),
                secret_key: config.secret_key.clone(),
            }
        } else {
            // Try secret_key first, then environment variable.
            let token = if config.secret_key.is_empty() {
                std::env::var("GOOGLE_ACCESS_TOKEN").unwrap_or_default()
            } else {
                config.secret_key.clone()
            };
            if token.is_empty() {
                AuthMode::Anonymous
            } else {
                AuthMode::OAuth2 {
                    token: Arc::new(RwLock::new(token)),
                }
            }
        };

        let endpoint = if config.endpoint.is_empty() {
            GCS_XML_ENDPOINT.to_string()
        } else {
            config.endpoint.clone()
        };

        let client = Client::builder()
            .build()
            .map_err(|e| SurgeError::Storage(format!("Failed to build HTTP client: {e}")))?;

        debug!(
            bucket = %config.bucket,
            auth_mode = match &auth {
                AuthMode::Anonymous => "Anonymous",
                AuthMode::Hmac { .. } => "HMAC",
                AuthMode::OAuth2 { .. } => "OAuth2",
            },
            endpoint = %endpoint,
            "GCS backend initialized"
        );

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            endpoint,
            auth,
        })
    }

    /// Update the OAuth2 bearer token (for token refresh).
    pub async fn set_oauth2_token(&self, new_token: &str) -> Result<()> {
        match &self.auth {
            AuthMode::OAuth2 { token } => {
                let mut t = token.write().await;
                *t = new_token.to_string();
                Ok(())
            }
            AuthMode::Anonymous | AuthMode::Hmac { .. } => Err(SurgeError::Config(
                "Cannot set OAuth2 token on non-OAuth2 backend".to_string(),
            )),
        }
    }

    /// Return an error when a write is attempted without credentials.
    fn require_credentials(&self, operation: &str) -> Result<()> {
        match &self.auth {
            AuthMode::Anonymous => Err(SurgeError::Config(format!("GCS {operation} requires credentials"))),
            _ => Ok(()),
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

    /// Map an HTTP response status to a `SurgeError`.
    fn check_response_status(status: reqwest::StatusCode, key: &str, body: &str) -> Result<()> {
        if status.is_success() {
            return Ok(());
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(SurgeError::NotFound(format!("GCS object not found: {key}")));
        }
        Err(SurgeError::Storage(format!(
            "GCS request failed (HTTP {status}): {body}"
        )))
    }

    // -----------------------------------------------------------------------
    // HMAC (S3-interop XML API) helpers
    // -----------------------------------------------------------------------

    /// Build the URL for an object using the XML API (path-style).
    fn xml_object_url(&self, key: &str) -> String {
        let encoded = utf8_percent_encode(key, URI_ENCODE_PATH_SET);
        format!("{}/{}/{}", self.endpoint.trim_end_matches('/'), self.bucket, encoded)
    }

    /// Build the bucket-level URL (XML API).
    fn xml_bucket_url(&self) -> String {
        format!("{}/{}", self.endpoint.trim_end_matches('/'), self.bucket)
    }

    /// Host for the XML API endpoint.
    fn xml_host(&self) -> String {
        self.endpoint
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string()
    }

    /// Derive HMAC signing key, similar to AWS Signature V4 but for GCS.
    /// GCS HMAC keys use the same signing algorithm as AWS SigV4.
    fn hmac_signing_key(secret_key: &str, date_stamp: &str, region: &str) -> Vec<u8> {
        let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }

    /// Sign a request using HMAC (SigV4-compatible) for the GCS XML API.
    fn hmac_sign_request(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_querystring: &str,
        payload_hash: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Vec<(String, String)> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();
        let host = self.xml_host();
        // GCS HMAC uses "auto" as the region for the signing scope.
        let region = "auto";

        let canonical_headers = format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        trace!(canonical_request = %canonical_request, "GCS HMAC canonical request");

        let credential_scope = format!("{date_stamp}/{region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        let signing_key = Self::hmac_signing_key(secret_key, &date_stamp, region);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        );

        vec![
            ("Host".to_string(), host),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date),
            ("Authorization".to_string(), authorization),
        ]
    }

    /// URI-encode a path (preserves '/').
    fn encode_uri_path(path: &str) -> String {
        utf8_percent_encode(path, URI_ENCODE_PATH_SET).to_string()
    }

    /// URI-encode a query parameter value.
    fn encode_uri_component(value: &str) -> String {
        utf8_percent_encode(value, URI_ENCODE_SET).to_string()
    }

    // -----------------------------------------------------------------------
    // OAuth2 (JSON API) helpers
    // -----------------------------------------------------------------------

    /// Build the JSON API URL for an object.
    fn json_object_url(&self, key: &str) -> String {
        let encoded = Self::encode_uri_component(key);
        format!("{}/b/{}/o/{}", GCS_JSON_ENDPOINT, self.bucket, encoded)
    }

    /// Build the JSON API URL for listing objects.
    fn json_list_url(&self) -> String {
        format!("{}/b/{}/o", GCS_JSON_ENDPOINT, self.bucket)
    }

    /// Build the JSON API upload URL (media upload).
    fn json_upload_url(&self) -> String {
        format!("{}/b/{}/o", GCS_UPLOAD_ENDPOINT, self.bucket)
    }
}

#[async_trait]
impl StorageBackend for GcsBackend {
    async fn put_object(&self, key: &str, data: &[u8], content_type: &str) -> Result<()> {
        self.require_credentials("PUT")?;
        let full_key = self.full_key(key);

        match &self.auth {
            AuthMode::Anonymous => return Err(SurgeError::Config("GCS PUT requires credentials".to_string())),
            AuthMode::Hmac { access_key, secret_key } => {
                let url = self.xml_object_url(&full_key);
                let payload_hash = sha256_hex(data);
                let canonical_uri = format!("/{}/{}", self.bucket, Self::encode_uri_path(&full_key));

                let headers = self.hmac_sign_request("PUT", &canonical_uri, "", &payload_hash, access_key, secret_key);

                let mut req = self.client.put(&url).body(data.to_vec());
                req = req.header("Content-Type", content_type);
                for (name, value) in &headers {
                    req = req.header(name.as_str(), value.as_str());
                }

                let resp = req.send().await?;
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Self::check_response_status(status, &full_key, &body)?;
            }
            AuthMode::OAuth2 { token } => {
                let url = self.json_upload_url();
                let bearer = token.read().await.clone();

                let resp = self
                    .client
                    .post(&url)
                    .query(&[("uploadType", "media"), ("name", &full_key)])
                    .header("Authorization", format!("Bearer {bearer}"))
                    .header("Content-Type", content_type)
                    .body(data.to_vec())
                    .send()
                    .await?;

                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Self::check_response_status(status, &full_key, &body)?;
            }
        }

        debug!(key = %full_key, "GCS PUT completed");
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);

        let bytes = match &self.auth {
            AuthMode::Anonymous | AuthMode::Hmac { .. } => {
                // Both Anonymous and HMAC use the XML API; only signing differs.
                let url = self.xml_object_url(&full_key);
                let mut req = self.client.get(&url);
                if let AuthMode::Hmac { access_key, secret_key } = &self.auth {
                    let payload_hash = sha256_hex(b"");
                    let canonical_uri = format!("/{}/{}", self.bucket, Self::encode_uri_path(&full_key));
                    let headers =
                        self.hmac_sign_request("GET", &canonical_uri, "", &payload_hash, access_key, secret_key);
                    for (name, value) in &headers {
                        req = req.header(name.as_str(), value.as_str());
                    }
                } else {
                    req = req.header("Host", self.xml_host());
                }

                let resp = req.send().await?;
                let status = resp.status();
                let bytes = resp.bytes().await?;
                if !status.is_success() {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    Self::check_response_status(status, &full_key, &body)?;
                }
                bytes
            }
            AuthMode::OAuth2 { token } => {
                let url = self.json_object_url(&full_key);
                let bearer = token.read().await.clone();

                let resp = self
                    .client
                    .get(&url)
                    .query(&[("alt", "media")])
                    .header("Authorization", format!("Bearer {bearer}"))
                    .send()
                    .await?;

                let status = resp.status();
                let bytes = resp.bytes().await?;
                if !status.is_success() {
                    let body = String::from_utf8_lossy(&bytes).to_string();
                    Self::check_response_status(status, &full_key, &body)?;
                }
                bytes
            }
        };

        debug!(key = %full_key, size = bytes.len(), "GCS GET completed");
        Ok(bytes.to_vec())
    }

    async fn head_object(&self, key: &str) -> Result<ObjectInfo> {
        let full_key = self.full_key(key);

        match &self.auth {
            AuthMode::Anonymous | AuthMode::Hmac { .. } => {
                // Both Anonymous and HMAC use the XML API; only signing differs.
                let url = self.xml_object_url(&full_key);
                let mut req = self.client.head(&url);
                if let AuthMode::Hmac { access_key, secret_key } = &self.auth {
                    let payload_hash = sha256_hex(b"");
                    let canonical_uri = format!("/{}/{}", self.bucket, Self::encode_uri_path(&full_key));
                    let headers =
                        self.hmac_sign_request("HEAD", &canonical_uri, "", &payload_hash, access_key, secret_key);
                    for (name, value) in &headers {
                        req = req.header(name.as_str(), value.as_str());
                    }
                } else {
                    req = req.header("Host", self.xml_host());
                }

                let resp = req.send().await?;
                let status = resp.status();
                if !status.is_success() {
                    if status == reqwest::StatusCode::NOT_FOUND {
                        return Err(SurgeError::NotFound(format!("GCS object not found: {full_key}")));
                    }
                    return Err(SurgeError::Storage(format!("GCS HEAD failed (HTTP {status})")));
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

                debug!(key = %full_key, size, "GCS HEAD completed");
                Ok(ObjectInfo {
                    size,
                    etag,
                    content_type,
                })
            }
            AuthMode::OAuth2 { token } => {
                // JSON API: GET object metadata.
                let url = self.json_object_url(&full_key);
                let bearer = token.read().await.clone();

                let resp = self
                    .client
                    .get(&url)
                    .header("Authorization", format!("Bearer {bearer}"))
                    .send()
                    .await?;

                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if !status.is_success() {
                    Self::check_response_status(status, &full_key, &body)?;
                }

                let json: serde_json::Value = serde_json::from_str(&body)
                    .map_err(|e| SurgeError::Storage(format!("Failed to parse GCS metadata response: {e}")))?;

                let size = json
                    .get("size")
                    .and_then(|v| v.as_str())
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);

                let etag = json.get("etag").and_then(|v| v.as_str()).unwrap_or("").to_string();

                let content_type = json
                    .get("contentType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("application/octet-stream")
                    .to_string();

                debug!(key = %full_key, size, "GCS HEAD completed (OAuth2)");
                Ok(ObjectInfo {
                    size,
                    etag,
                    content_type,
                })
            }
        }
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.require_credentials("DELETE")?;
        let full_key = self.full_key(key);

        match &self.auth {
            AuthMode::Anonymous => return Err(SurgeError::Config("GCS DELETE requires credentials".to_string())),
            AuthMode::Hmac { access_key, secret_key } => {
                let url = self.xml_object_url(&full_key);
                let payload_hash = sha256_hex(b"");
                let canonical_uri = format!("/{}/{}", self.bucket, Self::encode_uri_path(&full_key));

                let headers =
                    self.hmac_sign_request("DELETE", &canonical_uri, "", &payload_hash, access_key, secret_key);

                let mut req = self.client.delete(&url);
                for (name, value) in &headers {
                    req = req.header(name.as_str(), value.as_str());
                }

                let resp = req.send().await?;
                let status = resp.status();
                if !status.is_success() && status != reqwest::StatusCode::NO_CONTENT {
                    let body = resp.text().await.unwrap_or_default();
                    // Don't error on 404 - object already deleted.
                    if status != reqwest::StatusCode::NOT_FOUND {
                        Self::check_response_status(status, &full_key, &body)?;
                    }
                }
            }
            AuthMode::OAuth2 { token } => {
                let url = self.json_object_url(&full_key);
                let bearer = token.read().await.clone();

                let resp = self
                    .client
                    .delete(&url)
                    .header("Authorization", format!("Bearer {bearer}"))
                    .send()
                    .await?;

                let status = resp.status();
                if !status.is_success()
                    && status != reqwest::StatusCode::NO_CONTENT
                    && status != reqwest::StatusCode::NOT_FOUND
                {
                    let body = resp.text().await.unwrap_or_default();
                    Self::check_response_status(status, &full_key, &body)?;
                }
            }
        }

        debug!(key = %full_key, "GCS DELETE completed");
        Ok(())
    }

    async fn list_objects(&self, prefix: &str, marker: Option<&str>, max_keys: i32) -> Result<ListResult> {
        let full_prefix = self.full_key(prefix);

        match &self.auth {
            AuthMode::Anonymous | AuthMode::Hmac { .. } => {
                // Both Anonymous and HMAC use the XML API; only signing differs.
                let bucket_url = self.xml_bucket_url();

                let mut query_parts: Vec<(String, String)> = vec![
                    ("max-keys".to_string(), max_keys.to_string()),
                    ("prefix".to_string(), full_prefix.clone()),
                ];
                if let Some(m) = marker {
                    query_parts.push(("marker".to_string(), m.to_string()));
                }
                query_parts.sort_by(|a, b| a.0.cmp(&b.0));

                let canonical_querystring = query_parts
                    .iter()
                    .map(|(k, v)| format!("{}={}", Self::encode_uri_component(k), Self::encode_uri_component(v)))
                    .collect::<Vec<_>>()
                    .join("&");

                let url = format!("{bucket_url}?{canonical_querystring}");
                let mut req = self.client.get(&url);
                if let AuthMode::Hmac { access_key, secret_key } = &self.auth {
                    let payload_hash = sha256_hex(b"");
                    let canonical_uri = format!("/{}", self.bucket);
                    let headers = self.hmac_sign_request(
                        "GET",
                        &canonical_uri,
                        &canonical_querystring,
                        &payload_hash,
                        access_key,
                        secret_key,
                    );
                    for (name, value) in &headers {
                        req = req.header(name.as_str(), value.as_str());
                    }
                } else {
                    req = req.header("Host", self.xml_host());
                }

                let resp = req.send().await?;
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Self::check_response_status(status, &full_prefix, &body)?;

                parse_gcs_xml_list_response(&body)
            }
            AuthMode::OAuth2 { token } => {
                let url = self.json_list_url();
                let bearer = token.read().await.clone();

                let mut query: Vec<(&str, String)> =
                    vec![("prefix", full_prefix.clone()), ("maxResults", max_keys.to_string())];
                if let Some(m) = marker {
                    query.push(("pageToken", m.to_string()));
                }

                let resp = self
                    .client
                    .get(&url)
                    .query(&query)
                    .header("Authorization", format!("Bearer {bearer}"))
                    .send()
                    .await?;

                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Self::check_response_status(status, &full_prefix, &body)?;

                parse_gcs_json_list_response(&body)
            }
        }
    }

    async fn download_to_file(&self, key: &str, dest: &Path, progress: Option<&TransferProgress>) -> Result<()> {
        let data = self.get_object(key).await?;
        let total = data.len() as u64;

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(dest, &data).await?;

        if let Some(cb) = progress {
            cb(total, total);
        }

        debug!(key = %self.full_key(key), dest = %dest.display(), "GCS download completed");
        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress>) -> Result<()> {
        self.require_credentials("upload")?;
        let data = tokio::fs::read(src).await?;
        let total = data.len() as u64;

        let full_key = self.full_key(key);
        match &self.auth {
            AuthMode::Anonymous => return Err(SurgeError::Config("GCS upload requires credentials".to_string())),
            AuthMode::Hmac { access_key, secret_key } => {
                let url = self.xml_object_url(&full_key);
                let payload_hash = sha256_hex(&data);
                let canonical_uri = format!("/{}/{}", self.bucket, Self::encode_uri_path(&full_key));

                let headers = self.hmac_sign_request("PUT", &canonical_uri, "", &payload_hash, access_key, secret_key);

                let mut req = self.client.put(&url).body(data);
                req = req.header("Content-Type", "application/octet-stream");
                for (name, value) in &headers {
                    req = req.header(name.as_str(), value.as_str());
                }

                let resp = req.send().await?;
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Self::check_response_status(status, &full_key, &body)?;
            }
            AuthMode::OAuth2 { token } => {
                let url = self.json_upload_url();
                let bearer = token.read().await.clone();

                let resp = self
                    .client
                    .post(&url)
                    .query(&[("uploadType", "media"), ("name", &full_key)])
                    .header("Authorization", format!("Bearer {bearer}"))
                    .header("Content-Type", "application/octet-stream")
                    .body(data)
                    .send()
                    .await?;

                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                Self::check_response_status(status, &full_key, &body)?;
            }
        }

        if let Some(cb) = progress {
            cb(total, total);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// XML parsing for GCS XML API (S3-compatible ListBucketResult)
// ---------------------------------------------------------------------------

/// Parse a GCS XML API ListBucketResult into a `ListResult`.
/// The format matches S3's `ListBucketResult` (v1).
fn parse_gcs_xml_list_response(xml: &str) -> Result<ListResult> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    let mut next_marker: Option<String> = None;
    let mut is_truncated = false;

    let mut in_contents = false;
    let mut current_key: Option<String> = None;
    let mut current_size: Option<i64> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag == "Contents" {
                    in_contents = true;
                    current_key = None;
                    current_size = None;
                }
                current_tag = tag;
            }
            Ok(Event::End(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag == "Contents" {
                    if let Some(key) = current_key.take() {
                        entries.push(ListEntry {
                            key,
                            size: current_size.unwrap_or(0),
                        });
                    }
                    in_contents = false;
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).to_string();
                if in_contents {
                    match current_tag.as_str() {
                        "Key" => current_key = Some(text),
                        "Size" => current_size = text.parse::<i64>().ok(),
                        _ => {}
                    }
                } else {
                    match current_tag.as_str() {
                        "IsTruncated" => is_truncated = text == "true",
                        "NextMarker" => next_marker = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(SurgeError::Storage(format!(
                    "Failed to parse GCS XML list response: {e}"
                )));
            }
            _ => {}
        }
        buf.clear();
    }

    debug!(count = entries.len(), is_truncated, "GCS XML LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}

// ---------------------------------------------------------------------------
// JSON parsing for GCS JSON API
// ---------------------------------------------------------------------------

/// Parse a GCS JSON API list-objects response into a `ListResult`.
///
/// Response format:
/// ```json
/// {
///   "items": [{"name": "key", "size": "123"}, ...],
///   "nextPageToken": "..."
/// }
/// ```
fn parse_gcs_json_list_response(json_str: &str) -> Result<ListResult> {
    let json: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| SurgeError::Storage(format!("Failed to parse GCS JSON list response: {e}")))?;

    let mut entries = Vec::new();
    if let Some(items) = json.get("items").and_then(|v| v.as_array()) {
        for item in items {
            let key = item.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let size = item
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            if !key.is_empty() {
                entries.push(ListEntry { key, size });
            }
        }
    }

    let next_marker = json.get("nextPageToken").and_then(|v| v.as_str()).map(String::from);
    let is_truncated = next_marker.is_some();

    debug!(count = entries.len(), is_truncated, "GCS JSON LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}
