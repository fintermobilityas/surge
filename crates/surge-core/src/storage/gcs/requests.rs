use std::path::Path;

use async_trait::async_trait;
use percent_encoding::utf8_percent_encode;
use tracing::debug;

use crate::context::StorageConfig;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};
use crate::storage::{ListResult, ObjectInfo, StorageBackend, TransferProgress, download_response_to_file};

use super::auth::auth_mode_from_config;
use super::json_listing::parse_gcs_json_list_response;
use super::xml_listing::parse_gcs_xml_list_response;
use super::{
    AuthMode, GCS_JSON_ENDPOINT, GCS_UPLOAD_ENDPOINT, GCS_XML_ENDPOINT, GcsBackend, URI_ENCODE_PATH_SET, URI_ENCODE_SET,
};

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

        let auth = auth_mode_from_config(config);
        let endpoint = if config.endpoint.is_empty() {
            GCS_XML_ENDPOINT.to_string()
        } else {
            config.endpoint.clone()
        };

        let client = crate::net::http_client()?;

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
    pub(super) fn xml_host(&self) -> String {
        self.endpoint
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string()
    }

    /// URI-encode a path (preserves '/').
    fn encode_uri_path(path: &str) -> String {
        utf8_percent_encode(path, URI_ENCODE_PATH_SET).to_string()
    }

    /// URI-encode a query parameter value.
    fn encode_uri_component(value: &str) -> String {
        utf8_percent_encode(value, URI_ENCODE_SET).to_string()
    }

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

    async fn download_to_file(&self, key: &str, dest: &Path, progress: Option<&TransferProgress<'_>>) -> Result<()> {
        let full_key = self.full_key(key);

        let resp = match &self.auth {
            AuthMode::Anonymous | AuthMode::Hmac { .. } => {
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
                req.send().await?
            }
            AuthMode::OAuth2 { token } => {
                let url = self.json_object_url(&full_key);
                let bearer = token.read().await.clone();

                self.client
                    .get(&url)
                    .query(&[("alt", "media")])
                    .header("Authorization", format!("Bearer {bearer}"))
                    .send()
                    .await?
            }
        };

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Self::check_response_status(status, &full_key, &body);
        }

        download_response_to_file(resp, dest, progress).await?;

        debug!(key = %full_key, dest = %dest.display(), "GCS download completed");
        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress<'_>>) -> Result<()> {
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
