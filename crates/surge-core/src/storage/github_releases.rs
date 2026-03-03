//! GitHub Releases storage backend.
//!
//! This backend maps storage object keys to assets in a single GitHub Release.
//! Object keys are encoded into safe asset names and uploaded/downloaded via
//! the GitHub REST API.

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::Deserialize;
use std::path::Path;

use crate::context::StorageConfig;
use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult, ObjectInfo, StorageBackend, TransferProgress};

const DEFAULT_GITHUB_API_BASE: &str = "https://api.github.com";
const DEFAULT_RELEASE_TAG: &str = "surge";
const GITHUB_API_VERSION: &str = "2022-11-28";
const ASSET_NAME_PREFIX: &str = "surge-obj-";

/// GitHub Releases storage backend.
pub struct GitHubReleasesBackend {
    client: Client,
    owner: String,
    repo: String,
    release_tag: String,
    token: String,
    api_base: String,
    prefix: String,
}

#[derive(Debug, Deserialize, Clone)]
struct GitHubAsset {
    id: i64,
    name: String,
    size: i64,
    #[serde(default)]
    content_type: String,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    id: i64,
    upload_url: String,
}

#[derive(Debug, Deserialize)]
struct GitHubApiError {
    message: String,
}

impl GitHubReleasesBackend {
    /// Create a new GitHub Releases backend from configuration.
    ///
    /// Configuration mapping:
    /// - `bucket` => repository in `owner/repo` form (required)
    /// - `region` => release tag (optional, defaults to `surge`)
    /// - `secret_key` or `access_key` => GitHub token (optional for public read)
    /// - `endpoint` => API base (optional, defaults to `https://api.github.com`)
    /// - `prefix` => object key prefix
    pub fn new(config: &StorageConfig) -> Result<Self> {
        let (owner, repo) = parse_repository(&config.bucket)?;
        let token = resolve_token(config);
        let release_tag = if config.region.trim().is_empty() {
            DEFAULT_RELEASE_TAG.to_string()
        } else {
            config.region.trim().to_string()
        };
        let api_base = if config.endpoint.trim().is_empty() {
            DEFAULT_GITHUB_API_BASE.to_string()
        } else {
            config.endpoint.trim().trim_end_matches('/').to_string()
        };

        let client = Client::builder()
            .user_agent("surge/0.1")
            .build()
            .map_err(|e| SurgeError::Storage(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            client,
            owner,
            repo,
            release_tag,
            token,
            api_base,
            prefix: config.prefix.clone(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), key)
        }
    }

    fn api_url(&self, suffix: &str) -> String {
        format!("{}/{}", self.api_base, suffix.trim_start_matches('/'))
    }

    fn apply_common_headers(&self, req: RequestBuilder) -> RequestBuilder {
        let req = req
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION);

        if self.token.is_empty() {
            req
        } else {
            req.bearer_auth(&self.token)
        }
    }

    fn apply_binary_headers(&self, req: RequestBuilder) -> RequestBuilder {
        let req = req
            .header("Accept", "application/octet-stream")
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION);

        if self.token.is_empty() {
            req
        } else {
            req.bearer_auth(&self.token)
        }
    }

    fn require_token(&self) -> Result<()> {
        if self.token.is_empty() {
            return Err(SurgeError::Config(
                "GitHub Releases write operations require a token. Set secret_key/access_key or GITHUB_TOKEN/GH_TOKEN"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn object_key_to_asset_name(key: &str) -> String {
        format!("{ASSET_NAME_PREFIX}{}", URL_SAFE_NO_PAD.encode(key.as_bytes()))
    }

    fn asset_name_to_object_key(asset_name: &str) -> Option<String> {
        let encoded = asset_name.strip_prefix(ASSET_NAME_PREFIX)?;
        let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        String::from_utf8(bytes).ok()
    }

    fn parse_api_error_message(body: &str) -> String {
        serde_json::from_str::<GitHubApiError>(body).map_or_else(|_| body.to_string(), |e| e.message)
    }

    async fn get_release_by_tag(&self) -> Result<GitHubRelease> {
        let url = self.api_url(&format!(
            "repos/{}/{}/releases/tags/{}",
            self.owner, self.repo, self.release_tag
        ));
        let req = self.apply_common_headers(self.client.get(url));
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if status == StatusCode::NOT_FOUND {
            return Err(SurgeError::NotFound(format!(
                "GitHub release tag '{}' not found in {}/{}",
                self.release_tag, self.owner, self.repo
            )));
        }
        if !status.is_success() {
            let message = Self::parse_api_error_message(&body);
            return Err(SurgeError::Storage(format!(
                "GitHub API release lookup failed (HTTP {status}): {message}"
            )));
        }

        serde_json::from_str(&body).map_err(|e| SurgeError::Storage(format!("Invalid GitHub release response: {e}")))
    }

    async fn create_release(&self) -> Result<GitHubRelease> {
        self.require_token()?;

        let url = self.api_url(&format!("repos/{}/{}/releases", self.owner, self.repo));
        let payload = serde_json::json!({
            "tag_name": self.release_tag,
            "name": self.release_tag,
            "draft": false,
            "prerelease": false,
        });

        let req = self.apply_common_headers(self.client.post(url)).json(&payload);
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if status == StatusCode::UNPROCESSABLE_ENTITY {
            // Likely already exists due to race; fetch by tag.
            return self.get_release_by_tag().await;
        }
        if !status.is_success() {
            let message = Self::parse_api_error_message(&body);
            return Err(SurgeError::Storage(format!(
                "GitHub API create release failed (HTTP {status}): {message}"
            )));
        }

        serde_json::from_str(&body)
            .map_err(|e| SurgeError::Storage(format!("Invalid GitHub release create response: {e}")))
    }

    async fn get_or_create_release(&self) -> Result<GitHubRelease> {
        match self.get_release_by_tag().await {
            Ok(release) => Ok(release),
            Err(SurgeError::NotFound(_)) => self.create_release().await,
            Err(e) => Err(e),
        }
    }

    async fn list_assets_for_release(&self, release_id: i64) -> Result<Vec<GitHubAsset>> {
        let mut assets = Vec::new();
        let mut page = 1;

        loop {
            let url = self.api_url(&format!(
                "repos/{}/{}/releases/{release_id}/assets",
                self.owner, self.repo
            ));

            let req = self
                .apply_common_headers(self.client.get(url))
                .query(&[("per_page", "100"), ("page", &page.to_string())]);

            let resp = req.send().await?;
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            if !status.is_success() {
                let message = Self::parse_api_error_message(&body);
                return Err(SurgeError::Storage(format!(
                    "GitHub API list assets failed (HTTP {status}): {message}"
                )));
            }

            let page_assets: Vec<GitHubAsset> = serde_json::from_str(&body)
                .map_err(|e| SurgeError::Storage(format!("Invalid GitHub assets response: {e}")))?;

            let page_count = page_assets.len();
            assets.extend(page_assets);

            if page_count < 100 {
                break;
            }
            page += 1;
        }

        Ok(assets)
    }

    async fn find_asset_by_full_key(&self, full_key: &str) -> Result<GitHubAsset> {
        let release = self.get_release_by_tag().await?;
        let assets = self.list_assets_for_release(release.id).await?;
        let expected_name = Self::object_key_to_asset_name(full_key);

        assets
            .into_iter()
            .find(|asset| asset.name == expected_name)
            .ok_or_else(|| SurgeError::NotFound(format!("GitHub release asset not found: {full_key}")))
    }

    async fn delete_asset_by_id(&self, asset_id: i64) -> Result<()> {
        self.require_token()?;

        let url = self.api_url(&format!(
            "repos/{}/{}/releases/assets/{asset_id}",
            self.owner, self.repo
        ));
        let req = self.apply_common_headers(self.client.delete(url));
        let resp = req.send().await?;
        let status = resp.status();

        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }

        let body = resp.text().await.unwrap_or_default();
        let message = Self::parse_api_error_message(&body);
        Err(SurgeError::Storage(format!(
            "GitHub API delete asset failed (HTTP {status}): {message}"
        )))
    }

    async fn upload_asset(
        &self,
        release: &GitHubRelease,
        asset_name: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> Result<()> {
        self.require_token()?;

        let upload_url = release
            .upload_url
            .split('{')
            .next()
            .unwrap_or(&release.upload_url)
            .to_string();

        let req = self
            .apply_common_headers(self.client.post(upload_url))
            .query(&[("name", asset_name)])
            .header("Content-Type", content_type)
            .body(data);

        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if !status.is_success() {
            let message = Self::parse_api_error_message(&body);
            return Err(SurgeError::Storage(format!(
                "GitHub API upload asset failed (HTTP {status}): {message}"
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl StorageBackend for GitHubReleasesBackend {
    async fn put_object(&self, key: &str, data: &[u8], content_type: &str) -> Result<()> {
        let full_key = self.full_key(key);
        let release = self.get_or_create_release().await?;
        let asset_name = Self::object_key_to_asset_name(&full_key);

        let assets = self.list_assets_for_release(release.id).await?;
        if let Some(existing) = assets.into_iter().find(|asset| asset.name == asset_name) {
            self.delete_asset_by_id(existing.id).await?;
        }

        self.upload_asset(&release, &asset_name, data.to_vec(), content_type)
            .await
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let full_key = self.full_key(key);
        let asset = self.find_asset_by_full_key(&full_key).await?;

        let url = self.api_url(&format!(
            "repos/{}/{}/releases/assets/{}",
            self.owner, self.repo, asset.id
        ));
        let req = self.apply_binary_headers(self.client.get(url));

        let resp = req.send().await?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Err(SurgeError::NotFound(format!(
                "GitHub release asset not found: {full_key}"
            )));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let message = Self::parse_api_error_message(&body);
            return Err(SurgeError::Storage(format!(
                "GitHub API download asset failed (HTTP {status}): {message}"
            )));
        }

        let bytes = resp.bytes().await?;
        Ok(bytes.to_vec())
    }

    async fn head_object(&self, key: &str) -> Result<ObjectInfo> {
        let full_key = self.full_key(key);
        let asset = self.find_asset_by_full_key(&full_key).await?;
        Ok(ObjectInfo {
            size: asset.size,
            etag: asset.id.to_string(),
            content_type: asset.content_type,
        })
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let full_key = self.full_key(key);
        match self.find_asset_by_full_key(&full_key).await {
            Ok(asset) => self.delete_asset_by_id(asset.id).await,
            Err(SurgeError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn list_objects(&self, prefix: &str, marker: Option<&str>, max_keys: i32) -> Result<ListResult> {
        let full_prefix = self.full_key(prefix);

        let release = match self.get_release_by_tag().await {
            Ok(release) => release,
            Err(SurgeError::NotFound(_)) => return Ok(ListResult::default()),
            Err(e) => return Err(e),
        };

        let assets = self.list_assets_for_release(release.id).await?;

        let mut all_entries: Vec<ListEntry> = assets
            .into_iter()
            .filter_map(|asset| {
                let key = Self::asset_name_to_object_key(&asset.name)?;
                if key.starts_with(&full_prefix) {
                    Some(ListEntry { key, size: asset.size })
                } else {
                    None
                }
            })
            .collect();

        all_entries.sort_by(|a, b| a.key.cmp(&b.key));

        let start_idx = marker
            .and_then(|m| all_entries.iter().position(|entry| entry.key.as_str() > m))
            .unwrap_or_else(|| if marker.is_some() { all_entries.len() } else { 0 });

        let max = max_keys.max(0) as usize;
        let entries: Vec<ListEntry> = all_entries.iter().skip(start_idx).take(max).cloned().collect();
        let is_truncated = start_idx + entries.len() < all_entries.len();
        let next_marker = if is_truncated {
            entries.last().map(|entry| entry.key.clone())
        } else {
            None
        };

        Ok(ListResult {
            entries,
            next_marker,
            is_truncated,
        })
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

        Ok(())
    }

    async fn upload_from_file(&self, key: &str, src: &Path, progress: Option<&TransferProgress>) -> Result<()> {
        let data = tokio::fs::read(src).await?;
        let total = data.len() as u64;
        let full_key = self.full_key(key);
        let release = self.get_or_create_release().await?;
        let asset_name = Self::object_key_to_asset_name(&full_key);

        let assets = self.list_assets_for_release(release.id).await?;
        if let Some(existing) = assets.into_iter().find(|asset| asset.name == asset_name) {
            self.delete_asset_by_id(existing.id).await?;
        }
        self.upload_asset(&release, &asset_name, data, "application/octet-stream")
            .await?;

        if let Some(cb) = progress {
            cb(total, total);
        }

        Ok(())
    }
}

fn parse_repository(repo: &str) -> Result<(String, String)> {
    let trimmed = repo.trim();
    let mut parts = trimmed.split('/');
    let owner = parts.next().unwrap_or_default().trim();
    let name = parts.next().unwrap_or_default().trim();

    if owner.is_empty() || name.is_empty() || parts.next().is_some() {
        return Err(SurgeError::Config(
            "GitHub Releases storage requires bucket in 'owner/repo' format".to_string(),
        ));
    }

    Ok((owner.to_string(), name.to_string()))
}

fn resolve_token(config: &StorageConfig) -> String {
    if !config.secret_key.is_empty() {
        return config.secret_key.clone();
    }
    if !config.access_key.is_empty() {
        return config.access_key.clone();
    }
    std::env::var("GITHUB_TOKEN")
        .or_else(|_| std::env::var("GH_TOKEN"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_repository_valid() {
        let (owner, repo) = parse_repository("octocat/hello-world").unwrap();
        assert_eq!(owner, "octocat");
        assert_eq!(repo, "hello-world");
    }

    #[test]
    fn test_parse_repository_invalid() {
        assert!(parse_repository("").is_err());
        assert!(parse_repository("owner-only").is_err());
        assert!(parse_repository("a/b/c").is_err());
    }

    #[test]
    fn test_asset_name_roundtrip() {
        let key = "path/to/release/file.tar.zst";
        let asset_name = GitHubReleasesBackend::object_key_to_asset_name(key);
        let decoded = GitHubReleasesBackend::asset_name_to_object_key(&asset_name).unwrap();
        assert_eq!(decoded, key);
    }
}
