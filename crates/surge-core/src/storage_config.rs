use crate::config::installer::InstallerManifest;
use crate::config::manifest::SurgeManifest;
use crate::context::{Context, StorageConfig, StorageProvider};
use crate::error::{Result, SurgeError};

pub struct StorageCredentials {
    pub access_key: String,
    pub secret_key: String,
}

pub fn parse_storage_provider(raw: &str) -> Result<StorageProvider> {
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    let provider = match normalized.as_str() {
        "s3" => StorageProvider::S3,
        "azure" | "azure_blob" | "azureblob" => StorageProvider::AzureBlob,
        "gcs" => StorageProvider::Gcs,
        "filesystem" | "fs" => StorageProvider::Filesystem,
        "github" | "github_releases" | "githubreleases" => StorageProvider::GitHubReleases,
        "" => return Err(SurgeError::Config("Storage provider is required".to_string())),
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };
    Ok(provider)
}

pub fn storage_credentials_from_env(provider: StorageProvider) -> StorageCredentials {
    storage_credentials_from_lookup(provider, |name| std::env::var(name).ok())
}

pub fn storage_credentials_from_lookup<F>(provider: StorageProvider, mut lookup: F) -> StorageCredentials
where
    F: FnMut(&str) -> Option<String>,
{
    match provider {
        StorageProvider::S3 => StorageCredentials {
            access_key: first_non_empty_env(&mut lookup, &["AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"]),
            secret_key: first_non_empty_env(&mut lookup, &["AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY"]),
        },
        StorageProvider::AzureBlob => StorageCredentials {
            access_key: first_non_empty_env(&mut lookup, &["AZURE_STORAGE_ACCOUNT_NAME", "AZURE_STORAGE_ACCOUNT"]),
            secret_key: first_non_empty_env(&mut lookup, &["AZURE_STORAGE_ACCOUNT_KEY"]),
        },
        StorageProvider::Gcs => StorageCredentials {
            access_key: first_non_empty_env(&mut lookup, &["GCS_ACCESS_KEY_ID", "GCS_ACCESS_KEY"]),
            secret_key: first_non_empty_env(
                &mut lookup,
                &["GCS_SECRET_ACCESS_KEY", "GCS_SECRET_KEY", "GOOGLE_ACCESS_TOKEN"],
            ),
        },
        StorageProvider::GitHubReleases => StorageCredentials {
            access_key: String::new(),
            secret_key: first_non_empty_env(&mut lookup, &["GITHUB_TOKEN", "GH_TOKEN"]),
        },
        StorageProvider::Filesystem => StorageCredentials {
            access_key: String::new(),
            secret_key: String::new(),
        },
    }
}

fn first_non_empty_env<F>(lookup: &mut F, keys: &[&str]) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    keys.iter()
        .filter_map(|key| lookup(key))
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
        .unwrap_or_default()
}

pub fn build_storage_context(manifest: &SurgeManifest) -> Result<Context> {
    build_storage_context_with_lookup(manifest, |name| std::env::var(name).ok())
}

pub fn build_storage_context_with_lookup<F>(manifest: &SurgeManifest, lookup: F) -> Result<Context>
where
    F: FnMut(&str) -> Option<String>,
{
    let provider = parse_storage_provider(&manifest.storage.provider)?;
    let creds = storage_credentials_from_lookup(provider, lookup);

    let ctx = Context::new();
    ctx.set_storage(
        provider,
        &manifest.storage.bucket,
        &manifest.storage.region,
        &creds.access_key,
        &creds.secret_key,
        &manifest.storage.endpoint,
    );
    ctx.set_storage_prefix(&manifest.storage.prefix);
    Ok(ctx)
}

pub fn build_storage_config(manifest: &SurgeManifest) -> Result<StorageConfig> {
    build_storage_config_with_lookup(manifest, |name| std::env::var(name).ok())
}

pub fn build_storage_config_with_lookup<F>(manifest: &SurgeManifest, lookup: F) -> Result<StorageConfig>
where
    F: FnMut(&str) -> Option<String>,
{
    Ok(build_storage_context_with_lookup(manifest, lookup)?.storage_config())
}

pub fn build_app_scoped_storage_config(manifest: &SurgeManifest, app_id: &str) -> Result<StorageConfig> {
    build_app_scoped_storage_config_with_lookup(manifest, app_id, |name| std::env::var(name).ok())
}

pub fn build_app_scoped_storage_config_with_lookup<F>(
    manifest: &SurgeManifest,
    app_id: &str,
    lookup: F,
) -> Result<StorageConfig>
where
    F: FnMut(&str) -> Option<String>,
{
    let mut config = build_storage_config_with_lookup(manifest, lookup)?;
    if manifest.apps.len() > 1 {
        config.prefix = append_prefix(&config.prefix, app_id);
    }
    Ok(config)
}

pub fn build_app_scoped_storage_context(manifest: &SurgeManifest, app_id: &str) -> Result<Context> {
    build_app_scoped_storage_context_with_lookup(manifest, app_id, |name| std::env::var(name).ok())
}

pub fn build_app_scoped_storage_context_with_lookup<F>(
    manifest: &SurgeManifest,
    app_id: &str,
    lookup: F,
) -> Result<Context>
where
    F: FnMut(&str) -> Option<String>,
{
    let ctx = build_storage_context_with_lookup(manifest, lookup)?;
    if manifest.apps.len() > 1 {
        let base_prefix = ctx.storage_config().prefix;
        ctx.set_storage_prefix(&append_prefix(&base_prefix, app_id));
    }
    Ok(ctx)
}

pub fn append_prefix(prefix: &str, segment: &str) -> String {
    let prefix = prefix.trim().trim_matches('/');
    let segment = segment.trim().trim_matches('/');

    if prefix.is_empty() {
        segment.to_string()
    } else if segment.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

pub fn build_storage_config_from_installer_manifest(manifest: &InstallerManifest) -> Result<StorageConfig> {
    build_storage_config_from_installer_manifest_with_lookup(manifest, |name| std::env::var(name).ok())
}

pub fn build_storage_config_from_installer_manifest_with_lookup<F>(
    manifest: &InstallerManifest,
    lookup: F,
) -> Result<StorageConfig>
where
    F: FnMut(&str) -> Option<String>,
{
    let provider = parse_storage_provider(&manifest.storage.provider)?;
    let creds = storage_credentials_from_lookup(provider, lookup);

    Ok(StorageConfig {
        provider: Some(provider),
        bucket: manifest.storage.bucket.clone(),
        region: manifest.storage.region.clone(),
        access_key: creds.access_key,
        secret_key: creds.secret_key,
        endpoint: manifest.storage.endpoint.clone(),
        prefix: manifest.storage.prefix.clone(),
    })
}
