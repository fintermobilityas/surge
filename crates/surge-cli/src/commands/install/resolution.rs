use std::path::Path;

use surge_core::config::manifest::SurgeManifest;
use surge_core::context::StorageConfig;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::ReleaseIndex;

use super::StorageOverrides;

pub(crate) fn selected_install_manifest_path<'a>(
    application_manifest_path: &'a Path,
    fallback_manifest_path: &'a Path,
) -> &'a Path {
    if application_manifest_path.is_file() {
        application_manifest_path
    } else {
        fallback_manifest_path
    }
}

pub(super) fn load_install_manifest_if_available(path: &Path) -> Result<Option<SurgeManifest>> {
    if path.is_file() {
        SurgeManifest::from_file(path).map(Some)
    } else {
        Ok(None)
    }
}

fn install_override_value(scope: &Path, explicit: Option<&str>, env_key: &str, app_id: Option<&str>) -> Option<String> {
    explicit
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| crate::envfile::storage_env_lookup(env_key, scope, app_id))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(super) fn build_storage_config_without_manifest(
    scope: &Path,
    app_id: Option<&str>,
    overrides: StorageOverrides<'_>,
) -> Result<StorageConfig> {
    let provider =
        install_override_value(scope, overrides.provider, "SURGE_STORAGE_PROVIDER", app_id).ok_or_else(|| {
            SurgeError::Config(
                "No install manifest was found. For manifestless install, provide --provider or SURGE_STORAGE_PROVIDER."
                    .to_string(),
            )
        })?;
    let bucket = install_override_value(scope, overrides.bucket, "SURGE_STORAGE_BUCKET", app_id).ok_or_else(|| {
        SurgeError::Config(
            "No install manifest was found. For manifestless install, provide --bucket or SURGE_STORAGE_BUCKET."
                .to_string(),
        )
    })?;
    let region = install_override_value(scope, overrides.region, "SURGE_STORAGE_REGION", app_id).unwrap_or_default();
    let endpoint =
        install_override_value(scope, overrides.endpoint, "SURGE_STORAGE_ENDPOINT", app_id).unwrap_or_default();
    let prefix = install_override_value(scope, overrides.prefix, "SURGE_STORAGE_PREFIX", app_id).unwrap_or_default();

    let provider = super::super::parse_storage_provider(&provider)?;
    let credentials = surge_core::storage_config::storage_credentials_from_lookup(provider, |name| {
        crate::envfile::storage_env_lookup(name, scope, app_id)
    });

    Ok(StorageConfig {
        provider: Some(provider),
        bucket,
        region,
        access_key: credentials.access_key,
        secret_key: credentials.secret_key,
        endpoint,
        prefix,
    })
}

pub(super) fn resolve_install_app_id_without_manifest(
    explicit_app_id: Option<String>,
    index: &ReleaseIndex,
) -> Result<(String, Option<String>)> {
    if let Some(app_id) = explicit_app_id {
        return Ok((app_id, None));
    }

    let app_id = index.app_id.trim();
    if !app_id.is_empty() {
        return Ok((
            app_id.to_string(),
            Some(format!(
                "No install manifest was found; using app id '{app_id}' from the release index."
            )),
        ));
    }

    Err(SurgeError::Config(
        "No install manifest was found and the release index does not declare an app id. Provide --app-id.".to_string(),
    ))
}

pub(super) fn resolve_tailscale_rid_without_manifest(
    explicit_rid: Option<&str>,
    index: &ReleaseIndex,
) -> Result<(String, Option<String>)> {
    if let Some(rid) = explicit_rid.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok((rid.to_string(), None));
    }

    let mut rids = index
        .releases
        .iter()
        .map(|release| release.rid.trim())
        .filter(|rid| !rid.is_empty())
        .collect::<Vec<_>>();
    rids.sort_unstable();
    rids.dedup();

    match rids.as_slice() {
        [rid] => Ok((
            (*rid).to_string(),
            Some(format!(
                "No install manifest was found; using the only RID '{rid}' advertised by the release index."
            )),
        )),
        [] => Err(SurgeError::Config(
            "No install manifest was found and the release index does not advertise a concrete RID. Provide --rid."
                .to_string(),
        )),
        _ => Err(SurgeError::Config(format!(
            "No install manifest was found and the release index advertises multiple RIDs ({}). Provide --rid.",
            rids.join(", ")
        ))),
    }
}

pub(super) fn build_storage_config_with_overrides(
    manifest: &SurgeManifest,
    manifest_path: &Path,
    app_id: &str,
    overrides: StorageOverrides<'_>,
) -> Result<StorageConfig> {
    let mut config = super::super::build_app_scoped_storage_config(manifest, manifest_path, app_id)?;

    if let Some(provider) = install_override_value(
        manifest_path,
        overrides.provider,
        "SURGE_STORAGE_PROVIDER",
        Some(app_id),
    ) {
        config.provider = Some(super::super::parse_storage_provider(&provider)?);
    }
    if let Some(bucket) = install_override_value(manifest_path, overrides.bucket, "SURGE_STORAGE_BUCKET", Some(app_id))
    {
        config.bucket = bucket;
    }
    if let Some(region) = install_override_value(manifest_path, overrides.region, "SURGE_STORAGE_REGION", Some(app_id))
    {
        config.region = region;
    }
    if let Some(endpoint) = install_override_value(
        manifest_path,
        overrides.endpoint,
        "SURGE_STORAGE_ENDPOINT",
        Some(app_id),
    ) {
        config.endpoint = endpoint;
    }
    if let Some(prefix) = install_override_value(manifest_path, overrides.prefix, "SURGE_STORAGE_PREFIX", Some(app_id))
    {
        config.prefix = prefix;
    }

    Ok(config)
}
