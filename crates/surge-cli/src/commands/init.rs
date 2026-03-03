use crate::ui::UiTheme;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::Path;
use uuid::Uuid;

use surge_core::config::manifest::{AppConfig, StorageManifestConfig, SurgeManifest, TargetConfig};
use surge_core::error::{Result, SurgeError};
use surge_core::platform::detect::current_rid;

/// Initialize a new surge.yml manifest file.
pub async fn execute(
    manifest_path: &Path,
    app_id: Option<&str>,
    name: Option<&str>,
    provider: Option<&str>,
    bucket: Option<&str>,
    rid: Option<&str>,
    main_exe: Option<&str>,
    install_directory: Option<&str>,
    supervisor_id: Option<&str>,
    wizard: bool,
) -> Result<()> {
    let manifest_path = manifest_path.to_path_buf();
    if manifest_path.exists() {
        tracing::warn!("Manifest already exists at {}", manifest_path.display());
        return Err(SurgeError::Config(format!(
            "Manifest already exists at {}",
            manifest_path.display()
        )));
    }

    let init = if wizard {
        gather_wizard_input(
            &manifest_path,
            app_id,
            name,
            provider,
            bucket,
            rid,
            main_exe,
            install_directory,
            supervisor_id,
        )?
    } else {
        build_non_wizard_input(
            app_id,
            name,
            provider,
            bucket,
            rid,
            main_exe,
            install_directory,
            supervisor_id,
        )?
    };

    let manifest = SurgeManifest {
        schema: surge_core::config::constants::SCHEMA_VERSION,
        storage: StorageManifestConfig {
            provider: init.provider,
            bucket: init.bucket,
            ..Default::default()
        },
        lock: None,
        channels: vec![],
        apps: vec![AppConfig {
            id: init.app_id.clone(),
            name: init.name,
            main_exe: init.main_exe,
            install_directory: init.install_directory,
            supervisor_id: init.supervisor_id,
            channels: vec![],
            os: String::new(),
            icon: String::new(),
            shortcuts: vec![],
            persistent_assets: vec![],
            installers: vec![],
            environment: BTreeMap::new(),
            targets: vec![TargetConfig {
                rid: init.rid,
                os: String::new(),
                distro: String::new(),
                variant: String::new(),
                artifacts_dir: String::new(),
                include: vec![],
                exclude: vec![],
                icon: String::new(),
                shortcuts: vec![],
                persistent_assets: vec![],
                installers: vec![],
                environment: BTreeMap::new(),
            }],
            target: None,
        }],
    };

    manifest.validate()?;

    if let Some(parent) = manifest_path.parent() {
        std::fs::create_dir_all(parent)?;
        if parent.file_name().and_then(|name| name.to_str()) == Some(".surge") {
            std::fs::create_dir_all(parent.join("packages"))?;
        }
    }

    let yaml = manifest.to_yaml()?;
    std::fs::write(&manifest_path, &yaml)?;

    tracing::info!("Created manifest at {}", manifest_path.display());
    Ok(())
}

struct InitInput {
    app_id: String,
    name: String,
    provider: String,
    bucket: String,
    rid: String,
    main_exe: String,
    install_directory: String,
    supervisor_id: String,
}

fn build_non_wizard_input(
    app_id: Option<&str>,
    name: Option<&str>,
    provider: Option<&str>,
    bucket: Option<&str>,
    rid: Option<&str>,
    main_exe: Option<&str>,
    install_directory: Option<&str>,
    supervisor_id: Option<&str>,
) -> Result<InitInput> {
    let app_id = app_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SurgeError::Config("`--app-id` is required unless `--wizard` is set".to_string()))?
        .to_string();

    let provider = normalize_provider(provider.unwrap_or("filesystem"))?;
    let bucket = resolve_default_bucket(&provider, bucket)?.to_string();
    let rid = rid
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(current_rid, std::borrow::ToOwned::to_owned);
    let name = name.unwrap_or(&app_id).to_string();
    let main_exe = main_exe
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&app_id)
        .to_string();
    let install_directory = install_directory
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&app_id)
        .to_string();
    let supervisor_id = parse_or_generate_supervisor_id(supervisor_id)?;

    Ok(InitInput {
        app_id,
        name,
        provider,
        bucket,
        rid,
        main_exe,
        install_directory,
        supervisor_id,
    })
}

fn gather_wizard_input(
    manifest_path: &Path,
    app_id: Option<&str>,
    name: Option<&str>,
    provider: Option<&str>,
    bucket: Option<&str>,
    rid: Option<&str>,
    main_exe: Option<&str>,
    install_directory: Option<&str>,
    supervisor_id: Option<&str>,
) -> Result<InitInput> {
    let theme = UiTheme::global();
    println!("{}", theme.title("Surge init wizard"));
    println!("{}", theme.info(&format!("Manifest path: {}", manifest_path.display())));

    let app_id_default = app_id.unwrap_or("my-app").trim();
    let app_id = prompt_with_default("App id", app_id_default)?;

    let name_default = name.unwrap_or(&app_id);
    let name = prompt_with_default("App display name", name_default)?;

    let provider_default_raw = provider.unwrap_or("filesystem");
    let provider_default = normalize_provider(provider_default_raw)?;

    let provider = loop {
        let entered = prompt_with_default(
            "Storage provider (filesystem, s3, azure, gcs, github_releases)",
            &provider_default,
        )?;
        match normalize_provider(&entered) {
            Ok(valid) => break valid,
            Err(err) => println!("{}", theme.warning(&format!("Invalid provider: {err}"))),
        }
    };

    let default_bucket = resolve_default_bucket(&provider, bucket)?;
    let bucket = prompt_with_default("Storage bucket/root", default_bucket)?;

    let rid_default = rid
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(current_rid, std::borrow::ToOwned::to_owned);
    let rid = prompt_with_default("Runtime identifier", &rid_default)?;
    let main_exe_default = main_exe
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&app_id);
    let main_exe = prompt_with_default("Main executable", main_exe_default)?;
    let install_directory_default = install_directory
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&app_id);
    let install_directory = prompt_with_default("Install directory name", install_directory_default)?;

    Ok(InitInput {
        app_id,
        name,
        provider,
        bucket,
        rid,
        main_exe,
        install_directory,
        supervisor_id: parse_or_generate_supervisor_id(supervisor_id)?,
    })
}

fn prompt_with_default(prompt: &str, default: &str) -> Result<String> {
    let theme = UiTheme::global();
    print!("{}", theme.blue(&format!("{prompt} [{default}]: ")));
    io::stdout()
        .flush()
        .map_err(|e| SurgeError::Config(format!("Failed to flush stdout: {e}")))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| SurgeError::Config(format!("Failed to read input: {e}")))?;

    let value = input.trim();
    if value.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(value.to_string())
    }
}

fn normalize_provider(raw: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    let provider = match normalized.as_str() {
        "filesystem" | "fs" => "filesystem",
        "s3" => "s3",
        "azure" | "azure_blob" | "azureblob" => "azure",
        "gcs" => "gcs",
        "github" | "github_releases" | "githubreleases" => "github_releases",
        other => return Err(SurgeError::Config(format!("Unknown storage provider: {other}"))),
    };
    Ok(provider.to_string())
}

fn resolve_default_bucket<'a>(provider: &str, bucket: Option<&'a str>) -> Result<&'a str> {
    match provider {
        "filesystem" => Ok(bucket.unwrap_or(".surge/storage")),
        "github_releases" => Ok(bucket.unwrap_or("owner/repo")),
        "s3" | "azure" | "gcs" => bucket.map(str::trim).filter(|value| !value.is_empty()).ok_or_else(|| {
            SurgeError::Config(format!(
                "Storage bucket/root is required for provider '{provider}' in non-wizard mode"
            ))
        }),
        _ => Ok(bucket.unwrap_or("my-bucket")),
    }
}

fn parse_or_generate_supervisor_id(supervisor_id: Option<&str>) -> Result<String> {
    match supervisor_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => {
            let parsed =
                Uuid::parse_str(value).map_err(|e| SurgeError::Config(format!("Invalid --supervisor-id UUID: {e}")))?;
            Ok(parsed.to_string())
        }
        None => Ok(Uuid::new_v4().to_string()),
    }
}
