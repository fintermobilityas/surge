use std::collections::BTreeSet;
use std::io::IsTerminal;

use crate::{logline, prompts};
use surge_core::config::manifest::SurgeManifest;
use surge_core::error::{Result, SurgeError};
use surge_core::releases::manifest::{ReleaseEntry, ReleaseIndex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedInstallChannel {
    pub(super) name: String,
    pub(super) note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InstallSelection {
    pub(super) app_id: String,
    pub(super) os: String,
    pub(super) rid: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AppInstallTargetOption {
    pub(super) os: String,
    pub(super) rid: String,
}

pub(super) fn resolve_install_channel(
    manifest: &SurgeManifest,
    index: &ReleaseIndex,
    app_id: &str,
    explicit: Option<&str>,
) -> Result<ResolvedInstallChannel> {
    if let Some(channel) = explicit {
        return Ok(ResolvedInstallChannel {
            name: channel.to_string(),
            note: None,
        });
    }

    let available_channels = collect_available_channels(&index.releases);
    if available_channels.len() == 1 {
        let selected = available_channels[0].clone();
        return Ok(ResolvedInstallChannel {
            name: selected.clone(),
            note: Some(format!(
                "No --channel provided; single available channel '{selected}' selected automatically."
            )),
        });
    }
    if available_channels.len() > 1 {
        return Err(SurgeError::Config(format!(
            "Multiple channels available for app '{app_id}': {}. Specify --channel <name> to choose.",
            available_channels.join(", ")
        )));
    }

    let configured_channels = collect_configured_channels(manifest, app_id);
    if configured_channels.len() == 1 {
        let selected = configured_channels[0].clone();
        return Ok(ResolvedInstallChannel {
            name: selected.clone(),
            note: Some(format!(
                "No --channel provided; single configured channel '{selected}' selected automatically."
            )),
        });
    }
    if configured_channels.len() > 1 {
        return Err(SurgeError::Config(format!(
            "Multiple channels configured for app '{app_id}': {}. Specify --channel <name> to choose.",
            configured_channels.join(", ")
        )));
    }

    Ok(ResolvedInstallChannel {
        name: "stable".to_string(),
        note: Some("No channel metadata found; defaulting to 'stable'.".to_string()),
    })
}

pub(super) fn resolve_install_channel_without_manifest(
    index: &ReleaseIndex,
    explicit: Option<&str>,
) -> Result<ResolvedInstallChannel> {
    if let Some(channel) = explicit {
        return Ok(ResolvedInstallChannel {
            name: channel.to_string(),
            note: None,
        });
    }

    let available_channels = collect_available_channels(&index.releases);
    if available_channels.len() == 1 {
        let selected = available_channels[0].clone();
        return Ok(ResolvedInstallChannel {
            name: selected.clone(),
            note: Some(format!(
                "No --channel provided; single available channel '{selected}' selected automatically."
            )),
        });
    }
    if available_channels.len() > 1 {
        return Err(SurgeError::Config(format!(
            "Multiple channels are available in the release index: {}. Specify --channel <name> to choose.",
            available_channels.join(", ")
        )));
    }

    Ok(ResolvedInstallChannel {
        name: "stable".to_string(),
        note: Some(
            "No --channel provided and the release index has no channel metadata; defaulting to 'stable'.".to_string(),
        ),
    })
}

pub(super) fn prompt_install_channel(
    manifest: &SurgeManifest,
    index: &ReleaseIndex,
    app_id: &str,
    requested: Option<&str>,
) -> Result<ResolvedInstallChannel> {
    let options = collect_install_channel_options(manifest, index, app_id);
    let default_index = requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|channel| options.iter().position(|option| option == channel))
        .unwrap_or(0);
    let selected_index = prompt_choice_index("Select channel", &options, default_index)?;
    let selected = options[selected_index].clone();
    Ok(ResolvedInstallChannel {
        name: selected.clone(),
        note: Some(format!("Selected channel '{selected}' via install wizard.")),
    })
}

pub(super) fn require_interactive_manifest(manifest: Option<&SurgeManifest>) -> Result<&SurgeManifest> {
    manifest.ok_or_else(|| SurgeError::Config("Interactive install requires an install manifest.".to_string()))
}

pub(super) fn collect_install_channel_options(
    manifest: &SurgeManifest,
    index: &ReleaseIndex,
    app_id: &str,
) -> Vec<String> {
    let mut options = collect_available_channels(&index.releases);
    if options.is_empty() {
        options = collect_configured_channels(manifest, app_id);
    }
    if options.is_empty() {
        options.push("stable".to_string());
    }
    options
}

fn collect_configured_channels(manifest: &SurgeManifest, app_id: &str) -> Vec<String> {
    let mut channels = Vec::new();

    if let Some(app) = manifest.apps.iter().find(|app| app.id == app_id) {
        for channel in &app.channels {
            let trimmed = channel.trim();
            if !trimmed.is_empty() && !channels.iter().any(|existing| existing == trimmed) {
                channels.push(trimmed.to_string());
            }
        }
    }

    if channels.is_empty() {
        for channel in &manifest.channels {
            let trimmed = channel.name.trim();
            if !trimmed.is_empty() && !channels.iter().any(|existing| existing == trimmed) {
                channels.push(trimmed.to_string());
            }
        }
    }

    channels
}

pub(super) fn collect_available_channels(releases: &[ReleaseEntry]) -> Vec<String> {
    let mut channels = BTreeSet::new();
    for release in releases {
        for channel in &release.channels {
            let trimmed = channel.trim();
            if !trimmed.is_empty() {
                channels.insert(trimmed.to_string());
            }
        }
    }
    channels.into_iter().collect()
}

pub(super) fn should_prompt_install_selection() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

pub(super) fn prompt_install_selection(
    manifest: &SurgeManifest,
    requested_app_id: Option<&str>,
    requested_rid: Option<&str>,
) -> Result<InstallSelection> {
    let mut app_ids = Vec::new();
    let mut app_labels = Vec::new();
    for app in &manifest.apps {
        let app_id = app.id.trim();
        if app_id.is_empty() || app_ids.iter().any(|existing: &String| existing == app_id) {
            continue;
        }
        app_ids.push(app_id.to_string());
        app_labels.push(prompts::format_app_label(manifest, app_id));
    }

    if app_ids.is_empty() {
        return Err(SurgeError::Config(
            "Manifest has no apps. Provide --app-id explicitly.".to_string(),
        ));
    }

    logline::title("Install target selection");
    let requested_app_id = requested_app_id.map(str::trim).filter(|value| !value.is_empty());
    let default_app_index = requested_app_id
        .and_then(|app_id| app_ids.iter().position(|candidate| candidate == app_id))
        .unwrap_or(0);
    let selected_app_index = prompt_choice_index("Select app", &app_labels, default_app_index)?;
    let selected_app_id = app_ids[selected_app_index].clone();

    let target_options = collect_target_options_for_app(manifest, &selected_app_id)?;
    if target_options.is_empty() {
        return Err(SurgeError::Config(format!(
            "App '{selected_app_id}' has no targets. Add targets to the manifest before install."
        )));
    }

    let selected_target = resolve_install_target_selection(&target_options, requested_rid)?;

    Ok(InstallSelection {
        app_id: selected_app_id,
        os: selected_target.os,
        rid: selected_target.rid,
    })
}

fn prompt_choice_index(prompt: &str, options: &[String], default_index: usize) -> Result<usize> {
    prompts::select(prompt, options, default_index)
}

pub(super) fn resolve_install_target_selection(
    target_options: &[AppInstallTargetOption],
    requested_rid: Option<&str>,
) -> Result<AppInstallTargetOption> {
    if target_options.is_empty() {
        return Err(SurgeError::Config(
            "App has no target options. Add at least one target to the manifest.".to_string(),
        ));
    }

    if target_options.len() == 1 {
        return Ok(target_options[0].clone());
    }

    let requested_rid = requested_rid.map(str::trim).filter(|value| !value.is_empty());
    if let Some(requested_rid) = requested_rid {
        let mut matching = target_options.iter().filter(|option| option.rid == requested_rid);
        if let (Some(selected), None) = (matching.next(), matching.next()) {
            return Ok(selected.clone());
        }
    }

    let labels = target_options
        .iter()
        .map(format_target_option_label)
        .collect::<Vec<_>>();
    let default_index = requested_rid
        .and_then(|rid| target_options.iter().position(|option| option.rid == rid))
        .unwrap_or(0);
    let selected_index = prompt_choice_index("Select target", &labels, default_index)?;
    Ok(target_options[selected_index].clone())
}

pub(super) fn format_target_option_label(option: &AppInstallTargetOption) -> String {
    let rid_parts: Vec<&str> = option.rid.split('-').collect();
    if rid_parts.len() >= 2 {
        format!("{}/{}", rid_parts[0], rid_parts[1])
    } else {
        option.rid.clone()
    }
}

pub(super) fn collect_target_options_for_app(
    manifest: &SurgeManifest,
    app_id: &str,
) -> Result<Vec<AppInstallTargetOption>> {
    let mut options = Vec::new();
    let mut app_found = false;

    for app in &manifest.apps {
        if app.id != app_id {
            continue;
        }
        app_found = true;
        for target in app.target.iter().chain(app.targets.iter()) {
            let rid = target.rid.trim();
            if rid.is_empty() {
                continue;
            }
            let os = if target.os.trim().is_empty() {
                infer_os_from_rid(rid).unwrap_or_else(|| "unknown".to_string())
            } else {
                target.os.trim().to_ascii_lowercase()
            };
            let option = AppInstallTargetOption {
                os,
                rid: rid.to_string(),
            };
            if !options
                .iter()
                .any(|existing: &AppInstallTargetOption| existing == &option)
            {
                options.push(option);
            }
        }
    }

    if !app_found {
        return Err(SurgeError::Config(format!(
            "App '{app_id}' was not found in manifest. Provide --app-id with a valid app id."
        )));
    }

    Ok(options)
}

pub(super) fn infer_os_from_rid(rid: &str) -> Option<String> {
    let prefix = rid.split('-').next()?.trim().to_ascii_lowercase();
    let normalized = match prefix.as_str() {
        "linux" => "linux",
        "win" | "windows" => "windows",
        "osx" | "macos" | "darwin" => "macos",
        _ => return None,
    };
    Some(normalized.to_string())
}
