use std::io::IsTerminal;

use surge_core::config::manifest::{AppConfig, SurgeManifest};
use surge_core::error::{Result, SurgeError};

/// Interactive selection from a list of options using dialoguer.
/// Falls back to error if stdin is not a terminal.
pub(crate) fn select(prompt: &str, options: &[String], default_index: usize) -> Result<usize> {
    if options.is_empty() {
        return Err(SurgeError::Config(format!(
            "No options available for prompt '{prompt}'."
        )));
    }

    if !std::io::stdin().is_terminal() {
        return Err(SurgeError::Config(format!(
            "Multiple options for '{prompt}' but stdin is not interactive. Provide an explicit value."
        )));
    }

    let default_index = default_index.min(options.len().saturating_sub(1));

    dialoguer::Select::with_theme(&dialoguer::theme::ColorfulTheme::default())
        .with_prompt(prompt)
        .items(options)
        .default(default_index)
        .interact()
        .map_err(|e| SurgeError::Config(format!("Selection cancelled: {e}")))
}

/// Returns true when the terminal is interactive (TTY on stdin).
pub(crate) fn is_interactive() -> bool {
    std::io::stdin().is_terminal()
}

/// Resolve app ID, prompting interactively when the manifest has multiple apps.
pub(crate) fn resolve_app_id(
    manifest: &SurgeManifest,
    requested: Option<&str>,
) -> Result<String> {
    if let Some(app_id) = requested.map(str::trim).filter(|v| !v.is_empty()) {
        return Ok(app_id.to_string());
    }

    let app_ids = manifest.app_ids();
    match app_ids.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(SurgeError::Config(
            "Manifest has no apps. Provide --app-id explicitly.".to_string(),
        )),
        multiple => {
            if !is_interactive() {
                return Err(SurgeError::Config(format!(
                    "Manifest contains multiple apps ({}). Provide --app-id.",
                    multiple.join(", ")
                )));
            }

            let labels: Vec<String> = multiple
                .iter()
                .map(|app_id| format_app_label(manifest, app_id))
                .collect();
            let idx = select("Select app", &labels, 0)?;
            Ok(multiple[idx].clone())
        }
    }
}

/// Resolve app ID with a RID hint, prompting interactively when ambiguous.
pub(crate) fn resolve_app_id_with_rid_hint(
    manifest: &SurgeManifest,
    requested_app_id: Option<&str>,
    requested_rid: Option<&str>,
) -> Result<String> {
    if let Some(app_id) = requested_app_id.map(str::trim).filter(|v| !v.is_empty()) {
        return Ok(app_id.to_string());
    }

    let requested_rid = requested_rid.map(str::trim).filter(|v| !v.is_empty());
    if let Some(rid) = requested_rid {
        let mut candidates: Vec<String> = manifest
            .app_ids()
            .into_iter()
            .filter(|app_id| manifest.target_rids(app_id).iter().any(|r| r == rid))
            .collect();
        candidates.sort();
        candidates.dedup();

        return match candidates.as_slice() {
            [single] => Ok(single.clone()),
            [] => {
                if manifest.apps.len() > 1 {
                    Err(SurgeError::Config(format!(
                        "No app in manifest defines target RID '{rid}'. Provide --app-id."
                    )))
                } else {
                    resolve_app_id(manifest, None)
                }
            }
            multiple => {
                if !is_interactive() {
                    return Err(SurgeError::Config(format!(
                        "RID '{rid}' matches multiple apps ({}). Provide --app-id.",
                        multiple.join(", ")
                    )));
                }

                let labels: Vec<String> = multiple
                    .iter()
                    .map(|app_id| format_app_label(manifest, app_id))
                    .collect();
                let idx = select("Select app", &labels, 0)?;
                Ok(multiple[idx].clone())
            }
        };
    }

    resolve_app_id(manifest, None)
}

/// Resolve RID, prompting interactively when the app has multiple targets.
pub(crate) fn resolve_rid(
    manifest: &SurgeManifest,
    app_id: &str,
    requested: Option<&str>,
) -> Result<String> {
    if let Some(rid) = requested.map(str::trim).filter(|v| !v.is_empty()) {
        return Ok(rid.to_string());
    }

    let rids = manifest.target_rids(app_id);
    match rids.as_slice() {
        [single] => Ok(single.clone()),
        [] => Err(SurgeError::Config(format!(
            "App '{app_id}' has no targets. Provide --rid explicitly."
        ))),
        multiple => {
            if !is_interactive() {
                return Err(SurgeError::Config(format!(
                    "App '{app_id}' has multiple targets ({}). Provide --rid.",
                    multiple.join(", ")
                )));
            }

            let labels: Vec<String> = multiple
                .iter()
                .map(|rid| format_rid_label(manifest, app_id, rid))
                .collect();
            let idx = select("Select target", &labels, 0)?;
            Ok(multiple[idx].clone())
        }
    }
}

/// Format an app label for display in selection prompts.
///
/// Uses structured data from the manifest (target distro, rid, variant) to
/// produce labels like:
///   `youpark · linux/x64 · ubuntu 24.04 · cpu`
///   `youpark · linux/arm64 · jetpack 5.0`
pub(crate) fn format_app_label(manifest: &SurgeManifest, app_id: &str) -> String {
    let app = manifest.apps.iter().find(|a| a.id == app_id);
    let name = app.map(AppConfig::effective_name).unwrap_or_default();
    let target = app.and_then(|a| a.target.as_ref().or_else(|| a.targets.first()));

    let Some(target) = target else {
        return if name.is_empty() || name == app_id {
            app_id.to_string()
        } else {
            format!("{name} ({app_id})")
        };
    };

    let display_name = if name.is_empty() { app_id } else { &name };

    let rid_parts: Vec<&str> = target.rid.split('-').collect();
    let os_arch = if rid_parts.len() >= 2 {
        format!("{}/{}", rid_parts[0], rid_parts[1])
    } else {
        target.rid.clone()
    };

    let distro = target.distro.trim();
    let variant = target.variant.trim();

    let mut label = format!("{display_name} · {os_arch}");
    if !distro.is_empty() {
        label.push_str(&format!(" · {distro}"));
    }
    if !variant.is_empty() {
        label.push_str(&format!(" · {variant}"));
    }
    label
}

/// Format a RID label for display in target selection prompts.
///
/// Produces labels like:
///   `linux/x64 · ubuntu 24.04 · cpu`
///   `linux/arm64 · jetpack 5.0`
fn format_rid_label(manifest: &SurgeManifest, app_id: &str, rid: &str) -> String {
    let app = manifest.apps.iter().find(|a| a.id == app_id);
    let target = app.and_then(|a| {
        a.target
            .as_ref()
            .filter(|t| t.rid == rid)
            .or_else(|| a.targets.iter().find(|t| t.rid == rid))
    });

    let rid_parts: Vec<&str> = rid.split('-').collect();
    let os_arch = if rid_parts.len() >= 2 {
        format!("{}/{}", rid_parts[0], rid_parts[1])
    } else {
        rid.to_string()
    };

    let Some(target) = target else {
        return os_arch;
    };

    let distro = target.distro.trim();
    let variant = target.variant.trim();

    let mut label = os_arch;
    if !distro.is_empty() {
        label.push_str(&format!(" · {distro}"));
    }
    if !variant.is_empty() {
        label.push_str(&format!(" · {variant}"));
    }
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &[u8]) -> SurgeManifest {
        SurgeManifest::parse(yaml).unwrap()
    }

    #[test]
    fn format_app_label_includes_distro_and_variant() {
        let m = parse(br"schema: 2
storage: { provider: filesystem, bucket: /tmp }
channels: [{ name: stable }]
apps:
  - id: youpark
    name: youpark
    targets:
      - rid: linux-x64
        distro: ubuntu24.04
        variant: cpu
      - rid: linux-x64
        distro: ubuntu24.04
        variant: cuda
");
        let label = format_app_label(&m, "youpark-ubuntu24.04-linux-x64-cpu");
        assert_eq!(label, "youpark · linux/x64 · ubuntu24.04 · cpu");
    }

    #[test]
    fn format_app_label_omits_empty_variant() {
        let m = parse(br"schema: 2
storage: { provider: filesystem, bucket: /tmp }
channels: [{ name: stable }]
apps:
  - id: youpark
    name: youpark
    targets:
      - rid: linux-arm64
        distro: jetpack5.0
      - rid: linux-arm64
        distro: jetpack4.6
");
        let label = format_app_label(&m, "youpark-jetpack5.0-linux-arm64");
        assert_eq!(label, "youpark · linux/arm64 · jetpack5.0");
    }

    #[test]
    fn format_app_label_falls_back_to_app_id() {
        let m = parse(br"schema: 2
storage: { provider: filesystem, bucket: /tmp }
channels: [{ name: stable }]
apps:
  - id: simple-app
    targets:
      - rid: linux-x64
");
        let label = format_app_label(&m, "simple-app");
        assert_eq!(label, "simple-app · linux/x64");
    }

    #[test]
    fn format_rid_label_includes_distro_and_variant() {
        let m = parse(br"schema: 2
storage: { provider: filesystem, bucket: /tmp }
channels: [{ name: stable }]
apps:
  - id: myapp
    targets:
      - rid: linux-x64
        distro: ubuntu24.04
        variant: cuda
");
        let label = format_rid_label(&m, "myapp", "linux-x64");
        assert_eq!(label, "linux/x64 · ubuntu24.04 · cuda");
    }

    #[test]
    fn format_rid_label_plain_rid() {
        let m = parse(br"schema: 2
storage: { provider: filesystem, bucket: /tmp }
channels: [{ name: stable }]
apps:
  - id: myapp
    targets:
      - rid: linux-arm64
");
        let label = format_rid_label(&m, "myapp", "linux-arm64");
        assert_eq!(label, "linux/arm64");
    }

}
