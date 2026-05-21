mod evaluation;
mod fingerprint;

use surge_core::config::manifest::TargetCompatibilityConfig;
use surge_core::error::{Result, SurgeError};

use self::evaluation::{CompatibilityVerdict, evaluate_compatibility};
use self::fingerprint::{RuntimeFingerprint, collect_local_runtime_fingerprint, collect_remote_runtime_fingerprint};
use super::logline;

#[derive(Debug, Clone, Copy)]
pub(super) enum CompatibilityInstallTarget<'a> {
    Local,
    Tailscale { ssh_target: &'a str, file_target: &'a str },
}

pub(super) async fn run_platform_compatibility_preflight(
    target: CompatibilityInstallTarget<'_>,
    selected_rid: &str,
    compatibility: &TargetCompatibilityConfig,
    allow_platform_mismatch: bool,
) -> Result<()> {
    if compatibility.is_empty() {
        return Ok(());
    }

    let fingerprint = match target {
        CompatibilityInstallTarget::Local => collect_local_runtime_fingerprint(compatibility),
        CompatibilityInstallTarget::Tailscale { ssh_target, .. } => {
            collect_remote_runtime_fingerprint(ssh_target, compatibility).await?
        }
    };
    log_fingerprint(target, &fingerprint);

    let evaluation = evaluate_compatibility(compatibility, &fingerprint);
    logline::info(&format!(
        "Compatibility verdict for RID '{selected_rid}': {}",
        evaluation.verdict.as_str()
    ));
    for reason in &evaluation.reasons {
        logline::info(&format!("Compatibility detail: {reason}"));
    }

    if evaluation.verdict == CompatibilityVerdict::Incompatible {
        let reason = evaluation.reasons.join("; ");
        if allow_platform_mismatch {
            logline::warn(&format!(
                "Platform compatibility mismatch overridden by --allow-platform-mismatch for RID '{selected_rid}': {reason}"
            ));
        } else {
            return Err(SurgeError::Platform(format!(
                "Selected target RID '{selected_rid}' is incompatible with the install host: {reason}. Use --allow-platform-mismatch to override in an emergency."
            )));
        }
    } else if evaluation.verdict == CompatibilityVerdict::Unknown {
        logline::warn(&format!(
            "Platform compatibility for RID '{selected_rid}' is unknown; continuing because no explicit incompatibility was detected."
        ));
    }

    Ok(())
}

fn log_fingerprint(target: CompatibilityInstallTarget<'_>, fingerprint: &RuntimeFingerprint) {
    let os_id = fingerprint.os_release.get("ID").map_or("unknown", String::as_str);
    let os_version = fingerprint
        .os_release
        .get("VERSION_ID")
        .map_or("unknown", String::as_str);
    let arch = if fingerprint.arch.is_empty() {
        "unknown"
    } else {
        fingerprint.arch.as_str()
    };
    let kernel = if fingerprint.kernel.is_empty() {
        "unknown"
    } else {
        fingerprint.kernel.as_str()
    };
    let gpu = if fingerprint.gpu.is_empty() {
        "unknown"
    } else {
        fingerprint.gpu.as_str()
    };

    match target {
        CompatibilityInstallTarget::Local => logline::info(&format!(
            "Local runtime fingerprint: os={os_id} version={os_version} arch={arch} kernel={kernel} gpu={gpu}"
        )),
        CompatibilityInstallTarget::Tailscale { file_target, .. } => logline::info(&format!(
            "Remote runtime fingerprint for {file_target}: os={os_id} version={os_version} arch={arch} kernel={kernel} gpu={gpu}"
        )),
    }
}
