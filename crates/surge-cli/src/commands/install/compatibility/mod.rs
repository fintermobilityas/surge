mod architecture;
mod evaluation;
mod fingerprint;

use surge_core::config::manifest::TargetCompatibilityConfig;
use surge_core::error::{Result, SurgeError};

use self::architecture::{ArchitectureCheck, ArchitectureVerdict, check_rid_architecture};
use self::evaluation::{CompatibilityEvaluation, CompatibilityVerdict, evaluate_compatibility};
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
    let fingerprint = match target {
        CompatibilityInstallTarget::Local => collect_local_runtime_fingerprint(compatibility),
        CompatibilityInstallTarget::Tailscale { ssh_target, .. } => {
            collect_remote_runtime_fingerprint(ssh_target, compatibility).await?
        }
    };
    log_fingerprint(target, compatibility, &fingerprint);

    if let Some(error) = assess_preflight(selected_rid, compatibility, &fingerprint, allow_platform_mismatch) {
        return Err(error);
    }

    Ok(())
}

/// Logs the architecture and declarative compatibility verdicts and returns the
/// first error that must block the install. The architecture check runs
/// unconditionally and before the declarative evaluation, because a
/// wrong-architecture package is the more fundamental failure.
fn assess_preflight(
    selected_rid: &str,
    compatibility: &TargetCompatibilityConfig,
    fingerprint: &RuntimeFingerprint,
    allow_platform_mismatch: bool,
) -> Option<SurgeError> {
    let architecture = check_rid_architecture(selected_rid, &fingerprint.arch);
    if let Some(error) =
        report_architecture_verdict(selected_rid, &fingerprint.arch, &architecture, allow_platform_mismatch)
    {
        return Some(error);
    }

    if compatibility.is_empty() {
        return None;
    }

    let evaluation = evaluate_compatibility(compatibility, fingerprint);
    report_compatibility_verdict(selected_rid, &evaluation, allow_platform_mismatch)
}

fn report_architecture_verdict(
    selected_rid: &str,
    host_arch: &str,
    check: &ArchitectureCheck,
    allow_platform_mismatch: bool,
) -> Option<SurgeError> {
    logline::info(&format!(
        "Architecture verdict for RID '{selected_rid}': {} ({})",
        check.verdict.as_str(),
        check.reason
    ));

    match check.verdict {
        ArchitectureVerdict::Match => None,
        ArchitectureVerdict::Unknown => {
            logline::warn(&format!(
                "Platform architecture for RID '{selected_rid}' could not be verified; continuing: {}",
                check.reason
            ));
            None
        }
        ArchitectureVerdict::Mismatch if allow_platform_mismatch => {
            logline::warn(&format!(
                "Platform architecture mismatch overridden by --allow-platform-mismatch for RID '{selected_rid}': {}",
                check.reason
            ));
            None
        }
        ArchitectureVerdict::Mismatch => Some(SurgeError::Platform(format!(
            "Selected target RID '{selected_rid}' does not match install host architecture '{host_arch}'. Use --allow-platform-mismatch to override in an emergency."
        ))),
    }
}

fn report_compatibility_verdict(
    selected_rid: &str,
    evaluation: &CompatibilityEvaluation,
    allow_platform_mismatch: bool,
) -> Option<SurgeError> {
    logline::info(&format!(
        "Compatibility verdict for RID '{selected_rid}': {}",
        evaluation.verdict.as_str()
    ));
    for reason in &evaluation.reasons {
        logline::info(&format!("Compatibility detail: {reason}"));
    }

    match evaluation.verdict {
        CompatibilityVerdict::Compatible => None,
        CompatibilityVerdict::Unknown => {
            logline::warn(&format!(
                "Platform compatibility for RID '{selected_rid}' is unknown; continuing because no explicit incompatibility was detected."
            ));
            None
        }
        CompatibilityVerdict::Incompatible if allow_platform_mismatch => {
            logline::warn(&format!(
                "Platform compatibility mismatch overridden by --allow-platform-mismatch for RID '{selected_rid}': {}",
                evaluation.reasons.join("; ")
            ));
            None
        }
        CompatibilityVerdict::Incompatible => Some(SurgeError::Platform(format!(
            "Selected target RID '{selected_rid}' is incompatible with the install host: {}. Use --allow-platform-mismatch to override in an emergency.",
            evaluation.reasons.join("; ")
        ))),
    }
}

fn log_fingerprint(
    target: CompatibilityInstallTarget<'_>,
    compatibility: &TargetCompatibilityConfig,
    fingerprint: &RuntimeFingerprint,
) {
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
    // Only render the GPU part when the release asks for a GPU; otherwise the probe
    // is skipped on purpose and `gpu=unknown` would misreport it as undetected.
    let gpu = if compatibility.gpu.is_some() {
        let gpu = if fingerprint.gpu.is_empty() {
            "unknown"
        } else {
            fingerprint.gpu.as_str()
        };
        format!(" gpu={gpu}")
    } else {
        String::new()
    };

    match target {
        CompatibilityInstallTarget::Local => logline::info(&format!(
            "Local runtime fingerprint: os={os_id} version={os_version} arch={arch} kernel={kernel}{gpu}"
        )),
        CompatibilityInstallTarget::Tailscale { file_target, .. } => logline::info(&format!(
            "Remote runtime fingerprint for {file_target}: os={os_id} version={os_version} arch={arch} kernel={kernel}{gpu}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use surge_core::config::manifest::GpuCompatibilityConfig;

    use super::*;

    fn fingerprint(arch: &str, gpu: &str) -> RuntimeFingerprint {
        RuntimeFingerprint {
            os_release: BTreeMap::new(),
            arch: arch.to_string(),
            kernel: String::new(),
            gpu: gpu.to_string(),
            files: BTreeMap::new(),
            packages: BTreeMap::new(),
        }
    }

    fn nvidia_only() -> TargetCompatibilityConfig {
        TargetCompatibilityConfig {
            gpu: Some(GpuCompatibilityConfig {
                vendor: Some("nvidia".to_string()),
            }),
            ..TargetCompatibilityConfig::default()
        }
    }

    #[test]
    fn empty_compatibility_still_rejects_wrong_architecture() {
        let compatibility = TargetCompatibilityConfig::default();
        assert!(compatibility.is_empty());

        let error = assess_preflight("linux-x64", &compatibility, &fingerprint("aarch64", "unknown"), false)
            .expect("an empty compatibility block must not skip the architecture check");

        assert_eq!(
            error.to_string(),
            "Platform error: Selected target RID 'linux-x64' does not match install host architecture 'aarch64'. Use --allow-platform-mismatch to override in an emergency."
        );
    }

    #[test]
    fn empty_compatibility_passes_matching_architecture() {
        let compatibility = TargetCompatibilityConfig::default();

        assert!(assess_preflight("linux-arm64", &compatibility, &fingerprint("aarch64", "unknown"), false).is_none());
    }

    #[test]
    fn empty_compatibility_passes_unparseable_rid() {
        let compatibility = TargetCompatibilityConfig::default();

        assert!(assess_preflight("custom-thing", &compatibility, &fingerprint("x86_64", "unknown"), false).is_none());
    }

    #[test]
    fn architecture_mismatch_can_be_overridden() {
        let compatibility = TargetCompatibilityConfig::default();

        assert!(assess_preflight("linux-x64", &compatibility, &fingerprint("aarch64", "unknown"), true).is_none());
    }

    #[test]
    fn architecture_mismatch_is_reported_before_declarative_incompatibility() {
        let compatibility = nvidia_only();

        let error = assess_preflight("linux-x64", &compatibility, &fingerprint("aarch64", "none"), false)
            .expect("a wrong-architecture package must be rejected");

        assert_eq!(
            error.to_string(),
            "Platform error: Selected target RID 'linux-x64' does not match install host architecture 'aarch64'. Use --allow-platform-mismatch to override in an emergency."
        );
    }

    #[test]
    fn declarative_incompatibility_is_still_enforced() {
        let compatibility = nvidia_only();

        let error = assess_preflight("linux-x64", &compatibility, &fingerprint("x86_64", "none"), false)
            .expect("a missing NVIDIA GPU must be rejected");

        assert!(error.to_string().contains("gpu vendor expected 'nvidia', got 'none'"));
    }

    #[test]
    fn declarative_incompatibility_can_be_overridden() {
        let compatibility = nvidia_only();

        assert!(assess_preflight("linux-x64", &compatibility, &fingerprint("x86_64", "none"), true).is_none());
    }

    #[test]
    fn declarative_compatibility_is_evaluated_on_match() {
        let compatibility = nvidia_only();

        assert!(assess_preflight("linux-x64", &compatibility, &fingerprint("x86_64", "nvidia"), false).is_none());
    }
}
