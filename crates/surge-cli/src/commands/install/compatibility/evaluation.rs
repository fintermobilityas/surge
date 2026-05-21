use surge_core::config::manifest::{GpuCompatibilityConfig, OsReleaseCompatibilityConfig, TargetCompatibilityConfig};

use super::fingerprint::{ProbeValue, RuntimeFingerprint};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompatibilityVerdict {
    Compatible,
    Incompatible,
    Unknown,
}

impl CompatibilityVerdict {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Compatible => "compatible",
            Self::Incompatible => "incompatible",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompatibilityEvaluation {
    pub(super) verdict: CompatibilityVerdict,
    pub(super) reasons: Vec<String>,
}

pub(super) fn evaluate_compatibility(
    compatibility: &TargetCompatibilityConfig,
    fingerprint: &RuntimeFingerprint,
) -> CompatibilityEvaluation {
    let mut incompatible = Vec::new();
    let mut unknown = Vec::new();
    let mut compatible = Vec::new();

    if let Some(os_release) = &compatibility.os_release {
        evaluate_os_release(
            os_release,
            fingerprint,
            &mut compatible,
            &mut incompatible,
            &mut unknown,
        );
    }
    if let Some(gpu) = &compatibility.gpu {
        evaluate_gpu(gpu, fingerprint, &mut compatible, &mut incompatible, &mut unknown);
    }
    for (path, pattern) in &compatibility.files {
        evaluate_probe_value(
            &format!("file '{path}'"),
            pattern,
            fingerprint.files.get(path),
            &mut compatible,
            &mut incompatible,
            &mut unknown,
        );
    }
    for (package, pattern) in &compatibility.packages {
        evaluate_probe_value(
            &format!("package '{package}'"),
            pattern,
            fingerprint.packages.get(package),
            &mut compatible,
            &mut incompatible,
            &mut unknown,
        );
    }

    if incompatible.is_empty() {
        if unknown.is_empty() {
            CompatibilityEvaluation {
                verdict: CompatibilityVerdict::Compatible,
                reasons: compatible,
            }
        } else {
            CompatibilityEvaluation {
                verdict: CompatibilityVerdict::Unknown,
                reasons: unknown,
            }
        }
    } else {
        CompatibilityEvaluation {
            verdict: CompatibilityVerdict::Incompatible,
            reasons: incompatible,
        }
    }
}

fn evaluate_os_release(
    expected: &OsReleaseCompatibilityConfig,
    fingerprint: &RuntimeFingerprint,
    compatible: &mut Vec<String>,
    incompatible: &mut Vec<String>,
    unknown: &mut Vec<String>,
) {
    if let Some(expected_id) = expected.id.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        match fingerprint.os_release.get("ID") {
            Some(actual_id) if os_id_matches(expected_id, actual_id, fingerprint.os_release.get("ID_LIKE")) => {
                compatible.push(format!("os-release id '{actual_id}' matches '{expected_id}'"));
            }
            Some(actual_id) => incompatible.push(format!("os-release id expected '{expected_id}', got '{actual_id}'")),
            None => unknown.push(format!(
                "os-release id expected '{expected_id}', but /etc/os-release did not expose ID"
            )),
        }
    }

    if let Some(expected_version) = expected
        .version_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match fingerprint.os_release.get("VERSION_ID") {
            Some(actual_version) if wildcard_matches(expected_version, actual_version) => {
                compatible.push(format!(
                    "os-release version-id '{actual_version}' matches '{expected_version}'"
                ));
            }
            Some(actual_version) => incompatible.push(format!(
                "os-release version-id expected '{expected_version}', got '{actual_version}'"
            )),
            None => unknown.push(format!(
                "os-release version-id expected '{expected_version}', but /etc/os-release did not expose VERSION_ID"
            )),
        }
    }

    if let Some(expected_id_like) = expected
        .id_like
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match fingerprint.os_release.get("ID_LIKE") {
            Some(actual_id_like) if os_id_like_matches(expected_id_like, actual_id_like) => {
                compatible.push(format!(
                    "os-release id-like '{actual_id_like}' contains '{expected_id_like}'"
                ));
            }
            Some(actual_id_like) => incompatible.push(format!(
                "os-release id-like expected '{expected_id_like}', got '{actual_id_like}'"
            )),
            None => unknown.push(format!(
                "os-release id-like expected '{expected_id_like}', but /etc/os-release did not expose ID_LIKE"
            )),
        }
    }
}

fn os_id_matches(expected: &str, actual: &str, id_like: Option<&String>) -> bool {
    wildcard_matches(&expected.to_ascii_lowercase(), &actual.to_ascii_lowercase())
        || id_like.is_some_and(|value| os_id_like_matches(expected, value))
}

fn os_id_like_matches(expected: &str, actual_id_like: &str) -> bool {
    let expected = expected.to_ascii_lowercase();
    actual_id_like
        .split_whitespace()
        .any(|part| wildcard_matches(&expected, &part.to_ascii_lowercase()))
}

fn evaluate_gpu(
    expected: &GpuCompatibilityConfig,
    fingerprint: &RuntimeFingerprint,
    compatible: &mut Vec<String>,
    incompatible: &mut Vec<String>,
    unknown: &mut Vec<String>,
) {
    let Some(expected_vendor) = expected
        .vendor
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let expected_vendor = expected_vendor.to_ascii_lowercase();
    let actual = fingerprint.gpu.trim().to_ascii_lowercase();
    if actual.is_empty() || actual == "unknown" {
        unknown.push(format!(
            "gpu vendor expected '{expected_vendor}', but GPU vendor could not be detected"
        ));
    } else if expected_vendor == "required" {
        if actual == "none" {
            incompatible.push("gpu required, but no GPU was detected".to_string());
        } else {
            compatible.push(format!("gpu vendor '{actual}' satisfies required GPU"));
        }
    } else if actual == expected_vendor {
        compatible.push(format!("gpu vendor '{actual}' matches '{expected_vendor}'"));
    } else {
        incompatible.push(format!("gpu vendor expected '{expected_vendor}', got '{actual}'"));
    }
}

fn evaluate_probe_value(
    label: &str,
    pattern: &str,
    actual: Option<&ProbeValue>,
    compatible: &mut Vec<String>,
    incompatible: &mut Vec<String>,
    unknown: &mut Vec<String>,
) {
    match actual {
        Some(ProbeValue::Present(value)) if wildcard_matches(pattern, value) => {
            compatible.push(format!("{label} matches '{pattern}'"));
        }
        Some(ProbeValue::Present(value)) => incompatible.push(format!(
            "{label} expected '{pattern}', got '{}'",
            trim_for_display(value)
        )),
        Some(ProbeValue::Missing) => incompatible.push(format!("{label} is missing")),
        Some(ProbeValue::Unknown) | None => unknown.push(format!("{label} could not be probed")),
    }
}

fn trim_for_display(value: &str) -> String {
    const MAX_DISPLAY_LEN: usize = 160;
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.len() <= MAX_DISPLAY_LEN {
        collapsed
    } else {
        format!("{}...", &collapsed[..MAX_DISPLAY_LEN])
    }
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let mut pattern_index = 0;
    let mut value_index = 0;
    let mut star_index = None;
    let mut star_value_index = 0;

    while value_index < value.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == b'?' || pattern[pattern_index] == value[value_index])
        {
            pattern_index += 1;
            value_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
            star_index = Some(pattern_index);
            star_value_index = value_index;
            pattern_index += 1;
        } else if let Some(star) = star_index {
            pattern_index = star + 1;
            star_value_index += 1;
            value_index = star_value_index;
        } else {
            return false;
        }
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }

    pattern_index == pattern.len()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn compatibility() -> TargetCompatibilityConfig {
        let mut files = BTreeMap::new();
        files.insert(
            "/etc/example_runtime_release".to_string(),
            "R35.*REVISION: 1.*".to_string(),
        );
        let mut packages = BTreeMap::new();
        packages.insert("example-dnn-runtime".to_string(), "8.4.*".to_string());
        TargetCompatibilityConfig {
            os_release: Some(OsReleaseCompatibilityConfig {
                id: Some("ubuntu".to_string()),
                version_id: Some("24.04".to_string()),
                id_like: None,
            }),
            gpu: Some(GpuCompatibilityConfig {
                vendor: Some("nvidia".to_string()),
            }),
            files,
            packages,
        }
    }

    fn compatible_fingerprint() -> RuntimeFingerprint {
        let mut os_release = BTreeMap::new();
        os_release.insert("ID".to_string(), "ubuntu".to_string());
        os_release.insert("VERSION_ID".to_string(), "24.04".to_string());
        let mut files = BTreeMap::new();
        files.insert(
            "/etc/example_runtime_release".to_string(),
            ProbeValue::Present("R35.4.1 REVISION: 1.2".to_string()),
        );
        let mut packages = BTreeMap::new();
        packages.insert(
            "example-dnn-runtime".to_string(),
            ProbeValue::Present("8.4.2-1".to_string()),
        );
        RuntimeFingerprint {
            os_release,
            arch: "x86_64".to_string(),
            kernel: "6.8.0".to_string(),
            gpu: "nvidia".to_string(),
            files,
            packages,
        }
    }

    #[test]
    fn evaluates_compatible_runtime() {
        let evaluation = evaluate_compatibility(&compatibility(), &compatible_fingerprint());

        assert_eq!(evaluation.verdict, CompatibilityVerdict::Compatible);
    }

    #[test]
    fn detects_explicit_incompatibility() {
        let mut fingerprint = compatible_fingerprint();
        fingerprint
            .os_release
            .insert("VERSION_ID".to_string(), "22.04".to_string());

        let evaluation = evaluate_compatibility(&compatibility(), &fingerprint);

        assert_eq!(evaluation.verdict, CompatibilityVerdict::Incompatible);
        assert!(
            evaluation
                .reasons
                .iter()
                .any(|reason| reason.contains("version-id expected '24.04'"))
        );
    }

    #[test]
    fn detects_missing_runtime_marker() {
        let mut fingerprint = compatible_fingerprint();
        fingerprint
            .files
            .insert("/etc/example_runtime_release".to_string(), ProbeValue::Missing);

        let evaluation = evaluate_compatibility(&compatibility(), &fingerprint);

        assert_eq!(evaluation.verdict, CompatibilityVerdict::Incompatible);
        assert!(
            evaluation
                .reasons
                .iter()
                .any(|reason| reason == "file '/etc/example_runtime_release' is missing")
        );
    }

    #[test]
    fn reports_unknown_when_package_manager_cannot_probe() {
        let mut fingerprint = compatible_fingerprint();
        fingerprint
            .packages
            .insert("example-dnn-runtime".to_string(), ProbeValue::Unknown);

        let evaluation = evaluate_compatibility(&compatibility(), &fingerprint);

        assert_eq!(evaluation.verdict, CompatibilityVerdict::Unknown);
    }

    #[test]
    fn wildcard_supports_runtime_patterns() {
        assert!(wildcard_matches("8.4.*", "8.4.2-1"));
        assert!(wildcard_matches("R35.*REVISION: 1.*", "R35.4 REVISION: 1.0"));
        assert!(!wildcard_matches("8.4.*", "8.5.0"));
    }
}
