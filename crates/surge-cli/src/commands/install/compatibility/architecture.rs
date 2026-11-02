#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ArchitectureVerdict {
    Match,
    Mismatch,
    Unknown,
}

impl ArchitectureVerdict {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::Mismatch => "mismatch",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ArchitectureCheck {
    pub(super) verdict: ArchitectureVerdict,
    pub(super) rid_arch: Option<&'static str>,
    pub(super) host_arch: Option<&'static str>,
    pub(super) reason: String,
}

/// Compares the architecture encoded in the selected target RID against the
/// architecture reported by the install host (`uname -m`).
pub(super) fn check_rid_architecture(selected_rid: &str, host_arch: &str) -> ArchitectureCheck {
    let rid = selected_rid.trim();
    let host = host_arch.trim();
    let rid_arch = parse_rid_architecture(rid);
    let host_arch_normalized = normalize_architecture(host);

    let (verdict, reason) = match (rid_arch, host_arch_normalized) {
        (Some(rid_arch), Some(host_normalized)) if rid_arch == host_normalized => (
            ArchitectureVerdict::Match,
            format!("target RID architecture '{rid_arch}' matches install host architecture '{host}'"),
        ),
        (Some(rid_arch), Some(_)) => (
            ArchitectureVerdict::Mismatch,
            format!("target RID '{rid}' is built for '{rid_arch}' but the install host reports '{host}'"),
        ),
        (None, _) => (
            ArchitectureVerdict::Unknown,
            format!("target RID '{rid}' does not encode a recognizable architecture"),
        ),
        (Some(_), None) if host.is_empty() => (
            ArchitectureVerdict::Unknown,
            "install host architecture could not be detected".to_string(),
        ),
        (Some(rid_arch), None) => (
            ArchitectureVerdict::Unknown,
            format!(
                "install host architecture '{host}' is not recognized, so target RID architecture '{rid_arch}' cannot be verified"
            ),
        ),
    };

    ArchitectureCheck {
        verdict,
        rid_arch,
        host_arch: host_arch_normalized,
        reason,
    }
}

/// RIDs look like `<os>-<arch>` with optional trailing qualifiers, so the
/// architecture is the first dash-separated segment that normalizes.
fn parse_rid_architecture(selected_rid: &str) -> Option<&'static str> {
    selected_rid
        .trim()
        .to_ascii_lowercase()
        .split('-')
        .find_map(normalize_architecture)
}

fn normalize_architecture(token: &str) -> Option<&'static str> {
    match token.trim().to_ascii_lowercase().as_str() {
        "x64" | "x86_64" | "amd64" => Some("x64"),
        "arm64" | "aarch64" => Some("arm64"),
        "x86" | "i386" | "i486" | "i586" | "i686" => Some("x86"),
        "arm" | "armv7l" | "armv6l" | "armhf" | "armel" => Some("arm"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_mismatch_between_rid_and_host_arch() {
        let check = check_rid_architecture("linux-x64", "aarch64");

        assert_eq!(check.verdict, ArchitectureVerdict::Mismatch);
        assert_eq!(check.rid_arch, Some("x64"));
        assert_eq!(check.host_arch, Some("arm64"));
        assert_eq!(
            check.reason,
            "target RID 'linux-x64' is built for 'x64' but the install host reports 'aarch64'"
        );
    }

    #[test]
    fn matches_arm64_rid_on_arm64_host() {
        let check = check_rid_architecture("linux-arm64", "aarch64");

        assert_eq!(check.verdict, ArchitectureVerdict::Match);
        assert_eq!(check.rid_arch, Some("arm64"));
        assert_eq!(check.host_arch, Some("arm64"));
    }

    #[test]
    fn matches_architecture_when_rid_has_trailing_qualifiers() {
        let cuda = check_rid_architecture("linux-x64-cuda", "x86_64");
        let nvidia = check_rid_architecture("linux-arm64-nvidia", "aarch64");
        let cpu = check_rid_architecture("linux-x64-cpu", "amd64");

        assert_eq!(cuda.verdict, ArchitectureVerdict::Match);
        assert_eq!(cuda.rid_arch, Some("x64"));
        assert_eq!(nvidia.verdict, ArchitectureVerdict::Match);
        assert_eq!(nvidia.rid_arch, Some("arm64"));
        assert_eq!(cpu.verdict, ArchitectureVerdict::Match);
    }

    #[test]
    fn matches_windows_and_macos_rids() {
        assert_eq!(
            check_rid_architecture("win-x64", "amd64").verdict,
            ArchitectureVerdict::Match
        );
        assert_eq!(
            check_rid_architecture("osx-arm64", "arm64").verdict,
            ArchitectureVerdict::Match
        );
    }

    #[test]
    fn matches_32_bit_architectures() {
        let arm = check_rid_architecture("linux-arm", "armv7l");
        let x86 = check_rid_architecture("linux-x86", "i686");

        assert_eq!(arm.verdict, ArchitectureVerdict::Match);
        assert_eq!(arm.rid_arch, Some("arm"));
        assert_eq!(arm.host_arch, Some("arm"));
        assert_eq!(x86.verdict, ArchitectureVerdict::Match);
        assert_eq!(x86.rid_arch, Some("x86"));
    }

    #[test]
    fn reports_unknown_when_host_arch_is_empty() {
        let check = check_rid_architecture("linux-x64", "");

        assert_eq!(check.verdict, ArchitectureVerdict::Unknown);
        assert_eq!(check.rid_arch, Some("x64"));
        assert_eq!(check.host_arch, None);
        assert_eq!(check.reason, "install host architecture could not be detected");
    }

    #[test]
    fn reports_unknown_when_host_arch_is_unrecognized() {
        let check = check_rid_architecture("linux-x64", "riscv64");

        assert_eq!(check.verdict, ArchitectureVerdict::Unknown);
        assert_eq!(check.host_arch, None);
        assert!(check.reason.contains("riscv64"));
        assert!(check.reason.contains("'x64'"));
    }

    #[test]
    fn reports_unknown_when_rid_has_no_architecture() {
        let check = check_rid_architecture("weird-rid", "x86_64");

        assert_eq!(check.verdict, ArchitectureVerdict::Unknown);
        assert_eq!(check.rid_arch, None);
        assert_eq!(check.host_arch, Some("x64"));
        assert_eq!(
            check.reason,
            "target RID 'weird-rid' does not encode a recognizable architecture"
        );
    }

    #[test]
    fn compares_architecture_case_insensitively() {
        let check = check_rid_architecture("LINUX-X64", "X86_64");

        assert_eq!(check.verdict, ArchitectureVerdict::Match);
        assert_eq!(check.rid_arch, Some("x64"));
        assert_eq!(check.host_arch, Some("x64"));
    }

    #[test]
    fn ignores_surrounding_whitespace() {
        let check = check_rid_architecture("  linux-arm64  ", " aarch64\n");

        assert_eq!(check.verdict, ArchitectureVerdict::Match);
    }

    #[test]
    fn reports_mismatch_across_architecture_families() {
        assert_eq!(
            check_rid_architecture("linux-arm", "x86_64").verdict,
            ArchitectureVerdict::Mismatch
        );
        assert_eq!(
            check_rid_architecture("linux-x86", "aarch64").verdict,
            ArchitectureVerdict::Mismatch
        );
    }
}
