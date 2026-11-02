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
        (Some(rid_arch), Some(host_normalized)) if host_can_execute(parse_rid_os(rid), rid_arch, host_normalized) => (
            ArchitectureVerdict::Match,
            format!(
                "install host architecture '{host}' can execute target RID architecture '{rid_arch}' for RID '{rid}'"
            ),
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

/// A different architecture is not automatically an incompatibility: a host can
/// execute some other architectures natively or under emulation. Only pairs that
/// no host mechanism can run must block an install.
fn host_can_execute(rid_os: Option<&'static str>, rid_arch: &str, host_arch: &str) -> bool {
    match (host_arch, rid_arch) {
        // 32-bit userland on a 64-bit host: WOW64 on Windows, multilib on Linux,
        // and a 32-bit armhf userland on an aarch64 kernel.
        ("x64", "x86") | ("arm64", "arm") => true,
        // x64 emulation on 64-bit ARM: Rosetta on macOS, and the x64 emulator on
        // Windows on ARM. There is no such layer for Linux, which is the case the
        // architecture guard exists to catch.
        ("arm64", "x64") => matches!(rid_os, Some("osx" | "win")),
        _ => false,
    }
}

/// The OS segment of a RID, needed because x64-on-arm64 emulation exists only on
/// some platforms.
fn parse_rid_os(selected_rid: &str) -> Option<&'static str> {
    selected_rid
        .trim()
        .to_ascii_lowercase()
        .split('-')
        .find_map(|segment| match segment {
            "linux" => Some("linux"),
            "win" | "windows" => Some("win"),
            "osx" | "macos" | "darwin" => Some("osx"),
            _ => None,
        })
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
    fn accepts_32_bit_target_on_a_64_bit_host() {
        // WOW64 on Windows and multilib on Linux run x86 on x64.
        assert_eq!(
            check_rid_architecture("win-x86", "x86_64").verdict,
            ArchitectureVerdict::Match
        );
        // A 32-bit armhf userland on an aarch64 kernel reports aarch64.
        assert_eq!(
            check_rid_architecture("linux-arm", "aarch64").verdict,
            ArchitectureVerdict::Match
        );
    }

    #[test]
    fn accepts_x64_on_arm64_only_where_emulation_exists() {
        // Rosetta on macOS and the x64 emulator on Windows on ARM.
        assert_eq!(
            check_rid_architecture("osx-x64", "arm64").verdict,
            ArchitectureVerdict::Match
        );
        assert_eq!(
            check_rid_architecture("win-x64", "aarch64").verdict,
            ArchitectureVerdict::Match
        );
        // Linux has no such layer: this is the case the guard exists to catch.
        assert_eq!(
            check_rid_architecture("linux-x64", "aarch64").verdict,
            ArchitectureVerdict::Mismatch
        );
    }

    #[test]
    fn still_rejects_pairs_no_host_can_execute() {
        // A 64-bit target on a 32-bit host cannot run anywhere.
        assert_eq!(
            check_rid_architecture("linux-x64", "i686").verdict,
            ArchitectureVerdict::Mismatch
        );
        assert_eq!(
            check_rid_architecture("linux-arm64", "armv7l").verdict,
            ArchitectureVerdict::Mismatch
        );
        // Cross-family without emulation.
        assert_eq!(
            check_rid_architecture("linux-arm64", "x86_64").verdict,
            ArchitectureVerdict::Mismatch
        );
        assert_eq!(
            check_rid_architecture("linux-x86", "aarch64").verdict,
            ArchitectureVerdict::Mismatch
        );
    }

    #[test]
    fn parses_the_rid_os_segment() {
        assert_eq!(parse_rid_os("linux-x64-cuda"), Some("linux"));
        assert_eq!(parse_rid_os("win-x64"), Some("win"));
        assert_eq!(parse_rid_os("osx-arm64"), Some("osx"));
        assert_eq!(parse_rid_os("weird-rid"), None);
    }

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
