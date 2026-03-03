#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Os {
    Windows,
    Linux,
    MacOs,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    X86_64,
    Arm64,
    X86,
    Unknown,
}

#[must_use]
pub fn current_os() -> Os {
    if cfg!(target_os = "windows") {
        Os::Windows
    } else if cfg!(target_os = "linux") {
        Os::Linux
    } else if cfg!(target_os = "macos") {
        Os::MacOs
    } else {
        Os::Unknown
    }
}

#[must_use]
pub fn current_arch() -> Arch {
    if cfg!(target_arch = "x86_64") {
        Arch::X86_64
    } else if cfg!(target_arch = "aarch64") {
        Arch::Arm64
    } else if cfg!(target_arch = "x86") {
        Arch::X86
    } else {
        Arch::Unknown
    }
}

/// Returns a Runtime Identifier like `"linux-x64"` or `"win-arm64"`.
#[must_use]
pub fn current_rid() -> String {
    let os = match current_os() {
        Os::Windows => "win",
        Os::Linux => "linux",
        Os::MacOs => "osx",
        Os::Unknown => "unknown",
    };
    let arch = match current_arch() {
        Arch::X86_64 => "x64",
        Arch::Arm64 => "arm64",
        Arch::X86 => "x86",
        Arch::Unknown => "unknown",
    };
    format!("{os}-{arch}")
}

#[must_use]
pub fn cpu_count() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_rid_format() {
        let rid = current_rid();
        assert!(rid.contains('-'), "RID should contain a dash: {rid}");
    }

    #[test]
    fn test_cpu_count_positive() {
        assert!(cpu_count() > 0);
    }
}
