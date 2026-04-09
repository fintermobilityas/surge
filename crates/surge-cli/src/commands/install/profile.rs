#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RuntimeProfile {
    pub(super) os: String,
    pub(super) arch: String,
    pub(super) gpu: String,
}

impl RuntimeProfile {
    pub(super) fn has_nvidia_gpu(&self) -> bool {
        let gpu = self.gpu.trim().to_ascii_lowercase();
        gpu == "nvidia" || gpu == "true" || gpu == "yes"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RidSignature {
    pub(super) os: &'static str,
    pub(super) arch: &'static str,
    pub(super) has_gpu_hint: bool,
}

pub(super) fn detect_local_profile() -> RuntimeProfile {
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();
    let gpu = if has_local_nvidia_gpu() {
        "nvidia".to_string()
    } else {
        "none".to_string()
    };
    RuntimeProfile { os, arch, gpu }
}

fn has_local_nvidia_gpu() -> bool {
    std::process::Command::new("nvidia-smi")
        .arg("-L")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

pub(super) fn warn_if_local_rid_looks_incompatible(rid: &str, profile: &RuntimeProfile) {
    for warning in local_rid_incompatibility_warnings(rid, profile) {
        crate::logline::warn(&warning);
    }
}

pub(super) fn local_rid_incompatibility_warnings(rid: &str, profile: &RuntimeProfile) -> Vec<String> {
    let Some(selected) = parse_rid_signature(rid) else {
        return Vec::new();
    };
    let Some(local_os) = normalize_os(&profile.os) else {
        return Vec::new();
    };
    let Some(local_arch) = normalize_arch(&profile.arch) else {
        return Vec::new();
    };

    let mut warnings = Vec::new();
    if selected.os != local_os {
        warnings.push(format!(
            "Selected RID '{rid}' targets OS '{}', but local host OS appears '{}'.",
            selected.os, local_os
        ));
    }
    if selected.arch != local_arch {
        warnings.push(format!(
            "Selected RID '{rid}' targets architecture '{}', but local host architecture appears '{}'.",
            selected.arch, local_arch
        ));
    }
    if selected.has_gpu_hint && !profile.has_nvidia_gpu() {
        warnings.push(format!(
            "Selected RID '{rid}' implies GPU acceleration, but no local NVIDIA GPU was detected."
        ));
    }
    warnings
}

pub(super) fn parse_rid_signature(rid: &str) -> Option<RidSignature> {
    let mut parts = rid.trim().split('-');
    let raw_os = parts.next()?.trim().to_ascii_lowercase();
    let os = match raw_os.as_str() {
        "linux" => "linux",
        "win" | "windows" => "win",
        "osx" | "macos" | "darwin" => "osx",
        _ => normalize_os(raw_os.as_str())?,
    };
    let arch = normalize_arch(parts.next()?)?;
    let has_gpu_hint = parts.any(|part| {
        let part = part.trim().to_ascii_lowercase();
        part == "cuda" || part == "nvidia" || part == "gpu"
    });
    Some(RidSignature { os, arch, has_gpu_hint })
}

pub(super) fn derive_base_rid(profile: &RuntimeProfile) -> Option<String> {
    let os = normalize_os(&profile.os)?;
    let arch = normalize_arch(&profile.arch)?;
    Some(format!("{os}-{arch}"))
}

fn normalize_os(raw: &str) -> Option<&'static str> {
    let os = raw.trim().to_ascii_lowercase();
    if os.contains("linux") {
        Some("linux")
    } else if os.contains("darwin") || os.contains("mac") {
        Some("osx")
    } else if os.contains("windows") || os.contains("mingw") || os.contains("msys") {
        Some("win")
    } else {
        None
    }
}

fn normalize_arch(raw: &str) -> Option<&'static str> {
    let arch = raw.trim().to_ascii_lowercase();
    if arch == "x86_64" || arch == "amd64" || arch == "x64" {
        Some("x64")
    } else if arch == "aarch64" || arch == "arm64" {
        Some("arm64")
    } else if arch == "x86" || arch == "i386" || arch == "i686" {
        Some("x86")
    } else {
        None
    }
}

pub(super) fn build_rid_candidates(base_rid: &str, nvidia_gpu: bool) -> Vec<String> {
    let mut candidates: Vec<String> = Vec::new();
    let mut push_unique = |candidate: String| {
        if !candidates.iter().any(|existing| existing == &candidate) {
            candidates.push(candidate);
        }
    };

    if nvidia_gpu {
        push_unique(format!("{base_rid}-nvidia"));
        push_unique(format!("{base_rid}-cuda"));
        push_unique(format!("{base_rid}-gpu"));
    }
    push_unique(base_rid.to_string());
    if !nvidia_gpu {
        push_unique(format!("{base_rid}-cpu"));
    }

    candidates
}
