use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

use super::BundledArtifact;

pub(super) fn resolve_surge_dotnet_native_runtime_bundle(
    artifacts_path: &Path,
    rid: &str,
) -> Result<Option<BundledArtifact>> {
    let host_rid = crate::platform::detect::current_rid();
    let search_roots = surge_toolchain_search_roots(rid);
    resolve_surge_dotnet_native_runtime_bundle_with_host(artifacts_path, rid, &host_rid, &search_roots)
}

pub(super) fn resolve_surge_dotnet_native_runtime_bundle_with_host(
    artifacts_path: &Path,
    rid: &str,
    host_rid: &str,
    search_roots: &[PathBuf],
) -> Result<Option<BundledArtifact>> {
    if !artifacts_path.join("Surge.NET.dll").is_file() {
        return Ok(None);
    }

    let candidates = native_library_candidates_for_rid(rid);
    if candidates.iter().any(|name| artifacts_path.join(name).is_file()) {
        return Ok(None);
    }

    ensure_host_compatible_toolchain_runtime_rid(rid, host_rid)?;

    for root in search_roots {
        for candidate in &candidates {
            let source = root.join(candidate);
            if source.is_file() {
                return Ok(Some(BundledArtifact {
                    source,
                    archive_name: (*candidate).to_string(),
                }));
            }
        }
    }

    Err(SurgeError::Pack(format!(
        "Surge.NET.dll found in artifacts, but no native Surge runtime library for RID '{rid}' was found in the artifacts or next to an installed surge toolchain. Expected one of: {}. Use the official Surge release bundle for this platform or place the native runtime next to surge.",
        candidates.join(", ")
    )))
}

fn ensure_host_compatible_toolchain_runtime_rid(target_rid: &str, host_rid: &str) -> Result<()> {
    let target = parse_rid(target_rid).ok_or_else(|| {
        SurgeError::Pack(format!(
            "Unsupported target RID '{target_rid}'. Supported values use linux|win|windows|osx|macos and x86|x64|arm64."
        ))
    })?;
    let host = parse_rid(host_rid).ok_or_else(|| {
        SurgeError::Pack(format!(
            "Unsupported host RID '{host_rid}'. Host-only native runtime bundling is unavailable."
        ))
    })?;

    if target != host {
        return Err(SurgeError::Pack(format!(
            "Surge.NET native runtime bundling is host-only. Requested target RID '{target_rid}', but current host RID is '{host_rid}'. Include the native runtime in the artifacts to pack cross-target."
        )));
    }

    Ok(())
}

pub(super) fn native_library_candidates_for_rid(rid: &str) -> Vec<&'static str> {
    let os = rid.split('-').next().unwrap_or_default();
    match os {
        "linux" => vec!["libsurge.so", "surge.so"],
        "osx" | "macos" => vec!["libsurge.dylib", "surge.dylib"],
        "win" | "windows" => vec!["surge.dll", "libsurge.dll"],
        _ => vec![
            "libsurge.so",
            "surge.so",
            "libsurge.dylib",
            "surge.dylib",
            "surge.dll",
            "libsurge.dll",
        ],
    }
}

fn surge_toolchain_search_roots(rid: &str) -> Vec<PathBuf> {
    let mut roots = Vec::new();

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        roots.push(parent.to_path_buf());
    }

    let surge_name = surge_binary_name_for_rid(rid);
    if let Some(path_env) = std::env::var_os("PATH") {
        for path_dir in std::env::split_paths(&path_env) {
            if path_dir.join(surge_name).is_file() && !roots.iter().any(|existing| existing == &path_dir) {
                roots.push(path_dir);
            }
        }
    }

    roots
}

fn surge_binary_name_for_rid(rid: &str) -> &'static str {
    match rid.split('-').next().unwrap_or_default() {
        "win" | "windows" => "surge.exe",
        _ => "surge",
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RidOs {
    Linux,
    Windows,
    MacOs,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RidArch {
    X86,
    X64,
    Arm64,
}

fn parse_rid(rid: &str) -> Option<(RidOs, RidArch)> {
    let mut parts = rid.trim().split('-');
    let raw_os = parts.next()?;
    let raw_arch = parts.next()?;
    let os = match raw_os {
        "linux" => RidOs::Linux,
        "win" | "windows" => RidOs::Windows,
        "osx" | "macos" => RidOs::MacOs,
        _ => return None,
    };
    let arch = match raw_arch {
        "x86" => RidArch::X86,
        "x64" => RidArch::X64,
        "arm64" => RidArch::Arm64,
        _ => return None,
    };
    Some((os, arch))
}

pub(super) fn supervisor_binary_name_for_rid(rid: &str) -> &'static str {
    match rid.split('-').next().unwrap_or_default() {
        "win" | "windows" => "surge-supervisor.exe",
        "linux" | "osx" | "macos" => "surge-supervisor",
        _ => crate::platform::process::supervisor_binary_name(),
    }
}

pub(super) fn find_supervisor_binary(name: &str) -> Result<PathBuf> {
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let candidate = parent.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(SurgeError::Pack(format!(
        "Supervisor binary '{name}' is required (supervisor_id is configured) but was not found next to the surge binary. Use the official Surge release bundle for this platform or place '{name}' next to surge."
    )))
}
