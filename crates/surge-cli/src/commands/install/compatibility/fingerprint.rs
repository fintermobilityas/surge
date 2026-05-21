use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::process::Command as StdCommand;

use surge_core::config::manifest::TargetCompatibilityConfig;
use surge_core::error::{Result, SurgeError};

use super::super::{remote, shell_single_quote};

const FINGERPRINT_BEGIN: &str = "__SURGE_FINGERPRINT_BEGIN__";
const OS_RELEASE_BEGIN: &str = "__SURGE_OS_RELEASE_BEGIN__";
const OS_RELEASE_END: &str = "__SURGE_OS_RELEASE_END__";
const FIELD_PREFIX: &str = "__SURGE_FIELD\t";
const FILE_PREFIX: &str = "__SURGE_FILE\t";
const PACKAGE_PREFIX: &str = "__SURGE_PACKAGE\t";
const PROBE_MISSING: &str = "__SURGE_MISSING__";
const PROBE_UNKNOWN: &str = "__SURGE_UNKNOWN__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ProbeValue {
    Present(String),
    Missing,
    Unknown,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RuntimeFingerprint {
    pub(super) os_release: BTreeMap<String, String>,
    pub(super) arch: String,
    pub(super) kernel: String,
    pub(super) gpu: String,
    pub(super) files: BTreeMap<String, ProbeValue>,
    pub(super) packages: BTreeMap<String, ProbeValue>,
}

pub(super) async fn collect_remote_runtime_fingerprint(
    ssh_target: &str,
    compatibility: &TargetCompatibilityConfig,
) -> Result<RuntimeFingerprint> {
    let probe = build_runtime_fingerprint_probe(compatibility);
    let command = format!("sh -c {}", shell_single_quote(&probe));
    let output = remote::run_tailscale_capture(&["ssh", ssh_target, command.as_str()]).await?;
    parse_runtime_fingerprint_output(&output)
}

pub(super) fn collect_local_runtime_fingerprint(compatibility: &TargetCompatibilityConfig) -> RuntimeFingerprint {
    let mut fingerprint = RuntimeFingerprint {
        os_release: parse_os_release(&std::fs::read_to_string("/etc/os-release").unwrap_or_default()),
        arch: command_stdout("uname", &["-m"]).unwrap_or_else(|| std::env::consts::ARCH.to_string()),
        kernel: command_stdout("uname", &["-r"]).unwrap_or_default(),
        gpu: detect_local_gpu_vendor(),
        files: BTreeMap::new(),
        packages: BTreeMap::new(),
    };

    for path in compatibility.files.keys() {
        let value = match std::fs::read_to_string(path) {
            Ok(content) => ProbeValue::Present(content),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ProbeValue::Missing,
            Err(_) => ProbeValue::Unknown,
        };
        fingerprint.files.insert(path.clone(), value);
    }

    for package in compatibility.packages.keys() {
        fingerprint
            .packages
            .insert(package.clone(), probe_local_package_version(package));
    }

    fingerprint
}

fn command_stdout(command: &str, args: &[&str]) -> Option<String> {
    let output = StdCommand::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn detect_local_gpu_vendor() -> String {
    if StdCommand::new("nvidia-smi")
        .arg("-L")
        .output()
        .is_ok_and(|output| output.status.success())
    {
        return "nvidia".to_string();
    }

    if let Some(lspci) = command_stdout("lspci", &[]) {
        let lower = lspci.to_ascii_lowercase();
        if lower.contains("nvidia") {
            return "nvidia".to_string();
        }
        if lower.contains("vga") || lower.contains("3d controller") || lower.contains("display controller") {
            return "other".to_string();
        }
        return "none".to_string();
    }

    "unknown".to_string()
}

fn probe_local_package_version(package: &str) -> ProbeValue {
    if command_exists("dpkg-query") {
        return command_stdout("dpkg-query", &["-W", "-f=${Version}", package])
            .map_or(ProbeValue::Missing, ProbeValue::Present);
    }
    if command_exists("rpm") {
        return command_stdout("rpm", &["-q", "--qf", "%{VERSION}-%{RELEASE}", package])
            .map_or(ProbeValue::Missing, ProbeValue::Present);
    }
    if command_exists("apk") {
        if !StdCommand::new("apk")
            .args(["info", "-e", package])
            .output()
            .is_ok_and(|output| output.status.success())
        {
            return ProbeValue::Missing;
        }
        return command_stdout("apk", &["info", "-v", package])
            .map(|value| {
                value
                    .lines()
                    .next()
                    .map_or_else(String::new, ToOwned::to_owned)
                    .trim_start_matches(package)
                    .trim_start_matches('-')
                    .to_string()
            })
            .filter(|value| !value.is_empty())
            .map_or(ProbeValue::Unknown, ProbeValue::Present);
    }
    ProbeValue::Unknown
}

fn command_exists(command: &str) -> bool {
    StdCommand::new(command)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn build_runtime_fingerprint_probe(compatibility: &TargetCompatibilityConfig) -> String {
    let mut script = String::from(
        r#"set +e
echo __SURGE_FINGERPRINT_BEGIN__
echo __SURGE_OS_RELEASE_BEGIN__
cat /etc/os-release 2>/dev/null || true
echo __SURGE_OS_RELEASE_END__
printf '__SURGE_FIELD\tarch\t%s\n' "$(uname -m 2>/dev/null || true)"
printf '__SURGE_FIELD\tkernel\t%s\n' "$(uname -r 2>/dev/null || true)"
gpu=unknown
if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L >/dev/null 2>&1; then
  gpu=nvidia
elif command -v lspci >/dev/null 2>&1; then
  pci="$(lspci 2>/dev/null || true)"
  if printf '%s\n' "$pci" | grep -qi nvidia; then
    gpu=nvidia
  elif printf '%s\n' "$pci" | grep -Eqi 'vga|3d controller|display controller'; then
    gpu=other
  else
    gpu=none
  fi
fi
printf '__SURGE_FIELD\tgpu\t%s\n' "$gpu"
probe_package_version() {
  pkg="$1"
  if command -v dpkg-query >/dev/null 2>&1; then
    dpkg-query -W -f='${Version}' "$pkg" 2>/dev/null || printf '%s' __SURGE_MISSING__
  elif command -v rpm >/dev/null 2>&1; then
    rpm -q --qf '%{VERSION}-%{RELEASE}' "$pkg" 2>/dev/null || printf '%s' __SURGE_MISSING__
  elif command -v apk >/dev/null 2>&1; then
    if apk info -e "$pkg" >/dev/null 2>&1; then
      apk info -v "$pkg" 2>/dev/null | sed -n '1p'
    else
      printf '%s' __SURGE_MISSING__
    fi
  else
    printf '%s' __SURGE_UNKNOWN__
  fi
}
"#,
    );

    for path in compatibility.files.keys() {
        let quoted_path = shell_single_quote(path);
        let display_path = path.replace('\t', " ");
        let quoted_display_path = shell_single_quote(&display_path);
        let _ = write!(
            script,
            "display_path={quoted_display_path}\nif [ -r {quoted_path} ]; then\n  value=\"$(head -c 65536 {quoted_path} 2>/dev/null | tr '\\n' '\\r')\"\n  printf '__SURGE_FILE\\t%s\\tpresent\\t%s\\n' \"$display_path\" \"$value\"\nelse\n  printf '__SURGE_FILE\\t%s\\tmissing\\t\\n' \"$display_path\"\nfi\n"
        );
    }

    for package in compatibility.packages.keys() {
        let quoted_package = shell_single_quote(package);
        let display_package = package.replace('\t', " ");
        let quoted_display_package = shell_single_quote(&display_package);
        let _ = write!(
            script,
            "display_package={quoted_display_package}\nvalue=\"$(probe_package_version {quoted_package})\"\nprintf '__SURGE_PACKAGE\\t%s\\t%s\\n' \"$display_package\" \"$value\"\n"
        );
    }

    script
}

fn parse_runtime_fingerprint_output(output: &str) -> Result<RuntimeFingerprint> {
    let mut fingerprint = RuntimeFingerprint::default();
    let mut os_release = String::new();
    let mut in_os_release = false;
    let mut saw_begin = false;

    for line in output.lines() {
        if line == FINGERPRINT_BEGIN {
            saw_begin = true;
            continue;
        }
        if line == OS_RELEASE_BEGIN {
            in_os_release = true;
            continue;
        }
        if line == OS_RELEASE_END {
            in_os_release = false;
            fingerprint.os_release = parse_os_release(&os_release);
            continue;
        }
        if in_os_release {
            os_release.push_str(line);
            os_release.push('\n');
            continue;
        }
        if let Some(rest) = line.strip_prefix(FIELD_PREFIX) {
            parse_field(rest, &mut fingerprint);
            continue;
        }
        if let Some(rest) = line.strip_prefix(FILE_PREFIX) {
            parse_file(rest, &mut fingerprint);
            continue;
        }
        if let Some(rest) = line.strip_prefix(PACKAGE_PREFIX) {
            parse_package(rest, &mut fingerprint);
        }
    }

    if !saw_begin {
        return Err(SurgeError::Platform(
            "Remote platform fingerprint probe did not return the expected marker".to_string(),
        ));
    }

    Ok(fingerprint)
}

fn parse_field(rest: &str, fingerprint: &mut RuntimeFingerprint) {
    let mut parts = rest.splitn(2, '\t');
    let key = parts.next().unwrap_or_default();
    let value = parts.next().unwrap_or_default().trim();
    match key {
        "arch" => fingerprint.arch = value.to_string(),
        "kernel" => fingerprint.kernel = value.to_string(),
        "gpu" => fingerprint.gpu = value.to_string(),
        _ => {}
    }
}

fn parse_file(rest: &str, fingerprint: &mut RuntimeFingerprint) {
    let mut parts = rest.splitn(3, '\t');
    let path = parts.next().unwrap_or_default().to_string();
    let state = parts.next().unwrap_or_default();
    let value = parts.next().unwrap_or_default().replace('\r', "\n");
    fingerprint.files.insert(path, parse_probe_value(state, &value));
}

fn parse_package(rest: &str, fingerprint: &mut RuntimeFingerprint) {
    let mut parts = rest.splitn(2, '\t');
    let package = parts.next().unwrap_or_default().to_string();
    let value = parts.next().unwrap_or_default();
    fingerprint
        .packages
        .insert(package, parse_probe_value("present", value));
}

fn parse_probe_value(state: &str, value: &str) -> ProbeValue {
    match state {
        "missing" => ProbeValue::Missing,
        "present" if value.trim() == PROBE_MISSING => ProbeValue::Missing,
        "present" if value.trim() == PROBE_UNKNOWN => ProbeValue::Unknown,
        "present" => ProbeValue::Present(value.trim().to_string()),
        _ => ProbeValue::Unknown,
    }
}

fn parse_os_release(contents: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for line in contents.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        values.insert(key.to_string(), unquote_os_release_value(value.trim()));
    }
    values
}

fn unquote_os_release_value(value: &str) -> String {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"')) || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_remote_fingerprint_output() {
        let output = "\
noise
__SURGE_FINGERPRINT_BEGIN__
__SURGE_OS_RELEASE_BEGIN__
ID=ubuntu
VERSION_ID=\"24.04\"
__SURGE_OS_RELEASE_END__
__SURGE_FIELD\tarch\tx86_64
__SURGE_FIELD\tkernel\t6.8.0
__SURGE_FIELD\tgpu\tnvidia
__SURGE_FILE\t/etc/example_runtime_release\tpresent\tR35.4 REVISION: 1.0
__SURGE_PACKAGE\texample-dnn-runtime\t8.4.1-1
";

        let fingerprint = parse_runtime_fingerprint_output(output).unwrap();

        assert_eq!(fingerprint.os_release.get("ID").map(String::as_str), Some("ubuntu"));
        assert_eq!(fingerprint.arch, "x86_64");
        assert_eq!(
            fingerprint.files.get("/etc/example_runtime_release"),
            Some(&ProbeValue::Present("R35.4 REVISION: 1.0".to_string()))
        );
        assert_eq!(
            fingerprint.packages.get("example-dnn-runtime"),
            Some(&ProbeValue::Present("8.4.1-1".to_string()))
        );
    }
}
