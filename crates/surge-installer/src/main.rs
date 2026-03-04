#![forbid(unsafe_code)]

use std::process::ExitCode;

use surge_core::archive::extractor::extract_to;
use surge_core::error::{Result, SurgeError};
use surge_core::installer_bundle::read_embedded_payload;
use surge_core::platform::fs::make_executable;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let executable = std::env::current_exe()
        .map_err(|e| SurgeError::Pack(format!("Failed to locate installer executable path: {e}")))?;
    let payload = read_embedded_payload(&executable)?;

    let extracted = tempfile::tempdir().map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to create temporary extraction directory for '{}': {e}",
            executable.display()
        ))
    })?;
    extract_to(&payload, extracted.path(), None)?;

    let surge_binary = extracted.path().join(surge_binary_name_for_host());
    if !surge_binary.is_file() {
        return Err(SurgeError::Pack(format!(
            "Embedded installer payload is missing '{}'",
            surge_binary_name_for_host()
        )));
    }
    make_executable(&surge_binary)?;

    let passthrough_args: Vec<_> = std::env::args_os().skip(1).collect();
    let status = std::process::Command::new(&surge_binary)
        .arg("setup")
        .arg(extracted.path())
        .args(&passthrough_args)
        .status()
        .map_err(|e| {
            SurgeError::Pack(format!(
                "Failed to execute embedded surge setup binary '{}': {e}",
                surge_binary.display()
            ))
        })?;
    if !status.success() {
        return Err(SurgeError::Pack(format!(
            "Embedded setup process exited with status {status}"
        )));
    }

    Ok(())
}

fn surge_binary_name_for_host() -> &'static str {
    if cfg!(target_os = "windows") {
        "surge.exe"
    } else {
        "surge"
    }
}
