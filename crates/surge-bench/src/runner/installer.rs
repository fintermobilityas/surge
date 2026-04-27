use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use surge_core::archive::packer::ArchivePacker;
use surge_core::error::{Result, SurgeError};
use surge_core::installer_bundle;
use surge_core::platform::detect::current_rid;
use surge_core::platform::fs::make_executable;

use crate::payload::PayloadTemplate;
use crate::report::BenchmarkResult;

use super::fs_compare::{assert_directories_match, dir_size_recursive};
use super::manifest::{installer_manifest, version_label, write_bench_manifest};
use super::update::{configure_benchmark_context, publish_release};
use super::{BENCH_APP_ID, time};

fn surge_binary_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge.exe"
    } else {
        "surge"
    }
}

fn installer_launcher_name_for_rid(rid: &str) -> &'static str {
    if rid.starts_with("win-") || rid.starts_with("windows-") {
        "surge-installer.exe"
    } else {
        "surge-installer"
    }
}

fn resolve_tool_binary(env_var: &str, binary_name: &str) -> Result<PathBuf> {
    if let Ok(path) = std::env::var(env_var) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        return Err(SurgeError::Pack(format!(
            "{env_var} points to '{}' which does not exist",
            candidate.display()
        )));
    }

    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        let candidate = parent.join(binary_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    if let Some(path_env) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_env) {
            let candidate = dir.join(binary_name);
            if candidate.is_file() {
                return Ok(candidate);
            }
        }
    }

    Err(SurgeError::Pack(format!(
        "Required benchmark helper '{binary_name}' was not found. Put it next to surge-bench, add it to PATH, or set {env_var}."
    )))
}

fn build_console_installer(
    output_dir: &Path,
    store_dir: &Path,
    app_id: &str,
    rid: &str,
    version: &str,
    full_package_path: &Path,
    installer_type: &str,
    install_root: &Path,
    surge_binary: &Path,
    launcher: &Path,
    compression_level: i32,
) -> Result<(PathBuf, BenchmarkResult)> {
    let full_filename = full_package_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| {
            SurgeError::Pack(format!(
                "Invalid full package path (missing filename): {}",
                full_package_path.display()
            ))
        })?;
    fs::create_dir_all(output_dir)?;

    let installer_ext = if rid.starts_with("win-") || rid.starts_with("windows-") {
        "exe"
    } else {
        "bin"
    };
    let installer_path = output_dir.join(format!("Setup-{rid}-{installer_type}.{installer_ext}"));
    let staged_surge_name = surge_binary_name_for_rid(rid);
    let input_size = fs::metadata(full_package_path).map_or(0, |meta| meta.len());

    let (build_result, duration) = time(|| -> Result<()> {
        let staging_dir = tempfile::tempdir()
            .map_err(|e| SurgeError::Pack(format!("Failed to create installer staging directory: {e}")))?;
        let staging = staging_dir.path();
        let manifest = installer_manifest(
            store_dir,
            app_id,
            rid,
            version,
            &full_filename,
            installer_type,
            install_root,
        );
        let manifest_yaml = serde_yaml::to_string(&manifest)
            .map_err(|e| SurgeError::Pack(format!("Failed to serialize installer manifest: {e}")))?;
        fs::write(staging.join("installer.yml"), manifest_yaml.as_bytes())?;

        let staged_surge = staging.join(staged_surge_name);
        fs::copy(surge_binary, &staged_surge).map_err(|e| {
            SurgeError::Pack(format!(
                "Failed to copy surge binary '{}' into installer staging: {e}",
                surge_binary.display()
            ))
        })?;
        make_executable(&staged_surge)?;

        if installer_type == "offline" {
            let payload_dir = staging.join("payload");
            fs::create_dir_all(&payload_dir)?;
            fs::copy(full_package_path, payload_dir.join(&full_filename)).map_err(|e| {
                SurgeError::Pack(format!(
                    "Failed to copy full package '{}' into offline installer staging: {e}",
                    full_package_path.display()
                ))
            })?;
        }

        let payload_archive = tempfile::NamedTempFile::new()
            .map_err(|e| SurgeError::Pack(format!("Failed to create installer payload archive temp file: {e}")))?;
        let mut payload_packer = ArchivePacker::new(compression_level)?;
        payload_packer.add_directory(staging, "")?;
        payload_packer.finalize_to_file(payload_archive.path())?;

        installer_bundle::write_embedded_installer(launcher, payload_archive.path(), &installer_path)?;
        make_executable(&installer_path)?;
        Ok(())
    });
    build_result?;
    let output_size = fs::metadata(&installer_path).map_or(0, |meta| meta.len());

    Ok((
        installer_path,
        BenchmarkResult {
            name: format!("Installer create ({installer_type})"),
            duration,
            input_size,
            output_size,
        },
    ))
}

fn run_console_installer(
    installer_path: &Path,
    install_root: &Path,
    expected_payload_dir: &Path,
    installer_type: &str,
    home_dir: &Path,
) -> Result<BenchmarkResult> {
    if install_root.exists() {
        fs::remove_dir_all(install_root)?;
    }
    fs::create_dir_all(home_dir)?;
    let input_size = fs::metadata(installer_path).map_or(0, |meta| meta.len());
    let (status, duration) = time(|| {
        Command::new(installer_path)
            .arg("--no-start")
            .env("HOME", home_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
    });
    let status = status.map_err(|e| {
        SurgeError::Pack(format!(
            "Failed to execute installer '{}': {e}",
            installer_path.display()
        ))
    })?;
    if !status.success() {
        return Err(SurgeError::Pack(format!(
            "Installer '{}' exited with status {status}",
            installer_path.display()
        )));
    }

    let installed_app_dir = install_root.join("app");
    assert_directories_match(&installed_app_dir, expected_payload_dir)?;

    Ok(BenchmarkResult {
        name: format!("Installer run ({installer_type})"),
        duration,
        input_size,
        output_size: dir_size_recursive(&installed_app_dir),
    })
}

pub async fn run_installer_scenario(
    work_dir: &Path,
    scale: f64,
    seed: u64,
    pack_zstd_level: i32,
    pack_max_threads: Option<usize>,
    pack_memory_mb: u64,
) -> Result<Vec<BenchmarkResult>> {
    let app_id = BENCH_APP_ID;
    let rid = current_rid();
    let version = version_label(1);
    let store_dir = work_dir.join("installer-store");
    let artifacts_dir = work_dir.join("installer-artifacts");
    let installers_dir = work_dir.join("installer-bundles");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&artifacts_dir)?;
    fs::create_dir_all(&installers_dir)?;

    let manifest_path = work_dir.join("installer-bench.surge.yml");
    write_bench_manifest(&manifest_path, &store_dir, app_id, &rid, pack_zstd_level)?;
    let ctx = configure_benchmark_context(&store_dir, pack_zstd_level, pack_max_threads, pack_memory_mb)?;

    let template = PayloadTemplate::new(scale, seed);
    template.write_base(&artifacts_dir, seed)?;
    let publication = publish_release(Arc::clone(&ctx), &manifest_path, app_id, &rid, &version, &artifacts_dir).await?;
    let full_package_path = store_dir.join(&publication.full_build.filename);

    let surge_binary = resolve_tool_binary("SURGE_INSTALLER_BINARY", surge_binary_name_for_rid(&rid))?;
    let installer_launcher = resolve_tool_binary("SURGE_INSTALLER_LAUNCHER", installer_launcher_name_for_rid(&rid))?;

    let online_install_root = work_dir.join("online-install-root");
    let offline_install_root = work_dir.join("offline-install-root");
    let online_home = work_dir.join("online-installer-home");
    let offline_home = work_dir.join("offline-installer-home");

    let (online_installer, online_create) = build_console_installer(
        &installers_dir,
        &store_dir,
        app_id,
        &rid,
        &version,
        &full_package_path,
        "online",
        &online_install_root,
        &surge_binary,
        &installer_launcher,
        pack_zstd_level,
    )?;
    let online_run = run_console_installer(
        &online_installer,
        &online_install_root,
        &artifacts_dir,
        "online",
        &online_home,
    )?;

    let (offline_installer, offline_create) = build_console_installer(
        &installers_dir,
        &store_dir,
        app_id,
        &rid,
        &version,
        &full_package_path,
        "offline",
        &offline_install_root,
        &surge_binary,
        &installer_launcher,
        pack_zstd_level,
    )?;
    let offline_run = run_console_installer(
        &offline_installer,
        &offline_install_root,
        &artifacts_dir,
        "offline",
        &offline_home,
    )?;

    Ok(vec![online_create, online_run, offline_create, offline_run])
}
