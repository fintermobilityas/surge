use std::path::{Path, PathBuf};
use std::process::ExitCode;

use crate::cli::{Cli, Commands};
use crate::{commands, envfile, logline, ui};

pub(crate) fn init_tracing(verbose: bool) {
    let filter = if verbose { "debug" } else { "info" };
    let theme = ui::UiTheme::global();
    tracing_subscriber::fmt()
        .with_timer(logline::CommandTimer::new())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .with_ansi(theme.enabled())
        .init();
}

pub(crate) fn load_env_files_for_cli(cli: &Cli) -> surge_core::error::Result<()> {
    match &cli.command {
        Commands::Init { .. } | Commands::Lock { .. } | Commands::Sha256 { .. } => Ok(()),
        Commands::Setup { dir, .. } => load_env_files_for_scope(dir, &envfile::candidate_paths_for_setup(dir)),
        Commands::Install { options, .. } => {
            let manifest_path =
                commands::install::selected_install_manifest_path(&options.application_manifest, &cli.manifest_path);
            load_env_files_for_scope(manifest_path, &envfile::candidate_paths_for_manifest(manifest_path))
        }
        Commands::Migrate { dest_manifest, .. } => {
            load_env_files_for_scope(
                &cli.manifest_path,
                &envfile::candidate_paths_for_manifest(&cli.manifest_path),
            )?;
            load_env_files_for_scope(dest_manifest, &envfile::candidate_paths_for_manifest(dest_manifest))
        }
        _ => load_env_files_for_scope(
            &cli.manifest_path,
            &envfile::candidate_paths_for_manifest(&cli.manifest_path),
        ),
    }
}

pub(crate) fn load_env_files_for_setup(dir: &Path) -> surge_core::error::Result<()> {
    let loaded = envfile::load_storage_env_files(dir, &envfile::candidate_paths_for_setup(dir))?;
    for path in loaded {
        logline::info(&format!("Loaded storage env overrides from {}", path.display()));
    }
    Ok(())
}

fn load_env_files_for_scope(scope: &Path, candidates: &[PathBuf]) -> surge_core::error::Result<()> {
    let loaded = envfile::load_storage_env_files(scope, candidates)?;
    for path in loaded {
        logline::info(&format!("Loaded storage env overrides from {}", path.display()));
    }
    Ok(())
}

pub(crate) fn detect_installer_context() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let manifest = dir.join("installer.yml");
    if manifest.is_file() {
        Some(dir.to_path_buf())
    } else {
        None
    }
}

pub(crate) fn handle_parse_error(err: &clap::Error) -> ExitCode {
    let is_success = matches!(
        err.kind(),
        clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
    );
    let rendered = err.to_string();
    let output = rendered.trim_end();
    if is_success {
        logline::emit_raw(output);
        ExitCode::SUCCESS
    } else {
        logline::emit_raw_stderr(output);
        ExitCode::FAILURE
    }
}
