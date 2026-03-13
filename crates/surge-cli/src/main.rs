#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use surge_core::config::constants::PACK_DEFAULT_DELTA_STRATEGY;

mod commands;
mod envfile;
mod formatters;
mod logline;
mod prompts;
mod ui;

#[derive(Parser)]
#[command(name = "surge", version, about = "Surge update framework CLI")]
struct Cli {
    /// Path to surge.yml manifest
    #[arg(long, short = 'm', default_value = ".surge/surge.yml")]
    manifest_path: PathBuf,

    /// Enable verbose logging
    #[arg(long, short = 'v')]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new surge.yml manifest
    Init {
        /// Application ID
        #[arg(long)]
        app_id: Option<String>,

        /// Application display name
        #[arg(long)]
        name: Option<String>,

        /// Storage provider (s3, azure, gcs, filesystem, `github_releases`)
        #[arg(long)]
        provider: Option<String>,

        /// Storage bucket or root path
        #[arg(long)]
        bucket: Option<String>,

        /// Runtime identifier (defaults to current RID for non-wizard init)
        #[arg(long)]
        rid: Option<String>,

        /// Main executable (defaults to app id for non-wizard init)
        #[arg(long)]
        main_exe: Option<String>,

        /// Install directory name (defaults to app id for non-wizard init)
        #[arg(long)]
        install_directory: Option<String>,

        /// Supervisor ID GUID (defaults to random UUID v4 for non-wizard init)
        #[arg(long)]
        supervisor_id: Option<String>,

        /// Force interactive setup wizard
        #[arg(long)]
        wizard: bool,

        /// Disable wizard and use command-line options only
        #[arg(long, conflicts_with = "wizard")]
        no_wizard: bool,
    },

    /// Build release packages (full + delta)
    Pack {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Release version
        #[arg(long)]
        version: String,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Path to build artifacts directory (defaults to .surge/artifacts/<app>/<rid>/<version>)
        #[arg(long)]
        artifacts_dir: Option<PathBuf>,

        /// Output directory for packages
        #[arg(long, short = 'o', default_value = ".surge/packages")]
        output_dir: PathBuf,
    },

    /// Push packages to storage
    Push {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Release version
        #[arg(long)]
        version: String,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Channel to publish to
        #[arg(long, default_value = "stable")]
        channel: String,

        /// Directory containing built packages
        #[arg(long, default_value = ".surge/packages")]
        packages_dir: PathBuf,
    },

    /// Promote a release to a channel
    Promote {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Release version to promote
        #[arg(long)]
        version: String,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Target channel
        #[arg(long)]
        channel: String,
    },

    /// Demote a release from a channel
    Demote {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Release version to demote
        #[arg(long)]
        version: String,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Channel to remove from
        #[arg(long)]
        channel: String,
    },

    /// List releases and channels
    List {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Filter by channel
        #[arg(long)]
        channel: Option<String>,
    },

    /// Compact a channel to a single latest full release and prune stale artifacts
    Compact {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Channel to compact
        #[arg(long, default_value = "stable")]
        channel: String,
    },

    /// Manage distributed locks
    Lock {
        #[command(subcommand)]
        action: LockAction,
    },

    /// Benchmark pack policy candidates and optionally write the recommendation to the manifest
    Tune {
        #[command(subcommand)]
        action: TuneAction,
    },

    /// Migrate release data between storage backends
    Migrate {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Path to destination manifest
        #[arg(long)]
        dest_manifest: PathBuf,
    },

    /// Restore releases from local packages or build installers from existing packages
    Restore {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Specific version to restore (defaults to latest when using --installers)
        #[arg(long)]
        version: Option<String>,

        /// Build installers only (snapx-compatible restore mode)
        #[arg(long, short = 'i')]
        installers: bool,

        /// Upload generated installers to storage under installers/<filename>
        #[arg(long, requires = "installers", conflicts_with = "package_file")]
        upload_installers: bool,

        /// Write a cache-manifest file for the selected installer package and exit
        #[arg(long, requires = "installers")]
        package_file: Option<PathBuf>,

        /// Path to build artifacts directory (defaults to .surge/artifacts/<app>/<rid>/<version> with --installers)
        #[arg(long)]
        artifacts_dir: Option<PathBuf>,

        /// Directory containing built packages (used with --installers)
        #[arg(long, default_value = ".surge/packages", requires = "installers")]
        packages_dir: PathBuf,
    },

    /// Install from an extracted installer directory (used by self-extracting installers)
    Setup {
        /// Path to extracted installer directory
        #[arg(default_value = ".")]
        dir: PathBuf,

        /// Do not start the application after installation
        #[arg(long)]
        no_start: bool,
    },

    /// Print SHA-256 hash of a file
    #[command(name = "sha256")]
    Sha256 {
        /// File to hash
        file: PathBuf,
    },

    /// Install packages using a selected transport method
    Install {
        /// Install method (defaults to backend)
        #[arg(value_enum, default_value_t = InstallMethod::Backend)]
        method: InstallMethod,

        /// Target node for tailscale method as positional value (for example: my-node or user@my-node)
        #[arg(index = 2, value_name = "NODE", conflicts_with = "node")]
        target: Option<String>,

        /// Target node for tailscale method (for example: my-node or user@my-node)
        #[arg(long)]
        node: Option<String>,

        /// Node user account used for tailscale SSH login (tailscale method)
        #[arg(long = "node-user", alias = "ssh-user")]
        node_user: Option<String>,

        #[command(flatten)]
        options: InstallOptions,
    },
}

#[derive(Subcommand)]
enum LockAction {
    /// Acquire a distributed lock
    Acquire {
        /// Lock name
        #[arg(long)]
        name: String,

        /// Lock timeout in seconds
        #[arg(long, default_value = "300")]
        timeout: u32,
    },

    /// Release a distributed lock
    Release {
        /// Lock name
        #[arg(long)]
        name: String,

        /// Challenge token from acquire
        #[arg(long)]
        challenge: String,
    },
}

#[derive(Subcommand)]
enum TuneAction {
    /// Benchmark pack policy candidates for a specific app target and version
    Pack {
        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Release version to benchmark
        #[arg(long)]
        version: String,

        /// Runtime identifier (auto-selected when app has exactly one target)
        #[arg(long)]
        rid: Option<String>,

        /// Path to build artifacts directory (defaults to .surge/artifacts/<app>/<rid>/<version>)
        #[arg(long)]
        artifacts_dir: Option<PathBuf>,

        /// Comma-separated zstd compression levels to benchmark
        #[arg(long, default_value = "1,3,5,9", value_delimiter = ',')]
        zstd_levels: Vec<i32>,

        /// Comma-separated delta strategies to benchmark
        #[arg(long, default_value = PACK_DEFAULT_DELTA_STRATEGY, value_delimiter = ',')]
        delta_strategies: Vec<String>,

        /// Write the recommended pack policy back to the manifest
        #[arg(long)]
        write_manifest: bool,
    },
}

#[derive(ValueEnum, Clone, Debug)]
enum InstallMethod {
    /// Resolve a release from configured backend and download it locally
    Backend,
    /// Install to a tailscale node using an explicit/selected RID and transfer package
    #[value(alias = "ssh")]
    Tailscale,
}

#[derive(Args, Clone)]
struct InstallOptions {
    /// Path to application manifest used for install defaults
    #[arg(long, default_value = ".surge/application.yml")]
    application_manifest: PathBuf,

    /// Application ID (auto-selected when manifest has exactly one app)
    #[arg(long)]
    app_id: Option<String>,

    /// Channel to resolve releases from (required only when multiple channels exist)
    #[arg(long)]
    channel: Option<String>,

    /// Explicit target RID (required when app has multiple targets and no interactive selection)
    #[arg(long)]
    rid: Option<String>,

    /// Specific version to install (defaults to latest matching version)
    #[arg(long)]
    version: Option<String>,

    /// Only show the selected package and command hints, do not download/transfer
    #[arg(long)]
    plan_only: bool,

    /// Do not start the application after installation
    #[arg(long)]
    no_start: bool,

    /// Local cache directory for downloaded packages
    #[arg(long, default_value = ".surge/install-cache")]
    download_dir: PathBuf,

    /// Override storage provider from application manifest (s3, azure, gcs, filesystem, `github_releases`)
    #[arg(long)]
    provider: Option<String>,

    /// Override storage bucket/root from application manifest
    #[arg(long)]
    bucket: Option<String>,

    /// Override storage region from application manifest
    #[arg(long)]
    region: Option<String>,

    /// Override storage endpoint from application manifest
    #[arg(long)]
    endpoint: Option<String>,

    /// Override storage prefix from application manifest
    #[arg(long)]
    prefix: Option<String>,
}

fn init_tracing(verbose: bool) {
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

fn main() -> ExitCode {
    let started = Instant::now();
    logline::init_timer(started);

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            if err.kind() == clap::error::ErrorKind::MissingSubcommand
                && let Some(installer_dir) = detect_installer_context()
            {
                init_tracing(false);
                if let Err(e) = load_env_files_for_setup(&installer_dir) {
                    logline::error_chain(&e);
                    return ExitCode::FAILURE;
                }
                let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build();
                let rt = match rt {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        logline::error(&format!("failed to create tokio runtime: {e}"));
                        return ExitCode::FAILURE;
                    }
                };
                return match rt.block_on(commands::setup::execute(&installer_dir, false)) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => {
                        logline::error_chain(&e);
                        ExitCode::FAILURE
                    }
                };
            }
            return handle_parse_error(&err);
        }
    };
    logline::init_verbose(cli.verbose);
    init_tracing(cli.verbose);
    if let Err(e) = load_env_files_for_cli(&cli) {
        logline::error_chain(&e);
        return ExitCode::FAILURE;
    }

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build();
    let rt = match rt {
        Ok(runtime) => runtime,
        Err(e) => {
            logline::error(&format!("failed to create tokio runtime: {e}"));
            return ExitCode::FAILURE;
        }
    };

    let result = rt.block_on(run(cli));

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            logline::error_chain(&e);
            ExitCode::FAILURE
        }
    }
}

fn load_env_files_for_cli(cli: &Cli) -> surge_core::error::Result<()> {
    match &cli.command {
        Commands::Init { .. } | Commands::Lock { .. } | Commands::Sha256 { .. } => Ok(()),
        Commands::Setup { dir, .. } => load_env_files_for_scope(dir, envfile::candidate_paths_for_setup(dir)),
        Commands::Install { options, .. } => {
            let manifest_path =
                commands::install::selected_install_manifest_path(&options.application_manifest, &cli.manifest_path);
            load_env_files_for_scope(manifest_path, envfile::candidate_paths_for_manifest(manifest_path))
        }
        Commands::Migrate { dest_manifest, .. } => {
            load_env_files_for_scope(
                &cli.manifest_path,
                envfile::candidate_paths_for_manifest(&cli.manifest_path),
            )?;
            load_env_files_for_scope(dest_manifest, envfile::candidate_paths_for_manifest(dest_manifest))
        }
        _ => load_env_files_for_scope(
            &cli.manifest_path,
            envfile::candidate_paths_for_manifest(&cli.manifest_path),
        ),
    }
}

fn load_env_files_for_setup(dir: &std::path::Path) -> surge_core::error::Result<()> {
    let loaded = envfile::load_storage_env_files(dir, &envfile::candidate_paths_for_setup(dir))?;
    for path in loaded {
        logline::info(&format!("Loaded storage env overrides from {}", path.display()));
    }
    Ok(())
}

fn load_env_files_for_scope(scope: &Path, candidates: Vec<PathBuf>) -> surge_core::error::Result<()> {
    let loaded = envfile::load_storage_env_files(scope, &candidates)?;
    for path in loaded {
        logline::info(&format!("Loaded storage env overrides from {}", path.display()));
    }
    Ok(())
}

/// Check if `installer.yml` exists next to the current executable.
/// This is the auto-detection path for warp-extracted bundles.
fn detect_installer_context() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let manifest = dir.join("installer.yml");
    if manifest.is_file() {
        Some(dir.to_path_buf())
    } else {
        None
    }
}

fn handle_parse_error(err: &clap::Error) -> ExitCode {
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

async fn run(cli: Cli) -> surge_core::error::Result<()> {
    let manifest_path = cli.manifest_path;

    match cli.command {
        Commands::Init {
            app_id,
            name,
            provider,
            bucket,
            rid,
            main_exe,
            install_directory,
            supervisor_id,
            wizard,
            no_wizard,
        } => {
            let option_driven = app_id.is_some()
                || name.is_some()
                || provider.is_some()
                || bucket.is_some()
                || rid.is_some()
                || main_exe.is_some()
                || install_directory.is_some()
                || supervisor_id.is_some();
            let wizard_mode = if wizard {
                true
            } else if no_wizard {
                false
            } else {
                !option_driven
            };

            commands::init::execute(
                &manifest_path,
                app_id.as_deref(),
                name.as_deref(),
                provider.as_deref(),
                bucket.as_deref(),
                rid.as_deref(),
                main_exe.as_deref(),
                install_directory.as_deref(),
                supervisor_id.as_deref(),
                wizard_mode,
            )
            .await
        }

        Commands::Pack {
            app_id,
            version,
            rid,
            artifacts_dir,
            output_dir,
        } => {
            commands::pack::execute(
                &manifest_path,
                app_id.as_deref(),
                &version,
                rid.as_deref(),
                artifacts_dir.as_deref(),
                &output_dir,
            )
            .await
        }

        Commands::Push {
            app_id,
            version,
            rid,
            channel,
            packages_dir,
        } => {
            commands::push::execute(
                &manifest_path,
                app_id.as_deref(),
                &version,
                rid.as_deref(),
                &channel,
                &packages_dir,
            )
            .await
        }

        Commands::Promote {
            app_id,
            version,
            rid,
            channel,
        } => commands::promote::execute(&manifest_path, app_id.as_deref(), &version, rid.as_deref(), &channel).await,

        Commands::Demote {
            app_id,
            version,
            rid,
            channel,
        } => commands::demote::execute(&manifest_path, app_id.as_deref(), &version, rid.as_deref(), &channel).await,

        Commands::Compact { app_id, rid, channel } => {
            commands::compact::execute(&manifest_path, app_id.as_deref(), rid.as_deref(), &channel).await
        }

        Commands::List { app_id, rid, channel } => {
            commands::list::execute(&manifest_path, app_id.as_deref(), rid.as_deref(), channel.as_deref()).await
        }

        Commands::Lock { action } => match action {
            LockAction::Acquire { name, timeout } => commands::lock::acquire(&manifest_path, &name, timeout).await,
            LockAction::Release { name, challenge } => commands::lock::release(&manifest_path, &name, &challenge).await,
        },

        Commands::Tune { action } => match action {
            TuneAction::Pack {
                app_id,
                version,
                rid,
                artifacts_dir,
                zstd_levels,
                delta_strategies,
                write_manifest,
            } => {
                commands::tune::execute_pack(
                    &manifest_path,
                    app_id.as_deref(),
                    &version,
                    rid.as_deref(),
                    artifacts_dir.as_deref(),
                    &zstd_levels,
                    &delta_strategies,
                    write_manifest,
                )
                .await
            }
        },

        Commands::Migrate {
            app_id,
            rid,
            dest_manifest,
        } => commands::migrate::execute(&manifest_path, app_id.as_deref(), rid.as_deref(), &dest_manifest).await,

        Commands::Restore {
            app_id,
            rid,
            version,
            installers,
            upload_installers,
            package_file,
            artifacts_dir,
            packages_dir,
        } => {
            if installers {
                commands::pack::execute_installers_only(
                    &manifest_path,
                    app_id.as_deref(),
                    version.as_deref(),
                    rid.as_deref(),
                    artifacts_dir.as_deref(),
                    &packages_dir,
                    package_file.as_deref(),
                    upload_installers,
                )
                .await
            } else {
                let packages_dir = PathBuf::from(".surge/packages");
                commands::restore::execute(
                    &manifest_path,
                    app_id.as_deref(),
                    rid.as_deref(),
                    version.as_deref(),
                    packages_dir.as_path(),
                )
                .await
            }
        }

        Commands::Setup { dir, no_start } => commands::setup::execute(&dir, no_start).await,

        Commands::Sha256 { file } => {
            let hash = surge_core::crypto::sha256::sha256_hex_file(&file)?;
            logline::emit_raw(&hash);
            Ok(())
        }

        Commands::Install {
            method,
            target,
            node,
            node_user,
            options,
        } => {
            let node = node.or(target);
            let node = match method {
                InstallMethod::Backend => {
                    if node.is_some() || node_user.is_some() {
                        return Err(surge_core::error::SurgeError::Config(
                            "--node/--node-user require 'tailscale' install method".to_string(),
                        ));
                    }
                    None
                }
                InstallMethod::Tailscale => Some(node.as_deref().ok_or_else(|| {
                    surge_core::error::SurgeError::Config(
                        "--node is required for 'tailscale' install method".to_string(),
                    )
                })?),
            };

            commands::install::execute(
                &manifest_path,
                &options.application_manifest,
                node,
                node_user.as_deref(),
                options.app_id.as_deref(),
                options.channel.as_deref(),
                options.rid.as_deref(),
                options.version.as_deref(),
                options.plan_only,
                options.no_start,
                &options.download_dir,
                commands::install::StorageOverrides {
                    provider: options.provider.as_deref(),
                    bucket: options.bucket.as_deref(),
                    region: options.region.as_deref(),
                    endpoint: options.endpoint.as_deref(),
                    prefix: options.prefix.as_deref(),
                },
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_package_file_requires_installers_flag() {
        let Err(err) = Cli::try_parse_from(["surge", "restore", "--package-file", "packages.txt"]) else {
            panic!("package-file should require installers mode");
        };

        assert!(err.to_string().contains("--installers"));
    }

    #[test]
    fn restore_upload_installers_requires_installers_flag() {
        let Err(err) = Cli::try_parse_from(["surge", "restore", "--upload-installers"]) else {
            panic!("upload-installers should require installers mode");
        };

        assert!(err.to_string().contains("--installers"));
    }

    #[test]
    fn restore_upload_installers_conflicts_with_package_file() {
        let Err(err) = Cli::try_parse_from([
            "surge",
            "restore",
            "--installers",
            "--upload-installers",
            "--package-file",
            "packages.txt",
        ]) else {
            panic!("upload-installers should conflict with package-file");
        };

        assert!(err.to_string().contains("--package-file"));
    }
}
