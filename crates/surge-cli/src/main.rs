#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

mod commands;
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

        /// Storage provider (s3, azure, gcs, filesystem, github_releases)
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

    /// Manage distributed locks
    Lock {
        #[command(subcommand)]
        action: LockAction,
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

    /// Restore releases from backup or build installers from existing packages
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

        /// Path to build artifacts directory (defaults to .surge/artifacts/<app>/<rid>/<version> with --installers)
        #[arg(long)]
        artifacts_dir: Option<PathBuf>,

        /// Path to backup directory
        #[arg(long, required_unless_present = "installers", conflicts_with = "installers")]
        backup_dir: Option<PathBuf>,

        /// Directory containing built packages (used with --installers)
        #[arg(long, default_value = ".surge/packages", requires = "installers")]
        packages_dir: PathBuf,
    },

    /// Tailscale-assisted install planning and package transfer
    Tailscale {
        #[command(subcommand)]
        action: TailscaleAction,
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
enum TailscaleAction {
    /// Detect remote RID via Tailscale, resolve release by channel, and transfer package
    Install {
        /// Target node (for example: my-node or user@my-node)
        #[arg(long)]
        node: String,

        /// SSH user account used for remote profile detection (optional)
        #[arg(long)]
        ssh_user: Option<String>,

        /// Application ID (auto-selected when manifest has exactly one app)
        #[arg(long)]
        app_id: Option<String>,

        /// Channel to resolve releases from
        #[arg(long, default_value = "stable")]
        channel: String,

        /// Override remote RID detection with an explicit RID
        #[arg(long)]
        rid: Option<String>,

        /// Specific version to install (defaults to latest matching version)
        #[arg(long)]
        version: Option<String>,

        /// Only show the selected package and command hints, do not download/copy
        #[arg(long)]
        plan_only: bool,

        /// Local cache directory for downloaded packages before transfer
        #[arg(long, default_value = ".surge/tailscale-cache")]
        download_dir: PathBuf,
    },
}

fn init_tracing(verbose: bool) {
    let filter = if verbose { "debug" } else { "info" };
    let theme = ui::UiTheme::global();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .with_ansi(theme.enabled())
        .init();
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build();
    let rt = match rt {
        Ok(runtime) => runtime,
        Err(e) => {
            tracing::error!("failed to create tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = rt.block_on(run(cli));

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e}");
            ExitCode::FAILURE
        }
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

        Commands::List { app_id, rid, channel } => {
            commands::list::execute(&manifest_path, app_id.as_deref(), rid.as_deref(), channel.as_deref()).await
        }

        Commands::Lock { action } => match action {
            LockAction::Acquire { name, timeout } => commands::lock::acquire(&manifest_path, &name, timeout).await,
            LockAction::Release { name, challenge } => commands::lock::release(&manifest_path, &name, &challenge).await,
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
            backup_dir,
            installers,
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
                )
                .await
            } else {
                let backup_dir = backup_dir.as_deref().ok_or_else(|| {
                    surge_core::error::SurgeError::Config(
                        "--backup-dir is required unless --installers is used".to_string(),
                    )
                })?;
                commands::restore::execute(
                    &manifest_path,
                    app_id.as_deref(),
                    rid.as_deref(),
                    version.as_deref(),
                    backup_dir,
                )
                .await
            }
        }

        Commands::Tailscale { action } => match action {
            TailscaleAction::Install {
                node,
                ssh_user,
                app_id,
                channel,
                rid,
                version,
                plan_only,
                download_dir,
            } => {
                commands::tailscale::install_execute(
                    &manifest_path,
                    &node,
                    ssh_user.as_deref(),
                    app_id.as_deref(),
                    &channel,
                    rid.as_deref(),
                    version.as_deref(),
                    plan_only,
                    &download_dir,
                )
                .await
            }
        },
    }
}
