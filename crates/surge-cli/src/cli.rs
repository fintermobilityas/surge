use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use surge_core::config::constants::PACK_DEFAULT_DELTA_STRATEGY;

#[derive(Parser)]
#[command(name = "surge", version, about = "Surge update framework CLI")]
pub(crate) struct Cli {
    /// Path to surge.yml manifest
    #[arg(long, short = 'm', default_value = ".surge/surge.yml")]
    pub(crate) manifest_path: PathBuf,

    /// Enable verbose logging
    #[arg(long, short = 'v')]
    pub(crate) verbose: bool,

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
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

        /// Channel to compact (auto-selected only when exactly one channel exists)
        #[arg(long)]
        channel: Option<String>,
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

        /// Only cache the package locally without installing (used by --stage)
        #[arg(long)]
        stage: bool,
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
pub(crate) enum LockAction {
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
pub(crate) enum TuneAction {
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
pub(crate) enum InstallMethod {
    /// Resolve a release from configured backend and download it locally
    Backend,
    /// Install to a tailscale node using an explicit/selected RID and transfer package
    #[value(alias = "ssh")]
    Tailscale,
}

#[derive(Args, Clone)]
pub(crate) struct InstallOptions {
    /// Path to application manifest used for install defaults
    #[arg(long, default_value = ".surge/application.yml")]
    pub(crate) application_manifest: PathBuf,

    /// Application ID (auto-selected when manifest has exactly one app)
    #[arg(long)]
    pub(crate) app_id: Option<String>,

    /// Channel to resolve releases from (required only when multiple channels exist)
    #[arg(long)]
    pub(crate) channel: Option<String>,

    /// Explicit target RID (required when app has multiple targets and no interactive selection)
    #[arg(long)]
    pub(crate) rid: Option<String>,

    /// Specific version to install (defaults to latest matching version)
    #[arg(long)]
    pub(crate) version: Option<String>,

    /// Only show the selected package and command hints, do not download/transfer
    #[arg(long)]
    pub(crate) plan_only: bool,

    /// Do not start the application after installation
    #[arg(long)]
    pub(crate) no_start: bool,

    #[command(flatten)]
    pub(crate) stage_options: InstallStageOptions,

    /// Reinstall even if the selected version/channel is already installed on the target
    #[arg(long)]
    pub(crate) force: bool,

    /// Local cache directory for downloaded packages
    #[arg(long, default_value = ".surge/install-cache")]
    pub(crate) download_dir: PathBuf,

    /// Override storage provider from application manifest (s3, azure, gcs, filesystem, `github_releases`)
    #[arg(long)]
    pub(crate) provider: Option<String>,

    /// Override storage bucket/root from application manifest
    #[arg(long)]
    pub(crate) bucket: Option<String>,

    /// Override storage region from application manifest
    #[arg(long)]
    pub(crate) region: Option<String>,

    /// Override storage endpoint from application manifest
    #[arg(long)]
    pub(crate) endpoint: Option<String>,

    /// Override storage prefix from application manifest
    #[arg(long)]
    pub(crate) prefix: Option<String>,
}

#[derive(Args, Clone)]
pub(crate) struct InstallStageOptions {
    /// Pre-stage packages on remote nodes without activating (tailscale method only)
    #[arg(long)]
    pub(crate) stage: bool,

    /// Verify that the selected release is already staged and ready for the next tailscale install
    #[arg(long, conflicts_with = "stage")]
    pub(crate) verify_stage: bool,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Commands};

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

    #[test]
    fn install_force_flag_parses() {
        let cli = Cli::try_parse_from(["surge", "install", "tailscale", "my-node", "--force"])
            .expect("install command with --force should parse");

        let Commands::Install { options, .. } = cli.command else {
            panic!("expected install command");
        };

        assert!(options.force);
    }

    #[test]
    fn install_verify_stage_flag_parses() {
        let cli = Cli::try_parse_from(["surge", "install", "tailscale", "my-node", "--verify-stage"])
            .expect("install command with --verify-stage should parse");

        let Commands::Install { options, .. } = cli.command else {
            panic!("expected install command");
        };

        assert!(options.stage_options.verify_stage);
    }

    #[test]
    fn install_verify_stage_conflicts_with_stage() {
        let Err(err) = Cli::try_parse_from(["surge", "install", "tailscale", "my-node", "--stage", "--verify-stage"])
        else {
            panic!("--verify-stage should conflict with --stage");
        };

        assert!(err.to_string().contains("--stage"));
    }
}
