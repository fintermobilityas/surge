#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

mod bootstrap;
mod cli;
mod commands;
mod envfile;
mod formatters;
mod logline;
mod prompts;
mod ui;

use cli::{Cli, Commands, InstallMethod, LockAction, TuneAction};

fn main() -> ExitCode {
    let started = Instant::now();
    logline::init_timer(started);

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err) => {
            if err.kind() == clap::error::ErrorKind::MissingSubcommand
                && let Some(installer_dir) = bootstrap::detect_installer_context()
            {
                bootstrap::init_tracing(false);
                if let Err(e) = bootstrap::load_env_files_for_setup(&installer_dir) {
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
                return match rt.block_on(commands::setup::execute(&installer_dir, false, false)) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => {
                        logline::error_chain(&e);
                        ExitCode::FAILURE
                    }
                };
            }
            return bootstrap::handle_parse_error(&err);
        }
    };
    logline::init_verbose(cli.verbose);
    bootstrap::init_tracing(cli.verbose);
    if let Err(e) = bootstrap::load_env_files_for_cli(&cli) {
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
            commands::compact::execute(&manifest_path, app_id.as_deref(), rid.as_deref(), channel.as_deref()).await
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

        Commands::Setup { dir, no_start, stage } => commands::setup::execute(&dir, no_start, stage).await,

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
                    if options.stage_options.stage {
                        return Err(surge_core::error::SurgeError::Config(
                            "--stage requires 'tailscale' install method".to_string(),
                        ));
                    }
                    if options.stage_options.verify_stage {
                        return Err(surge_core::error::SurgeError::Config(
                            "--verify-stage requires 'tailscale' install method".to_string(),
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
                commands::install::InstallBehavior {
                    plan_only: options.plan_only,
                    no_start: options.no_start,
                    force: options.force,
                    mode: if options.stage_options.verify_stage {
                        commands::install::InstallMode::VerifyStage
                    } else if options.stage_options.stage {
                        commands::install::InstallMode::StageOnly
                    } else {
                        commands::install::InstallMode::Install
                    },
                },
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
