//! Convergence-plan reporting and slow-link stage guidance for remote
//! installs, split out of the orchestration module.

use super::{ReleaseEntry, RemoteConvergenceAction, RemoteConvergencePlan, logline};
use surge_core::update::manager::ApplyStrategy;

pub(crate) fn log_remote_convergence_plan(
    file_target: &str,
    app_id: &str,
    channel: &str,
    release: &ReleaseEntry,
    plan: &RemoteConvergencePlan,
) {
    let installed = plan.installed_version.as_deref().unwrap_or("<none>");
    logline::info(&format!(
        "Remote install plan for '{app_id}' on '{file_target}': {} ({} -> {}, channel '{channel}').",
        remote_action_label(plan.action),
        installed,
        plan.target_version
    ));

    match plan.action {
        RemoteConvergenceAction::Update => {
            if let Some(update) = &plan.update_info {
                let artifacts = selected_update_artifact_labels(update);
                logline::info(&format!(
                    "Selected update artifacts: {} ({} total), apply strategy: {}.",
                    artifacts.join(", "),
                    crate::formatters::format_bytes(u64::try_from(update.download_size.max(0)).unwrap_or(0)),
                    update_strategy_label(update.apply_strategy)
                ));
                if let Some(reason) = &update.fallback_reason {
                    logline::warn(&format!("Delta update unavailable; full package selected: {reason}"));
                }
            } else if let Some(reason) = &plan.reason {
                logline::warn(&format!(
                    "Update plan unavailable; full install transfer will be used: {reason}"
                ));
            }
        }
        RemoteConvergenceAction::CleanInstall | RemoteConvergenceAction::Reinstall => {
            logline::info(&format!(
                "Selected install artifact: {} ({}), transfer/apply strategy: full installer.",
                release.full_filename,
                crate::formatters::format_bytes(u64::try_from(release.full_size.max(0)).unwrap_or(0))
            ));
            if let Some(reason) = &plan.reason {
                logline::info(&format!("Plan reason: {reason}"));
            }
        }
        RemoteConvergenceAction::RepairMetadata => {
            logline::info("Selected action only repairs runtime metadata; no package artifact should be downloaded.");
        }
        RemoteConvergenceAction::ConvergeRuntime => {
            if let Some(reason) = &plan.reason {
                logline::info(&format!("Plan reason: {reason}"));
            }
            logline::info(
                "Selected action verifies runtime state and restarts the supervisor only if runtime proof is missing.",
            );
        }
        RemoteConvergenceAction::Skip => {}
    }
}

pub(crate) fn selected_update_artifact_labels(update: &surge_core::update::manager::UpdateInfo) -> Vec<String> {
    if matches!(update.apply_strategy, ApplyStrategy::Delta) {
        update
            .apply_releases
            .iter()
            .filter_map(ReleaseEntry::selected_delta)
            .map(|delta| {
                format!(
                    "{} ({})",
                    delta.filename,
                    crate::formatters::format_bytes(u64::try_from(delta.size.max(0)).unwrap_or(0))
                )
            })
            .collect()
    } else {
        update
            .apply_releases
            .last()
            .map(|release| {
                vec![format!(
                    "{} ({})",
                    release.full_filename,
                    crate::formatters::format_bytes(u64::try_from(release.full_size.max(0)).unwrap_or(0))
                )]
            })
            .unwrap_or_default()
    }
}

pub(crate) fn remote_action_label(action: RemoteConvergenceAction) -> &'static str {
    match action {
        RemoteConvergenceAction::CleanInstall => "clean install",
        RemoteConvergenceAction::Update => "update existing install",
        RemoteConvergenceAction::RepairMetadata => "repair runtime metadata",
        RemoteConvergenceAction::ConvergeRuntime => "converge runtime",
        RemoteConvergenceAction::Reinstall => "reinstall",
        RemoteConvergenceAction::Skip => "skip",
    }
}

pub(crate) fn update_strategy_label(strategy: ApplyStrategy) -> &'static str {
    match strategy {
        ApplyStrategy::Full => "full package",
        ApplyStrategy::Delta => "delta",
    }
}

/// Full-package transfers on a slow tailnet link take a long time, and a
/// direct (non-staged) install keeps the app down for the whole transfer.
/// Steer slow-link operators toward the staged flow, which downloads while
/// the app stays up and cuts over quickly.
const SLOW_LINK_STAGE_GUIDANCE_MIN_BYTES: i64 = 50 * 1024 * 1024;

pub(crate) fn warn_remote_full_download_downtime(
    file_target: &str,
    app_id: &str,
    action: RemoteConvergenceAction,
    release: &ReleaseEntry,
    stage_mode: bool,
) {
    if stage_mode {
        return;
    }
    let stops_running_app = matches!(
        action,
        RemoteConvergenceAction::Reinstall | RemoteConvergenceAction::Update
    );
    if !stops_running_app || release.full_size < SLOW_LINK_STAGE_GUIDANCE_MIN_BYTES {
        return;
    }
    logline::warn(&format!(
        "This install will stop '{app_id}' on '{file_target}' and transfer {} over the tailscale link before the app comes back.",
        crate::formatters::format_bytes(u64::try_from(release.full_size).unwrap_or(0))
    ));
    logline::warn(
        "On a slow link that can take a long time. To keep the app up during the transfer, run the same command with --stage first, then re-run it to cut over.",
    );
}
