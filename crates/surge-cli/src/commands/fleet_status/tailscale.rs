use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use surge_core::error::{Result, SurgeError};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[cfg(test)]
use super::probe::{AppProcessSummary, SupervisorHandoffSummary, SupervisorProcessSummary};
use super::probe::{
    ProcessSummary, RemoteUpdateStatus, build_remote_probe_script, parse_remote_probe, run_tailscale_capture_timeout,
};
use crate::cli::FleetStatusTailscaleOptions;
use crate::commands::install::{resolve_tailscale_targets, shell_single_quote};
use crate::logline;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FleetStatusOutcome {
    pub(crate) all_healthy: bool,
}

#[derive(Debug, Clone)]
struct ProbeConfig {
    app_id: String,
    rid: String,
    channel: String,
    version: String,
    timeout: Duration,
}

#[derive(Debug, Clone)]
struct TargetNode {
    index: usize,
    ssh_target: String,
    display_target: String,
}

#[derive(Debug, Clone, Serialize)]
struct FleetStatusReport {
    app_id: String,
    rid: String,
    channel: String,
    version: String,
    total: usize,
    healthy: usize,
    unhealthy: usize,
    nodes: Vec<NodeStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct NodeStatus {
    #[serde(skip)]
    index: usize,
    node: String,
    status: NodeHealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    installed: Option<InstalledSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_status: Option<UpdateStatusSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process: Option<ProcessSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NodeHealthStatus {
    Healthy,
    Unreachable,
    Missing,
    WrongApp,
    WrongChannel,
    Stale,
    Degraded,
}

impl NodeHealthStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Unreachable => "unreachable",
            Self::Missing => "missing",
            Self::WrongApp => "wrong_app",
            Self::WrongChannel => "wrong_channel",
            Self::Stale => "stale",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct InstalledSummary {
    app_id: String,
    version: String,
    channel: String,
    install_directory: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    supervisor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct UpdateStatusSummary {
    state: String,
    installed_version: String,
    target_version: String,
    channel: String,
    app_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_progress_at_utc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RuntimeManifest {
    id: String,
    version: String,
    channel: String,
    #[serde(rename = "installDirectory", default)]
    install_directory: String,
    #[serde(rename = "supervisorId", default)]
    supervisor_id: String,
}

pub(crate) async fn execute_tailscale(options: FleetStatusTailscaleOptions) -> Result<FleetStatusOutcome> {
    let config = ProbeConfig {
        app_id: required_value(&options.app_id, "--app-id")?,
        rid: required_value(&options.rid, "--rid")?,
        channel: required_value(&options.channel, "--channel")?,
        version: required_value(&options.version, "--version")?,
        timeout: Duration::from_secs(validate_positive_u64(options.timeout_seconds, "--timeout-seconds")?),
    };
    let concurrency = validate_positive_usize(options.concurrency, "--concurrency")?;
    let nodes = collect_nodes(&options.node, options.nodes_file.as_deref())?;
    if nodes.is_empty() {
        return Err(SurgeError::Config(
            "Provide at least one --node or a non-empty --nodes-file.".to_string(),
        ));
    }
    ensure_tailscale_command_available()?;

    let targets = resolve_targets(&nodes, options.node_user.as_deref())?;
    let report = probe_nodes(config, targets, concurrency).await?;
    emit_report(&report, options.json)?;

    Ok(FleetStatusOutcome {
        all_healthy: report.unhealthy == 0,
    })
}

fn required_value(value: &str, option: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(SurgeError::Config(format!("{option} cannot be empty.")));
    }
    Ok(value.to_string())
}

fn validate_positive_usize(value: usize, option: &str) -> Result<usize> {
    if value == 0 {
        return Err(SurgeError::Config(format!("{option} must be greater than zero.")));
    }
    Ok(value)
}

fn validate_positive_u64(value: u64, option: &str) -> Result<u64> {
    if value == 0 {
        return Err(SurgeError::Config(format!("{option} must be greater than zero.")));
    }
    Ok(value)
}

fn collect_nodes(inline_nodes: &[String], nodes_file: Option<&Path>) -> Result<Vec<String>> {
    let mut nodes = Vec::new();
    for node in inline_nodes {
        push_node_value(&mut nodes, node);
    }

    if let Some(path) = nodes_file {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| SurgeError::Config(format!("Failed to read nodes file '{}': {e}", path.display())))?;
        for line in raw.lines() {
            push_node_value(&mut nodes, line);
        }
    }

    Ok(nodes)
}

fn push_node_value(nodes: &mut Vec<String>, value: &str) {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return;
    }
    nodes.push(trimmed.to_string());
}

fn ensure_tailscale_command_available() -> Result<()> {
    which::which("tailscale")
        .map(|_| ())
        .map_err(|e| SurgeError::Config(format!("tailscale command is not available on PATH: {e}")))
}

fn resolve_targets(nodes: &[String], node_user: Option<&str>) -> Result<Vec<TargetNode>> {
    nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let (ssh_target, display_target) = resolve_tailscale_targets(node, node_user)?;
            Ok(TargetNode {
                index,
                ssh_target,
                display_target,
            })
        })
        .collect()
}

async fn probe_nodes(config: ProbeConfig, targets: Vec<TargetNode>, concurrency: usize) -> Result<FleetStatusReport> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let config = Arc::new(config);
    let mut tasks = JoinSet::new();

    for target in targets {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| SurgeError::Platform(format!("Failed to acquire probe concurrency permit: {e}")))?;
        let config = Arc::clone(&config);
        tasks.spawn(async move {
            let _permit = permit;
            probe_node(config, target).await
        });
    }

    let mut nodes = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let status = joined.map_err(|e| SurgeError::Platform(format!("Fleet status probe task failed: {e}")))?;
        nodes.push(status);
    }
    nodes.sort_by_key(|node| node.index);

    let healthy = nodes
        .iter()
        .filter(|node| node.status == NodeHealthStatus::Healthy)
        .count();
    let total = nodes.len();
    Ok(FleetStatusReport {
        app_id: config.app_id.clone(),
        rid: config.rid.clone(),
        channel: config.channel.clone(),
        version: config.version.clone(),
        total,
        healthy,
        unhealthy: total.saturating_sub(healthy),
        nodes,
    })
}

async fn probe_node(config: Arc<ProbeConfig>, target: TargetNode) -> NodeStatus {
    let script = build_remote_probe_script(&config.app_id, &config.version);
    let remote_command = format!("sh -lc {}", shell_single_quote(&script));
    match run_tailscale_capture_timeout(&target.ssh_target, &remote_command, config.timeout).await {
        Ok(output) => classify_probe_output(&config, &target, &output),
        Err(error) => NodeStatus {
            index: target.index,
            node: target.display_target,
            status: NodeHealthStatus::Unreachable,
            reason: Some("tailscale ssh probe failed".to_string()),
            installed: None,
            update_status: None,
            process: None,
            error: Some(error),
        },
    }
}

fn classify_probe_output(config: &ProbeConfig, target: &TargetNode, output: &str) -> NodeStatus {
    let parsed = match parse_remote_probe(output) {
        Ok(parsed) => parsed,
        Err(error) => {
            return NodeStatus {
                index: target.index,
                node: target.display_target.clone(),
                status: NodeHealthStatus::Degraded,
                reason: Some("remote probe returned an unreadable result".to_string()),
                installed: None,
                update_status: None,
                process: None,
                error: Some(error),
            };
        }
    };

    if parsed.missing {
        return NodeStatus {
            index: target.index,
            node: target.display_target.clone(),
            status: NodeHealthStatus::Missing,
            reason: Some(format!("no runtime manifest found for '{}'", config.app_id)),
            installed: None,
            update_status: None,
            process: Some(parsed.process),
            error: None,
        };
    }

    let Some(runtime_yaml) = parsed.runtime_yaml.as_deref() else {
        return NodeStatus {
            index: target.index,
            node: target.display_target.clone(),
            status: NodeHealthStatus::Missing,
            reason: Some("runtime manifest was not returned by the probe".to_string()),
            installed: None,
            update_status: None,
            process: Some(parsed.process),
            error: None,
        };
    };

    let manifest = match serde_yaml::from_str::<RuntimeManifest>(runtime_yaml) {
        Ok(manifest) => manifest,
        Err(error) => {
            return NodeStatus {
                index: target.index,
                node: target.display_target.clone(),
                status: NodeHealthStatus::Degraded,
                reason: Some("runtime manifest could not be parsed".to_string()),
                installed: None,
                update_status: None,
                process: Some(parsed.process),
                error: Some(error.to_string()),
            };
        }
    };
    let update_status = parsed.update_status;
    let (status, reason) = classify_node(config, &manifest, update_status.as_ref(), &parsed.process);
    NodeStatus {
        index: target.index,
        node: target.display_target.clone(),
        status,
        reason,
        installed: Some(installed_summary(&manifest)),
        update_status: update_status.map(update_status_summary),
        process: Some(parsed.process),
        error: None,
    }
}

fn classify_node(
    config: &ProbeConfig,
    manifest: &RuntimeManifest,
    update_status: Option<&RemoteUpdateStatus>,
    process: &ProcessSummary,
) -> (NodeHealthStatus, Option<String>) {
    if manifest.id.trim() != config.app_id {
        return (
            NodeHealthStatus::WrongApp,
            Some(format!(
                "expected app id '{}' but found '{}'",
                config.app_id,
                manifest.id.trim()
            )),
        );
    }
    if manifest.channel.trim() != config.channel {
        return (
            NodeHealthStatus::WrongChannel,
            Some(format!(
                "expected channel '{}' but found '{}'",
                config.channel,
                manifest.channel.trim()
            )),
        );
    }
    if manifest.version.trim() != config.version {
        return (
            NodeHealthStatus::Stale,
            Some(format!(
                "expected version '{}' but found '{}'",
                config.version,
                manifest.version.trim()
            )),
        );
    }
    if process.app_process_running() && !process.target_app_process_running() {
        return (
            NodeHealthStatus::Stale,
            Some("app process is running without target-version first-run proof".to_string()),
        );
    }
    if process.stale_supervisor_process_running() {
        return (
            NodeHealthStatus::Stale,
            Some("supervisor process is running with stale first-run proof".to_string()),
        );
    }

    let mut degraded_reasons = Vec::new();
    if !process.proc_available {
        degraded_reasons.push("process table was not readable".to_string());
    }
    if !process.app_process_running() {
        degraded_reasons.push("app process was not found".to_string());
    }
    if process.supervisor_configured() && !process.supervisor_process_running() {
        degraded_reasons.push("supervisor process was not found".to_string());
    }
    if process.supervisor_waiting_for_previous_child() {
        degraded_reasons.push("supervisor is still waiting for a previous child process".to_string());
    }
    if let Some(status) = update_status {
        collect_update_status_degradation(config, status, &mut degraded_reasons);
    }

    if degraded_reasons.is_empty() {
        (NodeHealthStatus::Healthy, None)
    } else {
        (NodeHealthStatus::Degraded, Some(degraded_reasons.join("; ")))
    }
}

fn collect_update_status_degradation(
    config: &ProbeConfig,
    status: &RemoteUpdateStatus,
    degraded_reasons: &mut Vec<String>,
) {
    let state = status.state.trim();
    if matches!(state, "failed" | "in_progress" | "pending_restart") {
        degraded_reasons.push(format!("update status is {state}"));
    }
    if !status.app_id.trim().is_empty() && status.app_id.trim() != config.app_id {
        degraded_reasons.push(format!(
            "update status app id is '{}' instead of '{}'",
            status.app_id.trim(),
            config.app_id
        ));
    }
    if !status.channel.trim().is_empty() && status.channel.trim() != config.channel {
        degraded_reasons.push(format!(
            "update status channel is '{}' instead of '{}'",
            status.channel.trim(),
            config.channel
        ));
    }
    if !status.target_version.trim().is_empty() && status.target_version.trim() != config.version {
        degraded_reasons.push(format!(
            "update status target version is '{}' instead of '{}'",
            status.target_version.trim(),
            config.version
        ));
    }
}

fn installed_summary(manifest: &RuntimeManifest) -> InstalledSummary {
    InstalledSummary {
        app_id: manifest.id.trim().to_string(),
        version: manifest.version.trim().to_string(),
        channel: manifest.channel.trim().to_string(),
        install_directory: manifest.install_directory.trim().to_string(),
        supervisor_id: non_empty_string(&manifest.supervisor_id),
    }
}

fn update_status_summary(status: RemoteUpdateStatus) -> UpdateStatusSummary {
    UpdateStatusSummary {
        state: status.state,
        installed_version: status.installed_version,
        target_version: status.target_version,
        channel: status.channel,
        app_id: status.app_id,
        current_phase: status.current_phase,
        last_progress_at_utc: status.last_progress_at_utc,
        reason: status.reason,
    }
}

fn non_empty_string(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn emit_report(report: &FleetStatusReport, as_json: bool) -> Result<()> {
    if as_json {
        let json = serde_json::to_string_pretty(report)
            .map_err(|e| SurgeError::Config(format!("Failed to encode fleet status as JSON: {e}")))?;
        logline::emit_raw(&json);
        return Ok(());
    }

    logline::info(&format!(
        "Fleet status for '{}' v{} ({}, {}) across {} node(s)",
        report.app_id, report.version, report.channel, report.rid, report.total
    ));
    for node in &report.nodes {
        let message = if let Some(reason) = node.reason.as_deref() {
            format!("{}: {} - {reason}", node.node, node.status.as_str())
        } else {
            format!("{}: {}", node.node, node.status.as_str())
        };
        if node.status == NodeHealthStatus::Healthy {
            logline::success(&message);
        } else {
            logline::warn(&message);
        }
    }
    if report.unhealthy == 0 {
        logline::success(&format!("All {} node(s) are healthy.", report.total));
    } else {
        logline::warn(&format!(
            "{} of {} node(s) are unhealthy or unreachable.",
            report.unhealthy, report.total
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> ProbeConfig {
        ProbeConfig {
            app_id: "sample-app".to_string(),
            rid: "linux-x64".to_string(),
            channel: "sample-channel".to_string(),
            version: "1.2.3".to_string(),
            timeout: Duration::from_secs(20),
        }
    }

    fn sample_manifest() -> RuntimeManifest {
        RuntimeManifest {
            id: "sample-app".to_string(),
            version: "1.2.3".to_string(),
            channel: "sample-channel".to_string(),
            install_directory: "sample-app".to_string(),
            supervisor_id: "sample-supervisor".to_string(),
        }
    }

    fn healthy_process() -> ProcessSummary {
        ProcessSummary {
            proc_available: true,
            app: AppProcessSummary {
                app_process_running: true,
                target_app_process_running: true,
                stale_app_process_running: false,
            },
            supervisor: SupervisorProcessSummary {
                supervisor_configured: true,
                supervisor_process_running: true,
                handoff: SupervisorHandoffSummary {
                    supervisor_waiting_for_previous_child: false,
                    stale_supervisor_process_running: false,
                },
            },
        }
    }

    #[test]
    fn collect_nodes_combines_inline_and_file_entries() {
        let tmp = tempfile::NamedTempFile::new().expect("nodes file");
        std::fs::write(tmp.path(), "\n# ignored\nnode-b\noperator@example-node\n").expect("write nodes file");
        let nodes = collect_nodes(&["node-a".to_string()], Some(tmp.path())).expect("nodes should parse");

        assert_eq!(nodes, ["node-a", "node-b", "operator@example-node"]);
    }

    #[test]
    fn classify_node_marks_matching_runtime_healthy() {
        let (status, reason) = classify_node(&sample_config(), &sample_manifest(), None, &healthy_process());

        assert_eq!(status, NodeHealthStatus::Healthy);
        assert!(reason.is_none());
    }

    #[test]
    fn classify_node_detects_wrong_app() {
        let mut manifest = sample_manifest();
        manifest.id = "other-app".to_string();

        let (status, reason) = classify_node(&sample_config(), &manifest, None, &healthy_process());

        assert_eq!(status, NodeHealthStatus::WrongApp);
        assert!(reason.expect("reason").contains("other-app"));
    }

    #[test]
    fn classify_node_detects_wrong_channel() {
        let mut manifest = sample_manifest();
        manifest.channel = "other-channel".to_string();

        let (status, reason) = classify_node(&sample_config(), &manifest, None, &healthy_process());

        assert_eq!(status, NodeHealthStatus::WrongChannel);
        assert!(reason.expect("reason").contains("other-channel"));
    }

    #[test]
    fn classify_node_detects_stale_version() {
        let mut manifest = sample_manifest();
        manifest.version = "1.2.2".to_string();

        let (status, reason) = classify_node(&sample_config(), &manifest, None, &healthy_process());

        assert_eq!(status, NodeHealthStatus::Stale);
        assert!(reason.expect("reason").contains("1.2.2"));
    }

    #[test]
    fn classify_node_detects_degraded_process_state() {
        let mut process = healthy_process();
        process.app.app_process_running = false;
        process.app.target_app_process_running = false;

        let (status, reason) = classify_node(&sample_config(), &sample_manifest(), None, &process);

        assert_eq!(status, NodeHealthStatus::Degraded);
        assert!(reason.expect("reason").contains("app process was not found"));
    }
}
