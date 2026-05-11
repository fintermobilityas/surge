use std::path::Path;

use surge_core::error::{Result, SurgeError};
use surge_core::update::status::{read_update_status, update_status_path};

use crate::logline;

pub(crate) fn execute(install_dir: &Path, as_json: bool) -> Result<()> {
    if !install_dir.is_dir() {
        return Err(SurgeError::NotFound(format!(
            "Install directory not found: {}",
            install_dir.display()
        )));
    }

    let record = read_update_status(install_dir)?;
    match record {
        Some(record) if as_json => {
            let json = serde_json::to_string_pretty(&record)
                .map_err(|e| SurgeError::Config(format!("Failed to encode status as JSON: {e}")))?;
            logline::emit_raw(&json);
        }
        Some(record) => {
            logline::info(&format!("state: {}", record.state));
            logline::info(&format!("app_id: {}", record.app_id));
            logline::info(&format!("channel: {}", record.channel));
            logline::info(&format!("installed_version: {}", record.installed_version));
            logline::info(&format!("target_version: {}", record.target_version));
            logline::info(&format!(
                "supervisor_restart_confirmed: {}",
                record.supervisor_restart_confirmed
            ));
            if let Some(attempted) = record.attempted_at_utc.as_deref() {
                logline::info(&format!("attempted_at_utc: {attempted}"));
            }
            if let Some(completed) = record.completed_at_utc.as_deref() {
                logline::info(&format!("completed_at_utc: {completed}"));
            }
            if let Some(reason) = record.reason.as_deref() {
                logline::info(&format!("reason: {reason}"));
            }
        }
        None => {
            let path = update_status_path(install_dir);
            if as_json {
                logline::emit_raw("null");
            } else {
                logline::info(&format!("No update status record present at {}", path.display()));
                logline::info(
                    "The install has not run through a Surge update flow that writes the convergence record yet.",
                );
            }
        }
    }
    Ok(())
}
