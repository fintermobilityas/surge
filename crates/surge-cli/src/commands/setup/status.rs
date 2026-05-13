use std::path::{Path, PathBuf};

use serde::Deserialize;
use surge_core::config::installer::InstallerManifest;
use surge_core::install::RUNTIME_MANIFEST_RELATIVE_PATH;
use surge_core::update::status::{self as update_status, FailureContext, UpdateStatusRecord, write_update_status};

pub(super) const PHASE_STAGE_RECEIVED: &str = "stage received";
pub(super) const PHASE_RELEASE_RESOLVED: &str = "release or delta resolved";
pub(super) const PHASE_PACKAGE_DOWNLOADING: &str = "package downloading";
pub(super) const PHASE_PACKAGE_DOWNLOADED: &str = "package downloaded";
pub(super) const PHASE_SUPERVISOR_STOP_REQUESTED: &str = "supervisor stop requested";
pub(super) const PHASE_APP_SWAP_STARTED: &str = "app swap started";
pub(super) const PHASE_PERSISTENT_ASSETS_COPIED: &str = "persistent assets copied";
pub(super) const PHASE_SUPERVISOR_RESTART_REQUESTED: &str = "supervisor restart requested";
pub(super) const PHASE_SUPERVISOR_RESTART_CONFIRMED: &str = "supervisor restart confirmed";

const NOT_INSTALLED_VERSION: &str = "not_installed";

#[derive(Debug)]
pub(super) struct SetupStatus {
    install_root: PathBuf,
    app_id: String,
    installed_version: String,
    target_version: String,
    channel: String,
    attempted_at_utc: String,
}

impl SetupStatus {
    pub(super) fn new(manifest: &InstallerManifest, install_root: &Path) -> Self {
        Self {
            install_root: install_root.to_path_buf(),
            app_id: manifest.app_id.trim().to_string(),
            installed_version: installed_version(install_root),
            target_version: manifest.version.trim().to_string(),
            channel: manifest.channel.trim().to_string(),
            attempted_at_utc: update_status::now_utc_rfc3339(),
        }
    }

    pub(super) fn record_phase(&self, phase: &'static str) {
        let now = update_status::now_utc_rfc3339();
        let mut record = self.in_progress_record().with_current_phase_at(phase, now);
        if let Some(existing) = self.current_record() {
            record.last_completed_phase = existing.last_completed_phase;
        }
        self.write(&record);
    }

    pub(super) fn record_completed_phase(&self, phase: &'static str) {
        let record = self
            .in_progress_record()
            .with_completed_phase_at(phase, update_status::now_utc_rfc3339());
        self.write(&record);
    }

    pub(super) fn record_converged(&self, supervisor_restart_confirmed: bool) {
        self.write(&UpdateStatusRecord::converged(
            &self.app_id,
            &self.target_version,
            &self.channel,
            Some(self.attempted_at_utc.clone()),
            update_status::now_utc_rfc3339(),
            supervisor_restart_confirmed,
        ));
    }

    pub(super) fn record_pending_restart(&self, reason: &str) {
        self.write(&UpdateStatusRecord::pending_restart(
            &self.app_id,
            &self.target_version,
            &self.target_version,
            &self.channel,
            self.attempted_at_utc.clone(),
            update_status::now_utc_rfc3339(),
            reason,
        ));
    }

    pub(super) fn record_failed(&self, reason: &str) {
        let current = self.current_record();
        self.write(&UpdateStatusRecord::failed_with_context(
            &self.app_id,
            &self.installed_version,
            &self.target_version,
            &self.channel,
            self.attempted_at_utc.clone(),
            reason,
            FailureContext::from_record(current.as_ref(), true),
        ));
    }

    fn in_progress_record(&self) -> UpdateStatusRecord {
        UpdateStatusRecord::in_progress(
            &self.app_id,
            &self.installed_version,
            &self.target_version,
            &self.channel,
            self.attempted_at_utc.clone(),
        )
    }

    fn current_record(&self) -> Option<UpdateStatusRecord> {
        update_status::read_update_status(&self.install_root)
            .ok()
            .flatten()
            .filter(|record| record.app_id == self.app_id && record.target_version == self.target_version)
    }

    fn write(&self, record: &UpdateStatusRecord) {
        let _ = write_update_status(&self.install_root, record);
    }
}

#[derive(Debug, Deserialize)]
struct RuntimeManifest {
    #[serde(default)]
    version: String,
}

fn installed_version(install_root: &Path) -> String {
    let path = install_root.join("app").join(RUNTIME_MANIFEST_RELATIVE_PATH);
    let Ok(bytes) = std::fs::read(path) else {
        return NOT_INSTALLED_VERSION.to_string();
    };
    let Ok(manifest) = serde_yaml::from_slice::<RuntimeManifest>(&bytes) else {
        return NOT_INSTALLED_VERSION.to_string();
    };
    let version = manifest.version.trim();
    if version.is_empty() {
        NOT_INSTALLED_VERSION.to_string()
    } else {
        version.to_string()
    }
}
