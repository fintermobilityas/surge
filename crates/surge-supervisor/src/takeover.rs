use std::path::Path;

use surge_core::supervisor::state::{
    SupervisorTakeoverAcknowledgement, SupervisorTakeoverCancellation, SupervisorTakeoverInstance,
    accept_supervisor_takeover_request, cancel_supervisor_takeover_request, read_supervisor_takeover_commit,
    read_supervisor_takeover_request, write_supervisor_takeover_acknowledgement,
};

use crate::SupervisorError;
use crate::child::StopTriggers;
use crate::ownership::supervisor_was_superseded;

const TAKEOVER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TakeoverResolution {
    Accepted,
    Cancelled,
    LegacyStop(Option<u32>),
    ShutdownRequested,
    Superseded,
}

pub(crate) fn complete_supervisor_takeover(
    install_dir: &Path,
    supervisor_id: &str,
    instance: &SupervisorTakeoverInstance,
    stop_triggers: &StopTriggers<'_>,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    supervisor_pid_file: &Path,
    own_pid: u32,
    mut current_child_pid: impl FnMut() -> Result<Option<u32>, SupervisorError>,
) -> Result<TakeoverResolution, SupervisorError> {
    let mut acknowledgement: Option<SupervisorTakeoverAcknowledgement> = None;

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(TakeoverResolution::ShutdownRequested);
        }
        if supervisor_was_superseded(supervisor_pid_file, own_pid) {
            return Ok(TakeoverResolution::Superseded);
        }

        let request = match read_supervisor_takeover_request(install_dir, supervisor_id) {
            Ok(Some(request)) => request,
            Ok(None) => return legacy_stop_or_cancelled(stop_triggers, &mut current_child_pid),
            Err(error) => {
                tracing::warn!(
                    supervisor_id,
                    %error,
                    "Cannot read supervisor takeover request; continuing supervision"
                );
                std::thread::sleep(TAKEOVER_POLL_INTERVAL);
                continue;
            }
        };

        if !request.matches_instance(instance) || request.is_expired() {
            match cancel_supervisor_takeover_request(install_dir, supervisor_id, &request) {
                Ok(SupervisorTakeoverCancellation::Cancelled | SupervisorTakeoverCancellation::Missing) => {
                    return legacy_stop_or_cancelled(stop_triggers, &mut current_child_pid);
                }
                Ok(SupervisorTakeoverCancellation::Replaced) => {
                    acknowledgement = None;
                    continue;
                }
                Ok(SupervisorTakeoverCancellation::Accepted) if request.matches_instance(instance) => {
                    return Ok(TakeoverResolution::Accepted);
                }
                Ok(SupervisorTakeoverCancellation::Accepted) => {
                    tracing::warn!(
                        supervisor_id,
                        "Ignoring an accepted takeover request for a different supervisor instance"
                    );
                    return legacy_stop_or_cancelled(stop_triggers, &mut current_child_pid);
                }
                Err(error) => {
                    tracing::warn!(
                        supervisor_id,
                        %error,
                        "Cannot cancel unusable supervisor takeover request; continuing supervision"
                    );
                    std::thread::sleep(TAKEOVER_POLL_INTERVAL);
                    continue;
                }
            }
        }

        let observed_child_pid = current_child_pid()?;
        let acknowledgement_matches_observation = acknowledgement.as_ref().is_some_and(|acknowledgement| {
            acknowledgement.matches_request(&request) && acknowledgement.child_pid == observed_child_pid
        });
        if !acknowledgement_matches_observation {
            let next_acknowledgement = SupervisorTakeoverAcknowledgement::new(&request, observed_child_pid);
            match write_supervisor_takeover_acknowledgement(install_dir, supervisor_id, &next_acknowledgement) {
                Ok(()) => acknowledgement = Some(next_acknowledgement),
                Err(error) => {
                    tracing::warn!(
                        supervisor_id,
                        %error,
                        "Cannot persist supervisor takeover acknowledgement; continuing supervision"
                    );
                    std::thread::sleep(TAKEOVER_POLL_INTERVAL);
                    continue;
                }
            }
        }

        let Some(current_acknowledgement) = acknowledgement.as_ref() else {
            continue;
        };
        let commit = match read_supervisor_takeover_commit(install_dir, supervisor_id) {
            Ok(commit) => commit,
            Err(error) => {
                tracing::warn!(
                    supervisor_id,
                    %error,
                    "Cannot read supervisor takeover commit; continuing supervision"
                );
                std::thread::sleep(TAKEOVER_POLL_INTERVAL);
                continue;
            }
        };
        let Some(commit) = commit else {
            std::thread::sleep(TAKEOVER_POLL_INTERVAL);
            continue;
        };
        if !commit.matches_acknowledgement(current_acknowledgement) {
            std::thread::sleep(TAKEOVER_POLL_INTERVAL);
            continue;
        }

        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(TakeoverResolution::ShutdownRequested);
        }
        if supervisor_was_superseded(supervisor_pid_file, own_pid) {
            return Ok(TakeoverResolution::Superseded);
        }
        if request.is_expired() {
            continue;
        }

        let refreshed_child_pid = current_child_pid()?;
        if refreshed_child_pid != current_acknowledgement.child_pid {
            acknowledgement = None;
            continue;
        }

        match accept_supervisor_takeover_request(install_dir, supervisor_id, &request) {
            Ok(true) => return Ok(TakeoverResolution::Accepted),
            Ok(false) => {
                acknowledgement = None;
            }
            Err(error) => {
                tracing::warn!(
                    supervisor_id,
                    %error,
                    "Cannot accept supervisor takeover request; continuing supervision"
                );
                std::thread::sleep(TAKEOVER_POLL_INTERVAL);
            }
        }
    }
}

fn legacy_stop_or_cancelled(
    stop_triggers: &StopTriggers<'_>,
    current_child_pid: &mut impl FnMut() -> Result<Option<u32>, SupervisorError>,
) -> Result<TakeoverResolution, SupervisorError> {
    if stop_triggers.legacy_requested() {
        Ok(TakeoverResolution::LegacyStop(current_child_pid()?))
    } else {
        Ok(TakeoverResolution::Cancelled)
    }
}
