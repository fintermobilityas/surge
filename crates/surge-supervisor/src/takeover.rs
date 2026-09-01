use std::path::Path;

use surge_core::platform::process::ProcessIdentity;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildObservation {
    Absent,
    Verified(ProcessIdentity),
    Unverified(u32),
}

impl ChildObservation {
    fn pid(self) -> Option<u32> {
        match self {
            Self::Absent => None,
            Self::Verified(identity) => Some(identity.pid),
            Self::Unverified(pid) => Some(pid),
        }
    }

    fn verified_identity(self) -> Option<ProcessIdentity> {
        match self {
            Self::Verified(identity) => Some(identity),
            Self::Absent | Self::Unverified(_) => None,
        }
    }
}

pub(crate) fn complete_supervisor_takeover(
    install_dir: &Path,
    supervisor_id: &str,
    instance: &SupervisorTakeoverInstance,
    stop_triggers: &StopTriggers<'_>,
    shutdown: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    supervisor_pid_file: &Path,
    own_pid: u32,
    mut current_child: impl FnMut() -> ChildObservation,
) -> Result<TakeoverResolution, SupervisorError> {
    let mut acknowledgement: Option<SupervisorTakeoverAcknowledgement> = None;
    let mut warned_unverified_request: Option<String> = None;

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(TakeoverResolution::ShutdownRequested);
        }
        if supervisor_was_superseded(supervisor_pid_file, own_pid) {
            return Ok(TakeoverResolution::Superseded);
        }

        let request = match read_supervisor_takeover_request(install_dir, supervisor_id) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(legacy_stop_or_cancelled(stop_triggers, &mut current_child)),
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
                    return Ok(legacy_stop_or_cancelled(stop_triggers, &mut current_child));
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
                    return Ok(legacy_stop_or_cancelled(stop_triggers, &mut current_child));
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

        let observed_child = current_child();
        if let ChildObservation::Unverified(pid) = observed_child {
            if warned_unverified_request.as_deref() != Some(&request.request_token) {
                tracing::warn!(
                    pid,
                    supervisor_id,
                    "Cannot acknowledge takeover without a verified child process generation; continuing supervision"
                );
                warned_unverified_request = Some(request.request_token.clone());
            }
            std::thread::sleep(TAKEOVER_POLL_INTERVAL);
            continue;
        }
        let observed_child_identity = observed_child.verified_identity();
        let acknowledgement_matches_observation = acknowledgement.as_ref().is_some_and(|acknowledgement| {
            acknowledgement.matches_request(&request) && acknowledgement.child_identity == observed_child_identity
        });
        if !acknowledgement_matches_observation {
            let next_acknowledgement = SupervisorTakeoverAcknowledgement::new(&request, observed_child_identity);
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

        let refreshed_child = current_child();
        if matches!(refreshed_child, ChildObservation::Unverified(_))
            || refreshed_child.verified_identity() != current_acknowledgement.child_identity
        {
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
    current_child: &mut impl FnMut() -> ChildObservation,
) -> TakeoverResolution {
    if stop_triggers.legacy_requested() {
        TakeoverResolution::LegacyStop(current_child().pid())
    } else {
        TakeoverResolution::Cancelled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unverified_child_keeps_supervision_until_request_is_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let supervisor_id = "demo-supervisor";
        let instance = SupervisorTakeoverInstance::new(std::process::id());
        let request = surge_core::supervisor::state::SupervisorTakeoverRequest::new(
            &instance,
            std::time::Duration::from_millis(25),
        );
        let request_file = surge_core::supervisor::state::supervisor_takeover_request_file(dir.path(), supervisor_id);
        surge_core::supervisor::state::write_supervisor_takeover_request(dir.path(), supervisor_id, &request).unwrap();
        let legacy_stop_file = dir.path().join("legacy.stop");
        let pid_file = dir.path().join("supervisor.pid");
        std::fs::write(&pid_file, std::process::id().to_string()).unwrap();
        let stop_triggers = StopTriggers::new(&legacy_stop_file, Some(&request_file));
        let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let outcome = complete_supervisor_takeover(
            dir.path(),
            supervisor_id,
            &instance,
            &stop_triggers,
            &shutdown,
            &pid_file,
            std::process::id(),
            || ChildObservation::Unverified(84),
        )
        .unwrap();

        assert_eq!(outcome, TakeoverResolution::Cancelled);
        assert!(pid_file.exists());
        assert!(
            surge_core::supervisor::state::read_supervisor_takeover_acknowledgement(dir.path(), supervisor_id)
                .unwrap()
                .is_none()
        );
    }
}
