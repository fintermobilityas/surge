use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use crate::error::{Result, SurgeError};
use crate::platform::fs::write_file_atomic;

use super::supervisor_state_path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTakeoverInstance {
    pub supervisor_pid: u32,
    pub instance_token: String,
}

impl SupervisorTakeoverInstance {
    #[must_use]
    pub fn new(supervisor_pid: u32) -> Self {
        Self {
            supervisor_pid,
            instance_token: new_token(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.supervisor_pid == 0 || self.instance_token.trim().is_empty() {
            return Err(invalid_record("supervisor instance"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTakeoverRequest {
    pub supervisor_pid: u32,
    pub instance_token: String,
    pub request_token: String,
    pub expires_at_unix_ms: i64,
}

impl SupervisorTakeoverRequest {
    #[must_use]
    pub fn new(instance: &SupervisorTakeoverInstance, lease: Duration) -> Self {
        let lease_ms = i64::try_from(lease.as_millis()).unwrap_or(i64::MAX);
        Self {
            supervisor_pid: instance.supervisor_pid,
            instance_token: instance.instance_token.clone(),
            request_token: new_token(),
            expires_at_unix_ms: chrono::Utc::now().timestamp_millis().saturating_add(lease_ms),
        }
    }

    #[must_use]
    pub fn matches_instance(&self, instance: &SupervisorTakeoverInstance) -> bool {
        self.supervisor_pid == instance.supervisor_pid && self.instance_token == instance.instance_token
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(chrono::Utc::now().timestamp_millis())
    }

    #[must_use]
    pub fn is_expired_at(&self, unix_ms: i64) -> bool {
        unix_ms >= self.expires_at_unix_ms
    }

    fn validate(&self) -> Result<()> {
        if self.supervisor_pid == 0
            || self.instance_token.trim().is_empty()
            || self.request_token.trim().is_empty()
            || self.expires_at_unix_ms <= 0
        {
            return Err(invalid_record("takeover request"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTakeoverAcknowledgement {
    pub supervisor_pid: u32,
    pub instance_token: String,
    pub request_token: String,
    pub acknowledgement_token: String,
    pub child_pid: Option<u32>,
}

impl SupervisorTakeoverAcknowledgement {
    #[must_use]
    pub fn new(request: &SupervisorTakeoverRequest, child_pid: Option<u32>) -> Self {
        Self {
            supervisor_pid: request.supervisor_pid,
            instance_token: request.instance_token.clone(),
            request_token: request.request_token.clone(),
            acknowledgement_token: new_token(),
            child_pid,
        }
    }

    #[must_use]
    pub fn matches_request(&self, request: &SupervisorTakeoverRequest) -> bool {
        self.supervisor_pid == request.supervisor_pid
            && self.instance_token == request.instance_token
            && self.request_token == request.request_token
    }

    fn validate(&self) -> Result<()> {
        if self.supervisor_pid == 0
            || self.instance_token.trim().is_empty()
            || self.request_token.trim().is_empty()
            || self.acknowledgement_token.trim().is_empty()
            || self.child_pid == Some(0)
        {
            return Err(invalid_record("takeover acknowledgement"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTakeoverCommit {
    pub supervisor_pid: u32,
    pub instance_token: String,
    pub request_token: String,
    pub acknowledgement_token: String,
}

impl SupervisorTakeoverCommit {
    #[must_use]
    pub fn new(acknowledgement: &SupervisorTakeoverAcknowledgement) -> Self {
        Self {
            supervisor_pid: acknowledgement.supervisor_pid,
            instance_token: acknowledgement.instance_token.clone(),
            request_token: acknowledgement.request_token.clone(),
            acknowledgement_token: acknowledgement.acknowledgement_token.clone(),
        }
    }

    #[must_use]
    pub fn matches_acknowledgement(&self, acknowledgement: &SupervisorTakeoverAcknowledgement) -> bool {
        self.supervisor_pid == acknowledgement.supervisor_pid
            && self.instance_token == acknowledgement.instance_token
            && self.request_token == acknowledgement.request_token
            && self.acknowledgement_token == acknowledgement.acknowledgement_token
    }

    fn validate(&self) -> Result<()> {
        if self.supervisor_pid == 0
            || self.instance_token.trim().is_empty()
            || self.request_token.trim().is_empty()
            || self.acknowledgement_token.trim().is_empty()
        {
            return Err(invalid_record("takeover commit"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisorTakeoverHandoff {
    pub supervisor_pid: u32,
    pub instance_token: String,
    pub request_token: String,
    pub child_pid: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorTakeoverCancellation {
    Cancelled,
    Accepted,
    Missing,
    Replaced,
}

#[must_use]
pub fn supervisor_takeover_request_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    takeover_request_file(install_dir, supervisor_id)
}

pub fn write_supervisor_takeover_instance(
    install_dir: &Path,
    supervisor_id: &str,
    instance: &SupervisorTakeoverInstance,
) -> Result<()> {
    instance.validate()?;
    write_record(&takeover_instance_file(install_dir, supervisor_id), instance)
}

pub fn read_supervisor_takeover_instance(
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<Option<SupervisorTakeoverInstance>> {
    let instance: Option<SupervisorTakeoverInstance> =
        read_record(&takeover_instance_file(install_dir, supervisor_id))?;
    if let Some(instance) = &instance {
        instance.validate()?;
    }
    Ok(instance)
}

pub fn clear_supervisor_takeover_instance_if_owned(
    install_dir: &Path,
    supervisor_id: &str,
    expected: &SupervisorTakeoverInstance,
) -> Result<bool> {
    if read_supervisor_takeover_instance(install_dir, supervisor_id)?.as_ref() != Some(expected) {
        return Ok(false);
    }
    remove_file_if_exists(&takeover_instance_file(install_dir, supervisor_id))?;
    Ok(true)
}

pub fn write_supervisor_takeover_request(
    install_dir: &Path,
    supervisor_id: &str,
    request: &SupervisorTakeoverRequest,
) -> Result<()> {
    request.validate()?;
    write_record(&takeover_request_file(install_dir, supervisor_id), request)
}

pub fn read_supervisor_takeover_request(
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<Option<SupervisorTakeoverRequest>> {
    let request: Option<SupervisorTakeoverRequest> = read_record(&takeover_request_file(install_dir, supervisor_id))?;
    if let Some(request) = &request {
        request.validate()?;
    }
    Ok(request)
}

pub fn clear_supervisor_takeover_request(install_dir: &Path, supervisor_id: &str) -> Result<()> {
    remove_file_if_exists(&takeover_request_file(install_dir, supervisor_id))
}

pub fn write_supervisor_takeover_acknowledgement(
    install_dir: &Path,
    supervisor_id: &str,
    acknowledgement: &SupervisorTakeoverAcknowledgement,
) -> Result<()> {
    acknowledgement.validate()?;
    write_record(
        &takeover_acknowledgement_file(install_dir, supervisor_id),
        acknowledgement,
    )
}

pub fn read_supervisor_takeover_acknowledgement(
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<Option<SupervisorTakeoverAcknowledgement>> {
    let acknowledgement: Option<SupervisorTakeoverAcknowledgement> =
        read_record(&takeover_acknowledgement_file(install_dir, supervisor_id))?;
    if let Some(acknowledgement) = &acknowledgement {
        acknowledgement.validate()?;
    }
    Ok(acknowledgement)
}

pub fn write_supervisor_takeover_commit(
    install_dir: &Path,
    supervisor_id: &str,
    commit: &SupervisorTakeoverCommit,
) -> Result<()> {
    commit.validate()?;
    write_record(&takeover_commit_file(install_dir, supervisor_id), commit)
}

pub fn read_supervisor_takeover_commit(
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<Option<SupervisorTakeoverCommit>> {
    let commit: Option<SupervisorTakeoverCommit> = read_record(&takeover_commit_file(install_dir, supervisor_id))?;
    if let Some(commit) = &commit {
        commit.validate()?;
    }
    Ok(commit)
}

pub fn clear_supervisor_takeover_exchange(install_dir: &Path, supervisor_id: &str) -> Result<()> {
    remove_file_if_exists(&takeover_request_file(install_dir, supervisor_id))?;
    remove_file_if_exists(&takeover_acknowledgement_file(install_dir, supervisor_id))?;
    remove_file_if_exists(&takeover_commit_file(install_dir, supervisor_id))?;
    remove_file_if_exists(&takeover_accepted_file(install_dir, supervisor_id))?;
    Ok(())
}

pub fn accept_supervisor_takeover_request(
    install_dir: &Path,
    supervisor_id: &str,
    expected: &SupervisorTakeoverRequest,
) -> Result<bool> {
    if read_supervisor_takeover_request(install_dir, supervisor_id)?.as_ref() != Some(expected) {
        return Ok(false);
    }
    let accepted_path = takeover_accepted_file(install_dir, supervisor_id);
    if accepted_path.try_exists()? {
        return Err(SurgeError::Update(
            "Cannot accept supervisor takeover request while another accepted handoff record exists".to_string(),
        ));
    }
    match std::fs::rename(takeover_request_file(install_dir, supervisor_id), &accepted_path) {
        Ok(()) => {
            let accepted: SupervisorTakeoverRequest =
                read_required_record(&accepted_path, "accepted takeover request")?;
            if &accepted != expected {
                return Err(SurgeError::Update(
                    "Accepted supervisor takeover request changed during ownership transfer".to_string(),
                ));
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub fn cancel_supervisor_takeover_request(
    install_dir: &Path,
    supervisor_id: &str,
    expected: &SupervisorTakeoverRequest,
) -> Result<SupervisorTakeoverCancellation> {
    match read_supervisor_takeover_request(install_dir, supervisor_id)? {
        Some(current) if current == *expected => {
            match std::fs::remove_file(takeover_request_file(install_dir, supervisor_id)) {
                Ok(()) => return Ok(SupervisorTakeoverCancellation::Cancelled),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Some(_) => return Ok(SupervisorTakeoverCancellation::Replaced),
        None => {}
    }

    if read_accepted_request(install_dir, supervisor_id)?.as_ref() == Some(expected) {
        Ok(SupervisorTakeoverCancellation::Accepted)
    } else {
        Ok(SupervisorTakeoverCancellation::Missing)
    }
}

pub fn read_accepted_supervisor_takeover(
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<Option<SupervisorTakeoverHandoff>> {
    let Some(accepted) = read_accepted_request(install_dir, supervisor_id)? else {
        return Ok(None);
    };
    let acknowledgement: SupervisorTakeoverAcknowledgement = read_required_record(
        &takeover_acknowledgement_file(install_dir, supervisor_id),
        "takeover acknowledgement",
    )?;
    acknowledgement.validate()?;
    if !acknowledgement.matches_request(&accepted) {
        return Err(SurgeError::Update(
            "Supervisor takeover acknowledgement does not match the accepted request".to_string(),
        ));
    }
    let commit: SupervisorTakeoverCommit =
        read_required_record(&takeover_commit_file(install_dir, supervisor_id), "takeover commit")?;
    commit.validate()?;
    if !commit.matches_acknowledgement(&acknowledgement) {
        return Err(SurgeError::Update(
            "Supervisor takeover commit does not match the accepted acknowledgement".to_string(),
        ));
    }
    Ok(Some(SupervisorTakeoverHandoff {
        supervisor_pid: accepted.supervisor_pid,
        instance_token: accepted.instance_token,
        request_token: accepted.request_token,
        child_pid: acknowledgement.child_pid,
    }))
}

pub fn take_accepted_supervisor_takeover(
    install_dir: &Path,
    supervisor_id: &str,
) -> Result<Option<SupervisorTakeoverHandoff>> {
    let handoff = read_accepted_supervisor_takeover(install_dir, supervisor_id)?;
    let Some(handoff) = handoff else {
        return Ok(None);
    };

    for path in [
        takeover_accepted_file(install_dir, supervisor_id),
        takeover_acknowledgement_file(install_dir, supervisor_id),
        takeover_commit_file(install_dir, supervisor_id),
    ] {
        if let Err(error) = remove_file_if_exists(&path) {
            warn!(path = %path.display(), %error, "Failed to clean up consumed supervisor takeover state");
        }
    }
    Ok(Some(handoff))
}

fn read_accepted_request(install_dir: &Path, supervisor_id: &str) -> Result<Option<SupervisorTakeoverRequest>> {
    let accepted: Option<SupervisorTakeoverRequest> = read_record(&takeover_accepted_file(install_dir, supervisor_id))?;
    if let Some(accepted) = &accepted {
        accepted.validate()?;
    }
    Ok(accepted)
}

fn takeover_instance_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".takeover.instance.json")
}

fn takeover_request_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".takeover.request.json")
}

fn takeover_acknowledgement_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".takeover.ack.json")
}

fn takeover_commit_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".takeover.commit.json")
}

fn takeover_accepted_file(install_dir: &Path, supervisor_id: &str) -> PathBuf {
    supervisor_state_path(install_dir, supervisor_id, ".takeover.accepted.json")
}

fn new_token() -> String {
    Uuid::new_v4().simple().to_string()
}

fn write_record<T: Serialize>(path: &Path, record: &T) -> Result<()> {
    let encoded = serde_json::to_vec(record)?;
    write_file_atomic(path, &encoded)
}

fn read_record<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(encoded) => Ok(Some(serde_json::from_slice(&encoded)?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_required_record<T: DeserializeOwned>(path: &Path, description: &str) -> Result<T> {
    read_record(path)?.ok_or_else(|| SurgeError::Update(format!("Missing {description}")))
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn invalid_record(description: &str) -> SurgeError {
    SurgeError::Update(format!("Invalid {description} record"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_acknowledgement_tokens_are_unique_and_bound() {
        let first_instance = SupervisorTakeoverInstance::new(42);
        let second_instance = SupervisorTakeoverInstance::new(42);
        let first_request = SupervisorTakeoverRequest::new(&first_instance, Duration::from_secs(5));
        let second_request = SupervisorTakeoverRequest::new(&first_instance, Duration::from_secs(5));
        let acknowledgement = SupervisorTakeoverAcknowledgement::new(&first_request, Some(84));
        let commit = SupervisorTakeoverCommit::new(&acknowledgement);

        assert_ne!(first_instance.instance_token, second_instance.instance_token);
        assert_ne!(first_request.request_token, second_request.request_token);
        assert!(first_request.matches_instance(&first_instance));
        assert!(!first_request.matches_instance(&second_instance));
        assert!(acknowledgement.matches_request(&first_request));
        assert!(!acknowledgement.matches_request(&second_request));
        assert!(commit.matches_acknowledgement(&acknowledgement));
    }

    #[test]
    fn request_expiry_uses_an_explicit_lease_boundary() {
        let instance = SupervisorTakeoverInstance::new(42);
        let request = SupervisorTakeoverRequest {
            supervisor_pid: instance.supervisor_pid,
            instance_token: instance.instance_token,
            request_token: "request".to_string(),
            expires_at_unix_ms: 1_000,
        };

        assert!(!request.is_expired_at(999));
        assert!(request.is_expired_at(1_000));
    }

    #[test]
    fn accepted_request_wins_the_cancellation_race() {
        let dir = tempfile::tempdir().unwrap();
        let instance = SupervisorTakeoverInstance::new(42);
        let request = SupervisorTakeoverRequest::new(&instance, Duration::from_secs(5));
        write_supervisor_takeover_request(dir.path(), "demo", &request).unwrap();

        assert!(accept_supervisor_takeover_request(dir.path(), "demo", &request).unwrap());
        assert_eq!(
            cancel_supervisor_takeover_request(dir.path(), "demo", &request).unwrap(),
            SupervisorTakeoverCancellation::Accepted
        );
    }

    #[test]
    fn cancelled_request_cannot_be_accepted_later() {
        let dir = tempfile::tempdir().unwrap();
        let instance = SupervisorTakeoverInstance::new(42);
        let request = SupervisorTakeoverRequest::new(&instance, Duration::from_secs(5));
        write_supervisor_takeover_request(dir.path(), "demo", &request).unwrap();

        assert_eq!(
            cancel_supervisor_takeover_request(dir.path(), "demo", &request).unwrap(),
            SupervisorTakeoverCancellation::Cancelled
        );
        assert!(!accept_supervisor_takeover_request(dir.path(), "demo", &request).unwrap());
    }

    #[test]
    fn accepted_handoff_requires_matching_acknowledgement_and_commit() {
        let dir = tempfile::tempdir().unwrap();
        let instance = SupervisorTakeoverInstance::new(42);
        let request = SupervisorTakeoverRequest::new(&instance, Duration::from_secs(5));
        let acknowledgement = SupervisorTakeoverAcknowledgement::new(&request, Some(84));
        let commit = SupervisorTakeoverCommit::new(&acknowledgement);
        write_supervisor_takeover_request(dir.path(), "demo", &request).unwrap();
        write_supervisor_takeover_acknowledgement(dir.path(), "demo", &acknowledgement).unwrap();
        write_supervisor_takeover_commit(dir.path(), "demo", &commit).unwrap();
        assert!(accept_supervisor_takeover_request(dir.path(), "demo", &request).unwrap());

        let handoff = take_accepted_supervisor_takeover(dir.path(), "demo").unwrap().unwrap();

        assert_eq!(handoff.supervisor_pid, 42);
        assert_eq!(handoff.child_pid, Some(84));
        assert!(read_accepted_supervisor_takeover(dir.path(), "demo").unwrap().is_none());
    }

    #[test]
    fn mismatched_commit_is_rejected_without_consuming_handoff() {
        let dir = tempfile::tempdir().unwrap();
        let instance = SupervisorTakeoverInstance::new(42);
        let request = SupervisorTakeoverRequest::new(&instance, Duration::from_secs(5));
        let acknowledgement = SupervisorTakeoverAcknowledgement::new(&request, None);
        let other_acknowledgement = SupervisorTakeoverAcknowledgement::new(&request, None);
        let commit = SupervisorTakeoverCommit::new(&other_acknowledgement);
        write_supervisor_takeover_request(dir.path(), "demo", &request).unwrap();
        write_supervisor_takeover_acknowledgement(dir.path(), "demo", &acknowledgement).unwrap();
        write_supervisor_takeover_commit(dir.path(), "demo", &commit).unwrap();
        assert!(accept_supervisor_takeover_request(dir.path(), "demo", &request).unwrap());

        let error = take_accepted_supervisor_takeover(dir.path(), "demo").unwrap_err();

        assert!(error.to_string().contains("commit does not match"));
        assert!(read_accepted_request(dir.path(), "demo").unwrap().is_some());
    }

    #[test]
    fn exchange_cleanup_is_fallible() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(takeover_acknowledgement_file(dir.path(), "demo")).unwrap();

        let error = clear_supervisor_takeover_exchange(dir.path(), "demo").unwrap_err();

        assert!(matches!(error, SurgeError::Io(_)));
    }

    #[test]
    fn instance_cleanup_requires_the_exact_launch_token() {
        let dir = tempfile::tempdir().unwrap();
        let first = SupervisorTakeoverInstance::new(42);
        let second = SupervisorTakeoverInstance::new(42);
        write_supervisor_takeover_instance(dir.path(), "demo", &first).unwrap();

        assert!(!clear_supervisor_takeover_instance_if_owned(dir.path(), "demo", &second).unwrap());
        assert!(read_supervisor_takeover_instance(dir.path(), "demo").unwrap().is_some());
        assert!(clear_supervisor_takeover_instance_if_owned(dir.path(), "demo", &first).unwrap());
        assert!(read_supervisor_takeover_instance(dir.path(), "demo").unwrap().is_none());
    }
}
