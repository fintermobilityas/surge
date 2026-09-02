use std::path::Path;
use std::time::Duration;

use crate::error::{Result, SurgeError};
use crate::platform::process::ProcessIdentity;
use crate::supervisor::state::{
    SupervisorTakeoverCancellation, SupervisorTakeoverCommit, SupervisorTakeoverHandoff, SupervisorTakeoverInstance,
    SupervisorTakeoverRequest, cancel_supervisor_takeover_request, clear_supervisor_takeover_exchange,
    read_accepted_supervisor_takeover, read_supervisor_takeover_acknowledgement, read_supervisor_takeover_instance,
    read_supervisor_takeover_pid, supervisor_pid_file, take_accepted_supervisor_takeover,
    try_clear_supervisor_takeover_pid, write_supervisor_takeover_commit, write_supervisor_takeover_request,
};

const SUPERVISOR_TAKEOVER_EXIT_GRACE: Duration = Duration::from_secs(2);

pub(super) async fn request_shutdown(
    install_dir: &Path,
    supervisor_id: &str,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<Option<ProcessIdentity>> {
    let pid_file = supervisor_pid_file(install_dir, supervisor_id);
    let Some(owner_pid) = read_supervisor_pid_owner(&pid_file)? else {
        if let Some(handoff) = take_accepted_supervisor_takeover(install_dir, supervisor_id)? {
            return Ok(handoff.child_identity);
        }
        if let Some(pid) = read_supervisor_takeover_pid(install_dir, supervisor_id)? {
            return Err(SurgeError::Update(format!(
                "Legacy supervisor takeover state for PID {pid} has no process generation; refusing to continue the update"
            )));
        }
        return Ok(None);
    };

    let instance = read_supervisor_takeover_instance(install_dir, supervisor_id)?.ok_or_else(|| {
        SurgeError::Update(format!(
            "Supervisor '{supervisor_id}' does not advertise acknowledged takeover support"
        ))
    })?;
    if instance.supervisor_pid != owner_pid {
        return Err(SurgeError::Update(format!(
            "Supervisor '{supervisor_id}' takeover instance belongs to PID {}, but its pid file belongs to PID {owner_pid}",
            instance.supervisor_pid
        )));
    }

    if let Some(handoff) = read_accepted_supervisor_takeover(install_dir, supervisor_id)? {
        ensure_handoff_matches_instance(supervisor_id, &handoff, &instance)?;
        return finish_accepted_takeover(install_dir, supervisor_id, &pid_file, &instance, None, poll_interval).await;
    }

    clear_supervisor_takeover_exchange(install_dir, supervisor_id)?;
    try_clear_supervisor_takeover_pid(install_dir, supervisor_id)?;

    let request = SupervisorTakeoverRequest::new(&instance, timeout.saturating_add(poll_interval));
    write_supervisor_takeover_request(install_dir, supervisor_id, &request)?;
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Some(handoff) = read_accepted_supervisor_takeover(install_dir, supervisor_id)? {
            ensure_handoff_matches_request(supervisor_id, &handoff, &request)?;
            return finish_accepted_takeover(
                install_dir,
                supervisor_id,
                &pid_file,
                &instance,
                Some(&request),
                poll_interval,
            )
            .await;
        }

        if let Some(acknowledgement) = read_supervisor_takeover_acknowledgement(install_dir, supervisor_id)? {
            if !acknowledgement.matches_request(&request) {
                if cancel_after_protocol_error(install_dir, supervisor_id, &request)?
                    == SupervisorTakeoverCancellation::Accepted
                {
                    return finish_accepted_takeover(
                        install_dir,
                        supervisor_id,
                        &pid_file,
                        &instance,
                        Some(&request),
                        poll_interval,
                    )
                    .await;
                }
                return Err(SurgeError::Update(format!(
                    "Supervisor '{supervisor_id}' wrote an acknowledgement for a different takeover request"
                )));
            }
            write_supervisor_takeover_commit(
                install_dir,
                supervisor_id,
                &SupervisorTakeoverCommit::new(&acknowledgement),
            )?;
        }

        if read_supervisor_pid_owner(&pid_file)? != Some(owner_pid) {
            if let Some(handoff) = read_accepted_supervisor_takeover(install_dir, supervisor_id)? {
                ensure_handoff_matches_request(supervisor_id, &handoff, &request)?;
                return take_matching_accepted_handoff(install_dir, supervisor_id, &request);
            }
            if cancel_after_protocol_error(install_dir, supervisor_id, &request)?
                == SupervisorTakeoverCancellation::Accepted
            {
                return finish_accepted_takeover(
                    install_dir,
                    supervisor_id,
                    &pid_file,
                    &instance,
                    Some(&request),
                    poll_interval,
                )
                .await;
            }
            return Err(SurgeError::Update(format!(
                "Supervisor '{supervisor_id}' exited without accepting the takeover request"
            )));
        }

        if tokio::time::Instant::now() >= deadline {
            return finish_timed_out_takeover(
                install_dir,
                supervisor_id,
                &pid_file,
                &instance,
                &request,
                poll_interval,
            )
            .await;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn read_supervisor_pid_owner(pid_file: &Path) -> Result<Option<u32>> {
    let contents = match std::fs::read_to_string(pid_file) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let pid = contents.trim().parse::<u32>().map_err(|_| {
        SurgeError::Update(format!(
            "Supervisor pid file '{}' does not contain a valid process id",
            pid_file.display()
        ))
    })?;
    if pid == 0 {
        return Err(SurgeError::Update(format!(
            "Supervisor pid file '{}' contains process id 0",
            pid_file.display()
        )));
    }
    Ok(Some(pid))
}

fn ensure_handoff_matches_instance(
    supervisor_id: &str,
    handoff: &SupervisorTakeoverHandoff,
    instance: &SupervisorTakeoverInstance,
) -> Result<()> {
    if handoff.supervisor_pid == instance.supervisor_pid && handoff.instance_token == instance.instance_token {
        return Ok(());
    }
    Err(SurgeError::Update(format!(
        "Supervisor '{supervisor_id}' has an accepted takeover from a different supervisor instance"
    )))
}

fn ensure_handoff_matches_request(
    supervisor_id: &str,
    handoff: &SupervisorTakeoverHandoff,
    request: &SupervisorTakeoverRequest,
) -> Result<()> {
    if handoff.supervisor_pid == request.supervisor_pid
        && handoff.instance_token == request.instance_token
        && handoff.request_token == request.request_token
    {
        return Ok(());
    }
    Err(SurgeError::Update(format!(
        "Supervisor '{supervisor_id}' accepted a different takeover request"
    )))
}

fn cancel_after_protocol_error(
    install_dir: &Path,
    supervisor_id: &str,
    request: &SupervisorTakeoverRequest,
) -> Result<SupervisorTakeoverCancellation> {
    let cancellation = cancel_supervisor_takeover_request(install_dir, supervisor_id, request)?;
    if cancellation == SupervisorTakeoverCancellation::Cancelled {
        clear_supervisor_takeover_exchange(install_dir, supervisor_id)?;
    }
    Ok(cancellation)
}

async fn finish_timed_out_takeover(
    install_dir: &Path,
    supervisor_id: &str,
    pid_file: &Path,
    instance: &SupervisorTakeoverInstance,
    request: &SupervisorTakeoverRequest,
    poll_interval: Duration,
) -> Result<Option<ProcessIdentity>> {
    match cancel_supervisor_takeover_request(install_dir, supervisor_id, request)? {
        SupervisorTakeoverCancellation::Cancelled => {
            clear_supervisor_takeover_exchange(install_dir, supervisor_id)?;
            Err(SurgeError::Update(format!(
                "Timed out waiting for supervisor '{supervisor_id}' to acknowledge takeover before applying update"
            )))
        }
        SupervisorTakeoverCancellation::Accepted => {
            finish_accepted_takeover(
                install_dir,
                supervisor_id,
                pid_file,
                instance,
                Some(request),
                poll_interval,
            )
            .await
        }
        SupervisorTakeoverCancellation::Missing => Err(SurgeError::Update(format!(
            "Supervisor '{supervisor_id}' removed the takeover request without accepting it"
        ))),
        SupervisorTakeoverCancellation::Replaced => Err(SurgeError::Update(format!(
            "Supervisor '{supervisor_id}' takeover request was replaced before timeout"
        ))),
    }
}

async fn finish_accepted_takeover(
    install_dir: &Path,
    supervisor_id: &str,
    pid_file: &Path,
    instance: &SupervisorTakeoverInstance,
    request: Option<&SupervisorTakeoverRequest>,
    poll_interval: Duration,
) -> Result<Option<ProcessIdentity>> {
    let deadline = tokio::time::Instant::now() + SUPERVISOR_TAKEOVER_EXIT_GRACE;
    while read_supervisor_pid_owner(pid_file)? == Some(instance.supervisor_pid) {
        if tokio::time::Instant::now() >= deadline {
            return Err(SurgeError::Update(format!(
                "Supervisor '{supervisor_id}' accepted takeover but did not release its pid file"
            )));
        }
        tokio::time::sleep(poll_interval).await;
    }

    if let Some(request) = request {
        take_matching_accepted_handoff(install_dir, supervisor_id, request)
    } else {
        let handoff = read_accepted_supervisor_takeover(install_dir, supervisor_id)?.ok_or_else(|| {
            SurgeError::Update(format!(
                "Supervisor '{supervisor_id}' accepted takeover record disappeared before it was consumed"
            ))
        })?;
        ensure_handoff_matches_instance(supervisor_id, &handoff, instance)?;
        let consumed = take_accepted_supervisor_takeover(install_dir, supervisor_id)?.ok_or_else(|| {
            SurgeError::Update(format!(
                "Supervisor '{supervisor_id}' accepted takeover record disappeared before it was consumed"
            ))
        })?;
        ensure_handoff_matches_instance(supervisor_id, &consumed, instance)?;
        Ok(consumed.child_identity)
    }
}

fn take_matching_accepted_handoff(
    install_dir: &Path,
    supervisor_id: &str,
    request: &SupervisorTakeoverRequest,
) -> Result<Option<ProcessIdentity>> {
    let handoff = read_accepted_supervisor_takeover(install_dir, supervisor_id)?.ok_or_else(|| {
        SurgeError::Update(format!(
            "Supervisor '{supervisor_id}' accepted takeover record disappeared before it was consumed"
        ))
    })?;
    ensure_handoff_matches_request(supervisor_id, &handoff, request)?;
    let consumed = take_accepted_supervisor_takeover(install_dir, supervisor_id)?.ok_or_else(|| {
        SurgeError::Update(format!(
            "Supervisor '{supervisor_id}' accepted takeover record disappeared before it was consumed"
        ))
    })?;
    ensure_handoff_matches_request(supervisor_id, &consumed, request)?;
    Ok(consumed.child_identity)
}
