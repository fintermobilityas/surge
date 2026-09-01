use crate::child::is_process_running;
#[cfg(unix)]
use crate::takeover::ChildObservation;

#[cfg(unix)]
use std::process::Child;
#[cfg(unix)]
use surge_core::platform::process::{ProcessIdentity, process_identity, process_identity_matches};

#[derive(Debug, Clone, Copy)]
pub(crate) enum WatchedProcess {
    #[cfg(unix)]
    Verified(ProcessIdentity),
    Unverified(u32),
    Exited(u32),
}

impl WatchedProcess {
    pub(crate) fn new(pid: u32, generation: Option<u64>) -> Self {
        #[cfg(unix)]
        {
            if let Some(generation) = generation {
                return Self::Verified(ProcessIdentity { pid, generation });
            }
            match process_identity(pid) {
                Ok(Some(identity)) => Self::Verified(identity),
                Ok(None) => Self::Exited(pid),
                Err(error) => {
                    tracing::warn!(pid, %error, "Cannot capture watched process generation; using liveness-only fallback");
                    Self::Unverified(pid)
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = generation;
            Self::Unverified(pid)
        }
    }

    pub(crate) fn pid(self) -> u32 {
        match self {
            #[cfg(unix)]
            Self::Verified(identity) => identity.pid,
            Self::Unverified(pid) | Self::Exited(pid) => pid,
        }
    }

    pub(crate) fn is_running(self) -> bool {
        #[cfg(unix)]
        {
            !matches!(self.observation(), ChildObservation::Absent)
        }
        #[cfg(not(unix))]
        {
            match self {
                Self::Unverified(pid) => is_process_running(pid),
                Self::Exited(_) => false,
            }
        }
    }

    #[cfg(unix)]
    pub(crate) fn observation(self) -> ChildObservation {
        match self {
            Self::Verified(identity) => match process_identity_matches(identity) {
                Ok(true) => ChildObservation::Verified(identity),
                Ok(false) => ChildObservation::Absent,
                Err(error) => {
                    tracing::warn!(pid = identity.pid, %error, "Cannot revalidate watched process generation");
                    ChildObservation::Unverified(identity.pid)
                }
            },
            Self::Unverified(pid) if is_process_running(pid) => ChildObservation::Unverified(pid),
            Self::Unverified(_) | Self::Exited(_) => ChildObservation::Absent,
        }
    }
}

#[cfg(unix)]
pub(crate) fn capture_process_identity(pid: u32) -> Option<ProcessIdentity> {
    match process_identity(pid) {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(pid, %error, "Cannot capture supervised child process generation");
            None
        }
    }
}

#[cfg(unix)]
pub(crate) fn observe_spawned_child(
    child: &mut Child,
    child_pid: u32,
    child_identity: Option<ProcessIdentity>,
) -> ChildObservation {
    match child.try_wait() {
        Ok(Some(_)) => ChildObservation::Absent,
        Ok(None) => child_identity.map_or(ChildObservation::Unverified(child_pid), ChildObservation::Verified),
        Err(error) => {
            tracing::warn!(pid = child_pid, %error, "Cannot refresh supervised child state");
            ChildObservation::Unverified(child_pid)
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn explicit_generation_does_not_follow_a_reused_pid() {
        let current = process_identity(std::process::id()).unwrap().unwrap();
        let watched = WatchedProcess::new(current.pid, Some(current.generation.wrapping_add(1)));

        assert_eq!(watched.observation(), ChildObservation::Absent);
        assert!(!watched.is_running());
    }

    #[test]
    fn watch_start_captures_the_current_process_generation() {
        let current = process_identity(std::process::id()).unwrap().unwrap();
        let watched = WatchedProcess::new(current.pid, None);

        assert_eq!(watched.observation(), ChildObservation::Verified(current));
    }
}
