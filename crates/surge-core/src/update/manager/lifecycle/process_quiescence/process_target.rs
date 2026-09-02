use crate::error::{Result, SurgeError};
#[cfg(target_os = "linux")]
use crate::platform::process::StableProcessHandle;
#[cfg(not(target_os = "linux"))]
use crate::platform::process::process_identity_matches;
use crate::platform::process::{ProcessIdentity, ProcessSignalOutcome};

#[derive(Debug)]
pub(super) struct ProcessTarget {
    identity: ProcessIdentity,
    #[cfg(target_os = "linux")]
    handle: StableProcessHandle,
}

pub(super) fn add_process_targets(
    targets: &mut Vec<ProcessTarget>,
    identities: impl IntoIterator<Item = ProcessIdentity>,
) -> Result<()> {
    for identity in identities {
        if targets.iter().any(|target| target.identity() == identity) {
            continue;
        }
        if let Some(target) = ProcessTarget::open(identity)? {
            targets.push(target);
        }
    }
    Ok(())
}

pub(super) fn process_targets_are_running(targets: &[ProcessTarget]) -> Result<bool> {
    for target in targets {
        if target.is_running()? {
            return Ok(true);
        }
    }
    Ok(false)
}

impl ProcessTarget {
    pub(super) fn open(identity: ProcessIdentity) -> Result<Option<Self>> {
        #[cfg(target_os = "linux")]
        {
            StableProcessHandle::open(identity)
                .map(|handle| handle.map(|handle| Self { identity, handle }))
                .map_err(|error| {
                    SurgeError::Platform(format!(
                        "Failed to open stable handle for process {} before application swap: {error}",
                        identity.pid
                    ))
                })
        }
        #[cfg(not(target_os = "linux"))]
        {
            if process_identity_matches(identity).map_err(|error| {
                SurgeError::Platform(format!(
                    "Failed to revalidate process {} before application swap: {error}",
                    identity.pid
                ))
            })? {
                Ok(Some(Self { identity }))
            } else {
                Ok(None)
            }
        }
    }

    pub(super) fn identity(&self) -> ProcessIdentity {
        self.identity
    }

    pub(super) fn is_running(&self) -> Result<bool> {
        #[cfg(target_os = "linux")]
        {
            self.handle.is_running().map_err(|error| {
                SurgeError::Platform(format!(
                    "Failed to poll stable handle for process {} before application swap: {error}",
                    self.identity.pid
                ))
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            process_identity_matches(self.identity).map_err(|error| {
                SurgeError::Platform(format!(
                    "Failed to revalidate process {} before application swap: {error}",
                    self.identity.pid
                ))
            })
        }
    }

    pub(super) fn terminate(&self) -> Result<ProcessSignalOutcome> {
        self.signal(nix::sys::signal::Signal::SIGTERM)
    }

    pub(super) fn kill(&self) -> Result<ProcessSignalOutcome> {
        self.signal(nix::sys::signal::Signal::SIGKILL)
    }

    fn signal(&self, signal: nix::sys::signal::Signal) -> Result<ProcessSignalOutcome> {
        #[cfg(target_os = "linux")]
        {
            let result = match signal {
                nix::sys::signal::Signal::SIGTERM => self.handle.terminate(),
                nix::sys::signal::Signal::SIGKILL => self.handle.kill(),
                _ => unreachable!("process target supports termination signals only"),
            };
            result.map_err(|error| {
                SurgeError::Platform(format!(
                    "Failed to signal stable process {} before application swap: {error}",
                    self.identity.pid
                ))
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            if !self.is_running()? {
                return Ok(ProcessSignalOutcome::Exited);
            }
            match signal_pid(self.identity.pid, signal) {
                Ok(()) => Ok(ProcessSignalOutcome::Delivered),
                Err(nix::errno::Errno::ESRCH) => Ok(ProcessSignalOutcome::Exited),
                Err(error) => Err(SurgeError::Platform(format!(
                    "Failed to signal process {} before application swap: {error}",
                    self.identity.pid
                ))),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn signal_pid(pid: u32, signal: nix::sys::signal::Signal) -> std::result::Result<(), nix::errno::Errno> {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let Ok(raw_pid) = i32::try_from(pid) else {
        return Ok(());
    };
    kill(Pid::from_raw(raw_pid), signal)
}
