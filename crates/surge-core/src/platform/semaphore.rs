use crate::error::{Result, SurgeError};

/// A named semaphore for cross-process synchronization.
pub struct NamedSemaphore {
    #[cfg(unix)]
    name: String,
    #[cfg(unix)]
    fd: std::os::fd::OwnedFd,
}

#[cfg(unix)]
impl NamedSemaphore {
    /// Create or open a named semaphore.
    pub fn open(name: &str) -> Result<Self> {
        use nix::fcntl::{OFlag, open};
        use nix::sys::stat::Mode;

        let sem_path = format!("/tmp/.surge_sem_{name}");
        let fd = open(
            sem_path.as_str(),
            OFlag::O_CREAT | OFlag::O_RDWR,
            Mode::from_bits_truncate(0o666),
        )
        .map_err(|e| SurgeError::Platform(format!("Failed to open semaphore: {e}")))?;

        Ok(Self {
            name: name.to_string(),
            fd,
        })
    }

    /// Try to acquire the semaphore (non-blocking file lock).
    #[allow(deprecated)]
    pub fn try_acquire(&self) -> Result<bool> {
        use nix::fcntl::{FlockArg, flock};
        use std::os::fd::AsRawFd;
        match flock(self.fd.as_raw_fd(), FlockArg::LockExclusiveNonblock) {
            Ok(()) => Ok(true),
            Err(nix::errno::Errno::EWOULDBLOCK) => Ok(false),
            Err(e) => Err(SurgeError::Platform(format!("Semaphore lock failed: {e}"))),
        }
    }

    /// Release the semaphore.
    #[allow(deprecated)]
    pub fn release(&self) -> Result<()> {
        use nix::fcntl::{FlockArg, flock};
        use std::os::fd::AsRawFd;
        flock(self.fd.as_raw_fd(), FlockArg::Unlock)
            .map_err(|e| SurgeError::Platform(format!("Semaphore unlock failed: {e}")))?;
        Ok(())
    }

    /// Get the semaphore name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(not(unix))]
impl NamedSemaphore {
    pub fn open(_name: &str) -> Result<Self> {
        Err(SurgeError::Platform(
            "Named semaphores not yet implemented for this platform".to_string(),
        ))
    }

    pub fn try_acquire(&self) -> Result<bool> {
        Ok(false)
    }

    pub fn release(&self) -> Result<()> {
        Ok(())
    }
}
