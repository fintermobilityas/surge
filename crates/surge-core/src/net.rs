//! Shared HTTP client construction for storage backends and lock clients.
//!
//! Every outbound HTTP client must carry connect and read timeouts. Update
//! checks and artifact downloads run unattended on remote nodes with flaky
//! links; a request without timeouts can pin an update attempt on a dead TCP
//! connection indefinitely, leaving the install in `in_progress` with no
//! failure ever surfaced.

use std::time::Duration;

use crate::error::{Result, SurgeError};

pub(crate) const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum idle gap between body reads before the request errors. Resets on
/// every received chunk, so slow-but-progressing transfers on constrained
/// links are unaffected; only genuinely stalled connections are cut.
pub(crate) const HTTP_READ_STALL_TIMEOUT: Duration = Duration::from_secs(90);

pub(crate) fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(format!("surge/{}", crate::config::constants::VERSION))
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .read_timeout(HTTP_READ_STALL_TIMEOUT)
        .build()
        .map_err(|e| SurgeError::Storage(format!("Failed to build HTTP client: {e}")))
}
