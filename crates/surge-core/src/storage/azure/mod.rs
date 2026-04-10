//! Azure Blob Storage backend with SharedKey authentication.

mod auth;
mod listing;
mod requests;

use reqwest::Client;

/// Azure Blob Storage REST API version.
pub(super) const AZURE_API_VERSION: &str = "2024-08-04";

/// Azure Blob Storage backend using SharedKey authentication.
pub struct AzureBlobBackend {
    pub(super) client: Client,
    pub(super) account: String,
    /// Base64-decoded account key (raw bytes) for HMAC signing.
    pub(super) key_bytes: Vec<u8>,
    pub(super) container: String,
    pub(super) endpoint: String,
    pub(super) prefix: String,
}
