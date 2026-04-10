//! Google Cloud Storage backend with HMAC (S3-interop XML API) and OAuth2 bearer token auth.

mod auth;
mod json_listing;
mod requests;
mod xml_listing;

use std::sync::Arc;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};
use reqwest::Client;
use tokio::sync::RwLock;

/// Characters that must NOT be percent-encoded in URI paths.
pub(super) const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// Same set but preserves '/'.
pub(super) const URI_ENCODE_PATH_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// GCS XML API endpoint (S3-interop).
pub(super) const GCS_XML_ENDPOINT: &str = "https://storage.googleapis.com";

/// GCS JSON API endpoint (used for OAuth2 bearer-token auth).
pub(super) const GCS_JSON_ENDPOINT: &str = "https://storage.googleapis.com/storage/v1";

/// GCS JSON API upload endpoint.
pub(super) const GCS_UPLOAD_ENDPOINT: &str = "https://storage.googleapis.com/upload/storage/v1";

/// Authentication mode for GCS.
#[derive(Debug, Clone)]
pub(super) enum AuthMode {
    /// No credentials — only anonymous (public) reads are possible.
    Anonymous,
    /// HMAC-based authentication using the S3-interop XML API.
    Hmac { access_key: String, secret_key: String },
    /// OAuth2 bearer token authentication using the JSON API.
    OAuth2 { token: Arc<RwLock<String>> },
}

/// Google Cloud Storage backend.
pub struct GcsBackend {
    pub(super) client: Client,
    pub(super) bucket: String,
    pub(super) prefix: String,
    pub(super) endpoint: String,
    pub(super) auth: AuthMode,
}
