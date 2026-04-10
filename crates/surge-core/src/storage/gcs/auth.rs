use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tracing::trace;

use crate::context::StorageConfig;
use crate::crypto::hmac_sha256::hmac_sha256;
use crate::crypto::sha256::sha256_hex;
use crate::error::{Result, SurgeError};

use super::{AuthMode, GcsBackend};

pub(super) fn auth_mode_from_config(config: &StorageConfig) -> AuthMode {
    if !config.access_key.is_empty() && !config.secret_key.is_empty() {
        AuthMode::Hmac {
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
        }
    } else {
        let token = if config.secret_key.is_empty() {
            std::env::var("GOOGLE_ACCESS_TOKEN").unwrap_or_default()
        } else {
            config.secret_key.clone()
        };
        if token.is_empty() {
            AuthMode::Anonymous
        } else {
            AuthMode::OAuth2 {
                token: Arc::new(RwLock::new(token)),
            }
        }
    }
}

impl GcsBackend {
    /// Update the OAuth2 bearer token (for token refresh).
    pub async fn set_oauth2_token(&self, new_token: &str) -> Result<()> {
        match &self.auth {
            AuthMode::OAuth2 { token } => {
                let mut t = token.write().await;
                *t = new_token.to_string();
                Ok(())
            }
            AuthMode::Anonymous | AuthMode::Hmac { .. } => Err(SurgeError::Config(
                "Cannot set OAuth2 token on non-OAuth2 backend".to_string(),
            )),
        }
    }

    /// Return an error when a write is attempted without credentials.
    pub(super) fn require_credentials(&self, operation: &str) -> Result<()> {
        match &self.auth {
            AuthMode::Anonymous => Err(SurgeError::Config(format!("GCS {operation} requires credentials"))),
            _ => Ok(()),
        }
    }

    /// Derive HMAC signing key, similar to AWS Signature V4 but for GCS.
    /// GCS HMAC keys use the same signing algorithm as AWS SigV4.
    fn hmac_signing_key(secret_key: &str, date_stamp: &str, region: &str) -> Vec<u8> {
        let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date_stamp.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }

    /// Sign a request using HMAC (SigV4-compatible) for the GCS XML API.
    pub(super) fn hmac_sign_request(
        &self,
        method: &str,
        canonical_uri: &str,
        canonical_querystring: &str,
        payload_hash: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Vec<(String, String)> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();
        let host = self.xml_host();
        let region = "auto";

        let canonical_headers = format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );

        trace!(canonical_request = %canonical_request, "GCS HMAC canonical request");

        let credential_scope = format!("{date_stamp}/{region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        let signing_key = Self::hmac_signing_key(secret_key, &date_stamp, region);
        let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
        );

        vec![
            ("Host".to_string(), host),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date),
            ("Authorization".to_string(), authorization),
        ]
    }
}
