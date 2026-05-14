use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::Utc;
use tracing::trace;

use crate::crypto::hmac_sha256::hmac_sha256;
use crate::error::{Result, SurgeError};

use super::{AZURE_API_VERSION, AzureBlobBackend};

impl AzureBlobBackend {
    /// Returns `true` when credentials are available for signing requests.
    pub(super) fn has_credentials(&self) -> bool {
        !self.key_bytes.is_empty()
    }

    /// Return an error when a write is attempted without credentials.
    pub(super) fn require_credentials(&self, operation: &str) -> Result<()> {
        if self.has_credentials() {
            Ok(())
        } else {
            Err(SurgeError::Config(format!(
                "Azure Blob {operation} requires account credentials"
            )))
        }
    }

    /// Produce an RFC 1123 date string (e.g. `Mon, 02 Mar 2026 12:34:56 GMT`).
    fn rfc1123_date(now: &chrono::DateTime<Utc>) -> String {
        now.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
    }

    /// Sign a request using SharedKey authentication and return the headers
    /// that must be attached to the request.
    ///
    /// The string-to-sign format for Blob service:
    /// ```text
    /// VERB\n
    /// Content-Encoding\n
    /// Content-Language\n
    /// Content-Length\n
    /// Content-MD5\n
    /// Content-Type\n
    /// Date\n
    /// If-Modified-Since\n
    /// If-Match\n
    /// If-None-Match\n
    /// If-Unmodified-Since\n
    /// Range\n
    /// CanonicalizedHeaders\n
    /// CanonicalizedResource
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub(super) fn sign_request(
        &self,
        method: &str,
        resource_path: &str,
        query_params: &[(String, String)],
        content_length: Option<usize>,
        content_type: &str,
        range_header: Option<&str>,
        extra_headers: &[(String, String)],
    ) -> Vec<(String, String)> {
        let now = Utc::now();
        let date = Self::rfc1123_date(&now);

        let mut ms_headers: Vec<(String, String)> = vec![
            ("x-ms-date".to_string(), date.clone()),
            ("x-ms-version".to_string(), AZURE_API_VERSION.to_string()),
        ];
        for (k, v) in extra_headers {
            if k.starts_with("x-ms-") {
                ms_headers.push((k.clone(), v.clone()));
            }
        }
        ms_headers.sort_by(|a, b| a.0.cmp(&b.0));
        ms_headers.dedup_by(|a, b| a.0 == b.0);

        let canonicalized_headers = ms_headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut canonicalized_resource = format!("/{}{}", self.account, resource_path);
        if !query_params.is_empty() {
            let mut sorted_params = query_params.to_vec();
            sorted_params.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in &sorted_params {
                canonicalized_resource.push('\n');
                canonicalized_resource.push_str(k);
                canonicalized_resource.push(':');
                canonicalized_resource.push_str(v);
            }
        }

        let content_length_str = match content_length {
            Some(n) if n > 0 => n.to_string(),
            _ => String::new(),
        };
        let range_header = range_header.unwrap_or_default();

        let string_to_sign = format!(
            "{method}\n\
             \n\
             \n\
             {content_length_str}\n\
             \n\
             {content_type}\n\
             \n\
             \n\
             \n\
             \n\
             \n\
             {range_header}\n\
             {canonicalized_headers}\n\
             {canonicalized_resource}"
        );

        trace!(string_to_sign = %string_to_sign, "Azure string to sign");

        let signature = BASE64.encode(hmac_sha256(&self.key_bytes, string_to_sign.as_bytes()));
        let authorization = format!("SharedKey {}:{signature}", self.account);

        let mut headers: Vec<(String, String)> = ms_headers;
        headers.push(("Authorization".to_string(), authorization));
        if !content_type.is_empty() {
            headers.push(("Content-Type".to_string(), content_type.to_string()));
        }

        headers
    }
}

pub(super) fn decode_account_key(account_key: &str) -> Result<Vec<u8>> {
    if account_key.is_empty() {
        Ok(Vec::new())
    } else {
        BASE64
            .decode(account_key)
            .map_err(|e| SurgeError::Config(format!("Azure account key is not valid base64: {e}")))
    }
}
