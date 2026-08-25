//! S3 wire helpers: URI encoding, response-status mapping, and
//! ListObjectsV2 XML parsing, split out of the backend impl to keep it
//! under the maintainability line target.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use tracing::debug;

use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult};

/// Characters that must NOT be percent-encoded in S3 URI paths (RFC 3986 unreserved + '/').
pub(super) const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'.').remove(b'_').remove(b'~');

/// Same set but also preserves '/' (used for path components).
pub(super) const URI_ENCODE_PATH_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// URI-encode a single path segment (does not encode '/').
pub(super) fn encode_uri_path(path: &str) -> String {
    utf8_percent_encode(path, URI_ENCODE_PATH_SET).to_string()
}

/// URI-encode a query parameter value.
pub(super) fn encode_uri_component(value: &str) -> String {
    utf8_percent_encode(value, URI_ENCODE_SET).to_string()
}

/// Map an HTTP response status to a `SurgeError` when appropriate.
pub(super) fn check_response_status(status: reqwest::StatusCode, key: &str, body: &str) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(SurgeError::NotFound(format!("S3 object not found: {key}")));
    }
    Err(SurgeError::Storage(format!(
        "S3 request failed (HTTP {status}): {body}"
    )))
}

/// Parse a ListObjectsV2 XML response into a `ListResult`.
pub(super) fn parse_list_objects_v2_xml(xml: &str) -> Result<ListResult> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    let mut next_marker: Option<String> = None;
    let mut is_truncated = false;

    // State for parsing <Contents> elements.
    let mut in_contents = false;
    let mut current_key: Option<String> = None;
    let mut current_size: Option<i64> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag == "Contents" {
                    in_contents = true;
                    current_key = None;
                    current_size = None;
                }
                current_tag = tag;
            }
            Ok(Event::End(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag == "Contents" {
                    if let Some(key) = current_key.take() {
                        entries.push(ListEntry {
                            key,
                            size: current_size.unwrap_or(0),
                        });
                    }
                    in_contents = false;
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).to_string();
                if in_contents {
                    match current_tag.as_str() {
                        "Key" => current_key = Some(text),
                        "Size" => current_size = text.parse::<i64>().ok(),
                        _ => {}
                    }
                } else {
                    match current_tag.as_str() {
                        "IsTruncated" => is_truncated = text == "true",
                        "NextContinuationToken" => next_marker = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(SurgeError::Storage(format!("Failed to parse S3 list response: {e}")));
            }
            _ => {}
        }
        buf.clear();
    }

    debug!(count = entries.len(), is_truncated, "S3 LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}
