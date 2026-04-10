use tracing::debug;

use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult};

/// Parse a GCS JSON API list-objects response into a `ListResult`.
///
/// Response format:
/// ```json
/// {
///   "items": [{"name": "key", "size": "123"}, ...],
///   "nextPageToken": "..."
/// }
/// ```
pub(super) fn parse_gcs_json_list_response(json_str: &str) -> Result<ListResult> {
    let json: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| SurgeError::Storage(format!("Failed to parse GCS JSON list response: {e}")))?;

    let mut entries = Vec::new();
    if let Some(items) = json.get("items").and_then(|v| v.as_array()) {
        for item in items {
            let key = item.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let size = item
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            if !key.is_empty() {
                entries.push(ListEntry { key, size });
            }
        }
    }

    let next_marker = json.get("nextPageToken").and_then(|v| v.as_str()).map(String::from);
    let is_truncated = next_marker.is_some();

    debug!(count = entries.len(), is_truncated, "GCS JSON LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_gcs_json_list_response;

    #[test]
    fn parse_gcs_json_list_response_reads_entries_and_next_page_token() {
        let json = r#"{
  "items": [
    {"name": "prefix/app-1.0.0.zip", "size": "123"},
    {"name": "prefix/app-1.1.0.zip", "size": "456"}
  ],
  "nextPageToken": "next-token"
}"#;

        let result = parse_gcs_json_list_response(json).expect("gcs json list response should parse");
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].key, "prefix/app-1.0.0.zip");
        assert_eq!(result.entries[0].size, 123);
        assert_eq!(result.entries[1].key, "prefix/app-1.1.0.zip");
        assert_eq!(result.entries[1].size, 456);
        assert_eq!(result.next_marker.as_deref(), Some("next-token"));
        assert!(result.is_truncated);
    }
}
