use quick_xml::Reader;
use quick_xml::events::Event;
use tracing::debug;

use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult};

/// Parse a GCS XML API ListBucketResult into a `ListResult`.
/// The format matches S3's `ListBucketResult` (v1).
pub(super) fn parse_gcs_xml_list_response(xml: &str) -> Result<ListResult> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    let mut next_marker: Option<String> = None;
    let mut is_truncated = false;

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
                        "NextMarker" => next_marker = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(SurgeError::Storage(format!(
                    "Failed to parse GCS XML list response: {e}"
                )));
            }
            _ => {}
        }
        buf.clear();
    }

    debug!(count = entries.len(), is_truncated, "GCS XML LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_gcs_xml_list_response;

    #[test]
    fn parse_gcs_xml_list_response_reads_entries_and_marker() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>prefix/app-1.0.0.zip</Key>
    <Size>123</Size>
  </Contents>
  <Contents>
    <Key>prefix/app-1.1.0.zip</Key>
    <Size>456</Size>
  </Contents>
  <IsTruncated>true</IsTruncated>
  <NextMarker>next-marker</NextMarker>
</ListBucketResult>"#;

        let result = parse_gcs_xml_list_response(xml).expect("gcs xml list response should parse");
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].key, "prefix/app-1.0.0.zip");
        assert_eq!(result.entries[0].size, 123);
        assert_eq!(result.entries[1].key, "prefix/app-1.1.0.zip");
        assert_eq!(result.entries[1].size, 456);
        assert_eq!(result.next_marker.as_deref(), Some("next-marker"));
        assert!(result.is_truncated);
    }
}
