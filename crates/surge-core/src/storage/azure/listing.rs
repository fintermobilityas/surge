use quick_xml::Reader;
use quick_xml::events::Event;
use tracing::debug;

use crate::error::{Result, SurgeError};
use crate::storage::{ListEntry, ListResult};

/// Parse an Azure List Blobs XML response into a `ListResult`.
///
/// Azure response structure:
/// ```xml
/// <EnumerationResults>
///   <Blobs>
///     <Blob>
///       <Name>key</Name>
///       <Properties>
///         <Content-Length>123</Content-Length>
///       </Properties>
///     </Blob>
///   </Blobs>
///   <NextMarker>...</NextMarker>
/// </EnumerationResults>
/// ```
pub(super) fn parse_azure_list_blobs_xml(xml: &str) -> Result<ListResult> {
    let mut reader = Reader::from_str(xml);
    let mut buf = Vec::new();
    let mut entries = Vec::new();
    let mut next_marker: Option<String> = None;

    let mut in_blob = false;
    let mut in_properties = false;
    let mut current_name: Option<String> = None;
    let mut current_size: Option<i64> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match tag.as_str() {
                    "Blob" => {
                        in_blob = true;
                        current_name = None;
                        current_size = None;
                    }
                    "Properties" if in_blob => {
                        in_properties = true;
                    }
                    _ => {}
                }
                current_tag = tag;
            }
            Ok(Event::End(ref e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match tag.as_str() {
                    "Blob" => {
                        if let Some(name) = current_name.take() {
                            entries.push(ListEntry {
                                key: name,
                                size: current_size.unwrap_or(0),
                            });
                        }
                        in_blob = false;
                        in_properties = false;
                    }
                    "Properties" => {
                        in_properties = false;
                    }
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let text = String::from_utf8_lossy(e.as_ref()).to_string();
                if in_blob && !in_properties && current_tag == "Name" {
                    current_name = Some(text);
                } else if in_properties && current_tag == "Content-Length" {
                    current_size = text.parse::<i64>().ok();
                } else if !in_blob && current_tag == "NextMarker" && !text.is_empty() {
                    next_marker = Some(text);
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(SurgeError::Storage(format!("Failed to parse Azure list response: {e}")));
            }
            _ => {}
        }
        buf.clear();
    }

    let is_truncated = next_marker.is_some();
    debug!(count = entries.len(), is_truncated, "Azure LIST parsed");
    Ok(ListResult {
        entries,
        next_marker,
        is_truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::parse_azure_list_blobs_xml;

    #[test]
    fn parse_azure_list_blobs_xml_reads_entries_and_marker() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Blobs>
    <Blob>
      <Name>prefix/app-1.0.0.zip</Name>
      <Properties>
        <Content-Length>123</Content-Length>
      </Properties>
    </Blob>
    <Blob>
      <Name>prefix/app-1.1.0.zip</Name>
      <Properties>
        <Content-Length>456</Content-Length>
      </Properties>
    </Blob>
  </Blobs>
  <NextMarker>opaque-marker</NextMarker>
</EnumerationResults>"#;

        let result = parse_azure_list_blobs_xml(xml).expect("azure list xml should parse");
        assert_eq!(result.entries.len(), 2);
        assert_eq!(result.entries[0].key, "prefix/app-1.0.0.zip");
        assert_eq!(result.entries[0].size, 123);
        assert_eq!(result.entries[1].key, "prefix/app-1.1.0.zip");
        assert_eq!(result.entries[1].size, 456);
        assert_eq!(result.next_marker.as_deref(), Some("opaque-marker"));
        assert!(result.is_truncated);
    }

    #[test]
    fn parse_azure_list_blobs_xml_handles_empty_listing() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Blobs />
  <NextMarker></NextMarker>
</EnumerationResults>"#;

        let result = parse_azure_list_blobs_xml(xml).expect("empty azure list xml should parse");
        assert!(result.entries.is_empty());
        assert_eq!(result.next_marker, None);
        assert!(!result.is_truncated);
    }
}
