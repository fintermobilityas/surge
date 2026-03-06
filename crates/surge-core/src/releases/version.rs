//! Semantic version comparison for dotted-integer version strings.

use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PrereleaseIdentifier<'a> {
    Numeric(u64),
    Text(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedVersion<'a> {
    core: Vec<u64>,
    prerelease: Option<Vec<PrereleaseIdentifier<'a>>>,
}

fn parse_core_version(version: &str) -> Vec<u64> {
    let parts: Vec<u64> = version
        .split('.')
        .filter_map(|p| p.trim().parse::<u64>().ok())
        .collect();

    let mut result = parts;
    while result.len() < 3 {
        result.push(0);
    }
    result
}

fn parse_prerelease(version: &str) -> Option<Vec<PrereleaseIdentifier<'_>>> {
    let (_, raw_prerelease) = version.split_once('-')?;
    let identifiers = raw_prerelease
        .split('.')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            segment
                .parse::<u64>()
                .map_or(PrereleaseIdentifier::Text(segment), PrereleaseIdentifier::Numeric)
        })
        .collect::<Vec<_>>();

    if identifiers.is_empty() {
        None
    } else {
        Some(identifiers)
    }
}

fn parse_version(version: &str) -> ParsedVersion<'_> {
    let without_build_metadata = version.split_once('+').map_or(version, |(base, _)| base);
    let core = without_build_metadata
        .split_once('-')
        .map_or(without_build_metadata, |(base, _)| base);

    ParsedVersion {
        core: parse_core_version(core),
        prerelease: parse_prerelease(without_build_metadata),
    }
}

fn compare_prerelease_identifiers(a: &[PrereleaseIdentifier<'_>], b: &[PrereleaseIdentifier<'_>]) -> Ordering {
    let max_len = a.len().max(b.len());

    for i in 0..max_len {
        match (a.get(i), b.get(i)) {
            (Some(PrereleaseIdentifier::Numeric(lhs)), Some(PrereleaseIdentifier::Numeric(rhs))) => {
                let ordering = lhs.cmp(rhs);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            (Some(PrereleaseIdentifier::Text(lhs)), Some(PrereleaseIdentifier::Text(rhs))) => {
                let ordering = lhs.cmp(rhs);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            (Some(PrereleaseIdentifier::Numeric(_)), Some(PrereleaseIdentifier::Text(_)))
            | (None, Some(_)) => return Ordering::Less,
            (Some(PrereleaseIdentifier::Text(_)), Some(PrereleaseIdentifier::Numeric(_))) => {
                return Ordering::Greater;
            }
            (Some(_), None) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }

    Ordering::Equal
}

/// Compare two dotted-integer version strings.
///
/// Parses "major.minor.patch" format, filling missing parts with 0.
/// Supports versions with more than 3 parts (e.g., "1.2.3.4").
///
/// # Examples
///
/// ```
/// use surge_core::releases::version::compare_versions;
/// use std::cmp::Ordering;
///
/// assert_eq!(compare_versions("1.2.3", "1.2.3"), Ordering::Equal);
/// assert_eq!(compare_versions("2.0.0", "1.9.9"), Ordering::Greater);
/// assert_eq!(compare_versions("1.0", "1.0.0"), Ordering::Equal);
/// ```
#[must_use]
pub fn compare_versions(a: &str, b: &str) -> Ordering {
    let parts_a = parse_version(a);
    let parts_b = parse_version(b);

    let max_len = parts_a.core.len().max(parts_b.core.len());

    for i in 0..max_len {
        let va = parts_a.core.get(i).copied().unwrap_or(0);
        let vb = parts_b.core.get(i).copied().unwrap_or(0);
        if va != vb {
            return va.cmp(&vb);
        }
    }

    match (&parts_a.prerelease, &parts_b.prerelease) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(lhs), Some(rhs)) => compare_prerelease_identifiers(lhs, rhs),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equal_versions() {
        assert_eq!(compare_versions("1.0.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("0.0.0", "0.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("10.20.30", "10.20.30"), Ordering::Equal);
    }

    #[test]
    fn test_greater_major() {
        assert_eq!(compare_versions("2.0.0", "1.0.0"), Ordering::Greater);
        assert_eq!(compare_versions("10.0.0", "9.0.0"), Ordering::Greater);
    }

    #[test]
    fn test_greater_minor() {
        assert_eq!(compare_versions("1.2.0", "1.1.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.10.0", "1.9.0"), Ordering::Greater);
    }

    #[test]
    fn test_greater_patch() {
        assert_eq!(compare_versions("1.0.2", "1.0.1"), Ordering::Greater);
        assert_eq!(compare_versions("1.0.10", "1.0.9"), Ordering::Greater);
    }

    #[test]
    fn test_less_than() {
        assert_eq!(compare_versions("1.0.0", "2.0.0"), Ordering::Less);
        assert_eq!(compare_versions("1.0.0", "1.1.0"), Ordering::Less);
        assert_eq!(compare_versions("1.0.0", "1.0.1"), Ordering::Less);
    }

    #[test]
    fn test_missing_parts_filled_with_zero() {
        assert_eq!(compare_versions("1", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.2", "1.2.0"), Ordering::Equal);
        assert_eq!(compare_versions("1", "1.0.1"), Ordering::Less);
    }

    #[test]
    fn test_four_part_versions() {
        assert_eq!(compare_versions("1.0.0.1", "1.0.0.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.0.0", "1.0.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.0.0.1", "1.0.0.2"), Ordering::Less);
    }

    #[test]
    fn test_complex_comparisons() {
        assert_eq!(compare_versions("2.0.0", "1.9.9"), Ordering::Greater);
        assert_eq!(compare_versions("1.0.0", "0.99.99"), Ordering::Greater);
        assert_eq!(compare_versions("0.0.1", "0.0.0"), Ordering::Greater);
    }

    #[test]
    fn test_large_version_numbers() {
        assert_eq!(compare_versions("100.200.300", "100.200.300"), Ordering::Equal);
        assert_eq!(compare_versions("100.200.301", "100.200.300"), Ordering::Greater);
    }

    #[test]
    fn test_stable_release_is_newer_than_matching_prerelease() {
        assert_eq!(
            compare_versions("2859.0.0", "2859.0.0-prerelease.56"),
            Ordering::Greater
        );
        assert_eq!(compare_versions("2859.0.0-prerelease.56", "2859.0.0"), Ordering::Less);
    }

    #[test]
    fn test_prerelease_versions_compare_by_numeric_suffix() {
        assert_eq!(
            compare_versions("2859.0.0-prerelease.56", "2859.0.0-prerelease.55"),
            Ordering::Greater
        );
        assert_eq!(
            compare_versions("2859.0.0-prerelease.54", "2859.0.0-prerelease.56"),
            Ordering::Less
        );
    }

    #[test]
    fn test_build_metadata_does_not_affect_ordering() {
        assert_eq!(compare_versions("1.2.3+build.1", "1.2.3+build.9"), Ordering::Equal);
        assert_eq!(
            compare_versions("1.2.3-beta.1+build.1", "1.2.3-beta.1"),
            Ordering::Equal
        );
    }
}
