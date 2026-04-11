//! Strict Semantic Versioning 2.0 parsing and comparison helpers.

use std::cmp::Ordering;

use semver::Version;

use crate::error::{Result, SurgeError};

fn parse_semver(version: &str, label: &str) -> Result<Version> {
    if version.is_empty() {
        return Err(SurgeError::Config(format!("{label} cannot be empty")));
    }

    if version.trim() != version {
        return Err(SurgeError::Config(format!(
            "{label} must not contain leading or trailing whitespace: '{version}'"
        )));
    }

    Version::parse(version)
        .map_err(|error| SurgeError::Config(format!("Invalid {label} semantic version '{version}': {error}")))
}

fn parse_semver_for_compare(version: &str) -> Version {
    match parse_semver(version, "version") {
        Ok(version) => version,
        Err(error) => panic!("internal semantic-version invariant violated: {error}"),
    }
}

/// Parse and validate a strict Semantic Versioning 2.0 version string.
pub fn validate_version_string(version: &str, label: &str) -> Result<()> {
    parse_semver(version, label).map(|_| ())
}

/// Parse and canonicalize a strict Semantic Versioning 2.0 version string.
pub fn canonicalize_version(version: &str, label: &str) -> Result<String> {
    parse_semver(version, label).map(|parsed| parsed.to_string())
}

/// Return whether the provided string is a valid strict Semantic Versioning 2.0 value.
#[must_use]
pub fn is_valid_version_string(version: &str) -> bool {
    parse_semver(version, "version").is_ok()
}

/// Compare two validated semantic versions by precedence.
///
/// This ignores build metadata as required by SemVer 2.0 precedence rules.
#[must_use]
pub fn compare_versions(a: &str, b: &str) -> Ordering {
    let left = parse_semver_for_compare(a);
    let right = parse_semver_for_compare(b);
    left.cmp_precedence(&right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_versions_follows_semver_precedence_examples() {
        let ordered = [
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",
        ];

        for pair in ordered.windows(2) {
            assert_eq!(compare_versions(pair[0], pair[1]), Ordering::Less);
        }
    }

    #[test]
    fn compare_versions_ignores_build_metadata() {
        assert_eq!(compare_versions("1.2.3+build.1", "1.2.3+build.9"), Ordering::Equal);
        assert_eq!(
            compare_versions("1.2.3-beta.1+build.1", "1.2.3-beta.1"),
            Ordering::Equal
        );
    }

    #[test]
    fn compare_versions_treats_release_as_newer_than_matching_prerelease() {
        assert_eq!(
            compare_versions("2859.0.0", "2859.0.0-prerelease.56"),
            Ordering::Greater
        );
        assert_eq!(compare_versions("2859.0.0-prerelease.56", "2859.0.0"), Ordering::Less);
    }

    #[test]
    fn validate_version_string_accepts_strict_semver_values() {
        for version in [
            "0.0.0",
            "1.2.3",
            "10.20.30",
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-0A.is.legal",
            "1.0.0+build.1",
            "1.0.0-alpha+build.1",
        ] {
            validate_version_string(version, "version").unwrap();
        }
    }

    #[test]
    fn validate_version_string_rejects_non_compliant_inputs() {
        for version in [
            "",
            "1",
            "1.2",
            "1.2.3.4",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-01",
            "1.2.3-alpha..1",
            "1.2.3+meta+meta",
            " 1.2.3",
            "1.2.3 ",
        ] {
            assert!(validate_version_string(version, "version").is_err(), "{version}");
        }
    }

    #[test]
    fn canonicalize_version_preserves_valid_semver_shape() {
        assert_eq!(
            canonicalize_version("1.2.3-rc.1+build.5", "version").unwrap(),
            "1.2.3-rc.1+build.5"
        );
    }

    #[test]
    fn is_valid_version_string_reports_strict_semver_status() {
        assert!(is_valid_version_string("1.2.3"));
        assert!(!is_valid_version_string("1.2"));
    }
}
