//! Environment that belongs to one login of the graphical session.

use std::collections::BTreeMap;

/// Variables whose value the graphical session creates anew on every login.
///
/// `XAUTHORITY` names the credential file of the session's X server. The display
/// manager or the compositor creates that file per login: mutter gives it a
/// random suffix each time, and the display manager's own path disappears when
/// the session type changes. A process started outside the session, such as an
/// install over SSH, has to be handed the live value. A copy written to durable
/// state is stale by the next login, and because it is applied explicitly it
/// shadows the live value that the session would otherwise have supplied.
const SESSION_SCOPED_KEYS: &[&str] = &["XAUTHORITY"];

/// Whether `key` holds a value that is only valid for the current login.
#[must_use]
pub fn is_session_scoped(key: &str) -> bool {
    SESSION_SCOPED_KEYS.contains(&key)
}

/// The part of `environment` that may be stored and applied to a later start.
#[must_use]
pub fn durable_environment(environment: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    environment
        .iter()
        .filter(|(key, _)| !is_session_scoped(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_environment_drops_only_session_scoped_values() {
        let environment = BTreeMap::from([
            ("DEMO_MODE".to_string(), "kiosk".to_string()),
            ("DISPLAY".to_string(), ":0".to_string()),
            ("XAUTHORITY".to_string(), "/run/user/1000/session-auth".to_string()),
        ]);

        let durable = durable_environment(&environment);

        assert_eq!(
            durable,
            BTreeMap::from([
                ("DEMO_MODE".to_string(), "kiosk".to_string()),
                ("DISPLAY".to_string(), ":0".to_string()),
            ])
        );
    }

    #[test]
    fn session_scoped_keys_match_exactly() {
        assert!(is_session_scoped("XAUTHORITY"));
        assert!(!is_session_scoped("xauthority"));
        assert!(!is_session_scoped("XAUTHORITY_BACKUP"));
        assert!(!is_session_scoped("DISPLAY"));
    }
}
