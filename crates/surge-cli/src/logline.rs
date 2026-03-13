use std::sync::OnceLock;
use std::time::Instant;

use chrono::{SecondsFormat, Utc};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

use crate::formatters::format_duration;
use crate::ui::UiTheme;

static COMMAND_START: OnceLock<Instant> = OnceLock::new();
static VERBOSE: OnceLock<bool> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Level {
    Info,
    Success,
    Warn,
    Error,
    Subtle,
    Plain,
    Title,
}

pub(crate) fn init_timer(started_at: Instant) {
    let _ = COMMAND_START.set(started_at);
}

pub(crate) fn init_verbose(verbose: bool) {
    let _ = VERBOSE.set(verbose);
}

fn is_verbose() -> bool {
    VERBOSE.get().copied().unwrap_or(false)
}

pub(crate) fn command_start() -> Instant {
    *COMMAND_START.get_or_init(Instant::now)
}

pub(crate) fn info(message: &str) {
    emit(Level::Info, message);
}

pub(crate) fn success(message: &str) {
    emit(Level::Success, message);
}

pub(crate) fn warn(message: &str) {
    emit(Level::Warn, message);
}

pub(crate) fn error(message: &str) {
    emit_inner(Level::Error, message, true);
}

/// Display an error and its full cause chain.
///
/// The top-level error is printed on the first line. Each chained cause
/// is printed on a subsequent line indented with "  caused by: ".
pub(crate) fn error_chain(err: &dyn std::error::Error) {
    emit_inner(Level::Error, &err.to_string(), true);
    let mut source = err.source();
    while let Some(cause) = source {
        emit_inner(Level::Error, &format!("  caused by: {cause}"), true);
        source = cause.source();
    }
}

pub(crate) fn subtle(message: &str) {
    emit(Level::Subtle, message);
}

pub(crate) fn plain(message: &str) {
    emit(Level::Plain, message);
}

pub(crate) fn title(message: &str) {
    emit(Level::Title, message);
}

pub(crate) fn emit(level: Level, message: &str) {
    emit_inner(level, message, false);
}

/// Write lines with no prefix and no styling (for clap help/error output).
pub(crate) fn emit_raw(message: &str) {
    for line in message.lines() {
        println!("{line}");
    }
}

/// Write lines to stderr with no prefix and no styling.
pub(crate) fn emit_raw_stderr(message: &str) {
    for line in message.lines() {
        eprintln!("{line}");
    }
}

fn emit_inner(level: Level, message: &str, stderr: bool) {
    let theme = UiTheme::global();
    let started_at = command_start();

    if let Some(prefix) = format_prefix(level, started_at) {
        if message.is_empty() {
            write_line(stderr, &prefix);
            return;
        }
        for line in message.lines() {
            let rendered = format!("{prefix} {line}");
            write_line(stderr, &style_message(theme, level, &rendered));
        }
    } else {
        // No prefix — emit lines as-is with styling only
        if message.is_empty() {
            write_line(stderr, "");
            return;
        }
        for line in message.lines() {
            write_line(stderr, &style_message(theme, level, line));
        }
    }
}

fn style_message(theme: UiTheme, level: Level, message: &str) -> String {
    match level {
        Level::Info => theme.info(message),
        Level::Success => theme.success(message),
        Level::Warn => theme.warning(message),
        Level::Error => theme.error(message),
        Level::Subtle => theme.subtle(message),
        Level::Plain => message.to_string(),
        Level::Title => theme.title(message),
    }
}

fn write_line(stderr: bool, line: &str) {
    if stderr {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

fn format_prefix(level: Level, started_at: Instant) -> Option<String> {
    let label = match level {
        Level::Info => " info",
        Level::Success => "   ok",
        Level::Warn => " warn",
        Level::Error => "error",
        Level::Subtle | Level::Plain | Level::Title => return None,
    };
    let elapsed = format_duration(started_at.elapsed());
    if is_verbose() {
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
        Some(format!("{timestamp} {label} {elapsed:>7}"))
    } else {
        Some(format!("{label} {elapsed:>7}"))
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CommandTimer {
    started_at: Instant,
}

impl CommandTimer {
    pub(crate) fn new() -> Self {
        Self {
            started_at: command_start(),
        }
    }
}

impl FormatTime for CommandTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let elapsed = format_duration(self.started_at.elapsed());
        if is_verbose() {
            let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
            write!(w, "{timestamp} +{elapsed}")
        } else {
            write!(w, "+{elapsed}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn prefixed_levels_produce_label_and_elapsed() {
        let started_at = Instant::now().checked_sub(Duration::from_millis(42)).unwrap();
        let prefix = format_prefix(Level::Info, started_at);
        assert!(prefix.is_some());
        let p = prefix.unwrap();
        assert!(p.contains(" info"), "expected ' info' in {p:?}");
    }

    #[test]
    fn success_level_uses_ok_label() {
        let started_at = Instant::now();
        let prefix = format_prefix(Level::Success, started_at).unwrap();
        assert!(prefix.contains("   ok"), "expected '   ok' in {prefix:?}");
    }

    #[test]
    fn unprefixed_levels_return_none() {
        let started_at = Instant::now();
        assert!(format_prefix(Level::Subtle, started_at).is_none());
        assert!(format_prefix(Level::Plain, started_at).is_none());
        assert!(format_prefix(Level::Title, started_at).is_none());
    }
}
