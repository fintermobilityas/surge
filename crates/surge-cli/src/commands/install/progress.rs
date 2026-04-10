use std::io::IsTerminal;

use indicatif::{ProgressBar, ProgressStyle};

pub(crate) fn make_progress_bar(message: &str, total: u64) -> Option<ProgressBar> {
    if !std::io::stdout().is_terminal() {
        return None;
    }

    let bar = ProgressBar::new(total);
    let style = ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("=> ");
    bar.set_style(style);
    bar.set_message(message.to_string());
    Some(bar)
}

pub(crate) fn make_spinner(message: &str) -> Option<ProgressBar> {
    if !std::io::stdout().is_terminal() {
        return None;
    }

    let spinner = ProgressBar::new_spinner();
    let style = ProgressStyle::with_template("{spinner} {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_spinner())
        .tick_chars("|/-\\ ");
    spinner.set_style(style);
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    Some(spinner)
}

pub(crate) fn shell_single_quote(raw: &str) -> String {
    let mut escaped = String::from("'");
    for ch in raw.chars() {
        if ch == '\'' {
            escaped.push_str("'\"'\"'");
        } else {
            escaped.push(ch);
        }
    }
    escaped.push('\'');
    escaped
}
