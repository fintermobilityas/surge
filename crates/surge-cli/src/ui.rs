use std::io::IsTerminal;

#[derive(Debug, Clone, Copy)]
pub(crate) struct UiTheme {
    enabled: bool,
}

impl UiTheme {
    pub(crate) fn global() -> Self {
        Self {
            enabled: should_colorize_output(),
        }
    }

    pub(crate) fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) fn title(self, text: &str) -> String {
        self.bold(&self.cyan(text))
    }

    pub(crate) fn info(self, text: &str) -> String {
        self.cyan(text)
    }

    pub(crate) fn success(self, text: &str) -> String {
        self.green(text)
    }

    pub(crate) fn warning(self, text: &str) -> String {
        self.yellow(text)
    }

    pub(crate) fn error(self, text: &str) -> String {
        self.red(text)
    }

    pub(crate) fn subtle(self, text: &str) -> String {
        self.dim(text)
    }

    pub(crate) fn bold(self, text: &str) -> String {
        self.apply(text, "1")
    }

    pub(crate) fn dim(self, text: &str) -> String {
        self.apply(text, "2")
    }

    pub(crate) fn cyan(self, text: &str) -> String {
        self.apply(text, "36")
    }

    pub(crate) fn blue(self, text: &str) -> String {
        self.apply(text, "34")
    }

    pub(crate) fn green(self, text: &str) -> String {
        self.apply(text, "32")
    }

    pub(crate) fn red(self, text: &str) -> String {
        self.apply(text, "31")
    }

    pub(crate) fn yellow(self, text: &str) -> String {
        self.apply(text, "33")
    }

    pub(crate) fn magenta(self, text: &str) -> String {
        self.apply(text, "35")
    }

    fn apply(self, text: &str, code: &str) -> String {
        if self.enabled {
            format!("\u{1b}[{code}m{text}\u{1b}[0m")
        } else {
            text.to_string()
        }
    }
}

fn should_colorize_output() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }

    if let Ok(force) = std::env::var("CLICOLOR_FORCE")
        && force != "0"
    {
        return true;
    }

    if !std::io::stdout().is_terminal() {
        return false;
    }

    !matches!(std::env::var("CLICOLOR").ok().as_deref(), Some("0"))
}
