use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

pub(super) struct Identity {
    pub(super) interpreter: PathBuf,
    pub(super) script_argument_index: usize,
}

pub(super) fn resolve(active_exe: &Path) -> Result<Option<Identity>> {
    let mut file = std::fs::File::open(active_exe).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application executable before swap: {e}"
        ))
    })?;
    let mut prefix = Vec::new();
    file.by_ref().take(4097).read_to_end(&mut prefix).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application executable before swap: {e}"
        ))
    })?;
    if !prefix.starts_with(b"#!") {
        return Ok(None);
    }

    let line_end = prefix.iter().position(|byte| *byte == b'\n').unwrap_or(prefix.len());
    if line_end > 4096 {
        return Err(SurgeError::Platform(
            "Active application shebang exceeds the supported 4096-byte limit".to_string(),
        ));
    }
    let shebang = std::str::from_utf8(&prefix[2..line_end])
        .map_err(|e| SurgeError::Platform(format!("Active application shebang is not valid UTF-8: {e}")))?
        .trim();
    let interpreter_end = shebang.find(char::is_whitespace).unwrap_or(shebang.len());
    let interpreter = &shebang[..interpreter_end];
    let argument = shebang[interpreter_end..].trim();
    if interpreter.is_empty() {
        return Err(SurgeError::Platform(
            "Active application shebang has no interpreter".to_string(),
        ));
    }

    let (interpreter, fixed_argument_count) =
        if Path::new(interpreter).file_name().and_then(|name| name.to_str()) == Some("env") {
            let command = parse_env_command(argument)?;
            let Some((program, fixed_arguments)) = command.split_first() else {
                return Err(SurgeError::Platform(
                    "Active application env shebang has no interpreter command".to_string(),
                ));
            };
            (resolve_interpreter_path(program)?, fixed_arguments.len())
        } else {
            (
                resolve_interpreter_path(interpreter)?,
                usize::from(!argument.is_empty()),
            )
        };

    Ok(Some(Identity {
        interpreter,
        script_argument_index: 1 + fixed_argument_count,
    }))
}

fn parse_env_command(argument: &str) -> Result<Vec<String>> {
    let split_command = argument
        .strip_prefix("--split-string=")
        .or_else(|| argument.strip_prefix("-S"))
        .or_else(|| argument.strip_prefix("--split-string "))
        .map(str::trim_start);
    let command = split_command.unwrap_or(argument);
    let words = split_command_words(command)?;
    if split_command.is_none() && words.len() > 1 {
        return Err(SurgeError::Platform(
            "Active application env shebang with multiple arguments must use -S".to_string(),
        ));
    }
    Ok(words)
}

fn split_command_words(command: &str) -> Result<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;

    for character in command.chars() {
        if escaped {
            word.push(character);
            escaped = false;
            started = true;
        } else if character == '\\' && quote != Some('\'') {
            escaped = true;
            started = true;
        } else if matches!(character, '\'' | '"') && quote == Some(character) {
            quote = None;
        } else if matches!(character, '\'' | '"') && quote.is_none() {
            quote = Some(character);
            started = true;
        } else if character.is_whitespace() && quote.is_none() {
            if started {
                words.push(std::mem::take(&mut word));
                started = false;
            }
        } else {
            word.push(character);
            started = true;
        }
    }

    if escaped || quote.is_some() {
        return Err(SurgeError::Platform(
            "Active application env shebang contains an unterminated escape or quote".to_string(),
        ));
    }
    if started {
        words.push(word);
    }
    Ok(words)
}

fn resolve_interpreter_path(interpreter: &str) -> Result<PathBuf> {
    let interpreter = Path::new(interpreter);
    if interpreter.is_absolute() || interpreter.components().count() > 1 {
        return std::fs::canonicalize(interpreter).map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to resolve active application interpreter '{}': {e}",
                interpreter.display()
            ))
        });
    }

    let path = std::env::var_os("PATH").ok_or_else(|| {
        SurgeError::Platform(format!(
            "Failed to resolve active application interpreter '{}': PATH is unavailable",
            interpreter.display()
        ))
    })?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(interpreter))
        .find_map(|candidate| {
            let metadata = candidate.metadata().ok()?;
            (metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
                .then(|| std::fs::canonicalize(candidate).ok())
                .flatten()
        })
        .ok_or_else(|| {
            SurgeError::Platform(format!(
                "Failed to resolve active application interpreter '{}' from PATH",
                interpreter.display()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_command_words_preserves_quoted_and_escaped_arguments() {
        assert_eq!(
            split_command_words("/bin/sh '-e value' plain\\ value").unwrap(),
            vec!["/bin/sh", "-e value", "plain value"]
        );
    }
}
