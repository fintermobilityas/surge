use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

pub(super) struct Identity {
    interpreter: Interpreter,
    pub(super) script_argument_index: usize,
}

enum Interpreter {
    Resolved(PathBuf),
    EnvCommand(OsString),
}

struct EnvCommand {
    program: OsString,
    fixed_argument_count: usize,
}

impl Identity {
    pub(super) fn matches_interpreter(&self, executable: &Path, argv0: Option<&OsStr>) -> bool {
        match &self.interpreter {
            Interpreter::Resolved(expected) => {
                executable == expected
                    || argv0.is_some_and(|argument| {
                        let argument = Path::new(argument);
                        argument.is_absolute()
                            && std::fs::canonicalize(argument).is_ok_and(|resolved| resolved == *expected)
                    })
            }
            Interpreter::EnvCommand(expected) => argv0 == Some(expected.as_os_str()),
        }
    }
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
            (Interpreter::EnvCommand(command.program), command.fixed_argument_count)
        } else {
            (
                Interpreter::Resolved(resolve_direct_interpreter_path(interpreter)?),
                direct_interpreter_argument_count(argument)?,
            )
        };

    Ok(Some(Identity {
        interpreter,
        script_argument_index: 1 + fixed_argument_count,
    }))
}

fn parse_env_command(argument: &str) -> Result<EnvCommand> {
    let split_command = argument
        .strip_prefix("--split-string=")
        .or_else(|| argument.strip_prefix("-S"))
        .or_else(|| argument.strip_prefix("--split-string "))
        .map(str::trim_start);
    let command = split_command.unwrap_or(argument);
    let words = split_env_command_words(command)?;
    #[cfg(not(target_os = "macos"))]
    if split_command.is_none() && words.len() > 1 {
        return Err(SurgeError::Platform(
            "Active application env shebang with multiple arguments must use -S".to_string(),
        ));
    }

    let command_index = env_command_index(&words)?;
    Ok(EnvCommand {
        program: OsString::from(&words[command_index]),
        fixed_argument_count: words.len() - command_index - 1,
    })
}

fn split_env_command_words(command: &str) -> Result<Vec<String>> {
    if command
        .chars()
        .any(|character| matches!(character, '\\' | '\'' | '"' | '$' | '#'))
    {
        return Err(SurgeError::Platform(
            "Active application env shebang uses unsupported split-string quoting, escaping, expansion, or comments"
                .to_string(),
        ));
    }

    Ok(command.split_whitespace().map(ToString::to_string).collect())
}

fn env_command_index(words: &[String]) -> Result<usize> {
    let mut index = 0;
    let mut options = true;

    while let Some(word) = words.get(index) {
        if options {
            match word.as_str() {
                "--" => {
                    options = false;
                    index += 1;
                    continue;
                }
                "-i" | "--ignore-environment" | "-0" | "--null" | "-v" | "--debug" | "--list-signal-handling" => {
                    index += 1;
                    continue;
                }
                "-u" | "--unset" | "-C" | "--chdir" | "-P" => {
                    if words.get(index + 1).is_none() {
                        return Err(SurgeError::Platform(format!(
                            "Active application env shebang option '{word}' has no value"
                        )));
                    }
                    index += 2;
                    continue;
                }
                _ if word.starts_with("--unset=") || word.starts_with("--chdir=") => {
                    index += 1;
                    continue;
                }
                _ if (word.starts_with("-u") || word.starts_with("-C") || word.starts_with("-P")) && word.len() > 2 => {
                    index += 1;
                    continue;
                }
                _ if word.starts_with('-') => {
                    return Err(SurgeError::Platform(format!(
                        "Active application env shebang uses unsupported option '{word}'"
                    )));
                }
                _ => {}
            }
        }

        if is_env_assignment(word) {
            options = false;
            index += 1;
            continue;
        }

        return Ok(index);
    }

    Err(SurgeError::Platform(
        "Active application env shebang has no interpreter command".to_string(),
    ))
}

fn is_env_assignment(word: &str) -> bool {
    let Some((name, _)) = word.split_once('=') else {
        return false;
    };
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(any(target_os = "macos", test))]
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

fn resolve_direct_interpreter_path(interpreter: &str) -> Result<PathBuf> {
    let interpreter = Path::new(interpreter);
    if !interpreter.is_absolute() {
        return Err(SurgeError::Platform(format!(
            "Active application interpreter '{}' must be an absolute path",
            interpreter.display()
        )));
    }
    let resolved = std::fs::canonicalize(interpreter).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to resolve active application interpreter '{}': {e}",
            interpreter.display()
        ))
    })?;
    let mut file = std::fs::File::open(&resolved).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application interpreter '{}': {e}",
            resolved.display()
        ))
    })?;
    let mut prefix = [0_u8; 2];
    if file.read(&mut prefix).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application interpreter '{}': {e}",
            resolved.display()
        ))
    })? == prefix.len()
        && prefix == *b"#!"
    {
        return Err(SurgeError::Platform(format!(
            "Active application interpreter '{}' is itself a shebang script; nested interpreters are unsupported",
            resolved.display()
        )));
    }

    Ok(resolved)
}

fn direct_interpreter_argument_count(argument: &str) -> Result<usize> {
    #[cfg(target_os = "macos")]
    {
        return Ok(split_command_words(argument)?.len());
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(usize::from(!argument.is_empty()))
    }
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

    #[test]
    fn env_command_skips_options_assignments_and_separator() {
        let command = parse_env_command("-S -i -u OLD -- FEATURE=true /bin/sh -e").unwrap();
        assert_eq!(command.program, OsString::from("/bin/sh"));
        assert_eq!(command.fixed_argument_count, 1);
    }

    #[test]
    fn env_command_rejects_unknown_options_instead_of_treating_them_as_the_interpreter() {
        let error = parse_env_command("-S --argv0 app /bin/sh").err().unwrap();
        assert!(error.to_string().contains("unsupported option '--argv0'"));
    }

    #[test]
    fn env_command_rejects_escape_sequences_instead_of_misparsing_operand_boundaries() {
        let error = parse_env_command(r"-S /bin/sh\_-e").err().unwrap();
        assert!(error.to_string().contains("unsupported split-string"));
    }

    #[test]
    fn direct_interpreter_must_be_absolute() {
        let error = resolve_direct_interpreter_path("./interpreter").unwrap_err();
        assert!(error.to_string().contains("must be an absolute path"));
    }

    #[test]
    fn nested_shebang_interpreter_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("interpreter");
        std::fs::write(&interpreter, "#!/bin/sh\n").unwrap();

        let error = resolve_direct_interpreter_path(interpreter.to_str().unwrap()).unwrap_err();

        assert!(error.to_string().contains("nested interpreters are unsupported"));
    }
}
