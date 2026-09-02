use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::error::{Result, SurgeError};

#[cfg(target_os = "macos")]
const SHEBANG_IDENTITY_LIMIT: usize = 512;
#[cfg(target_os = "macos")]
const SHEBANG_READ_LIMIT: u64 = 513;
#[cfg(not(target_os = "macos"))]
const SHEBANG_IDENTITY_LIMIT: usize = 256;
#[cfg(not(target_os = "macos"))]
const SHEBANG_READ_LIMIT: u64 = 257;
const SUPPORTED_ENV_INTERPRETERS: [&str; 2] = ["/usr/bin/env", "/bin/env"];
const DEFAULT_ENV_SEARCH_PATH: &str = "/bin:/usr/bin";

pub(super) struct Identity {
    interpreter: Interpreter,
    pub(super) script_argument_index: usize,
}

enum Interpreter {
    Resolved(PathBuf),
    EnvCommand(EnvCommand),
}

struct EnvCommand {
    program: OsString,
    fixed_argument_count: usize,
    search_path: EnvSearchPath,
}

#[derive(Debug, PartialEq, Eq)]
enum EnvSearchPath {
    Inherited,
    Default,
    Explicit(OsString),
}

impl Identity {
    pub(super) fn requires_environment(&self) -> bool {
        matches!(
            &self.interpreter,
            Interpreter::EnvCommand(command) if command.requires_environment()
        )
    }

    pub(super) fn matches_interpreter_in_environment(
        &self,
        executable: &Path,
        command: &[OsString],
        environment: &[OsString],
    ) -> Result<bool> {
        match &self.interpreter {
            Interpreter::Resolved(expected) => Ok(paths_resolve_to_same_executable(executable, expected)),
            Interpreter::EnvCommand(expected) => {
                let Some(argv0) = command.first() else {
                    #[cfg(target_os = "macos")]
                    if expected.matches_executable(executable, environment)? {
                        return Err(SurgeError::Platform(
                            "Cannot inspect the command line of the configured env interpreter before swap".to_string(),
                        ));
                    }
                    return Ok(false);
                };
                if argv0 != &expected.program {
                    return Ok(false);
                }
                expected.matches_executable(executable, environment)
            }
        }
    }

    pub(super) fn executable_may_match_in_environment(
        &self,
        executable: &Path,
        environment: &[OsString],
    ) -> Result<bool> {
        match &self.interpreter {
            Interpreter::Resolved(expected) => Ok(paths_resolve_to_same_executable(executable, expected)),
            Interpreter::EnvCommand(expected) => expected.matches_executable(executable, environment),
        }
    }

    pub(super) fn matches_interpreter(&self, executable: &Path, argv0: Option<&OsStr>) -> Result<bool> {
        let command = argv0.map_or_else(Vec::new, |argv0| vec![argv0.to_os_string()]);
        let environment = current_process_path_environment();
        if let Interpreter::EnvCommand(expected) = &self.interpreter {
            if argv0 != Some(expected.program.as_os_str()) {
                return Ok(false);
            }
            let _ = resolve_env_command(expected)?;
        }
        if self.matches_interpreter_in_environment(executable, &command, &environment)? {
            return Ok(true);
        }
        if let Interpreter::EnvCommand(expected) = &self.interpreter
            && argv0 == Some(expected.program.as_os_str())
        {
            return Err(SurgeError::Platform(format!(
                "Cannot verify env interpreter '{}' from the updater environment; refusing to swap while its process identity is ambiguous",
                expected.program.to_string_lossy()
            )));
        }
        Ok(false)
    }

    pub(super) fn executable_may_match(&self, executable: &Path) -> Result<bool> {
        let environment = current_process_path_environment();
        if let Interpreter::EnvCommand(expected) = &self.interpreter {
            let _ = resolve_env_command(expected)?;
        }
        if self.executable_may_match_in_environment(executable, &environment)? {
            return Ok(true);
        }
        let Interpreter::EnvCommand(expected) = &self.interpreter else {
            return Ok(false);
        };
        let program = Path::new(&expected.program);
        Ok(
            (self.requires_environment() || matches!(expected.search_path, EnvSearchPath::Default))
                && program.components().count() == 1,
        )
    }
}

impl EnvCommand {
    fn requires_environment(&self) -> bool {
        matches!(self.search_path, EnvSearchPath::Inherited) && Path::new(&self.program).components().count() == 1
    }

    fn matches_executable(&self, executable: &Path, environment: &[OsString]) -> Result<bool> {
        let Some(resolved) = self.resolve_executable(Some(environment))? else {
            return Ok(false);
        };
        Ok(paths_resolve_to_same_executable(executable, &resolved))
    }

    fn resolve_executable(&self, environment: Option<&[OsString]>) -> Result<Option<PathBuf>> {
        let program = Path::new(&self.program);
        if program.is_absolute() {
            let resolved = std::fs::canonicalize(program).map_err(|e| {
                SurgeError::Platform(format!(
                    "Failed to resolve active application env interpreter '{}': {e}",
                    program.display()
                ))
            })?;
            if !is_executable_file(&resolved) {
                return Err(SurgeError::Platform(format!(
                    "Active application env interpreter '{}' is not executable",
                    program.display()
                )));
            }
            return validate_resolved_interpreter(resolved).map(Some);
        }
        if program.components().count() != 1 {
            return Err(SurgeError::Platform(format!(
                "Cannot safely resolve relative env interpreter '{}' before swap",
                program.display()
            )));
        }

        let current_search_path;
        let search_path = match (&self.search_path, environment) {
            (EnvSearchPath::Inherited, Some(environment)) => {
                environment_variable(environment, b"PATH").unwrap_or_else(|| OsStr::new(DEFAULT_ENV_SEARCH_PATH))
            }
            (EnvSearchPath::Inherited, None) => {
                current_search_path =
                    std::env::var_os("PATH").unwrap_or_else(|| OsString::from(DEFAULT_ENV_SEARCH_PATH));
                current_search_path.as_os_str()
            }
            (EnvSearchPath::Default, _) => OsStr::new(DEFAULT_ENV_SEARCH_PATH),
            (EnvSearchPath::Explicit(search_path), _) => search_path,
        };
        for directory in std::env::split_paths(search_path) {
            if directory.as_os_str().is_empty() || directory.is_relative() {
                return Err(SurgeError::Platform(format!(
                    "Cannot safely resolve env interpreter '{}' through a relative PATH entry before swap",
                    program.display()
                )));
            }
            let candidate = directory.join(program);
            let Ok(candidate) = std::fs::canonicalize(candidate) else {
                continue;
            };
            if !is_executable_file(&candidate) {
                continue;
            }
            return validate_resolved_interpreter(candidate).map(Some);
        }
        Ok(None)
    }
}

fn paths_resolve_to_same_executable(actual: &Path, expected: &Path) -> bool {
    if actual == expected {
        return true;
    }
    if std::fs::canonicalize(actual)
        .ok()
        .zip(std::fs::canonicalize(expected).ok())
        .is_some_and(|(actual, expected)| actual == expected)
    {
        return true;
    }

    std::fs::metadata(actual)
        .ok()
        .zip(std::fs::metadata(expected).ok())
        .is_some_and(|(actual, expected)| actual.dev() == expected.dev() && actual.ino() == expected.ino())
        || macos_system_shell_identity_matches(actual, expected)
}

#[cfg(target_os = "macos")]
fn macos_system_shell_identity_matches(actual: &Path, expected: &Path) -> bool {
    // macOS reports a stable /bin/sh script process as /bin/bash while retaining /bin/sh as argv[0].
    actual == Path::new("/bin/bash") && expected == Path::new("/bin/sh")
}

#[cfg(not(target_os = "macos"))]
fn macos_system_shell_identity_matches(_actual: &Path, _expected: &Path) -> bool {
    false
}

fn resolve_env_command(command: &EnvCommand) -> Result<PathBuf> {
    command.resolve_executable(None)?.ok_or_else(|| {
        SurgeError::Platform(format!(
            "Failed to resolve active application env interpreter '{}' from the updater PATH",
            Path::new(&command.program).display()
        ))
    })
}

fn environment_variable<'a>(environment: &'a [OsString], name: &[u8]) -> Option<&'a OsStr> {
    use std::os::unix::ffi::OsStrExt;

    environment.iter().find_map(|entry| {
        let bytes = entry.as_os_str().as_bytes();
        let separator = bytes.iter().position(|byte| *byte == b'=')?;
        let (entry_name, value) = bytes.split_at(separator);
        let value = value.get(1..)?;
        (entry_name == name).then(|| OsStr::from_bytes(value))
    })
}

fn current_process_path_environment() -> Vec<OsString> {
    std::env::var_os("PATH")
        .map(|path| {
            let mut assignment = OsString::from("PATH=");
            assignment.push(path);
            assignment
        })
        .into_iter()
        .collect()
}

pub(super) fn resolve(active_exe: &Path) -> Result<Option<Identity>> {
    let mut file = std::fs::File::open(active_exe).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application executable before swap: {e}"
        ))
    })?;
    let mut prefix = Vec::new();
    file.by_ref()
        .take(SHEBANG_READ_LIMIT)
        .read_to_end(&mut prefix)
        .map_err(|e| {
            SurgeError::Platform(format!(
                "Failed to inspect active application executable before swap: {e}"
            ))
        })?;
    if !prefix.starts_with(b"#!") {
        return Ok(None);
    }

    let line_end = prefix.iter().position(|byte| *byte == b'\n').unwrap_or(prefix.len());
    if line_end >= SHEBANG_IDENTITY_LIMIT {
        return Err(SurgeError::Platform(format!(
            "Active application shebang exceeds the supported {}-byte process identity limit",
            SHEBANG_IDENTITY_LIMIT - 1
        )));
    }
    if prefix[2..line_end].contains(&0) {
        return Err(SurgeError::Platform(
            "Active application shebang contains an embedded NUL byte".to_string(),
        ));
    }
    let shebang = std::str::from_utf8(&prefix[2..line_end])
        .map_err(|e| SurgeError::Platform(format!("Active application shebang is not valid UTF-8: {e}")))?
        .trim_matches(is_shebang_separator);
    let interpreter_end = shebang.find(is_shebang_separator).unwrap_or(shebang.len());
    let interpreter = &shebang[..interpreter_end];
    let argument = shebang[interpreter_end..].trim_matches(is_shebang_separator);
    if interpreter.is_empty() {
        return Err(SurgeError::Platform(
            "Active application shebang has no interpreter".to_string(),
        ));
    }

    let (interpreter, fixed_argument_count) =
        if Path::new(interpreter).file_name().and_then(|name| name.to_str()) == Some("env") {
            ensure_supported_env_interpreter(interpreter)?;
            let command = parse_env_command(argument)?;
            let fixed_argument_count = command.fixed_argument_count;
            (Interpreter::EnvCommand(command), fixed_argument_count)
        } else {
            #[cfg(target_os = "macos")]
            let fixed_argument_count = macos_direct_interpreter_argument_count(argument);
            #[cfg(not(target_os = "macos"))]
            let fixed_argument_count = direct_interpreter_argument_count(argument);
            (
                Interpreter::Resolved(resolve_direct_interpreter_path(interpreter)?),
                fixed_argument_count,
            )
        };

    Ok(Some(Identity {
        interpreter,
        script_argument_index: 1 + fixed_argument_count,
    }))
}

fn is_shebang_separator(character: char) -> bool {
    matches!(character, ' ' | '\t')
}

fn parse_env_command(argument: &str) -> Result<EnvCommand> {
    let split_command = argument
        .strip_prefix("--split-string=")
        .or_else(|| argument.strip_prefix("-S"))
        .or_else(|| argument.strip_prefix("--split-string "))
        .map(|command| command.trim_start_matches(|character: char| character.is_ascii_whitespace()));
    let command = split_command.unwrap_or(argument);
    let words = split_env_command_words(command)?;
    #[cfg(not(target_os = "macos"))]
    if split_command.is_none() && words.len() > 1 {
        return Err(SurgeError::Platform(
            "Active application env shebang with multiple arguments must use -S".to_string(),
        ));
    }
    let (command_index, search_path) = env_command_index(&words)?;
    Ok(EnvCommand {
        program: OsString::from(&words[command_index]),
        fixed_argument_count: words.len() - command_index - 1,
        search_path,
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

    Ok(command
        .split(|character: char| character.is_ascii_whitespace())
        .filter(|word| !word.is_empty())
        .map(ToString::to_string)
        .collect())
}

fn env_command_index(words: &[String]) -> Result<(usize, EnvSearchPath)> {
    let mut index = 0;
    let mut options = true;
    let mut search_path = None;
    let mut path_unset = false;
    let mut ignore_environment = false;

    while let Some(word) = words.get(index) {
        if options {
            match word.as_str() {
                "--" => {
                    options = false;
                    index += 1;
                    continue;
                }
                "-i" | "--ignore-environment" => {
                    ignore_environment = true;
                    index += 1;
                    continue;
                }
                "-0" | "--null" | "-v" | "--debug" | "--list-signal-handling" => {
                    index += 1;
                    continue;
                }
                "-u" | "--unset" | "-C" | "--chdir" | "-P" => {
                    let Some(value) = words.get(index + 1) else {
                        return Err(SurgeError::Platform(format!(
                            "Active application env shebang option '{word}' has no value"
                        )));
                    };
                    if word == "-P" {
                        search_path = Some(value.clone());
                        path_unset = false;
                    } else if matches!(word.as_str(), "-u" | "--unset") && value == "PATH" {
                        search_path = None;
                        path_unset = true;
                    }
                    index += 2;
                    continue;
                }
                _ if word.starts_with("--unset=") => {
                    if word.strip_prefix("--unset=") == Some("PATH") {
                        search_path = None;
                        path_unset = true;
                    }
                    index += 1;
                    continue;
                }
                _ if word.starts_with("--chdir=") => {
                    index += 1;
                    continue;
                }
                _ if word.starts_with("--path=") => {
                    search_path = word.strip_prefix("--path=").map(ToString::to_string);
                    path_unset = false;
                    index += 1;
                    continue;
                }
                _ if (word.starts_with("-u") || word.starts_with("-C") || word.starts_with("-P")) && word.len() > 2 => {
                    if let Some(path) = word.strip_prefix("-P") {
                        search_path = Some(path.to_string());
                        path_unset = false;
                    } else if word.strip_prefix("-u") == Some("PATH") {
                        search_path = None;
                        path_unset = true;
                    }
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
            if let Some(search_path_assignment) = word.strip_prefix("PATH=") {
                search_path = Some(search_path_assignment.to_string());
                path_unset = false;
            }
            index += 1;
            continue;
        }

        let search_path = match search_path {
            Some(search_path) => EnvSearchPath::Explicit(OsString::from(search_path)),
            None if ignore_environment || path_unset => EnvSearchPath::Default,
            None => EnvSearchPath::Inherited,
        };
        return Ok((index, search_path));
    }

    Err(SurgeError::Platform(
        "Active application env shebang has no interpreter command".to_string(),
    ))
}

fn is_env_assignment(word: &str) -> bool {
    word.contains('=')
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
    validate_resolved_interpreter(resolved)
}

fn validate_resolved_interpreter(resolved: PathBuf) -> Result<PathBuf> {
    let mut file = std::fs::File::open(&resolved).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application interpreter '{}': {e}",
            resolved.display()
        ))
    })?;
    let mut prefix = [0_u8; 4];
    let prefix_len = file.read(&mut prefix).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to inspect active application interpreter '{}': {e}",
            resolved.display()
        ))
    })?;
    if prefix[..prefix_len].starts_with(b"#!") {
        return Err(SurgeError::Platform(format!(
            "Active application interpreter '{}' is itself a shebang script; nested interpreters are unsupported",
            resolved.display()
        )));
    }
    if prefix_len < prefix.len() || !is_supported_native_executable(prefix) {
        return Err(SurgeError::Platform(format!(
            "Active application interpreter '{}' is not a supported native executable; shell fallback interpreters are unsupported",
            resolved.display()
        )));
    }

    Ok(resolved)
}

fn is_executable_file(path: &Path) -> bool {
    use nix::unistd::{AccessFlags, access};

    path.is_file() && access(path, AccessFlags::X_OK).is_ok()
}

#[cfg(target_os = "linux")]
fn is_supported_native_executable(prefix: [u8; 4]) -> bool {
    prefix == *b"\x7fELF"
}

#[cfg(target_os = "macos")]
fn is_supported_native_executable(prefix: [u8; 4]) -> bool {
    matches!(
        prefix,
        [0xfe, 0xed, 0xfa, 0xce]
            | [0xfe, 0xed, 0xfa, 0xcf]
            | [0xce, 0xfa, 0xed, 0xfe]
            | [0xcf, 0xfa, 0xed, 0xfe]
            | [0xca, 0xfe, 0xba, 0xbe]
            | [0xca, 0xfe, 0xba, 0xbf]
            | [0xbe, 0xba, 0xfe, 0xca]
            | [0xbf, 0xba, 0xfe, 0xca]
    )
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn is_supported_native_executable(_prefix: [u8; 4]) -> bool {
    true
}

fn ensure_supported_env_interpreter(interpreter: &str) -> Result<()> {
    let interpreter = Path::new(interpreter);
    if !interpreter.is_absolute() {
        return Err(SurgeError::Platform(format!(
            "Active application env interpreter '{}' must be an absolute path",
            interpreter.display()
        )));
    }
    let resolved = std::fs::canonicalize(interpreter).map_err(|e| {
        SurgeError::Platform(format!(
            "Failed to resolve active application env interpreter '{}': {e}",
            interpreter.display()
        ))
    })?;
    if SUPPORTED_ENV_INTERPRETERS
        .iter()
        .map(Path::new)
        .any(|supported| paths_resolve_to_same_executable(&resolved, supported))
    {
        return Ok(());
    }

    Err(SurgeError::Platform(format!(
        "Active application env interpreter '{}' is not a supported system env executable",
        interpreter.display()
    )))
}

#[cfg(any(target_os = "macos", test))]
fn macos_direct_interpreter_argument_count(argument: &str) -> usize {
    argument.split_ascii_whitespace().count()
}

#[cfg(not(target_os = "macos"))]
fn direct_interpreter_argument_count(argument: &str) -> usize {
    usize::from(!argument.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).unwrap();
    }

    #[test]
    fn macos_direct_interpreter_arguments_are_counted_literally() {
        assert_eq!(
            macos_direct_interpreter_argument_count(r"-e 'two words' plain\ value"),
            5
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_system_sh_matches_reported_bash_process_identity() {
        assert!(paths_resolve_to_same_executable(
            Path::new("/bin/bash"),
            Path::new("/bin/sh")
        ));
        assert!(!paths_resolve_to_same_executable(
            Path::new("/bin/sleep"),
            Path::new("/bin/sh")
        ));
    }

    #[test]
    fn env_command_skips_options_assignments_and_separator() {
        let command = parse_env_command("-S -i -u OLD -- FEATURE=true /bin/sh -e").unwrap();
        assert_eq!(command.program, OsString::from("/bin/sh"));
        assert_eq!(command.fixed_argument_count, 1);
        assert_eq!(command.search_path, EnvSearchPath::Default);
    }

    #[test]
    fn env_command_accepts_non_shell_assignment_names() {
        let command = parse_env_command("-S A-B=value /bin/sh").unwrap();

        assert_eq!(command.program, OsString::from("/bin/sh"));
        assert_eq!(command.fixed_argument_count, 0);
        assert_eq!(command.search_path, EnvSearchPath::Inherited);
    }

    #[test]
    fn env_path_unset_uses_the_default_search_path() {
        for argument in ["-S -u PATH sh", "-S --unset=PATH sh", "-S -uPATH sh"] {
            let command = parse_env_command(argument).unwrap();

            assert_eq!(command.search_path, EnvSearchPath::Default);
        }

        let command = parse_env_command("-S -u PATH PATH=/opt/interpreters sh").unwrap();
        assert_eq!(
            command.search_path,
            EnvSearchPath::Explicit(OsString::from("/opt/interpreters"))
        );

        let command = parse_env_command("-S -u PATH -P /opt/interpreters sh").unwrap();
        assert_eq!(
            command.search_path,
            EnvSearchPath::Explicit(OsString::from("/opt/interpreters"))
        );
    }

    #[test]
    fn env_command_preserves_explicit_search_path() {
        let command = parse_env_command("-S -i -P /opt/interpreters demo-interpreter -e").unwrap();

        assert_eq!(command.program, OsString::from("demo-interpreter"));
        assert_eq!(
            command.search_path,
            EnvSearchPath::Explicit(OsString::from("/opt/interpreters"))
        );

        let long_option = parse_env_command("-S --path=/srv/interpreters demo-interpreter").unwrap();
        assert_eq!(
            long_option.search_path,
            EnvSearchPath::Explicit(OsString::from("/srv/interpreters"))
        );
    }

    #[test]
    fn observed_environment_uses_the_first_path_assignment_and_executable() {
        use std::os::unix::fs::symlink;

        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        symlink("/bin/sleep", first.path().join("demo-interpreter")).unwrap();
        symlink("/bin/sh", second.path().join("demo-interpreter")).unwrap();
        let environment = [
            OsString::from(format!("PATH={}", first.path().display())),
            OsString::from(format!("PATH={}", second.path().display())),
        ];
        let command = EnvCommand {
            program: OsString::from("demo-interpreter"),
            fixed_argument_count: 0,
            search_path: EnvSearchPath::Inherited,
        };

        assert!(
            command
                .matches_executable(&std::fs::canonicalize("/bin/sleep").unwrap(), &environment)
                .unwrap()
        );
        assert!(
            !command
                .matches_executable(&std::fs::canonicalize("/bin/sh").unwrap(), &environment)
                .unwrap()
        );
        assert_eq!(
            environment_variable(&environment, b"PATH"),
            Some(OsStr::new(&format!("{}", first.path().display())))
        );
    }

    #[test]
    fn inherited_env_identity_requires_the_observed_environment() {
        let inherited = Identity {
            interpreter: Interpreter::EnvCommand(EnvCommand {
                program: OsString::from("sh"),
                fixed_argument_count: 0,
                search_path: EnvSearchPath::Inherited,
            }),
            script_argument_index: 1,
        };
        let explicit = Identity {
            interpreter: Interpreter::EnvCommand(EnvCommand {
                program: OsString::from("sh"),
                fixed_argument_count: 0,
                search_path: EnvSearchPath::Explicit(OsString::from("/bin")),
            }),
            script_argument_index: 1,
        };

        assert!(inherited.requires_environment());
        assert!(!explicit.requires_environment());
    }

    #[test]
    fn env_path_assignment_preserves_launch_resolution() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("demo-interpreter");
        symlink("/bin/sh", &interpreter).unwrap();
        let command = parse_env_command(&format!("-S PATH={} demo-interpreter -e", tmp.path().display())).unwrap();
        let identity = Identity {
            interpreter: Interpreter::EnvCommand(command),
            script_argument_index: 2,
        };

        assert!(identity.executable_may_match(&interpreter).unwrap());
    }

    #[test]
    fn env_path_lookup_skips_non_executable_candidates() {
        use std::os::unix::fs::symlink;

        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        std::fs::write(first.path().join("demo-interpreter"), "not executable").unwrap();
        let expected = second.path().join("demo-interpreter");
        symlink("/bin/sh", &expected).unwrap();
        let search_path = std::env::join_paths([first.path(), second.path()]).unwrap();
        let command = EnvCommand {
            program: OsString::from("demo-interpreter"),
            fixed_argument_count: 0,
            search_path: EnvSearchPath::Explicit(search_path),
        };

        assert!(paths_resolve_to_same_executable(
            &resolve_env_command(&command).unwrap(),
            &expected
        ));
    }

    #[test]
    fn env_command_rejects_executable_shell_fallback_text() {
        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("demo-interpreter");
        std::fs::write(&interpreter, "echo fallback\n").unwrap();
        make_executable(&interpreter);
        let command = EnvCommand {
            program: interpreter.into_os_string(),
            fixed_argument_count: 0,
            search_path: EnvSearchPath::Inherited,
        };

        let error = resolve_env_command(&command).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("shell fallback interpreters are unsupported")
        );
    }

    #[test]
    fn absolute_env_command_rejects_nested_interpreter() {
        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("demo-interpreter");
        std::fs::write(&interpreter, "#!/bin/sh\n").unwrap();
        make_executable(&interpreter);
        let command = parse_env_command(interpreter.to_str().unwrap()).unwrap();

        let error = resolve_env_command(&command).unwrap_err();

        assert!(error.to_string().contains("nested interpreters are unsupported"));
    }

    #[test]
    fn search_path_env_command_rejects_nested_interpreter() {
        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("demo-interpreter");
        std::fs::write(&interpreter, "#!/bin/sh\n").unwrap();
        make_executable(&interpreter);
        let command = parse_env_command(&format!("-S -P {} demo-interpreter", tmp.path().display())).unwrap();

        let error = resolve_env_command(&command).unwrap_err();

        assert!(error.to_string().contains("nested interpreters are unsupported"));
    }

    #[test]
    fn inherited_env_path_retains_same_named_candidate_uncertainty() {
        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("sh");
        std::fs::write(&interpreter, "fixture").unwrap();
        let identity = Identity {
            interpreter: Interpreter::EnvCommand(EnvCommand {
                program: OsString::from("sh"),
                fixed_argument_count: 0,
                search_path: EnvSearchPath::Inherited,
            }),
            script_argument_index: 1,
        };

        assert!(identity.executable_may_match(&interpreter).unwrap());
    }

    #[test]
    fn inherited_env_path_retains_symlink_target_uncertainty() {
        let identity = Identity {
            interpreter: Interpreter::EnvCommand(EnvCommand {
                program: OsString::from("sh"),
                fixed_argument_count: 0,
                search_path: EnvSearchPath::Inherited,
            }),
            script_argument_index: 1,
        };

        assert!(identity.executable_may_match(Path::new("/bin/sleep")).unwrap());
    }

    #[test]
    fn default_env_path_retains_candidate_uncertainty() {
        let identity = Identity {
            interpreter: Interpreter::EnvCommand(EnvCommand {
                program: OsString::from("sh"),
                fixed_argument_count: 0,
                search_path: EnvSearchPath::Default,
            }),
            script_argument_index: 1,
        };

        assert!(identity.executable_may_match(Path::new("/bin/sleep")).unwrap());
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
    fn env_split_string_preserves_unicode_whitespace_as_command_data() {
        let command = parse_env_command("-S /tmp/demo\u{a0}interpreter -e").unwrap();

        assert_eq!(command.program, OsString::from("/tmp/demo\u{a0}interpreter"));
        assert_eq!(command.fixed_argument_count, 1);

        let attached = parse_env_command("-S\u{a0}/bin/sh").unwrap();
        assert_eq!(attached.program, OsString::from("\u{a0}/bin/sh"));
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

    #[test]
    fn shebang_with_embedded_nul_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let app = tmp.path().join("app");
        std::fs::write(&app, b"#!/usr/bin/env sh\0 -e\n").unwrap();

        let error = resolve(&app).err().unwrap();

        assert!(error.to_string().contains("embedded NUL"));
    }

    #[test]
    fn shebang_beyond_process_identity_limit_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let app = tmp.path().join("app");
        let mut contents = b"#!/usr/bin/env -S /bin/sh".to_vec();
        contents.resize(SHEBANG_IDENTITY_LIMIT, b' ');
        contents.push(b'\n');
        std::fs::write(&app, contents).unwrap();

        let error = resolve(&app).err().unwrap();

        assert!(error.to_string().contains("process identity limit"));
    }

    #[test]
    fn custom_env_interpreter_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let custom_env = tmp.path().join("env");
        let app = tmp.path().join("app");
        std::fs::write(&custom_env, "fixture").unwrap();
        std::fs::write(&app, format!("#!{} /bin/sh\n", custom_env.display())).unwrap();

        let error = resolve(&app).err().unwrap();

        assert!(error.to_string().contains("not a supported system env executable"));
    }

    #[test]
    fn unicode_whitespace_in_interpreter_path_is_not_a_shebang_separator() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("demo\u{a0}interpreter");
        let app = tmp.path().join("app");
        symlink("/bin/sh", &interpreter).unwrap();
        std::fs::write(&app, format!("#!{}\n", interpreter.display())).unwrap();

        assert!(resolve(&app).unwrap().is_some());
    }

    #[test]
    fn hard_link_interpreter_aliases_share_executable_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let interpreter = tmp.path().join("interpreter");
        let alias = tmp.path().join("interpreter-alias");
        std::fs::write(&interpreter, "fixture").unwrap();
        std::fs::hard_link(&interpreter, &alias).unwrap();

        assert!(paths_resolve_to_same_executable(&interpreter, &alias));
    }
}
