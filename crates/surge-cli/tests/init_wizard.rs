use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use surge_core::config::manifest::SurgeManifest;
use uuid::Uuid;

fn run_wizard(current_dir: &Path, args: &[&str], stdin_input: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_surge"))
        .current_dir(current_dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn surge binary");

    {
        let stdin = child.stdin.as_mut().expect("missing child stdin");
        stdin
            .write_all(stdin_input.as_bytes())
            .expect("failed to write wizard stdin");
    }

    child.wait_with_output().expect("failed to wait for surge")
}

fn run_command(current_dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_surge"))
        .current_dir(current_dir)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to execute surge command")
}

fn output_to_debug(output: &Output) -> String {
    format!(
        "status={:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn test_init_wizard_defaults_to_dot_surge_manifest() {
    let tmp = tempfile::tempdir().unwrap();

    // Accept defaults for all 7 wizard prompts.
    let output = run_wizard(tmp.path(), &["init", "--wizard"], "\n\n\n\n\n\n\n");
    assert!(output.status.success(), "{}", output_to_debug(&output));

    let manifest_path = tmp.path().join(".surge").join("surge.yml");
    assert!(manifest_path.exists(), "manifest missing: {}", manifest_path.display());

    let packages_dir = tmp.path().join(".surge").join("packages");
    assert!(
        packages_dir.is_dir(),
        "packages dir missing: {}",
        packages_dir.display()
    );

    let manifest = SurgeManifest::from_file(&manifest_path).unwrap();
    assert_eq!(manifest.storage.provider, "filesystem");
    assert_eq!(manifest.storage.bucket, ".surge/storage");
    assert_eq!(manifest.apps.len(), 1);
    assert_eq!(manifest.apps[0].id, "my-app");
    assert_eq!(manifest.apps[0].name, "my-app");
    assert_eq!(manifest.apps[0].main_exe, "my-app");
    assert_eq!(manifest.apps[0].install_directory, "my-app");
    assert!(Uuid::parse_str(&manifest.apps[0].supervisor_id).is_ok());
    assert_eq!(manifest.apps[0].targets.len(), 1);
}

#[test]
fn test_init_defaults_to_wizard_mode() {
    let tmp = tempfile::tempdir().unwrap();

    // No --wizard flag; should still run interactive mode by default.
    let output = run_wizard(tmp.path(), &["init"], "\n\n\n\n\n\n\n");
    assert!(output.status.success(), "{}", output_to_debug(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Surge init wizard"),
        "expected default wizard output, got:\n{stdout}"
    );
    assert!(tmp.path().join(".surge").join("surge.yml").exists());
}

#[test]
fn test_init_wizard_respects_custom_manifest_path() {
    let tmp = tempfile::tempdir().unwrap();

    // App id, name, provider, bucket, rid, main exe, install directory.
    let wizard_input = "demo-app\nDemo App\nfilesystem\n/tmp/surge-store\nlinux-x64\ndemo-app\ndemo-install\n";
    let output = run_wizard(
        tmp.path(),
        &["-m", "custom/surge.yml", "init", "--wizard"],
        wizard_input,
    );
    assert!(output.status.success(), "{}", output_to_debug(&output));

    let custom_manifest = tmp.path().join("custom").join("surge.yml");
    assert!(
        custom_manifest.exists(),
        "custom manifest missing: {}",
        custom_manifest.display()
    );

    let default_manifest = tmp.path().join(".surge").join("surge.yml");
    assert!(
        !default_manifest.exists(),
        "unexpected default manifest at {}",
        default_manifest.display()
    );

    let manifest = SurgeManifest::from_file(&custom_manifest).unwrap();
    assert_eq!(manifest.storage.provider, "filesystem");
    assert_eq!(manifest.storage.bucket, "/tmp/surge-store");
    assert_eq!(manifest.apps[0].id, "demo-app");
    assert_eq!(manifest.apps[0].name, "Demo App");
    assert_eq!(manifest.apps[0].main_exe, "demo-app");
    assert_eq!(manifest.apps[0].install_directory, "demo-install");
    assert!(Uuid::parse_str(&manifest.apps[0].supervisor_id).is_ok());
    assert_eq!(manifest.apps[0].targets[0].rid, "linux-x64");

    // `packages` auto-creation only happens for `.surge` parent.
    assert!(!tmp.path().join("custom").join("packages").exists());
}

#[test]
fn test_init_non_wizard_when_options_are_provided() {
    let tmp = tempfile::tempdir().unwrap();
    let output = run_command(
        tmp.path(),
        &[
            "init",
            "--app-id",
            "demo-app",
            "--provider",
            "filesystem",
            "--bucket",
            "/tmp/surge-store",
            "--rid",
            "linux-x64",
            "--main-exe",
            "demo-main",
            "--install-directory",
            "demo-install",
        ],
    );
    assert!(output.status.success(), "{}", output_to_debug(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains("Surge init wizard"),
        "did not expect wizard output when explicit options are supplied"
    );

    let manifest_path = tmp.path().join(".surge").join("surge.yml");
    let manifest = SurgeManifest::from_file(&manifest_path).unwrap();
    let app = &manifest.apps[0];
    assert_eq!(app.id, "demo-app");
    assert_eq!(app.main_exe, "demo-main");
    assert_eq!(app.install_directory, "demo-install");
    assert_eq!(app.targets[0].rid, "linux-x64");
}
