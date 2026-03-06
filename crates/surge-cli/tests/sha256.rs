use std::process::Command;

use surge_core::crypto::sha256::sha256_hex_file;

#[test]
fn sha256_command_prints_file_hash_to_stdout() {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let file_path = temp_dir.path().join("payload.bin");
    std::fs::write(&file_path, b"sha256 coverage payload").expect("payload file");

    let output = Command::new(env!("CARGO_BIN_EXE_surge"))
        .arg("sha256")
        .arg(&file_path)
        .output()
        .expect("failed to run surge sha256");

    assert!(output.status.success(), "command should succeed: {output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert_eq!(stdout.trim(), sha256_hex_file(&file_path).expect("expected hash"));
    assert!(
        String::from_utf8(output.stderr)
            .expect("utf-8 stderr")
            .trim()
            .is_empty(),
        "stderr should be empty"
    );
}
