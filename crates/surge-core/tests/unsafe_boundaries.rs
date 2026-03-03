use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(root).expect("failed to read directory");
    for entry in entries {
        let entry = entry.expect("failed to read directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn unsafe_is_confined_to_diff_modules() {
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut rust_files = Vec::new();
    collect_rust_files(&src_root, &mut rust_files);

    for file in rust_files {
        let rel = file
            .strip_prefix(Path::new(env!("CARGO_MANIFEST_DIR")))
            .expect("failed to strip source root")
            .to_string_lossy()
            .replace('\\', "/");

        let allowed_unsafe_file = matches!(
            rel.as_str(),
            "src/diff/bsdiff_sys.rs" | "src/diff/wrapper.rs" | "src/diff/mod.rs"
        );

        let content = fs::read_to_string(&file).expect("failed to read rust source file");
        for (line_no, line) in content.lines().enumerate() {
            if !line.contains("unsafe") {
                continue;
            }

            if allowed_unsafe_file {
                continue;
            }

            if rel == "src/lib.rs" && line.trim() == "#![deny(unsafe_code)]" {
                continue;
            }

            panic!("unexpected unsafe usage in {rel}:{}", line_no + 1);
        }
    }
}
