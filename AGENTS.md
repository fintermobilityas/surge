# Repository Guidelines

## Project Structure & Module Organization
Surge is a Cargo workspace plus a .NET wrapper:
- `crates/surge-core/`: update engine (storage, releases, diff, pack, update, supervisor).
- `crates/surge-cli/`: `surge` CLI.
- `crates/surge-ffi/`: C ABI used by native/.NET callers.
- `crates/surge-supervisor/`: supervisor binary.
- `dotnet/`: managed wrapper and tests (`Surge.NET`, `Surge.NET.Tests`).
- `include/surge/`: public C headers.
- `vendor/bsdiff/`: required submodule for C bsdiff backend.
- `assets/`, `demoapp/`: examples and fixtures.

## Build, Test, and Development Commands
Initialize submodules first:
```bash
git submodule update --init --recursive
```
Rust:
```bash
RUSTFLAGS="-D warnings" cargo build --release
RUSTFLAGS="-D warnings" cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
.NET:
```bash
dotnet build dotnet/Surge.slnx --configuration Release
dotnet test dotnet/Surge.slnx --configuration Release
```

## Rust Quality Bar (Best Practices)
- Prefer self-documenting code: clear types, names, and small functions over explanatory comments.
- Use comments sparingly; add them only for invariants, non-obvious tradeoffs, or safety contracts.
- Keep modules cohesive and APIs explicit (`Result<T, E>`, typed structs/enums instead of ad-hoc tuples).
- Minimize `unsafe`: isolate it to FFI/boundary layers, prefer safe wrappers, and remove unnecessary `unsafe impl`.
- Every remaining unsafe block must include a short `SAFETY:` rationale.

## Coding Style & Naming
- Rust edition: 2024; format with `cargo fmt --all`.
- Clippy/rustc warnings are treated as errors in CI; fix warnings instead of suppressing.
- Use idiomatic Rust naming: `snake_case` (functions/modules), `CamelCase` (types), `SCREAMING_SNAKE_CASE` (consts).
- Prefer `thiserror` for errors and `tracing` for logs.
- Keep interfaces cross-platform and avoid embedding credentials in manifests or client configs.

## Testing Guidelines
- Keep unit tests close to code (`#[cfg(test)]` in modules).
- Add integration tests under `crates/*/tests` when behavior spans modules/commands.
- For update/diff changes, include regression coverage for full + delta flows.
- Before push, run Rust workspace tests + clippy and .NET tests.

## Commit & Pull Request Guidelines
- Use concise imperative commit messages, optionally scoped (examples: `feat(cli): ...`, `fix(core): ...`, `ci: ...`).
- Keep commits focused (one logical change per commit).
- PRs should include: purpose, behavior impact, test evidence (commands run), and migration notes if applicable.
- Ensure GitHub Actions are green before merge.
