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
- Run periodic panic-path sweeps in non-test targets with:
  - `cargo clippy --workspace -- -D warnings -D clippy::unwrap_used -D clippy::expect_used`
  - fix runtime `unwrap/expect` in production/build paths instead of suppressing lints.
  - `expect_used` is treated the same as `unwrap_used`: both can hide panic paths in runtime code.
- CI hardening tiers:
  - blocking: `cargo clippy --workspace --lib --bins --examples -- -D warnings -D clippy::unwrap_used -D clippy::expect_used`
  - advisory debt visibility: `cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic`
  - keep pedantic advisory until backlog is reduced; then promote selected pedantic lints to blocking.
- Enforce unsafe boundaries by crate/module:
  - `surge-core` denies unsafe by default; only `src/diff/*` is allowed to use it.
  - `surge-cli` and `surge-supervisor` forbid unsafe.
  - `surge-ffi` is the primary unsafe boundary and must stay explicit/auditable.

## FFI Safety Rules
- Do not store raw back-pointers to context handles in long-lived FFI handles; clone shared `Arc` state instead.
- Clear out-parameters at function entry (`*out = null`) before any fallible work.
- Free functions must safely handle zero-length buffers and null pointers.
- Keep shared error propagation consistent across context/manager/pack handles.
- Prefer checked conversions for lengths/indices (`i64/i32 -> usize`) and reject invalid values early.
- Prefer safe `extern "C" fn` callback types when the function pointer itself has no extra unsafe preconditions.
- If converting C strings for outbound FFI, sanitize embedded NULs to avoid truncated/empty fallback errors.

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
- For FFI changes, add regression tests for:
  - handle lifetime behavior after context destruction,
  - out-pointer behavior on failure paths,
  - zero-length and null-pointer edge cases.
- Keep `crates/surge-core/tests/unsafe_boundaries.rs` passing (unsafe confined to approved modules).
- Before push, run Rust workspace tests + clippy and .NET tests.

## Releasing

Versioning is managed by GitVersion (`GitVersion.yml`). The `next-version` field controls the base version.

### Cutting a release
1. Merge `develop` → `main` (creates the release, e.g. `0.3.0`).
2. **Immediately** bump `next-version` in `GitVersion.yml` on `develop` to the next minor (e.g. `0.4.0`).
3. Commit and push to `develop`.

If step 2 is skipped, develop will keep producing preview versions under the *old* release number (e.g. `0.3.0-preview.N` instead of `0.4.0-preview.N`).

### Major version bumps
For a major release (e.g. `1.0.0`), manually set `next-version` to the target version before merging to main.

## Commit & Pull Request Guidelines
- Use concise imperative commit messages, optionally scoped (examples: `feat(cli): ...`, `fix(core): ...`, `ci: ...`).
- Keep commits focused (one logical change per commit).
- PRs should include: purpose, behavior impact, test evidence (commands run), and migration notes if applicable.
- Ensure GitHub Actions are green before merge.
