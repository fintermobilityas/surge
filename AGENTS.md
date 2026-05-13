# Repository Guidelines

## Project Structure & Module Organization
Surge is a Cargo workspace plus a .NET wrapper:
- `crates/surge-core/`: update engine (storage, releases, diff, pack, update, supervisor).
- `crates/surge-cli/`: `surge` CLI.
- `crates/surge-ffi/`: C ABI used by native/.NET callers.
- `crates/surge-installer/`: console installer launcher (extracts payload, delegates to `surge setup`).
- `crates/surge-installer-ui/`: GUI installer with egui (self-contained graphical installer).
- `crates/surge-supervisor/`: supervisor binary.
- `dotnet/`: managed wrapper and tests (`Surge.NET`, `Surge.NET.Tests`).
- `include/surge/`: public C headers.
- `vendor/bsdiff/`: required submodule for C bsdiff backend.
- `crates/surge-core/vendor/`: committed publishable snapshot generated from `vendor/bsdiff` for `surge-core` builds and crates.io packaging.
- `assets/`, `demoapp/`: examples and fixtures.

### Installer Types
Surge supports four installer types configured via `installers:` in the manifest:
- `online`: Console installer that downloads the package at install time (uses `surge-installer`).
- `offline`: Console installer with the full package embedded (uses `surge-installer`).
- `online-gui`: GUI installer (egui) that downloads at install time (uses `surge-installer-ui`).
- `offline-gui`: GUI installer (egui) with the full package embedded (uses `surge-installer-ui`).

The legacy `web` type has been removed; use `online` instead.

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

- Run the workspace validation commands non-interactively. Do not allocate a TTY/PTY for `cargo test --workspace` or the chained pre-push validation suite, because prompt-path tests can wait on an interactive terminal and hang the run.

## Mandatory Pre-Push Validation
Before any push, run the same quality gates CI uses. Do not push if any command fails.

```bash
./scripts/sync-surge-core-vendor.sh --check
./scripts/check-version-sync.sh
cargo fmt --all -- --check
RUSTFLAGS="-D warnings" cargo test --workspace
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --workspace --lib --bins --examples -- -D warnings -D clippy::unwrap_used -D clippy::expect_used
cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic
dotnet format dotnet/Surge.slnx --verify-no-changes
dotnet test dotnet/Surge.slnx --configuration Release
```

If the local environment cannot run a listed command, document the exact gap in the PR and run it in CI before merge.

- When updating `vendor/bsdiff` or anything under `crates/surge-core/vendor/`, regenerate the publishable snapshot with `./scripts/sync-surge-core-vendor.sh` before running the checks above.

## Rust Quality Bar (Best Practices)
- Prefer self-documenting code: clear types, names, and small functions over explanatory comments.
- Use comments sparingly; add them only for invariants, non-obvious tradeoffs, or safety contracts.
- Keep modules cohesive and APIs explicit (`Result<T, E>`, typed structs/enums instead of ad-hoc tuples).
- Treat roughly 600 production lines as the point to split a Rust source file; keep module roots orchestration-focused and move detailed behavior into focused leaf modules.
- Prefer typed error enums (`thiserror`) over `Box<dyn Error>` in binaries/crates where error cases are known.
- Consolidate repeated crate-local helpers (for example mutex poison recovery and C-string sanitization) into a shared internal module.
- Prefer `unwrap_or_else(std::sync::PoisonError::into_inner)` over manual `match` when recovering poisoned mutexes.
- Avoid unnecessary `crate::` path prefixes in module-local code/tests when imports already provide the item.
- For multi-app manifests, always scope storage access and emitted installer metadata prefixes by app id; never mix base-prefix release indexes with app-scoped artifact flows.
- For CLI commands that accept both optional `--app-id` and `--rid`, use a shared RID-hint resolver to infer app id only when the RID uniquely identifies one app.
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

## Dependencies
- When adding new dependencies, always check crates.io for the latest stable version before specifying a version in `Cargo.toml`. Never assume a version number from memory.

## Integrating Surge Into Other App Repos
- Treat `docs/integrating-surge.md` as the shared playbook for future app integrations. It is written for both humans and agents.
- Default to released Surge tags for app integrations. Use local checkout or pinned-commit overrides only while validating an unmerged Surge fix.
- When a new Surge beta is cut for downstream consumption, do not update listed app repos such as `../youpark`, `../youpayv2`, or `../accesspad` until the matching `Surge.NET` NuGet package is available from the configured feed and a restore can see it. After the package is available, bump and commit the downstream repo updates as part of the beta cut.
- If an app repo integrates `surge-core`, treat that as a runtime dependency as well as a publishing dependency; do not reduce it to "CLI only" in smoke or release guidance.
- App repos should expose wrapper scripts for local filesystem smoke and Windows Azure smoke. Agents should use those wrappers instead of ad hoc `surge pack` / `surge push` commands.
- If a Rust app temporarily overrides `surge-core`, prefer a local `file://` Git source pinned to the exact Surge commit instead of a raw crate path. This preserves workspace dependency resolution in clean and cross-platform smoke runs.
- If a failure is in Surge itself, fix it upstream in this repo first, release a new tag, then update the app repo. Do not normalize long-lived downstream patches.

## CLI Logging Output
- For `surge-cli` command output (including progress/status updates), use the existing `logline` facility in `crates/surge-cli/src/logline.rs`.
- Do not introduce alternative output paths for status/progress (for example ad-hoc `println!`-driven progress UIs) when `logline` can represent the same information.

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

## Performance Memory
- Benchmark and pack-policy memory lives under `docs/performance/`.
- When changing pack defaults, delta strategy, `surge tune pack`, `surge-bench`, or `.github/workflows/benchmark.yml`, update the relevant files in `docs/performance/` in the same change.
- Keep benchmark payload descriptions anonymized and generic; do not add private product names or file names to docs, workflow labels, or benchmark fixtures.
- Keep `.github/workflows/benchmark.yml` aligned with `docs/performance/benchmark-profiles.md` and `docs/performance/pack-policy.md`.

## Anonymized Communication
- In agent-authored issues, PRs, plans, summaries, and troubleshooting notes, use fictional placeholder names for products, customers, sites, hosts, and environments by default.
- Do not repeat real deployment names or hostname patterns such as `*-master` in user-facing prose when a fictional placeholder will do.
- Keep real identifiers only when technically required for literal commands, file paths, workflow names, or source references.

## Releasing

Versioning is tag-driven. The single source of truth is `[workspace.package].version` in `Cargo.toml`.

### Version scripts
- `scripts/version-lib.sh` — shared helpers for reading/writing workspace versions.
- `scripts/check-version-sync.sh` — CI guard: ensures `[workspace.package].version`, `[workspace.dependencies].surge-core` version, and `Cargo.lock` entries all match.
- `scripts/next-version.sh <alpha|beta|stable>` — suggests the next tag for a given channel by scanning existing git tags.
- `scripts/set-release-version.sh <version>` — rewrites `Cargo.toml` to the exact release version (including prerelease suffixes) and updates `Cargo.lock`. Used in the release workflow.

### Cutting a prerelease
1. Run `./scripts/next-version.sh alpha` (or `beta`) to get the next tag (e.g. `v0.4.0-alpha.1`).
2. Create a GitHub Release with that tag, marking it as a prerelease.
3. The release workflow validates the tag, builds artifacts, publishes `Surge.NET` to NuGet and Rust crates to crates.io, and uploads assets.

### Cutting a stable release
1. Run `./scripts/next-version.sh stable` to get the tag (e.g. `v0.4.0`).
2. Create a GitHub Release with that tag (not marked as prerelease).
3. The release workflow validates, builds artifacts, publishes `Surge.NET` to NuGet and Rust crates to crates.io, and uploads assets.
4. After a stable release, bump `Cargo.toml` to the next release line in a normal PR before cutting more prereleases:
   - `[workspace.package].version`
   - `[workspace.dependencies].surge-core` version

If step 4 is skipped, `next-version.sh` will refuse to produce new tags until the version is bumped.

### Major version bumps
For a major release (e.g. `1.0.0`), manually set `[workspace.package].version` and `[workspace.dependencies].surge-core` version to the target before creating the release.

## Commit & Pull Request Guidelines
- Use concise imperative commit messages, optionally scoped (examples: `feat(cli): ...`, `fix(core): ...`, `ci: ...`).
- Keep commits focused (one logical change per commit).
- PRs should include: purpose, behavior impact, test evidence (commands run), and migration notes if applicable.
- Agent-authored or agent-managed PRs must use GitHub `Squash and merge`; do not use merge commits or rebase merge.
- Ensure GitHub Actions are green before merge.
