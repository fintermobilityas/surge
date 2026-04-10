# Cleanup Rollout Plan

This document is the working plan for finishing the Surge maintainability
cleanup after the first refactor wave. It tracks what has already landed, what
is in flight, and what still needs to be split before the file-size baseline
can be retired.

## Objectives

- Keep `main` green throughout the cleanup campaign.
- Land one scoped PR at a time from a fresh worktree.
- Use GitHub `Squash and merge` for every cleanup PR.
- Delete the local branch and remove the local worktree after each merge.
- Wait for the merged-`main` CI run to finish green before starting the next PR.
- Reduce oversized Rust source files below the `600` production-line target so
  [`maintainability-baseline.txt`](./maintainability-baseline.txt) can be
  removed.

## Completed Phases

These PRs are already merged:

- `#51` `ci: add maintainability guardrails`
- `#52` `refactor(cli): split install selection and manifest resolution helpers`
- `#53` `refactor(cli): extract install runtime profile and release helpers`
- `#54` `refactor(cli): isolate remote tailscale install helpers`
- `#55` `refactor(ffi): split context and configuration entrypoints`
- `#56` `refactor(ffi): extract update manager and release entrypoints`
- `#57` `refactor(ffi): extract diff and pack entrypoints`
- `#58` `refactor(core): split update manager progress and release index logic`
- `#59` `refactor(core): extract update download and apply pipeline helpers`
- `#60` `refactor(core): isolate update supervisor and lifecycle restart helpers`
- `#61` `refactor(core): split pack builder full delta and index update flows`
- `#62` `refactor(cli): split pack command installer resolution and upload flow`
- `#63` `refactor(core): split release restore planning and artifact recovery`

## Active Phase

### `refactor/manifest-phase-1`

Current goal:

- split [`crates/surge-core/src/config/manifest/mod.rs`](../../crates/surge-core/src/config/manifest/mod.rs)
  into:
  - `manifest/types.rs`
  - `manifest/normalize.rs`
  - `manifest/lookup.rs`
  - `manifest/validate.rs`

Current checkpoint:

- the leaf modules have been created
- the root module has been reduced to parse/load orchestration plus tests
- targeted compile of `surge-core` passes
- focused `config::manifest` tests pass
- focused `surge-core` clippy passes
- the manifest baseline entry has been removed
- the full pre-push suite still needs to run before push

Exit criteria:

- `cargo test -p surge-core config::manifest` passes
- `cargo clippy -p surge-core --all-targets --all-features -- -D warnings -W clippy::pedantic` passes
- `./scripts/check-maintainability.sh` reports the file below the target so the
  manifest baseline entry can be removed
- the full pre-push suite passes
- the PR is merged with squash, local cleanup is done, and merged-`main` CI is green

## Remaining First-Wave PRs

These are the remaining planned PRs from the original Rust-first campaign.

### 1. `refactor/shortcuts-phase-1`

- split [`crates/surge-core/src/platform/shortcuts.rs`](../../crates/surge-core/src/platform/shortcuts.rs)
  into a module tree
- keep the root orchestration-only
- move platform behavior into `linux`, `windows`, `macos`, and shared helper modules

### 2. `refactor/manifest-phase-1`

- split [`crates/surge-core/src/config/manifest/mod.rs`](../../crates/surge-core/src/config/manifest/mod.rs)
  into focused modules for:
  - types
  - normalization
  - validation
  - effective-config or installer metadata helpers

### 3. `refactor/maintainability-phase-2`

- switch maintainability enforcement from advisory-only to blocking for the
  remaining Rust source tree
- remove stale baseline entries that have been burned down
- keep any still-deferred files explicitly listed until they are actually split

## Remaining Second-Wave File Splits

Once the first-wave PRs above land, the following oversized files still need to
be decomposed to fully retire the baseline.

### CLI and Installer surfaces

- [`crates/surge-cli/src/commands/install/mod.rs`](../../crates/surge-cli/src/commands/install/mod.rs)
- [`crates/surge-cli/src/commands/install/remote.rs`](../../crates/surge-cli/src/commands/install/remote.rs)
- [`crates/surge-cli/src/main.rs`](../../crates/surge-cli/src/main.rs)
- [`crates/surge-installer-ui/src/app.rs`](../../crates/surge-installer-ui/src/app.rs)

### Core surfaces

- [`crates/surge-core/src/install.rs`](../../crates/surge-core/src/install.rs)
- [`crates/surge-core/src/releases/delta.rs`](../../crates/surge-core/src/releases/delta.rs)
- [`crates/surge-core/src/storage/azure.rs`](../../crates/surge-core/src/storage/azure.rs)
- [`crates/surge-core/src/storage/gcs.rs`](../../crates/surge-core/src/storage/gcs.rs)

### Bench debt

- [`crates/surge-bench/src/payload.rs`](../../crates/surge-bench/src/payload.rs)
- [`crates/surge-bench/src/runner.rs`](../../crates/surge-bench/src/runner.rs)

## Execution Rules

Every cleanup PR follows the same loop:

1. Pull the latest `main`.
2. Create a fresh branch and worktree for one scoped PR only.
3. Make the split without changing user-facing behavior, file formats, or ABI.
4. Run focused crate checks first.
5. Run the mandatory pre-push suite from `AGENTS.md`.
6. Push and open a draft PR.
7. Wait for GitHub checks to turn green.
8. Mark the PR ready and merge with `Squash and merge`.
9. Pull the squashed result back onto local `main`.
10. Delete the merged local branch and remove the temporary worktree.
11. Wait for the merged-`main` CI run to finish green.
12. Start the next PR from a new clean worktree.

## Validation Gates

Before any push, run the repository pre-push suite:

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

During active development, run focused checks first:

- CLI splits: crate-specific `cargo check`, targeted tests, then crate clippy
- `surge-core` splits: `cargo check -p surge-core`, focused module tests, then
  crate clippy
- FFI splits: `cargo check -p surge-ffi`, targeted FFI regression tests, then
  crate clippy

## Completion Criteria

The cleanup campaign is complete when all of the following are true:

- no Rust source file in `crates/*/src` exceeds `600` production lines unless
  it is explicitly accepted debt
- [`maintainability-baseline.txt`](./maintainability-baseline.txt) is empty or removed
- the maintainability check is blocking rather than advisory
- the module roots for install, restore, pack, update, shortcuts, manifest, and
  FFI surfaces are orchestration-first rather than monolithic
- each merged PR has been cleaned up locally with no stale worktrees left behind
