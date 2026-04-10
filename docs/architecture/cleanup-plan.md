# Cleanup Rollout Plan

This document records the first Surge maintainability cleanup wave. The campaign
is complete: the oversized Rust source backlog is gone, the baseline ledger is
empty, and the maintainability guardrail is now blocking in CI.

## Final Status

- `main` stayed green throughout the campaign.
- Each cleanup PR landed as one scoped branch and merged with GitHub `Squash and merge`.
- The Rust source backlog is below the `600` production-line target.
- [`maintainability-baseline.txt`](./maintainability-baseline.txt) is empty and
  available only for future reviewed exceptions.
- The maintainability check is now blocking rather than advisory.

## Completed Phases

These PRs landed during the campaign:

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
- `#64` `refactor(core): split shortcut installation by platform`
- `#65` `refactor(core): split manifest module responsibilities`
- `#66` `refactor(core): split install module responsibilities`
- `#67` `refactor(core): split azure storage backend helpers`
- `#68` `refactor(core): split gcs storage backend helpers`
- `#69` `refactor(bench): split payload generation helpers`
- `#70` `refactor(bench): split runner helpers`
- `#71` `refactor(cli): split main entrypoint helpers`
- `#72` `refactor(installer-ui): split app rendering helpers`
- `#74` `refactor(core): split delta module helpers`
- `#75` `refactor(cli): split remote install helpers`
- `#76` `refactor(cli): split install root orchestration`

## Ongoing Rules

Future maintainability work should continue using the same rollout rules:

- keep `main` green throughout the change
- land one scoped PR at a time from a fresh worktree
- use GitHub `Squash and merge`
- wait for the merged-`main` CI run to finish green before starting the next PR

## Execution Rules

When a future hotspot needs a focused cleanup PR, follow the same loop:

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
12. Start the next PR from a new clean worktree if more cleanup remains.

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

The first-wave campaign closed because all of the following are now true:

- no Rust source file in `crates/*/src` exceeds `600` production lines unless
  it is explicitly accepted debt
- [`maintainability-baseline.txt`](./maintainability-baseline.txt) is empty
- the maintainability check is blocking rather than advisory
- the module roots for install, restore, pack, update, shortcuts, manifest,
  delta, and FFI surfaces are orchestration-first rather than monolithic
