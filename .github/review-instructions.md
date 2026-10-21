# Review instructions

The Claude review workflow (`.github/workflows/claude-review.yml`) reads this file from the base branch. It sets what a review reports. `AGENTS.md` holds the full repository rules.

Report a finding only for one of these:

- a concrete input or state and the wrong result it causes: a bug, a panic, a race, a corrupt or partial install or update, a release index that points at the wrong artifact, undefined behaviour across the FFI boundary, a security hole, or a broken contract with a consuming application;
- an item that `AGENTS.md` or this file requires for the kind of change, and that the pull request does not have.

Do not report style, naming, formatting, comment wording, or a choice between two correct designs.

## What to check in this repository

- **Update safety.** Pack, diff, update and supervisor changes that can leave an installed application half updated, lose executable mode bits after download or extraction, skip a checksum, or retry a step that is not safe to repeat. Full and delta flows both need regression coverage.
- **Multi-app manifests.** Storage access and installer metadata prefixes scoped by app id; no mix of base-prefix release indexes with app-scoped artifacts. The app id is inferred from `--rid` only when the RID names exactly one app.
- **Panic paths.** `unwrap` or `expect` in non-test code, and a suppressed clippy warning.
- **Unsafe code.** Unsafe outside `crates/surge-ffi` and `crates/surge-core/src/diff/`, an unsafe block without a `SAFETY:` rationale, and changes that break `crates/surge-core/tests/unsafe_boundaries.rs`.
- **FFI.** The `FFI Safety Rules` in `AGENTS.md`: no raw back-pointers in long-lived handles, out-parameters cleared at entry, null and zero-length inputs handled in free functions, checked length conversions, NULs sanitized in outbound C strings. FFI changes need the regression tests that `AGENTS.md` lists. The C headers in `include/surge/` and `Surge.NET` match the exported functions.
- **Versions and vendoring.** `[workspace.package].version`, the `surge-core` workspace dependency and `Cargo.lock` in sync. A change under `vendor/bsdiff` or `crates/surge-core/vendor/` with the regenerated snapshot.
- **CLI output.** Progress and status output that bypasses `logline`.
- **Performance memory.** A change to pack defaults, delta strategy, `surge tune pack`, `surge-bench` or the `auto/` harnesses without the matching update in `docs/performance/`.
- **Anonymization.** Customer, site or lot names, hostnames, IPs, ids, license plates, exact node counts, downstream app ids or versions, storage names or package paths in code, docs, fixtures, commit messages or the pull request text.
- **Secrets.** Credentials, tokens, signed SAS URLs, private keys or secret-bearing configuration anywhere in the change.
