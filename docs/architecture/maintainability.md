# Maintainability Guardrails

This document describes the module boundaries and file-size guardrails for the
Surge Rust source tree. The goal is to keep large multi-purpose source files
from growing back into hotspots after the first refactor wave burned down the
initial backlog.

## Module Boundaries

### `surge-core`

- Owns shared domain logic, manifest normalization and validation, release graph
  planning, storage-facing workflows, install/update orchestration, and
  platform abstractions.
- Keep module roots orchestration-focused. Split detailed behavior into leaf
  modules by responsibility, such as restore planning vs restore execution,
  update progress vs supervisor handoff, and per-platform shortcut handling.
- If CLI, installer, or FFI code needs shared release, manifest, or runtime
  logic, move that logic into `surge-core` instead of duplicating it.

### `surge-cli`

- Owns command orchestration, user-facing prompts, logline output, and
  command-specific progress reporting.
- Large commands should become module trees where the root coordinates flow and
  sibling modules handle focused concerns such as manifest resolution, target
  selection, remote staging, or installer upload.
- Do not mix prompt collection, manifest parsing, remote execution, and output
  formatting in one growing file when they can live in separate modules.

### `surge-ffi`

- Owns the explicit unsafe boundary and C ABI surface.
- Split the root by API surface: context/configuration, update manager,
  releases, diff/pack, and shared pointer or callback helpers.
- Preserve exported symbol names and signatures while moving implementation
  details behind focused internal modules.

### Platform and Provider Surfaces

- Keep platform-specific behavior in per-platform modules instead of
  `cfg`-heavy monoliths.
- Keep storage-provider-specific behavior isolated by backend. Shared retry,
  auth, or upload helpers belong in common internal helpers instead of being
  reimplemented per backend.

## File Size Policy

- Start splitting Rust source files before they reach roughly `600` production
  lines.
- Production lines are measured up to the inline `#[cfg(test)] mod tests { ... }`
  block at the end of a file.
- Inline tests should stay at the end of the file. When tests dominate a large
  source file, move them into a colocated `tests/` tree next to the module.
- `#[allow(clippy::too_many_lines)]` is temporary debt and should not be added
  to new code as a substitute for decomposition.
- Existing `clippy::too_many_lines` allowances are separate lint debt. They do
  not override the file-size guardrail or justify growing a source file past the
  `600`-line target.

## Blocking Guardrail

- `./scripts/check-maintainability.sh` is a blocking local and CI check.
- The script uses [`maintainability-baseline.txt`](./maintainability-baseline.txt)
  only for explicitly accepted temporary exceptions.
- The baseline ledger is currently empty. Any Rust source file in `crates/*/src`
  that crosses the threshold now fails immediately unless a reviewed baseline
  entry is added in the same change.
- If a temporary baseline entry is ever needed again, it must record the
  current measured production-line count and be removed in the PR that brings
  the file back under the target.

## Review Heuristics

Use these checks while refactoring or reviewing:

- Does this file have one reason to change?
- Should this root file delegate to a small module tree instead of gaining one
  more helper?
- Is shared manifest, release, install, or runtime logic duplicated outside
  `surge-core`?
- Would moving tests into a neighboring `tests/` tree keep the production file
  focused?
