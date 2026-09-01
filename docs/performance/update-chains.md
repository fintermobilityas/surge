# Update Chains

## What Is Known

The native updater path is now benchmarked end to end through the real `UpdateManager` flow.

That means the benchmark now measures:

- release-index lookup
- delta-chain selection
- full-pack rebuild time during publishing
- delta-pack build time during publishing
- full and delta artifact upload time
- artifact download size
- restore-and-apply behavior
- final installed payload verification
- local checkpoint reuse when reconstructed full archives are cached

## Current Findings

> Measurement note: the long-chain figures below were re-measured
> under the `sparse-file-ops` bench configuration (the production
> default); the earlier `archive-chunked-bsdiff` figures are kept in
> parentheses for comparison. Small-scale reference under the current
> configuration: `sdk_only`, 20 deltas, scale 0.05 — apply ≈ 8.5 s,
> download ≈ 102 KiB (48-core/251 GB host, seed 42).

### Localized long chains: wire cost is negligible, full-chain apply is not

Large anonymized profile, `sdk_only`, `100` deltas, `sparse-file-ops`
(48-core/251 GB host, seed 42):

- client download: `510 KiB` (archive-chunked: `15.6 MiB` — ~30x less)
- client apply, full 100-delta walk: ~`160 s` (session history:
  `657 s` → `489 s` → `221 s` → `157 s` → ~`160 s`; archive-chunked:
  ~`18 s`; load regimes differ between sessions — compare within a
  session)
- per-delta apply cost is **flat in chain depth**: with per-step
  instrumentation, every step took a constant ~`6.4 s` originally,
  ~`4.8 s` after the carried-tree apply, ~`2.2 s` after identity-chunk
  patches, ~`1.6 s` after the verified-hash carry, and ~`1.5-2.0 s`
  after the streamed target hash (the range is host-load variance, not
  a regression). Per-step cost is CPU/IO-bound and varies ~3x with
  host load
- per-step cost breakdown (loaded host, scale 1.0, phase-instrumented):
  chunked `bspatch` over the 1 GB file ~`3.5-5.0 s` **before** the
  identity-chunk fix (15 of 16 unchanged 64 MiB chunks re-derived),
  basis + target SHA-256 ~`0.55 s` each, repack ~`0.6-3.0 s`
  (load-dependent). An earlier breakdown attributed ~`4.3 s` to
  "materializing the changed files" — that was mislabeled bspatch
  work, not the 4 KiB of actual change
- **Carried-tree chain apply (landed)**: the chain walker now extracts
  the starting archive once and applies consecutive sparse deltas in
  place, repacking + SHA-256-verifying per step as before. This drops
  the per-step extract: 100-delta apply `657 s` → `489 s` (−26%),
  20-delta apply `128.7 s` → `100.7 s` (−22%), download bytes and the
  applied payload unchanged (install-tree assertion + byte-identical
  unit equivalence)
- **Identity-chunk chunked patches (landed)**: chunked bsdiff format
  v2 (`CSDF`) marks unchanged chunks in a per-chunk bitset instead of
  carrying a whole-chunk identity bsdiff. The diff side skips the
  per-chunk bsdiff for identical chunks (memcmp instead of a suffix
  array over 64 MiB) and the apply side copies unchanged chunks
  straight through. Version 1 patches still apply; version 2 patches
  are rejected by version 1 readers via the version check.
  Same-session 100-delta rerun: apply `480.5 s` → `223.1/218.3 s`
  (−54%), publish `846.1 s` → `707.8/741.5 s` (−14%), download
  `522,171 B` → `503,524 B` (−3.6%, deterministic), install tree
  byte-identical
- **Verified-hash carry across chain steps (landed)**: the chain
  walker carries a path → verified-SHA-256 map; each step's target
  hash is exactly the next step's basis hash for the same file, so the
  redundant full-file basis re-read + re-hash is skipped when the
  cache records the expected hash (first step and any mismatch still
  verify fully; the post-patch target hash and the per-step
  full-archive SHA-256 are unchanged, so external modification between
  steps is still caught). Same-session 100-delta A/B: apply
  `216.8 s` → `156.6/158.1 s` (−27%, 0.9% spread), per step ~`2.2 s`
  → ~`1.6 s`, download bytes and install tree unchanged
- **Streamed target hash (landed)**: the per-step target SHA-256 is
  computed while the patched file is written (the chunked bspatch
  write path returns the output hash) instead of a separate full read
  + hash pass afterwards. Removes one full-file re-read per step
  (the `0.55 s` target-hash phase at scale 1.0); the hashing CPU is
  unchanged. Phase-instrumented 10-step A/B (same session, 4 pairs):
  per step ~`2,270 ms` → ~`1,980-2,060 ms` in 3 of 4 pairs (the 4th
  was a load-spike window that hit both sides); 100-delta same-session
  medians ~`167 s` vs ~`172 s` — the ~`0.2-0.3 s`/step gain sits near
  the shared-host wall-clock noise floor, so the phase-level evidence
  carries the claim. Payload and verification are unchanged (tamper
  test still fail-closed)

Meaning:

- for field bandwidth, sparse deltas win decisively: a 100-release
  localized chain costs half a MiB on the wire
- the ~`157 s` full-chain apply is a worst-case full-chain bench
  walk; production caps the client walk at `max_chain_length` (8)
  with checkpoint fulls every `checkpoint_every` (10), so real client
  applies are bounded far below this figure
- the remaining per-step cost (~`1.6 s` at scale 1.0) is: one full
  read + one full write of the changed file for the bspatch, one
  full-file target SHA-256, and the per-step repack (variable
  `0.6-3.0 s` under host load)

### Broad churn is now bounded by file-aware deltas and full fallback

Large anonymized profile, `full_release`, `10` deltas:

- changed file payload dominates transfer size instead of whole-archive churn
- local apply remains bounded because reconstructed fulls are cached for reuse

Meaning:

- the system no longer depends on archive-level deltas staying stable
- pathological deltas still need a full-checkpoint escape hatch

### Publisher cost remains important

Localized `100`-delta chain, `sparse-file-ops`:

- publishing the `101`-release chain took `897 s` (archive-chunked:
  ~`337 s`) — the per-release delta build (~`5.1 s` per delta,
  per-file diffs) dominates; after identity-chunk patches the same
  session measured ~`725 s` (−14%) and ~`4.2 s` per delta

Meaning:

- even when the client path is acceptable, history retention and
  checkpoint policy still matter
- sparse deltas trade publisher wall time for a ~30x wire reduction;
  the trade favors the fleet because publisher cost is one-time per
  release while wire cost scales with fleet size

## What Is Not Solved Yet

- the per-step bspatch still pays a full read + full write of the
  changed file even for a 4 KiB change (the target SHA-256 is now
  streamed into the write, so no extra read). Anything deeper
  (range-scoped ops) needs a new sparse op kind
- the per-step **repack** is the most load-variable component
  (`0.6-3.0 s`): zstd over ~1.2 GB per step, re-encoding unchanged
  files even though only one file changed
- retained full checkpoints still need long-history tuning in real feeds
- broad-churn chains can still justify a fresh full checkpoint
- local checkpoint retention policy may need calibration for very long-lived installs

## Recommended Direction

Short term:

- keep sparse file-aware deltas as the default path
- keep pack defaults aligned with the measured recommendation
- tune remote checkpoint retention and local checkpoint cache limits

Long term:

- consider content-addressed chunk storage if sparse file deltas are still too large
- avoid letting remote history drift far from recent checkpoint fulls

## When To Rerun

Rerun the long-chain benchmarks when:

- delta strategy changes
- pack defaults change
- restore logic changes
- update planning changes
- retention or chain-cap logic is implemented
