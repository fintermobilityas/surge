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
- client apply, full 100-delta walk: `489 s` (before the carried-tree
  apply below: `657 s`; archive-chunked: ~`18 s`)
- per-delta apply cost is **flat in chain depth**: with per-step
  progress instrumentation, every step took a constant ~`6.4 s` before
  the carried-tree apply and ~`4.8 s` after. Per-step cost is
  CPU/IO-bound and varies ~3x with host load — an earlier 20-delta run
  on an idle host measured ~`2.2 s`/delta, which is the same per-step
  cost, not a depth effect
- per-step cost breakdown (loaded host, scale 1.0): applying the
  file ops ~`4.3 s` (materializing the changed files — the dominant,
  unavoidable cost), extract ~`1.0 s`, repack ~`0.6 s`, hash ~`0.07 s`
- **Carried-tree chain apply (landed)**: the chain walker now extracts
  the starting archive once and applies consecutive sparse deltas in
  place, repacking + SHA-256-verifying per step as before. This drops
  the per-step extract: 100-delta apply `657 s` → `489 s` (−26%),
  20-delta apply `128.7 s` → `100.7 s` (−22%), download bytes and the
  applied payload unchanged (install-tree assertion + byte-identical
  unit equivalence)

Meaning:

- for field bandwidth, sparse deltas win decisively: a 100-release
  localized chain costs half a MiB on the wire
- the 489 s apply is a worst-case full-chain bench walk; production
  caps the client walk at `max_chain_length` (8) with checkpoint
  fulls every `checkpoint_every` (10), so real client applies are
  bounded far below this figure
- the remaining per-step cost is dominated by the file-ops phase
  itself (materializing changed files), not by the archive
  rebuild — see the open item below

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
  per-file diffs) dominates

Meaning:

- even when the client path is acceptable, history retention and
  checkpoint policy still matter
- sparse deltas trade publisher wall time for a ~30x wire reduction;
  the trade favors the fleet because publisher cost is one-time per
  release while wire cost scales with fleet size

## What Is Not Solved Yet

- the per-delta **ops phase** dominates apply (~4.3 s of ~4.8 s per
  step at scale 1.0): materializing the changed files through
  per-file patches is the work itself, and the carried-tree apply
  only removed the re-extract overhead on top of it. The per-step
  repack + hash (~0.7 s) is kept deliberately — it is what makes the
  per-step full SHA-256 verification possible. A further win would
  require applying against the installed tree instead of rebuilding
  archives per step (changes the apply flow and verification shape;
  not worth it while `max_chain_length` bounds walks at 8)
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
