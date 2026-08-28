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
- client apply, full 100-delta walk: `657 s` (archive-chunked: ~`18 s`)
- per-delta apply cost is **flat in chain depth**: with per-step
  progress instrumentation, every one of the 100 steps took
  ~`6.4 s` (a 20-delta control run under the same host load measured
  ~`6.3 s`/delta). Per-step cost is CPU-bound (each step re-extracts
  and re-packs the full current archive) and varies ~3x with host
  load — an earlier 20-delta run on an idle host measured
  ~`2.2 s`/delta, which is the same per-step cost, not a depth effect

Meaning:

- for field bandwidth, sparse deltas win decisively: a 100-release
  localized chain costs half a MiB on the wire
- the 657 s apply is a worst-case full-chain bench walk; production
  caps the client walk at `max_chain_length` (8) with checkpoint
  fulls every `checkpoint_every` (10), so real client applies are
  bounded far below this figure
- the per-step cost itself is the open item (see below): every delta
  apply re-extracts and re-packs the entire current archive

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

- each delta apply is O(archive size): `apply_target_deltas`
  re-extracts the current full archive to a temp dir, applies the
  per-file ops, re-packs it, and SHA-256s the result — so a chain
  walk pays extract+repack per delta. Measured flat per step
  (~2.2 s/delta idle host, ~6.4 s/delta loaded host, scale 1.0
  sdk-only); production checkpointing bounds the walk at 8 deltas,
  but edge-node CPU and SSD wear on long walks scale with archive
  size. Candidate shapes: apply ops against the extracted tree
  incrementally across the chain (extract once, repack once), or
  batch the chain against a checkpoint full
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
