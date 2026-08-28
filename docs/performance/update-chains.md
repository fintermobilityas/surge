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
- apply is superlinear in chain length: ~`2.2 s` per delta at 20
  deltas vs ~`6.6 s` per delta at 100 deltas

Meaning:

- for field bandwidth, sparse deltas win decisively: a 100-release
  localized chain costs half a MiB on the wire
- the 657 s apply is a worst-case full-chain bench walk; production
  caps the client walk at `max_chain_length` (8) with checkpoint
  fulls every `checkpoint_every` (10), so real client applies are
  bounded far below this figure
- the superlinear apply growth is an open item (see below)

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

- per-delta apply under `sparse-file-ops` grows superlinearly with
  chain depth (~2.2 s/delta at 20 deltas vs ~6.6 s/delta at 100
  deltas, scale 1.0 sdk-only); the cause is unexplained. Production
  checkpointing bounds real client walks, but any client that does
  pay long-chain apply time would need this investigated before
  `max_chain_length` is ever raised
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
