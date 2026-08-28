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

> Measurement note: the figures below were recorded while the update
> scenario ran the `archive-chunked-bsdiff` bench strategy. The bench
> manifest now pins `sparse-file-ops`, the production default, so the
> large-scale numbers are pending a rerun before they can be attributed
> to the default strategy (see "When To Rerun"). Small-scale reference
> under the new configuration: `sdk_only`, 20 deltas, scale 0.05 —
> apply ≈ 8.5 s, download ≈ 102 KiB (48-core/251 GB host, seed 42).

### Localized long chains are acceptable

Large anonymized profile, `sdk_only`, `100` deltas (measured under
`archive-chunked-bsdiff` — see measurement note above):

- client download stayed around `15.6 MiB`
- client apply time was about `18s`

Meaning:

- repeated localized SDK changes are not the catastrophe case on the client side

### Broad churn is now bounded by file-aware deltas and full fallback

Large anonymized profile, `full_release`, `10` deltas:

- changed file payload dominates transfer size instead of whole-archive churn
- local apply remains bounded because reconstructed fulls are cached for reuse

Meaning:

- the system no longer depends on archive-level deltas staying stable
- pathological deltas still need a full-checkpoint escape hatch

### Publisher cost remains important

Localized `100`-delta chain:

- publishing the `101`-release chain took about `337s`

Meaning:

- even when the client path is acceptable, history retention and checkpoint policy still matter

## What Is Not Solved Yet

- the large-scale chain numbers above have not been re-measured under
  the `sparse-file-ops` bench configuration; per-file apply was ~2.9x
  slower than archive-chunked at small scale, so whether the 15.6 MiB /
  18 s client figures hold at scale is open
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
