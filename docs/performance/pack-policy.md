# Pack Policy

## Current Default Recommendation

Current recommended pack policy:

```yaml
pack:
  delta:
    strategy: archive-chunked-bsdiff
    max_chain_length: 8
  compression:
    format: zstd
    level: 3
  retention:
    keep_latest_fulls: 2
    checkpoint_every: 10
```

Current operational node policy:

- all visible CPU cores
- `256 MiB` diff budget

## Why These Defaults Were Chosen

### Delta strategy

Use `archive-chunked-bsdiff`.

Reason:

- it materially improved the real pack path over the old legacy diff flow
- it is much better than the old path for localized changes
- it fits the current artifact model without introducing a new on-disk format family

### Compression level

Use `zstd=3`.

Reason:

- on the calibrated heavy large case, `zstd=3` was the fastest measured default
- it beat `zstd=1` on total build time
- it also slightly improved artifact sizes versus `zstd=1` in the sweep

### Retention and chain policy

Recommended policy:

- keep latest full
- keep previous full
- keep periodic checkpoint fulls
- cap chain length

Reason:

- publisher cost otherwise grows with history reconstruction work
- client update cost otherwise grows with chain length
- broad churn can still produce poor delta quality even with the improved pack path

## What `surge tune pack` May Change

The tune command is intended to update artifact policy, not machine-local execution policy.

Allowed manifest writes:

- `pack.delta.strategy`
- `pack.compression.level`

Accepted but not yet auto-tuned:

- `pack.delta.max_chain_length`
- `pack.retention.keep_latest_fulls`
- `pack.retention.checkpoint_every`

Not allowed in the manifest:

- thread count
- memory budget
- runner-specific concurrency

## Current Tune Command Role

Use:

```bash
surge tune pack --version <VERSION> [--write-manifest]
```

Purpose:

- benchmark candidate strategy and compression-level combinations against the real pack builder path
- print the measured results
- optionally write the recommended strategy and compression level to `surge.yml`

It should remain:

- explicit
- opt-in
- policy-focused

It should not become a hidden stage inside normal `surge pack`.
