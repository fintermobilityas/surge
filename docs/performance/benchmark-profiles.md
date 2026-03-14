# Benchmark Profiles

## Goal

These profiles exist to make pack and update benchmarking reproducible without referring to any private application.

They are calibrated to an anonymized large desktop-app shape with:

- `319` files
- about `1.24 GiB` raw payload
- one dominant native SDK binary around `1.06 GiB`

## Scenario Labels

### `full_release`

Represents broad churn:

- rewrites many top files
- mutates the dominant SDK
- adds small feature files
- removes one small config file

Use this when measuring:

- worst-case or near-worst-case archive churn
- pack throughput under broad changes
- how quickly archive-level deltas degrade toward full-size transfers

### `sdk_only`

Represents localized churn:

- only mutates a small region inside the dominant SDK

Use this when measuring:

- realistic repeated SDK updates
- long chain behavior when changes stay localized
- whether client apply time remains acceptable across many releases

## Scale Labels

The benchmark generator uses one calibrated large profile and scales down from there.

Recommended scale labels:

- `small = 0.05`
- `medium = 0.25`
- `large = 1.0`

Why:

- `large = 1.0` is the calibrated reference profile
- `medium = 0.25` is large enough to expose meaningful diff and archive behavior without full runtime cost
- `small = 0.05` is cheap enough for fast drift detection

## Repro Commands

### Microbench pack/archive drift

Small:

```bash
cargo run -p surge-bench --release -- --scale 0.05 --scenario full-release --zstd-levels 3 --skip-classic-diff --skip-installers --skip-update-scenario
```

Medium:

```bash
cargo run -p surge-bench --release -- --scale 0.25 --scenario full-release --zstd-levels 3 --skip-classic-diff --skip-installers --skip-update-scenario
```

Large:

```bash
cargo run -p surge-bench --release -- --scale 1.0 --scenario full-release --zstd-levels 3 --skip-classic-diff --skip-installers --skip-update-scenario
```

### Real update-manager chains

Localized long chain:

```bash
cargo run -p surge-bench --release -- --update-only --scale 1.0 --scenario sdk-only --num-deltas 100 --pack-zstd-level 3 --pack-memory-mb 256 --json
```

Broad churn chain:

```bash
cargo run -p surge-bench --release -- --update-only --scale 1.0 --scenario full-release --num-deltas 10 --pack-zstd-level 3 --pack-memory-mb 256 --json
```

## CI Tracking Guidance

Recommended CI benchmark coverage:

- small microbench, `full_release`
- medium microbench, `full_release`
- large microbench, `full_release`
- large localized update chain, `sdk_only`, `100` deltas
- large broad-churn update chain, `full_release`, `10` deltas

This combination tracks:

- archive/pack drift
- chunked diff drift
- real updater drift for long localized chains
- real updater drift for broad-churn chains

## Release KPI Coverage

The long-chain benchmark now also breaks out publisher-side release KPIs so CI can answer:

- how long the first full package takes to build
- how long later full packages take to rebuild
- how long delta artifacts take to build
- how long full and delta artifacts take to upload
- how expensive release-index updates are
- how large the resulting full, delta, installer, and download artifacts are

These are the numbers to watch when asking whether Surge is getting faster to publish, not just faster to patch.

## Installer KPI Coverage

CI also tracks a dedicated real installer scenario at medium scale:

- build an online console installer from a published release
- run that installer end to end against the filesystem backend
- build an offline console installer from the same release
- run that installer end to end with bundled payload

This is intentionally separate from the archive microbench so installer regressions are visible even when low-level zstd or diff timings look stable.
