# Update Surface

Autoresearch surface for end-to-end client update cost: the real
`UpdateManager` flow (check → download → verify → apply) across a delta
chain.

## Why this surface matters

This is the number the fleet feels: bytes a node downloads over the
field network, and how long the node is busy applying (edge CPUs are
weak; an update that takes 20 minutes blocks the lane). Chain and
checkpoint policy decides both. The delta and pack surfaces optimize the
parts; this surface measures the whole and owns the policy knobs that
shape it.

## Scope

In scope:

- `crates/surge-core/src/update/manager/` — chain selection, download,
  restore/apply, verification, progress
- `crates/surge-core/src/releases/restore*` + `artifact_cache.rs` —
  checkpoint reuse, local cache retention
- `crates/surge-core/src/releases/delta/mod.rs` — chain walk and
  apply-ladder logic
- policy knobs that shape the client path: `max_chain_length`,
  `checkpoint_every`, `keep_latest_fulls`, local full-count retention

Out of scope:

- per-file diff algorithm cost (see `auto/delta/`)
- full-pack build cost (see `auto/pack/`)
- publisher-side publish timing (tracked as a metric, not the score)

## Baseline Setup

Canonical bench: `auto/update/bench.sh`, the real update scenario at
small scale — small enough for a fast loop, real enough to exercise
index lookup, chain selection, download, verify, and apply:

```bash
SCALE=0.05 SCENARIO=sdk-only NUM_DELTAS=20 ./bench.sh
```

- seed fixed at 42; the scenario publishes `NUM_DELTAS` real releases
  through the pack builder to a filesystem store, then runs one real
  `UpdateManager` check + `download_and_apply` from the first version
- the generated bench manifest pins `strategy: sparse-file-ops` (the
  production default) and an app-scoped storage prefix (post-#79
  contract: the release index lives on `<prefix>/<app_id>`)
- the scenario asserts the final install tree matches the last release —
  a keep that changes the applied payload fails the bench
- re-baseline in the same session; this bench is IO-bound on the local
  disk, so serial runs only (no parallel GPU/IO work)

Promotion gate before a keep lands: the large localized chain is the
reference for long-lived installs (numbers live in
`docs/performance/update-chains.md`):

```bash
cargo run -p surge-bench --release -- --update-only --scale 1.0 \
  --scenario sdk-only --num-deltas 100 --pack-zstd-level 3 \
  --pack-memory-mb 256 --json
```

Confirm download bytes and apply time held or improved and the install
tree still matches.

## Metric Definitions

- **score** = `Update apply (N deltas)` duration in ms (client wall time
  for check→download→verify→apply, lower is better)
- **metric** = `Update check (N deltas)` `output_size` (planned download
  bytes), `Delta pack build (avg)` ms, `Full pack build (incremental
  avg)` ms, `Release index update (avg)` ms

## Current State

- sparse file-ops deltas, zstd 3, `max_chain_length` 8,
  `checkpoint_every` 10, keep 2 latest fulls (pack-policy defaults)
- localized 100-delta chain at large scale: download ≈ 15.6 MiB, apply
  ≈ 18 s — acceptable (`docs/performance/update-chains.md`)
- broad churn is bounded by file-aware deltas + full fallback
- publisher cost for a 101-release chain ≈ 337 s — retention policy
  still matters
- baseline (b397c0c, 48-core/251 GB machine, scale 0.05, 20 deltas):
  apply 8,569 ms, download 104,328 bytes, delta build 810 ms
- same-session reference: the `archive-chunked-bsdiff` strategy applied
  the identical chain in 2,931 ms (98,714 bytes) — per-file sparse apply
  is ~2.9x slower at this small scale; verify at large scale before
  drawing conclusions (per-file patches are smaller there)

Open questions from `docs/performance/update-chains.md`:

- long-history tuning for retained full checkpoints in real feeds
- when a broad-churn chain should force a fresh full checkpoint
- local checkpoint cache limits for very long-lived installs

## Optimization Ideas

Ranked by expected value; read `results.tsv` and
`git log --oneline --all | grep -iE "dead end|autoresearch"` first.

1. **Verification cost.** SHA-256 over the full payload on the client is
   measured in the microbench (`SHA-256 (file)`). Sweep: verify-then-
   apply vs apply-then-verify ordering, streaming hashes during
   download, and whether the delta's embedded hashes let us skip
   re-hashing untouched files in sparse ops.
2. **Checkpoint reuse.** Local cache retention (`keepFullCount: 1`)
   decides whether a chain walk rebuilds fulls from deltas or reuses a
   cached checkpoint. Measure the apply-time knee as a function of
   `keepFullCount` and chain distance from the nearest checkpoint.
3. **Chain walk planning cost.** `Update check` reads the compressed
   release index and walks the chain; for long histories the index
   grows unboundedly. Measure check time vs history length and tune
   `max_chain_length`/`checkpoint_every` against the large chain.
4. **Restore parallelism.** Sparse apply writes per-file patches; check
   whether patch application parallelizes safely across files (it may
   already) and whether the 256 MiB budget is the binding constraint on
   apply throughput for broad-churn deltas.
5. **Download overlap.** Whether verify can start before the full
   download lands (streaming hash) on the common delta path.

## Dead Ends to Respect

None recorded yet. Record every failed attempt here and in
`results.tsv` with the measured numbers.
