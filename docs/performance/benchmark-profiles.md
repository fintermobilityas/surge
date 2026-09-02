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

- worst-case or near-worst-case file churn
- pack throughput under broad changes
- when sparse file-aware deltas should fall back to a full checkpoint

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

The microbench's chunked bsdiff/bspatch section also accepts:

- `--chunk-mb <MB>` — chunk size for the chunked diff (default 64)
- `--diff-threads <N>` — max threads for the chunked diff (default 0 =
  memory-aware auto)

The autoresearch delta surface (`auto/delta/`) sweeps these via the
`CHUNK_MB` / `DIFF_THREADS` env knobs in its `bench.sh`.

### Real update-manager chains

The update scenario's manifest takes its delta strategy from
`--pack-strategy` (default `sparse-file-ops`, the production default;
use `archive-chunked-bsdiff` to measure the archive-level fallback
shape). The `auto/update/` surface exposes it as the `STRATEGY` env
knob.

Set `BENCH_STEP_TIMING=1` to print one stderr line per applied delta
(`[step] items_done=N elapsed_ms=...`) for chain-apply profiling —
used to establish that per-delta apply cost is flat in chain depth
(`docs/performance/update-chains.md`).

Localized long chain:

```bash
cargo run -p surge-bench --release -- --update-only --scale 1.0 --scenario sdk-only --num-deltas 100 --pack-zstd-level 3 --pack-memory-mb 256 --json
```

Broad churn chain:

```bash
cargo run -p surge-bench --release -- --update-only --scale 1.0 --scenario full-release --num-deltas 10 --pack-zstd-level 3 --pack-memory-mb 256 --json
```

### Sparse delta build (publisher side)

The sparse `file-ops` delta builder compares the two packed archives
in memory: both zstd frames are decoded (concurrently, two threads),
entry contents are borrowed as zero-copy slices of the decoded tar
buffer, and the changed-file basis SHA-256 runs on a worker thread
while the chunked diff executes. No archive is extracted to disk.

- The in-memory build is byte-identical to the previous disk-based
  build; `in_memory_sparse_patch_is_byte_identical_to_disk_build`
  pins that equivalence and round-trips the patch against the next
  tree.
- Same-session A/B (10 deltas, `sdk_only` scale 1.0, seed 42):
  `Delta pack build (avg)` 4,163→2,187 ms/version (−47%); the 11-release
  publish total drops 93.3s→61.2s (−34%).
- The changed-file work is three independent CPU-bound passes over the
  same file (newer hash, basis hash, chunked diff); the two hashes run on
  worker threads while the diff runs on the calling thread (wall time is
  then dominated by the single-threaded hash passes, ~0.5 s for a 1 GB
  file at scale 1.0).
- Consecutive publishes can hand the decoded tree of the previous full
  archive to the next `PackBuilder`
  (`PackBuilder::with_sparse_tree_reuse` / `take_sparse_tree_reuse`,
  bound to the release's `full_sha256`; a mismatched hash degrades to a
  cold decode). On a 48-core host the cold decodes already overlap, so
  the reuse saves the remaining serial collect pass plus decode
  contention; the bench publish loop threads it through.
- The newer side is not decoded from the archive at all: within one
  `build()`, the full package is packed from the staging root first, and
  the sparse delta then walks that exact directory (same walk semantics
  and executable-bit overrides as the packer, contents read from the
  page cache) instead of re-decoding the zstd frame it just wrote.
  The patch bytes are identical to the archive-based build
  (`directory_newer_side_matches_archive_build` pins the equivalence);
  if a file changed between the full and delta build, the builder falls
  back to the archive-based path (the published bytes stay the source of
  truth).
- Cumulative publisher-side result on the canonical payload
  (scale 1.0, `sdk_only`, seed 42, same-session A/Bs): delta pack build
  4,163 ms/version (disk build) -> 2,187 (in-memory) -> 1,551 (tree
  reuse + parallel passes) -> ~1,540 (directory newer side; 10-delta
  A/B pairs 1,437/1,640 vs 1,789/2,148), i.e. -63% from the pre-round-11
  baseline. Remaining per-version floor: the staging-directory walk and
  page-cache read of the newer files (~0.3-0.4 s) plus the parallel
  hash/diff pass (~0.5 s). The single-threaded zstd decode remains on
  the client apply side (`extractor`), where MT decode would still pay
  off.
- Many changed files (broad churn) run their hash/diff pipelines across
  a bounded pool (cap 2, thread budget split evenly, largest files
  first so the cold-cache cost of a big chunked diff runs on the full
  budget before smaller files share the machine). The chunked diff
  output is independent of its thread count (per-chunk results are
  serialized by chunk index), so patch bytes are identical to the
  sequential build (`parallel_file_passes_produce_identical_payloads`).
  The single-file canonical shape is untouched (pool of one). Same-
  session A/B (scale 0.25, `full_release`, 10 deltas): 7,169/7,148 ->
  5,407/5,512 ms/version (-21 to -25%); the cap was measured, not
  picked: cap 4 and 8 make the cold first delta 2-4x slower (page-
  cache fill + random chunk access contend) and lose the average,
  while cap 2 never regresses the cold step.

## CI Tracking Guidance

Recommended CI benchmark coverage:

- small microbench, `full_release`
- medium microbench, `full_release`
- large microbench, `full_release`
- large localized update chain, `sdk_only`, `100` deltas
- large broad-churn update chain, `full_release`, `10` deltas

This combination tracks:

- sparse-delta pack drift
- per-file diff drift
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
