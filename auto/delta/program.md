# Delta Surface

Autoresearch surface for delta generation cost and size: bsdiff / chunked-
bsdiff, chunk sizing, and the compression applied to patches. This is the
canonical surface when the user says "autoresearch" without further
qualification.

## Why this surface matters

Surge ships updates to edge devices (camera units, payment terminals,
access pads) over constrained field networks. The delta patch is the
dominant cost on both ends:

- publisher side: diff build time inside `surge pack` (CI cost per release)
- client side: bytes downloaded + `bspatch` apply time on weak edge CPUs

The installed result must never change: post-update install trees are
byte-identical across any keep.

## Scope

In scope:

- `crates/surge-core/src/diff/` — classic bsdiff backend (`wrapper.rs`,
  `bsdiff_sys.rs`) and chunked bsdiff (`chunked.rs`, 64 MiB default chunks,
  memory-aware thread count)
- `crates/surge-core/vendor/` + `vendor/bsdiff` — the C bsdiff/bspatch
  implementation (regenerate the publishable snapshot with
  `./scripts/sync-surge-core-vendor.sh` when touching `vendor/bsdiff`)
- `crates/surge-core/src/releases/delta/` — sparse file-ops patching
  (per-file diff, the `sparse-file-ops` default) and archive-level
  chunked/bsdiff patches
- `crates/surge-core/src/pack/builder/delta.rs` — strategy selection,
  chunk-size derivation from the memory budget (256 MiB default), and the
  delta-vs-full fallback (`delta_size >= full_size` → publish full)

Out of scope:

- full-pack build cost (see `auto/pack/`)
- end-to-end chain/checkpoint policy (see `auto/update/`)
- anything that changes release-index semantics or installed payloads

## Baseline Setup

The canonical bench is `auto/delta/bench.sh`, a `surge-bench` microbench at
medium scale with localized churn (the realistic repeated-update shape):

```bash
SCALE=0.25 SCENARIO=sdk-only ./bench.sh
```

- seed fixed at 42 (`--seed`), payloads generated identically every run
- `--scenario sdk-only` mutates only a region of the dominant SDK file,
  matching `sdk_only` in `docs/performance/benchmark-profiles.md`
- the microbench diff section diffs the **full v1→v2 archive byte
  stream** (the `ArchiveChunkedBsdiff` shape — archives are more
  realistic than raw files, and it is the delta fallback path). The
  production default per-file `sparse-file-ops` patching is measured
  end to end by the update surface (`auto/update/`), which is also this
  surface's promotion gate.
- chunk size and diff threads are knobs: `CHUNK_MB` (default 64) and
  `DIFF_THREADS` (default 0 = memory-aware auto) pass through to
  `--chunk-mb` / `--diff-threads` on `surge-bench`.
- classic (non-chunked) bsdiff is skipped by default: it needs ~8x file
  size of RAM and is not the production default path. Set
  `INCLUDE_CLASSIC=1` to include it as a reference metric.

Re-baseline the reference config in the same session before claiming any
delta. The bench machine is a shared box; when load is uncertain, use
`BENCH_RUNS=3` and record the median in the description.

## Metric Definitions

- **score** = `chunked bsdiff` `output_size` (archive patch bytes,
  lower is better) for the v1→v2 archive at `SCALE`/`SCENARIO`
- **metric** = `chunked bsdiff` + `chunked bspatch` + `bsdiff` durations
  in ms, plus classic patch bytes when `INCLUDE_CLASSIC=1`

Promotion gate before a keep lands: rerun the full validation scenario
(large localized chain through the real `UpdateManager`) and confirm the
applied install tree still matches and the real-chain download/apply
numbers improved or held:

```bash
cargo run -p surge-bench --release -- --update-only --scale 1.0 \
  --scenario sdk-only --num-deltas 100 --pack-zstd-level 3 \
  --pack-memory-mb 256 --json
```

## Current State

- production default strategy: `sparse-file-ops` (per-file chunked bsdiff,
  zstd level 3, 256 MiB diff budget)
- the sparse delta builder compares archives in memory (concurrent
  zstd decode, zero-copy offset-based entry slices, changed-file
  hashes on worker threads parallel to the chunked diff); no disk
  extraction. Byte-identical to the previous disk-based build
  (equivalence test in
  `crates/surge-core/src/releases/delta/tests.rs`). Cross-step decoded
  tree reuse across publishes via `SparseTreeReuse` (SHA-256-guarded).
  Cumulative same-session A/B: Delta pack build 4,163 -> 2,187
  (in-memory) -> 1,551 ms/version (reuse + parallel passes), -63%
- chunk size derived per pack from the memory budget: `per_thread / 12`,
  clamped to `[4 MiB, 64 MiB]`
- fallback: patch >= full package → publish a full checkpoint instead
- zstd level 3 was measured fastest on the calibrated large profile
  (see `docs/performance/pack-policy.md`)
- baseline (4578166, 48-core/251 GB machine): 42,595-byte patch for the
  v1→v2 archive at scale 0.25 sdk-only; chunked bsdiff 2,053 ms, chunked
  bspatch 199 ms (chunk 64 MiB, threads auto)
- chunk-size sweep (8/16/32/64/128 MiB, same session): bytes are flat at
  64 and 128 (single-chunk regime, archive ≈ 40 MiB at this scale) and
  **grow as chunks shrink** (58,145 B at 8 MiB, +36%) while diff time
  **falls** (389 ms at 8 MiB, −82%). Cause: the zstd-compressed archive
  is a chained stream — a localized mutation changes bytes from the
  mutation point to the end, so smaller chunks both parallelize the
  suffix-array build and cut cross-chunk copy opportunities, which bsdiff
  would otherwise exploit
- production does NOT run the 64 MiB default: `chunked_diff_options`
  derives the chunk from the memory budget, so the operational 256 MiB
  budget on a 48-core publisher clamps to 4 MiB — the byte-tax regime
  the sweep measured, but at ~40 chunks on a large-scale archive
- large-scale knee (scale 1.0 sdk-only, same session): patch bytes fall
  monotonically with chunk size — 4 MiB 277,540 B (+76% vs 64), 16 MiB
  181,149 (+15%), 32 MiB 165,419 (+5%), 64 MiB 157,939, 256 MiB 150,284
  (−4.8%, asymptote). Diff time falls the other way (375 ms → 9,874 ms).
  The byte knee sits at ~64 MiB; the 256 MiB budget's 4 MiB clamp sits
  on the byte-curve floor. Publisher-side diff time is CI minutes, not
  fleet cost — the fleet pays the bytes on every node
- the knee is archive-level: the production default `sparse-file-ops`
  diffs raw files, where the byte tax does not exist (no chained zstd
  stream inside a file). Measured: the knee-first derivation (21.3 MiB ×
  1 thread at 256 MiB/48) vs current (4 MiB × 48) on the real update
  chain at scale 0.25 — download bytes +0.4% (104,689 vs 104,305),
  delta build 4.7x slower (7,918 vs 1,689 ms), client apply +23%
  (45,170 vs 36,690 ms). The current floor + full-parallel regime wins
  on both metrics for the default strategy

## Optimization Ideas

Ranked by expected value; read `results.tsv` and
`git log --oneline --all | grep -iE "dead end|autoresearch"` before
starting any of these.

1. **MT zstd decode (apply side).** The PUBLISHER no longer decodes the
   newer archive (round 13: it walks the staging directory it just
   packed), but the per-VERSION floor on the client apply side is still
   one single-threaded zstd decode of the downloaded full archive
   (`extractor`); the publisher still cold-decodes the OLDER archive on
   a cache miss (reuse hit = no decode). zstd-rs 0.13 has no MT decode
   API; the frames the publisher writes ARE MT-encoded (48 workers in
   the bench), so an upgrade or raw-FFI `ZSTD_decompressMultiFrame`-
   style path would help the apply side. Measure before building: is
   decode still the top phase at the production payload shape (real
   native SDKs compress ~2-3:1, not the bench's 7:1)?

2026-09-03 (round 16, update-surface change): CSDF v3 chunk-target
digests skip the client's full-file target re-read. Publisher side is
flat: the per-chunk target digests are computed in the diff workers,
which already hold each changed chunk's target content in memory (no
extra pass, no extra I/O). Same-session delta pack build: 1,343/1,349
vs 1,348/1,343 ms/version (noise). Client 100-delta apply -87%/-85%
(see auto/update/results.tsv).
2026-09-02 (round 15): per-file hash/diff pipelines of the sparse
delta build run across a bounded pool (cap 2, budget split evenly,
largest files first). Same-session interleaved A/B (scale 0.25,
full-release, 10 deltas): 7,022/6,935 -> 4,302/4,288 ms/version
(-38%); sdk-only canonical unchanged (pool of one); patch bytes
identical across parallelism levels
(`parallel_file_passes_produce_identical_payloads`). Cap 4/8 rejected
after measurement: the cold first delta is 2-4x slower (concurrent
random-access bsdiff while the page cache fills), which loses the
10-delta average (cap 4 interleaved: 5,407/5,512 = -21 to -25%; cap 8
solo: 9,332 ms). Footgun: an early draft accidentally carried the
cap-4 constant into the final A/B (docs said cap 2) - caught in
review, corrected; always check the constant against the claim.
2026-09-02 (round 13): newer side of the publisher delta build from the
packed staging directory (canonical pack root kept alive on the
`PackBuilder`; same walk + executable-bit overrides as the packer;
contents read from the page cache). Same-session A/B (10 deltas,
sdk-only 1.0, seed 42): `Delta pack build (avg)` 1,789/2,148 ->
1,437/1,640 ms/version (about -20 to -24%, new < base both pairs).
Byte-identical to the archive-based build
(`directory_newer_side_matches_archive_build`); on directory drift
between the full and delta build the builder falls back to the
archive-based path. Per-version profile: staging walk + page-cache
read ~0.3-0.4 s + parallel hash/diff pass ~0.5 s.
2. **DONE (round 12a) — Cross-step decoded-tree reuse.**
   `SparseTreeReuse` (decoded tar buffer + offset-based entry map,
   bound to the archive SHA-256) is handed from one publish to the next
   via `PackBuilder::with_sparse_tree_reuse` /
   `take_sparse_tree_reuse`; the bench publish loop threads it through.
   Measured: only ~0.05-0.15 s/version on the 48-core bench host —
   the two cold decodes already overlapped under round 11's parallel
   decode, so the reuse mainly saves the collect pass and decode
   contention. Kept: correct under checkpoint-full fallbacks and
   rebuilt packages (sha guard fails closed to a cold decode) and worth
   more on loaded publisher CI.
3. **DONE (round 12b) — Parallel changed-file passes.** The changed
   file needs three independent CPU passes (newer SHA-256, basis
   SHA-256, chunked diff). Both hashes now run on worker threads while
   the diff runs on the calling thread: ops phase 970 -> 495 ms
   (scale 1.0, 1.06 GB file). Same-session A/B (10 deltas, 2 pairs):
   delta pack build 2,142/2,158 -> 1,551/1,586 ms/version (-27.6%);
   100-delta: 1,619 ms avg, publish 536.9 s, install tree
   1,214,024,073 B identical. Cumulative vs the pre-round-11 disk
   build: -63%.
4. **DONE (round 11) — In-memory sparse delta build.** Replaced the
   extract-to-disk + walk comparison with in-memory decode (two
   archives decoded concurrently), zero-copy entry slices, and the
   basis hash on a worker thread while the chunked diff runs.
   Byte-identical to the disk build (equivalence test). Same-session
   A/B (10 deltas, sdk_only 1.0, seed 42): Delta pack build
   4,163->2,187 ms/version (-47%); publish 93.3s->61.2s (-34%).
   Remaining per-version floor: single-threaded zstd decode x2
   (~0.9 s wall) + full-file SHA-256 passes (~0.6 s).
5. **CLOSED — Knee-first chunking for the archive fallback only.**
   Measured through the real update chain at scale 1.0 / 20 deltas /
   `archive-chunked-bsdiff` / 256 MiB budget on 48 cores: wire bytes
   flat (+0.4%, 98,981 vs 98,597 B), 12x slower delta build (34,135 vs
   2,822 ms), apply flat. The raw chunk-boundary byte tax (277 KB vs
   158 KB patch) is redundant copy/seek structure that zstd crushes in
   the packed pipeline — the microbench's raw-byte score overstates it.
   Wire bytes are flat across 4-64 MiB chunks, so chunk policy is a
   pure time lever and the current floor + parallel derivation is
   correct as-is for both strategies. Do not retry either knee variant.
6. **CLOSED — Patch compression level.** Measured on 20 real
   sparse-file-ops patch documents (scale 1.0 sdk-only): wire bytes
   L9 −0.09% / L19 −0.97% vs L3, with L19 encode +38%. The
   compressible part of an SFD1 patch is JSON structure, which L3
   already crushes; the payload is bsdiff patch data with no level
   headroom. Not worth a manifest/pack-policy knob. **With ideas 1-2
   closed, the wire-byte axis (chunk size, zstd level) is exhausted
   for this payload shape** — remaining headroom is format-level
   (ideas 8-10) or a different payload shape (broad churn, many large
   changed files). The large-scale validation owed to the update
   surface is the 100-delta sparse chain rerun (see
   `docs/performance/update-chains.md`, "When To Rerun").
7. **Parallelism/memory trade.** Subsumed by idea 1: the sweep showed
   diff time tracks the largest chunk under the thread count the budget
   allows, so the lever is the (chunk, threads) pair, not threads alone.
8. **bsdiff C-backend tuning.** Suffix-array construction dominates
   classic bsdiff. Candidate: early-skip chunks whose content hashes are
   identical before diffing (needs a format version bump).
9. **Per-file strategy heuristics.** Sparse ops already diff per file;
   consider skipping the diff entirely for files below a threshold where
   a full-file entry in the sparse patch would be smaller (measure the
   overhead of the per-file patch header first).
10. **Alternative algorithms** (xdelta/vcdiff, binpatch, lz4-based
   rolling) as a fourth `PackDeltaStrategy`. Only after 1-5 show the
   bsdiff family is the ceiling; a new format is a large surface area
   (FFI, .NET, Kotlin are not involved, but restore/apply + tests are).

## Dead Ends to Respect

- **Chunk-size sweep 8/16/32/64/128 MiB** (scale 0.25 sdk-only, session
  of 4578166): no size beats 64 MiB on the score (patch bytes); smaller
  chunks trade bytes for diff time (+36% bytes / −82% time at 8 MiB).
- **Chunk-size sweep 4/8/16/32/64/128/256 MiB** (scale 1.0 sdk-only,
  same session): bytes monotonic in chunk size, knee at ~64 MiB, 4 MiB
  (the production clamp) is the byte-curve floor at +76%. Do not
  re-sweep the axis; the open question is policy (idea 1): who gets
  which (chunk, threads) pair under which budget.
- **Knee-first `chunked_diff_options` for all strategies** (scale 0.25
  real update chain, 20 deltas, seed 42): byte-neutral (+0.4%),
  4.7x slower delta build, +23% client apply vs the current
  floor + full-parallel regime. The archive-level byte tax does not
  transfer to raw per-file sparse diffs.
- **Knee-first scoped to `ArchiveChunkedBsdiff`** (scale 1.0 real
  update chain, 20 deltas, seed 42, strategy `archive-chunked-bsdiff`):
  wire bytes flat (+0.4%) and 12x slower delta build. The raw
  chunk-boundary tax is masked by zstd in the packed pipeline; no
  chunk size in 4-64 MiB moves download bytes. Both knee variants are
  closed — do not retry.
- **Metric caution:** the microbench score here is RAW patch bytes.
  Wire cost is the zstd-compressed delta measured by the update
  surface's `download_bytes`; the two diverge strongly for bsdiff
  patches (redundant copy/seek structure compresses ~40:1). Always
  gate on the update surface before treating raw bytes as the field
  metric.
