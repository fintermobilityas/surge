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
- chunk size derived per pack from the memory budget: `per_thread / 12`,
  clamped to `[4 MiB, 64 MiB]`
- fallback: patch >= full package → publish a full checkpoint instead
- zstd level 3 was measured fastest on the calibrated large profile
  (see `docs/performance/pack-policy.md`)
- baseline (b397c0c, 48-core/251 GB machine): 42,595-byte patch for the
  v1→v2 archive at scale 0.25 sdk-only; chunked bsdiff 2,247 ms, chunked
  bspatch 201 ms

## Optimization Ideas

Ranked by expected value; read `results.tsv` and
`git log --oneline --all | grep -iE "dead end|autoresearch"` before
starting any of these.

1. **Chunk-size sweep.** The 64 MiB default (and the budget-derived clamp)
   is never tuned per workload. Sweep 8/16/32/64/128 MiB for both the
   dominant-SDK and broad-churn shapes; chunk boundaries determine which
   unchanged regions diff cleanly.
2. **Patch compression level.** Patches are zstd-compressed at the pack
   level; patches are far more compressible than archives. A higher
   zstd level *on the patch only* may shrink download bytes at negligible
   edge apply cost (decompression is the cheap half).
3. **Parallelism/memory trade.** `max_threads` scales with the memory
   budget (`/12` per thread). Measure the time/bytes knee; the edge
   publisher (CI) and the edge node have very different budgets.
4. **bsdiff C-backend tuning.** Suffix-array construction dominates
   classic bsdiff. Candidate: smaller rolling hash / block-restricted
   matching for the localized-churn shape where most chunks are
   unchanged (early-skip chunks whose hashes are identical before
   diffing — needs a format version bump).
5. **Per-file strategy heuristics.** Sparse ops already diff per file;
   consider skipping the diff entirely for files below a threshold where
   a full-file entry in the sparse patch would be smaller (measure the
   overhead of the per-file patch header first).
6. **Alternative algorithms** (xdelta/vcdiff, binpatch, lz4-based
   rolling) as a fourth `PackDeltaStrategy`. Only after 1-5 show the
   bsdiff family is the ceiling; a new format is a large surface area
   (FFI, .NET, Kotlin are not involved, but restore/apply + tests are).

## Dead Ends to Respect

None recorded yet. Record every failed attempt here (and in
`results.tsv`) with the measured numbers so the next agent does not
re-test it.
