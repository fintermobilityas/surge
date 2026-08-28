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

## Optimization Ideas

Ranked by expected value; read `results.tsv` and
`git log --oneline --all | grep -iE "dead end|autoresearch"` before
starting any of these.

1. **Chunk-size policy in `chunked_diff_options`.** The sweep shows the
   64 MiB microbench default is a local bytes optimum while production
   (256 MiB budget, many cores) clamps to 4 MiB and pays the byte tax.
   Measure the bytes/time knee at large scale (scale 1.0, ~40+ chunks)
   and tune `BYTES_PER_THREAD_FACTOR` / the 4 MiB clamp so the budget
   derivation lands on the knee instead of the floor. Keep the memory
   guardrail explicit.
2. **Patch compression level.** Patches are zstd-compressed at the pack
   level; patches are far more compressible than archives. A higher
   zstd level *on the patch only* may shrink download bytes at negligible
   edge apply cost (decompression is the cheap half).
3. **Parallelism/memory trade.** `max_threads` scales with the memory
   budget (`/12` per thread). The sweep showed diff time tracks the
   largest chunk, not total work — the time lever is chunk granularity
   under a memory cap, i.e. the same knee as idea 1.
4. **bsdiff C-backend tuning.** Suffix-array construction dominates
   classic bsdiff. Candidate: early-skip chunks whose content hashes are
   identical before diffing (needs a format version bump).
5. **Per-file strategy heuristics.** Sparse ops already diff per file;
   consider skipping the diff entirely for files below a threshold where
   a full-file entry in the sparse patch would be smaller (measure the
   overhead of the per-file patch header first).
6. **Alternative algorithms** (xdelta/vcdiff, binpatch, lz4-based
   rolling) as a fourth `PackDeltaStrategy`. Only after 1-5 show the
   bsdiff family is the ceiling; a new format is a large surface area
   (FFI, .NET, Kotlin are not involved, but restore/apply + tests are).

## Dead Ends to Respect

- **Chunk-size sweep 8/16/32/64/128 MiB** (scale 0.25 sdk-only, session
  of 4578166): no size beats 64 MiB on the score (patch bytes); smaller
  chunks trade bytes for diff time (+36% bytes / −82% time at 8 MiB).
  Do not re-sweep the same axis at this scale; the open question is the
  knee at large scale under the production memory budget (idea 1).
