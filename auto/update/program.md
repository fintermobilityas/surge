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
- localized 100-delta chain at large scale (measured 2026-08-28):
  download ≈ 510 KiB, full-chain apply ≈ 489 s with the carried-tree
  walk (657 s before it; archive-chunked reference: 15.6 MiB / ~18 s)
  — `docs/performance/update-chains.md`
- **KEPT (this session): carried-tree chain apply.** The chain walker
  extracts the starting archive once and applies consecutive sparse
  deltas in place, keeping the per-step repack + full SHA-256 check.
  Measured: 100-delta apply 657 s → 489 s (−26%), 20-delta 128.7 s →
  100.7 s / 101.1 s (two runs, −22%), 0.25-scale 35.1 s → 28.2 s;
  download bytes and installed payload byte-identical (unit
  equivalence test + install-tree bench assertion). Per-step cost is
  now ~4.8 s, dominated by the file-ops phase (~4.3 s) — the repack
  + hash (~0.7 s) is what keeps per-step verification possible.
- per-delta apply cost is flat in chain depth (measured with
  `BENCH_STEP_TIMING=1`); per-step cost is CPU/IO-bound and ~3x
  host-load sensitive (an idle-host 2.2 s/delta reading was a load
  outlier, NOT a depth effect — see the correction history)
- broad churn is bounded by file-aware deltas + full fallback
- publisher cost for a 101-release chain ≈ 897 s under sparse
  (~337 s under archive-chunked); identity-chunk patches (v2) cut the
  same session's 100-delta publish to ~725 s (−14%) — retention
  policy still matters
- baseline (b397c0c, 48-core/251 GB machine, scale 0.05, 20 deltas):
  apply 8,569 ms, download 104,328 bytes, delta build 810 ms
- small-scale strategy reference: `archive-chunked-bsdiff` applied the
  identical 20-delta chain in 2,931 ms vs 8,526 ms for sparse (0.05
  scale, 40 MiB archives); the gap widens at large scale because
  sparse apply re-extracts/re-packs the full archive per step

Open questions from `docs/performance/update-chains.md`:

- long-history tuning for retained full checkpoints in real feeds
- when a broad-churn chain should force a fresh full checkpoint
- local checkpoint cache limits for very long-lived installs

## Optimization Ideas

Ranked by expected value; read `results.tsv` and
`git log --oneline --all | grep -iE "dead end|autoresearch"` first.

0. **KEPT — Identity-chunk chunked patches (format v2, `CSDF`).**
   Phase instrumentation of a 100-delta step showed the "ops" cost
   was ~3.5-5.0 s of chunked `bspatch` re-deriving 15 of 16
   unchanged 64 MiB chunks of the 1 GB file (the actual change: one
   4 KiB page). Format v2 adds a per-chunk identity bitset: the diff
   side skips bsdiff for identical chunks (memcmp instead of a
   suffix array), the apply side copies unchanged chunks straight
   through. v1 patches still apply; v2 patches fail closed on v1
   readers via the version byte. Same-session 100-delta A/B: apply
   480,505 → 223,108/218,318 ms (−54%, 2.2% spread), publish
   846,109 → 707,846/741,469 ms (−14%), download 522,171 →
   503,524 B (−3.6%, deterministic), install tree byte-identical.
1. **KEPT — Carried-tree chain apply (per-delta re-extract removed).**
   The 100-delta per-step profile showed each step paying extract
   (~1.0 s) + ops (~4.3 s) + repack (~0.6 s) + hash (~0.07 s). The
   walker now carries the extracted tree across consecutive sparse
   deltas: extract once, apply ops in place per step, repack +
   SHA-256-verify per step (verification semantics unchanged).
   Result: −22 to −26% apply across scales, payload byte-identical.
   **Remaining shape (not worth it today):** applying against the
   installed tree instead of rebuilding archives per step would also
   remove the per-step repack + hash (~0.7 s, ~14% of the step), but
   changes the apply flow and verification shape; with
   `max_chain_length` = 8 the absolute win is small.
2. **KEPT — Verified-hash carry across sparse chain steps.** The
   chain walker carries a `VerifiedFileHashes` map (path →
   verified sha256) through `apply_target_deltas`: the target hash
   verified at the end of step N is exactly step N+1's basis hash,
   so the redundant full-file basis re-read + re-hash is skipped
   (first step / mismatch still verify fully; post-patch target hash
   + per-step full-archive SHA-256 unchanged — a tamper-between-
   steps regression test keeps the fail-closed behavior).
   Same-session 100-delta A/B: apply 216,777 → 156,599/158,057 ms
   (−27.4%, 0.9% spread), per step ~2.2 s → ~1.6 s, download +
   install tree identical.
3. **KEPT — Streamed target hash into the bspatch write.** The
   per-step target SHA-256 was a separate full read after the bspatch
   write; `chunked_bspatch_file_with_progress_and_sha256` now returns
   the output hash computed while writing (additive API; the
   existing functions are untouched). The 0.55 s/step target-hash
   phase is eliminated, but the hashing CPU moves into the write
   loop, so the net is ~0.2-0.3 s/step. Phase-instrumented 10-step
   A/B (same session, 4 pairs): per step ~2,270 ms → ~1,980-2,060 ms
   in 3 of 4 pairs (the 4th was a load-spike window hitting both
   sides); 100-delta same-session medians ~167 s vs ~172 s — the
   gain sits near the shared-host wall-clock noise floor, which is
   why the phase-level evidence carries the claim. Payload and
   verification unchanged (tamper test still fail-closed).
4. **MEASURED / EXHAUSTED — Per-step repack variance.** Phase
   measurement (2026-09-01): the repack is the publisher-side pack and
   the client-side repack cost the same (~500-550 ms stable at
   zstd-3 MT-48 for the 1.2 GB bench tree; 0.6-3.0 s is host-load
   variance, not algorithmic waste). Reusing encoded frames for
   unchanged entries needs a per-entry archive format change (the
   archive is one tar + zstd stream, and client repack bytes must hash
   equal to publisher bytes) — a design change, not an experiment.
5. **KEPT — In-place sparse patching (same-size format v2).** A
   same-size format v2 patch rewrites only the changed chunks at their
   existing offsets; the target SHA-256 is one full read of the
   patched file (keeps the step fail-closed). Size-changing and
   format v1 patches use the temp-file flow. 10-step phase A/B:
   bspatch phase ~870-1,180 ms -> ~590-650 ms/step; 100-delta
   same-session: 147.0/161.5 s vs 163.1/170.1 s (-7.4%), install tree
   byte-identical.
6. **Verification cost (general).** SHA-256 over the full payload on
   the client is measured in the microbench (`SHA-256 (file)`).
   Sweep: verify-then-apply vs apply-then-verify ordering, streaming
   hashes during download, and whether the delta's embedded hashes
   let us skip re-hashing untouched files in sparse ops (the in-place
   target hash is now a full read — the next target).
3. **Checkpoint reuse.** Local cache retention (`keepFullCount: 1`)
   decides whether a chain walk rebuilds fulls from deltas or reuses a
   cached checkpoint. Measure the apply-time knee as a function of
   `keepFullCount` and chain distance from the nearest checkpoint.
4. **Chain walk planning cost.** `Update check` reads the compressed
   release index and walks the chain; for long histories the index
   grows unboundedly. Measure check time vs history length and tune
   `max_chain_length`/`checkpoint_every` against the large chain.
5. **Restore parallelism.** Sparse apply writes per-file patches; check
   whether patch application parallelizes safely across files (it may
   already) and whether the 256 MiB budget is the binding constraint on
   apply throughput for broad-churn deltas.
7. **Download overlap.** Whether verify can start before the full
   download lands (streaming hash) on the common delta path.

## Dead Ends to Respect

None recorded yet. Record every failed attempt here and in
`results.tsv` with the measured numbers.
