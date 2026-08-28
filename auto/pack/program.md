# Pack Surface

Autoresearch surface for full-pack build throughput and artifact size:
`ArchivePacker`, zstd compression policy, and the full-package build path
in `PackBuilder`.

## Why this surface matters

Every release ships a full package (install + restore baseline +
checkpoint fallback). The full pack is the publisher-side CI cost per
release and the fallback download cost on nodes. The delta surface owns
the incremental path; this surface owns everything that is not a delta.

## Scope

In scope:

- `crates/surge-core/src/archive/packer.rs` — directory packing, zstd
  compression (level, `zstdmt` worker count), file ordering and entry
  layout
- `crates/surge-core/src/archive/extractor.rs` — extract/restore cost
  (client-side unpack is part of update time; keep it from regressing)
- `crates/surge-core/src/pack/builder.rs` — full-pack build orchestration
  (workers, memory budget, deterministic output)
- `crates/surge-cli/src/commands/tune.rs` — `surge tune pack` candidate
  sweeps (tune stays explicit/opt-in; autoresearch findings may change
  which candidates it sweeps, not when it runs)

Out of scope:

- delta patch generation (see `auto/delta/`)
- chain/checkpoint retention policy (see `auto/update/`)

Hard constraints:

- **Determinism is load-bearing.** Rebuilding the same release must yield
  byte-identical archives (fingerprinted release-index entries depend on
  it). A keep that changes archive bytes without changing inputs is a
  discard, full stop.
- Archive bytes are a guardrail: at fixed inputs the score is build time,
  but a keep that grows the full package must justify the size/time
  tradeoff in the results.tsv description (it changes fallback download
  cost for the whole fleet).

## Baseline Setup

Canonical bench: `auto/pack/bench.sh`, the `surge-bench` microbench at
small scale with broad churn (worst-case file churn for the packer):

```bash
SCALE=0.05 SCENARIO=full-release ./bench.sh
```

- seed fixed at 42, `--zstd-levels 3` so the run is fast; the score only
  reads the `Archive create (zstd=3)` row
- small scale keeps one loop iteration to a few seconds; raise to
  `SCALE=0.25` when a candidate needs more signal (record it in the
  description)
- re-baseline in the same session; shared machine → `BENCH_RUNS=3`
  median when load is uncertain

Promotion gate before a keep lands: run the real pack path and confirm
`Full pack build (baseline)` / `(incremental avg)` held or improved and
the full artifact size did not grow beyond noise:

```bash
cargo run -p surge-bench --release -- --update-only --scale 0.25 \
  --scenario full-release --num-deltas 10 --pack-zstd-level 3 \
  --pack-memory-mb 256 --json
```

## Metric Definitions

- **score** = `Archive create (zstd=3)` duration in ms (lower is better)
- **metric** = archive output bytes, `Zstd compress (level=3)` ms,
  `Zstd decompress` ms, `Archive extract` ms

## Current State

- zstd level 3 default: measured fastest on the calibrated large profile
  and slightly smaller than level 1 (`docs/performance/pack-policy.md`)
- `zstdmt` (multithread zstd) with worker count from the pack budget
- deterministic file ordering; same inputs → same archive bytes
- operational node policy: all visible cores, 256 MiB budget
- baseline (b397c0c, 48-core/251 GB machine): `Archive create (zstd=3)`
  81.9 ms, 7,997,458-byte archive at scale 0.05 full-release (archive
  bytes were byte-identical across runs — determinism confirmed)

## Optimization Ideas

Ranked by expected value; read `results.tsv` and
`git log --oneline --all | grep -iE "dead end|autoresearch"` first.

1. **Worker-count knee.** `zstdmt` thread count is currently the whole
   budget. Sweep 1/2/4/8/all at small scale: diminishing returns and
   frame-sync overhead vary by payload shape; the 256 MiB budget may be
   leaving throughput on the table (or paying for nothing).
2. **File grouping by compressibility.** The packer compresses per entry
   at one level. Measuring a two-tier policy (cheap level for
   incompressible entries like large binaries, higher level for
   text-heavy entries) at fixed total bytes — only if the size/time
   tradeoff is defensible.
3. **Entry ordering for parallelism.** Confirm the add order does not
   serialize what could run concurrently; the packer's internal batching
   is the suspect, not zstd.
4. **Extract path.** `Archive extract` feeds restore/apply time; if a
   pack change trades a bit of build time for faster client unpack
   (same bytes), that is a candidate for the update surface's apply
   metric — cross-reference before keeping.

## Dead Ends to Respect

None recorded yet. Record every failed attempt here and in
`results.tsv` with the measured numbers.
