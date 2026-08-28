#!/usr/bin/env bash
# Canonical benchmark for the delta surface (patch size + diff/apply cost).
#
# Score  = chunked-bsdiff patch bytes for the mutated dominant file
#          (lower is better; this is the field-bandwidth cost)
# Metric = chunked bsdiff + chunked bspatch durations in ms
#
# Knobs (env vars):
#   SCALE=0.25        payload scale (medium: diff behavior visible, loop is fast)
#   SCENARIO=sdk-only localized churn in the dominant SDK (realistic update shape)
#   LEVELS=3          zstd level for the archive sections of the microbench
#   BENCH_SEED=42     payload PRNG seed (do not change without re-baselining)
#   BENCH_RUNS=1      repeat the whole bench and record the median score
#   INCLUDE_CLASSIC=0 set 1 to also report classic (non-chunked) bsdiff as a
#                     reference metric (needs ~8x file size of RAM, slower)
#   CHUNK_MB=64       chunk size (MiB) for the chunked bsdiff/bspatch section
#   DIFF_THREADS=0    max threads for that section (0 = memory-aware auto)
set -euo pipefail
SURFACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SURFACE_DIR")"
source config.sh

SCALE="${SCALE:-0.25}"
SCENARIO="${SCENARIO:-sdk-only}"
LEVELS="${LEVELS:-3}"
CHUNK_MB="${CHUNK_MB:-64}"
DIFF_THREADS="${DIFF_THREADS:-0}"
INCLUDE_CLASSIC="${INCLUDE_CLASSIC:-0}"

ensure_bench_bin

declare -a diff_flags
if [ "$INCLUDE_CLASSIC" = "1" ]; then
  diff_flags=(--skip-classic-diff=false)
else
  diff_flags=(--skip-classic-diff)
fi

declare -a jsons=()
for _ in $(seq 1 "$BENCH_RUNS"); do
  json="$(mktemp)"
  "$BENCH_BIN" \
    --scale "$SCALE" \
    --scenario "$SCENARIO" \
    --zstd-levels "$LEVELS" \
    "${diff_flags[@]}" \
    --skip-installers \
    --skip-update-scenario \
    --chunk-mb "$CHUNK_MB" \
    --diff-threads "$DIFF_THREADS" \
    --seed "$BENCH_SEED" \
    --json >"$json"
  jsons+=("$json")
done
trap 'rm -f "${jsons[@]}"' EXIT

scores=()
for json in "${jsons[@]}"; do
  scores+=("$(bench_json "$json" "chunked bsdiff" output_size)")
done
# median of scores
score="$(printf '%s\n' "${scores[@]}" | sort -n | awk 'NR==int((NR+1)/2){print; exit}')"

diff_ms="$(bench_json "${jsons[0]}" "chunked bsdiff" duration)"
patch_ms="$(bench_json "${jsons[0]}" "chunked bspatch" duration)"
metric="diff_ms=${diff_ms} patch_ms=${patch_ms}"

if [ "$INCLUDE_CLASSIC" = "1" ]; then
  classic_bytes="$(bench_json "${jsons[0]}" "bsdiff" output_size)"
  classic_ms="$(bench_json "${jsons[0]}" "bsdiff" duration)"
  metric="$metric classic_bytes=${classic_bytes} classic_diff_ms=${classic_ms}"
fi

commit="$(bench_commit "${STATUS:-baseline}")"
desc="scale=${SCALE} scenario=${SCENARIO} chunk_mb=${CHUNK_MB} diff_threads=${DIFF_THREADS} seed=${BENCH_SEED} runs=${BENCH_RUNS}"
if [ "${#scores[@]}" -gt 1 ]; then
  desc="$desc scores=[${scores[*]}]"
fi

# STATUS=keep/discard + DESC="..." to log a finished experiment, otherwise
# this is a baseline row for the current tree.
append_result "$SURFACE_DIR/results.tsv" "$commit" "$score" "$metric" "${STATUS:-baseline}" "${DESC:-$desc}"
