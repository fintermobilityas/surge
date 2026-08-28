#!/usr/bin/env bash
# Canonical benchmark for the update surface (end-to-end client update
# cost across a real delta chain).
#
# Score  = "Update apply (N deltas)" duration in ms: client wall time for
#          check -> download -> verify -> apply (lower is better)
# Metric = planned download bytes + publisher-side build/index timings
#
# The scenario publishes NUM_DELTAS real releases through the pack
# builder to a filesystem store, then runs one real UpdateManager
# check + download_and_apply and asserts the install tree matches the
# final release.
#
# Knobs (env vars):
#   SCALE=0.05        payload scale (small: fast loop, real UpdateManager)
#   SCENARIO=sdk-only localized churn (long-lived-install shape)
#   NUM_DELTAS=20     chain length the single update walks
#   STRATEGY=sparse-file-ops  delta strategy in the generated manifest
#                     (production default; set archive-chunked-bsdiff to
#                     measure the archive-level fallback shape)
#   BENCH_SEED=42     payload PRNG seed (do not change without re-baselining)
#   BENCH_RUNS=1      repeat the whole bench and record the median score
set -euo pipefail
SURFACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SURFACE_DIR")"
source config.sh

SCALE="${SCALE:-0.05}"
SCENARIO="${SCENARIO:-sdk-only}"
NUM_DELTAS="${NUM_DELTAS:-20}"
STRATEGY="${STRATEGY:-sparse-file-ops}"
PACK_LEVEL="${PACK_LEVEL:-3}"
PACK_MEMORY_MB="${PACK_MEMORY_MB:-256}"

ensure_bench_bin

declare -a jsons=()
for _ in $(seq 1 "$BENCH_RUNS"); do
  json="$(mktemp)"
  "$BENCH_BIN" \
    --update-only \
    --scale "$SCALE" \
    --scenario "$SCENARIO" \
    --num-deltas "$NUM_DELTAS" \
    --pack-zstd-level "$PACK_LEVEL" \
    --pack-memory-mb "$PACK_MEMORY_MB" \
    --pack-strategy "$STRATEGY" \
    --seed "$BENCH_SEED" \
    --json >"$json"
  jsons+=("$json")
done
trap 'rm -f "${jsons[@]}"' EXIT

scores=()
for json in "${jsons[@]}"; do
  scores+=("$(bench_json "$json" "Update apply (${NUM_DELTAS} deltas)" duration)")
done
# median of scores
score="$(printf '%s\n' "${scores[@]}" | sort -n | awk 'NR==int((NR+1)/2){print; exit}')"

first="${jsons[0]}"
download_bytes="$(bench_json "$first" "Update check (${NUM_DELTAS} deltas)" output_size)"
delta_build_ms="$(bench_json "$first" "Delta pack build (avg)" duration)"
index_ms="$(bench_json "$first" "Release index update (avg)" duration)"
metric="download_bytes=${download_bytes} delta_build_ms=${delta_build_ms} index_ms=${index_ms}"

commit="$(bench_commit "${STATUS:-baseline}")"
desc="scale=${SCALE} scenario=${SCENARIO} num_deltas=${NUM_DELTAS} strategy=${STRATEGY} seed=${BENCH_SEED} runs=${BENCH_RUNS}"
if [ "${#scores[@]}" -gt 1 ]; then
  desc="$desc scores=[${scores[*]}]"
fi

# STATUS=keep/discard + DESC="..." to log a finished experiment, otherwise
# this is a baseline row for the current tree.
append_result "$SURFACE_DIR/results.tsv" "$commit" "$score" "$metric" "${STATUS:-baseline}" "${DESC:-$desc}"
