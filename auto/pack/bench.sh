#!/usr/bin/env bash
# Canonical benchmark for the pack surface (full-pack build throughput
# and artifact size).
#
# Score  = "Archive create (zstd=3)" duration in ms (publisher cost at
#          the default level, lower is better)
# Metric = archive output bytes (guardrail) + zstd compress/decompress +
#          extract durations in ms
#
# Knobs (env vars):
#   SCALE=0.05        payload scale (small: few-second loop)
#   SCENARIO=full-release  broad churn (worst-case file churn for the packer)
#   BENCH_SEED=42     payload PRNG seed (do not change without re-baselining)
#   BENCH_RUNS=1      repeat the whole bench and record the median score
#   LEVELS=3          zstd levels to sweep in the archive section
set -euo pipefail
SURFACE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(dirname "$SURFACE_DIR")"
source config.sh

SCALE="${SCALE:-0.05}"
SCENARIO="${SCENARIO:-full-release}"
LEVELS="${LEVELS:-3}"

ensure_bench_bin

declare -a jsons=()
for _ in $(seq 1 "$BENCH_RUNS"); do
  json="$(mktemp)"
  "$BENCH_BIN" \
    --scale "$SCALE" \
    --scenario "$SCENARIO" \
    --zstd-levels "$LEVELS" \
    --skip-classic-diff \
    --skip-installers \
    --skip-update-scenario \
    --seed "$BENCH_SEED" \
    --json >"$json"
  jsons+=("$json")
done
trap 'rm -f "${jsons[@]}"' EXIT

scores=()
for json in "${jsons[@]}"; do
  scores+=("$(bench_json "$json" "Archive create (zstd=3)" duration)")
done
# median of scores
score="$(printf '%s\n' "${scores[@]}" | sort -n | awk 'NR==int((NR+1)/2){print; exit}')"

first="${jsons[0]}"
archive_bytes="$(bench_json "$first" "Archive create (zstd=3)" output_size)"
zstd_ms="$(bench_json "$first" "Zstd compress (level=3)" duration)"
unzstd_ms="$(bench_json "$first" "Zstd decompress" duration)"
extract_ms="$(bench_json "$first" "Archive extract" duration)"
metric="archive_bytes=${archive_bytes} zstd_ms=${zstd_ms} unzstd_ms=${unzstd_ms} extract_ms=${extract_ms}"

commit="$(bench_commit "${STATUS:-baseline}")"
desc="scale=${SCALE} scenario=${SCENARIO} seed=${BENCH_SEED} runs=${BENCH_RUNS}"
if [ "${#scores[@]}" -gt 1 ]; then
  desc="$desc scores=[${scores[*]}]"
fi

# STATUS=keep/discard + DESC="..." to log a finished experiment, otherwise
# this is a baseline row for the current tree.
append_result "$SURFACE_DIR/results.tsv" "$commit" "$score" "$metric" "${STATUS:-baseline}" "${DESC:-$desc}"
