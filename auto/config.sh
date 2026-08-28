# Shared helpers for surge autoresearch surfaces.
# Source from each surface's bench.sh:
#   source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/config.sh"
#
# Every surface bench:
#   1. runs the canonical surge-bench invocation (one metric, fixed seed),
#   2. parses the score + secondary metrics from --json output,
#   3. appends one row to the surface results.tsv:
#        commit  score  <metric>  status  description
#
# Conventions:
#   - status is one of: baseline | keep | discard
#   - commit is the real short hash for rows tied to a landed change,
#     0000000 for experiments that did not land (discard)
#   - baseline rows carry the hash of the tree the baseline was measured on

SURGE_ROOT="$(git rev-parse --show-toplevel)"
BENCH_BIN="$SURGE_ROOT/target/release/surge-bench"
BENCH_SEED="${BENCH_SEED:-42}"
BENCH_RUNS="${BENCH_RUNS:-1}"

# Build surge-bench when the release binary is missing or older than the
# newest Rust/C source (diff backend changes live in vendor/bsdiff).
ensure_bench_bin() {
  local stale
  stale="$(find "$SURGE_ROOT/crates" "$SURGE_ROOT/vendor" \
    \( -name '*.rs' -o -name '*.c' -o -name '*.h' \) \
    -newer "$BENCH_BIN" -print -quit 2>/dev/null || true)"
  if [ ! -x "$BENCH_BIN" ] || [ -n "$stale" ]; then
    echo "[config] Building surge-bench (release)..." >&2
    (cd "$SURGE_ROOT" && RUSTFLAGS="-D warnings" cargo build --release -p surge-bench) >&2
  fi
}

# bench_commit [status] — commit field for a results.tsv row.
# Discards that did not land record 0000000.
bench_commit() {
  local status="${1:-}"
  if [ "$status" = "discard" ]; then
    printf '0000000'
  else
    git -C "$SURGE_ROOT" rev-parse --short HEAD
  fi
}

# bench_json <results.json> <result-name> <field>
# Fields: duration (ms), input_size, output_size
bench_json() {
  python3 - "$1" "$2" "$3" <<'PY'
import json
import sys

report = json.load(open(sys.argv[1]))
name, field = sys.argv[2], sys.argv[3]
for r in report["results"]:
    if r["name"] == name:
        print(r[field])
        break
else:
    sys.exit(f"result {name!r} not found in {sys.argv[1]}")
PY
}

# append_result <results.tsv> <commit> <score> <metric> <status> <description>
append_result() {
  local tsv="$1" commit="$2" score="$3" metric="$4" status="$5" desc="$6"
  printf '%s\t%s\t%s\t%s\t%s\n' "$commit" "$score" "$metric" "$status" "$desc" >>"$tsv"
  echo "logged: [$status] score=$score $metric — $desc"
}
