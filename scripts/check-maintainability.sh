#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOFT_MAX_LINES=600
BASELINE_FILE="$ROOT_DIR/docs/architecture/maintainability-baseline.txt"
status=0

declare -A baseline_limits=()
declare -A baseline_seen=()

run_self_tests() {
  local tmpdir
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' RETURN

  cat >"$tmpdir/inline-tests-at-end.rs" <<'EOF'
fn alpha() {}

#[cfg(test)]
mod tests {
    #[test]
    fn works() {}
}
EOF

  cat >"$tmpdir/test_helpers_before_prod.rs" <<'EOF'
#[cfg(test)]
fn helper() {}

fn alpha() {}

#[cfg(test)]
mod tests {
    #[test]
    fn works() {}
}
EOF

  local measured
  measured="$(measure_production_lines "$tmpdir/inline-tests-at-end.rs")"
  if [[ "$measured" != "3" ]]; then
    printf 'maintainability self-test failed: expected 3 lines before inline tests, got %s\n' "$measured" >&2
    status=1
  fi

  measured="$(measure_production_lines "$tmpdir/test_helpers_before_prod.rs")"
  if [[ "$measured" != "6" ]]; then
    printf 'maintainability self-test failed: expected 6 lines including helper spacing before inline tests, got %s\n' \
      "$measured" >&2
    status=1
  fi
}

measure_production_lines() {
  local file="$1"
  awk '
    function flush_pending_attr() {
      if (pending_test_attr) {
        count += 1
        pending_test_attr = 0
      }
    }

    /^[[:space:]]*#\[cfg\(test\)\][[:space:]]*$/ {
      flush_pending_attr()
      pending_test_attr = 1
      next
    }

    pending_test_attr && /^[[:space:]]*(\/\/.*)?$/ {
      next
    }

    pending_test_attr && /^[[:space:]]*mod[[:space:]]+tests[[:space:]]*\{/ {
      exit
    }

    {
      flush_pending_attr()
      count += 1
    }

    END {
      flush_pending_attr()
      print count + 0
    }
  ' "$file"
}

load_baseline() {
  if [[ ! -f "$BASELINE_FILE" ]]; then
    printf 'maintainability check misconfigured: missing baseline file %s\n' "${BASELINE_FILE#"$ROOT_DIR"/}" >&2
    status=1
    return
  fi

  local line limit path
  while IFS= read -r line; do
    [[ -z "$line" || "$line" =~ ^# ]] && continue
    limit="${line%% *}"
    path="${line#* }"

    if [[ -z "$limit" || -z "$path" || "$limit" == "$line" || ! "$limit" =~ ^[0-9]+$ ]]; then
      printf 'maintainability check misconfigured: invalid baseline entry "%s"\n' "$line" >&2
      status=1
      continue
    fi

    baseline_limits["$path"]="$limit"
  done <"$BASELINE_FILE"
}

check_file_length() {
  local file="$1"
  local rel_path lines baseline_limit

  rel_path="${file#"$ROOT_DIR"/}"
  lines="$(measure_production_lines "$file")"

  if (( lines <= SOFT_MAX_LINES )); then
    return
  fi

  baseline_limit="${baseline_limits[$rel_path]-}"
  if [[ -z "$baseline_limit" ]]; then
    printf 'maintainability regression: %s has %d production lines and is above the %d-line target without a baseline entry\n' \
      "$rel_path" "$lines" "$SOFT_MAX_LINES" >&2
    status=1
    return
  fi

  baseline_seen["$rel_path"]=1
  if (( lines > baseline_limit )); then
    printf 'maintainability regression: %s grew from %d to %d production lines (target: %d)\n' \
      "$rel_path" "$baseline_limit" "$lines" "$SOFT_MAX_LINES" >&2
    status=1
  fi
}

report_stale_baseline_entries() {
  local rel_path file lines

  for rel_path in "${!baseline_limits[@]}"; do
    if [[ -n "${baseline_seen[$rel_path]-}" ]]; then
      continue
    fi

    file="$ROOT_DIR/$rel_path"
    if [[ ! -f "$file" ]]; then
      printf 'maintainability note: baseline entry %s no longer exists; update %s\n' \
        "$rel_path" "${BASELINE_FILE#"$ROOT_DIR"/}" >&2
      continue
    fi

    lines="$(measure_production_lines "$file")"
    if (( lines <= SOFT_MAX_LINES )); then
      printf 'maintainability note: %s is down to %d production lines; update %s\n' \
        "$rel_path" "$lines" "${BASELINE_FILE#"$ROOT_DIR"/}" >&2
    fi
  done
}

run_self_tests
load_baseline

while IFS= read -r -d '' file; do
  check_file_length "$file"
done < <(
  find \
    "$ROOT_DIR/crates" \
    -path '*/src/*' \
    -type f \
    -name '*.rs' \
    -print0 \
    | sort -z
)

report_stale_baseline_entries

exit "$status"
