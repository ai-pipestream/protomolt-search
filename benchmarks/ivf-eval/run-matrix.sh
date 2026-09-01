#!/usr/bin/env bash
# Reproducible CourtListener matrix for the standalone residual-IVF adapter.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
MANIFEST="$HERE/Cargo.toml"
INPUT=${INPUT:-/work/court-corpus/embeddings-full.bin}
OUT=${OUT:-/work/court-corpus/bench/ivf-eval/run-$(date +%Y%m%d-%H%M%S)}
SIZES=${SIZES:-100000,500000,1000000,2000000}
QUERIES=${QUERIES:-16}
KS=${KS:-10,100,10000}
NPROBES=${NPROBES:-8,16,32,64,128,256,all}
WARMUP=${WARMUP:-2}
ITERATIONS=${ITERATIONS:-5}
CPUSET=${CPUSET:-}
MAX_EXTERNAL_CPU_PERCENT=${MAX_EXTERNAL_CPU_PERCENT:-100}

die() { echo "ivf-eval: $*" >&2; exit 1; }

for command in cargo git jq rg sha256sum; do
  command -v "$command" >/dev/null || die "missing command: $command"
done
if [[ -n $CPUSET ]]; then
  command -v taskset >/dev/null || die "CPUSET requires taskset"
  [[ $CPUSET =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] ||
    die "CPUSET must be a taskset CPU list"
fi
clock_ticks=$(getconf CLK_TCK)

read_busy_ticks() {
  awk -v cpuset="$CPUSET" '
    function selected(cpu, segments, bounds, count, i) {
      if (cpuset == "") {
        return 1
      }
      count = split(cpuset, segments, ",")
      for (i = 1; i <= count; i++) {
        split(segments[i], bounds, "-")
        if (cpu >= bounds[1] && cpu <= (bounds[2] == "" ? bounds[1] : bounds[2])) {
          return 1
        }
      }
      return 0
    }
    /^cpu[0-9]+ / && selected(substr($1, 4)) {
      busy += $2 + $3 + $4 + $7 + $8 + $9
    }
    END { printf "%.0f\n", busy }
  ' /proc/stat
}

read_process_ticks() {
  [[ -r /proc/$benchmark_pid/stat ]] || return 1
  awk '{ print $14 + $15 }' "/proc/$benchmark_pid/stat"
}
[[ -f $INPUT ]] || die "missing CourtListener embedding artifact: $INPUT"
[[ $SIZES =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] || die "SIZES must be comma-separated positive integers"
[[ $QUERIES =~ ^[1-9][0-9]*$ ]] || die "QUERIES must be positive"
[[ $KS =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] || die "KS must be comma-separated positive integers"
[[ $ITERATIONS =~ ^[1-9][0-9]*$ ]] || die "ITERATIONS must be positive"
[[ $WARMUP =~ ^[0-9]+$ ]] || die "WARMUP must be nonnegative"
[[ $MAX_EXTERNAL_CPU_PERCENT =~ ^[1-9][0-9]*$ ]] ||
  die "MAX_EXTERNAL_CPU_PERCENT must be a positive integer"
[[ ! -e $OUT ]] || die "refusing existing output path: $OUT"
mkdir -p "$OUT"

PRODUCT_REVISION=$(git -C "$REPO" rev-parse HEAD)
git -C "$REPO" status --short --branch >"$OUT/git-status.txt"
git -C "$REPO" remote -v >"$OUT/git-remotes.txt"
rustc --version --verbose >"$OUT/rustc.txt"
printf '%s\n' "${CPUSET:-unrestricted}" >"$OUT/cpuset.txt"
{
  uptime
  ps -eo pid,psr,comm,%cpu,%mem,etime --sort=-%cpu | head -20
} >"$OUT/host-before.txt"
cargo build --release --locked --manifest-path "$MANIFEST"
cargo tree --locked --manifest-path "$MANIFEST" -e normal >"$OUT/dependency-tree.txt"
if rg -qi '(^|[^[:alnum:]_-])(pyo3|turbovec-python)([^[:alnum:]_-]|$)' \
  "$OUT/dependency-tree.txt"; then
  die "Python binding dependency found; this benchmark must remain pure Rust"
fi

RUNNER=()
if [[ -n $CPUSET ]]; then
  RUNNER=(taskset -c "$CPUSET")
fi
for size in ${SIZES//,/ }; do
  output="$OUT/court-${size}.json"
  pressure_log="$OUT/court-${size}.host-pressure.log"
  echo "== CourtListener rows=$size"
  "${RUNNER[@]}" "$HERE/target/release/protomolt-ivf-eval" \
    --source=court --input="$INPUT" --vectors="$size" \
    --queries="$QUERIES" --k="$KS" --nprobe="$NPROBES" \
    --warmup="$WARMUP" --iterations="$ITERATIONS" \
    --product-revision="$PRODUCT_REVISION" --out="$output" \
    >"$OUT/court-${size}.stdout" \
    2>"$OUT/court-${size}.log" &
  benchmark_pid=$!
  peak_external_cpu=0
  pressure_detected=false
  busy_before=$(read_busy_ticks)
  benchmark_before=$(read_process_ticks)
  sample_before_ns=$(date +%s%N)
  while kill -0 "$benchmark_pid" 2>/dev/null; do
    sleep 1
    benchmark_after=$(read_process_ticks) || break
    busy_after=$(read_busy_ticks)
    sample_after_ns=$(date +%s%N)
    busy_delta=$((busy_after - busy_before))
    benchmark_delta=$((benchmark_after - benchmark_before))
    external_delta=$((busy_delta - benchmark_delta))
    (( external_delta >= 0 )) || external_delta=0
    external_cpu=$((
      (external_delta * 100000000000 +
       clock_ticks * (sample_after_ns - sample_before_ns) / 2) /
      (clock_ticks * (sample_after_ns - sample_before_ns))
    ))
    if (( external_cpu > peak_external_cpu )); then
      peak_external_cpu=$external_cpu
    fi
    if (( external_cpu > MAX_EXTERNAL_CPU_PERCENT )); then
      pressure_detected=true
      printf '%s external_cpu_percent=%d threshold=%d\n' \
        "$(date --iso-8601=seconds)" "$external_cpu" "$MAX_EXTERNAL_CPU_PERCENT" \
        >>"$pressure_log"
    fi
    busy_before=$busy_after
    benchmark_before=$benchmark_after
    sample_before_ns=$sample_after_ns
  done
  wait "$benchmark_pid"
  if [[ $pressure_detected == true ]]; then
    validity_reason="external CPU exceeded the reproducibility threshold during this cell"
  else
    validity_reason="external CPU remained within the reproducibility threshold"
  fi
  tmp="$output.validity.tmp"
  jq \
    --argjson latency_valid "$([[ $pressure_detected == false ]] && echo true || echo false)" \
    --argjson peak_external_cpu_percent "$peak_external_cpu" \
    --argjson max_external_cpu_percent "$MAX_EXTERNAL_CPU_PERCENT" \
    --arg reason "$validity_reason" \
    '.host_validity = {
       latency_valid: $latency_valid,
       peak_external_cpu_percent: $peak_external_cpu_percent,
       max_external_cpu_percent: $max_external_cpu_percent,
       reason: $reason
     }
     | if $latency_valid then . else
         .decision_gate.passed = false
         | .decision_gate.reasons += ["host pressure invalidated latency and build-time comparisons"]
       end' \
    "$output" >"$tmp"
  mv "$tmp" "$output"
done

{
  uptime
  ps -eo pid,psr,comm,%cpu,%mem,etime --sort=-%cpu | head -20
} >"$OUT/host-after.txt"

jq -s '{
  format: "protomolt-ivf-eval-matrix-v1",
  product_revision: .[0].product_revision,
  upstream_ivf_revision: .[0].upstream_ivf_revision,
  source_path: .[0].source_path,
  cells: map({vectors, dimensions, host_validity, decision_gate}),
  required_large_cells_passed: (
    map(select(.vectors >= 1000000) |
      (.host_validity.latency_valid and .decision_gate.passed)) as $passes |
    ($passes | length) >= 2 and ($passes | all(. == true))
  )
}' "$OUT"/court-[0-9]*.json >"$OUT/summary.json"

(
  cd "$OUT"
  sha256sum -- *.json *.txt *.log *.stdout >SHA256SUMS
  sha256sum -c SHA256SUMS
)

cat "$OUT/summary.json"
