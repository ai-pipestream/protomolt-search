#!/usr/bin/env bash
# The #53 gate matrix on one host: fixture, every in-process engine, FAISS
# and Lucene, single and segmented shapes, then the gate table.
#
#   benchmarks/ann/run-matrix.sh challenge           # 100k x 64, minutes to hours
#   INPUT=/work/court-corpus/embeddings-full.bin \
#     benchmarks/ann/run-matrix.sh court              # 1M court rows, overnight
#
# Environment:
#   OUT            results directory (default benchmarks/ann/results/<fixture>-<host>)
#   QUERIES        held-out queries (default 500)
#   SEGMENT_ROWS   sealed segment size for the segmented shape; 0 skips it
#                  (default 0 for challenge, 120000 for court: 8 sealed + a 40k tail)
#   M, EFC, EF     sweep lists (defaults: 16,32,48,64 / 100,200,400 / 16..512)
#   ENGINES        in-process engines (default: all)
#   SKIP_FAISS, SKIP_LUCENE   set to 1 to skip a runner
#   MATRIX         expected-cell matrix the gate judges (default gates/replacement.json;
#                  the unfiltered margin lives in the matrix)
#   CALIBRATION    calibration JSON for the gate (default results/calibration-hnsw_rs.json)
#   MAVEN_SETTINGS a settings.xml for Maven (mirrors), passed as -s
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
source_name=${1:-challenge}
host=${ANN_BENCH_HOST:-$(hostname)}
export ANN_BENCH_HOST=$host
OUT=${OUT:-$here/results/$source_name-$host}
QUERIES=${QUERIES:-500}
M=${M:-16,32,48,64}
EFC=${EFC:-100,200,400}
EF=${EF:-16,32,48,64,96,128,192,256,384,512}
MATRIX=${MATRIX:-$here/gates/replacement.json}
CALIBRATION=${CALIBRATION:-$here/results/calibration-hnsw_rs.json}
FIXTURE=${FIXTURE:-$OUT/fixture}

case $source_name in
  challenge) SEGMENT_ROWS=${SEGMENT_ROWS:-0}; fixture_args=(--source=challenge) ;;
  court)
    SEGMENT_ROWS=${SEGMENT_ROWS:-120000}
    INPUT=${INPUT:-/work/court-corpus/embeddings-full.bin}
    [[ -f $INPUT ]] || { echo "run-matrix: missing $INPUT" >&2; exit 1; }
    fixture_args=(--source=court --input="$INPUT" --vectors="${VECTORS:-1000000}") ;;
  *) echo "run-matrix: unknown fixture $source_name (challenge|court)" >&2; exit 1 ;;
esac

mkdir -p "$OUT"
git -C "$here" rev-parse HEAD > "$OUT/revision" 2>/dev/null || true
lscpu > "$OUT/lscpu.txt" 2>/dev/null || true
export ANN_BENCH_COMMIT=$(cat "$OUT/revision" 2>/dev/null || true)

# CoreNN's kernels crate uses #![feature(f16)]; allow exactly that crate.
RUSTC_BOOTSTRAP=corenn_kernels cargo build --release --locked --manifest-path "$here/Cargo.toml"
bench=$here/target/release/protomolt-ann-bench

[[ -f $FIXTURE/manifest.json ]] || "$bench" fixture "${fixture_args[@]}" --queries="$QUERIES" --out="$FIXTURE"

sweep=(--m="$M" --ef-construction="$EFC" --ef="$EF")
shapes=(0)
[[ $SEGMENT_ROWS != 0 ]] && shapes+=("$SEGMENT_ROWS")
results=()
for seg in "${shapes[@]}"; do
  tag=$([[ $seg == 0 ]] && echo single || echo "seg$seg")
  engines_arg=()
  [[ -n ${ENGINES:-} ]] && engines_arg=(--engines="$ENGINES")
  "$bench" run --fixture="$FIXTURE" --segment-rows="$seg" "${sweep[@]}" "${engines_arg[@]}" \
    --out="$OUT/rust-$tag.json" 2>&1 | tee "$OUT/rust-$tag.log"
  results+=("$OUT/rust-$tag.json")
  if [[ ${SKIP_FAISS:-0} != 1 ]]; then
    python3 "$here/faiss/run.py" --fixture="$FIXTURE" --segment-rows="$seg" \
      --m="$M" --ef-construction="$EFC" --ef="$EF" --out="$OUT/faiss-$tag.json" 2>&1 | tee "$OUT/faiss-$tag.log"
    results+=("$OUT/faiss-$tag.json")
  fi
  if [[ ${SKIP_LUCENE:-0} != 1 ]]; then
    mvn_args=(-q -f "$here/lucene/pom.xml" package -DskipTests)
    [[ -n ${MAVEN_SETTINGS:-} ]] && mvn_args=(-s "$MAVEN_SETTINGS" "${mvn_args[@]}")
    mvn "${mvn_args[@]}"
    java --add-modules jdk.incubator.vector \
      -cp "$here/lucene/target/protomolt-ann-bench-lucene-0.1.0.jar:$(cat "$here/lucene/target/classpath.txt")" \
      ai.pipestream.annbench.LuceneRunner --fixture="$FIXTURE" --segment-rows="$seg" \
      --m="$M" --ef-construction="$EFC" --ef="$EF" --work="$OUT/lucene-index" \
      --out="$OUT/lucene-$tag.json" 2>&1 | grep -v '^WARNING' | tee "$OUT/lucene-$tag.log"
    results+=("$OUT/lucene-$tag.json")
  fi
done

calibration_arg=()
[[ -f $CALIBRATION ]] && calibration_arg=(--calibration="$CALIBRATION")
"$bench" gate --results="$(IFS=,; echo "${results[*]}")" --matrix="$MATRIX" \
  "${calibration_arg[@]}" --out="$OUT/gate.md"
echo "run-matrix: done; gate table in $OUT/gate.md"
