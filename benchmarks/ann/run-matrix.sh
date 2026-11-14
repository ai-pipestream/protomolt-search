#!/usr/bin/env bash
# The #53 gate matrix on one host: fixture, every in-process engine, FAISS
# and Lucene, single, segmented and sharded shapes, then the gate tables.
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
#   ENGINES        in-process engines for the single and segmented shapes (default: all)
#   SHARDS         shard counts of the sharded shape (default 4,16, as gates/floor.json
#                  lists them; 0 skips the sharded shape)
#   PLACEMENTS     placements of the sharded shape (default hash,skewed)
#   SHARD_ENGINES  in-process engines for the sharded shape (default hnsw,enumerate)
#   SHARD_K, SHARD_EF  k and breadth lists of the sharded shape (default: the runners'
#                  own, k = 100,1000,10000 and breadths up to 32768)
#   SKIP_FAISS, SKIP_LUCENE   set to 1 to skip a runner (FAISS has no sharded shape)
#   LUCENE_16357_VERSION  the version under which a build of apache/lucene PR #16357
#                  is installed in the local Maven repository; when set, that build
#                  runs the sharded shapes as lucene-hnsw-16357 (see README.md)
#   MATRIX         matrix the gate judges into gate.md (default gates/replacement.json;
#                  the unfiltered margin lives in the matrix)
#   FLOOR_MATRIX   matrix the gate judges into gate-floor.md when sharded shapes ran
#                  (default gates/floor.json)
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
SHARDS=${SHARDS:-4,16}
PLACEMENTS=${PLACEMENTS:-hash,skewed}
SHARD_ENGINES=${SHARD_ENGINES:-hnsw,enumerate}
MATRIX=${MATRIX:-$here/gates/replacement.json}
FLOOR_MATRIX=${FLOOR_MATRIX:-$here/gates/floor.json}
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

# The fixture writes a placement file for every shard count the sharded
# shape runs; a fixture built without one is refused by the runners.
shard_counts=$([[ $SHARDS == 0 ]] && echo 4 || echo "$SHARDS")
[[ -f $FIXTURE/manifest.json ]] || "$bench" fixture "${fixture_args[@]}" --queries="$QUERIES" \
  --shard-counts="$shard_counts" --out="$FIXTURE"

# Shapes: single, then segmented, then every shard count and placement.
shapes=(single)
[[ $SEGMENT_ROWS != 0 ]] && shapes+=("seg:$SEGMENT_ROWS")
if [[ $SHARDS != 0 ]]; then
  for n in ${SHARDS//,/ }; do
    for p in ${PLACEMENTS//,/ }; do shapes+=("sharded:$n:$p"); done
  done
fi

# The arguments every runner takes for a shape, and the shape's file tag.
shape_args() {
  case $1 in
    single) args=(); tag=single ;;
    seg:*) args=(--segment-rows="${1#seg:}"); tag="seg${1#seg:}" ;;
    sharded:*)
      IFS=: read -r _ n p <<< "$1"
      args=(--shards="$n" --placement="$p")
      [[ -n ${SHARD_K:-} ]] && args+=(--k="$SHARD_K")
      [[ -n ${SHARD_EF:-} ]] && args+=(--ef="$SHARD_EF")
      tag="sharded$n-$p" ;;
  esac
}

# Packages the Lucene runner for one Lucene build; later runs use that jar.
package_lucene() {
  local mvn_args=(-q -f "$here/lucene/pom.xml" package -DskipTests "$@")
  [[ -n ${MAVEN_SETTINGS:-} ]] && mvn_args=(-s "$MAVEN_SETTINGS" "${mvn_args[@]}")
  mvn "${mvn_args[@]}"
}

run_lucene() {
  local out=$1; shift
  java --add-modules jdk.incubator.vector \
    -cp "$here/lucene/target/protomolt-ann-bench-lucene-0.1.0.jar:$(cat "$here/lucene/target/classpath.txt")" \
    ai.pipestream.annbench.LuceneRunner --fixture="$FIXTURE" --m="$M" --ef-construction="$EFC" \
    --work="$OUT/lucene-index" "$@" --out="$out" 2>&1 | grep -v '^WARNING' | tee "${out%.json}.log"
}

results=()
floor_results=()
[[ ${SKIP_LUCENE:-0} != 1 ]] && package_lucene
for shape in "${shapes[@]}"; do
  shape_args "$shape"
  sharded=0
  [[ $shape == sharded:* ]] && sharded=1
  engines_arg=()
  if [[ $sharded == 1 ]]; then
    engines_arg=(--engines="$SHARD_ENGINES")
    sweep=(--m="$M" --ef-construction="$EFC")
  else
    [[ -n ${ENGINES:-} ]] && engines_arg=(--engines="$ENGINES")
    sweep=(--m="$M" --ef-construction="$EFC" --ef="$EF")
  fi
  "$bench" run --fixture="$FIXTURE" "${args[@]}" "${sweep[@]}" "${engines_arg[@]}" \
    --out="$OUT/rust-$tag.json" 2>&1 | tee "$OUT/rust-$tag.log"
  produced=("$OUT/rust-$tag.json")
  if [[ ${SKIP_FAISS:-0} != 1 && $sharded == 0 ]]; then
    python3 "$here/faiss/run.py" --fixture="$FIXTURE" "${args[@]}" \
      --m="$M" --ef-construction="$EFC" --ef="$EF" --out="$OUT/faiss-$tag.json" 2>&1 | tee "$OUT/faiss-$tag.log"
    produced+=("$OUT/faiss-$tag.json")
  fi
  if [[ ${SKIP_LUCENE:-0} != 1 ]]; then
    lucene_sweep=()
    [[ $sharded == 0 ]] && lucene_sweep=(--ef="$EF")
    run_lucene "$OUT/lucene-$tag.json" "${args[@]}" "${lucene_sweep[@]}"
    produced+=("$OUT/lucene-$tag.json")
  fi
  if [[ $sharded == 1 ]]; then
    floor_results+=("${produced[@]}")
  else
    results+=("${produced[@]}")
  fi
done

# The apache/lucene PR #16357 build: the sharded shapes only, as its own engine.
if [[ ${SKIP_LUCENE:-0} != 1 && -n ${LUCENE_16357_VERSION:-} && $SHARDS != 0 ]]; then
  package_lucene -Dlucene.version="$LUCENE_16357_VERSION" -Dlucene.engine=lucene-hnsw-16357
  for shape in "${shapes[@]}"; do
    [[ $shape == sharded:* ]] || continue
    shape_args "$shape"
    run_lucene "$OUT/lucene-16357-$tag.json" "${args[@]}"
    floor_results+=("$OUT/lucene-16357-$tag.json")
  done
  # Leave the stock jar in place for the next run.
  package_lucene
fi

calibration_arg=()
[[ -f $CALIBRATION ]] && calibration_arg=(--calibration="$CALIBRATION")
"$bench" gate --results="$(IFS=,; echo "${results[*]}")" --matrix="$MATRIX" \
  "${calibration_arg[@]}" --out="$OUT/gate.md"
if [[ ${#floor_results[@]} -gt 0 ]]; then
  "$bench" gate --results="$(IFS=,; echo "${floor_results[*]}")" --matrix="$FLOOR_MATRIX" \
    "${calibration_arg[@]}" --out="$OUT/gate-floor.md"
fi
echo "run-matrix: done; gate tables in $OUT/gate.md${floor_results:+ and $OUT/gate-floor.md}"
