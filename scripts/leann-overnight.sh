#!/usr/bin/env bash
# Overnight leann-graph vs flat TurboVec matrix on the CourtListener prefix.
# One JSON per job; each file is rewritten after every graph so a killed
# run keeps what finished. See examples/leann_eval.rs.

set -euo pipefail

INPUT=${INPUT:-/work/court-corpus/embeddings-full.bin}
OUT=${OUT:-/work/court-corpus/bench/leann/run-$(date +%Y%m%d-%H%M%S)}
# size=configs jobs, run in order. The build is single-threaded and grows
# with rows and degree: a 50k x 64 synthetic build of 32:8:64 took 68 s, so
# expect roughly 1.5 h for 1M court rows and 3-4 h for 2M at the defaults,
# and two to three times that for the wider 64:16:128 graph.
JOBS=${JOBS:-100000=32:8:64,64:16:128 1000000=32:8:64 2000000=32:8:64 1000000=64:16:128}
QUERIES=${QUERIES:-64}
KS=${KS:-10,100}
BEAMS=${BEAMS:-10,32,64,128,256,512,1024}

[[ -f $INPUT ]] || { echo "leann-overnight: missing $INPUT" >&2; exit 1; }
mkdir -p "$OUT"
git rev-parse HEAD > "$OUT/revision"
lscpu > "$OUT/lscpu.txt" 2>/dev/null || true
cargo build --release --example leann_eval

for job in $JOBS; do
  size=${job%%=*}
  configs=${job#*=}
  name="$size-${configs//[:,]/_}"
  echo "leann-overnight: $size rows, $configs -> $OUT/$name.json ($(date -Is))"
  target/release/examples/leann_eval \
    --input="$INPUT" --vectors="$size" --queries="$QUERIES" \
    --k="$KS" --beams="$BEAMS" --configs="$configs" \
    --out="$OUT/$name.json" 2>&1 | tee "$OUT/$name.log"
done
echo "leann-overnight: done ($(date -Is)); results in $OUT"
