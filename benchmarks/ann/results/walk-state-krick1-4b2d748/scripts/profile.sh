#!/usr/bin/env bash
# profile.sh <variant> <tag> <filter> <k> <ef>: one pinned run under the ITIMER_PROF sampler.
set -euo pipefail
S=${SCRATCH:?a scratch directory holding bench-<variant> binaries, fixture/ and sampler.so}
v=$1; tag=$2; filter=$3; k=$4; ef=$5
out=$S/profiles; mkdir -p $out
bin=$S/bench-$v
PROF_OUT=$out/$v-$tag.prof PROF_DELAY_MS=11000 PROF_PERIOD_US=1000 LD_PRELOAD=$S/sampler.so \
  taskset -c 14 $bin run --fixture=$S/fixture --engines=hnsw --m=16 --ef-construction=200 \
  --k=$k --ef=$ef --filters=$filter --recall-stop=2 --min-time-ms=20000 --threads=1 \
  --out=$out/$v-$tag.json > $out/$v-$tag.log 2>&1
python3 -I $(dirname "$0")/symbolize.py $out/$v-$tag.prof $bin > $out/$v-$tag.txt
head -40 $out/$v-$tag.txt
