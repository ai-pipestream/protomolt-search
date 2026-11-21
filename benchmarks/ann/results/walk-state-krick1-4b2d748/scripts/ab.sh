#!/usr/bin/env bash
# ab.sh <outdir> <rounds> <mode> <variant...>
# Interleaved rounds over the variant binaries in $AB (bench-<variant>).
#   mode one: pinned to CPU 14, --threads=1 (a one-thread build is the same
#             graph for every variant, so the walks are the same walks)
#   mode all: unpinned, --threads=32
set -euo pipefail
S=${SCRATCH:?a scratch directory holding bench-<variant> binaries, fixture/ and sampler.so}
AB=$S; FX=${FX:-$AB/fixture}
out=$1; rounds=$2; mode=$3; shift 3
MIN=${MIN_TIME_MS:-1000}
mkdir -p "$out"
for r in $(seq 1 "$rounds"); do
  for v in "$@"; do
    if [[ $mode == one ]]; then pin=(taskset -c 14); threads=1; else pin=(); threads=32; fi
    common=(--fixture="$FX" --engines=hnsw --m=16 --ef-construction=200 --ef=16,64,128,256,1000
            --recall-stop=2 --min-time-ms="$MIN" --threads="$threads")
    "${pin[@]}" "$AB/bench-$v" run "${common[@]}" --k=10,100,1000 --filters=none \
      --out="$out/$mode-$v-none-$r.json" > "$out/$mode-$v-none-$r.log" 2>&1
    if [[ ${CELLS:-both} == both ]]; then
      "${pin[@]}" "$AB/bench-$v" run "${common[@]}" --k=10,100 --filters=uncorrelated-50,uncorrelated-10 \
        --out="$out/$mode-$v-filt-$r.json" > "$out/$mode-$v-filt-$r.log" 2>&1
    fi
    echo "$(date +%T) round $r $mode $v done"
  done
done
