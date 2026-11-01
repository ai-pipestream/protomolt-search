#!/usr/bin/env bash
# Fetch the Fashion-MNIST IDX images that ann-benchmarks'
# fashion-mnist-784-euclidean is built from (60,000 train, 10,000 test).
# The labels are not needed; the truth is computed exactly by `calibrate`.
set -euo pipefail
out="${1:-fashion-mnist}"
mkdir -p "$out"
base="https://raw.githubusercontent.com/zalandoresearch/fashion-mnist/master/data/fashion"
for f in train-images-idx3-ubyte.gz t10k-images-idx3-ubyte.gz; do
  [ -s "$out/$f" ] || curl -fsSL -o "$out/$f" "$base/$f"
done
sha256sum "$out"/*.gz
