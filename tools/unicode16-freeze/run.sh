#!/usr/bin/env bash
# Regenerate crates/protomolt-unicode16/src/tables.rs: dump every scalar
# value's ICU4X answers under 2.0.0 and under 2.3.0, compare them, and write
# the tables of what the freeze restores. Refuses, by name, any difference
# the freeze does not handle. Build output goes to target/unicode16-freeze.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
target="$root/target/unicode16-freeze"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
for release in 2.0 2.3; do
    cargo run --release --quiet --target-dir "$target" \
        --manifest-path "$here/icu-$release/Cargo.toml" > "$work/icu-$release.tsv"
done
cargo run --release --quiet --target-dir "$target" \
    --manifest-path "$here/generate/Cargo.toml" -- \
    "$work/icu-2.0.tsv" "$work/icu-2.3.tsv" > "$work/tables.rs"
rustfmt --edition 2021 "$work/tables.rs"
mv "$work/tables.rs" "$root/crates/protomolt-unicode16/src/tables.rs"
echo "wrote crates/protomolt-unicode16/src/tables.rs"
