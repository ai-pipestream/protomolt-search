#!/usr/bin/env bash
# Regenerates ui/src/gen (TypeScript stubs) and src/console/console_bff.binpb
# (the BFF descriptor the console compiles in) from ui/proto plus the
# vendored protos, or checks both for drift with --check. Run `bun install`
# in ui/ first so the pinned protoc-gen-es is in node_modules.
set -euo pipefail
ui=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
repo=$(cd -- "$ui/.." && pwd)
buf_version=1.70.0
if [[ "$(buf --version)" != "$buf_version" ]]; then
  echo "BFF stub generation requires buf $buf_version." >&2
  exit 1
fi
plugin="$ui/node_modules/.bin/protoc-gen-es"
[[ -x "$plugin" ]] || { echo "Missing $plugin: run bun install in ui/." >&2; exit 1; }
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/protos"
cp -r "$repo/proto/." "$work/protos/"
cp -r "$ui/proto/." "$work/protos/"
printf 'version: v2\nmodules:\n  - path: protos\n' > "$work/buf.yaml"
sed -e "s#local: node_modules#local: $ui/node_modules#" -e "s#out: src/gen#out: $work/gen#" \
  "$ui/buf.gen.yaml" > "$work/buf.gen.yaml"
bff=ai/protomolt/console/bff/v1/console_bff.proto
(cd "$work" && buf generate --template buf.gen.yaml --path "protos/$bff")
(cd "$work" && buf build --path "protos/$bff" --exclude-imports -o bff.binpb)
# Vendored proto comments carry em dashes; the generated docs use hyphens.
find "$work/gen" -name '*.ts' -exec perl -CSD -pi -e 's/\x{2014}/-/g' {} +
descriptor="$repo/src/console/console_bff.binpb"
if [[ "${1:-}" == --check ]]; then
  diff -r "$work/gen" "$ui/src/gen" && cmp "$work/bff.binpb" "$descriptor" || {
    echo "ui/src/gen or src/console/console_bff.binpb is stale: run ui/scripts/gen.sh." >&2
    exit 1
  }
else
  rm -rf "$ui/src/gen"
  cp -r "$work/gen" "$ui/src/gen"
  cp "$work/bff.binpb" "$descriptor"
fi
