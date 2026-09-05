#!/bin/sh
# Byte-identity gate for the protos vendored from protomolt
# (docs/descriptor-mappings.md section 5). The vendored copies must stay
# byte-identical to the owning repository; this repo never edits them.
#
# Pinned upstream: protomolt rev 75ae2c60
#   (contains the ai.protomolt.proto namespace migration)
#
# Two checks, one always-on and one opt-in:
#   1. Each vendored file's SHA-256 must match the sum pinned below. The
#      sums were taken from the pinned rev, so a local edit to a vendored
#      copy fails here with no network and no checkout needed.
#   2. When PROTOMOLT_DIR names a protomolt checkout, each vendored file
#      is also diffed byte-for-byte against that tree, which is how the
#      pin itself is advanced: update the copy, re-pin the sums, name the
#      new rev in this header.
#
# Exit code 0 means identical; anything else is a real drift.

set -eu
cd "$(dirname "$0")/.."

fail=0

check() {
    vendored="$1"
    pinned="$2"
    upstream_rel="$3"
    actual=$(sha256sum "$vendored" | cut -d' ' -f1)
    if [ "$actual" != "$pinned" ]; then
        echo "DRIFT: $vendored" >&2
        echo "  pinned  $pinned" >&2
        echo "  actual  $actual" >&2
        fail=1
    fi
    if [ -n "${PROTOMOLT_DIR:-}" ]; then
        if ! cmp -s "$vendored" "$PROTOMOLT_DIR/$upstream_rel"; then
            echo "DRIFT vs $PROTOMOLT_DIR: $vendored != $upstream_rel" >&2
            fail=1
        fi
    fi
}

check proto/ai/protomolt/proto/schema/registry/v1/descriptor_exchange.proto \
    70a67352e5d32a8eb88ce62cfdb6f3864cbb5892ac610e7c9d342d9756f1d352 \
    schema/registry/proto/src/main/proto/ai/protomolt/proto/schema/registry/v1/descriptor_exchange.proto

check proto/ai/protomolt/proto/validate/v1/validate.proto \
    e0c9c4d255860ddfaa3b8efdcca021d5b452996be8bc35a0a7e2e90e8f32127d \
    protobuf/validation/src/main/proto/ai/protomolt/proto/validate/v1/validate.proto

check proto/ai/protomolt/proto/index/hints/v1/indexing_hints.proto \
    e5660c2feddf83bd821936dc0f8a3673e79d92ecc4a475a3f81eea278672f3db \
    search/index/spi/src/main/proto/ai/protomolt/proto/index/hints/v1/indexing_hints.proto

if [ "$fail" -ne 0 ]; then
    echo "vendored protos have drifted; re-copy from protomolt and re-pin" >&2
    exit 1
fi
echo "vendored protos match their pins"
