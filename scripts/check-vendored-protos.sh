#!/bin/sh
# Byte-identity gate for the protos vendored from protomolt
# (docs/descriptor-mappings.md section 5). The vendored copies must stay
# byte-identical to the owning repository; this repo never edits them.
#
# Pinned upstream: protomolt rev 74d172d9
#   ("Add descriptor exchange contract under schema/registry")
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

check proto/ai/pipestream/proto/schema/registry/v1/descriptor_exchange.proto \
    d5804506d4522b8259cbb3f7ce76601f0e1d30c389348637d54c1d7d66976cde \
    schema/registry/proto/src/main/proto/ai/pipestream/proto/schema/registry/v1/descriptor_exchange.proto

check proto/ai/pipestream/proto/validate/v1/validate.proto \
    ea91109e2f3c33e3272a10039b1564c200a6a16d76f609eef98e75c6290df79e \
    protobuf/validation/src/main/proto/ai/pipestream/proto/validate/v1/validate.proto

check proto/ai/pipestream/proto/index/hints/v1/indexing_hints.proto \
    69942c35f182fb967ce713fcc50a1f74dd7cf61a727859dd7c58bdabc8c0422b \
    search/index/spi/src/main/proto/ai/pipestream/proto/index/hints/v1/indexing_hints.proto

if [ "$fail" -ne 0 ]; then
    echo "vendored protos have drifted; re-copy from protomolt and re-pin" >&2
    exit 1
fi
echo "vendored protos match their pins"
