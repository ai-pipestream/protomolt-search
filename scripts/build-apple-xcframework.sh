#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${1:-$repo_dir/target/mobile/ProtomoltSearch.xcframework}"
export IPHONEOS_DEPLOYMENT_TARGET="${IOS_MIN_VERSION:-15.0}"

if ! command -v xcodebuild >/dev/null 2>&1 || ! command -v lipo >/dev/null 2>&1; then
  echo "xcodebuild and lipo are required; build the XCFramework on macOS with Xcode" >&2
  exit 2
fi
if [[ -e "$output" ]]; then
  echo "refusing to overwrite existing XCFramework: $output" >&2
  exit 2
fi

targets=(aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
installed="$(rustup target list --installed)"
for target in "${targets[@]}"; do
  if ! grep -Fxq "$target" <<<"$installed"; then
    echo "required Rust target is not installed: $target" >&2
    echo "install it with: rustup target add $target" >&2
    exit 2
  fi
  cargo build --manifest-path "$repo_dir/Cargo.toml" --locked --release \
    -p protomolt-search-embedded --target "$target"
done

stage="$(mktemp -d)"
cleanup() { rm -rf "$stage"; }
trap cleanup EXIT
simulator="$stage/libprotomolt_search_embedded-simulator.a"
lipo -create \
  "$repo_dir/target/aarch64-apple-ios-sim/release/libprotomolt_search_embedded.a" \
  "$repo_dir/target/x86_64-apple-ios/release/libprotomolt_search_embedded.a" \
  -output "$simulator"
mkdir -p "$(dirname "$output")"
xcodebuild -create-xcframework \
  -library "$repo_dir/target/aarch64-apple-ios/release/libprotomolt_search_embedded.a" \
  -headers "$repo_dir/mobile/apple/include" \
  -library "$simulator" \
  -headers "$repo_dir/mobile/apple/include" \
  -output "$output"
echo "$output"
