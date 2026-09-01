#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

targets=(
  aarch64-linux-android
  x86_64-linux-android
  aarch64-apple-ios
  aarch64-apple-ios-sim
  x86_64-apple-ios
)

installed="$(rustup target list --installed)"
for target in "${targets[@]}"; do
  if ! grep -Fxq "$target" <<<"$installed"; then
    echo "required Rust target is not installed: $target" >&2
    echo "install it with: rustup target add $target" >&2
    exit 2
  fi
  cargo check --locked -p protomolt-search-embedded --target "$target"
done
