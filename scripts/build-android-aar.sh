#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${1:-$repo_dir/target/mobile/ProtomoltSearch.aar}"
min_api="${ANDROID_MIN_API:-26}"

find_ndk() {
  if [[ -n "${ANDROID_NDK_HOME:-}" ]]; then
    printf '%s\n' "$ANDROID_NDK_HOME"
    return
  fi
  if [[ -n "${ANDROID_NDK_ROOT:-}" ]]; then
    printf '%s\n' "$ANDROID_NDK_ROOT"
    return
  fi
  if [[ -n "${ANDROID_HOME:-}" && -d "$ANDROID_HOME/ndk" ]]; then
    find "$ANDROID_HOME/ndk" -mindepth 1 -maxdepth 1 -type d -print | sort -V | tail -1
    return
  fi
}

ndk_dir="$(find_ndk)"
if [[ -z "$ndk_dir" || ! -d "$ndk_dir/toolchains/llvm/prebuilt" ]]; then
  echo "Android NDK not found; set ANDROID_NDK_HOME or ANDROID_HOME" >&2
  exit 2
fi

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) host_tag="linux-x86_64" ;;
  Darwin-x86_64) host_tag="darwin-x86_64" ;;
  Darwin-arm64) host_tag="darwin-arm64" ;;
  *) echo "unsupported Android build host: $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

toolchain="$ndk_dir/toolchains/llvm/prebuilt/$host_tag/bin"
if [[ ! -d "$toolchain" ]]; then
  echo "NDK has no toolchain for host $host_tag: $toolchain" >&2
  exit 2
fi

stage="$(mktemp -d)"
cleanup() { rm -rf "$stage"; }
trap cleanup EXIT
mkdir -p "$stage/jni" "$stage/assets/ai/protomolt/search/mobile/v1" \
  "$stage/assets/ai/protomolt/search/v1" "$(dirname "$output")"

targets=(
  "aarch64-linux-android|arm64-v8a|aarch64-linux-android"
  "x86_64-linux-android|x86_64|x86_64-linux-android"
)
installed="$(rustup target list --installed)"
for entry in "${targets[@]}"; do
  IFS='|' read -r rust_target abi clang_prefix <<<"$entry"
  if ! grep -Fxq "$rust_target" <<<"$installed"; then
    echo "required Rust target is not installed: $rust_target" >&2
    echo "install it with: rustup target add $rust_target" >&2
    exit 2
  fi
  linker="$toolchain/${clang_prefix}${min_api}-clang"
  if [[ ! -x "$linker" ]]; then
    echo "Android linker is missing: $linker" >&2
    exit 2
  fi
  linker_var="CARGO_TARGET_${rust_target^^}_LINKER"
  linker_var="${linker_var//-/_}"
  export "$linker_var=$linker"
  cargo build --manifest-path "$repo_dir/Cargo.toml" --locked --release \
    -p protomolt-search-embedded --target "$rust_target"
  mkdir -p "$stage/jni/$abi"
  cp "$repo_dir/target/$rust_target/release/libprotomolt_search_embedded.so" \
    "$stage/jni/$abi/"
done

classes="$stage/classes"
mkdir -p "$classes"
javac --release 8 -d "$classes" \
  "$repo_dir/mobile/android/src/main/java/ai/pipestream/search/mobile/ProtomoltSearch.java"
jar --create --date=2020-01-01T00:00:00Z --file "$stage/classes.jar" -C "$classes" .
rm -rf "$classes"

cp "$repo_dir/mobile/android/AndroidManifest.xml" "$stage/AndroidManifest.xml"
cp "$repo_dir/mobile/android/proguard.txt" "$stage/proguard.txt"
cp "$repo_dir/mobile/android/R.txt" "$stage/R.txt"
cp "$repo_dir/proto/ai/protomolt/search/mobile/v1/mobile.proto" \
  "$stage/assets/ai/protomolt/search/mobile/v1/mobile.proto"
cp "$repo_dir/proto/ai/protomolt/search/v1/"*.proto \
  "$stage/assets/ai/protomolt/search/v1/"

if [[ -e "$output" ]]; then
  echo "refusing to overwrite existing AAR: $output" >&2
  exit 2
fi
jar --create --date=2020-01-01T00:00:00Z --file "$output" -C "$stage" .
echo "$output"
