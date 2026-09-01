#!/usr/bin/env bash
# Release dependency gate for the Rust product and isolated Rust experiments.

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

for command in cargo git rg; do
  command -v "$command" >/dev/null || {
    echo "dependency gate requires $command" >&2
    exit 2
  }
done
if ! audit_version=$(cargo audit --version 2>/dev/null); then
  echo "dependency gate requires cargo-audit 0.22.2: cargo install cargo-audit --locked --version 0.22.2" >&2
  exit 2
fi
if [[ $audit_version != "cargo-audit 0.22.2" ]]; then
  echo "dependency gate requires cargo-audit 0.22.2, found: $audit_version" >&2
  exit 2
fi

check_freshness() {
  local label=$1
  local manifest=$2
  local output
  output=$(cargo update --locked --dry-run -v --manifest-path "$manifest" 2>&1)
  printf '%s\n' "$output"
  if rg -q 'Locking [1-9][0-9]* package' <<<"$output"; then
    echo "$label lockfile has compatible updates; run cargo update and validate them" >&2
    return 1
  fi
}

check_live_ref() {
  local label=$1
  local url=$2
  local ref=$3
  local lockfile=$4
  local revision
  revision=$(git ls-remote "$url" "$ref" | awk 'NR == 1 { print $1 }')
  if [[ ! $revision =~ ^[0-9a-f]{40}$ ]]; then
    echo "$label live ref could not be resolved: $url $ref" >&2
    return 1
  fi
  if ! rg -q "#$revision\"$" "$lockfile"; then
    echo "$label lockfile does not pin live $ref at $revision" >&2
    return 1
  fi
  printf '%s\t%s\n' "$label" "$revision"
}

check_freshness "product" "$repo_dir/Cargo.toml"
check_freshness "residual-IVF experiment" "$repo_dir/benchmarks/ivf-eval/Cargo.toml"
check_freshness "route-cost sidecar" "$repo_dir/sidecars/route-cost/Cargo.toml"

check_live_ref \
  "production TurboVec fork" \
  "https://github.com/ai-pipestream/turbovec.git" \
  "refs/heads/turbovec-pipestream-s17" \
  "$repo_dir/Cargo.lock"
latest_chain_ref=$(git ls-remote --heads \
  "https://github.com/ai-pipestream/turbovec.git" \
  'refs/heads/turbovec-pipestream-s*' |
  awk '$2 ~ /^refs\/heads\/turbovec-pipestream-s[0-9]+$/ { print $2 }' |
  sort -V | tail -1)
if [[ $latest_chain_ref != "refs/heads/turbovec-pipestream-s17" ]]; then
  echo "production Cargo.toml is not on the newest immutable TurboVec patch chain: $latest_chain_ref" >&2
  exit 1
fi
check_live_ref \
  "distributed TurboVec facade" \
  "https://github.com/ai-pipestream/turbovec-grpc.git" \
  "refs/heads/main" \
  "$repo_dir/Cargo.lock"
check_live_ref \
  "experimental upstream residual IVF" \
  "https://github.com/RyanCodrai/turbovec.git" \
  "refs/heads/feat/ivf-residual" \
  "$repo_dir/benchmarks/ivf-eval/Cargo.lock"

tree_file=$(mktemp)
trap 'rm -f "$tree_file"' EXIT
cargo tree --locked --manifest-path "$repo_dir/benchmarks/ivf-eval/Cargo.toml" \
  -e normal >"$tree_file"
if rg -qi '(^|[^[:alnum:]_-])(pyo3|turbovec-python)([^[:alnum:]_-]|$)' "$tree_file"; then
  echo "Python binding dependency found; the product and IVF experiment must remain pure Rust" >&2
  exit 1
fi

cargo audit --file "$repo_dir/Cargo.lock"
cargo audit --file "$repo_dir/benchmarks/ivf-eval/Cargo.lock"
cargo audit --file "$repo_dir/sidecars/route-cost/Cargo.lock"

echo "dependency gate passed"
