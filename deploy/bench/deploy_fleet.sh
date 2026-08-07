#!/usr/bin/env bash
#
# deploy_fleet.sh -- get a turbovec-search aarch64 binary and the bench
# node scripts onto the pi fleet.
#
# usage:
#   deploy_fleet.sh <host>...      deploy to the named pis
#   deploy_fleet.sh all            deploy to every reachable pi in inventory
#   deploy_fleet.sh doctor [host...]  per-host health check (no changes)
#   deploy_fleet.sh -h             this help
#
# The binary comes from, in order:
#   1. BINARY=<path>               a prebuilt aarch64 binary you supply
#   2. a local cross build         only when the aarch64 target AND a
#                                  linker are already installed -- this
#                                  script never installs anything
# If neither is available it prints the exact one-time setup and stops.
#
# Per host it creates $PI_REMOTE_DIR/{bin,run,logs,shards}, rsyncs the
# binary to bin/, and installs start-node.sh / stop-bench.sh (nice -n 10,
# analysis pointed at krick's sidecar). Nothing is started here; run
# run_matrix.sh fleet for that.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
# shellcheck source=inventory.env
source "$HERE/inventory.env"

die() { echo "deploy_fleet: $*" >&2; exit 1; }
say() { echo "== $*"; }

usage() {
  sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# --- binary ---------------------------------------------------------------

cross_setup_ready() {
  rustup target list --installed 2>/dev/null | grep -qx aarch64-unknown-linux-gnu || return 1
  # A linker, either on PATH or pinned in cargo config.
  command -v aarch64-linux-gnu-gcc >/dev/null && return 0
  grep -rqs 'aarch64-unknown-linux-gnu' "$HOME/.cargo/config.toml" "$HOME/.cargo/config" 2>/dev/null
}

print_cross_setup_instructions() {
  cat >&2 <<'EOF'
deploy_fleet: no aarch64 binary and no working cross setup found.

One-time setup on this box (then re-run):

  rustup target add aarch64-unknown-linux-gnu
  sudo apt install gcc-aarch64-linux-gnu
  cat >> ~/.cargo/config.toml <<'TOML'
  [target.aarch64-unknown-linux-gnu]
  linker = "aarch64-linux-gnu-gcc"
  TOML

Or skip the cross toolchain entirely and hand over a binary built
elsewhere (e.g. natively on a pi):

  BINARY=/path/to/turbovec-search deploy_fleet.sh all
EOF
}

ensure_binary() {
  if [[ -n ${BINARY:-} ]]; then
    [[ -x $BINARY ]] || die "BINARY=$BINARY is not an executable file"
  elif cross_setup_ready; then
    say "cross-building aarch64 release binary (this takes a while)"
    (cd "$REPO" && cargo build --release --target aarch64-unknown-linux-gnu -j 8)
    BINARY="$REPO/target/aarch64-unknown-linux-gnu/release/turbovec-search"
    [[ -x $BINARY ]] || die "cross build finished but $BINARY is missing"
  else
    print_cross_setup_instructions
    exit 1
  fi
  file "$BINARY" | grep -qi aarch64 ||
    die "$BINARY is not an aarch64 binary (file says: $(file -b "$BINARY" | cut -d, -f1-2))"
  say "binary: $BINARY"
}

# --- deploy ----------------------------------------------------------------

deploy_host() {
  local h=$1 ip root
  ip=$(host_ipv4 "$h" 2>/dev/null) || die "$h: ssh failed (host down?)"
  [[ -n $ip ]] || die "$h: ssh ok but no IPv4 from hostname -I"
  root=$(host_root "$h")
  say "$h ($ip): layout + scripts"
  local tmp; tmp=$(mktemp -d)
  bench_gen_scripts "$tmp" "$NICE_PI"
  host_sh "$h" "mkdir -p $(printf '%q' "$root")/bin $(printf '%q' "$root")/run $(printf '%q' "$root")/logs $(printf '%q' "$root")/shards" ||
    die "$h: could not create $root"
  rsync -a "$tmp/start-node.sh" "$tmp/stop-bench.sh" "$h:$root/" ||
    die "$h: rsync of node scripts failed"
  rm -rf "$tmp"
  say "$h ($ip): binary"
  rsync -aW --partial --info=progress2 "$BINARY" "$h:$root/bin/turbovec-search" ||
    die "$h: rsync of binary failed"
  host_sh "$h" "chmod +x $(printf '%q' "$root")/bin/turbovec-search" ||
    die "$h: chmod failed"
  say "$h done (run scripts + binary under $root)"
}

doctor_host() {
  local h=$1 root ip
  root=$(host_root "$h")
  if ! ip=$(host_ipv4 "$h" 2>/dev/null); then
    printf '%-8s ssh UNREACHABLE\n' "$h"
    return 1
  fi
  printf '%-8s ssh ok (%s)\n' "$h" "$ip"
  host_sh "$h" "
    b=$(q "$root")/bin/turbovec-search
    if [[ -x \"\$b\" ]]; then echo \"  binary   present+exec\"; else echo \"  binary   MISSING (\$b)\"; fi
    echo \"  disk     \$(d=$(q "$root"); while [[ ! -d \"\$d\" && \"\$d\" != / ]]; do d=\$(dirname \"\$d\"); done; df -h \"\$d\" | awk -v d=\"\$d\" 'NR==2{print \$4\" free of \"\$2\" (at \"d\")\"}')\"
    echo \"  memory   \$(free -g | awk '/Mem:/{print \$7\" GB avail of \"\$2}')\"
    if (exec 3<>/dev/tcp/127.0.0.1/$FLEET_PORT) 2>/dev/null; then echo '  port     $FLEET_PORT BUSY'; else echo '  port     $FLEET_PORT free'; fi
  " || printf '  doctor script failed\n'
}

# --- main ------------------------------------------------------------------

((${#@} > 0)) || usage 1
case ${1:-} in -h | --help) usage 0 ;; esac

cmd=$1
shift
hosts=()
case $cmd in
  doctor)
    if (($# > 0)); then hosts=("$@"); else hosts=("${PI_HOSTS[@]}"); fi
    rc=0
    for h in "${hosts[@]}"; do doctor_host "$h" || rc=1; done
    exit $rc
    ;;
  all) hosts=("${PI_HOSTS[@]}") ;;
  *) hosts=("$cmd" "$@") ;;
esac

((${#hosts[@]} > 0)) || die "no hosts given"
ensure_binary
deployed=0
for h in "${hosts[@]}"; do
  deploy_host "$h" && deployed=$((deployed + 1))
done
say "deployed to $deployed/${#hosts[@]} host(s)"
