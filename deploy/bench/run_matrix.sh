#!/usr/bin/env bash
#
# run_matrix.sh -- the standard post-engine-update benchmark matrix.
#
# usage:
#   run_matrix.sh <solo|duo|fleet|all> [--shard-set=PATH] [--k=list]
#                 [--concurrency=list]
#   run_matrix.sh teardown           stop every bench process, every host
#   run_matrix.sh -h                 this help
#
# The three setups:
#   solo   all shards on krick-1, coordinator on krick-1. The fast-box
#          ceiling: what the engine does with no slow collaborator.
#   duo    shards split evenly across krick-1 + krick, coordinator on
#          krick. Two fast collaborators.
#   fleet  shard 0 on krick-1, shard 1 on krick, shards 2.. round-robin
#          over the live pis, coordinator on krick. The floor-scout
#          measurement: two fast collaborators plus a fleet of slow ones.
#
# Every setup stages the shard files (rsync -aW --partial; rsync's own
# size+mtime check skips files a host already has), starts a floor-sharing
# node AND a no-sharing twin per shard (same files, twin doubles as the
# hedge replica), starts the coordinator, waits for real readiness with
# the same v7_verify --ready-only poll rebuild.sh uses, then runs
# cluster_sweep twice -- concurrency 1 (40 queries + 5 warmup) and
# concurrency 8 (64 + 5) -- over k=10,100,1000,10000. The sweep's
# bitwise correctness gate (sharing on/off must return identical hits)
# must pass; if it fails the script exits 1 and leaves every process and
# log in place for debugging (tear down with `run_matrix.sh teardown`).
# On success it tears down what it started and appends one summary line
# to $BENCH_OUT/history.jsonl.
#
# Ports: on each host the n-th shard it serves uses FLEET_PORT+2n for the
# sharing node and TWIN_PORT+2n+1 for the twin (59700/59701 for a host
# serving one pair -- the previous fleet's fixed convention). The bench
# coordinator listens on COORD_PORT 59295, NOT the live fleet's 59291.
# This suite never touches the live v7 fleet (ports 59300-59307/59291)
# beyond read-only analysis calls to the shared sidecar on 59202.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
# shellcheck source=inventory.env
source "$HERE/inventory.env"

BIN_LOCAL=${BIN_LOCAL:-$REPO/target/release/turbovec-search}
SWEEP=${SWEEP:-$REPO/target/release/examples/cluster_sweep}
VERIFY=${VERIFY:-$REPO/target/release/examples/v7_verify}
# Where krick-1's own checkout would put a release binary, if it has one.
KRICK1_CLONE_BIN=/work/worktrees/turbovec-workspace/turbovec-search/target/release/turbovec-search

die() { echo "run_matrix: $*" >&2; exit 1; }
say() { echo "== $*"; }

usage() {
  sed -n '2,36p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# --- args ------------------------------------------------------------------

SETUP=""
K_LIST=10,100,1000,10000
CONCURRENCY_LIST=1,8
for a in "$@"; do
  case $a in
    -h | --help) usage 0 ;;
    --shard-set=*) SHARD_SET=${a#*=} ;;
    --k=*) K_LIST=${a#*=} ;;
    --concurrency=*) CONCURRENCY_LIST=${a#*=} ;;
    solo | duo | fleet | all | teardown)
      [[ -z $SETUP ]] || die "one setup at a time (got $SETUP and $a)"
      SETUP=$a
      ;;
    *) die "unknown argument: $a" ;;
  esac
done
[[ -n $SETUP ]] || usage 1

# --- shared state ------------------------------------------------------------

declare -a SHARD_IDS=()          # shard ordinals discovered under SHARD_SET
declare -A SHARD_HOST=()         # shard ordinal -> host
declare -A SHARD_PORT=()         # shard ordinal -> sharing port (twin is +1... see below)
declare -A HOST_SHARDS=()        # host -> space-separated shard ordinals
declare -A HOST_BIN=()           # host -> turbovec-search path on that host
N_SHARDS=0

# host_cmd helper for embedding values safely into remote scripts (q lives
# in inventory.env).

# must <host> <what> <script>: run a script on a host, fail fast naming it.
must() {
  local host=$1 what=$2 script=$3
  host_sh "$host" "$script" || die "$host: $what failed"
}

# --- shard discovery -----------------------------------------------------------

discover_shards() {
  [[ -d $SHARD_SET ]] || die "no shard set at $SHARD_SET (build one with deploy/v7-rebuild/rebuild.sh, or pass --shard-set=)"
  shopt -s nullglob
  local f i
  local files=("$SHARD_SET"/shard-*.tv)
  shopt -u nullglob
  ((${#files[@]} > 0)) || die "$SHARD_SET holds no shard-*.tv files"
  for f in "${files[@]}"; do
    i=$(basename "$f" .tv)
    i=${i#shard-}
    [[ -f $f.bm25 ]] || die "$f has no .bm25 beside it"
    [[ ! -d $f.bm25.build ]] || die "$f.bm25.build present: interrupted build, not servable"
    SHARD_IDS+=("$i")
  done
  # Sort numerically so shard 7 never lands before shard 0.
  IFS=$'\n' read -r -d '' -a SHARD_IDS < <(printf '%s\n' "${SHARD_IDS[@]}" | sort -n && printf '\0')
  N_SHARDS=${#SHARD_IDS[@]}
  say "shard set $SHARD_SET: $N_SHARDS shards (${SHARD_IDS[*]})"
}

# --- assignment ------------------------------------------------------------

# note_shard <host> <shard>: record the assignment and its per-host port.
note_shard() {
  local host=$1 shard=$2 ord
  ord=$(wc -w <<<"${HOST_SHARDS[$host]:-}" | tr -d ' ')
  HOST_SHARDS[$host]="${HOST_SHARDS[$host]:-} $shard"
  SHARD_HOST[$shard]=$host
  SHARD_PORT[$shard]=$((FLEET_PORT + 2 * ord))
}

assign_shards() {
  local setup=$1 i
  case $setup in
    solo)
      for i in "${SHARD_IDS[@]}"; do note_shard krick-1 "$i"; done
      ;;
    duo)
      local half=$(((N_SHARDS + 1) / 2)) pos=0
      for i in "${SHARD_IDS[@]}"; do
        if ((pos < half)); then note_shard krick-1 "$i"; else note_shard krick "$i"; fi
        pos=$((pos + 1))
      done
      ;;
    fleet)
      note_shard krick-1 "${SHARD_IDS[0]}"
      note_shard krick "${SHARD_IDS[1]}"
      resolve_pis || die "fleet setup needs at least one live pi (resolve_pis found none)"
      local p=0
      for i in "${SHARD_IDS[@]:2}"; do
        note_shard "${LIVE_PIS[$((p % ${#LIVE_PIS[@]}))]}" "$i"
        p=$((p + 1))
      done
      ;;
    *) die "internal: unknown setup $setup" ;;
  esac
  say "$setup assignment:"
  for i in "${SHARD_IDS[@]}"; do
    printf '  shard %-3s %-9s :%d/:%d\n' "$i" "${SHARD_HOST[$i]}" \
      "${SHARD_PORT[$i]}" "$((SHARD_PORT[$i] + 1))"
  done
}

# The hosts this setup uses, in first-use order.
setup_hosts() {
  local h seen=" "
  for i in "${SHARD_IDS[@]}"; do
    h=${SHARD_HOST[$i]}
    [[ $seen == *" $h "* ]] || { seen+="$h "; printf '%s\n' "$h"; }
  done
}

# --- staging ----------------------------------------------------------------

resolve_fast_ips() {
  local h ip
  for h in "${FAST_HOSTS[@]}"; do
    ip=$(host_ipv4 "$h") || die "$h: cannot resolve LAN IPv4 (ssh down?)"
    [[ -n $ip ]] || die "$h: hostname -I returned nothing"
    HOST_IP[$h]=$ip
  done
}

stage_host() {
  local host=$1 sdir files=() i
  sdir=$(host_shard_dir "$host")
  if [[ $host == krick && -z $(bench_ssh_target krick) ]]; then
    say "krick serves shards straight from $sdir (no staging)"
    return
  fi
  for i in ${HOST_SHARDS[$host]}; do
    files+=("$SHARD_SET/shard-$i.tv" "$SHARD_SET/shard-$i.tv.bm25")
    [[ -f $SHARD_SET/shard-$i.tv.wal ]] && files+=("$SHARD_SET/shard-$i.tv.wal")
  done
  # Disk preflight: refuse by name before a multi-GB rsync runs out of
  # room halfway (cm5ai1's eMMC is the case this guards).
  local need=0 f
  for f in "${files[@]}"; do need=$((need + $(stat -c%s "$f"))); done
  must "$host" "mkdir $sdir" "mkdir -p $(q "$sdir")"
  local avail
  avail=$(host_sh "$host" "df -B1 --output=avail $(q "$sdir") | tail -1 | tr -d ' '") ||
    die "$host: cannot check free space at $sdir"
  if ((avail < need)); then
    die "$host: $sdir has $((avail / 2**30)) GB free, needs $((need / 2**30)) GB -- skipping (free space or pick another host)"
  fi
  say "staging ${#files[@]} files ($((need / 2**30)) GB) to $host:$sdir ($((avail / 2**30)) GB free)"
  rsync -aW --partial --info=progress2 "${files[@]}" "$host:$sdir/" ||
    die "$host: shard staging rsync failed"
}

# --- binaries and scripts ----------------------------------------------------

ensure_host_binary() {
  local host=$1 root
  root=$(host_root "$host")
  case $host in
    krick)
      [[ -x $BIN_LOCAL ]] || die "no local binary at $BIN_LOCAL (cargo build --release)"
      HOST_BIN[$host]=$BIN_LOCAL
      ;;
    krick-1)
      if host_sh krick-1 "[[ -x $(q "$KRICK1_CLONE_BIN") ]]"; then
        HOST_BIN[$host]=$KRICK1_CLONE_BIN
      else
        [[ -x $BIN_LOCAL ]] || die "no local binary at $BIN_LOCAL to rsync to krick-1"
        say "krick-1 has no built clone; rsyncing this repo's release binary"
        must krick-1 "mkdir $root/bin" "mkdir -p $(q "$root")/bin"
        rsync -aW --partial "$BIN_LOCAL" "krick-1:$root/bin/turbovec-search" ||
          die "krick-1: binary rsync failed"
        HOST_BIN[$host]=$root/bin/turbovec-search
      fi
      ;;
    *)
      host_sh "$host" "[[ -x $(q "$root")/bin/turbovec-search ]]" ||
        die "$host: no binary at $root/bin/turbovec-search -- run deploy_fleet.sh $host first"
      HOST_BIN[$host]=$root/bin/turbovec-search
      ;;
  esac
}

install_host_scripts() {
  local host=$1 nice=$2 root tmp
  root=$(host_root "$host")
  tmp=$(mktemp -d)
  bench_gen_scripts "$tmp" "$nice"
  must "$host" "mkdir $root" "mkdir -p $(q "$root")/run $(q "$root")/logs"
  if [[ -z $(bench_ssh_target "$host") ]]; then
    cp "$tmp/start-node.sh" "$tmp/stop-bench.sh" "$root/"
  else
    rsync -a "$tmp/start-node.sh" "$tmp/stop-bench.sh" "$host:$root/" ||
      die "$host: rsync of node scripts failed"
  fi
  rm -rf "$tmp"
}

# --- bring-up ----------------------------------------------------------------

start_host_nodes() {
  local host=$1 root sdir i port script=""
  root=$(host_root "$host")
  sdir=$(host_shard_dir "$host")
  for i in ${HOST_SHARDS[$host]}; do
    port=${SHARD_PORT[$i]}
    script+="$(q "$root")/start-node.sh $(q "$sdir/shard-$i.tv") $port $((i * SLOT_STRIDE)) true"$'\n'
    script+="$(q "$root")/start-node.sh $(q "$sdir/shard-$i.tv") $((port + 1)) $((i * SLOT_STRIDE)) false"$'\n'
  done
  say "starting nodes on $host"
  must "$host" "node bring-up" "$script"
}

start_coordinator() {
  local host=$COORD_HOST root nodes="" i script
  root=$(host_root "$host")
  for i in "${SHARD_IDS[@]}"; do
    nodes+="${nodes:+,}${HOST_IP[${SHARD_HOST[$i]}]}:${SHARD_PORT[$i]}"
  done
  script="
    mkdir -p $(q "$root")/run $(q "$root")/logs
    pidfile=$(q "$root")/run/bench-coordinator.pid
    if [[ -f \$pidfile ]] && kill -0 \"\$(cat \"\$pidfile\")\" 2>/dev/null; then
      echo \"bench coordinator already running (pid \$(cat \"\$pidfile\"))\" >&2
      exit 1
    fi
    nice -n $NICE_FAST $(q "${HOST_BIN[$host]}") --role=coordinator \
      --coord-listen=0.0.0.0:$COORD_PORT \
      --nodes=$(q "$nodes") \
      --chunk-blocks=$CHUNK_BLOCKS \
      --stream-search \
      --analysis-addr=$(q "$SIDECAR_ADDR") \
      >>$(q "$root")/logs/bench-coordinator.log 2>&1 &
    echo \$! >\"\$pidfile\"
    for _ in \$(seq 1 120); do
      (exec 3<>/dev/tcp/127.0.0.1/$COORD_PORT) 2>/dev/null && { echo \"coordinator on :$COORD_PORT\"; exit 0; }
      kill -0 \$! 2>/dev/null || { echo 'coordinator exited; see logs/bench-coordinator.log' >&2; exit 1; }
      sleep 1
    done
    echo 'coordinator never opened :$COORD_PORT' >&2
    exit 1
  "
  say "starting coordinator on $host (:$COORD_PORT, nodes: $nodes)"
  must "$host" "coordinator bring-up" "$script"
}

wait_ready() {
  local addr=${HOST_IP[$COORD_HOST]}:$COORD_PORT waited
  [[ -x $VERIFY ]] || die "no v7_verify at $VERIFY (cargo build --release --examples)"
  # READY_TIMEOUT_S: cold-starting 8x57G shards pages postings in far
  # slower than 600s on a fresh box (measured: >750s on krick-1).
  local budget=${READY_TIMEOUT_S:-3600}
  say "waiting for all $N_SHARDS shards to answer via $addr (postings still paging in; budget ${budget}s)"
  for waited in $(seq 1 $((budget / 2))); do
    "$VERIFY" --coord="$addr" --shards="$N_SHARDS" \
      --ready-only --wait-ready=2 >/dev/null 2>&1 && { say "fleet ready after ~$((waited * 2))s"; return; }
    sleep 2
  done
  die "fleet never became ready in ${budget}s; processes left up, logs under each host's $(host_root "$COORD_HOST")/logs"
}

# --- measurement -------------------------------------------------------------

run_sweeps() {
  local setup=$1 date=$2 jsonl=$3
  local sharing="" twin="" i c q w label
  for i in "${SHARD_IDS[@]}"; do
    sharing+="${sharing:+,}${HOST_IP[${SHARD_HOST[$i]}]}:${SHARD_PORT[$i]}"
    twin+="${twin:+,}${HOST_IP[${SHARD_HOST[$i]}]}:$((SHARD_PORT[$i] + 1))"
  done
  [[ -x $SWEEP ]] || die "no cluster_sweep at $SWEEP (cargo build --release --examples)"
  [[ -f $PROBES ]] || die "no probe embeddings at $PROBES"
  SWEEP_LABELS=()
  for c in ${CONCURRENCY_LIST//,/ }; do
    # The standard cells: c=1 measures latency (40q+5w), c=8 throughput (64q+5w).
    q=64
    ((c == 1)) && q=40
    w=5
    label="$setup-c$c-$date"
    SWEEP_LABELS+=("$label")
    say "sweep: $label (k=$K_LIST, $q queries + $w warmup)"
    if ! "$SWEEP" \
      --nodes-sharing="$sharing" \
      --nodes-nosharing="$twin" \
      --k="$K_LIST" \
      --queries="$q" --warmup="$w" --concurrency="$c" \
      --probes-from="$PROBES" \
      --label="$label" \
      --json="$jsonl"; then
      say "CORRECTNESS GATE FAILED (or sweep errored) for $label."
      say "Leaving every process up and every log in place for debugging."
      say "Tear down with: $0 teardown"
      exit 1
    fi
  done
}

summarize() {
  local setup=$1 date=$2 jsonl=$3 engine_rev tv_rev labels
  engine_rev=$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)
  tv_rev=$(sed -n '/^name = "turbovec"$/,/^$/p' "$REPO/Cargo.lock" |
    sed -n 's/.*#\([0-9a-f]\{8\}\).*/\1/p' | head -1)
  labels=$(printf '%s\n' "${SWEEP_LABELS[@]}" | jq -R . | jq -cs .)
  jq -cs --arg setup "$setup" --arg date "$date" \
    --arg eng "$engine_rev" --arg tv "${tv_rev:-unknown}" --argjson labels "$labels" \
    '{setup: $setup, date: $date, engine_rev: $eng, turbovec_rev: $tv,
      cells: [.[] | select(.label as $l | $labels | index($l))
              | {label, k, floor_sharing, qps, wall_p50_ms}]}' \
    "$jsonl" >>"$BENCH_OUT/history.jsonl"
  say "summary appended to $BENCH_OUT/history.jsonl"
}

# --- teardown ---------------------------------------------------------------

teardown_hosts() {
  local hosts=("$@") h root
  ((${#hosts[@]} > 0)) || { say "nothing to tear down"; return; }
  for h in "${hosts[@]}"; do
    root=$(host_root "$h")
    if host_sh "$h" "[[ -x $(q "$root")/stop-bench.sh ]]" 2>/dev/null; then
      say "tearing down $h"
      host_sh "$h" "bash $(q "$root")/stop-bench.sh" ||
        echo "run_matrix: $h: teardown reported failure; check $root/run manually" >&2
    else
      say "$h: no bench scripts, nothing to stop"
    fi
  done
}

# --- main --------------------------------------------------------------------

COORD_HOST=krick
SWEEP_LABELS=()

case $SETUP in
  teardown)
    ALL_HOSTS=("${FAST_HOSTS[@]}")
    if resolve_pis 2>/dev/null; then ALL_HOSTS+=("${LIVE_PIS[@]}"); fi
    teardown_hosts "${ALL_HOSTS[@]}"
    exit 0
    ;;
  all) SETUPS=(solo duo fleet) ;;
  *) SETUPS=("$SETUP") ;;
esac

for SETUP in "${SETUPS[@]}"; do
  DATE=$(date +%Y%m%d-%H%M%S)
  JSONL="$BENCH_OUT/$SETUP-$DATE.jsonl"
  mkdir -p "$BENCH_OUT"
  say "setup $SETUP ($DATE)"
  # Fresh state per setup: discovery and assignment accumulate.
  SHARD_IDS=()
  SHARD_HOST=()
  SHARD_PORT=()
  HOST_SHARDS=()
  HOST_BIN=()
  discover_shards
  resolve_fast_ips
  assign_shards "$SETUP"
  [[ $SETUP == solo ]] && COORD_HOST=krick-1 || COORD_HOST=krick
  mapfile -t HOSTS < <(setup_hosts)
  for h in "${HOSTS[@]}"; do stage_host "$h"; done
  for h in "${HOSTS[@]}"; do
    ensure_host_binary "$h"
    case $h in
      krick | krick-1) install_host_scripts "$h" "$NICE_FAST" ;;
      *) install_host_scripts "$h" "$NICE_PI" ;;
    esac
  done
  for h in "${HOSTS[@]}"; do start_host_nodes "$h"; done
  start_coordinator
  wait_ready
  run_sweeps "$SETUP" "$DATE" "$JSONL"
  summarize "$SETUP" "$DATE" "$JSONL"
  teardown_hosts "${HOSTS[@]}"
  say "setup $SETUP done; cells in $JSONL"
done
