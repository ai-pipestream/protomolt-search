#!/usr/bin/env bash
#
# v7 rebuild driver: build a fresh multi-field shard set from the raw
# chunk texts + embeddings, with block-aligned shard cuts.
#
# The .tv format is v7 (per-block TQ+ calibration) and the .bm25 format is
# v6 (multi-field). Neither reads an older file, so adoption is a rebuild,
# never a migration -- see docs/multi-field.md and README-block-max.md.
#
# Usage: rebuild.sh <stage> [stage...]
#
#   plan       print the corpus math, the cut plan, and the disk estimate
#   up         start the analysis sidecar and N EMPTY shard nodes
#   sidecar    start just the analysis sidecar (it is shared by every node)
#   calibrate  fit the seed calibration once into $OUT/calibration.json
#   ingest     run one court_ingest driver per shard, in parallel
#   down       stop the nodes (they flush their indexes on SIGTERM)
#   serve      restart the nodes on the built shards, plus a coordinator
#   stop       stop everything this script started
#   status     show what is running
#
# Every stage is idempotent-ish and safe to run alone; `up` refuses to
# start on top of an existing index unless FORCE=1.
#
# Configuration is environment variables, all with full-corpus defaults:
#
#   OUT, CHUNKS, EMB, CASE_NAMES, SHARDS, DIM, BLOCK, CHUNK_BLOCKS,
#   FIELDS, PORT_BASE, COORD_PORT, SIDECAR_PORT, SIDECAR_BIN,
#   OFFSET_STRIDE, EMBEDDINGS_DIR, BIN, INGEST,
#   WAVE (shards ingesting at once), DISK_MARGIN_GB, BODY_COLUMNS
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

OUT=${OUT:-/work/court-corpus/shards-v7}
CHUNKS=${CHUNKS:-/work/court-corpus/chunks-full.ndjson}
EMB=${EMB:-/work/court-corpus/embeddings-full.bin}
CASE_NAMES=${CASE_NAMES:-/work/court-corpus/case-names.tsv}
SHARDS=${SHARDS:-8}
DIM=${DIM:-256}
BLOCK=${BLOCK:-8192}
CHUNK_BLOCKS=${CHUNK_BLOCKS:-8192}
FIELDS=${FIELDS:-body,case_name}
# A/B columns: extra copies of the body text under different analysis,
# as extra BM25 fields over the SAME slot space, so comparing two
# analysis chains is a query-time choice of field rather than a second
# index. Format: name:tokenizer:stemmer:source,... (numeric sidecar enum
# values). Every name listed here must also appear in FIELDS, after
# "body" and before "case_name". A body column costs roughly another
# whole .bm25, so this is for slices, not for the full corpus.
#
#   FIELDS=body,body_norm,case_name BODY_COLUMNS=body_norm:1:2:3
#
# (source 3 = SOURCE_NORMALIZED_STEMS: rungs run, then the stemmer.)
BODY_COLUMNS=${BODY_COLUMNS:-}
PORT_BASE=${PORT_BASE:-59300}
COORD_PORT=${COORD_PORT:-59291}
SIDECAR_PORT=${SIDECAR_PORT:-59202}
# The analysis sidecar MUST implement AnalyzeStream. Without it the node
# falls back to one unary call per document, and the sidecar's server
# GOAWAYs the connection after ~70 streams, which reaches the bulk driver
# as an opaque "h2 protocol error" mid-ingest. The JVM distribution is
# built straight from the repo (./gradlew installDist); the checked-in
# native image may predate the RPC -- `strings <bin> | grep AnalyzeStream`
# is the check.
SIDECAR_BIN=${SIDECAR_BIN:-/work/worktrees/turbovec-workspace/grpc-opennlp-analysis/build/install/grpc-opennlp-analysis/bin/grpc-opennlp-analysis}
# Bounded sidecar heap: the default max heap is a quarter of RAM, which is
# tens of GB on this box, and ingest runs it alongside N shard nodes.
export JAVA_OPTS=${JAVA_OPTS:--Xmx4g}
# The static (Model2Vec) embedding model the sidecar serves query
# embeddings from. Ingest does not need it -- vectors come from the
# embeddings file -- but every query path does, so the serve stage would
# otherwise come up unable to embed a query.
EMBEDDINGS_DIR=${EMBEDDINGS_DIR:-/work/court-corpus/models/minilm-l6-v2-static}
BIN=${BIN:-$REPO/target/release/turbovec-search}
INGEST=${INGEST:-$REPO/target/release/examples/court_ingest}
PROBE=${PROBE:-$REPO/target/release/examples/analyze_probe}
VERIFY=${VERIFY:-$REPO/target/release/examples/v7_verify}

RUN="$OUT/run"
LOGS="$OUT/logs"

die() { echo "rebuild: $*" >&2; exit 1; }
say() { echo "== $*"; }

# --- corpus math ------------------------------------------------------
#
# The embeddings file is fixed stride, so the corpus size is arithmetic on
# its length -- no walk of the 126 GB chunks file. The chunk/embedding
# join is verified per position during ingest, which is the real check.

[[ -f $EMB ]] || die "no embeddings file at $EMB"
REC=$((12 + DIM * 4))
EMB_BYTES=$(stat -c%s "$EMB")
(((EMB_BYTES - 12) % REC == 0)) ||
  die "$EMB is $EMB_BYTES bytes: not a 12-byte header plus whole dim-$DIM records"
M=$(((EMB_BYTES - 12) / REC))

FULL_BLOCKS=$((M / BLOCK))
TAIL=$((M % BLOCK))
((FULL_BLOCKS >= SHARDS)) || die "$M chunks is fewer than $BLOCK x $SHARDS: raise BLOCK or drop SHARDS"
BASE_BLOCKS=$((FULL_BLOCKS / SHARDS))
EXTRA=$((FULL_BLOCKS % SHARDS))

# Rows per shard: whole blocks everywhere, with the corpus tail landing on
# the LAST shard. Whole-block shards are what makes a distributed scan
# bitwise-equal to a monolithic one under per-block calibration: a sealed
# block refits on exactly its own rows, so aligned cuts give every shard
# the same block content the monolith had. The tail rides its shard's last
# seal, which is the monolith's last seal too -- hence the tail goes last.
declare -a ROWS STARTS OFFSETS
start=0
for ((i = 0; i < SHARDS; i++)); do
  blocks=$((BASE_BLOCKS + (i < EXTRA ? 1 : 0)))
  rows=$((blocks * BLOCK))
  ((i == SHARDS - 1)) && rows=$((rows + TAIL))
  ROWS[i]=$rows
  STARTS[i]=$start
  start=$((start + rows))
done
((start == M)) || die "internal: cut plan covers $start of $M chunks"

MAXROWS=0
for ((i = 0; i < SHARDS; i++)); do ((ROWS[i] > MAXROWS)) && MAXROWS=${ROWS[i]}; done
# Global id space: shard i owns [i*STRIDE, i*STRIDE + rows). A block-
# aligned stride keeps `global_id / BLOCK` a unique (shard, block) pair,
# and the default leaves 100% headroom for later appends.
if [[ -n ${OFFSET_STRIDE:-} ]]; then
  STRIDE=$OFFSET_STRIDE
else
  STRIDE=$(((MAXROWS * 2 + BLOCK - 1) / BLOCK * BLOCK))
fi
((STRIDE % BLOCK == 0)) || die "OFFSET_STRIDE=$STRIDE is not a multiple of $BLOCK"
((STRIDE >= MAXROWS)) || die "OFFSET_STRIDE=$STRIDE is smaller than the largest shard ($MAXROWS)"
for ((i = 0; i < SHARDS; i++)); do OFFSETS[i]=$((i * STRIDE)); done

SPLITS=""
for ((i = 1; i < SHARDS; i++)); do SPLITS="${SPLITS:+$SPLITS,}${STARTS[i]}"; done

# --- disk model -------------------------------------------------------
#
# Bytes per chunk, measured on the 827k canary rebuild (dim 256, 4-bit,
# body + case_name). They scale linearly: the postings, the stored texts,
# and the log are all per-document.
#
# The build directory is what makes a rebuild disk-hungry: while a shard
# ingests, its texts spill and sort runs sit on disk at ~1.5x the size of
# the .bm25 they eventually become, and both exist at once during Flush.
# So the peak is NOT the finished size -- it is
#   (finished shards) + (concurrently building shards, mid-Flush)
# and WAVE is the knob that bounds the second term.
B_TV=${B_TV:-133}
B_BM25=${B_BM25:-5178}
B_WAL=${B_WAL:-2480}
B_BUILD=${B_BUILD:-7630}
# How many shards ingest at once. Fewer = lower peak, same total work.
WAVE=${WAVE:-$SHARDS}
((WAVE >= 1 && WAVE <= SHARDS)) || die "WAVE=$WAVE must be between 1 and $SHARDS"
# Free space a wave must leave behind, so a mis-modelled shard cannot
# fill the filesystem out from under the rest of the box.
DISK_MARGIN_GB=${DISK_MARGIN_GB:-100}

free_gb() { df --output=avail -k "$OUT_PARENT" | tail -1 | awk '{printf "%d", $1/1048576}'; }
OUT_PARENT=$(dirname "$OUT")

# Finished-shard and mid-flush footprints, in bytes, for an average shard.
shard_finished_bytes() { echo $((M / SHARDS * (B_TV + B_BM25 + B_WAL))); }
shard_flushing_bytes() { echo $((M / SHARDS * (B_TV + B_BM25 + B_WAL + B_BUILD))); }
steady_gb() { echo $((M * (B_TV + B_BM25 + B_WAL) / 1000000000)); }
# Worst moment: the last wave flushing while every earlier wave sits
# finished on disk.
peak_gb() {
  echo $(((SHARDS - WAVE) * $(shard_finished_bytes) / 1000000000 +
    WAVE * $(shard_flushing_bytes) / 1000000000))
}

NODE_LIST=""
for ((i = 0; i < SHARDS; i++)); do
  NODE_LIST="${NODE_LIST:+$NODE_LIST,}127.0.0.1:$((PORT_BASE + i))"
done

# --- stages -----------------------------------------------------------

stage_plan() {
  say "corpus"
  printf '  embeddings   %s (%s bytes, dim %d, %d records)\n' "$EMB" "$EMB_BYTES" "$DIM" "$M"
  printf '  chunks       %s\n' "$CHUNKS"
  printf '  case names   %s (%s lines)\n' "$CASE_NAMES" \
    "$([[ -f $CASE_NAMES ]] && wc -l <"$CASE_NAMES" || echo MISSING)"
  say "block math"
  printf '  %d chunks = %d x %d + %d\n' "$M" "$FULL_BLOCKS" "$BLOCK" "$TAIL"
  printf '  %d shards: %d whole blocks each, %d shards get one more, tail on the last\n' \
    "$SHARDS" "$BASE_BLOCKS" "$EXTRA"
  say "cut plan (--split-points=$SPLITS)"
  printf '  %-5s %-12s %-12s %-12s %s\n' shard start rows blocks slot_offset
  for ((i = 0; i < SHARDS; i++)); do
    printf '  %-5d %-12d %-12d %-12s %d\n' "$i" "${STARTS[i]}" "${ROWS[i]}" \
      "$((ROWS[i] / BLOCK))$( ((i == SHARDS - 1 && TAIL > 0)) && echo "+$TAIL")" "${OFFSETS[i]}"
  done
  say "disk (per-chunk constants measured on the 827k canary)"
  printf '  out dir      %s\n' "$OUT"
  printf '  free now     %d GB\n' "$(free_gb)"
  printf '  %-12s %6d B/chunk  %6d GB\n' vectors "$B_TV" "$((M * B_TV / 1000000000))"
  printf '  %-12s %6d B/chunk  %6d GB\n' postings "$B_BM25" "$((M * B_BM25 / 1000000000))"
  printf '  %-12s %6d B/chunk  %6d GB\n' wal "$B_WAL" "$((M * B_WAL / 1000000000))"
  printf '  %-12s %6d B/chunk  %6d GB  (transient: texts + sort runs, freed at Flush)\n' \
    build-dir "$B_BUILD" "$((M * B_BUILD / 1000000000))"
  printf '  steady state after the rebuild: %d GB\n' "$(steady_gb)"
  printf '  peak with WAVE=%d concurrent shards: %d GB\n' "$WAVE" "$(peak_gb)"
  say "nodes"
  printf '  %s\n' "$NODE_LIST"
  printf '  fields=%s stream-search=on chunk-blocks=%d\n' "$FIELDS" "$CHUNK_BLOCKS"
}

stage_up() {
  mkdir -p "$OUT" "$RUN" "$LOGS"
  if ! [[ ${FORCE:-0} == 1 ]]; then
    shopt -s nullglob
    local existing=("$OUT"/shard-*.tv)
    shopt -u nullglob
    ((${#existing[@]} == 0)) ||
      die "$OUT already holds ${#existing[@]} shard indexes; this stage builds FRESH shards (FORCE=1 to override)"
  fi
  sidecar_up
  for ((i = 0; i < SHARDS; i++)); do
    [[ -f $RUN/node-$i.pid ]] && kill -0 "$(cat "$RUN/node-$i.pid")" 2>/dev/null &&
      die "node $i already running (pid $(cat "$RUN/node-$i.pid"))"
    start_node "$i"
    say "node $i on :$((PORT_BASE + i)), offset ${OFFSETS[i]}, ${ROWS[i]} rows expected"
  done
}

# Launch one node and wait for it to listen.
#
# The node ports live inside this box's ephemeral range
# (/proc/sys/net/ipv4/ip_local_port_range is 32768-60999), so an unrelated
# outbound connection can be holding one at exactly the wrong moment and
# the bind fails with EADDRINUSE. It is transient, so retry; the permanent
# fix is to reserve the block:
#   sysctl -w net.ipv4.ip_local_reserved_ports=59290-59310
start_node() {
  local i=$1 port=$((PORT_BASE + i)) attempt
  for attempt in 1 2 3 4 5; do
    "$BIN" --role=node \
      --node-listen="127.0.0.1:$port" \
      --index="$OUT/shard-$i.tv" \
      --slot-offset="${OFFSETS[i]}" \
      --chunk-blocks="$CHUNK_BLOCKS" \
      --dim="$DIM" --bit-width=4 \
      --bm25-fields="$FIELDS" \
      --stream-search \
      --analysis-addr="http://127.0.0.1:$SIDECAR_PORT" \
      >>"$LOGS/node-$i.log" 2>&1 &
    local pid=$!
    echo "$pid" >"$RUN/node-$i.pid"
    local waited
    for waited in $(seq 1 120); do
      port_open "$port" && return 0
      kill -0 "$pid" 2>/dev/null || break
      sleep 1
    done
    kill -0 "$pid" 2>/dev/null && die "node $i never opened :$port"
    grep -q 'AddrInUse' "$LOGS/node-$i.log" ||
      die "node $i exited; see $LOGS/node-$i.log"
    say "node $i lost the bind race for :$port (attempt $attempt), retrying"
    sleep 2
  done
  die "node $i could not bind :$port after 5 attempts"
}

stage_sidecar() { sidecar_up; }

# Analysis and embedding are separate capabilities of the same process,
# and a sidecar can hold one without the other: an open port proves
# neither. Probe embedding specifically, because that is the half ingest
# never exercises (vectors come from the file), so a sidecar that cannot
# embed serves a whole rebuild without complaint and then fails the first
# hybrid query.
sidecar_can_embed() {
  "$PROBE" --addr="http://127.0.0.1:$SIDECAR_PORT" --text=probe --embed >/dev/null 2>&1
}

sidecar_up() {
  if port_open "$SIDECAR_PORT"; then
    if [[ ! -x $PROBE ]] || sidecar_can_embed; then
      say "analysis sidecar already listening on :$SIDECAR_PORT"
      return
    fi
    # Most likely its jars were replaced under it: a running JVM loads
    # classes lazily, so a rebuild mid-session leaves the process serving
    # everything it had already touched and unable to load anything it
    # had not. Ingest keeps working; embedding, which ingest never calls,
    # does not.
    say "sidecar on :$SIDECAR_PORT answers but CANNOT EMBED; restarting it"
    if [[ -f $RUN/sidecar.pid ]]; then
      kill "$(cat "$RUN/sidecar.pid")" 2>/dev/null || true
      rm -f "$RUN/sidecar.pid"
    fi
    for _ in $(seq 1 30); do port_open "$SIDECAR_PORT" || break; sleep 1; done
    port_open "$SIDECAR_PORT" &&
      die "sidecar on :$SIDECAR_PORT will not stop; kill it and re-run"
  fi
  [[ -x $SIDECAR_BIN ]] || die "no analysis sidecar binary at $SIDECAR_BIN"
  [[ -d $EMBEDDINGS_DIR ]] || die "no embedding model at EMBEDDINGS_DIR=$EMBEDDINGS_DIR"
  mkdir -p "$RUN" "$LOGS"
  PORT="$SIDECAR_PORT" OPENNLP_EMBEDDINGS_DIR="$EMBEDDINGS_DIR" \
    "$SIDECAR_BIN" >>"$LOGS/sidecar.log" 2>&1 &
  echo $! >"$RUN/sidecar.pid"
  wait_port "$SIDECAR_PORT" "analysis sidecar"
  say "analysis sidecar on :$SIDECAR_PORT (pid $(cat "$RUN/sidecar.pid"))"
}

stage_calibrate() {
  mkdir -p "$OUT" "$LOGS"
  if [[ -f $OUT/calibration.json ]]; then
    say "calibration already at $OUT/calibration.json"
    return
  fi
  say "fitting the seed calibration (streams $EMB once)"
  "$INGEST" --nodes="$NODE_LIST" --embeddings="$EMB" --chunks="$CHUNKS" \
    --chunk-count="$M" --calibration="$OUT/calibration.json" --fit-only 2>&1 |
    tee -a "$LOGS/calibrate.log"
}

stage_ingest() {
  mkdir -p "$LOGS" "$RUN"
  [[ -f $OUT/calibration.json ]] || die "run the calibrate stage first"
  [[ -f $CASE_NAMES ]] || die "no case-name table at $CASE_NAMES"
  say "projected peak $(peak_gb) GB with WAVE=$WAVE, $(free_gb) GB free"
  local first
  for ((first = 0; first < SHARDS; first += WAVE)); do
    local last=$((first + WAVE - 1))
    ((last >= SHARDS)) && last=$((SHARDS - 1))
    # A wave adds the building shards' spill on top of whatever is
    # already on disk. Refuse to start one that cannot finish rather
    # than discovering it hours in with a full filesystem.
    local need=$(((last - first + 1) * $(shard_flushing_bytes) / 1000000000))
    local have; have=$(free_gb)
    ((have > need + DISK_MARGIN_GB)) ||
      die "wave $first..$last needs ~$need GB (+$DISK_MARGIN_GB margin) but only $have GB is free"
    say "wave: shards $first..$last (~$need GB of spill, $have GB free)"
    local pids=()
    for ((i = first; i <= last; i++)); do
      "$INGEST" --nodes="$NODE_LIST" \
        --chunks="$CHUNKS" --embeddings="$EMB" \
        --case-names="$CASE_NAMES" \
        ${BODY_COLUMNS:+--body-columns="$BODY_COLUMNS"} \
        --chunk-count="$M" \
        --split-points="$SPLITS" \
        --calibration="$OUT/calibration.json" \
        --first-shard="$i" --end-shard="$((i + 1))" \
        --analysis-addr="http://127.0.0.1:$SIDECAR_PORT" \
        >>"$LOGS/ingest-$i.log" 2>&1 &
      pids+=($!)
      echo $! >"$RUN/ingest-$i.pid"
      say "  driver for shard $i: ${ROWS[i]} chunks from ${STARTS[i]} (pid ${pids[-1]})"
    done
    local rc=0 p
    for p in "${pids[@]}"; do wait "$p" || rc=$?; done
    ((rc == 0)) || die "an ingest driver failed (rc $rc); see $LOGS/ingest-*.log"
    say "wave $first..$last done, $(free_gb) GB free"
  done
  say "all drivers finished"
  grep -h 'chunks ingested' "$LOGS"/ingest-*.log || true
}

stage_down() {
  local pids=() pid
  for ((i = 0; i < SHARDS; i++)); do
    [[ -f $RUN/node-$i.pid ]] || continue
    pid=$(cat "$RUN/node-$i.pid")
    if kill -0 "$pid" 2>/dev/null; then
      say "stopping node $i (pid $pid); it flushes on the way out"
      kill "$pid"
      pids+=("$pid")
    fi
    rm -f "$RUN/node-$i.pid"
  done
  # Wait on the recorded pids, never on a pattern: a pgrep for the node's
  # own command line also matches the shell running this script.
  for pid in "${pids[@]:-}"; do
    [[ -n $pid ]] || continue
    while kill -0 "$pid" 2>/dev/null; do sleep 2; done
  done
  say "nodes down"
}

stage_serve() {
  mkdir -p "$RUN" "$LOGS"
  # Check the whole set BEFORE starting anything. A missing .bm25 is the
  # one that matters: the node comes up healthy on vectors alone and puts
  # a silent hole in every lexical query, so catching it here beats
  # discovering it from a ranking. (The binary refuses an interrupted
  # build too; this also catches a .bm25 that was never started.)
  local missing=()
  for ((i = 0; i < SHARDS; i++)); do
    [[ -f $OUT/shard-$i.tv ]] || missing+=("shard-$i.tv")
    [[ -f $OUT/shard-$i.tv.bm25 ]] || missing+=("shard-$i.tv.bm25")
    [[ -d $OUT/shard-$i.tv.bm25.build ]] && missing+=("shard-$i: build unfinished")
  done
  ((${#missing[@]} == 0)) || die "not ready to serve: ${missing[*]}"
  sidecar_up
  for ((i = 0; i < SHARDS; i++)); do
    start_node "$i"
  done
  say "serving $SHARDS shards on $NODE_LIST (mmaps page in on first query)"
  "$BIN" --role=coordinator \
    --coord-listen="127.0.0.1:$COORD_PORT" \
    --nodes="$NODE_LIST" \
    --chunk-blocks="$CHUNK_BLOCKS" \
    --stream-search \
    --analysis-addr="http://127.0.0.1:$SIDECAR_PORT" \
    >>"$LOGS/coordinator.log" 2>&1 &
  echo $! >"$RUN/coordinator.pid"
  wait_port "$COORD_PORT" coordinator
  say "coordinator on :$COORD_PORT"
  # An open port is not readiness: a node binds before it opens its
  # .bm25, and opening 50 GB of postings reads every document length.
  # Report when the fleet can actually answer, so the next stage does not
  # measure a cluster that is still loading.
  say "waiting for all $SHARDS shards to answer health (they are still opening their postings)"
  local waited
  for waited in $(seq 1 300); do
    "$VERIFY" --coord="127.0.0.1:$COORD_PORT" --shards="$SHARDS" \
      --ready-only --wait-ready=2 >/dev/null 2>&1 && break
    sleep 2
  done
  say "fleet ready on :$COORD_PORT"
}

stage_stop() {
  stage_down
  for name in coordinator sidecar; do
    [[ -f $RUN/$name.pid ]] || continue
    local pid; pid=$(cat "$RUN/$name.pid")
    kill -0 "$pid" 2>/dev/null && { say "stopping $name (pid $pid)"; kill "$pid"; }
    rm -f "$RUN/$name.pid"
  done
}

# Deep-verify every shard's .bm25 against its recorded section CRCs
# (v8 builds; pre-v8 shards report NONE and the stage says so). Reads
# every byte, so expect roughly a minute per 50 GB shard; run it after
# a build, after copying shards between machines, or on bit-rot
# suspicion. Serving keeps working while it runs (read-only mmap).
stage_verify() {
  local bin=${BM25_VERIFY:-$REPO/target/release/examples/bm25_verify}
  [[ -x $bin ]] || die "no bm25_verify at $bin (cargo build --release --examples)"
  local files=()
  for ((i = 0; i < SHARDS; i++)); do
    [[ -f $OUT/shard-$i.tv.bm25 ]] || die "missing $OUT/shard-$i.tv.bm25"
    files+=("$OUT/shard-$i.tv.bm25")
  done
  local rc=0
  "$bin" "${files[@]}" || rc=$?
  case $rc in
    0) say "all $SHARDS bm25 shards verified" ;;
    2) say "shards predate v8: nothing to verify until the next rebuild" ;;
    *) die "bm25 integrity verification FAILED" ;;
  esac
}

stage_status() {
  for f in "$RUN"/*.pid; do
    [[ -e $f ]] || { say "nothing running"; return; }
    local pid; pid=$(cat "$f")
    printf '  %-16s pid %-8s %s\n' "$(basename "$f" .pid)" "$pid" \
      "$(kill -0 "$pid" 2>/dev/null && echo up || echo DEAD)"
  done
  shopt -s nullglob
  for f in "$OUT"/shard-*.tv "$OUT"/shard-*.tv.bm25; do
    printf '  %-52s %s\n' "$(basename "$f")" "$(du -h "$f" | cut -f1)"
  done
  shopt -u nullglob
}

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

wait_port() {
  local port=$1 what=$2
  for _ in $(seq 1 120); do
    port_open "$port" && return 0
    sleep 1
  done
  die "$what never opened :$port"
}

(($# > 0)) || die "usage: rebuild.sh <plan|up|calibrate|ingest|down|serve|verify|stop|status>..."
for stage in "$@"; do
  case "$stage" in
    plan | up | sidecar | calibrate | ingest | down | serve | verify | stop | status) "stage_$stage" ;;
    *) die "unknown stage $stage" ;;
  esac
done
