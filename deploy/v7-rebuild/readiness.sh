# shellcheck shell=bash
#
# Readiness gates for rebuild.sh. A gate reports success only after it has
# observed the capability it names: a bound port, a missing probe, or a
# wait that ran out is a failure, never a pass. Sourced by rebuild.sh,
# which supplies the rest of the configuration, `say`, and `die`.

# How long `serve` waits for every shard to answer health before it fails.
# The bench fleet budgets the same wait: cold-starting 8x57G shards took
# more than 750s on krick-1.
READY_TIMEOUT_S=${READY_TIMEOUT_S:-3600}
# How long one embed probe may take to answer, and how long a sidecar
# started here has to load its bundles and pass one.
EMBED_TIMEOUT_S=${EMBED_TIMEOUT_S:-120}
# 1 declares a deployment that embeds no queries: in-process analysis
# (`SIDECAR_ADDR=turbo` or `native`) with no embedding bundle. Nothing
# else makes the missing bundle acceptable.
LEXICAL_ONLY=${LEXICAL_ONLY:-0}
[[ $READY_TIMEOUT_S =~ ^[1-9][0-9]*$ && $EMBED_TIMEOUT_S =~ ^[1-9][0-9]*$ ]] ||
  die "READY_TIMEOUT_S=$READY_TIMEOUT_S and EMBED_TIMEOUT_S=$EMBED_TIMEOUT_S must be positive whole seconds"
[[ $LEXICAL_ONLY == [01] ]] || die "LEXICAL_ONLY=$LEXICAL_ONLY must be 1 or 0"

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

wait_port() {
  local port=$1 what=$2
  for _ in $(seq 1 120); do
    port_open "$port" && return 0
    sleep 1
  done
  die "$what never opened :$port"
}

sidecar_is_local() { [[ $SIDECAR_ADDR == http://127.0.0.1:* || $SIDECAR_ADDR == http://localhost:* ]]; }

# Where query embeddings come from, by the engine's own rule for an
# analysis address: a sidecar URL embeds over gRPC, in-process `turbo`
# analysis embeds only from the bundle TURBO_EMBED_BUNDLE names, and
# in-process analysis without one embeds nothing.
embedding_provider() {
  case $SIDECAR_ADDR in
    native | native://) echo none ;;
    turbo | turbo://) if [[ -v TURBO_EMBED_BUNDLE ]]; then echo in-process; else echo none; fi ;;
    *) echo sidecar ;;
  esac
}

# One embedding of the index dimension through the configured provider,
# answered within $1 seconds (default EMBED_TIMEOUT_S). Analysis and
# embedding are separate capabilities of the same process, and ingest
# exercises only the first (vectors come from the file), so a provider
# that cannot embed serves a whole rebuild without complaint and then
# fails the first hybrid query. So does a model of another dimension: it
# embeds, and every node refuses the vector. The probe sets no deadline of
# its own, so it runs under one here: a provider that accepts the call and
# never answers is a failure too. EMBED_DIAG keeps the probe's last lines
# for the failure message.
embed_probe() {
  local limit=${1:-$EMBED_TIMEOUT_S} out rc=0
  out=$(timeout --foreground "$limit" "$PROBE" --addr="$SIDECAR_ADDR" \
    --text=probe --embed --expect-dim="$DIM" 2>&1) || rc=$?
  EMBED_DIAG=$(tail -n 3 <<<"$out")
  ((rc != 124)) || EMBED_DIAG="no answer within ${limit}s"
  return "$rc"
}

# Make the configured embedding provider usable, or fail the stage. Only a
# deployment declared lexical-only has no provider and nothing to check;
# the declaration and the configuration must agree.
sidecar_up() {
  local provider
  provider=$(embedding_provider)
  if [[ $LEXICAL_ONLY == 1 ]]; then
    [[ $provider == none ]] ||
      die "LEXICAL_ONLY=1, but SIDECAR_ADDR=$SIDECAR_ADDR${TURBO_EMBED_BUNDLE:+ with TURBO_EMBED_BUNDLE=$TURBO_EMBED_BUNDLE} provides embeddings ($provider); drop one of the two"
    say "lexical-only (LEXICAL_ONLY=1): analysis runs in-process ($SIDECAR_ADDR); no sidecar to start or probe, and no query can be embedded"
    return
  fi
  [[ $provider != none ]] ||
    die "SIDECAR_ADDR=$SIDECAR_ADDR analyzes in-process and TURBO_EMBED_BUNDLE names no bundle, so no query could be embedded; set the bundle, point SIDECAR_ADDR at a sidecar, or declare the deployment lexical-only with LEXICAL_ONLY=1"
  [[ -x $PROBE ]] ||
    die "no analyze_probe at $PROBE, so nothing can show that $SIDECAR_ADDR embeds (cargo build --release --examples, or set PROBE)"
  if [[ $provider == in-process ]]; then
    embed_probe ||
      die "in-process embedding from TURBO_EMBED_BUNDLE=$TURBO_EMBED_BUNDLE failed the embed probe: $EMBED_DIAG"
    say "in-process embedding from $TURBO_EMBED_BUNDLE (embed probe passed)"
    return
  fi
  if ! sidecar_is_local; then
    embed_probe ||
      die "remote analysis sidecar at $SIDECAR_ADDR failed the embed probe: $EMBED_DIAG"
    say "using the remote analysis sidecar at $SIDECAR_ADDR (embed probe passed)"
    return
  fi
  if port_open "$SIDECAR_PORT"; then
    if embed_probe; then
      say "reusing the sidecar on :$SIDECAR_PORT (embed probe passed)"
      return
    fi
    # Most likely the server was started without TURBO_KSERVE_BUNDLE_DIR
    # pointing at a loaded TASK_EMBED bundle: lexical analysis still
    # answers, while embedding fails.
    stop_recorded_sidecar
  fi
  start_sidecar
}

# Stop the sidecar a previous run of this script started. One that was
# started some other way has no recorded pid to signal, so it is the
# operator's to stop.
stop_recorded_sidecar() {
  [[ -f $RUN/sidecar.pid ]] ||
    die "sidecar on :$SIDECAR_PORT cannot embed and was not started here (no $RUN/sidecar.pid); stop it and re-run: $EMBED_DIAG"
  say "sidecar on :$SIDECAR_PORT answers but CANNOT EMBED; restarting it"
  kill "$(cat "$RUN/sidecar.pid")" 2>/dev/null || true
  rm -f "$RUN/sidecar.pid"
  for _ in $(seq 1 30); do
    port_open "$SIDECAR_PORT" || return 0
    sleep 1
  done
  die "sidecar on :$SIDECAR_PORT will not stop; kill it and re-run"
}

start_sidecar() {
  [[ -x $SIDECAR_BIN ]] || die "no turbo-kserve binary at $SIDECAR_BIN"
  [[ -d $EMBEDDINGS_DIR ]] || die "no embedding bundle dir at EMBEDDINGS_DIR=$EMBEDDINGS_DIR"
  mkdir -p "$RUN" "$LOGS"
  TURBO_KSERVE_LISTEN="127.0.0.1:$SIDECAR_PORT" TURBO_KSERVE_BUNDLE_DIR="$EMBEDDINGS_DIR" \
    "$SIDECAR_BIN" >>"$LOGS/sidecar.log" 2>&1 &
  local pid=$!
  echo "$pid" >"$RUN/sidecar.pid"
  wait_port "$SIDECAR_PORT" "analysis sidecar"
  # turbo-kserve listens before it loads its bundles, so the port opens
  # ahead of the first embedding it can serve. The budget covers the whole
  # wait: each probe gets what is left of it.
  local deadline=$((SECONDS + EMBED_TIMEOUT_S)) left
  while :; do
    left=$((deadline - SECONDS))
    ((left > 0)) ||
      die "analysis sidecar bound :$SIDECAR_PORT but did not embed within ${EMBED_TIMEOUT_S}s; see $LOGS/sidecar.log: $EMBED_DIAG"
    if embed_probe "$left"; then break; fi
    kill -0 "$pid" 2>/dev/null ||
      die "analysis sidecar exited before it could embed; see $LOGS/sidecar.log: $EMBED_DIAG"
    sleep 1
  done
  say "analysis sidecar on :$SIDECAR_PORT (pid $pid, embed probe passed)"
}

# Return once the verifier has seen every expected primary shard answer
# health through the coordinator, in one scoring space; die on anything
# else. An open port is not readiness: a node binds before it opens its
# .bm25, and opening 50 GB of postings reads every document length.
#
# The verifier exits 3 for "not ready yet" (shards still loading, or a
# coordinator that is not serving), the only outcome worth another
# attempt. Any other failure is one the next attempt would repeat: a token
# file it cannot read, credentials the coordinator refuses, a TLS
# handshake that fails, a bad flag, a binary that is gone. Its calls carry
# no deadline, so each attempt runs under what is left of the budget, and
# no attempt starts once nothing is left: a coordinator that accepts the
# connection and never answers ends the wait instead of outlasting it.
wait_fleet_ready() {
  local coord_pid=$1 deadline=$((SECONDS + READY_TIMEOUT_S)) left out rc report=""
  while :; do
    left=$((deadline - SECONDS))
    ((left > 0)) || break
    rc=0
    out=$(timeout --foreground "$left" "$VERIFY" \
      --coord="127.0.0.1:$COORD_PORT" --shards="$SHARDS" \
      --ready-only --wait-ready=2 "${CLIENT_ARG_LIST[@]}" 2>&1) || rc=$?
    case $rc in
      0) return 0 ;;
      3) report=$out ;;
      124) report+="${report:+$'\n'}the coordinator on :$COORD_PORT gave the verifier no answer for ${left}s" ;;
      *) die "fleet NOT ready: the verifier $VERIFY failed (exit $rc): $(tail -n 3 <<<"$out")" ;;
    esac
    kill -0 "$coord_pid" 2>/dev/null ||
      die "fleet NOT ready: the coordinator exited; see $LOGS/coordinator.log"
    sleep 2
  done
  die "fleet NOT ready after ${READY_TIMEOUT_S}s (READY_TIMEOUT_S); processes left up, logs in $LOGS; the verifier's last report:
$report"
}
