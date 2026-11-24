# shellcheck shell=bash
#
# Readiness gates for rebuild.sh. A gate reports success only after it has
# observed the capability it names: a bound port, a missing probe, or a
# wait that ran out is a failure, never a pass. Sourced by rebuild.sh,
# which supplies the configuration, `say`, and `die`.

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
# in-process analysis without one is the lexical-only deployment.
embedding_provider() {
  case $SIDECAR_ADDR in
    native | native://) echo none ;;
    turbo | turbo://) if [[ -v TURBO_EMBED_BUNDLE ]]; then echo in-process; else echo none; fi ;;
    *) echo sidecar ;;
  esac
}

# One embedding through the configured provider. Analysis and embedding
# are separate capabilities of the same process, and ingest exercises only
# the first (vectors come from the file), so a provider that cannot embed
# serves a whole rebuild without complaint and then fails the first hybrid
# query. The probe sets no deadline of its own, so it runs under one here:
# a provider that accepts the call and never answers is a failure too.
# EMBED_DIAG keeps the probe's last lines for the failure message.
embed_probe() {
  local out rc=0
  out=$(timeout --foreground "$EMBED_TIMEOUT_S" "$PROBE" --addr="$SIDECAR_ADDR" --text=probe --embed 2>&1) || rc=$?
  EMBED_DIAG=$(tail -n 3 <<<"$out")
  ((rc != 124)) || EMBED_DIAG="no answer within ${EMBED_TIMEOUT_S}s"
  return "$rc"
}

# Make the configured embedding provider usable, or fail the stage. A
# lexical-only deployment has no provider and nothing to check.
sidecar_up() {
  local provider
  provider=$(embedding_provider)
  if [[ $provider == none ]]; then
    say "lexical-only: analysis runs in-process ($SIDECAR_ADDR) with no embedding bundle; no sidecar to start or probe"
    return
  fi
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
  # ahead of the first embedding it can serve.
  local deadline=$((SECONDS + EMBED_TIMEOUT_S))
  until embed_probe; do
    kill -0 "$pid" 2>/dev/null ||
      die "analysis sidecar exited before it could embed; see $LOGS/sidecar.log: $EMBED_DIAG"
    ((SECONDS < deadline)) ||
      die "analysis sidecar bound :$SIDECAR_PORT but did not embed within ${EMBED_TIMEOUT_S}s; see $LOGS/sidecar.log: $EMBED_DIAG"
    sleep 1
  done
  say "analysis sidecar on :$SIDECAR_PORT (pid $pid, embed probe passed)"
}

# Return once the verifier has seen every expected primary shard answer
# health through the coordinator; die on anything else. An open port is
# not readiness: a node binds before it opens its .bm25, and opening 50 GB
# of postings reads every document length.
#
# The verifier exits 1 for "not ready yet" (shards still loading, or the
# coordinator refusing the call), which is the only outcome worth another
# attempt. Any other status means the verifier itself failed. Its calls
# carry no deadline, so each attempt runs under what is left of the budget:
# a coordinator that accepts the connection and never answers ends the
# wait instead of outlasting it.
wait_fleet_ready() {
  local coord_pid=$1 deadline=$((SECONDS + READY_TIMEOUT_S)) report rc
  while :; do
    rc=0
    report=$(timeout --foreground "$((deadline - SECONDS + 1))" "$VERIFY" \
      --coord="127.0.0.1:$COORD_PORT" --shards="$SHARDS" \
      --ready-only --wait-ready=2 "${CLIENT_ARG_LIST[@]}" 2>&1) || rc=$?
    case $rc in
      0) return 0 ;;
      1) ;;
      124) report="the coordinator on :$COORD_PORT gave the verifier no answer" ;;
      *) die "fleet NOT ready: the verifier $VERIFY failed (exit $rc): $(tail -n 3 <<<"$report")" ;;
    esac
    kill -0 "$coord_pid" 2>/dev/null ||
      die "fleet NOT ready: the coordinator exited; see $LOGS/coordinator.log"
    ((SECONDS < deadline)) || break
    sleep 2
  done
  die "fleet NOT ready after ${READY_TIMEOUT_S}s (READY_TIMEOUT_S); processes left up, logs in $LOGS; the verifier's last report:
$report"
}
