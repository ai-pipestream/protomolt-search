#!/usr/bin/env bash
#
# Reproducible, same-host Protomolt Search versus OpenSearch challenge.
# See README.md for the fairness contract and metric definitions.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
DRIVER="$REPO/target/release/examples/opensearch_challenge"
ENGINE="$REPO/target/release/pipestream-search"
OPENSEARCH_IMAGE=${OPENSEARCH_IMAGE:-opensearchproject/opensearch:3.8.0@sha256:39a8f8c63028e8b5d6b70539af1d0339b15a6729002dd5b3f4a65f520376fd30}
DOCUMENTS=4096
DIMENSIONS=64
TOPICS=16
ITERATIONS=5
WARMUP=2
CONCURRENCY=1,8
CPUSET=${CPUSET:-0-7}
OUT=""
KEEP=false
NODE_PORT=${NODE_PORT:-58151}
COORD_PORT=${COORD_PORT:-58150}
OS_PORT=${OS_PORT:-19200}
OS_METRICS_PORT=${OS_METRICS_PORT:-19600}
CONTAINER="protomolt-os-challenge-${USER:-user}-$$"
WORK=""
MOCK_PID=""
PROTO_PID=""

die() { echo "opensearch-challenge: $*" >&2; exit 1; }
say() { echo "== $*"; }
ns() { date +%s%N; }
elapsed_ms() { awk -v start="$1" -v end="$2" 'BEGIN { printf "%.3f", (end-start)/1000000 }'; }

usage() {
  sed -n '2,4p' "${BASH_SOURCE[0]}" | sed 's/^# {0,1}//'
  echo "usage: run.sh [--documents=N] [--iterations=N] [--warmup=N] [--concurrency=1,8] [--cpuset=0-7] [--out=DIR] [--keep]"
  exit "${1:-0}"
}

for arg in "$@"; do
  case $arg in
    --documents=*) DOCUMENTS=${arg#*=} ;;
    --dimensions=*) DIMENSIONS=${arg#*=} ;;
    --topics=*) TOPICS=${arg#*=} ;;
    --iterations=*) ITERATIONS=${arg#*=} ;;
    --warmup=*) WARMUP=${arg#*=} ;;
    --concurrency=*) CONCURRENCY=${arg#*=} ;;
    --cpuset=*) CPUSET=${arg#*=} ;;
    --out=*) OUT=${arg#*=} ;;
    --keep) KEEP=true ;;
    -h | --help) usage 0 ;;
    *) die "unknown argument: $arg" ;;
  esac
done

for command in cargo curl docker jq sha256sum taskset; do
  command -v "$command" >/dev/null || die "missing command: $command"
done
[[ $DOCUMENTS =~ ^[1-9][0-9]*$ ]] || die "--documents must be positive"
[[ $DIMENSIONS =~ ^[1-9][0-9]*$ ]] || die "--dimensions must be positive"
[[ $TOPICS =~ ^[1-9][0-9]*$ ]] || die "--topics must be positive"
[[ $ITERATIONS =~ ^[1-9][0-9]*$ ]] || die "--iterations must be positive"
[[ $WARMUP =~ ^[0-9]+$ ]] || die "--warmup must be nonnegative"
[[ $CONCURRENCY =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] ||
  die "--concurrency must be a comma-separated positive integer list"
[[ $CONTAINER =~ ^[a-zA-Z0-9_.-]+$ ]] || die "unsafe generated container name"

port_free() {
  ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}
for port in "$NODE_PORT" "$COORD_PORT" "$OS_PORT" "$OS_METRICS_PORT"; do
  port_free "$port" || die "port $port is already in use"
done

if [[ -n $OUT ]]; then
  mkdir -p "$OUT"
  WORK="$(cd "$OUT" && pwd)"
  [[ -z $(find "$WORK" -mindepth 1 -maxdepth 1 -print -quit) ]] ||
    die "--out must name an empty directory: $WORK"
  KEEP=true
else
  WORK=$(mktemp -d "${TMPDIR:-/tmp}/protomolt-os-challenge.XXXXXX")
fi

cleanup() {
  if [[ -n $PROTO_PID ]] && kill -0 "$PROTO_PID" 2>/dev/null; then
    kill -INT "$PROTO_PID" 2>/dev/null || true
    wait "$PROTO_PID" 2>/dev/null || true
  fi
  if [[ -n $MOCK_PID ]] && kill -0 "$MOCK_PID" 2>/dev/null; then
    kill -INT "$MOCK_PID" 2>/dev/null || true
    wait "$MOCK_PID" 2>/dev/null || true
  fi
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  if [[ $KEEP == false && -n $WORK && $WORK == *protomolt-os-challenge.* ]]; then
    rm -rf -- "$WORK"
  fi
}
trap cleanup EXIT INT TERM

wait_http() {
  local url=$1 deadline=$((SECONDS + 180))
  while ((SECONDS < deadline)); do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

start_protomolt() {
  local started=$1
  taskset -c "$CPUSET" "$ENGINE" \
    --role=both \
    --index="$WORK/protomolt/shard.tv" \
    --dim="$DIMENSIONS" --bit-width=4 \
    --node-listen="127.0.0.1:$NODE_PORT" \
    --coord-listen="127.0.0.1:$COORD_PORT" \
    --nodes="127.0.0.1:$NODE_PORT" \
    --analysis-addr="$ANALYSIS_ADDR" \
    --integer-fields=year --facet-fields=group \
    --stream-search --bm25-stream=true \
    >>"$WORK/protomolt.log" 2>&1 &
  PROTO_PID=$!
  local deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    if "$DRIVER" health-protomolt --node="127.0.0.1:$NODE_PORT" --coordinator="127.0.0.1:$COORD_PORT" >/dev/null 2>&1; then
      break
    fi
    kill -0 "$PROTO_PID" 2>/dev/null ||
      die "Protomolt exited; see $WORK/protomolt.log"
    sleep 0.1
  done
  "$DRIVER" health-protomolt --node="127.0.0.1:$NODE_PORT" --coordinator="127.0.0.1:$COORD_PORT" >/dev/null 2>&1 ||
    die "Protomolt did not become RPC-ready; see $WORK/protomolt.log"
  PROTO_READY_MS=$(elapsed_ms "$started" "$(ns)")
}

stop_protomolt() {
  if [[ -n $PROTO_PID ]] && kill -0 "$PROTO_PID" 2>/dev/null; then
    kill -INT "$PROTO_PID"
    wait "$PROTO_PID"
  fi
  PROTO_PID=""
}

run_cells() {
  local engine=$1
  local concurrency
  for concurrency in ${CONCURRENCY//,/ }; do
    local output="$WORK/$engine-c$concurrency.jsonl"
    if [[ $engine == protomolt ]]; then
      "$DRIVER" run-protomolt \
        --coordinator="127.0.0.1:$COORD_PORT" \
        --workload="$WORK/data/workload.jsonl" \
        --output="$output" --iterations="$ITERATIONS" \
        --warmup="$WARMUP" --concurrency="$concurrency"
    else
      "$DRIVER" run-opensearch \
        --opensearch="127.0.0.1:$OS_PORT" \
        --workload="$WORK/data/workload.jsonl" \
        --output="$output" --iterations="$ITERATIONS" \
        --warmup="$WARMUP" --concurrency="$concurrency"
    fi
    RESULT_FILES+=("$output")
  done
}

say "building release engine and challenge driver"
(cd "$REPO" && cargo build --release --locked --bin pipestream-search --example opensearch_challenge)

mkdir -p "$WORK/data" "$WORK/protomolt" "$WORK/opensearch"
chmod 0777 "$WORK/opensearch"
"$DRIVER" generate --out="$WORK/data" --documents="$DOCUMENTS" \
  --dimensions="$DIMENSIONS" --topics="$TOPICS" >"$WORK/generate.json"

say "starting deterministic analysis service"
"$DRIVER" serve-mock >"$WORK/mock.log" 2>&1 &
MOCK_PID=$!
for _ in $(seq 1 200); do
  [[ -s $WORK/mock.log ]] && break
  kill -0 "$MOCK_PID" 2>/dev/null || die "mock analysis exited; see $WORK/mock.log"
  sleep 0.05
done
ANALYSIS_ADDR=$(jq -er .address "$WORK/mock.log") ||
  die "mock analysis did not publish its address"

RESULT_FILES=()
say "starting and ingesting Protomolt Search"
started=$(ns)
start_protomolt "$started"
PROTO_COLD_STARTUP_MS=$PROTO_READY_MS
"$DRIVER" ingest-protomolt --node="127.0.0.1:$NODE_PORT" \
  --corpus="$WORK/data/corpus.jsonl" >"$WORK/protomolt-ingest.json"
run_cells protomolt
PROTO_RSS_BYTES=$(awk '/VmRSS:/ {print $2*1024}' "/proc/$PROTO_PID/status")
ANALYSIS_RSS_BYTES=$(awk '/VmRSS:/ {print $2*1024}' "/proc/$MOCK_PID/status")
PROTO_TOTAL_RSS_BYTES=$((PROTO_RSS_BYTES + ANALYSIS_RSS_BYTES))
PROTO_DISK_BYTES=$(du -sb "$WORK/protomolt" | awk '{print $1}')

say "measuring Protomolt crash recovery to a certified query"
kill -KILL "$PROTO_PID"
wait "$PROTO_PID" 2>/dev/null || true
PROTO_PID=""
sed -n '1p' "$WORK/data/workload.jsonl" >"$WORK/data/recovery-workload.jsonl"
started=$(ns)
start_protomolt "$started"
"$DRIVER" run-protomolt --coordinator="127.0.0.1:$COORD_PORT" \
  --workload="$WORK/data/recovery-workload.jsonl" \
  --output="$WORK/protomolt-recovery.jsonl" --iterations=1 --warmup=0 \
  --concurrency=1 >/dev/null
PROTO_RECOVERY_MS=$(elapsed_ms "$started" "$(ns)")
stop_protomolt

say "starting pinned OpenSearch $OPENSEARCH_IMAGE"
docker pull "$OPENSEARCH_IMAGE" >"$WORK/opensearch-pull.log"
started=$(ns)
docker run -d --name "$CONTAINER" \
  --cpuset-cpus="$CPUSET" \
  -p "127.0.0.1:$OS_PORT:9200" -p "127.0.0.1:$OS_METRICS_PORT:9600" \
  -v "$WORK/opensearch:/usr/share/opensearch/data" \
  -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
  -e "OPENSEARCH_JAVA_OPTS=-Xms2g -Xmx2g" \
  "$OPENSEARCH_IMAGE" >"$WORK/opensearch-container-id"
wait_http "http://127.0.0.1:$OS_PORT/_cluster/health?wait_for_status=yellow" ||
  die "OpenSearch did not become ready; run: docker logs $CONTAINER"
OS_READY_MS=$(elapsed_ms "$started" "$(ns)")
"$DRIVER" ingest-opensearch --opensearch="127.0.0.1:$OS_PORT" \
  --corpus="$WORK/data/corpus.jsonl" >"$WORK/opensearch-ingest.json"
run_cells opensearch
OS_HOST_PID=$(docker inspect -f '{{.State.Pid}}' "$CONTAINER")
OS_RSS_BYTES=$(awk '/VmRSS:/ {print $2*1024}' "/proc/$OS_HOST_PID/status")
OS_DISK_BYTES=$(du -sb "$WORK/opensearch" | awk '{print $1}')

say "measuring OpenSearch crash recovery to a complete response"
docker kill --signal=KILL "$CONTAINER" >/dev/null
started=$(ns)
docker start "$CONTAINER" >/dev/null
wait_http "http://127.0.0.1:$OS_PORT/_cluster/health?wait_for_status=yellow" ||
  die "OpenSearch did not recover; run: docker logs $CONTAINER"
"$DRIVER" run-opensearch --opensearch="127.0.0.1:$OS_PORT" \
  --workload="$WORK/data/recovery-workload.jsonl" \
  --output="$WORK/opensearch-recovery.jsonl" --iterations=1 --warmup=0 \
  --concurrency=1 >/dev/null
OS_RECOVERY_MS=$(elapsed_ms "$started" "$(ns)")

ENGINE_REV=$(git -C "$REPO" rev-parse HEAD)
TURBOVEC_REV=$(sed -n '/^name = "turbovec"$/,/^$/p' "$REPO/Cargo.lock" |
  sed -n 's/.*#\([0-9a-f]\{40\}\).*/\1/p' | head -1)
OS_IMAGE_ID=$(docker image inspect -f '{{.Id}}' "$OPENSEARCH_IMAGE")
OS_REPO_DIGEST=$(docker image inspect -f '{{index .RepoDigests 0}}' "$OPENSEARCH_IMAGE")
CPU_MODEL=$(lscpu | sed -n 's/^Model name:[[:space:]]*//p')
MEM_BYTES=$(awk '/MemTotal:/ {print $2*1024}' /proc/meminfo)
HOST_KERNEL=$(uname -srmo)

jq -n \
  --slurpfile manifest "$WORK/data/manifest.json" \
  --slurpfile proto_ingest "$WORK/protomolt-ingest.json" \
  --slurpfile os_ingest "$WORK/opensearch-ingest.json" \
  --arg engine_rev "$ENGINE_REV" --arg turbovec_rev "${TURBOVEC_REV:-unknown}" \
  --arg os_image "$OPENSEARCH_IMAGE" --arg os_image_id "$OS_IMAGE_ID" \
  --arg os_repo_digest "$OS_REPO_DIGEST" --arg cpu_model "$CPU_MODEL" \
  --arg cpuset "$CPUSET" --arg kernel "$HOST_KERNEL" \
  --argjson memory_bytes "$MEM_BYTES" \
  --argjson protomolt_startup_ms "$PROTO_COLD_STARTUP_MS" \
  --argjson protomolt_recovery_ms "$PROTO_RECOVERY_MS" \
  --argjson protomolt_rss_bytes "$PROTO_RSS_BYTES" \
  --argjson analysis_rss_bytes "$ANALYSIS_RSS_BYTES" \
  --argjson protomolt_total_rss_bytes "$PROTO_TOTAL_RSS_BYTES" \
  --argjson protomolt_disk_bytes "$PROTO_DISK_BYTES" \
  --argjson opensearch_startup_ms "$OS_READY_MS" \
  --argjson opensearch_recovery_ms "$OS_RECOVERY_MS" \
  --argjson opensearch_rss_bytes "$OS_RSS_BYTES" \
  --argjson opensearch_disk_bytes "$OS_DISK_BYTES" \
  '{
    manifest: $manifest[0],
    hardware: {cpu_model: $cpu_model, cpuset: $cpuset, memory_bytes: $memory_bytes, kernel: $kernel},
    software: {
      protomolt_revision: $engine_rev, turbovec_revision: $turbovec_rev,
      opensearch_image: $os_image, opensearch_image_id: $os_image_id,
      opensearch_repo_digest: $os_repo_digest
    },
    protomolt: {
      startup_ms: $protomolt_startup_ms, failure_recovery_ms: $protomolt_recovery_ms,
      service_rss_bytes: $protomolt_rss_bytes, analysis_rss_bytes: $analysis_rss_bytes,
      total_rss_bytes: $protomolt_total_rss_bytes, disk_bytes: $protomolt_disk_bytes,
      ingest: $proto_ingest[0]
    },
    opensearch: {
      startup_ms: $opensearch_startup_ms, failure_recovery_ms: $opensearch_recovery_ms,
      rss_bytes: $opensearch_rss_bytes, disk_bytes: $opensearch_disk_bytes,
      ingest: $os_ingest[0]
    }
  }' >"$WORK/resources.json"

result_csv=$(IFS=,; echo "${RESULT_FILES[*]}")
"$DRIVER" report --workload="$WORK/data/workload.jsonl" \
  --results="$result_csv" --resources="$WORK/resources.json" \
  --output="$WORK/report.json" >"$WORK/report.stdout"
sha256sum "$WORK/data/corpus.jsonl" "$WORK/data/workload.jsonl" \
  "$WORK/resources.json" "$WORK/report.json" >"$WORK/SHA256SUMS"

say "challenge complete"
jq '{hardware, software, protomolt: .protomolt, opensearch: .opensearch}' \
  "$WORK/resources.json"
jq '{throughput_cells, cells}' "$WORK/report.json"
echo "artifacts: $WORK"
