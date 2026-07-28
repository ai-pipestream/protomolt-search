#!/usr/bin/env bash
# Court end-to-end pipeline driver (runs inside the pipeline container):
# pull the bulk snapshot from rustfs, extract the opinions sample (Rust),
# chunk + embed through the OpenNLP analysis sidecar, ingest into the
# shard nodes (calibration broadcast + AddDocuments/AddVectors + Flush),
# then gate on court_verify.
set -euo pipefail

NODES=${NODES:-node1:50051,node2:50051,node3:50051,node4:50051}
COORD=${COORD:-http://coordinator:50050}
ANALYSIS=${ANALYSIS:-http://analysis:50051}
S3_ENDPOINT=${S3_ENDPOINT:-http://rustfs:9000}
S3_BUCKET=${S3_BUCKET:-courtlistener}
BULK_FILE=${BULK_FILE:-opinions-2024-12-31.csv.bz2}
WORK=${WORK:-/corpus}
CONCURRENCY=${CONCURRENCY:-16}
EXTRACT_CAP=${EXTRACT_CAP:-50000}
EXTRACT_MIN_CHARS=${EXTRACT_MIN_CHARS:-1000}
EXTRACT_PREFIX_GB=${EXTRACT_PREFIX_GB:-4}

wait_tcp() { # host port
  for _ in $(seq 1 90); do
    if (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; then
      exec 3>&- 3<&-
      return 0
    fi
    sleep 2
  done
  echo "timeout waiting for $1:$2" >&2
  exit 1
}

# rclone remote "SEED" via env (no config file needed).
export RCLONE_CONFIG_SEED_TYPE=s3
export RCLONE_CONFIG_SEED_PROVIDER=Other
export RCLONE_CONFIG_SEED_ENDPOINT="$S3_ENDPOINT"
export RCLONE_CONFIG_SEED_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID}"
export RCLONE_CONFIG_SEED_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY}"
export RCLONE_CONFIG_SEED_FORCE_PATH_STYLE=true

echo "== waiting for services =="
for svc in analysis:50051 coordinator:50050 ${NODES//,/ }; do
  wait_tcp "${svc%:*}" "${svc#*:}"
done

echo "== pull s3://$S3_BUCKET/bulk-data/$BULK_FILE from rustfs =="
rclone copy "SEED:$S3_BUCKET/bulk-data/$BULK_FILE" "$WORK/"

echo "== stage 0: extract opinions sample (cap=$EXTRACT_CAP, prefix=${EXTRACT_PREFIX_GB}GiB) =="
court_extract \
  --input="$WORK/$BULK_FILE" \
  --output="$WORK/opinions-sample.ndjson" \
  --cap="$EXTRACT_CAP" \
  --min-chars="$EXTRACT_MIN_CHARS" \
  --prefix-gb="$EXTRACT_PREFIX_GB"

echo "== stage 1: chunk + embed (static embeddings via OpenNLP sidecar) =="
court_chunks \
  --input="$WORK/opinions-sample.ndjson" \
  --output="$WORK/chunks.ndjson" \
  --embeddings-out="$WORK/embeddings.bin" \
  --analysis-addr="$ANALYSIS" \
  --concurrency="$CONCURRENCY"

echo "== stage 2: calibrate, broadcast, ingest, flush =="
court_ingest \
  --nodes="$NODES" \
  --chunks="$WORK/chunks.ndjson" \
  --embeddings="$WORK/embeddings.bin" \
  --analysis-addr="$ANALYSIS"

echo "== gate: end-to-end verification =="
court_verify \
  --coordinator="$COORD" \
  --embeddings="$WORK/embeddings.bin"

echo "COURT E2E OK"
