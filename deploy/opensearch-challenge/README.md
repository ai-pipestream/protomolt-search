# Protomolt Search versus OpenSearch challenge

This suite runs both engines on the same host, CPU set, generated corpus,
queries, filters, parent groups, vectors, and relevance judgments. It is a
regression and prioritization tool, not a one-run marketing scoreboard.

The default OpenSearch opponent is the official 3.8.0 image pinned by digest.
It uses one shard, the whitespace-plus-lowercase analyzer, Lucene HNSW with
inner-product scoring, efficient inline k-NN filters, field collapse, and an
unweighted RRF search pipeline with rank constant 60. Those choices follow the
retained official documentation under
`docs/ai-slop/source-reference/opensearch`.

## Run it

```bash
# A useful local decision run. Results are retained in the named directory.
./deploy/opensearch-challenge/run.sh \
  --documents=16384 --iterations=20 --warmup=3 \
  --concurrency=1,8 --cpuset=0-7 \
  --out=/work/court-corpus/bench/opensearch-challenge/run-$(date +%Y%m%d-%H%M%S)

# Fast lifecycle smoke.
./deploy/opensearch-challenge/run.sh \
  --documents=512 --dimensions=32 --topics=8 \
  --iterations=1 --warmup=1 --concurrency=1,2
```

Without `--out`, the runner uses a validated `mktemp` directory and removes
it on success. `--keep` retains that temporary directory. It refuses occupied
ports and a nonempty output directory. The container name is process-unique,
and cleanup only targets that exact container and the PIDs the run started.

Requirements are Rust/Cargo, Docker, curl, jq, sha256sum, taskset, and enough
RAM for the pinned 2 GiB OpenSearch heap. Override ports with `NODE_PORT`,
`COORD_PORT`, `OS_PORT`, and `OS_METRICS_PORT`. Override
`OPENSEARCH_IMAGE` only deliberately: its resolved image id and repository
digest are recorded.

## Fairness contract

- Both engines run sequentially on the same host and `--cpuset`; they do not
  contend during timed cells.
- The manifest hashes the exact corpus and workload JSONL. IDs, unit vectors,
  parent groups, integer filters, and relevance judgments are shared.
- Text contains only ASCII whitespace-separated terms. Protomolt's deterministic
  mock analyzer and OpenSearch's configured analyzer both lowercase those terms.
- Protomolt uses one node plus its coordinator over loopback and its public
  gRPC API. OpenSearch uses one node over loopback HTTP. Both include their
  ordinary client/server transport.
- Protomolt's vector result is an exhaustive 4-bit TurboQuant scan. OpenSearch's
  result is Lucene HNSW. Neither result is treated as ground truth. Vector and
  collapse judgments come from exact FP32 inner-product rankings generated
  before either index is built.
- Hybrid relevance gives topic matches gain 2 and exact-vector top-k matches an
  additional gain 1. The engines both use unweighted RRF with rank constant 60
  and an explicit per-leg fusion depth of `max(k, 100)`.
- OpenSearch setup and Protomolt calibration happen outside corpus file reading.
  Timed ingest includes Protomolt calibration fitting, all data-plane writes,
  and flush/refresh. Index/pipeline schema creation is configuration, not ingest.
- Cold startup ends only after an application RPC or cluster-health request
  succeeds. An open port is never counted as readiness.
- Crash recovery kills the serving process/container with SIGKILL, restarts the
  persisted generation, waits for real readiness, and ends only after a
  workload query returns a complete result.

The synthetic fixture is intentionally controllable. A result does not
generalize to CourtListener, multilingual text, larger dimensions, multiple
shards, or a different OpenSearch engine. Use the same driver with a captured
production workload before making a product claim.

## Metrics and output

Every concurrency cell emits one sample per query and iteration plus a cell
record. `report.json` groups by engine, workload, and concurrency:

- time to first hit p50/p95/p99;
- terminal latency p50/p95/p99;
- measured cell throughput;
- mean recall@k and NDCG@k against shared judgments;
- successful terminal completion counts.

For Protomolt QueryStream, first hit is the first nonempty provisional
replacement revision. If a route has no provisional collector, it is the final
revision. Parent collapse currently uses its unary exact route, so its first-hit
time equals terminal time. For OpenSearch REST, first hit is when the response
body bytes first contain an `_id`; because OpenSearch does not stream ranked
hits from this API, it normally approaches terminal latency.

`resources.json` adds:

- CPU model, CPU set, RAM, kernel;
- exact Protomolt, TurboVec, and OpenSearch revisions/digests;
- ingest duration;
- steady-state RSS;
- persisted index bytes;
- cold startup;
- crash-to-complete-query recovery.

Protomolt reports service RSS, analysis-service RSS, and their sum. OpenSearch
reports the container's Java process RSS. The result directory also contains
raw JSONL, logs, manifest, persisted stores, recovery samples, and
`SHA256SUMS`.

## Workload families

| Family | Protomolt route | OpenSearch route |
|---|---|---|
| lexical | public QueryStream to exact streaming BM25 | match query |
| vector | public QueryStream to exact candidate stream | Lucene HNSW k-NN |
| hybrid | public QueryStream, global-rank RRF | hybrid query plus RRF pipeline |
| filtered lexical | lexical plus compiled CEL integer filter | bool/range filter |
| filtered vector | vector allowlist from compiled CEL | inline efficient k-NN filter |
| collapse vector | exact parent-collapse Search | field collapse over parent |

## Reading results

Do not collapse the output into “won” or “lost.” First reject invalid runs:
incomplete requests, hash drift, mismatched image/revision, host pressure, or a
changed CPU set. Then look for product decisions:

- lower first-hit time with similar terminal latency validates streaming value;
- worse recall/NDCG points to quantization or ANN tuning;
- high p99 with good p50 points to fan-out, allocation, or runtime pauses;
- high RSS/disk/startup cost identifies deployment advantages;
- slow crash recovery identifies persistence and readiness work;
- ingest differences identify writer, graph-build, analysis, or flush work.

The existing `deploy/bench/run_matrix.sh` remains the corpus-scale
Protomolt-only fleet test. Use this challenge for cross-engine comparison, then
use the fleet matrix to ensure an optimization still preserves exact
distributed behavior.
