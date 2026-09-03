# pipestream-search

Provider-neutral distributed lexical, vector, and hybrid search. The product
owns BM25, CEL selection, document semantics, fusion, generations, and public
quality claims. Vector engines plug in behind one descriptor/configuration
contract. The shipped `embedded-turbovec` adapter provides exhaustive
TurboQuant scoring and collaborative live-floor streaming.

## Repository map

| Repository | Role | Depends on |
|---|---|---|
| [RyanCodrai/turbovec](https://github.com/RyanCodrai/turbovec) | Upstream vector index library: 4-bit TurboQuant encoding, SIMD top-k search | — |
| [ai-pipestream/turbovec](https://github.com/ai-pipestream/turbovec), branch `turbovec-pipestream-s17` | Patch fork carrying the seedable top-k floor and live-floor streaming collector. Rebased onto upstream `main`; explicit TQ+ calibration is now upstream | upstream `main` |
| [ai-pipestream/turbovec-grpc](https://github.com/ai-pipestream/turbovec-grpc) | Network and sharding facade for the local turbovec engine | fork branch `turbovec-pipestream-s17` |
| [Pipestream Search](https://github.com/ai-pipestream/protomolt-search) (this repository) | Full search product: distributed vector, BM25, CEL selection, hybrid ranking, document semantics, persistence, and operations | fork branch `turbovec-pipestream-s17` |
| [ai-pipestream/grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis) | Text-analysis sidecar: sentence/token spans, term vectors, static embeddings, served over gRPC | — |

The in-repository [`protomolt-analyzer`](crates/protomolt-analyzer) crate is
the portable Rust lexical core. It runs the product's whitespace, Porter,
normalization, and term-vector contract in-process on servers and native
clients. OpenNLP remains the provider for embeddings, sentence and model
analysis, and analyzer options outside that native subset. See
[Native lexical analysis](docs/native-analysis.md) for the exact boundary and
Android/iOS build checks.

The [`protomolt-search-embedded`](crates/protomolt-search-embedded) package
runs the same public `SearchService` contract over private local shards through
an in-process coordinator. It binds and dials no sockets, forces the native
analyzer, and supports create/open, protobuf-mapped ingest, vector ingest,
delete/replace, `Query`, `QueryStream`, health, and flush on Android, iOS, and
desktop. See [Embedded and mobile Protomolt Search](docs/embedded-mobile.md).

The current embedded adapter pins the fork branch recorded in `Cargo.toml` and
uses TurboVec's current `.tv` persistence format. Provider images are opaque to
the product and are selected by manifest/config identity, never by extension.
See [the Pipestream Search migration note](docs/pipestream-search-migration.md)
for renamed surfaces, compatibility aliases, and rebuild impact.

Engine internals and measured numbers: [docs/optimizations.md](docs/optimizations.md).
The implemented public query contract is [docs/query-api.md](docs/query-api.md):
selection first, candidate-scoped boosts second, then a named-signal composite
scorer. [`SearchService.QueryStream`](docs/streaming-query.md) adds exact
provisional replacement revisions and an explicit terminal certificate.
Block-max pruning, designed for the lexical leg and measured dead on the
vector leg: [README-block-max.md](README-block-max.md) (overview) and
[docs/block-max.md](docs/block-max.md) (design doc).
The reproducible cross-engine gate is
[deploy/opensearch-challenge](deploy/opensearch-challenge/README.md); it records
quality, latency, throughput, resources, startup, and crash recovery without
turning a synthetic run into a marketing claim.

The network binary retains three roles (`node`, `coordinator`, `both`). Plain
node lists are static; versioned shard maps hot-swap atomically. The embedded
package uses the same node and coordinator implementations without a network
listener.

## Quickstart: dockerized end-to-end demo (CourtListener)

A one-command installer that syncs real data and exercises the whole stack:
CourtListener bulk opinions (public S3) → [rustfs](https://rustfs.com)
object store → Rust extraction → chunk + static-embedding via the
[grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis)
sidecar (GraalVM native, Model2Vec table) → a 4-shard pipestream-search
cluster → an automated search-verification gate. No Python anywhere.

```bash
cd deploy/court-e2e
cp .env.example .env            # defaults work out of the box
./e2e.sh                        # or ./e2e.sh --clean to wipe and reseed
```

`e2e.sh` exits 0 only after indexing ~50k opinions (~500k chunks) across
4 shards and passing both gates: a vector self-match through the
coordinator and a distributed BM25 query. On success the cluster stays
up — coordinator on `localhost:50050`, rustfs console on
`localhost:19001`. Details, scale knobs, and architecture:
[deploy/court-e2e/README.md](deploy/court-e2e/README.md).

Query it with any gRPC client generated from
[search.proto](proto/ai/pipestream/search/v1/search.proto) — `Query` or
`QueryStream` for the public contract, plus `Search` (vector top-k with floor
sharing), `Bm25Search` (distributed lexical), and `HybridSearch` (cascade,
RRF, or score-blend fusion) — or with the
bundled probe tool:

```bash
docker run --rm --network court-e2e_default -v court-e2e_corpus-data:/corpus \
  --entrypoint court_query court-e2e-pipeline \
  --nodes=node1:50051,node2:50051,node3:50051,node4:50051 \
  --analysis-addr=http://analysis:50051 --embeddings=/corpus/embeddings.bin \
  --probe-ids=12345 --docs-per-shard=128500
```

## Design

```mermaid
flowchart TB
    client([client]) -->|Search| coord["coordinator (SearchService)<br/>FloorTracker: max over shard floors"]
    coord -->|"StartShardSearch + FloorUpdate"| n0["node 0 (NodeService)<br/>shard index · chunked scan"]
    coord -->|"StartShardSearch + FloorUpdate"| n1["node 1 (NodeService)<br/>shard index · chunked scan"]
    coord -->|"StartShardSearch + FloorUpdate"| nn["node N (NodeService)<br/>shard index · chunked scan"]
    n0 -->|"FloorUpdate: k-th best (once heap fills)"| coord
    n1 -->|"FloorUpdate: k-th best"| coord
    nn -->|"FloorUpdate: k-th best"| coord
```

Floor flow for one query:

1. The coordinator opens a bidi `SearchShard` stream to every node and
   sends `StartShardSearch { query, k, request_id }`.
2. Each node scans its shard **in chunks** of `chunk_blocks` SIMD blocks
   (default 64 blocks = 2048 vectors). Each chunk is a
   `search_with_options` call restricted to that chunk's slot range by an
   allowlist mask, seeded with the best floor known at that moment
   (`initial_threshold`).
3. Once a node's running top-k heap is full, it publishes its k-th best
   after each chunk (`FloorUpdate` node → coordinator). The coordinator
   tracks the max over all shards and broadcasts every raise to all nodes
   (`FloorUpdate` coordinator → node). Nodes apply the raised floor to the
   next chunk.
4. Each node ends its stream with `SearchShardDone { hits, stats }` — its
   local top-k plus scan counters. The coordinator merges the shard lists
   (score descending; ties by stable vector id) and answers the
   client.

Chunking exists because turbovec's scan is a single synchronous call with a
call-time-fixed floor; scanning in masked chunks gives the floor flow
intra-query reactivity without patching the kernel. The union of the masked
chunk ranges is exactly the whole shard, so chunking alone changes nothing
about results.

## The lossless invariant, and why it holds

**Claim.** Pruning candidates that score below the max published floor can
never drop a true global top-k hit.

**Why.** A floor published by a shard is that shard's current k-th best,
emitted only once the shard holds k candidates. The k-th best of any subset
of the corpus is a lower bound on the k-th best of the whole corpus: the
global top-k picks the best k of the union, so its k-th entry scores at
least as high as the k-th entry of any shard's top-k. Therefore every
published floor ≤ the true global k-th best, and so is the max over
published floors. Any candidate scoring strictly below that max also scores
below the global k-th best — at least k other candidates beat it — so it
cannot belong to the global top-k. Candidates scoring exactly at the floor
are kept (turbovec's threshold is inclusive and the k-th-best seeding keeps
boundary ties), so tie scenarios are safe too.

The same argument covers the node's *local* floor (its own heap's k-th
best): it is a lower bound on the shard's final k-th best, hence on the
global one.

Empirically: `tests/lossless.rs` builds a 20k-vector corpus (dim 128,
4-bit), fits calibration on a sample, builds 3 shard indexes plus one
monolithic index with the same seeded calibration, and asserts the
coordinator's top-10 equals the monolithic top-10 **exactly** — same ids,
bitwise-same scores, same order — for several queries, with floor sharing
on and off. `tests/node_loopback.rs` injects a floor mid-scan over real
gRPC and asserts identical results.

## Why scores are comparable across shards at all

Scores are mergeable across shards only when the provider says they share one
scoring identity. `NodeService.GetVectorBackend` reports the provider,
version, metric, quality contract, capabilities, and scoring fingerprint.
`ConfigureVectorBackend` accepts provider-owned opaque state before ingest.
For `embedded-turbovec`, that state contains the fitted TQ+ parameters, so a
given vector is encoded consistently across shard layouts. The product does
not interpret those bytes.

## Running

```bash
cargo build --release

# Single-process demo: both roles, random demo corpus (calibration fitted
# on a 20% sample and seeded), one self-issued search at the end.
./target/release/pipestream-search --role=both \
    --demo-vectors=20000 --dim=128 --bit-width=4 --allow-plaintext \
    --nodes=127.0.0.1:50051 --demo-query

# A real shard node over a persisted embedded-TurboVec image.
./target/release/pipestream-search --role=node \
    --vector-backend=embedded-turbovec --index=/data/shard-0.tv \
    --slot-offset=0 --node-listen=0.0.0.0:50051

# A coordinator over three nodes.
./target/release/pipestream-search --role=coordinator \
    --coord-listen=0.0.0.0:50050 \
    --nodes=node0:50051,node1:50051,node2:50051
```

### Clustered TurboVec backend

The product coordinator can treat a complete `turbovec-grpc` collection as
its vector backend. The recommended distributed shape embeds that crate's
coordinator library in the Pipestream Search process, so there is no localhost
gRPC hop before the real shard fan-out:

```toml
role = "coordinator"
nodes = ["search-node0:50051", "search-node1:50051"] # BM25, columns, documents

[clustered_turbovec]
nodes = [
  "vector-node0:51051 shard-id-0 12",
  "vector-node1:51051 shard-id-1 9",
]
state = "/var/lib/pipestream-search/turbovec-topology.json"
```

An independently managed coordinator remains available when its lifecycle or
authorization must be separate:

```toml
[clustered_turbovec]
coordinator = "http://turbovec-coordinator:50050"
```

Both transports execute the same `turbovec-grpc::CoordinatorService`
candidate-stream contract. Cluster shards must carry stable labels equal to
Pipestream Search document ids. The adapter carries product filters as packed
stable-label bitmap ranges, sends conflated inclusive floor raises, and
requires an exhaustive completion certificate from every shard. Small
candidate-scoped rescoring sets use explicit labels.
`ClusterHealth` reports the selected transport, reachability, servable state,
row count, and topology generation.

This increment serves exact vector `Search`, dense public selections, and
candidate-scoped dense boosts. Parent collapse and every hybrid mode also use
the provider stream while keeping lineage, product-shard ownership, fusion,
and public ordering in Pipestream Search. The vector collection is built and
mutated through `turbovec-grpc`; Pipestream Search does not duplicate those
writes.
See [the clustered backend design](docs/clustered-turbovec.md).

### Cluster configuration file

For real deployments the binary reads a TOML file (`--config cluster.toml`,
or `PIPESTREAM_SEARCH_CONFIG`). Precedence: **CLI flag > env var > config file >
default**. Every flag takes `--key=value` or `--key value`.

```toml
role = "both"                                  # node | coordinator | both
coord_listen = "0.0.0.0:50050"
nodes = ["host-a:50051", "host-b:50051"]      # fan-out order = tie-break order
chunk_blocks = 64                              # scan chunk size (SIMD blocks)
floor_sharing = true
bm25_stream = true                              # exact lexical candidates; false is the unary A/B route
floor_delta = 0.0                              # min raise before a floor publishes (0 = every raise)
vector_backend = "embedded-turbovec"            # default provider for shards
shard_deadline_ms = 0                          # per-shard query deadline (0 = none)
hedge_delay_ms = 0                             # hedge slow shards to their replica after this (0 = failover only; set above healthy p99)
max_message_mib = 64                           # gRPC message cap (both directions)

[[shards]]                                     # shards this process serves
listen = "0.0.0.0:50051"                       # one NodeService listener per shard
index = "/data/turbovec/shard-0.tv"
slot_offset = 0                                # global id base for this shard

[[shards]]
listen = "0.0.0.0:50052"
index = "/data/turbovec/shard-1.tv"
slot_offset = 20000
```

The plain coordinator `nodes` list and each node's `[[shards]]` set are static.
A `shard_map` is generation-stamped and hot-reloaded atomically; requests pin
one immutable generation, and routed ingest requires the expected generation.
Single-shard shorthand
(`--index`, `--demo-vectors`, `--node-listen`, `--slot-offset`) overrides
the file's `[[shards]]` entirely.

### Operability

- **Pooled connections.** The coordinator keeps one lazily-established
  HTTP/2 channel per node address; every concurrent query multiplexes
  over it, and it reconnects on its own after a node restart.
- **Health.** `NodeService.Health` reports one shard's shape (vector
  count, dim, bit width, BM25 docs, ingest/build activity);
  `SearchService.ClusterHealth` fans it out to every primary and replica
  and reports per-target reachability without failing on down nodes.
- **Replicas, failover, and hedging.** A shard-map entry may name a
  `replica` serving the same data. On a primary error the coordinator
  fails over to it; with `hedge_delay_ms` set, a shard still running
  when the delay expires gets a second identical search on its replica
  and the first success wins. Search is exact, so either copy returns
  identical results. Hedging is stall insurance, not a latency
  optimization: set the delay ABOVE the healthy p99, or the timer fires
  on ordinary bottleneck shards and the duplicate scan compounds the
  saturation it was meant to escape. Measured both ways in Round 5 of
  TEST_RESULTS.md — a 26–37% p99 improvement against a stalled node, a
  25–40% throughput loss when hedging a healthy bandwidth-bound fleet.
- **Replica catch-up.** With a shard map containing replicas,
  `replica_sync_ms` defaults to 1000 and tails each primary's fully clocked WAL
  into its replica. Cursors persist beside the map by default. A WAL generation
  rotation requires installing the new base snapshot rather than guessing at
  missing history.
- **Durable control authority.** Optional `ClusterControl` leases nodes,
  reconciles failure-domain/capacity-aware placement, schedules compaction and
  split/merge work, and publishes only complete generation-stamped topologies.
  See [docs/cluster-control.md](docs/cluster-control.md).
- **Deadlines.** `shard_deadline_ms` bounds one query's whole per-shard
  attempt (primary plus any hedge); a shard that exceeds it fails the
  query with DEADLINE_EXCEEDED instead of stalling it.
- **Floor delta.** `floor_delta` suppresses floor publishes that improve
  the last published floor by less than the delta — fewer messages on
  real networks at a sliver of pruning reactivity, results unchanged.

## k-sweep benchmark harness

`sweep` is a second binary that builds a deterministic corpus, serves it as
N shards on loopback (real gRPC), and sweeps k with floor sharing on and
off, reporting candidates collected and wall medians/p90 per mode — the
harness for measuring how sharing's payoff varies with k. It also asserts
sharing never changes results at any k.

```bash
cargo run --release --example sweep -- \
    --vectors=60000 --dim=128 --shards=3 \
    --k=10,100,1000,10000 --queries=20 \
    --chunk-blocks=64 --modes=on,off
```

`--write-indexes DIR` additionally persists the shards as `.tv` files and
prints ready-to-paste `[[shards]]` config entries — this is how the indexes
for a real deployment are produced (shared calibration baked in).

## BM25 lexical search (hybrid half)

Each shard also carries a **BM25 postings index** next to its vector index:
term → postings (doc id, tf, occurrence offsets in original-text
coordinates), per-doc lengths and corpus totals, plus a doc store of raw
texts (the highlight source). Term identity is supplied by one of two
interchangeable providers. The in-process Rust provider implements the
production `ingest`/`folded` and `cased` analyzer specs. The
[grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis)
sidecar supplies the wider OpenNLP surface, including embeddings and
model-backed layers. Its proto is vendored at
`proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`; see the file header.
Both providers support original-text UTF-16 coordinates. The portable Rust
analyzer and sidecar can also return UTF-8 byte offsets to direct callers.
Protomolt Search explicitly requests and persists UTF-16 offsets so one index
generation cannot mix coordinate systems.

Glossary-backed phrase and entity search is an additive product capability:
ordinary body terms preserve recall, a dedicated phrase field stores only
explicit registered concepts, and an optional map column exposes concepts and
OpenNLP NER identities to CEL and facets. `PhraseSearch` adds only the strongest
phrase signal per document, so nested concepts do not stack. See
[`docs/phrase-search.md`](docs/phrase-search.md) for vocabulary format,
configuration, scoring, mobile use, WAL durability, and the required reindex
boundary.

Phrase and proximity queries (`PhraseMatch` on `Bm25Search` and on the
single lexical leaf of `Query`) are served by two opt-in per-field
payloads: a derived bigram column (`--bigram-fields=body`) answers
two-term exact phrases as one term, and token positions
(`--position-fields=body`) answer longer phrases and slop exactly at the
shared heap gate. Positions are token ordinals from the ingest analysis,
never character offsets, and a field that declared neither refuses the
query by name. See [`docs/phrase-proximity.md`](docs/phrase-proximity.md)
for the format, the routing table, the measured cost, and the reindex
boundary.

Prefix terms (`TermPrefix` on `Bm25Search`, on a `QueryField`, and on the
single lexical leaf of `Query`) expand against each shard's byte-sorted
term directory and refuse past a cap naming the count, never truncating.
Facet and map dictionaries are written in byte order too, so CEL string
ordering (`court < "b"`) and `startsWith` compile to one ordinal range;
a file with an older first-seen dictionary serves equality and refuses
ordering by name. See [`docs/prefix-terms.md`](docs/prefix-terms.md).

Server-side highlighting (`HighlightSpec` on `Bm25Search` and on the
lexical `Query` leaf) returns sentence-bounded snippets per hit with the
occurrence spans merged, cut by the shard from the stored text and the
sentence spans it kept at ingest (`--sentence-fields=body`); no analyzer
runs on the query path, offsets are UTF-16 units of the original text,
and a field without spans refuses sentence mode by name (window mode
cuts at whitespace instead). See [`docs/highlighting.md`](docs/highlighting.md).

Collections (`[[collections]]` in the config file, `collection` on every
public request) let one cluster serve many datasets with no shared state
between them: each collection has its own shard set, topology,
statistics, calibration, analysis backend, and control plane, and a node
serves only one (`--collection`, checked at ingest, in the WAL
manifest, and in health). Unknown names refuse, and an unnamed request
gets to a named collection only through a configured default. See
[`docs/collections.md`](docs/collections.md).

Segments are the default layout of a new persisted shard
(`docs/immutable-segments.md`): the catalog under `<index>.segments/` plus
a heap tail that each flush (or `--seal-tail-docs`) seals into a new
immutable segment; queries read every segment and the tail as one shard
with global statistics, chained block-max cursors, and union
dictionaries. `--layout=single-image` keeps the one-file layout, existing
single-image shards keep theirs, and nothing converts on open.

Security (`docs/security.md`): `--tls-cert`/`--tls-key` put every
listener on TLS (rustls; plaintext is then refused, and off loopback it
needs `--allow-plaintext`), node listeners require a client certificate
from `--tls-client-ca`, cluster control demands one per call, the
coordinator presents `--tls-client-cert` to its nodes, `--bearer-tokens`
declares public principals with `max_k`, concurrency, and ingest-rate
quotas that refuse by name, and `--udp-hmac-key` signs the UDP floor and
cancel datagrams.

Select native analysis with `--analysis-addr=native` or
`analysis_addr = "native"`. A single-shard `both` process propagates that
setting to its shard. Multi-shard node configurations set `analysis_addr` on
each shard. An absent backend remains an error.

**Ingest** (`NodeService.AddDocuments`, client-streaming): the node carries
the whole call through one analyzer stream. Native analysis uses bounded
in-process channels; OpenNLP uses `AnalyzeStream` and its server-side flow
control. Results are applied in arrival order. A sidecar that predates the
stream RPC is refused rather than silently downgraded. Either provider builds
the same postings and stores the raw text. Doc
ids share the shard's positional id space with vectors (next id =
max(vectors, docs)). Analysis options pass through (`AnalysisSpec`:
tokenizer/stemmer/term-vector mode+source/normalizer rungs, as the
sidecar's enum numbers). Unsupported native options fail explicitly. Per-shard
`analysis_addr` in the config; unset means UNAVAILABLE.

**Query** (`SearchService.Bm25Search`) — distributed correctness via global
statistics followed by an exact candidate stream:

```mermaid
sequenceDiagram
    participant C as coordinator
    participant A as analyzer
    participant S as shards
    C->>A: 1. Analyze(query text, same options)
    A-->>C: query terms
    C->>S: 2. TermStats{terms}
    S-->>C: per-shard df, N, Σlen
    C->>S: 3. Bm25QueryStream{terms, globals, k1, b}
    Note over S: every shard scores with IDENTICAL idf/avgdl
    S-->>C: 4. candidate batches
    C->>S: monotone global k-th floor raises
    S-->>C: 5. completion + fingerprint + local details
    Note over C: only global heap selects winners
```

Shard-local BM25 stats would make scores incomparable across shards; the
global-stats fan-out is what keeps distributed ranking identical to a
monolithic index (proven exactly by
`tests/bm25_search.rs::distributed_bm25_matches_monolithic_exactly`, and
`shard_local_stats_would_differ` guards the regression). The stream is default
on for flat and fused multi-field BM25; `--bm25-stream=false` provides an exact
legacy-unary A/B route. Phrase-aware BM25 remains unary. Hits carry
per-term occurrence offsets; fetch raw text with
`NodeService.GetDocuments` to highlight. BM25 k1/b are configurable
(`bm25_k1`/`bm25_b`, defaults 1.2/0.75) and sent to every shard with the
query so scoring is uniform.

**Block-max pruning** (see `docs/block-max.md`): the v5 `.bm25` format
stores per-term doc/occurrence/skip runs with two-level impact blocks
(Lucene-style block-max), and scoring skips postings that provably
cannot reach the running floor — bit-identical results, measured up to
~70x on high-df terms at k=10. `Bm25SearchRequest.min_score` seeds the
whole fleet with a floor the client already holds (e.g. a previous
query's `kth_best`, re-issued after appends); `min_score` and `kth_best`
are additive, optional, and 0 means unseeded. `--block-max=false`
(`PIPESTREAM_SEARCH_BLOCK_MAX`) forces the exhaustive scorer for A/B; results are
identical either way. `cluster_sweep --bm25-terms` sweeps the
`{seeding} x {block-max}` factorial with a hit-signature gate. The coordinator
owns the only authoritative global lexical heap, and it answers only after
every shard returns `completed=true` with the same non-empty scoring
fingerprint. See [docs/block-max.md](docs/block-max.md) for the proof and wire
contract.

**Persistence**: postings + doc store live in `<index path>.bm25` (custom
versioned binary format, atomic write), flushed with `Flush` and on
graceful shutdown, loaded at startup when present.

### Live interop with grpc-opennlp-analysis

Verified against the native sidecar rather than the test mock. The vendored
proto is byte-identical to upstream (drift-checked with `diff` before the
run). Setup:

```bash
# 1. the sidecar (native binary, no models needed)
PORT=59101 .../grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis &

# 2. a pipestream-search node+coordinator pointed at it
pipestream-search --role=both --index=/tmp/tv-live/shard-0.tv \
    --node-listen=127.0.0.1:50051 --coord-listen=127.0.0.1:50050 \
    --nodes=127.0.0.1:50051 --analysis-addr=127.0.0.1:59101 &

# 3. ingest + query + highlight (example in this repo)
cargo run --release --example ingest_demo -- \
    --node=127.0.0.1:50051 --coordinator=127.0.0.1:50050
```

`examples/ingest_demo.rs` ingests 4 documents with a real spec (WHITESPACE
tokenizer, PORTER stemmer, MODE_FULL, SOURCE_STEMS), prints the TermStats
(terms in the postings are real OpenNLP Porter stems), runs Bm25Search,
and slices one highlighted span out of the stored raw text. Captured
output:

```text
ingested 4 documents (total 4, first global id 0)

TermStats (df per term — Porter stems as stored in the postings):
  dog        df=2
  bark       df=2
  run        df=1
  runner     df=1
  fox        df=2
  kitchen    df=1
  (shard docs: 4, total doc length: 35)

query "dogs barking":
  doc 1 score 1.4367  dog@[9,12)  bark@[13,18)
  doc 0 score 1.3703  dog@[4,8)  bark@[13,20)
  highlight: doc 1 span [9,12) of "A single dog barks at every passing runner" = "dog" (term "dog")

query "running":
  doc 0 score 1.1901  run@[35,42)
  highlight: doc 0 span [35,42) of "The dogs are barking loudly at the running foxes" = "running" (term "run")

query "fox":
  doc 0 score 0.6851  fox@[43,48)
  doc 2 score 0.6851  fox@[32,35)
  highlight: doc 0 span [43,48) of "The dogs are barking loudly at the running foxes" = "foxes" (term "fox")
```

This output distinguishes the sidecar from the mock: "dogs"/"barking"
land as stems `dog`/`bark`, "foxes" as `fox` — and doc 2's capitalized
"Running" does not group under `run` (df=1), because the sidecar's
stemmers are case-sensitive on the token surface form exactly as its proto
documents; the mock lowercases it. Offsets slice the original
surface forms out of the stored text (`"running"`, `"foxes"`).

## Court-opinion pipeline (chunk + embed + ingest)

Two-stage-plus-ingest pipeline over the court-opinion corpus
(`/work/court-corpus/opinions-sample.ndjson`, 264k full-length opinions,
`{"id", "cluster_id", "plain_text"}`; not copied into the repo). The
DEFAULT embedding path is the OpenNLP analysis sidecar's static
embeddings; TEI/bge-m3 (`court_embed`) remains the quality path.

### Static-embedding path (default for court)

The native sidecar serves Model2Vec-family static embeddings (distilled
all-MiniLM-L6-v2, WordPiece layout, 256-dim) when started with
`OPENNLP_EMBEDDINGS_DIR` (the binaries spawn it that way; an instance
with the model lives at `/work/court-corpus/models/minilm-l6-v2-static`).
Its sentence detection is NEWLINE-BASED, so "sentences" are
paragraph-ish blocks with exact original-text offsets.

1. **Chunk + vector in one pass** (`court_chunks` v2): per opinion, ONE
   Analyze (sentence detection + whitespace tokens + EmbeddingOptions
   SOURCE_SENTENCES) returns per-block 256-dim vectors. `plan_chunks`
   packs whole blocks to ~`--target-tokens` (256) and splits blocks over
   `--hard-cap-tokens` (1024) at their own token boundaries via a solo
   re-Analyze — never dropping text, so the CONTIGUITY INVARIANT holds
   and is asserted per opinion (concatenated chunk texts reproduce
   `plain_text` byte-for-byte). Packed-chunk vectors are the
   token-weighted pool of the block vectors, L2-normalized: EXACT for a
   mean-pooled static table (mean of concatenation = weighted mean of
   block means) when weights are the analyzer's token counts, and
   normalization matches the model's own `Normalize` stage (turbovec
   scores true dot products, so vectors must be unit-length).
   Outputs `chunks.ndjson` + `embeddings-static.bin`, both resumable.
   Measured ~860 opinions/s (~9,000 chunks+vectors/s) — about 27x the
   TEI embedding stage's ~326 chunks/s end-to-end.
2. **Ingest** (`court_ingest`): unchanged mechanics — joins chunks and
   embeddings on `(opinion_id, ordinal)`, contiguous blocks to N shards,
   provider fit + BroadcastVectorBackend, AddDocuments with `DocLineage`
   + AddVectors with aligned ids, Flush (e.g.
   `/work/court-corpus/shards-static/`). dim comes from the embeddings
   file header (256 here, 1024 for TEI).

Pooling honesty note: block embeddings are normalized WordPiece-subtoken
means, while chunk weights are whitespace token counts, so pooled vs a
direct whole-chunk embedding agrees at ~0.95 median cosine (measured
min 0.86 / median 0.95 over 500 chunks) — not the theoretical 1.0,
because subtoken counts are not exposed by the analysis API. All chunks
share the same weighting, so scores stay mutually comparable.

### TEI/bge-m3 path (quality runs)

`court_embed` streams chunks through TEI (`tei-bge-m3` container, native
gRPC `localhost:8095`, bge-m3 fp16, 1024d; vendored proto at
`proto/tei/v1/tei.proto` from huggingface/text-embeddings-inference
v1.9.3). `normalize=true`, `truncate=false` (the chunker bounds inputs
well under TEI's 8192-token cap). Measured ~300 chunks/s (GPU-bound) —
use it when bge-m3 quality matters more than ingest speed; the two
vector families produce separate shard sets, never mixed.

Court/date metadata join from the CSVs is out of scope (second pass);
only opinion id + cluster id + span lineage is carried. Resume: every
stage skips work already present (`--limit` everywhere for slices).

## Real-data shakedown (wikipedia bge-m3)

`examples/wiki_shakedown.rs` ingests a full corpus end-to-end: the
bge-m3 embeddings + Simple English Wikipedia sentence pairs from the
earlier Lucene/OpenSearch distributed testing
(`/work/opensearch-grpc-knn/distributed_test_data/wikipedia/`, not
copied into this repo). Format: per part, a `.bin` of big-endian
`i32 count | i32 dim | count × dim f32` (61077 x 1024 per part, 4
parts) plus a one-sentence-per-line text file. The text files have no
trailing newline, so `wc -l` says 61076; the final newline-less segment
is a record (parser asserts exact vector/text pairing).

The 4 parts are the pre-existing shard partitioning: part N goes to
shard N, doc id `N * 61077 + index`. The run:

```bash
cargo run --release --example wiki_shakedown   # --data-dir, --out-dir, --sidecar-port
```

loads the parts, fits calibration on a sample, pushes it to every shard
with `BroadcastVectorBackend`, starts the GraalVM-native OpenNLP sidecar
(falls back to the in-repo mock with a loud warning), ingests documents
(AddDocuments, PORTER stems) and vectors (AddVectors) with aligned ids,
persists `.tv` + `.bm25` under `--out-dir` for the later two-machine
run, and runs hybrid cascade queries, printing per-leg scores and top
hit texts. Eyeball: the probe doc ranks first by vector score (self
match), BM25-rich siblings outrank vector-only neighbors, and query
terms slice correctly out of the stored text.

dim=1024 is just config (the parser reads it from the header). The ULP
caveat from `tests/score_layout.rs` applies: raw vector scores are
bit-exact only within same-shape kernel paths.

## Hybrid search: cascade (default), global-rank RRF, score blend, two-level RRF

`SearchService.HybridSearch{text, vector, k, analysis, legs}` offers
four modes (`HybridLegOptions.fusion_mode`); unspecified resolves to
**FUSION_MODE_CASCADE**.

### FUSION_MODE_CASCADE (default): vector gate, then BM25 rerank

No score fusion at all — the legs stay separate and only the cutoff is
shared:

1. **Phase 1, candidate generation.** The vector leg runs through the
   EXISTING floor-sharing bidi path (`SearchShard`), so cross-shard
   early termination applies: once any shard holds k candidates, its
   k-th best lower-bounds the global cutoff, every shard learns it, and
   the prefilter skips provably-dead blocks. This is the savings: the
   GLOBAL_RANK path makes every shard full-scan and ship leg_k hits
   (at k=10000 over ~10 shards that is the ~10x-k waste); cascade ships
   roughly k + boundary ties total.
2. **Tie-complete cutoff.** The pool is `{score >= s_k}` where s_k is
   the global k-th vector score — score-defined, hence layout-invariant.
   Floors let docs AT the floor through (only strictly-below is pruned),
   and the shard's running top-k (StartShardSearch.tie_complete) never
   evicts candidates tied at its current k-th score, so the whole
   boundary tie group rides along on every shard. The pool can exceed k
   by the tie-group size (worst case: a shard of identical scores).
3. **Phase 2, BM25 rerank.** The query is analyzed (same AnalysisSpec as
   ingest), each candidate is routed to its owning shard, and just those
   ids are scored against the postings (`NodeService.Bm25Rescore`:
   merge-join over the append-only, doc-id-sorted postings lists — no
   full postings walk) with the global idf stats, so scores are
   cross-shard comparable. Rerank: BM25 desc, vector desc, doc id asc;
   return the top k of the pool. Hits carry `vector_score` and
   `bm25_score` as separate fields plus the final rank — no fused score.
   The rescore is one stage behind a small seam; more rankers plug in
   later.

**Honest trade-off**: cascade makes vector recall a GATE for the BM25
leg — a keyword-strong but vector-weak document never enters the pool
and is not surfaced by HybridSearch in this mode. Use `Bm25Search` for
pure lexical queries and GLOBAL_RANK when you want true fusion. The
k=10000 sweep will benchmark cascade vs GLOBAL_RANK on exactly this
cutoff saving.

### FUSION_MODE_GLOBAL_RANK: exact RRF over global rankings

Shards return raw per-leg top-leg_k lists (`ShardLegs`); the coordinator
merges each leg across shards BY RAW SCORE into global rankings and
applies single-level RRF (`fused = Σ weight/(rrf_k + rank)`, rrf_k=60,
weights default 1.0). With globally comparable scores per leg this is
EXACTLY the monolithic result for k <= leg_k: a shard's leg is a
subsequence of the global leg (local rank <= global rank), so the union
of shard lists contains the exact global top-leg_k and merging by score
reconstructs it. Leg ranks use competition ranking (tied scores share a
rank) so fused scores are layout-invariant.

### FUSION_MODE_SCORE_BLEND: normalize and weighted-combine

Same leg-fetch path as GLOBAL_RANK (raw `ShardLegs`, global merge per
leg); only the fusion function differs. Each merged leg is truncated
TIE-COMPLETE to leg_k (the retained set is `{score >= s_k}`,
score-defined and thus layout-invariant), its retained scores are
normalized (`ScoreNormalization`: min-max onto [0,1] / z-score / none),
and each doc's normalized leg scores combine (`ScoreCombination`:
weighted arithmetic, geometric, or harmonic mean) with
vector_weight/bm25_weight. Rank-free, so score GAPS survive fusion — a
runaway leg leader stays far ahead where RRF compresses every gap to
the distance between adjacent ranks. Normalization statistics are
computed over the GLOBAL retained set, never per shard, which is what
keeps the mode partition-independent (pinned bitwise on the adversarial
partition test for every normalization). Semantics corners are on the
proto enums: absent legs under arithmetic contribute 0 with their
weight still counted (the classic weighted-sum formula);
geometric/harmonic skip non-positive scores and renormalize weights, so
pair them with min-max, not z-score.

### FUSION_MODE_TWO_LEVEL: fallback for incomparable scores

Each shard RRF-fuses its legs locally (`HybridShard`); the coordinator
RRF-merges the shard lists. Rank-based, needs NO comparable scores, but
NOT partition-independent (local ranks are compressed vs global ranks).
Use only when shards cannot share a calibration.

### Shared provider scoring identity and its caveats

`SearchService.BroadcastVectorBackend` pushes one opaque provider
configuration to every shard. Configure before ingest and verify matching
`scoring_fingerprint` values with `GetVectorBackend`. Provider configuration
is locked for a generation, so a scoring-identity change is a coordinated
rebuild and cutover event. The old calibration RPCs remain compatibility
adapters for existing embedded-TurboVec clients.

Fork caveats (read from the turbovec source, pinned in
`tests/score_layout.rs`): with a shared calibration, encoding is a pure
function of (vector, calibration, dim, bit width), and scores are
bit-identical across indexes of the SAME shape. Across DIFFERENTLY-SIZED
indexes the kernel's accumulation order can shift a score by a couple of
ULPs — so raw vector scores across shards are comparable but only
bit-exact within same-shape kernel paths; ordering is robust except
within ULP-ties (the tests assert exact ids/ranks/BM25 bits and vector
scores within a few ULPs).

**Per-leg k** (GLOBAL_RANK/SCORE_BLEND/TWO_LEVEL): leg_k defaults to
`max(k, rrf_k)`; override in `HybridLegOptions`, clamped to >= k.
Cascade ignores leg_k/weights/rrf_k (its depth is k plus ties).

**Leg disabling**: the weights are presence-aware (`optional`): absent
= 1.0, an EXPLICIT 0 turns the leg off (GLOBAL_RANK/SCORE_BLEND only;
both-off and TWO_LEVEL-off are rejected). A single-leg query is how
"vector primary" or "lexical primary" runs through the hybrid path,
composing with boost and debug — e.g. vector-only SCORE_BLEND with
`normalization=none` ranks by raw vector score, and a boost then adds
`boost_weight * bm25(boost text)` on top.

**Vector-score floor**: `HybridLegOptions.min_vector_score` requires
every returned hit to have a vector-leg score at or above the floor
(docs absent from the vector leg drop too). Applied BEFORE fusion,
truncation, and boost in every mode — deeper qualifying docs are
promoted rather than the list shrinking, blend statistics see only the
filtered set, and in cascade it tightens the phase-1 gate ahead of the
rescore fan-out. Score-defined, hence layout-invariant. 0 = off.

### The console (test harness UI)

`cargo run --release --bin console -- --coordinator=host:port
--nodes=host:port,... --analysis=host:port [--listen=127.0.0.1:8600]`
serves a single-file web UI for exercising every hybrid knob by hand
against a RUNNING cluster (the console is purely a client). Query text
is embedded through the sidecar's Model2Vec model (`EmbeddingOptions`,
sentence embeddings mean-pooled and L2-normalized), the search runs
through the coordinator's `HybridSearch` with `debug` always on, and
hit texts come from the owning nodes (`GetDocuments`, which is why the
console takes the node list in shard order). The UI exposes fusion
mode, leg_k/rrf_k/weights, score-blend normalization + combination,
boost rescore, and the analysis spec (tokenizer/stemmer/term source —
must match ingest); renders per-hit provenance with term highlighting,
the phase-timing bar, and the per-shard waterfall (cascade scan stats
included); and holds any result as "A" for side-by-side comparison
with movement markers.

### Boost rescore (any mode)

`HybridSearchRequest.boost{text, window, base_weight, boost_weight}`
adds a second-pass lexical boost after fusion: the top `window` hits
(0 = all) are rescored as `base_weight * base + boost_weight *
bm25(boost text)` and reordered; hits beyond the window keep their
relative order after it. `base` is the mode's ordering score (fused
score, or phase-2 BM25 for cascade). The boost runs candidate-scoped
through the existing `Bm25Rescore` seam with global stats fetched for
the BOOST terms — one TermStats fan-out plus one Bm25Rescore per
owning shard, never a full postings walk. Hits carry `boost_score`
separately so clients see both parts; the debug block reports
`boost_terms` and `boost_ms`.

## Ingest flow (write path)

Shards ingest over gRPC; prebuilt provider images are not required.
Deployment order for a from-scratch cluster is **fit → configure → ingest →
search**:

1. **Fit** provider state on a representative sample. The provider owns the
   fitting algorithm and returned payload.
2. **Configure** every empty shard with the same payload via
   `NodeService.ConfigureVectorBackend`, or copy it from a configured node:
   `pipestream-search configure-backend --fit-from=node0:50051 --apply-to=node1:50051,node2:50051`.
   An identical retry is idempotent. A different configuration or any change
   after vectors exist is rejected.
3. **Ingest** with `NodeService.AddVectors` (client-streaming, flat
   batches). Batches apply under the shard's write lock; searches hold the
   read lock for their whole scan, so no search observes a half-applied
   batch. Ids are server-assigned: the i-th vector of a shard is
   `slot_offset + i` (positional; turbovec's id-mapped index does not
   support the masked, floor-seeded scan this service uses).
4. **Search** as before — the lossless invariant holds for ingested data
   exactly as for prebuilt indexes (proven by `tests/multiprocess.rs`).

**Persistence**: `NodeService.Flush` writes the provider image to its config
`index` path, original FP32 rows to `<index>.exact`, and the live-row overlay
to `<index>.live`, then reopens the exact sidecar through mmap.
`save_on_shutdown = true` (the
default) flushes on SIGINT/SIGTERM. A shard whose index path does not
exist at startup starts empty; after ingest + flush (or graceful
shutdown), a restart with the same config comes back with all vectors
and the provider configuration. An empty but configured shard also persists
on shutdown. Resetting provider identity requires a new generation.

### Bulk load: InstallSnapshot

For pre-computed corpora, skip per-node ingest entirely: build the
shard image once (with the cluster's shared provider configuration) and push the
FINISHED index to every shard owner over one client stream —
`NodeService.InstallSnapshot`. The bundled
`snapshot::install_snapshot_generation(addr, vector_path, exact_path,
bm25_path, live_docs_path)` client sends all four aligned artifacts;
`install_snapshot_with_exact` sends an all-live generation; the compatibility
`install_snapshot` helper sends a native-only generation.

The node stages the image in a generation directory
(`<index path>.snap/`), validates it (well-formed provider image, exact-vector
checksum and shape when present, BM25 sidecar opens, and scoring fingerprint
matches the configured shard). A mismatch is rejected,
keeping scores comparable cluster-wide. The node then swaps it live
under the write lock. All files travel inside ONE directory rename, so
the generation is installed atomically; a crash mid-swap is recovered
deterministically at startup (see `recover_generation`). Once a shard
serves from a generation, Flush and restart loading follow it — the
legacy layout and the generation never split-brain.

Rules of thumb: configure the provider first (or let an empty shard adopt
the image's state), replace every shard together on scoring changes, and expect
an image without a `.bm25` sidecar to wholesale-replace the postings
store. An image without `vectors.f32` remains valid for provider-native
queries but cannot serve FP32 rerank. Covered by `tests/snapshot.rs` (eight
cases, including restart survival and crash recovery).

Deletes and replacements use one generation-local tombstone overlay shared by
every read path. Updates append the new row, then atomically retire the old
row; one-child resharding compacts tombstones into a new dense generation. See
[docs/mutations.md](docs/mutations.md) for the consistency and cutover rules.
Large compactions can instead publish several bounded, row-aligned immutable
segments under one atomic catalog snapshot; see
[docs/immutable-segments.md](docs/immutable-segments.md).

### Write log and resharding

Design rationale, invariants, and the deferred-work list:
[docs/resharding.md](docs/resharding.md).

Every shard with an `index` path keeps a write-ahead log at
`<index path>.wal/` (on by default; `--wal=false` / `PIPESTREAM_SEARCH_WAL=false`
to disable, always off for `--demo-vectors`). The log is a folder of
hash-bucketed files per generation:

```text
<index path>.wal/gen-<generation:06>/
    manifest.toml       dimension, provider state, slot offset,
                        bucket geometry, format version
    bucket-<NNN>.wal    records routed by fnv1a64(id) >> (64 - bucket_bits)
    markers.wal         FlushMarker / SnapshotMarker records
```

Frames are `[u32 len][u32 crc32][prost WalRecord]` with a 1-based gapless
sequence per file and a generation-wide logical clock, written after the mutation successfully applies under the shard
lock. Writes are
buffered; only Flush and generation rotation fsync, and the log is never
on the search path. A crash can leave a torn tail frame in any file;
replay ignores it (with a warning), and a restarted node truncates the
tail and continues that file's sequence — damage stays scoped to the one
bucket file it happened in. Provider state starts incomplete on a from-scratch
shard and the small manifest is rewritten atomically when it locks. A snapshot
install supersedes the log, so the node rotates to `gen-(g+1)` with a
fresh manifest (the installed image's provider state, same bucket geometry)
and a `SnapshotMarker` in its `markers.wal`.

Because records are routed by the SAME partition function the reshard
tool splits by (`bucket = fnv1a64(vector_id) >> (64 - log2(N))`), each
bucket file is a pre-partitioned log slice: a split with
`N <= bucket_count` hands each child a contiguous range of bucket files
without re-hashing a record. Finer splits still work but re-partition
every record — **`bucket_count` (default 64, `--wal-buckets` /
`PIPESTREAM_SEARCH_WAL_BUCKETS`, power of two, max 1024) caps cheap split
granularity**. The count is fixed at WAL creation; a resumed log keeps
its own and warns if the flag disagrees. Choose it with the bulk load's
growth in mind.

This is what makes split/merge of live shards replay-from-log instead of
re-embed:

1. **Snapshot** the shard (InstallSnapshot) — the base image.
2. **Catch up** by replaying the WAL generation(s) written after the
   snapshot.
3. **Swap** the reshaped images in (InstallSnapshot again) and point the
   coordinator at the new topology with `--shard-map`.

The `reshard` example is the offline tool for step 2:

```sh
# Split one shard 1 -> N (N a power of two):
cargo run --release --example reshard -- \
    --log=/data/shard-0.vector.wal --split=2 --out-dir=/data/split \
    --slot-base=0 --slot-stride=25000000 --analysis-addr=http://localhost:50051 \
    --stable-routing

# Merge several shards -> 1 (identical provider state AND bucket count):
cargo run --release --example reshard -- \
    --logs=/data/shard-0.vector.wal,/data/shard-1.vector.wal --out-dir=/data/merged \
    --analysis-addr=http://localhost:50051
```

It writes `<out>/shard-<i>.vector` (+ `.bm25`, documents re-analyzed with
their ingested analysis options), a `shard-map.toml`, and prints the
matching `[[shards]]` node config blocks. Invariants, enforced hard:

- **One provider configuration per split/merge.** The WAL manifest must carry
  locked provider state; merge requires byte-identical backend configuration
  and identical bucket counts across all inputs. Unconfigured shards cannot
  be resharded because their scores cannot be certified comparable.
- **Ids are generation-scoped.** Children re-assign dense local slots in
  original id order and take their slot base from the new shard map
  (stride 25M by default, matching deploy/court-e2e); parent ids never
  leak into a child.
- **The shard map is the id-to-shard authority.** `--shard-map=<file>`
  (`PIPESTREAM_SEARCH_SHARD_MAP`) on the coordinator replaces `--nodes` (passing
  both is an error): `generation = N` plus `[[shards]]` with `addr`,
  an optional `replica` (failover and hedged retries; see
  "Operability"), `slot_offset`, and the child's `hash_lo`/`hash_hi`
  range. The coordinator logs the generation at startup; plain `--nodes`
  keeps working as the implicit generation 0.

`--stable-routing` is the hitless 1→N baseline. It partitions by the stable
product keys recorded by routed mapped ingest and emits `live-cutoff.toml`.
After starting the children, `examples/live_reshard.rs` initializes durable
catch-up state, tails while the parent serves, and performs the final
freeze/catch-up/map-publish cutover. See [docs/resharding.md](docs/resharding.md)
for the exact commands and failure recovery.

Two operational rules follow from the design:

- **A shard without a WAL can serve but can never be split or merged —
  only rebuilt from source.** The log IS the resharding input; keep it
  on for any shard you may ever want to reshape.
- **Live compaction is a self-snapshot.** To bound log growth, push the
  shard's own flushed image back through InstallSnapshot: the install
  supersedes the log and rotates to a fresh generation directory, so old
  `gen-*` directories can be archived once the swap is confirmed.

Covered by `tests/reshard.rs`: split 1→2 reconstructs the parent's top-k
bitwise (union of child top-k, ids remapped), a split with
N == bucket_count consumes every bucket exactly once, a finer-than-
buckets split re-partitions correctly, merge 2→1 reproduces the
monolithic top-k and the BM25 doc set, and mixed-provider-configuration or
mixed-bucket-count merges are rejected.

## Two-machine embedded-TurboVec runbook

Topology: host A (this host) runs coordinator + shard 0; host B (`host-b`)
runs shard 1. Static membership — both configs list the same node set.

1. **Build and produce shard indexes** on host A:

   ```bash
   cargo build --release
   ./target/release/sweep --vectors=100000 --shards=2 --k=10 --queries=1 \
       --modes=off --write-indexes=/data/turbovec
   # writes /data/turbovec/shard-0.tv, shard-1.tv (same seeded calibration)
   ```

   (Any source of `.tv` files works as long as every shard was built with
   the SAME seeded calibration — that is what makes scores mergeable.
   Verify matching scoring fingerprints with `NodeService.GetVectorBackend`.
   Alternatively,
   skip files entirely: point each node's `index` at a fresh path, start
   empty, then seed + ingest over gRPC per "Ingest flow" above.)

2. **Copy the binary and shard 1 to host-b:**

   ```bash
   scp target/release/pipestream-search host-b:/usr/local/bin/
   scp /data/turbovec/shard-1.tv host-b:/data/turbovec/
   ```

3. **Config on host-b** (`/etc/turbovec/host-b.toml`):

   ```toml
   role = "node"
   [[shards]]
   listen = "0.0.0.0:50051"
   index = "/data/turbovec/shard-1.tv"
   slot_offset = 50000          # = vectors in shard 0 (contiguous offsets)
   ```

   Start: `pipestream-search --config /etc/turbovec/host-b.toml`

4. **Config on host A** (`/etc/turbovec/host-a.toml`):

   ```toml
   role = "both"
   coord_listen = "0.0.0.0:50050"
   nodes = ["host-a:50051", "host-b:50051"]

   [[shards]]
   listen = "0.0.0.0:50051"
   index = "/data/turbovec/shard-0.tv"
   slot_offset = 0
   ```

   Start: `pipestream-search --config /etc/turbovec/host-a.toml`

5. **Verify.** From host A (or any host that can reach `host-a:50050`),
   issue a real search. The binary's built-in check does one:

   ```bash
   pipestream-search --role=coordinator --nodes=host-a:50051,host-b:50051 \
       --coord-listen=127.0.0.1:59999 --demo-query --query-dim=128
   ```

   (spins a throwaway coordinator against the running nodes and prints the
   merged top-10). Or call `SearchService.Search` with any gRPC client
   against `host-a:50050` — proto at `proto/ai/pipestream/search/v1/search.proto`.

6. **The large-k two-machine experiment** uses `cluster_sweep`, which
   drives a pre-existing cluster over the network (no in-process shards).
   Floor sharing is a node-side flag, so run TWO clusters side by side —
   same shard files, different ports — and point the binary at both:

   ```bash
   # per shard, on the machine owning it (setsid to survive ssh):
   setsid nohup pipestream-search --role=node --index=/tmp/wiki-shards/shard-N.tv \
       --slot-offset=OFFSET --node-listen=0.0.0.0:PORT \
       --floor-sharing=true  > node-sharing.log 2>&1 &
   setsid nohup pipestream-search --role=node --index=/tmp/wiki-shards/shard-N.tv \
       --slot-offset=OFFSET --node-listen=0.0.0.0:PORT2 \
       --floor-sharing=false > node-nosharing.log 2>&1 &

   # then, anywhere with corpus access for probe vectors:
   cluster_sweep \
     --nodes-sharing=host-a:50061,host-a:50062,host-b:50063,host-b:50064 \
     --nodes-nosharing=host-a:50071,host-a:50072,host-b:50073,host-b:50074 \
     --k=10,100,1000,10000 --queries=20
   ```

   It reports candidates, floor counters, and wall p50/p90/p99 per mode
   per k and asserts the sharing on/off correctness gate (identical hit
   signatures) per k. `--nodes-nosharing` is optional: omit it to
   benchmark a single cluster, and add `--warmup=N` (discarded probes,
   default 2), `--concurrency=N` (parallel clients; the qps column
   becomes meaningful), `--label=NAME`, and `--json=bench.jsonl`
   (machine-readable records, appended) for load-test runs:

   ```bash
   cluster_sweep \
     --nodes-sharing=node1:50051,node2:50051,node3:50051,node4:50051 \
     --k=10,100,1000 --queries=100 --warmup=5 --concurrency=8 \
     --probes-from=/corpus/embeddings.bin --label=4shard-200gb \
     --json=bench.jsonl
   ```

   Since turbovec v5 (block-Hadamard rotation) the release binary is
   fully self-contained — no OpenBLAS/libgfortran to ship. (Pre-v5
   builds linked system OpenBLAS and needed `libopenblas.so.0` +
   `libgfortran.so.5` under `LD_LIBRARY_PATH` on bare hosts.)

   Executed 2026-07-27 on the wiki shards (4 x 61077 bge-m3 1024d docs;
   shards 0+1 on host-a, 2+3 on host-b): correctness gate green at every
   k; candidate reduction from sharing fell from ~7% at k=10 to ~3% at
   k=10000 (the leg approaches a full scan), with wall medians ~20-24ms
   and no consistent wall win at this scale.

## Testing and benchmarking

```bash
cargo test            # unit + integration (lossless incl. k=1000, loopback, benchmark)
cargo test --release --test bench_sharing -- --nocapture   # with numbers
```

The benchmark (`tests/bench_sharing.rs`) runs 50 queries against a 60k
corpus on 3 shards, with and without sharing, and reports
`candidates_collected` (every candidate that survived the floors in effect
when its chunk ran — the kernel-visible proxy for skipped work, since the
kernel exposes no block-skip counter) plus wall-time medians. It asserts
identical hit sequences in both modes and strictly fewer collected
candidates with sharing. `tests/lossless.rs` additionally proves exact
losslessness at k=1000 over a 24k corpus.

## Layout

- `proto/ai/pipestream/search/v1/search.proto` — the wire API (heavily
  commented), codegen via `build.rs` + tonic-build.
- `src/chunked.rs` — the chunked scan (mask per chunk, floor seeding,
  running heap, publish/poll points). Pure and unit-tested, including
  k=1000.
- `src/merge.rs` — global top-k merge (total order: score desc, shard, id)
  and the coordinator's floor tracker.
- `src/postings.rs` / `src/bm25.rs` — the BM25 postings index, doc store,
  persistence, and scoring (with externally supplied global stats).
- `src/fusion.rs` — reciprocal rank fusion (used at both fusion levels)
  and score-blend fusion (normalize + weighted-combine) over scored
  legs, with per-leg provenance.
- `src/analyzer.rs` — the analysis-sidecar client (text in, term vectors
  out). No local analysis by design.
- `src/vocab.rs` — the vocabulary index: streaming corpus statistics
  (HLL + count-min + space-saving, two channels) accumulated inline in
  the AddDocuments AnalyzeStream path, snapshot per window to
  `<index path>.vocab/`. Off by default (`--vocab=true`);
  `examples/vocab_drift.rs` reads and merges snapshots. See
  `docs/VOCABULARY-INDEX.md`.
- `src/node.rs` / `src/coordinator.rs` — the two gRPC services. The node
  owns the shard state machine (empty → seeded → live) behind a write
  lock: chunked scans under the read lock, adds/calibration under the
  write lock, flush on demand or shutdown.
- `src/config.rs` / `src/main.rs` — TOML/env/CLI config and process wiring
  (multi-shard, multi-role, graceful shutdown, `calibrate` subcommand).
- `src/harness.rs` — corpus generation, calibration fitting, shard building
  and loopback server startup shared by tests and the sweep binary.
- `examples/sweep.rs` — the k-sweep benchmark harness.
- `tests/` — lossless e2e (k=10 and k=1000), NodeService loopback with
  mid-scan injection, ingest/calibration rules, a multi-process
  ingest-and-restart acceptance test, BM25 tests with a mock analysis
  sidecar (postings, distributed-vs-monolithic equality, local-stats
  regression guard, STEMS identity, flush persistence), hybrid fusion
  tests (determinism, provenance, partition-stable exactness), and the
  skipped-work benchmark.

## Storage and memory model (disk-resident BM25)

Shard storage has two shapes behind one read surface
(`postings::Bm25Index`):

- **Heap builder** (`Bm25Store`) — ingest appends here. `Flush` writes
  the v5 `.bm25` format and immediately reopens the shard disk-resident.
- **Disk-resident** (`Bm25Reader`) — the v5 file is memory-mapped;
  postings slices and document texts are read from the map on demand.
  The OS page cache is the buffer pool (the Lucene model): after
  `Flush` or on startup with a v5 file, a shard holds NO postings or
  document texts in heap — only the per-doc length table (4 B/doc) and
  small lookup structures. Measured: opening a 164 MiB file and
  serving queries grows RSS by ~11 MiB, versus ~159 MiB for the heap
  load (`tests/mmap_store.rs`).

v5 layout (single file, atomic write): header with absolute section
offsets, per-doc lengths, document texts, an on-disk text index
(fixed-stride entries, so text reads never walk the file), lineage,
then per sorted term a fixed-stride doc run, an occurrence run, and a
skip run of two-level impact blocks (see `docs/block-max.md`), and a
fixed-stride term directory (binary search per term to its run offsets
+ df). v3/v4 files still load and serve — into the heap builder on the
append path, upgraded to v5 on the next flush.
A disk-resident shard that receives more documents first reloads into
the heap builder (bulk-load discipline: build in memory, flush back).

This is what makes a corpus larger than machine memory work: the
postings (~130 GB at full CourtListener scale) and doc text (~40 GB)
live in page cache shared across all consumers, not per-process heap.
The provider vector index remains heap-resident today. Its product-owned
original-FP32 rerank sidecar is mmap-backed after flush or load and faults only
the candidate rows used by reranking. A v7 `load()` transforms
the stored sequential blocks into the native blocked search layout and retains
that heap representation; mmap support remains a fork-level decision (see the
TODO list).

## TODO

- **mmap vector index.** Postings and doc text are disk-resident (page
  cache); the turbovec index is heap-resident (see above). A
  packed-bytes abstraction — owned Vec or mmap behind one accessor,
  with a paged blocked cache — is a fork-level decision, reported not
  built.
- **Provider verification.** Ingest drivers fit and broadcast opaque provider
  state; health and `GetVectorBackend` expose the scoring fingerprint so a
  mismatched fleet can be rejected before traffic.
