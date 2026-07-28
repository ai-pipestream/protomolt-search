# turbovec-search

Distributed top-k search over [turbovec](https://github.com/ai-pipestream/turbovec)
(TurboQuant) shard indexes, with **collaborative mid-query floor sharing**:
shard nodes publish their current k-th best score while they scan, the
coordinator aggregates the maximum and pushes it back, and nodes prune the
remainder of their scan against it — losslessly.

Phase 1: one crate, one binary, three roles (`node`, `coordinator`, `both`),
tonic gRPC + tokio, static cluster membership.

## Quickstart: dockerized end-to-end demo (CourtListener)

A one-command installer that syncs real data and proves the whole stack:
CourtListener bulk opinions (public S3) → [rustfs](https://rustfs.com)
object store → Rust extraction → chunk + static-embedding via the
[grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis)
sidecar (GraalVM native, Model2Vec table) → a 4-shard turbovec-search
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
[search.proto](proto/turbovec/search/v1/search.proto) — `Search`
(vector top-k with floor sharing), `Bm25Search` (distributed lexical),
`HybridSearch` (RRF fusion) — or with the bundled probe tool:

```bash
docker run --rm --network court-e2e_default -v court-e2e_corpus-data:/corpus \
  --entrypoint court_query court-e2e-pipeline \
  --nodes=node1:50051,node2:50051,node3:50051,node4:50051 \
  --analysis-addr=http://analysis:50051 --embeddings=/corpus/embeddings.bin \
  --probe-ids=12345 --docs-per-shard=128500
```

## Design

```
                        ┌──────────────────────┐
                        │     coordinator      │
   client ──Search──▶   │   (SearchService)    │
                        │                      │
                        │  FloorTracker: max   │◀──┐ k-th best per shard
                        │  over shard floors   │   │ (once heap fills)
                        └───┬───────┬───────┬──┘   │
              Start+FloorUpdate │       │       │  │ FloorUpdate
            ┌───────────────────┘       │       └───────────────┐
            ▼                           ▼                       ▼
     ┌─────────────┐             ┌─────────────┐         ┌─────────────┐
     │   node 0    │             │   node 1    │   ...   │   node N    │
     │ (NodeService)│            │ (NodeService)│         │ (NodeService)│
     │ shard index │             │ shard index │         │ shard index │
     │ chunked scan│             │ chunked scan│         │ chunked scan│
     └─────────────┘             └─────────────┘         └─────────────┘
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
   (score descending; ties by shard index, then vector id) and answers the
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

Quantized scores are only comparable across separately built indexes if
every index encodes vectors identically. turbovec's seeded TQ+ calibration
provides this: fit the per-coordinate `(shift, scale)` once on a
representative sample (build a throwaway index from the sample, read
`calibration()`), then construct every shard with
`TurboQuantIndex::new_with_calibration`. Same calibration ⇒ byte-identical
codes for the same vector ⇒ per-slot scores are pure functions of the
vector, so shard scores can be merged directly. `NodeService.GetCalibration`
exposes a shard's calibration so deployments can verify uniform seeding.

## Running

```bash
cargo build --release

# Single-process demo: both roles, random demo corpus (calibration fitted
# on a 20% sample and seeded), one self-issued search at the end.
./target/release/turbovec-search --role=both \
    --demo-vectors=20000 --dim=128 --bit-width=4 \
    --nodes=127.0.0.1:50051 --demo-query

# A real shard node over a persisted .tv index.
./target/release/turbovec-search --role=node \
    --index=/data/shard-0.tv --slot-offset=0 --node-listen=0.0.0.0:50051

# A coordinator over three nodes.
./target/release/turbovec-search --role=coordinator \
    --coord-listen=0.0.0.0:50050 \
    --nodes=node0:50051,node1:50051,node2:50051
```

### Cluster configuration file

For real deployments the binary reads a TOML file (`--config cluster.toml`,
or `TURBOVEC_CONFIG`). Precedence: **CLI flag > env var > config file >
default**. Every flag takes `--key=value` or `--key value`.

```toml
role = "both"                                  # node | coordinator | both
coord_listen = "0.0.0.0:50050"
nodes = ["host-a:50051", "krick-1:50051"]      # fan-out order = tie-break order
chunk_blocks = 64                              # scan chunk size (SIMD blocks)
floor_sharing = true
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

Membership is **static**: the coordinator's `nodes` list and each node's
`[[shards]]` set are fixed at startup. Changing topology means editing
configs and restarting — deliberate for this phase. Single-shard shorthand
(`--index`, `--demo-vectors`, `--node-listen`, `--slot-offset`) overrides
the file's `[[shards]]` entirely.

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
texts (the highlight source). This repo deliberately contains **no query
parser and no text analysis**: language analysis is the
[grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis)
sidecar's job (`AnalysisService.Analyze` → term vectors; its proto is
vendored at `proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`, see
the file header).

**Ingest** (`NodeService.AddDocuments`, client-streaming): for each
document the node calls the sidecar (term vectors, MODE_FULL → offsets in
ORIGINAL text coordinates), builds postings, and stores the raw text. Doc
ids share the shard's positional id space with vectors (next id =
max(vectors, docs)). Analysis options pass through (`AnalysisSpec`:
tokenizer/stemmer/term-vector mode+source/normalizer rungs, as the
sidecar's enum numbers). Per-shard `analysis_addr` in the config; unset →
UNAVAILABLE.

**Query** (`SearchService.Bm25Search`) — distributed correctness via the
two-phase global-stats flow:

```
coordinator                                   shards
    │ 1. Analyze(query text, same options) ──▶ sidecar → query terms
    │ 2. TermStats{terms} ──────────────────▶ per-shard df, N, Σlen
    │ 3. global N, avgdl, Σdf ──▶ Bm25Query{terms, globals, k, k1, b}
    │                                         every shard scores with
    │                                         IDENTICAL idf/avgdl
    │ 4. merge (score desc, shard, doc id) ◀── shard top-ks + offsets
```

Shard-local BM25 stats would make scores incomparable across shards; the
global-stats fan-out is what keeps distributed ranking identical to a
monolithic index (proven exactly by
`tests/bm25_search.rs::distributed_bm25_matches_monolithic_exactly`, and
`shard_local_stats_would_differ` guards the regression). Hits carry
per-term occurrence offsets; fetch raw text with
`NodeService.GetDocuments` to highlight. BM25 k1/b are configurable
(`bm25_k1`/`bm25_b`, defaults 1.2/0.75) and sent to every shard with the
query so scoring is uniform.

**Persistence**: postings + doc store live in `<index path>.bm25` (custom
versioned binary format, atomic write), flushed with `Flush` and on
graceful shutdown, loaded at startup when present.

### Live interop with grpc-opennlp-analysis

Verified against the REAL native sidecar (not the test mock). The vendored
proto is byte-identical to upstream (drift-checked with `diff` before the
run). Setup:

```bash
# 1. the sidecar (native binary, no models needed)
PORT=59101 .../grpc-opennlp-analysis/build/native/nativeCompile/grpc-opennlp-analysis &

# 2. a turbovec-search node+coordinator pointed at it
turbovec-search --role=both --index=/tmp/tv-live/shard-0.tv \
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

TermStats (df per term — these are the REAL Porter stems in the postings):
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

The evidence proves real interop, not mock behavior: "dogs"/"barking"
land as stems `dog`/`bark`, "foxes" as `fox` — and doc 2's capitalized
"Running" does NOT group under `run` (df=1), because the real sidecar's
stemmers are case-sensitive on the token surface form exactly as its proto
documents; the mock would have lowercased it. Offsets slice the original
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
   calibration fit + BroadcastCalibration, AddDocuments with `DocLineage`
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

`examples/wiki_shakedown.rs` ingests a REAL corpus end-to-end: the
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
with `BroadcastCalibration`, starts the REAL native analysis sidecar
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

## Hybrid search: cascade (default), global-rank RRF, two-level RRF

`SearchService.HybridSearch{text, vector, k, analysis, legs}` offers
three modes (`HybridLegOptions.fusion_mode`); unspecified resolves to
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

### FUSION_MODE_TWO_LEVEL: fallback for incomparable scores

Each shard RRF-fuses its legs locally (`HybridShard`); the coordinator
RRF-merges the shard lists. Rank-based, needs NO comparable scores, but
NOT partition-independent (local ranks are compressed vs global ranks).
Use only when shards cannot share a calibration.

### Shared calibration and its caveats

`SearchService.BroadcastCalibration` pushes ONE TQ+ calibration to every
shard (fan-out of `SetCalibration`, per-node outcomes). Fit once,
broadcast BEFORE ingest, verify with `GetCalibration`. Calibration is
locked for an index's lifetime, so recalibration on drift is a
coordinated re-seed + re-ingest event.

Fork caveats (read from the turbovec source, pinned in
`tests/score_layout.rs`): with a shared calibration, encoding is a pure
function of (vector, calibration, dim, bit width), and scores are
bit-identical across indexes of the SAME shape. Across DIFFERENTLY-SIZED
indexes the kernel's accumulation order can shift a score by a couple of
ULPs — so raw vector scores across shards are comparable but only
bit-exact within same-shape kernel paths; ordering is robust except
within ULP-ties (the tests assert exact ids/ranks/BM25 bits and vector
scores within a few ULPs).

**Per-leg k** (GLOBAL_RANK/TWO_LEVEL): leg_k defaults to
`max(k, rrf_k)`; override in `HybridLegOptions`, clamped to >= k.
Cascade ignores leg_k/weights/rrf_k (its depth is k plus ties).
**Deferred alternative**: weighted-linear fusion with global
normalization stats — deliberately not built.

## Ingest flow (write path)

Shards ingest over gRPC; prebuilt `.tv` files are no longer required.
Deployment order for a from-scratch cluster is **fit → seed → ingest →
search**:

1. **Fit** a calibration on a representative sample (any tool that can run
   turbovec: build a throwaway index from the sample, read `calibration()`).
2. **Seed** every shard with it via `NodeService.SetCalibration` — or let
   the CLI do it: start one seeded node (demo or loaded index), then
   `turbovec-search calibrate --fit-from=node0:50051 --apply-to=node1:50051,node2:50051`.
   SetCalibration is accepted only while a shard is empty; calibration is
   locked for the index's lifetime (turbovec's own rule), so a retry of the
   same calibration is an idempotent no-op and anything else is rejected.
3. **Ingest** with `NodeService.AddVectors` (client-streaming, flat
   batches). Batches apply under the shard's write lock; searches hold the
   read lock for their whole scan, so no search observes a half-applied
   batch. Ids are server-assigned: the i-th vector of a shard is
   `slot_offset + i` (positional; turbovec's id-mapped index does not
   support the masked, floor-seeded scan this service uses).
4. **Search** as before — the lossless invariant holds for ingested data
   exactly as for prebuilt indexes (proven by `tests/multiprocess.rs`).

**Persistence**: `NodeService.Flush` writes the shard to its config
`index` path (atomic `.tv` write), and `save_on_shutdown = true` (the
default) flushes on SIGINT/SIGTERM. A shard whose index path does not
exist at startup starts empty; after ingest + flush (or graceful
shutdown), a restart with the same config comes back with all vectors
and the locked calibration (`.tv` persists it). Note that an EMPTY but
calibration-seeded shard also persists on shutdown — restarting a node
does not "unseed" it; wiping the shard file (or installing a snapshot)
is the only reset.

### Bulk load: InstallSnapshot

For pre-computed corpora, skip per-node ingest entirely: build the
shard image once (with the cluster's seeded calibration) and push the
FINISHED index to every shard owner over one client stream —
`NodeService.InstallSnapshot`, with `snapshot::install_snapshot(addr,
tv_path, bm25_path)` as the bundled client.

The node stages the image in a generation directory
(`<index path>.snap/`), validates it (well-formed index, sidecar opens,
calibration matches any calibration locked on the shard — a mismatch is
rejected, keeping scores comparable cluster-wide), then swaps it live
under the write lock. Both files travel inside ONE directory rename, so
the pair is installed atomically; a crash mid-swap is recovered
deterministically at startup (see `recover_generation`). Once a shard
serves from a generation, Flush and restart loading follow it — the
legacy layout and the generation never split-brain.

Rules of thumb: seed calibration first (or let an unseeded shard adopt
the image's), replace every shard together on recalibration, and expect
an image without a `.bm25` sidecar to wholesale-replace the postings
store. Covered by `tests/snapshot.rs` (7 cases, incl. restart survival
and crash recovery).

## Two-machine runbook

Topology: host A (this host) runs coordinator + shard 0; host B (`krick-1`)
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
   Verify with `NodeService.GetCalibration` if in doubt. Alternatively,
   skip files entirely: point each node's `index` at a fresh path, start
   empty, then seed + ingest over gRPC per "Ingest flow" above.)

2. **Copy the binary and shard 1 to krick-1:**

   ```bash
   scp target/release/turbovec-search krick-1:/usr/local/bin/
   scp /data/turbovec/shard-1.tv krick-1:/data/turbovec/
   ```

3. **Config on krick-1** (`/etc/turbovec/krick-1.toml`):

   ```toml
   role = "node"
   [[shards]]
   listen = "0.0.0.0:50051"
   index = "/data/turbovec/shard-1.tv"
   slot_offset = 50000          # = vectors in shard 0 (contiguous offsets)
   ```

   Start: `turbovec-search --config /etc/turbovec/krick-1.toml`

4. **Config on host A** (`/etc/turbovec/host-a.toml`):

   ```toml
   role = "both"
   coord_listen = "0.0.0.0:50050"
   nodes = ["host-a:50051", "krick-1:50051"]

   [[shards]]
   listen = "0.0.0.0:50051"
   index = "/data/turbovec/shard-0.tv"
   slot_offset = 0
   ```

   Start: `turbovec-search --config /etc/turbovec/host-a.toml`

5. **Verify.** From host A (or any host that can reach `host-a:50050`),
   issue a real search. The binary's built-in check does one:

   ```bash
   turbovec-search --role=coordinator --nodes=host-a:50051,krick-1:50051 \
       --coord-listen=127.0.0.1:59999 --demo-query --query-dim=128
   ```

   (spins a throwaway coordinator against the running nodes and prints the
   merged top-10). Or call `SearchService.Search` with any gRPC client
   against `host-a:50050` — proto at `proto/turbovec/search/v1/search.proto`.

6. **The large-k two-machine experiment** uses `cluster_sweep`, which
   drives a pre-existing cluster over the network (no in-process shards).
   Floor sharing is a node-side flag, so run TWO clusters side by side —
   same shard files, different ports — and point the binary at both:

   ```bash
   # per shard, on the machine owning it (setsid to survive ssh):
   setsid nohup turbovec-search --role=node --index=/tmp/wiki-shards/shard-N.tv \
       --slot-offset=OFFSET --node-listen=0.0.0.0:PORT \
       --floor-sharing=true  > node-sharing.log 2>&1 &
   setsid nohup turbovec-search --role=node --index=/tmp/wiki-shards/shard-N.tv \
       --slot-offset=OFFSET --node-listen=0.0.0.0:PORT2 \
       --floor-sharing=false > node-nosharing.log 2>&1 &

   # then, anywhere with corpus access for probe vectors:
   cluster_sweep \
     --nodes-sharing=host-a:50061,host-a:50062,krick-1:50063,krick-1:50064 \
     --nodes-nosharing=host-a:50071,host-a:50072,krick-1:50073,krick-1:50074 \
     --k=10,100,1000,10000 --queries=20
   ```

   It reports candidates + wall median/p90 per mode per k and asserts the
   sharing on/off correctness gate (identical hit signatures) per k.
   Since turbovec v5 (block-Hadamard rotation) the release binary is
   fully self-contained — no OpenBLAS/libgfortran to ship. (Pre-v5
   builds linked system OpenBLAS and needed `libopenblas.so.0` +
   `libgfortran.so.5` under `LD_LIBRARY_PATH` on bare hosts.)

   Executed 2026-07-27 on the wiki shards (4 x 61077 bge-m3 1024d docs;
   shards 0+1 on krick, 2+3 on krick-1): correctness gate green at every
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

- `proto/turbovec/search/v1/search.proto` — the wire API (heavily
  commented), codegen via `build.rs` + tonic-build.
- `src/chunked.rs` — the chunked scan (mask per chunk, floor seeding,
  running heap, publish/poll points). Pure and unit-tested, including
  k=1000.
- `src/merge.rs` — global top-k merge (total order: score desc, shard, id)
  and the coordinator's floor tracker.
- `src/postings.rs` / `src/bm25.rs` — the BM25 postings index, doc store,
  persistence, and scoring (with externally supplied global stats).
- `src/fusion.rs` — reciprocal rank fusion over scored legs, with per-leg
  provenance. Used at both fusion levels.
- `src/analyzer.rs` — the analysis-sidecar client (text in, term vectors
  out). No local analysis by design.
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
  the v3 `.bm25` format and immediately reopens the shard disk-resident.
- **Disk-resident** (`Bm25Reader`) — the v3 file is memory-mapped;
  postings slices and document texts are read from the map on demand.
  The OS page cache is the buffer pool (the Lucene model): after
  `Flush` or on startup with a v3 file, a shard holds NO postings or
  document texts in heap — only the per-doc length table (4 B/doc) and
  small lookup structures. Measured: opening a 164 MiB v3 file and
  serving queries grows RSS by ~11 MiB, versus ~159 MiB for the heap
  load (`tests/mmap_store.rs`).

v3 layout (single file, atomic write): header with absolute section
offsets, per-doc lengths, document texts, an on-disk text index
(fixed-stride entries, so text reads never walk the file), lineage,
sequential postings (sorted terms), and a fixed-stride term directory
(binary search per term to its postings offset + df). Pre-v3 files
still load — into the heap builder, upgraded to v3 on the next flush.
A disk-resident shard that receives more documents first reloads into
the heap builder (bulk-load discipline: build in memory, flush back).

This is what makes a corpus larger than machine memory work: the
postings (~130 GB at full CourtListener scale) and doc text (~40 GB)
live in page cache shared across all consumers, not per-process heap.
The turbovec vector index remains heap-resident today (v5 `load()`
reads packed codes into a Vec and the search repacks a blocked copy) —
at full-court scale that is ~3 GB packed + ~3 GB blocked across the
cluster, which fits; mmap support there is a fork-level decision
(see the limitations list).

## Limitations

- **Static membership.** The coordinator's node list is fixed at startup;
  no discovery, no re-sharding, no node failure handling beyond surfacing
  the error.
- **No replication.** Each shard lives on exactly one node.
- **Calibration distribution is manual-trigger.** `SetCalibration` (or the
  `calibrate` subcommand) pushes a fitted calibration; nothing fits or
  verifies automatically, and shards with mismatched calibrations produce
  incomparable scores without warning beyond `GetCalibration` inspection.
- **Positional ids only.** Ingested vectors are identified by
  `slot_offset + slot` in insertion order; client-chosen ids would need
  turbovec's `IdMapIndex`, which lacks the masked, floor-seeded scan.
  Deletes/updates are not supported (append-only).
- **Durability is flush-based.** Vectors are durable after `Flush` or a
  graceful shutdown; an ungraceful kill loses everything since the last
  flush (no WAL, no save interval).
- **Per-query streams.** Each query opens a fresh channel + `SearchShard`
  stream per node (no pooling).
- **Skipped-work metric is a proxy.** `candidates_collected` is countable
  through the public API; a true per-block prefilter-skip counter needs a
  small patch to the turbovec kernel.
- **Postings are append-only and unscored-deleted.** No document deletes
  or updates; a changed document must be re-ingested as a new id.
- **Vector index is heap-resident.** turbovec v5's `load()` reads
  packed codes into heap Vecs and materializes a blocked copy for
  search (~2x packed size). Serving truly oversized vector indexes
  needs a fork-level packed-bytes abstraction (owned Vec or mmap behind
  one accessor, plus a paged or mmap-backed blocked cache; ~200-400
  lines in the fork) — reported, not built here.
- **Hybrid exactness requires shared calibration.** GLOBAL_RANK fusion
  is exactly monolithic for k <= leg_k, but only while every shard
  shares one TQ+ calibration; recalibration is a coordinated
  re-seed + re-ingest event. TWO_LEVEL is the fallback when scores are
  not comparable.
- **No node-local BM25 shortcut.** Single-node deployments go through the
  same coordinator flow (a 1-node fan-out).
