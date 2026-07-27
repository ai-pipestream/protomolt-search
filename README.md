# turbovec-search

Distributed top-k search over [turbovec](https://github.com/ai-pipestream/turbovec)
(TurboQuant) shard indexes, with **collaborative mid-query floor sharing**:
shard nodes publish their current k-th best score while they scan, the
coordinator aggregates the maximum and pushes it back, and nodes prune the
remainder of their scan against it — losslessly.

Phase 1: one crate, one binary, three roles (`node`, `coordinator`, `both`),
tonic gRPC + tokio, static cluster membership.

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
cargo run --release --bin sweep -- \
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

## Hybrid search (RRF fusion)

`SearchService.HybridSearch{text, vector, k, analysis, legs}` runs BOTH
legs and fuses them with **reciprocal rank fusion**. No score
normalization anywhere: turbovec scores and BM25 scores have unrelated
scales, and fusing ranks sidesteps that entirely.

```text
fused(doc) = Σ_legs weight_leg / (rrf_k + rank_leg(doc))     rrf_k = 60
```

**Two-level fusion** (the shared positional doc-ID space is what makes it
work — both legs rank the same ids on a shard):

1. **Shard level** (`NodeService.HybridShard`): each node runs its local
   vector leg (chunked scan) and BM25 leg (global stats supplied by the
   coordinator) and RRF-fuses them into one ranked list.
2. **Coordinator level**: the coordinator analyzes the query text (same
   AnalysisSpec as ingest), builds global BM25 stats (TermStats fan-out),
   collects the per-shard fused lists, and RRF-merges them into the
   global top-k.

**Per-leg k semantics**: each leg is fetched `leg_k` deep, defaulting to
`max(k, rrf_k)` — the RRF constant should never exceed a leg's depth, or
deep-ranked docs could not contribute their `1/(rrf_k + rank)` share.
`leg_k` is overridable per request (`HybridLegOptions.leg_k`); values
below `k` are clamped to `k`.

**Weights**: `vector_weight` / `bm25_weight` (default 1.0 each) act at
shard-level fusion; the coordinator merge is unweighted. Proto `0` means
"unset → default"; negatives are rejected.

**Explainability**: every hit carries its per-leg 1-based ranks and raw
scores from the owning shard (absent when the doc is not in that leg), so
clients can see why it ranked.

**Approximation note (read this)**: two-level RRF is NOT
partition-independent — it is not, in general, identical to fusing the
union of all docs in one shot. Counterexample: docs A1,A2 on shard A and
B1 on shard B, vector order A1,B1,A2, BM25 order B1,A2,A1. Single-level
RRF ranks B1 above A1 (1/62+1/61 vs 1/61+1/63); two-level fusion ties
them at 1/61 and the shard tie-break puts A1 first. The ranks a shard
reports are LOCAL (compressed), while a monolithic index ranks globally.
`tests/hybrid.rs` asserts exact distributed == monolithic equality on a
partition-stable corpus (constructed so local and global orders
coincide), which proves the machinery lossless; it is not a theorem
about arbitrary data.

**Deferred alternative**: weighted-linear fusion with global
normalization stats (normalize each leg's scores with corpus-wide
min/max or z-stats, then weighted sum). More faithful score mixing, but
needs global score distribution plumbing — deliberately not built.

Notes: the hybrid vector leg uses local floor seeding only — the
cross-shard floor-sharing protocol lives on `SearchShard`'s bidi stream
and does not apply to the unary `HybridShard` path.

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
and the locked calibration (`.tv` persists it).

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

6. **The large-k two-machine experiment** (manual, not a CI gate): run the
   sweep in-process for baseline numbers (`--k=10,100,1000,10000`), then
   repeat against the 2-machine cluster by pointing a sweep-style client at
   `host-a:50050`. Watch `candidates_collected` and wall medians per k per
   mode.

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
- `src/bin/sweep.rs` — the k-sweep benchmark harness.
- `tests/` — lossless e2e (k=10 and k=1000), NodeService loopback with
  mid-scan injection, ingest/calibration rules, a multi-process
  ingest-and-restart acceptance test, BM25 tests with a mock analysis
  sidecar (postings, distributed-vs-monolithic equality, local-stats
  regression guard, STEMS identity, flush persistence), hybrid fusion
  tests (determinism, provenance, partition-stable exactness), and the
  skipped-work benchmark.

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
- **Hybrid fusion is approximate across shards.** Two-level RRF (shard
  fuses locally, coordinator fuses shard lists) is not
  partition-independent; see the hybrid section for a counterexample.
  Weighted-linear fusion with global normalization is the deferred
  alternative.
- **No node-local BM25 shortcut.** Single-node deployments go through the
  same coordinator flow (a 1-node fan-out).
