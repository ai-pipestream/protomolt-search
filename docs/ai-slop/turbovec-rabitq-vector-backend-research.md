# TurboVec, TurboQuant, and RaBitQ as interchangeable vector backends

> **AI-GENERATED RESEARCH DRAFT. NOT A SOURCE OF TRUTH.** This document was
> produced by an AI coding agent on 2026-08-28. It contains tentative design
> recommendations, source interpretations, and unverified inferences. A human
> must review the papers, code, measurements, and proposed contracts before any
> architecture, naming, compatibility, or production decision is made.

## Status and scope

This report asks how the vector edge of `pipestream-search` can become
replaceable while the product continues to own BM25, filters, facets, hybrid
fusion, analysis, document semantics, and its public query contract. It
compares the current TurboVec implementation with the current official RaBitQ
Library and distinguishes both systems from the algorithms described in the
TurboQuant and RaBitQ papers.

This is research, not an implementation plan approved for execution. No
benchmark in this report was run locally. Paper results are reported as claims
by their authors, not as reproduced facts. OpenReview allegations and responses
are also attributed claims unless the underlying fact is directly visible in
the saved paper, forum record, repository, or code.

Implementation update, 2026-08-28: the provider-boundary recommendation was
implemented after the source snapshot below. The product is now named
Pipestream Search, uses the `ai.pipestream.search.v1` protobuf namespace, and
routes runtime vector operations through a neutral provider contract. The
shipped `embedded-turbovec` adapter is the only production adapter today.
Sections describing direct `TurboQuantIndex` coupling remain as historical
evidence for the inspected revision, not the current worktree.

Implementation update, 2026-08-30: the first clustered TurboVec product
adapter now embeds `turbovec-grpc::CoordinatorService` in-process by default
and retains an external-coordinator tonic transport. Its initial exact scope is
vector Search, stable-label filters, and candidate-scoped dense rescoring;
parent collapse and hybrid fusion still require the proposed provider
candidate stream and refuse rather than falling back to the embedded path.

Local code inspected:

| Checkout | Revision inspected | Role |
|---|---|---|
| `pipestream-search` | `4bcaeb82beb1d1d21aff8fec8497dbd8a55c5107` | Full search product |
| `turbovec-grpc` | `fb28aa70ef09d37793b7b1829d5a4a6965df48e9` | Distributed TurboVec facade |
| `turbovec` | `65699eff623cefa0aeddbf7c67847372c106a3e2` | Local TurboVec engine, fork chain `turbovec-pipestream-s17` |
| RaBitQ Library | `94a9b277571eecbed7e1338dce23d76c1420d874` | Official C++ RaBitQ library |
| RaBitQ/TurboQuant comparison | `59994ecf2371a78dd5dc191afbdc4c91686803e7` | Comparison experiments and reported results |

Both Rust services currently pin the same TurboVec commit. The root workspace
guidance that names an older chain stage is stale relative to `Cargo.toml`,
`Cargo.lock`, `FORK.md`, and the checked-out source.

## Executive conclusion

The product should gain an interchangeable vector-provider seam, but
`turbovec-grpc` should remain a distributed TurboVec engine. Making that
service algorithm-generic would move the abstraction to the wrong layer and
would weaken its exactness and encoded-row movement contracts.

The practical direction is:

1. Keep all search-product meaning in `pipestream-search`.
2. Extract a backend-neutral provider contract around its current direct
   `TurboQuantIndex` use.
3. Implement embedded TurboVec and remote `turbovec-grpc` adapters first.
4. Add RaBitQ as an experimental, separately deployed C++ service adapter.
5. Benchmark the complete systems under the same corpus, hardware, quality,
   and operational constraints before choosing a production default.

Supporting two adapters experimentally is justified. Committing to two
production storage engines is not yet justified. The current public evidence
does not answer which engine is better for this workload:

- TurboQuant and RaBitQ papers compare quantizers and recall experiments, not
  the current TurboVec and RaBitQ Library as complete distributed systems.
- A 2026 RaBitQ-authored comparison reports better RaBitQ results and serious
  reproducibility concerns in TurboQuant's evaluation. It supplies code and is
  important evidence, but it is also an adversarial response from RaBitQ's
  authors and has not been reproduced here.
- The complete public OpenReview record confirms that a reviewer requested a
  direct RaBitQ design comparison before acceptance, the request was initially
  misread as an SAQ comparison, and the reviewer corrected it. The current
  accepted PDF still does not contain that design comparison. Later public
  comments document the dispute but do not independently resolve it.
- TurboVec already supplies mutation, persistence, masking, exact
  quantized-score merging, and a live-floor streaming API needed by this
  product. Those system properties are not measured by quantizer-only recall.
- The official RaBitQ Library supplies strong optimized C++ kernels and IVF,
  HNSW, and graph indexes, but its current IVF interface is a static in-memory
  index with a monolithic save file. It has no distributed completion
  certificate, external live threshold, portable shard manifest, or encoded
  split/join API.

The abstraction must therefore express different quality contracts. TurboVec
can certify an exhaustive search under its quantized scoring function. RaBitQ
IVF and graph search are approximate index traversals. A common API may expose
both, but it must never label their completion or result guarantees as the
same thing.

## Keep three comparisons separate

The names are easy to collapse into one comparison, which produces bad design
decisions.

| Layer | Turbo side | RaBitQ side | What can actually be compared |
|---|---|---|---|
| Quantization method | TurboQuant MSE or product estimator, plus current upstream TQ+ extensions | 1-bit or multi-bit RaBitQ estimators | Distortion, estimator error, code size, encode/decode cost |
| Local search engine | `turbovec::TurboQuantIndex` | RaBitQ Library IVF, HNSW, or QG | Recall, latency, build, mutation, memory, persistence, filtering |
| Distributed service | `turbovec-grpc` | Does not exist in the inspected official library | Network cost, shard scaling, failure meaning, topology, split/join |
| Search product | `pipestream-search` with BM25 and document semantics | No corresponding RaBitQ product | End-to-end relevance and operations |

A result about a quantizer does not establish that its available index is a
better storage engine. Likewise, TurboVec's production features do not prove
that TurboQuant gives the best recall at a fixed memory budget.

## What the current code says

### The product boundary is already correct

`pipestream-search` owns the user-visible search behavior:

- BM25 with global statistics and block-max pruning;
- dense, lexical, and hybrid query strategies;
- CEL and typed filters, facets, stored values, and score functions;
- analysis fingerprints and the OpenNLP sidecar boundary;
- document, chunk, parent, and stable source identity;
- WAL, snapshots, shard maps, and offline resharding;
- exact-or-refuse semantics for routes that claim partition invariance.

`turbovec-grpc` owns a narrower concern: a network and sharding facade around
the local TurboVec engine. It already has the right low-level boundary. It
should continue to expose TurboVec-specific calibration, streaming scan, and
encoded-row movement where those details are necessary for exactness.

### The inspected revision was concretely coupled to TurboVec

This is not currently a small dependency-injection change. The coupling spans
five surfaces:

| Surface | Current assumption |
|---|---|
| Node state | `ShardState` contains `Option<TurboQuantIndex>` |
| Query | Chunked scan, streaming scan, masked rescore, and direct score ordering call TurboVec APIs |
| Ingest | Dimension, bit width, shared calibration, `add`, and positional slot assignment are TurboVec-specific |
| Persistence | Configuration, snapshots, restore, and atomic install assume `.tv` bytes plus `.bm25` |
| Resharding | Raw-vector WAL replay builds `TurboQuantIndex::from_parts`; image resharding is described as `.tv` row filtering |

The product coordinator is also coupled to its own vector node proto. It
opens `SearchShard` or `StreamSearch` directly against every product node and
uses `VectorRescore` for candidate-scoped vector scoring.

The raw-vector WAL is the best migration asset. It can rebuild a different
backend without pretending that TurboVec and RaBitQ encoded rows are portable.
The WAL and shard manifest need backend-neutral metadata before this becomes a
supported operation.

### Current TurboVec collaboration semantics

The fork intentionally carries two distributed-search primitives:

- `SearchOptions::initial_threshold`, a seed score floor;
- `TurboQuantIndex::search_streaming`, which emits candidates above a live,
  monotonically rising floor and returns a completion result.

The current `pipestream-search` streaming route opens one bidirectional stream
per shard. The coordinator owns the only global heap, sends increasing score
floors, receives candidate batches, and accepts the query only when every
shard reports `completed=true`. Shared coordinate-exact calibration makes
scores and codes comparable across arbitrary row partitions.

This is exact relative to TurboVec's quantized score and documented total
order. It is not exact raw-float nearest-neighbor search. The live floor also
does not currently prove that an unvisited suffix of the shard can be skipped.
Completion still follows a complete logical scan unless the request is
cancelled. Its immediate benefits are candidate suppression, global-heap
ownership, and whatever row/block work the local kernel can safely avoid.

## What the RaBitQ Library actually implements

The inspected source is the official C++17 RaBitQ Library at commit
`94a9b277571eecbed7e1338dce23d76c1420d874`.

### Algorithms and metrics

The library offers RaBitQ quantization with IVF, HNSW, and a graph index. The
public metric choices are L2 and inner product. Cosine search is implemented
by normalizing data and query vectors and using inner product. IVF selects
`nprobe` centroids and scans all vectors in those selected clusters.

Unless every cluster is probed, IVF is approximate before quantization error
is considered. HNSW and graph search are also approximate traversal
strategies. A completed search means the configured traversal ended, not that
the corpus-wide optimum under either raw vectors or the quantized estimator
was certified.

### Physical layout

RaBitQ IVF uses a useful split layout:

- 32-vector `batch_data` blocks contain the 1-bit code and factors;
- `ex_data` contains the remaining bits;
- IDs are cluster-contiguous;
- cluster metadata identifies the ranges.

The 32-vector granularity happens to match TurboVec's block size, but the
formats are unrelated. Equal block cardinality is not format compatibility.

The library saves one binary image containing metadata, cluster sizes,
rotator, initializer/centroids, batch data, extended-bit data, and IDs. `load`
allocates memory and reads the complete arrays. Its documentation explicitly
says querying currently requires the index to be loaded in memory.

No inspected API provides:

- mmap or block-store-backed loading;
- a backend/version/fingerprint shard manifest;
- row-block or cluster export/import;
- online append or remove for IVF;
- atomic snapshot installation;
- split/join across index images;
- an external score/distance threshold;
- candidate streaming or a distributed completion certificate.

Those are implementation gaps, not claims that RaBitQ cannot support them.

### The bound and incremental scan

For each 32-vector batch, the IVF scan computes a 1-bit estimated distance and
a lower distance bound. With multi-bit codes it reads and evaluates a vector's
extended bits only if that lower bound is better than the current local
worst-top-k distance. It then updates the local heap threshold.

This is valuable memory-traffic pruning. It is not current block-level early
termination:

- every batch of every selected IVF cluster is visited;
- the decision is per vector, not a proof covering all unseen batches;
- the threshold is local to the `search()` call;
- there is no callback or control stream through which a coordinator can
  supply a better global threshold.

RaBitQ's theoretical interval is a high-probability error bound with a tunable
confidence parameter. It is not a deterministic upper bound on an unvisited
block. If it is used to prune distributed search, the API must expose and
budget the query-wide failure probability. Descriptions such as “nearly
perfect confidence” are not sufficient as a production contract because
failure composes across vectors, batches, shards, and repeated queries.

## Direct answers to the design questions

### Does RaBitQ support block-storage sharding today?

**It has a shardable physical layout, but no supported distributed storage
contract.**

The packed 32-vector batches and cluster-contiguous arrays could be partitioned.
Two plausible layouts are:

1. **Whole-cluster sharding.** Each IVF cluster belongs to one shard. This
   minimizes duplicated code data, but cluster imbalance can be severe. A
   coordinator must select global probes once and route each chosen cluster to
   its owner.
2. **Batch ranges within clusters.** Large clusters are split across shards.
   This balances better, but every owner needs compatible centroid, rotation,
   metric, estimator, and code-layout metadata.

Independently training an IVF index per shard is not equivalent to partitioning
one monolithic IVF. Each shard can choose different centroids and cluster
assignments, so local `nprobe` results do not reconstruct the monolithic
candidate set. A distributed RaBitQ backend needs a shared immutable model
artifact and fingerprint covering at least:

- dimension and padded dimension;
- metric and normalization rule;
- bit width and estimator variant;
- random rotation and its implementation/version;
- IVF centroids and assignment rules;
- code layout and kernel compatibility;
- stable ID and generation semantics.

The current library's save image contains much of this state, but it is not
factored as a reusable model plus independently movable shard data.

### Can RaBitQ use collaborative early termination?

**It can plausibly use a collaborative threshold, but the current code does
not implement the protocol and the first useful optimization is refinement
suppression, not scan termination.**

For L2, the coordinator's current global worst accepted result is a decreasing
distance ceiling. Sending that ceiling to each RaBitQ shard could let the
existing per-vector lower-bound test skip more `ex_data` reads. For inner
product, the adapter can convert the engine's smaller-is-better distance to a
common higher-is-better rank key before crossing the product boundary.

That change still visits every 1-bit batch in every selected cluster. Stopping
before all selected clusters or batches are visited would require additional
cluster/block bounds, an ordering that makes those bounds useful, and explicit
probabilistic or deterministic completion semantics. The per-vector RaBitQ
error interval alone is not a certificate for unseen data.

Recommended experimental sequence:

1. Baseline local top-k per shard, one request and one reply, no floor traffic.
2. Add a conflated global distance ceiling solely for extended-bit suppression.
3. Measure extended-byte reads and wall-clock savings against control-message
   overhead.
4. Only then investigate cluster or block bounds for true early stop.

### Should the product support both or choose one?

**Build the seam and two experimental providers, then choose operational
support from measurements.**

The seam is needed even without RaBitQ because embedded TurboVec and remote
`turbovec-grpc` should already be interchangeable at the product layer. A
RaBitQ adapter is a good test of whether the seam accidentally exposes
TurboVec details.

Do not promise two production engines yet. Production support should require
passing a decision gate covering relevance, latency, memory, persistence,
recovery, filters, ingest, resharding, and operating cost. A quantizer recall
win alone is insufficient.

## Proposed architecture

```text
Public query and ingest API
           |
           v
pipestream-search product layer
  BM25 | CEL | facets | documents | hybrid fusion | lineage
           |
           v
VectorProvider contract, one backend per vector-space generation
       /                 |                    \
embedded TurboVec   remote TurboVec      experimental RaBitQ
      adapter        cluster adapter       service adapter
       |                 |                    |
TurboQuantIndex     turbovec-grpc       C++ RaBitQ Library
```

The product should not mix different backend algorithms inside one logical
vector-space generation. Their estimated scores, candidate sets, and quality
contracts differ. Backend interchangeability means the public product API and
document identity remain stable across a deliberate rebuild/cutover, not that
TurboVec and RaBitQ shards can share one global heap.

### Separate product and provider responsibilities

| Product owns | Provider owns |
|---|---|
| Vector-space name and source-document identity | Code format and algorithm version |
| Query vector and metric requested by the mapping | Query transformation and kernel execution |
| Filters compiled into generic allowlists/masks | Efficient application of supported masks |
| Candidate depth required by hybrid strategy | Vector candidate discovery and native scoring |
| Stable tie order at the public response | Backend raw score and stable local ID |
| Hybrid exactness/refusal rules | Quality/completion certificate |
| Generation cutover and source rebuild policy | Snapshot/image validation for that backend |

`pipestream-search` may compile CEL to a generic allowed-ID set or mask. The
provider must not understand CEL, protobuf document schemas, BM25, facets, or
parents.

### Descriptor and capability contract

Every opened vector generation should return a descriptor like:

```text
VectorBackendDescriptor
  backend_kind              TURBOVEC | RABITQ | ...
  backend_version
  index_generation
  dimension
  metric                    INNER_PRODUCT | COSINE | L2
  score_direction           HIGHER_IS_BETTER | LOWER_IS_BETTER
  scoring_fingerprint       opaque bytes
  quality_contract          EXHAUSTIVE_QUANTIZED | CONFIGURED_ANN |
                            PROBABILISTIC_BOUND
  failure_probability       optional, required for probabilistic pruning
  capabilities[]
```

`scoring_fingerprint` is the comparability gate. It is backend-defined and
must cover every parameter that changes encoded bytes or native scores.
TurboVec can include calibration, bit width, format, and engine revision.
RaBitQ can include rotation, centroids, estimator, bit width, metric, layout,
and engine revision. The generic layer should not expose TQ+ arrays or RaBitQ
centroid structures.

Useful capabilities include:

- `BATCH_QUERY`;
- `CANDIDATE_STREAM`;
- `LIVE_BOUND_INPUT`;
- `ALLOWLIST` or `DENSE_MASK`;
- `CANDIDATE_RESCORE`;
- `APPEND`, `REMOVE`, and `FLUSH` separately;
- `SNAPSHOT_INSTALL`;
- `OPAQUE_PARTITION_EXPORT_IMPORT`;
- `RAW_VECTOR_REBUILD`;
- `EXHAUSTIVE_COMPLETION`;
- `PROBABILISTIC_PRUNING`.

Capabilities are admission rules, not hints. For example, a decomposed hybrid
route that requires corpus-wide vector score bounds must refuse a
`CONFIGURED_ANN` provider unless the route has a separately proven candidate
contract.

### Query contract

Normalize score direction at the provider boundary. The product's `rank_key`
is always higher-is-better:

- inner product or cosine may use the provider score directly;
- L2 can use negative distance;
- the response may retain `native_score` and `native_score_kind` for audit.

A minimal request contains:

```text
VectorQuery
  generation
  request_id
  vector
  k or candidate_depth
  minimum_rank_key          optional
  allowed_ids               optional, bounded representation
  required_quality_contract
  deadline
```

A terminal certificate contains:

```text
VectorCompletion
  generation
  scoring_fingerprint
  completed
  quality_contract
  configured_search_parameters
  failure_probability       if applicable
  visited_units and total_units where meaningful
  cancellation_reason
```

For `turbovec-grpc`, the remote adapter should call the cluster coordinator as
one provider. `pipestream-search` should not learn the cluster's node topology or
reimplement its exact heap. This prevents two coordinators from competing for
ownership of floors, replicas, and completion.

### Low-chattiness collaborative stream

Use two query modes:

1. **Unary/bounded top-k.** Default for providers where collaboration does not
   measurably help. One request per provider and one bounded response.
2. **Bidirectional candidate stream.** Optional capability for providers that
   can consume a live bound or emit useful incremental candidates.

The stream should carry batched data and conflated control state:

```text
client -> provider: Start
provider -> client: CandidateBatch*
client -> provider: latest BoundUpdate only
provider -> client: Completion
```

Operational rules:

- Never send one message per hit.
- Bound updates are monotonic in normalized `rank_key`.
- Keep only the latest pending bound per stream. Do not queue stale floors.
- Send after a material delta or a short time gate, not every heap mutation.
- Separate data and control accounting in metrics.
- Batch candidates to a byte target and latency cap.
- A deadline or cancellation produces `completed=false`.
- A provider without `LIVE_BOUND_INPUT` receives no bound traffic.

This is nearly the current TurboVec streaming shape and can be adopted without
making it the lowest common denominator.

### Storage, split, and join contract

There should be two distinct movement paths.

#### Portable rebuild path

The common, cross-backend path replays canonical raw vectors plus stable
identity into a new generation. This is the only valid path when changing
backend kind, incompatible fingerprints, metric, model, or encoding.

The existing raw-vector WAL can support this after its manifest stops assuming
TurboVec calibration. Bulk-built generations still need retained source
artifacts because the current post-snapshot WAL intentionally does not contain
the base image.

#### Opaque same-backend path

An optional provider may export/import opaque partitions:

```text
PartitionManifest
  backend_kind and backend_version
  scoring_fingerprint
  source_generation
  partition_key and covered ID/routing range
  row_count
  content checksum
  opaque segments[]
```

Import must require an identical backend kind and scoring fingerprint. The
product orchestrates topology generations and stable identity, while the
provider decides whether its blocks are safely movable.

TurboVec can eventually use encoded-row movement under its shared
calibration. RaBitQ needs new model/data separation and cluster or batch export
before it can claim this capability. A generic interface must never imply
that `.tv` rows and RaBitQ batches can be converted without raw-vector replay.

### Hybrid-search implications

The current product's strongest invariant is that distributed results equal a
documented monolithic computation. That must become a named request and
response contract, not be silently abandoned for ANN.

Suggested quality vocabulary:

| Contract | Meaning |
|---|---|
| `EXHAUSTIVE_QUANTIZED` | All eligible rows evaluated under one backend-native quantized score and total order |
| `CONFIGURED_ANN` | The configured IVF/HNSW/graph traversal completed; no corpus-wide optimum claim |
| `PROBABILISTIC_BOUND(p)` | Pruning used a bound with declared query-wide failure probability `p` |
| `RAW_EXACT` | Optional reference path over original float vectors |

Consequences:

- RRF can combine approximate legs, but is exact only over the candidate lists
  actually produced.
- Cascade is naturally compatible with ANN when its vector gate is explicitly
  approximate and candidate depth is reported.
- A decomposed weighted sum cannot claim global exactness from a truncated ANN
  candidate set without a missing-candidate upper-bound proof.
- Search-after cursors must bind the backend generation, fingerprint, quality
  contract, and ANN parameters because changing them can change rank order.
- Tests must distinguish exact hit equality from quality thresholds such as
  recall.

## RaBitQ service shape

The first RaBitQ integration should be a separate C++ process using gRPC, not
Rust/C++ FFI inside the product node.

Reasons:

- isolates allocator, SIMD, compiler, and crash behavior;
- keeps Rust builds and licensing/provenance review straightforward;
- allows RaBitQ's native threading and memory ownership;
- makes the same provider contract usable on another host;
- prevents the experiment from contaminating product storage before it earns
  operational support.

The first increment should be read-only build/load/search with a unary top-k
RPC. It should use one shared rotation/IVF model for all shards and make its
approximate parameters explicit. Live threshold input, snapshots, and
partition movement come later only if measurements justify them.

## Benchmark before selection

### Required contestants

- exact raw-float brute force as relevance ground truth;
- embedded TurboVec at each viable bit width;
- `turbovec-grpc` with one node and multiple shard counts;
- RaBitQ full scan or full-probe IVF if a suitable path is implemented;
- RaBitQ IVF across an `nprobe` sweep;
- optionally RaBitQ HNSW/QG if they match the operational target.

TurboVec embedded and one-node `turbovec-grpc` must first pass a conformance
test. That proves the provider seam before RaBitQ adds different result
semantics.

### Controls

Hold constant:

- exact base vectors, queries, train/calibration split, and stable IDs;
- metric, normalization, dimensionality, and ground truth;
- memory or bits-per-vector budget, including scalar factors and codebooks;
- CPU model, SIMD features, frequency policy, NUMA placement, threads, and
  host pressure;
- query concurrency and warm/cold state;
- shard routing, replication, deadlines, and candidate depth;
- compiler versions and relevant optimization flags.

Report algorithm randomness across multiple fixed seeds. The RaBitQ-authored
comparison uses ten runs for its recall curves, which is better than selecting
one favorable rotation, but that experiment still scans a 100,000-vector
quantized array and is not an end-to-end storage-engine comparison.

### Measurements

Quality:

- Recall@1, Recall@10, and Recall@k;
- NDCG and workload-specific judgment metrics;
- hybrid result changes by fusion mode;
- score/rank stability across shard counts;
- probabilistic-bound violations against raw reranking.

Performance and operations:

- p50, p95, and p99 latency plus throughput;
- build/calibration time and peak memory;
- resident memory and complete bytes per vector;
- snapshot size, load time, restart time, and mmap/page-fault behavior;
- append/flush performance where supported;
- shard balance and scaling efficiency;
- candidates and bytes sent per query;
- bound-update count and bytes;
- 1-bit batch reads, extended-bit reads, and skipped refinements;
- recovery, incomplete-shard, corrupt-image, and version-mismatch behavior.

### Decision gate

A production backend must:

1. Meet an agreed relevance floor on the real corpus.
2. Win or acceptably trade latency, memory, and cost on target hardware.
3. Preserve fail-loud generation and fingerprint checks.
4. Have a credible snapshot, restore, and rebuild path.
5. Support required filters and candidate rescoring without product semantics
   entering the vector engine.
6. Pass multi-shard conformance for the quality contract it advertises.
7. Be operable by the same deployment and observability standards as the
   existing service.

Until then, TurboVec remains the production backend because it is the only one
of the two already integrated with those requirements, not because this report
has established superior quantization quality.

## Implementation sequence if approved

1. Introduce backend-neutral descriptor, quality, score-direction, and
   capability types.
2. Extract a node-side `VectorIndex` trait around current TurboVec calls with
   no behavior change.
3. Split snapshot/WAL metadata into generic product metadata plus
   backend-specific opaque metadata.
4. Add conformance tests against the existing embedded TurboVec behavior.
5. Add a remote provider whose single endpoint is the `turbovec-grpc`
   coordinator.
6. Run embedded-versus-remote TurboVec correctness and performance gates.
7. Build the read-only RaBitQ sidecar and shared-model sharding prototype.
8. Benchmark unary local-top-k first.
9. Add a live distance ceiling for extended-bit suppression and measure it.
10. Decide whether RaBitQ earns persistence, mutation, and opaque partition
    movement work.

No initial step should rename repositories, change the public API, launch a
corpus rebuild, or alter the serving cluster.

## Naming decision

The repository name overstated one attached vector implementation and
understated the full-search product. The implementation adopted **Pipestream
Search** for the crate, binary, documentation, configuration prefix, and
`ai.pipestream.search.v1` protocol namespace. The remote repository and local
checkout retain their old names until an explicit hosting-level rename.

The internal architecture uses neutral terms such as “search product,”
“vector provider,” and “vector generation.” `basic-search` remains only a
discarded research-era naming idea.

## Evidence assessment

| Finding | Evidence class | Confidence |
|---|---|---|
| The inspected pre-refactor revision directly embedded TurboVec across query, ingest, persistence, and resharding | Pinned local source snapshot | High |
| `turbovec-grpc` is correctly bounded as a distributed TurboVec engine | Current local source and docs | High |
| RaBitQ IVF uses 32-vector coarse batches and conditionally reads extended bits per vector | Current official source | High |
| RaBitQ IVF scans all selected clusters and has no external live threshold | Current official source and docs | High |
| RaBitQ's present save format lacks supported split/join and mmap loading | Current official source and docs | High for this revision |
| A coordinator ceiling could suppress more RaBitQ extended-bit reads | Design inference from current scan | Medium, requires implementation and benchmark |
| Cluster/batch sharding is feasible with a shared immutable RaBitQ model | Design inference | Medium, requires format work |
| RaBitQ outperforms TurboQuant in the 2026 reproduced recall experiment | Claim and artifacts from RaBitQ authors | Medium until independently reproduced |
| The current accepted PDF says TurboQuant leads almost all of its reported recall settings, with some RaBitQ wins on 2-bit GloVe | Current accepted paper claim | Low to medium as comparative evidence due the disputed and incompletely disclosed baseline |
| A reviewer requested direct TurboQuant/RaBitQ design experiments before acceptance, then said the camera-ready did not provide them | Complete public OpenReview record, notes `sNp6ee9fzN`, `jQ7NTMk5mP`, and `njaXBjsk6K` | High as a fact about the review record |
| The current accepted PDF still lacks an end-to-end TurboVec versus RaBitQ Library system comparison | Current accepted paper | High |
| Either quantizer will improve this product end to end | No direct evidence | Unknown |

## OpenReview record and current accepted PDF

The TurboQuant arXiv v1 paper reports higher recall than its RaBitQ baseline
and describes the baseline as lacking a fully vectorized implementation. The
2026 RaBitQ-authored technical note disputes the implementation, hardware, bit
accounting, recall, and runtime comparisons. Its reproduction repository
reports that RaBitQ wins its symmetric quantized-array recall experiment.

This report does not adjudicate that dispute. The response paper is authored
by the RaBitQ team, and some of its account relies on correspondence that is
not independently inspectable from code. Conversely, TurboQuant's paper does
not disclose enough about its RaBitQ baseline to reconstruct the reported
comparison from the paper alone.

The source bundle now contains the 22-page accepted OpenReview PDF and 28
unique public forum notes. The user downloaded them through a browser after
OpenReview returned HTTP 403 to this agent. The JSON contains four reviews,
author rebuttals, reviewer follow-ups, the area-chair recommendation, the
poster decision, and later public comments through 2026-04-22. It does not
contain alleged private email records, private chair correspondence, or note
revision histories.

### Review-phase record

The following are facts visible in the saved forum JSON:

1. Reviewer `WFrV` gave the submission a high score but explicitly requested
   a more detailed discussion and experiment comparing the design choices of
   TurboQuant and RaBitQ. The review identified random projection as shared
   structure and distinguished their codebooks and unbiased-estimation
   approaches. See note `sNp6ee9fzN`.
2. The author response treated the request as a comparison with SAQ, said the
   new method's code was unavailable, and proposed combining PCA or dimension
   segmentation with TurboQuant. It did not answer the requested RaBitQ
   comparison. See note `6PvsTE745U`.
3. Reviewer `WFrV` corrected the misunderstanding and again requested
   experiments on the RaBitQ versus TurboQuant design choices, noting that the
   RaBitQ code was available. See note `jQ7NTMk5mP`.
4. Another reviewer found the nearest-neighbor experiments least convincing,
   requested stronger ANN baselines, and questioned emphasizing one-time
   quantization instead of CPU search latency, accuracy, and memory. See note
   `9YkPjGeRbr`.
5. The rebuttal promised stronger ANN comparisons with NestQuant, LSQ++, and
   OPQ. The reviewer then recommended acceptance conditional on integrating
   the added results. See notes `1fM99Iv3Ck` and `EWTszxUm36`.
6. The area chair concluded that theoretical and stronger-baseline concerns
   had been addressed and recommended acceptance. The program chairs recorded
   `Accept (Poster)` on 2026-01-26. See notes `GH4IObSKJ0` and `Hhk4ilfcsV`.

Acceptance establishes the venue decision. It does not by itself validate the
RaBitQ baseline or resolve the later dispute. The RaBitQ-focused reviewer did
not make that comparison a rejection condition, and the area-chair summary
does not analyze it separately.

### Post-decision public record

The public discussion after acceptance contains competing claims:

- On 2026-03-27, RaBitQ authors alleged that the paper omitted the shared
  random-rotation structure, misstated their theoretical guarantee, and used
  an undisclosed single-core Python/CPU RaBitQ baseline against TurboQuant on
  an A100. They also described earlier private correspondence. See note
  `Arxq4fFVG1`. The private records are not in the source bundle.
- Reviewer `WFrV` then confirmed that the pre-decision review had requested a
  thorough TurboQuant/RaBitQ comparison and expressed surprise that the
  camera-ready only mentioned RaBitQ once in the main paper. See note
  `njaXBjsk6K`.
- TurboQuant author Majid Daliri responded that random rotation is standard,
  acknowledged after further inspection that RaBitQ supports the optimal
  bound, committed to update the manuscript, and argued that the runtime
  comparison was not material to the paper's primary compression-quality
  contribution. See note `X882cbyNNM`.
- RaBitQ authors disputed that response, repeated the baseline and timeline
  allegations, and said they had submitted email records to the ICLR chairs.
  See notes `G2xt2ALQNl` and `YH9ADYBz19`. The saved record contains no chair
  adjudication of those allegations.
- DRIVE/EDEN authors separately alleged unacknowledged overlap with their
  earlier rotation plus Lloyd-Max work and linked a technical note. See notes
  `EQrxyH5PXf` and `Ex8cseftAF`.
- On 2026-04-22, the RaBitQ team linked its technical report and reproduction
  repository and reported that its symmetric experiments contradicted the
  TurboQuant paper's comparative results. See note `TxgMnOFFFF`.

The saved 28-note export has no later TurboQuant response to the reproduction
report and no program-chair resolution of the public dispute. Absence from
this export does not prove that no private process occurred.

### What the 2026-05-13 PDF now says

The downloaded OpenReview PDF was produced on 2026-05-13, after the public
comments above. It makes some visible changes relative to arXiv v1:

- it cites DRIVE and EDEN in the related-work discussion;
- it describes RaBitQ as asymptotically optimal up to hidden constants rather
  than simply calling it suboptimal;
- it concedes that RaBitQ has slightly higher recall in a few 2-bit GloVe
  settings while claiming TurboQuant leads the remaining reported settings.

Important gaps remain directly visible in that PDF:

- it does not provide the requested structural/design ablation between RaBitQ
  and TurboQuant;
- the nearest-neighbor section still says RaBitQ lacks a vectorized
  implementation and GPU support, despite the official RaBitQ Library's SIMD
  implementation inspected for this report;
- it says all experiments use one A100, then describes RaBitQ as running on
  CPU, without stating the RaBitQ implementation, CPU model, thread count, or
  parallelism configuration;
- it asserts extra or hidden RaBitQ bit usage without giving complete byte
  accounting;
- the promised NestQuant, LSQ++, and OPQ ANN comparisons are not present;
- it compares quantized-array recall, not TurboVec and RaBitQ as storage,
  mutation, persistence, filtering, or distributed-search systems.

These observations strengthen the case for an independent benchmark. They do
not establish research misconduct, prove the private correspondence claims,
or establish RaBitQ as the better production backend for `pipestream-search`.
The architecture recommendation therefore remains unchanged: preserve a
neutral provider seam, reproduce both sides on the same system, and make the
production decision from measured workload evidence.

## Sources

Downloaded papers, OpenReview exports, checksums, and retrieval limitations are
in [`source-reference/`](source-reference/README.md). Repository sources are
normal Git checkouts under `/work/main/reference-code` at the revisions
recorded there.

Primary and official links:

- [Locally archived accepted TurboQuant PDF](source-reference/papers/turboquant-iclr2026-openreview-86df3c70.pdf)
- [Locally archived OpenReview forum notes](source-reference/openreview/turboquant-forum-notes.json)
- [TurboQuant arXiv:2504.19874](https://arxiv.org/abs/2504.19874)
- [TurboQuant OpenReview forum](https://openreview.net/forum?id=tO3ASKZlok)
- [RaBitQ 1-bit paper, arXiv:2405.12497](https://arxiv.org/abs/2405.12497)
- [Multi-bit RaBitQ, arXiv:2409.09913](https://arxiv.org/abs/2409.09913)
- [Official RaBitQ Library](https://github.com/VectorDB-NTU/RaBitQ-Library)
- [RaBitQ/TurboQuant comparison note, arXiv:2604.19528](https://arxiv.org/abs/2604.19528)
- [Comparison experiment repository](https://github.com/VectorDB-NTU/rabitq-turboquant-comparison)

Contextual sources that are useful but not neutral arbitration:

- [EDEN/TurboQuant technical note, arXiv:2604.18555](https://arxiv.org/abs/2604.18555)
- [TurboVec case study, arXiv:2607.16973](https://arxiv.org/abs/2607.16973)

## Questions requiring human review

1. Is approximate dense retrieval acceptable for every public query shape, or
   must the current exact-quantized contract remain the default?
2. Which real corpus, embedding space, and relevance judgments define the
   production decision?
3. Does the deployment target favor an in-process Rust engine, a remote
   cluster, or a C++ service strongly enough to dominate small recall
   differences?
4. Is raw-vector source retention sufficient for backend cutovers, or must the
   product support fast image-level resharding?
5. What numerical query-wide failure probability is acceptable if RaBitQ
   bounds participate in pruning?
6. Should the first RaBitQ prototype use full-probe IVF for a cleaner
   quantizer comparison, then sweep `nprobe`, or start at the intended ANN
   operating point?
7. Can the complete OpenReview thread be archived and checked for relevant
   reviewer concerns and author responses?
