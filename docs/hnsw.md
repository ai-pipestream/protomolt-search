# HNSW graph provider (`hnsw`)

An approximate dense engine beside the default exhaustive embedded TurboVec
provider: a Hierarchical Navigable Small World graph (Malkov & Yashunin,
[arXiv:1603.09320](https://arxiv.org/abs/1603.09320)) over FP32 rows, scored
by inner product. It is written from scratch for this product; nothing is
linked, vendored or ported from another implementation. The attribution is
in `NOTICE`.

**This is a milestone, not a replacement.** The provider is complete as a
backend: it builds, serves from a memory map, streams, honors masks and
floors, lives through the segment lifecycle and reports exact scores. It is
not the gate of #54, and this document makes no product-level performance
claim for it: the harness in `benchmarks/ann/` measures it beside the
reference libraries on a fixture, and whether it serves a product query
path is decided by the measured dense execution policy of #58, not here.
Sharing its rows with the `.exact` sidecar is #71.

Code: `src/hnsw/` (`config.rs`, `distance.rs`, `graph.rs`, `image.rs`,
`rows.rs`). Tests: the module's unit tests, `tests/hnsw.rs` (real nodes
behind a coordinator: planner refusals, corruption, streaming control,
masks and floors, the candidate depth, multi-node fan-out) and
`tests/hnsw_lifecycle.rs` (ingest, seal, reopen, deletes, replacement,
snapshot, compaction). Harness engine: `hnsw` in `benchmarks/ann/`.

## Selecting it

The provider is chosen per shard or collection by name, exactly like the
default:

```toml
vector_backend = "hnsw"
```

`VectorIndex::create("hnsw", dim, bit_width)` takes the default parameters;
the rows are FP32 so `bit_width` plays no part (the descriptor reports 32).
Other parameters travel as the backend config through the existing gRPC
`ConfigureVectorBackend` RPC: `backend_kind = "hnsw"`, `config_format =
application/vnd.ai-pipestream.hnsw-config+protobuf;version=1`, and the
payload is the serialized `HnswConfig` message of
`proto/ai/protomolt/search/v1/search.proto`:

| Field | Default | Meaning |
|---|---|---|
| `m` | 16 | Neighbors per node in the upper layers; layer 0 keeps `2M`. 4 to 256. |
| `ef_construction` | 200 | Insertion beam: candidates considered per layer before the neighbor heuristic selects. At least `m`, at most 65,536. |
| `build_mode` | `PARALLEL` | `FIXED_ORDER` inserts rows in row order on one thread and reproduces the graph bit for bit; `PARALLEL` inserts from every thread under per-node locks. `UNSPECIFIED` is refused. |
| `seed` | fixed | Seeds the level draw of every node. |

`HnswConfig::backend_config()` builds it. Every field is part of the scoring
fingerprint (with the dimension, the backend version and the config format),
so a dense execution policy measured at one setting does not qualify
another. The fingerprint hashes the canonical re-encoding of the decoded
message, so field order or unknown fields on the wire do not change it.

A shard with a write-ahead log locks its provider configuration in the
log's manifest up to its first record, so configure the backend before the
first row (as the TurboVec shards do with their calibration); a compaction
replays from that manifest and refuses one without a usable configuration.

## What it may serve

The descriptor advertises `CONFIGURED_ANN` without `exhaustive_completion`,
so nothing about the exact path changes:

- `EXACT` and the legacy unspecified mode refuse the provider by name.
- `ANN` runs it and reports `resolved_mode = ANN`, never exhaustive.
- `AUTO` admits it only through a measured dense execution policy bound to
  its `provider_backend` and `scoring_fingerprint`
  ([Dense execution policy](dense-execution-policy.md)); without one it
  refuses. No such policy ships with this milestone.
- Recursive Boolean evaluation needs every member's score and refuses a
  provider without exhaustive completion.

`completed` on a stream means every walk finished, never that every row was
evaluated.

## The beam

The query beam (`ef`) is deliberately not configuration: it is the request's
candidate depth, the number of candidates the planner asked the shard to
consider.

- `search(queries, k, options)` walks at `max(k, candidate_depth, 16)`:
  `options.candidate_depth` when the request names one, else `k`, never below
  16. Sixteen is the smallest breadth in the harness's sweep, so a product
  query never runs a beam narrower than one the harness measured; below it
  the walk degenerates toward a greedy descent.
- Streaming (`search_streaming_controlled`) has no `k`, so the depth must
  arrive in `VectorSearchOptions::candidate_depth`. The coordinator sends
  the request's depth in `StartStreamSearch.candidate_depth`, the node
  copies it into the options, the segmented wrapper copies it into each
  part's options, and a relay forwards it. Without a depth the provider
  refuses the stream by name rather than guessing.
- The depth a dense request executed at is reported:
  `DenseExecutionOutcome.candidate_depth` is set whenever the resolved mode
  is ANN (the request's `k` or `selection_k`, or the policy point's
  `candidates` when AUTO qualified through one). `tests/hnsw.rs` drives two
  depths through a real node's streaming path and checks that they change
  the walk and that the reported depth is the executed one.

Exhaustive providers ignore the depth.

## Scores

Every score the provider reports is the product's exact score.

- **Kernels steer, the scalar scorer reports.** The SIMD kernels of
  `src/hnsw/distance.rs` score rows during the walk and decide which rows
  enter the beam. Every row that enters the reported result, and every row
  a stream emits, is rescored with `crate::exact_vectors::dot`, the scalar
  row-order accumulation, before it leaves the provider. Reported scores are
  therefore bit for bit the exact FP32 inner products of the stored rows on
  every kernel and every host, and comparable across shards that share a
  configuration. `reported_scores_are_bit_identical_to_the_scalar_scorer_and_ties_order_by_row`
  pins this for the unary and the streamed path.
- **Tie order.** Results order by score descending, then row ascending, on
  the unary path and within every streamed batch. Across batches a stream
  is best effort in discovery order (the walk emits as it proceeds).
- **Determinism.** Which rows a walk discovers depends on the graph. A
  `FIXED_ORDER` build is reproducible bit for bit per kernel (same machine,
  same binary): the kernels differ from one another in the last bits of a
  traversal score, so a different kernel may walk a different path. A
  `PARALLEL` build depends on thread scheduling and differs from run to
  run. Neither changes a reported score, only which rows are found.
- **Kernel bound.** Each kernel computes a floating-point sum of the exact
  products `qᵢxᵢ` in some association order, so against the scalar scorer a
  traversal score differs by at most `2·γ(d)·Σ|qᵢxᵢ|`, with
  `γ(d) = d·u / (1 − d·u)` and `u = 2⁻²⁴` (Higham, *Accuracy and Stability
  of Numerical Algorithms*, §3.1: each of the two sums is within
  `γ(d)·Σ|qᵢxᵢ|` of the exact value). The bound is checked against the
  scalar scorer on every available kernel at dimensions 1, 2, 3, 4, 7, 8,
  15, 16, 17, 31, 32, 33, 63, 64, 65, 127 and 1023 with negative scores,
  cancellation and near ties
  (`kernels_stay_within_the_bound_of_the_scalar_scorer_on_adversarial_rows`).

## The graph

Layered HNSW at uniform degree: `M` neighbors per node in the upper layers,
`2M` at layer 0. A node's level is a geometric draw with multiplier
`1/ln(M)` from a splitmix64 hash of `(seed, node id)`, so levels depend on
neither insertion order nor thread scheduling; both build modes share them.

Insertion follows the paper's Algorithm 1: a greedy descent from the entry
point to the node's level, then at each layer down to 0 a beam of
`ef_construction` (Algorithm 2), the neighbor heuristic (Algorithm 4, without
`extendCandidates` or `keepPrunedConnections`) to pick the node's neighbors,
reciprocal links, and the same heuristic to shrink a neighbor's list that
grew past its cap. A node whose level exceeds the current top becomes the
entry point once it is linked.

**Heuristic on directions.** The product's metric is the inner product and
rows are not required to share a length. The paper's test, "keep candidate
`c` if it is closer to the inserted node than to every neighbor already
kept", is applied to directions: `cos(c, base) > cos(c, s)`. Candidates are
still ranked by inner product with the base (the order the metric gives);
only the diversity test is angular. With unit rows this is the paper's test
on inner products; with rows of different lengths a long row cannot crowd
its neighbors out of a list by being long. The image keeps one `1/|row|`
per node (4 bytes) for it. The walk itself uses the inner product throughout.

**Flat memory.** Each kind of data is one contiguous array: FP32 rows; the
inverse norms; one level byte per node; layer 0 for every node at a fixed
stride of `2M + 1` cells (the count, then the neighbors); and for the nodes
that have upper layers, a compact index (`upper_slot[node]`, `NO_NODE` for
level-0 nodes) into one array of `M + 1`-cell blocks, a node's layers
`1..=level` consecutive. There is no `Vec<Vec<u32>>`. The visited set is an
epoch-stamped `u32` array reset in O(1); a walk uses one candidate heap and
one result heap, both over 8-byte `(score, node)` entries ordered totally
(score, then node), so heaps behave the same on every run.

**One traversal distance per visited node.** Every row the walk visits is
scored once from its FP32 row with the provider's own single-row kernels
(`src/hnsw/distance.rs`): AVX-512F and AVX2+FMA on x86-64 chosen at run time
with `is_x86_feature_detected!` (the crate still compiles for the
x86-64-v2 baseline), NEON on aarch64, a four-accumulator scalar loop
elsewhere. The walk prefetches the first line of each unvisited neighbor's
row while collecting them and the next row in full while scoring.

**Parallel build.** `build_mode = PARALLEL` inserts a batch of at least 256
rows from every thread of the rayon pool. Link cells are `AtomicU32`, written
under striped per-node locks (16,384 stripes; one node lock held at a time,
so striping cannot deadlock); reads during a build take the node's lock too.
A node publishes its own lists by merging them with the back-links other
insertions wrote to it meanwhile (shrinking with the heuristic when the
merge exceeds the cap), so no concurrent link is lost; the test
`a_full_beam_visits_every_reachable_row` walks a parallel graph at a beam
of the row count and requires every row to be reached. A node that raises
the top level holds the entry lock through its insertion so no other
insertion starts from an entry whose upper lists are not yet written.
`FIXED_ORDER` runs the same insertion on one thread in row order.

## Appends

Every row is linked when `add` returns. The first rows of an empty graph are
a bulk build in the configured mode; later rows link into the live graph
one by one in row order with the same heuristic (a parallel-mode batch of at
least 256 rows still inserts from every thread). There is no unindexed
tail, no rebuild at a threshold and no background build thread; `prepare()`
has nothing to fold. In fixed-order mode a graph is the same whether its
rows arrived in one batch or many, which is what the determinism test pins.

## Masks and floors

An allow mask routes through disallowed rows (they enter the candidate heap
and are expanded) but never returns them. `minimum_score` and a floor
raised during a stream filter what is returned or emitted; neither cuts the
walk short. Filter regimes and floor pruning are later tickets (#55, #60).

## Streaming

One walk per query at the request's depth. Every allowed row the walk
scores at layer 0 (the entry point included; the rows the descent scores
are not) and whose exact score is at or above the floor is emitted, in
batches of up to 64 as the walk proceeds, each batch ordered score
descending then row ascending. The control callback is polled after every
expansion. `Stop` from either callback ends the stream uncompleted; a floor
raised by either gates the rest of that query and every later one, and a
buffered row that fell below a floor raised since it was scored is dropped
before the sink sees it.

## Rows and their identity

The graph traverses and scores FP32 rows behind the `RowSource` trait
(`src/hnsw/rows.rs`): the dimension, the row count, the row-major values
and a row accessor. In this milestone the provider owns them: `OwnedRows`
on the heap, `MappedRows` over the image's row section when the index is
served from a memory map. The trait exists so that #71 can bind the graph
to the product's `.exact` sidecar, which holds the same rows, and the trait
carries the identity that binding must match, `RowSourceIdentity`: the
dimension, the row count and the SHA-256 over every row's little-endian
`f32` bytes in row order. That digest is the one `ExactVectorStore` keeps
over its payload, so an image's rows and a sidecar's rows compare by one
number; `row_source_identity_is_stable_and_matches_the_exact_sidecar` pins
that the heap, loaded and mapped identities agree and equal the sidecar's.

A row's transcript (`row_transcripts`) is the bytes its score is computed
from: `HNSW`, the dimension, then the stored FP32 row little-endian. Nothing
else in the image reaches a reported score, so rewrite proofs compare rows
by content.

## Image and serving

The image (`PMHNSW01`, format 1) is a 192-byte little-endian header with a
CRC32, then the sections (rows, inverse norms, levels, upper slots, upper
bases, layer 0, upper layers), each on a 64-byte boundary, then a SHA-256
trailer over everything before it. Loading parses and checks the header
first (magic, format, CRC, every count, and that the counts lay out to
exactly the file's length) before any section is allocated or touched, then
verifies the trailer, then walks the structure: levels against the cap, the
compact upper index, every list's count against its cap, every neighbor id
against the row count, its own id and its level. After that no index into
the graph can go out of bounds.

`load_mapped` maps the file and serves the graph and the rows straight from
the map: the section alignment makes the typed views aligned, nothing is
copied to the heap, and `is_mapped()` is true. Scores are bit for bit those
of `load`. A mapped index is sealed: `add` and `write` refuse.

## Storage and memory

**Image bytes per row:** `4 × dim` for the row, 4 for the inverse norm, 1
for the level, 4 for the upper slot, `4 × (2M + 1)` for layer 0, plus `4 ×
(M + 1)` per upper layer the node has (about `4 × (M + 1) / (M − 1)` per row
on average) and 4 per upper node for its base. At `dim = 64`, `M = 16` the
challenge fixture's image (100,000 rows) measures 402 bytes per row, 256 of
them the FP32 row; at `M = 32`, 529.

**Whole shard.** A shard stores its FP32 rows twice in this milestone: in
the image and in the `.exact` sidecar every provider shares (`4 × dim` per
row behind an 80-byte header, so 256 bytes per row at `dim = 64`). The
dense storage of a shard is then about 658 bytes per row at `M = 16` and
785 at `M = 32` for `dim = 64`, before the BM25 store, the live-document
overlay and the WAL. #71 removes the duplicate by binding the graph to the
sidecar's rows.

**Peak build memory.** Measured as the peak resident set of the harness
process building the challenge fixture (100,000 × 64, `ef_construction =
200`) minus the same process with an engine that builds nothing
(`enumerate`, which only holds the fixture): 44.3 MB at `M = 16` against a
40.2 MB image, 57.2 MB at `M = 32` against 52.9 MB; a single-thread build
peaks at 43.3 MB at `M = 16`. The flat arrays grow once per batch to their
final size (every array is reserved for the batch before it is filled), so
the peak is the image plus the per-thread scratch (one `u32` visited stamp
per row per thread, two heaps bounded by `ef_construction`, the lock
stripes) and allocator slack: 8 to 10 percent over the image at this shape.
Writing the image streams the arrays and allocates nothing of that size;
loading it mapped allocates nothing.

## Lifecycle on a node

A node runs the provider through every path the exhaustive providers take;
`tests/hnsw_lifecycle.rs` drives each through real nodes.

- **Sealed segments.** Staging a segment and opening a catalog admit a
  provider whose contract is the exhaustive quantized scan or a configured
  ANN traversal (`segments::admitted_quality_contract`), with the artifact,
  row-alignment and fingerprint checks unchanged. A flush seals the tail
  into an hnsw image; a reopen serves the images through the map
  (`VectorLoad::Mapped`) or from the heap, and both give the sealed catalog's
  answers bit for bit. An unsealed tail is volatile until a flush seals it,
  as on every provider; a shard with a write-ahead log rebuilds it from the
  log on open.
- **Deletes.** Deleted rows reach the provider as the allow mask and are
  excluded from unary results and from streams, in sealed segments and in
  the tail alike.
- **Replacement.** `CommitReplacements` retires the old row from every dense
  answer and serves the revision, in the tail and after the seal that
  follows.
- **Snapshot.** An hnsw image installs with its `.exact` sidecar and BM25
  store, answers at the slot offset, and survives a restart.
- **Compaction.** A compaction replays the live rows into new segments and
  builds their graphs again; the rebuilt graphs answer exactly scored live
  rows at the recall the graph gives, not the old graph's misses, so a
  by-text comparison across a compaction is a recall check, not a bit
  comparison. A publication interrupted between the write of its outputs
  and the publish of its manifest rolls back at open: the staged segment
  and the marker are removed and the old generation serves.

## What this does not do yet

- No performance claim. The harness numbers in `benchmarks/ann/` describe a
  fixture on one host; the product decides what the provider serves through
  the measured dense execution policy of #58.
- The FP32 rows are the provider's own copy, as FAISS `HNSWFlat` keeps one.
  Sharing them with the `.exact` sidecar through `RowSource` is #71.
- Filter-aware traversal (#55), the shared floor pruning the walk, collapse
  and seeded entries (#54 follow-ups, #60) are later tickets.
- No measured dense execution policy ships for it; AUTO stays refused until
  one is measured on a real corpus.
