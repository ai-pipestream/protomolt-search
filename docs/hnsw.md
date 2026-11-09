# HNSW graph provider (`hnsw`)

An approximate dense engine beside the default exhaustive embedded TurboVec
provider: a Hierarchical Navigable Small World graph (Malkov & Yashunin,
[arXiv:1603.09320](https://arxiv.org/abs/1603.09320)) over FP32 rows, scored
by inner product. It is written from scratch for this product; nothing is
linked, vendored or ported from another implementation. The attribution is
in `NOTICE`.

Code: `src/hnsw/` (`config.rs`, `distance.rs`, `graph.rs`, `image.rs`).
Tests: the module's unit tests and `tests/hnsw.rs` (real nodes behind a
coordinator). Harness engine: `hnsw` in `benchmarks/ann/` (the gate's
default `--candidate`).

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

## The beam

The query beam (`ef`) is deliberately not configuration: it is the request's
candidate depth, the number of candidates the planner asked the shard to
consider.

- `search(queries, k, options)` walks at `max(k, candidate_depth, 16)`:
  `options.candidate_depth` when the request names one, else `k`, never below
  16. Sixteen is the smallest breadth in the harness's sweep, so a product
  query never runs a beam narrower than one the gate measured; below it the
  walk degenerates toward a greedy descent.
- Streaming (`search_streaming_controlled`) has no `k`, so the depth must
  arrive in `VectorSearchOptions::candidate_depth`. The coordinator sends
  the request's `k` in `StartStreamSearch.candidate_depth`, the node copies
  it into the options, and a relay forwards it. Without a depth the
  provider refuses the stream by name rather than guessing.

Exhaustive providers ignore the depth.

## Contract with the planner

The descriptor advertises `CONFIGURED_ANN` without `exhaustive_completion`,
so nothing about the exact path changes:

- `EXACT` and the legacy unspecified mode refuse the provider by name.
- `ANN` runs it and reports `resolved_mode = ANN`, never exhaustive.
- `AUTO` admits it only through a measured dense execution policy bound to
  its `provider_backend` and `scoring_fingerprint`
  ([Dense execution policy](dense-execution-policy.md)); without one it
  refuses.
- Recursive Boolean evaluation needs every member's score and refuses a
  provider without exhaustive completion.

Reported scores are exact FP32 inner products of the stored rows, so shards
that share a config produce comparable scores. `completed` on a stream means
every walk finished, never that every row was evaluated.

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

**One distance per visited node.** Every scored row is scored once, exactly,
from its FP32 row, with the provider's own single-row kernels
(`src/hnsw/distance.rs`): AVX-512F and AVX2+FMA on x86-64 chosen at run time
with `is_x86_feature_detected!` (the crate still compiles for the
x86-64-v2 baseline), NEON on aarch64, a four-accumulator scalar loop
elsewhere. The walk prefetches the first line of each unvisited neighbor's
row while collecting them and the next row in full while scoring. Kernels
differ from one another in the last bits of a sum, so a fixed-order build is
reproducible per kernel (same machine, same binary), not across machines.

**Parallel build.** `build_mode = PARALLEL` inserts a batch of at least 256
rows from every thread of the rayon pool. Link cells are `AtomicU32`, written
under striped per-node locks (16,384 stripes; one node lock held at a time,
so striping cannot deadlock); reads during a build take the node's lock too.
A node that raises the top level holds the entry lock through its insertion
so no other insertion starts from an entry whose upper lists are not yet
written. `FIXED_ORDER` runs the same insertion on one thread in row order.

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
scores exactly at layer 0 (the entry point included; the rows the descent
scores are not) and that is at or above the floor is emitted, in batches of
up to 64 as the walk proceeds, best effort in discovery order. The control
callback is polled after every expansion. `Stop` from either callback ends
the stream uncompleted; a floor raised by either gates the rest of that
query and every later one, and a buffered row that fell below a floor raised
since it was scored is dropped before the sink sees it.

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

Bytes per row: `4 × dim` for the row, 4 for the inverse norm, 1 for the
level, 4 for the upper slot, `4 × (2M + 1)` for layer 0, plus `4 × (M + 1)`
per upper layer the node has (about `4 × (M + 1) / (M - 1)` per row on
average) and 4 per upper node for its base. At `dim = 64`, `M = 16` the
challenge fixture's image measures 402 bytes per row, 256 of them the FP32
row; at `M = 32`, 529.

A row's transcript (`row_transcripts`) is the bytes its score is computed
from: `HNSW`, the dimension, then the stored FP32 row little-endian. Nothing
else in the image reaches a reported score, so rewrite proofs compare rows
by content.

## What this does not do yet

- The FP32 rows are the provider's own copy, as FAISS `HNSWFlat` keeps
  one. Sharing them with the `.exact` sidecar is #71.
- Filter-aware traversal (#55), the shared floor pruning the walk, collapse
  and seeded entries (#54 follow-ups, #60) are later tickets.
- No measured dense execution policy ships for it; AUTO stays refused until
  one is measured on a real corpus.
