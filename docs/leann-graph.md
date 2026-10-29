# LEANN-style graph provider (`leann-graph`)

An optional, approximate dense engine style beside the default exhaustive
embedded TurboVec provider. It is a native Rust port of the two techniques
of LEANN (Wang et al., "LEANN: A Low-Storage Vector Index",
[arXiv:2506.08276](https://arxiv.org/abs/2506.08276)); nothing from LEANN is
linked or vendored: no Python package, no FAISS or DiskANN fork. Both
algorithms follow the paper rather than the reference code, because the
reference implementation ships its degree pruning disabled and runs the
approximate-distance-guided search only in its DiskANN backend. The
attribution is in `NOTICE`; LEANN is MIT licensed.

Code: `src/leann_graph.rs`. Tests: the module's unit tests and
`tests/leann_graph.rs` (real nodes behind a coordinator).

## Selecting it

The provider is chosen per shard or collection by name, exactly like the
default:

```toml
vector_backend = "leann-graph"
```

`VectorIndex::create("leann-graph", dim, bit_width)` takes the default
parameters at `bit_width` (4 or 8). Any other parameters travel as the
backend config through the existing gRPC `ConfigureVectorBackend` RPC:
`backend_kind = "leann-graph"`, `config_format =
application/vnd.ai-pipestream.leann-graph-config;version=1`, and a 32-byte
payload of eight little-endian `u32` fields in this order:

| Field | Default | Meaning |
|---|---|---|
| `bits_per_dimension` | 8 | Approximate scalar code width, 4 or 8 |
| `hub_degree` (M) | 32 | Neighbor cap of hubs and shrink cap of every node |
| `base_degree` (m) | 8 | Neighbors a non-hub node selects on insertion |
| `hub_ratio_ppm` | 20,000 | Share of nodes kept as hubs (2%) |
| `build_beam` | 64 | Insertion search beam (`ef_construction`) |
| `search_beam` | 64 | Minimum query beam (`ef`); a query uses `max(k, ef)` |
| `prune_ratio_ppm` | 0 | Share of each hop's new neighbors left approximate |
| `storage` | 0 | 0 = stored embeddings, 1 = recomputed embeddings |

`LeannGraphConfig::backend_config()` builds it. Every field is part of the
scoring fingerprint, so a dense execution policy measured at one setting
does not qualify another.

## Contract with the planner

The descriptor advertises `CONFIGURED_ANN` without `exhaustive_completion`,
so nothing about the exact path changes:

- `EXACT` and the legacy unspecified mode refuse the provider by name.
- `ANN` runs it and reports `resolved_mode = ANN`, never exhaustive.
- `AUTO` admits it only through a measured dense execution policy bound to
  its `provider_backend` and `scoring_fingerprint`
  ([Dense execution policy](dense-execution-policy.md)); without one it
  refuses.

Reported scores are exact FP32 inner products (the approximate codes only
steer traversal), so shards that share a config produce comparable scores.

## High-degree preserving pruning

Paper Algorithm 3. A full insertion graph at degree M is built only to
measure node degrees (distinct neighbors over out- and in-links). The top `hub_ratio_ppm` of nodes become
hubs. The serving graph is then rebuilt by inserting the same nodes in the
same order: a hub selects up to M neighbors and every other node up to m,
using the HNSW neighbor heuristic on directions (cosine), so vectors of
different lengths do not crowd each other out; edges are added in both directions and a
list that grows past M is shrunk back with the same heuristic. Traversal
enters through the best-connected hub. At the defaults the serving graph
keeps well under half of the full graph's edges while hubs stay dense (the
unit test pins both).

## Two-level search and selective recomputation

Paper Algorithm 2. Each hop expands the best exactly-scored frontier node,
scores its unvisited neighbors approximately from per-dimension scalar
codes (4 or 8 bits; the paper uses PQ, scalar codes need no training and
keep builds deterministic) into one global approximate queue, and computes
exact scores for the best `1 - prune_ratio` of the newly discovered
neighbors (at least one). Nodes left in the approximate queue are evaluated
on later hops when they become the best remaining. When the best frontier
node falls below the beam's worst result, the queue is drained of every
row whose approximate score plus the codes' proven error bound (half a
quantization step times the query weight, summed over dimensions) could
still enter the beam; the walk stops only when none can. Approximate scores
never end the search on their own. A floor (`minimum_score` or a shared
cross-shard floor) only filters results: a node below it can still lead to
rows above it.

Streaming (`search_streaming_controlled`) traverses at `search_beam` and
emits every allowed row whose exact score was computed, not only the
beam's best, plus the tail. It polls the control callback after every hop:
`Stop` ends the stream uncompleted, and a raised floor gates the rest of
that query and every later one.

Exact embeddings come from one of two places:

- **Stored** (default): the image keeps every FP32 row. Nothing is
  recomputed; `RAW_VECTOR_REBUILD` is advertised.
- **Recomputed**: the build drops the FP32 rows of indexed nodes. Exact
  scores are recomputed in per-hop batches through a product-side
  `RecomputeSource` attached with `VectorIndex::attach_recompute_source`.
  This is LEANN's storage saving: at 256 dimensions the image keeps about
  256 B of 8-bit codes (128 B at 4 bits) plus roughly 4 B per edge per row
  instead of 1,024 B of FP32.

Every row's FP32 fingerprint (the first 128 bits of the SHA-256 of its bit
patterns) is kept. A
source is attached only after a determinism gate recomputes up to 32 evenly
spaced indexed rows and reproduces each fingerprint bit for bit, and every
later recompute is checked the same way: a recompute that disagrees with
the indexed vector (another model, a nondeterministic kernel) is an error
naming the row, never a silently different score. A recompute-mode image
with no source attached refuses searches and rebuilds by name.

## Appends and images

Rows added after a build form an unindexed tail that is scored exactly
and kept as FP32 in both modes. `prepare()` folds the tail into a full
rebuild; `add()` also rebuilds once the tail reaches the larger of 1,024
rows and the indexed row count, so appends rebuild O(log n) times. The
rebuild runs synchronously inside `add`, on the writer's path. A shard
below 1,024 rows that is never `prepare`d stays an exact scan, which at
that size is cheaper than a graph.

The image (`PMLEANN1`, format 1) holds the config, row counts, row
fingerprints, approximate codes, hub list, CSR adjacency, the held FP32
rows, and a SHA-256 trailer. Loading checks the trailer and every
structural field (row layout, offsets, neighbor and hub ranges, trailing
bytes); every array length is checked against the bytes that remain, with
overflow-checked sizes, before anything is allocated for it. `load_mapped`
reads the image onto the heap and reports `is_mapped() == false`, which is
the truth about where it serves from. A row's transcript is
its FP32 fingerprint, the identity the reported score is computed from.

## What this does not do yet

- The product does not yet bind a vector field to its embedding model, so
  nodes have no source text to recompute from. Recompute mode is usable
  through the library API (`RecomputeSource`); serving it from a node needs
  the model-identity step of the LEANN assessment first.
- No measured dense execution policy ships for it; AUTO stays refused
  until one is measured on a real corpus.
- Per the assessment it targets small k. At k = 10,000 the exhaustive SIMD
  scan remains the right engine and stays the default.
