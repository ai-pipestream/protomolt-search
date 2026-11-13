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
`merge.rs`, `reorder.rs`, `rows.rs`). Tests: the module's unit tests, `tests/hnsw.rs` (real nodes
behind a coordinator: planner refusals, corruption, streaming control,
masks and floors, the candidate depth, multi-node fan-out, filter regimes
through the Query API, Boolean dense leaves, the policy's enumerate
threshold) and
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
| `locality_order` | `NONE` | Optional graph-locality vertex permutation (`#98`): `NONE` keeps ingest/replay slot order unchanged; `RCM` runs `O(V + E)` symmetrized Reverse Cuthill–McKee; `RECURSIVE_BISECTION` runs bounded min-cut recursive graph bisection down to 4 KB page-sized leaves (`max(8, min(64, 4096 / (4 · dim)))`) and is the recommended locality mode. |

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
- Recursive Boolean evaluation needs every member's score, which a walk
  cannot give. The descriptor advertises `mask_enumeration`, and the shard
  runs a Boolean dense pass pinned to the enumerate regime ("Filters"):
  every member is scored exactly, so the leaf is exact without the graph
  certifying anything. `ANN` inside a Boolean stays refused, as on every
  provider.

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

**One traversal distance per visited node, scored in bulk.** Every row the
walk visits is scored once from its FP32 row with the provider's own kernels
(`src/hnsw/distance.rs`): 8-row and 4-row batches on AVX-512F, 4-row batches
on AVX2+FMA (x86-64, chosen at run time with `is_x86_feature_detected!` while
the crate compiles for the x86-64-v2 baseline), 4-row `dot_batch4` and a tight
row-sequential `score_nodes` loop on NEON (aarch64, keeping contiguous cache
lines per row intact so 4-core builds and walks do not thrash shared LPDDR4X
row buffers), and a four-accumulator 4-row scalar batch elsewhere, with
single-row tails. Every batch kernel preserves its single-row kernel's exact
accumulation order per row, so a `FIXED_ORDER` build and walk visit the exact
same nodes bit for bit as single-row `dot`. Neighbor expansions and `enumerate`
pipeline software prefetching one batch ahead on x86-64 (`_mm_prefetch`, capped
at 8 cache lines on AVX-512F and 4 on AVX2/scalar so fill requests stay within
per-core MSHR budgets), while heuristic `select` prefetches upcoming pool
candidates and scores already-selected cache-resident neighbors in 4-row batches
without redundant prefetches.

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

## Appends and seal-time build

`add` appends raw FP32 rows and their inverse norms only (`Store::append_unlinked`);
no graph links are built per row during ingestion (#97). There is no online
per-row insertion path:

- **Seal / prepare (`MIN_GRAPH_ROWS = 256`).** `HnswIndex::write` and
  `HnswIndex::prepare` build the graph in bulk across all appended rows when the
  segment holds at least `MIN_GRAPH_ROWS` (`256`) rows (`PARALLEL` across rayon
  workers at `>= 256` rows, or `FIXED_ORDER` sequentially in row order). Below
  `MIN_GRAPH_ROWS`, sequential SIMD enumeration is faster than descending and
  walking HNSW layers on both x86-64 and aarch64 while returning exact 1.0
  recall, so tiny segments leave `entry = NO_NODE` and write zero link bytes.
  In fixed-order mode a graph is identical whether its rows arrived in one batch
  or many, which the determinism test pins.
- **Unlinked tail and tiny segments.** Before `prepare()` / `write()` (and on
  any sealed segment below `MIN_GRAPH_ROWS`), queries answer by SIMD batch
  enumeration (`View::enumerate_skipping` for unary top-`k`, chunked SIMD
  `stream_rows` for streaming) with online floor and top-`k` error-bound pruning
  so exact scalar rescoring runs only on rows the traversal kernel cannot rule
  out. On a segmented shard (`SegmentedProvider`), linked HNSW segments run
  before tiny unlinked segments, any seal-in-flight `frozen` image, and the live
  `tail`, carrying the running `k`-th score floor into the unlinked enumeration
  pass. Every row scored across unlinked parts is reported in
  `tail_enumerated_rows` (`ShardScanStats`, `StreamSearchSummary`,
  `SearchResponse`, and `DenseExecutionOutcome`; redacted on document-scoped
  responses).

## Filters

A filtered search carries an allow mask, one flag per row, as a byte per
row or as a bitmap (`vector::Mask`). The provider tests, counts and lists
the mask a word at a time when it is a bitmap; the byte form costs a pass
over every row to count. The mask's cardinality and the query's own
neighborhood decide how the query is answered, and the answer says which
regime ran (`hnsw::Answer`).

- **Enumerate.** Every allowed row is scored in row order and the result is
  the exact top `k` of the mask. The traversal kernel scores them all; the
  scalar scorer then settles every row the kernel cannot rule out (a row
  whose kernel score plus its error bound is below the k-th largest score
  minus its bound cannot be in the top `k`). Chosen when the mask allows at
  most the enumerate threshold: the request's `enumerate_max_rows` when a
  dense execution policy point carries one ("Through the product"), else
  `ENUMERATE_ROWS_PER_WALK_ROW · ef · 2M` rows, which is fewer rows than a
  walk would score, read in order instead of behind a cache miss each.
- **Filter-aware walk.** The layer-0 beam scores allowed rows only, and
  only they enter the candidate and result heaps. A disallowed neighbor is
  a bridge: it is not scored, its own allowed neighbors are scored in its
  place, until the expansion has scored one neighbor list (`2M` rows). This
  is the predicate-subgraph idea of ACORN-1 (Patel et al.,
  arXiv:2403.04871) on an unmodified graph. Chosen for every larger mask.
- **Plain walk.** The unfiltered walk with the mask gating results:
  disallowed rows are scored and routed through. The provider never chooses
  it, since the filter-aware walk reached the same recall for less work at
  every selectivity measured; it can be pinned
  (`VectorSearchOptions::filter_regime`) so a measurement can compare.

A mask can sit where the graph cannot serve it, or cost more to walk through
than to scan in mask order: one court, one tenant, a region away from the
query, or a correlated mask whose landing neighborhood clears the initial
sample while its best matches sit across disallowed bridges. Three checks
hand such a query to enumerate, so the answer is exact and bounded in work:

- **The far probe**, before the walk. The sample is the landing row's
  layer-0 neighbors and theirs, read from the links alone. When the mask's
  share of the rows predicts at least 8 allowed rows in the sample and
  fewer than a quarter of them are there, the query is enumerated.
- **The visit budget**, during the walk (`#94`). Each expansion of the
  filter-aware walk counts layer-0 work units `layer0_scored + bridges`
  (one unit per layer-0 scored row plus one unit per disallowed bridge
  neighbor list read and probed). Across AVX2/AVX-512F and NEON (`d = 64` and
  `d = 256`), one random walk or bridge unit costs ~3.0–3.6× one sequential
  mask-ordered row in `enumerate` (`WALK_WORK_ENUMERATE_ROWS = 3.5` under the
  default `ENUMERATE_ROWS_PER_WALK_ROW = 4`, or scaled proportionally by a
  policy point's `enumerate_max_rows / (4 · ef · 2M)` with a floor of `1.0`).
  Once `work_units >= ceil(allowed / cost_ratio)` and the beam has not yet
  converged, the walk stops and hands the remaining unvisited allowed rows
  (`!scratch.seen(row)`) to `enumerate` (or `stream_rows`), merging with the
  allowed rows already scored so no row is scored or emitted twice.
- **A stranded walk**, after it. A walk that runs dry with its beam
  unfilled while the mask allows more rows reached an island; the query is
  finished by enumerating the unvisited allowed rows on both the unary and
  streaming paths, scoring and emitting no row twice.

None of the three applies to a pinned regime, which runs as pinned. All
three apply under a request's threshold: they only ever move a query to the
exact answer, and under a policy point the visit budget derives from that
point's `enumerate_max_rows`.

Enumerating a far mask costs its allowed rows, as a filtered flat scan
does. That is the price of the exact answer where the graph has none; the
regime in the answer lets a caller see it was paid.

Seeding the walk instead was tried and is not in the provider. With the
probe tripped, 64 allowed rows drawn from across the mask were scored and
used as the walk's entries. On the challenge fixture's far masks (whole
k-means cells away from the query) recall at k 10 was 0.16 to 0.59 for
beams of 16 to 256 at a 50% mask and 0.30 to 0.50 for beams of 16 to 64 at
10%: the allowed cells are islands joined only through rows the walk may
not score, and a seed that lands in the right island but scores under the
beam's worst is never expanded. Entries kept per facet value, or links
kept per value, are what #55 names next for these masks; until one of them
holds recall, a far mask is enumerated.

What the regimes cost on the harness's challenge fixture (100,000 rows of
64 dimensions, k 10, one core of a shared 4-vCPU cloud host, queries per
second at the first beam reaching recall 0.95; `--hnsw-regime` pins a
regime). The host is noisy and the numbers are not a claim; they are why
the rules above are what they are:

| filter (allowed share) | filter-aware | plain | enumerate | provider's choice |
|---|---|---|---|---|
| uncorrelated 50% | 6333 | 4920 | 833 | 6735 |
| uncorrelated 10% | 7817 | 4512 | 2724 | 6843 |
| uncorrelated 1% | 9523 | 777 | 50707 | 45172 (enumerate) |
| near the query 10% | 7749 | 6255 | 2747 | 7215 |
| near the query 1% | 8859 | 5672 | 16621 | 14775 (enumerate) |
| far from the query 50% | recall 0.50 | recall 0.94 | 827 | 678 (probe, enumerate) |
| far from the query 10% | recall 0.17 | 130 | 2704 | 2208 (probe, enumerate) |
| far from the query 1% | recall 0.03 | 81 | 14549 | 13940 (enumerate) |

`minimum_score` and a floor raised during a stream filter what is returned or
emitted; when shared floor pruning is enabled (`floor_prune = true` on the scan,
see "Shared floor pruning" below), a floor raised during a stream or carried
from an earlier sealed segment also prunes the layer-0 walk once the ascent gate
is satisfied (#72).

## Patience

A layer-0 walk at a wide beam (`ef`) often finishes discovering new beam
members well before the candidate heap's best drops below the beam's `ef`-th
member, especially under ACORN-1 bridge expansions (`ef >= k = 100` on
50% predicates) or when `EF_FLOOR = 16` forces more expansions than `k = 10`
needs. When a search carries a `PatienceSpec` (`patience_saturation_ppm` in
`1..=1,000,000` and `patience_hops >= 1`), `search_layer` and
`search_layer_aware` track hop-to-hop beam saturation in `O(1)` time and zero
per-hop heap allocations (#95):

- **Beam-window survival (`PatienceWindow::Beam`).** Tracking survival over the
  full `ef` result beam avoids the small-`k` quantization cliff (`k = 10` can
  only express 90% or 100% survival, turning `s = 0.95` and `s = 0.99` into
  identical 10/10 zero-insertion checks). At the start of each layer-0
  expansion once `scratch.res.len() >= ef`, an element of the beam fails to
  survive iff it is evicted from `scratch.res` during that expansion and was
  not itself inserted during the same expansion (`patience_added` tracks at
  most `2M` insertions in pre-allocated scratch). When
  `ef - lost >= ceil(saturation_ppm · ef / 1,000,000)` holds for
  `patience_hops` consecutive expansions, the walk ends early (`Walk::Patience`)
  and increments `patience_stopped`.
- **Ascent gate under a floor.** When a finite score floor is active, patience
  never counts until at least `max(ef, ascent_gate)` allowed rows have been
  scored locally at layer 0, so a cold entry point ascending toward the query
  neighborhood cannot be cut short before reaching the target basin (#72).
- **Determinism when off.** With `patience == 0` (the default on explicit `ANN`
  and unparameterized policy points), zero extra heap operations run and a
  `FIXED_ORDER` walk visits the exact same nodes bit for bit.
- **Measured Pareto wins (`protomolt-ann-bench patience`).** Across zero-jitter
  `pi500p` (`Neon`, 16 GB NVMe Pi 5) and `krick-1` (`Avx512f`), beam-window
  patience (`s = 0.99, p ∈ {7, 15, 30}`) wins QPS at matched recall on 10/20
  cells on `Neon` and 7/20 cells on `Avx512f`, led by `uncorrelated-50 k = 100`
  (`2.14×` 1c / `2.08×` 4c on `Neon`; `3.18×` 1c / `3.14×` 4c on `Avx512f`,
  `2.30×` faster even at `recall >= 0.99` because plain HNSW cannot set
  `ef < k = 100`), `uncorrelated-10 k = 10` (`1.16×`–`1.18×`), and `court200k`
  `none` / `near-10` / `uncorrelated-50` `k = 10` (`+7.7%`–`+22.9%` over
  interpolated plain-`ef` QPS). Patience is optional per policy point (`AUTO`
  enables it only where a point names `patience_saturation_ppm` and
  `patience_hops`), so cells where plain `ef` already sits on the Pareto
  frontier pay zero cost.

### Through the product

- **The mask.** A shard resolves the request's filters into a bitmap from
  its column indexes ([Vector filters](vector-filters.md), "The mask from
  the columns") and hands the provider that bitmap on both scan routes.
  Segmented vectors give each sealed image its own span of it, so the
  regime is chosen per segment from that segment's allowed rows.
- **One search per shard.** The unary scan (`SearchShard`) cuts an
  exhaustive index into chunks and masks each. A graph has no chunks to
  cut, and a mask naming one chunk's rows would make every chunk a filtered
  search of the whole graph, so a provider without `exhaustive_completion`
  is searched once per query at the request's depth
  (`chunked::searched_topk`). The floor known at the start filters the
  result and the k-th best found is offered once, at the end.
- **The regime is reported.** Every filtered search is counted under the
  regime that answered it, per segment, and the counts travel with the
  scan's statistics through relays to
  `QueryResponse.dense_execution.filter_regimes` (`enumerate`,
  `filter_aware`, `plain`, and `budget_handoffs`). A query the probe, the
  visit budget, or a stranded walk handed over counts as `enumerate`, and
  one handed over by the visit budget also increments `budget_handoffs` so a
  measurement separates mid-walk budget handoffs from pre-walk and stranded
  ones. Absent when no search was filtered; withheld, like the rest of the
  physical layout, from a document-scoped response
  ([Query execution disclosure](query-disclosure.md)).
- **The threshold is the policy's.** `AUTO` serves a measured recall claim,
  and a claim measured under one threshold must not be served under
  another. A policy point for a filtered band names the
  `enumerate_max_rows` it was measured with
  ([Dense execution policy](dense-execution-policy.md), "Filter regimes");
  the coordinator sends it with the scan and the provider chooses under it
  instead of its own constant. Explicit `ANN` accepts the provider's
  traversal and runs under the provider's threshold. The harness measures
  a threshold the same way: `--hnsw-enumerate-max-rows=N`.
- **Boolean dense leaves** run pinned to enumerate, as "What it may serve"
  says.

There is no second threshold. The issue that planned these regimes (#55)
expected a plain walk above some selectivity; the table above is why it is
never chosen, and a threshold nothing would cross is not stored.

## Streaming

One walk per query at the request's depth. Every allowed row the walk
scores at layer 0 (the entry point included; the rows the descent scores
are not) and whose exact score is at or above the floor is emitted, in
batches of up to 64 as the walk proceeds, each batch ordered score
descending then row ascending. To seed the coordinator's global top-`k`
floor without waiting for 64 layer-0 candidates, `StreamHook` performs an
early first-flush after `STREAM_FIRST_FLUSH_HOPS = 2` layer-0 expansions
whenever at least `STREAM_FIRST_FLUSH_ROWS = 8` rows are buffered above the
current floor.

The control callback is polled after every expansion:

- `Stop` (`StopStreamSearch` / `TVS1` UDP `CANCEL = 2`) abandons the walk
  immediately (`completed = false`, `end_reason = ABANDONED`); any partial
  shard emissions are discarded.
- `Finish` (`FinishStreamSearch` / `TVS1` UDP `FINISH = 3`, `#100`) ends the
  layer-0 walk cleanly at the next hop (`completed = true`,
  `end_reason = FINISH_REQUESTED` or `DEADLINE`), flushes any buffered
  candidates still at or above the latest floor, emits its normal terminal
  `StreamSearchSummary` (or `IdentityReady` followed by
  `ResolveStreamIdentities`), and closes the stream so its rows count as a
  usable ANN result. Exhaustive providers (`EXHAUSTIVE_NATIVE_SCORE`) and the
  exact `Enumerate` filter regime ignore `Finish` so an `EXACT` or
  exhaustive-regime scan never returns truncated results claiming completion.
- A floor raised by either callback gates the rest of that query and every
  later one, and a buffered row that fell below a floor raised since it was
  scored is dropped before the sink sees it.

### Graceful stream close (`#100`)

On a multi-shard streaming fanout (`fanout_stream_search` and
`fanout_stream_search_collapse`), once a shard has `floor-sharing` enabled
and an approximate traversal active, the coordinator may send
`FinishStreamSearch` (paired with an advisory `TVS1` UDP `FINISH = 3`
datagram, HMAC-SHA256 authenticated when `--udp-hmac-key` is configured) under
two optional microsecond windows (`--stream-finish-settle-us` /
`--stream-finish-deadline-us`, per-request `finish_settle_us` /
`finish_deadline_us`, or policy-bound `DensePolicyPoint.finish_settle_us`):

- **Settle window (`finish_settle_us`).** Armed only after the merged global
  top-`k` heap (or `k` parent groups in collapse mode) is full **and** a shard
  quorum (`1` for `<= 2` active shards, `floor(3N / 4)` for `N > 2`, so 12 of
  16 shards) has reached `IdentityReady` / `Summary`. Every improvement to the
  merged `k`-th floor resets the settle timer; if the timer expires with
  stragglers still walking, the coordinator pushes `Finish { deadline: false }`
  to the remaining active shards.
- **Deadline window (`finish_deadline_us`).** Bounds total streaming fanout
  wall time by pushing `Finish { deadline: true }` to remaining shards when
  expired, reporting `end_reason = DEADLINE` while keeping `completed = true`
  on approximate walks.

Relays (`RelayNodeService`) forward both the advisory UDP `FINISH` datagram
and the authoritative gRPC `FinishStreamSearch` message to unfinished children
and merge child `StreamEndReason` values (`Abandoned > Deadline >
FinishRequested > FloorPruned > Exhausted`). The number of shards that ended via
`FINISH_REQUESTED` or `DEADLINE` is reported in
`SearchResponse.shards_finished_early` and
`DenseExecutionOutcome.shards_finished_early` (withheld on document-scoped
responses).

On a 16-shard Raspberry Pi 5 fleet (`court200k`, 200,000 × 256 FP32 vectors,
8 shard hosts × 2 shards + 1 coordinator Pi 5, 5 staggered repeats):
- **Uniform fleet (`hash` and spherical `kmeans`):** `shards_finished_early = 0`
  and bit-identical recall across `k ∈ {100, 1000, 10000}` (zero false-positive
  early closes).
- **Straggler shard (`hash`, `350 µs/hop`, `settle_us = 500`):** cuts `p50`
  from `430.7 ms → 143.8 ms` (`3.00×`, `recall@1000 = 0.9976` vs `0.9977`) and
  from `4,295.4 ms → 261.8 ms` (`16.4×`, `recall@10000 = 0.9975` vs `0.9981`).
- **Straggler Voronoi shard (`kmeans`):** at `settle_us = 15,000..30,000`
  (wider than a stalled hot cluster's inter-batch interval so active top-`k`
  batches reset the settle timer while cold-cluster stragglers close), cuts
  `k = 1,000` `p50` (`350 µs/hop`) from `430.4 ms → 175.9 ms` (`2.45×`,
  `recall@1000 = 0.9658` vs `0.9698`) and `k = 10,000` `p50` (`50 µs/hop`) from
  `1,081.5 ms → 320.2 ms` (`3.38×`, `recall@10000 = 0.9712` vs `0.9734`).

### Shared floor pruning (`#72`)

When `floor_prune = true` on `StartShardSearch` / `StartStreamSearch` (paired
with `floor_k`, `floor_greediness_ppm`, and `floor_clamp_min`, advertised via
`VectorBackendCapabilities.floor_pruning`), HNSW uses the active score floor to
cut layer-0 walks short (`Walk::Floor`, `end_reason = FLOOR_PRUNED`,
`completed = true`) in addition to gating emitted rows:

- **Kernel-vs-exact numerical slack.** HNSW traverses with SIMD `dot` kernels
  whose lane association differs from `exact_vectors::dot`. Before comparing any
  traversal score `c.score` against a caller or shared exact floor `f`, the walk
  subtracts Higham's deterministic pairwise-summation error bound
  `4 · γ(d) · ‖q‖₂ · ‖x‖₂` (`γ(d) = d · u / (1 − d · u)`, `u = 2⁻²⁴`, with
  `‖x‖₂ = 1 / inv_norm[u]` already stored in `PMHNSW01` and `‖q‖₂` computed once
  per query), guaranteeing zero false-negative stops from floating-point
  reassociation across `Scalar`, `Avx2`, `Avx512f`, and `Neon`.
- **Expected-share ascent gate (`gateK`) and clamp.** Because a shard's fixed
  entry point may sit far from the query, applying a shared floor at hop 0 would
  sever the ascent path before reaching the query basin. Given `floor_k = k` and
  this walk's eligible-row share `p = share_ppm / 1,000,000` (`s_e / E` across
  the planned shard set, scaled proportionally across local segments), the
  variance-padded ascent gate is
  `gateK = ceil(k · p + 16 · sqrt(k · p · (1 − p)))` and the clamped gate is
  `G = min(local_eligible, max(clamp_min, ceil((1 − g) · gateK)))` with default
  `DEFAULT_FLOOR_CLAMP_MIN = 8` and `g = floor_greediness_ppm / 1,000,000`
  (`0` = full `gateK`, `500,000` = half gate clamped by `clamp_min`, `1,000,000`
  with `clamp_min = 1` = hop-0 floor). Only after `allowed_scored >= G` at
  layer 0 does a candidate with `c.score + slack < floor` terminate the walk.
- **Segmented search.** Within a shard (`SegmentedVectors`), sealed HNSW
  segments run before unlinked `frozen`/`tail` slices (`#97`) and raise the
  shared `ScoreFloor` as soon as `k` hits are collected, allowing later sealed
  segments to prune their layer-0 walks once their segment-proportional `gateK`
  is met.
- **Provenance and telemetry.** `DenseExecutionPolicy` points may name
  `floor_pruning = true` (with optional `floor_greediness_ppm` and
  `floor_clamp_min`); shards ending via `Walk::Floor` report
  `end_reason = FLOOR_PRUNED`, summed across relays into
  `SearchResponse.shards_floor_pruned` and
  `DenseExecutionOutcome.shards_floor_pruned` (withheld on document-scoped
  responses).

On the 16-shard Raspberry Pi 5 fleet (`court200k`, 200,000 × 256 FP32 vectors,
`ef = max(k, 16)`):
- **Uniform fleet (`hash` and spherical `kmeans`):** `gateK` (`greediness = 0`,
  `clamp_min = 32`) matches `emit_only` recall within `0.0000`–`0.0001` at every
  `k ∈ {100, 1000, 10000}` (`hash`: `0.9965` / `0.9988` / `0.9986`; `kmeans`:
  `0.9591` / `0.9809` / `0.9802` bit-identical across all modes).
- **Straggler shard (`350 µs/hop` on shard 13, `settle = off`):** once fast
  shards establish the global `k`-th floor, `gateK` prunes the slow shard's
  remaining tail walk without any coordinator timer:
  - `hash`: `k = 1,000` `p50` drops from `431.0 ms → 146.3 ms` (`2.95×`,
    `recall = 0.9986` vs `0.9988`, slow-shard scans `4,926 → 1,949`);
    `k = 10,000` `p50` drops from `4,245.1 ms → 293.6 ms` (`14.5×`,
    `recall = 0.9983` vs `0.9986`, slow-shard scans `11,804 → 4,043`).
  - `kmeans`: `k = 1,000` `p50` drops from `432.3 ms → 195.2 ms` (`2.21×`,
    `recall = 0.9801` vs `0.9809`); `k = 10,000` `p50` drops from
    `4,072.4 ms → 396.9 ms` (`10.3×`, `recall = 0.9797` vs `0.9802`).
- **Centroid topology hints (`#101`)** were measured against `gateK` on the same
  16-shard `hash` and `kmeans` matrices (both coordinator-relayed
  `FloorCentroidHint` bitmasks and zero-wire query-local top-`M` centroids) and
  omitted: `gateK` already loses `0.0000` recall on uniform Voronoi shards,
  whereas relayed centroid hints add `8.8–157.0 KB/query` of control traffic
  (`-13%` to `-16%` QPS at `k = 10,000`) and saturate the active-centroid mask at
  large `k`, defeating straggler floor pruning (`3,791.9 ms` vs `293.6 ms` on
  `hash` `k = 10,000`; `3,892.6 ms` vs `396.9 ms` on `kmeans` `k = 10,000`).

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
bases, layer 0, upper layers, plus optional 64-byte-aligned `node_to_row` and
`row_to_node` `u32` bijection sections when a seal-time `locality_order` map is
present), each on a 64-byte boundary, then a SHA-256 trailer over everything
before it. Offset 68 packs `top | (locality_order << 8) | (has_node_map << 16)`,
leaving `LocalityOrder::None` images byte-for-byte identical to the base format.
An unlinked image (`n == 0` or `n < MIN_GRAPH_ROWS`, recorded with
`entry = NO_NODE`, `top = 0`, `upper_nodes = 0`, `upper_cells = 0`) stores only
the header, aligned `rows` and `inv_norm` sections, and the SHA-256 trailer,
omitting all five link sections (saving ~141 B/row at `M = 16`). Loading parses
and checks the header first (magic, format, CRC, every count, unlinked and
locality-flag invariants, and that the counts lay out to exactly the file's
length) before any section is allocated or touched, then verifies the trailer,
validates the inverse `node_to_row` / `row_to_node` bijection when present, and
(when linked) walks the structure: levels against the cap, the compact upper
index, every list's count against its cap, every neighbor id against the row
count, its own id and its level. After that no index into the graph can go out
of bounds.

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
- **Compaction (`#76`, `#99`).** Online compaction (`src/compaction.rs`,
  `src/reshard.rs`, `src/hnsw/merge.rs`) snapshots the shard's sealed mapped HNSW
  segments before replay, matches each surviving dense output row against its
  source `(segment_idx, old_local_slot)` (verifying FP32 bit equality so a
  replaced vector never inherits stale edges), and merges the output graph via
  `HnswIndex::merge_with_remap` (`MergeStrategy::JoinSet`) rather than rebuilding
  from scratch:
  1. **Donor transplant and tombstone link healing (`#76`).** Qualifying linked
     source segments (`>= 64` surviving rows and `>= 35%` survival ratio, matching
     `dim` and `M`) elect the segment with the most surviving rows as the donor.
     Surviving donor nodes retain their levels and 100% of their surviving 1-hop
     edges under the dense `0..N` numbering; any adjacency list that held a
     deleted neighbor refills its vacated slots from heuristic-diversified and
     top-scoring 2-hop bridges across deleted neighbors, wires reciprocal
     `connect_back` links, and promotes a surviving top-level entry (inheriting
     the deleted entry's surviving highways across layers when `entry` itself was
     tombstoned). On 10% and 30% tombstone sweeps (`protomolt-ann-bench merge`),
     `HnswIndex::heal_tombstones` runs **9.5×–10.3×** (`10%`) and **4.1×–4.2×**
     (`30%`) faster than a clean rebuild on `challenge` (`100k × 64d`) and
     **22.3×–32.4×** (`10%`) and **11.0×–18.5×** (`30%`) faster on `court200k`
     (`200k × 256d`), matching rebuild `recall@10` within `0.003` and matching or
     exceeding rebuild `recall@100` (`+0.000` to `+0.005`).
  2. **Greedy `K`-dominating join-set merge (`#99`).** Each non-donor qualifying
     segment selects a join set comprising all upper-layer nodes (`level >= 1`)
     plus a greedy `K`-dominating layer-0 cover (`JOIN_DOMINATION_K = 2`, ordered
     by surviving degree descending; ~21–24% of non-donor rows on `challenge`,
     ~40–43% on `court200k`). Only the join set and any unlinked tail/tiny-segment
     rows run full `LinkBuilder` insertion, interleaved round-robin across
     segments; the remaining ~57–79% of non-donor layer-0 nodes run a bounded
     local layer-0 beam walk (`GRAFT_MAX_EXPANSIONS = 40`, zero upper-layer
     descent) seeded from their surviving intra-segment neighborhood and the
     merged layer-0 CSR neighborhoods of their join-set neighbors, followed by
     direction-heuristic pruning, intra-cluster slot retention, and reciprocal
     `connect_back`. Across 2, 4, and 8 equal and skewed segments, `JoinSet`
     runs **2.0×–5.7×** faster than `FullRebuild` (**1.6×–2.1×** faster than
     `#76`'s `DonorReinsert`) on both `avx2+fma` and `neon` while holding recall
     within `0.005` of `FullRebuild` (and exceeding `FullRebuild` `recall@10` on
     every 2/4/8 layout on `court200k` `neon`). When no linked segment qualifies
     (for example a multi-bucket hash sub-sample below 35% survival or a 100%
     tombstoned donor), merge falls back cleanly to `FullRebuild`.
  3. **Directed forward-BFS connectivity repair (`#99`).** Every merged, healed,
     or rebuilt image runs an `O(V + E)` forward BFS from `entry` on layer 0,
     tracking spanning-tree parents (`bfs_parent`) so reciprocal connections never
     detach a mutual cycle, bridging leaf/duplicate islands in `O(m0)` via their
     direct visited neighbors and multi-node islands via a masked layer-0 walk
     from `entry`, and leaving `unreachable_from_entry() == 0`.
  4. **Fresh column indexes after remap (`#76`).** `ChildImage` writes the
     post-remap `Bm25Store` with lazy `OnceLock` `FacetRows` and `NumberRows`
     caches uninitialized (`OnceLock::new()`), so pre-compaction cached bitmaps or
     sorted `(value, row)` tables are dropped with the retired `OpenedSegmentSet`
     and built fresh on first filtered query against the compacted segment.
  5. **Locality row reordering (`#98`).** When `locality_order` is enabled
     (`RECURSIVE_BISECTION` or `RCM` in `src/hnsw/reorder.rs`), neighbor lists keep
     `select()`'s exact relative candidate order while nodes and stored rows are
     permuted to cluster layer-0 neighborhoods onto shared cache lines and 4 KB OS
     pages:
     - **At seal (`Flush`):** WAL, `Bm25Store`, `.exact`, `LiveDocs`, `parent_ids`,
       and column filter structures keep their ingest-assigned slot IDs untouched;
       `HnswIndex::write(&self)` constructs the permuted `PMHNSW01` image without
       mutating the shared frozen-tail `Arc`, persisting an `8 B/row`
       (`node_to_row` + `row_to_node`) bijection served zero-copy from the mmap
       (translating external filter masks into node-order `Bits` once per query and
       remapping reported/streamed hits back to external slot IDs).
     - **At non-partitioned compaction:** `compact_log_with_sources` folds the
       locality permutation directly into the child segment's global slot order,
       aligning WAL, `id_map`, `Bm25Store`, `.exact`, `parent_ids`, fresh lazy
       `FacetRows` / `NumberRows` caches, and `.hnsw` (`has_node_map = false`,
       `0 B/row` map overhead) under one shared locality-ordered ID space
       (partitioned compaction preserves `PartitionRange` contiguity and falls back
       automatically to the seal-local `NodeMap`).
     - **Measured `RECURSIVE_BISECTION` vs `RCM` trade-off (`protomolt-ann-bench reorder`):**
       `RECURSIVE_BISECTION` seeds each balanced split with an `O(V + E)` bucketed
       max-overlap hemisphere queue (preventing HNSW's small-world angular-diversity
       shortcuts from polluting initial cuts) followed by `O(V + E)` bucket-sorted
       Fiduccia–Mattheyses pair-swap refinement down to page-sized leaves. On
       `challenge` (`100k × 64d`), `RECURSIVE_BISECTION` adds `4.2%–10.7%` to build
       time, shrinks mean `|u - v|` **20.5×** (`33,680 → 1,624`) and unique 4 KB
       vector pages/query up to **10.3×** (`4,037 → 391`), boosts pure in-RAM
       mapped QPS **+10% to +14%** (`Avx512f`), **+14% to +24%** (`Avx2`), and
       **+16% to +40%** (`Neon`) at `0.0000` recall delta, and cuts cold ext4 NVMe
       `majflt`/query **6.0×–9.5×** (`1,586 → 167` at `ef = 160`, **8.2×** higher
       cold NVMe QPS). On `court200k` (`200k × 256d`, `223 MiB` `.hnsw`),
       `RECURSIVE_BISECTION` adds only `1.0%–7.4%` to build time, cuts median
       `|u - v|` **7.7×–8.8×** (`35,908 → 4,064`), boosts pure in-RAM mapped QPS
       **+15% to +30%** on `Avx512f` (`p99` cut **2.0×–2.6×**), **+9% to +22%** on
       `Avx2`, and **+7% to +10%** on `Neon`, cuts cold NVMe `majflt`/query
       **30%–36%** (**+45% to +56%** cold NVMe QPS), and reduces `64 MiB` cgroup
       (`mapped > page cache`) `majflt`/query **6%–12%** (`+13% to +72%` 4t QPS) —
       whereas FIFO breadth-first `RCM` smears across small-world shortcuts on
       multi-chunk corpora (`same_4k_row_page_frac` `2.63% → 0.29%`). Prefer
       `RECURSIVE_BISECTION` whenever locality ordering is enabled.
  A publication interrupted between the write of its outputs and the publish of
  its manifest rolls back at open: the staged segment and the marker are removed
  and the old generation serves.

## What this does not do yet

- No performance claim. The harness numbers in `benchmarks/ann/` describe a
  fixture on one host; the product decides what the provider serves through
  the measured dense execution policy of #58.
- The FP32 rows are the provider's own copy, as FAISS `HNSWFlat` keeps one.
  Sharing them with the `.exact` sidecar through `RowSource` is #71.
- Collapse (#73) and per-value seeded entries (#55) are later tickets.
- No measured dense execution policy ships for it; AUTO stays refused until
  one is measured on a real corpus.
