# Public query contract

Status: increment 1 is IMPLEMENTED (2026-08-24): `SearchService.Query`
executes the shapes in the mapping table below by delegating to `Search`,
`Bm25Search`, and `HybridSearch` (`src/query.rs`, `tests/query_api.rs`),
with per-signal provenance and by-name refusal of everything else. The
generic composite scorer is IMPLEMENTED (2026-08-26, `src/ltr.rs`,
`tests/ltr.rs`): all six operations, per-dimension normalization and
missing policies, and per-dimension provenance (`QueryHit.dimensions`)
precise enough to recompute every final score client-side — the
reconstruction guarantee is a test, not a promise. The boost phase is
GENERALIZED (2026-08-26): boosts are lexical or dense (the `Bm25Rescore`
and `VectorRescore` seams), serve any scored selection including
single-leaf shapes, and combine through the scorer when there is more
than one — without a scorer, exactly one boost is served and its
`base_weight`/`boost_weight` reorder is the combination. Stored-value
dimensions are IMPLEMENTED (2026-08-26): `ScoreSignal.bounded_value`
carries a score stage evaluated at its IDENTITY score per candidate
(the factor for the multiplicative ops — exp decay, log boost, geo
decay — the addend for `ADD_LINEAR`) through the candidate-scoped
`NodeService.FetchValues` seam, with the stage admission rules and the
column typo rule exactly as on the lexical route; a candidate without
the value is a missing signal under the dimension's policy. The same
seam serves `QueryRequest.projections` on every shape
(`docs/cel-values.md`). `profile` is served (2026-08-26):
`QueryResponse.profile` reports per-phase timings (selection, boost,
values, scorer, projection, total) and never alters results — the
suite holds profiled hits bitwise to unprofiled ones. With that, every
response requirement in this document is met except arbitrary nested
boolean search. The phase split, the boost contract, and the refusal
rules in this document are the binding contract.

The public model separates three things that are easy to conflate:

1. A **search/filter query** decides which documents enter the candidate set
   and produces the base relevance signals.
2. A **boost query** scores only that fixed candidate set. It may reorder
   candidates, but it never admits a document.
3. A **composite search strategy** defines boolean membership and how the
   first-stage search signals establish the base order. A separate generic
   composite scorer combines named base and boost signals after boosting.

```mermaid
flowchart LR
    Request["Query request"] --> Select["Selection: search + filter clauses"]
    Select -->|"selection_k candidates"| Boost["Candidate-scoped boost queries"]
    Boost --> Score["Composite scorer over named signals"]
    Score -->|"top k"| Result["Hits + score provenance"]
```

This ordering is part of the contract. A boost is not a `should` clause in
disguise, and a filter is not a zero-weight scoring query.

If matching a would-be boost must affect `AND`/`OR` membership, that query is a
`SearchQuery` in the selection tree. Its relevance signal may still be reused
by the composite scorer. Calling it a boost cannot grant it permission to add
or remove candidates after selection.

## Selection

A selection tree contains three node kinds:

- `SearchQuery`: a scoring leaf such as BM25 or dense vector relevance. Every
  search leaf has a request-unique `id`, and its raw relevance score is a named
  signal.
- `FilterQuery`: a membership-only leaf, initially CEL and the typed geo
  predicates it compiles to. It never contributes a relevance score.
- `CompositeSearchStrategy`: `AND` or `OR` over child selection nodes plus an
  explicit strategy for the scoring leaves.

`AND` and `OR` apply to membership:

- `AND` admits a document only when every child admits it.
- `OR` admits a document when at least one child admits it.
- A lexical search admits documents that match its analyzed terms.
- A dense search admits every vector at or above its explicit score floor. With
  no floor, a dense leaf admits every document carrying that vector space.
- A filter admits documents for which its predicate is true. Under `OR`, a
  filter-only match may enter with no search relevance and therefore needs a
  scorer or deterministic tie order.

The boolean tree and scoring strategy are distinct. For example, `AND` says a
document must match both lexical and vector gates; it does not say whether the
two relevance scores combine through RRF, normalization, a raw weighted sum,
or a cascade.

The first public strategy vocabulary should reuse the algorithms already in
`FusionMode`:

- single scoring query;
- global-rank RRF;
- retained-set score blend;
- exact decomposed weighted sum;
- cascade, where one search is the candidate gate and another is the reranker.

Every strategy names its exactness domain. An implementation must reject a
shape it cannot certify. It must not translate an unsupported `AND` into a
union, apply a filter to only one hybrid leg, replace a decomposed sum with a
truncated blend, or return a partial shard set.

## Boost phase

A `BoostQuery` contains a normal scoring query and an `id`. It runs against the
selection's top `selection_k` documents, never the corpus. Its query relevance
becomes another named signal. The selected candidate set is immutable during
this phase.

`selection_k` and output `k` are deliberately separate:

\[
k \leq selection\_k
\]

`selection_k` is coordinator-owned. Streaming vector nodes remain completely
k-blind: they receive a query and a monotonically rising score floor, emit
every qualifying candidate, and issue their own completion certificate. The
coordinator uses `selection_k` for its first-stage global heap and later trims
the post-boost result to `k`.

The initial implementation may reuse the coordinator's existing `max_k`
guardrail, making request validation `k <= selection_k <= max_k`. A later
separate selection-depth cap is an operational choice, not node protocol.

The final response is the best `k` documents under the post-boost scorer from
that candidate set. It is not represented as the global top-k of the boosted
formula unless the selection strategy itself included the boost signal and
proved that stronger claim.

This is the honest form of the existing rescore-window behavior. A caller that
needs more recall under a strong boost increases `selection_k`; the server does
not silently over-fetch by an undocumented factor.

## Composite scorer

Implemented 2026-08-26 (`src/ltr.rs`): the scorer runs on the
coordinator over the fixed `selection_k` candidate pool, after the
selection strategy and after boost signals are computed. Decisions
pinned at implementation time:

- `UNSPECIFIED` normalization resolves to `MIN_MAX` and `UNSPECIFIED`
  missing policy to `ZERO` (the engine-wide defaults, matching the
  blend); the operation itself has no default and refuses unset.
- Normalization statistics are fitted over the pool's PRESENT values;
  a `ZERO`-policy miss contributes a normalized zero, not the
  normalization of a raw zero. Degenerate pools follow the blend
  rules (min-max 1.0, z-score 0.0).
- A negative weight is admitted under `weighted_sum` alone (a penalty
  term); an explicit zero weight is evaluated and reported but
  excluded from combination. Geometric and harmonic means keep the
  blend's positive-value skip rule.
- Scorer arithmetic is f64 in dimension list order; ties in the final
  order break on the f32 wire score, then doc id — exactly what the
  client sees, never hidden f64 residue.
- Because normalization statistics move with the pool, a scored query
  pages WITHIN its fixed `selection_k` pool (the composite rule);
  `selection_k` is therefore meaningful on single-leaf shapes when a
  scorer is present.
- With a scorer present a boost is signal-only: `window`,
  `base_weight`, and `boost_weight` refuse by name, and the whole
  pool is scored.

The scorer follows ProtoMolt quality scoring's useful shape: stable dimension
ids, explicit weights, deterministic combination, and per-dimension reporting.
Search adds two requirements because its raw signals do not naturally share a
`[0, 1]` scale:

- each dimension names its normalization or explicitly selects raw identity;
- missing-signal behavior is explicit (`ZERO`, `SKIP`, or `ERROR`).

A dimension can source:

- the first-stage composite base score;
- a search query's raw relevance by query id;
- a boost query's raw relevance by query id;
- a bounded stored-value score function when the backend supports it.

The initial combination vocabulary should be weighted sum, weighted mean,
maximum, product, geometric mean, and harmonic mean. For weighted mean:

\[
S(d) = \frac{\sum_i w_i s_i(d)}{\sum_i w_i}
\]

Dimensions skipped under the missing policy are absent from both sums. A zero
weight is an explicit disable, not an unset default. All weights and produced
scores must be finite.

The response reports the normalized value and weighted contribution for every
dimension. A client must be able to explain the final score without
reimplementing server arithmetic.

The generic scorer runs after candidate selection, so it cannot invalidate a
first-stage pruning certificate. A score function that must influence corpus
search belongs in the composite search strategy and is admitted only when the
engine has a safe upper-bound rule. This preserves the existing rule that CEL
selects while bounded functions score; arbitrary CEL scoring must not make
block-max or live-floor pruning unsound.

## Proposed protobuf shape

Names and field numbers remain provisional until the RPC is implemented. The
shape, phase ordering, and refusal rules are the stable design decisions.

```proto
message QueryRequest {
  string request_id = 1;
  uint32 k = 2;
  uint32 selection_k = 3;
  SelectionQuery selection = 4;
  repeated BoostQuery boosts = 5;
  CompositeScorer scorer = 6;
  bool profile = 7;
}

message SelectionQuery {
  oneof node {
    SearchQuery search = 1;
    FilterQuery filter = 2;
    CompositeSearchStrategy composite = 3;
  }
}

message SearchQuery {
  string id = 1;
  oneof query {
    LexicalQuery lexical = 2;
    DenseQuery dense = 3;
  }
}

message FilterQuery {
  string id = 1;
  oneof predicate {
    string cel = 2;
    GeoFilter geo = 3;
  }
}

message CompositeSearchStrategy {
  SelectionOperator operator = 1;
  repeated SelectionQuery clauses = 2;
  SelectionScoreStrategy scoring = 3;
}

enum SelectionOperator {
  SELECTION_OPERATOR_UNSPECIFIED = 0;
  SELECTION_OPERATOR_AND = 1;
  SELECTION_OPERATOR_OR = 2;
}

message SelectionScoreStrategy {
  oneof strategy {
    SingleScore single = 1;
    RrfScore rrf = 2;
    ScoreBlend score_blend = 3;
    DecomposedScore decomposed = 4;
    CascadeScore cascade = 5;
  }
}

message BoostQuery {
  SearchQuery query = 1;
  uint32 window = 2;
}

message CompositeScorer {
  CompositeScoreOperation operation = 1;
  repeated ScoreDimension dimensions = 2;
}

message ScoreDimension {
  string id = 1;
  optional double weight = 2;
  ScoreSignal source = 3;
  ScoreNormalization normalization = 4;
  MissingScorePolicy missing = 5;
}

message ScoreSignal {
  oneof source {
    BaseRelevance base = 1;
    string query_relevance_id = 2;
    ScoreStage bounded_value = 3;
  }
}
```

`query_relevance_id` may reference a selection search or a boost query. It may
not reference a filter. IDs are unique across the whole request, which makes
score provenance and validation unambiguous.

## Mapping to the current engine

Increment 1 of the adapter executes exactly these shapes (each delegating
to the route named, bitwise — `tests/query_api.rs` holds it to that):

| Public shape | Route |
|---|---|
| one lexical leaf (+ AND filters, + score stages) | `Bm25Search` |
| one dense leaf (+ AND filters) | `Search` |
| `OR(dense, lexical)` + rrf | `HybridSearch` GLOBAL_RANK |
| `OR(dense, lexical)` + score_blend | `HybridSearch` SCORE_BLEND |
| `OR(dense, lexical)` + decomposed | `HybridSearch` DECOMPOSED |
| cascade(gate = dense) over {dense, lexical} | `HybridSearch` CASCADE |
| one lexical boost on a composite selection, no scorer | `BoostRescore` (bitwise the original path) |
| boosts otherwise (dense, single-leaf shapes, several under a scorer) | `Bm25Rescore` / `VectorRescore` candidate-scoped seams, adapter-side |
| filters only (browse, id order, `after`-floor paging) | `BrowseShard` fan-out |
| browse + `sort` by i64/f64 column (asc/desc) | `BrowseShard` column-keyed heap |
| projections on one lexical leaf | `Bm25Search.projections` (`docs/cel-values.md`) |
| projections on dense/composite/browse | `FetchValues` post-selection fetch, same semantics |
| any scored shape + composite scorer | the route above, then the coordinator-side scorer (`src/ltr.rs`) |
| projections on any other shape; stored-value dimensions | `FetchValues` candidate-scoped fan-out, post-selection |

`selection_k` maps to the hybrid leg depth; the response is the best `k`
of that candidate set (`k <= selection_k` enforced; a `selection_k` that
no candidate-scoped phase uses is refused as a silent no-op). Cascade
requires the composite operator UNSPECIFIED — membership is the gate's,
and neither AND nor OR describes it.

The underlying routes:

| Public concept | Existing implementation |
|---|---|
| One dense search | `Search` |
| One lexical search plus CEL/geo filters | `Bm25Search` |
| One dense search plus CEL/geo filters | `Search` (`docs/vector-filters.md`) |
| Dense plus lexical composite scoring | `HybridSearch` and `FusionMode` |
| One candidate-scoped lexical boost | `BoostRescore` |
| Bounded value functions during lexical selection | `ScoreStage` |
| Named raw leg provenance | `HybridHit.vector_score`, `bm25_score`, and `boost_score` |

The adapter must execute those ordinary paths rather than fork their scoring
logic. Vector-plus-CEL has since acquired its ordinary path
(`docs/vector-filters.md`): `SearchRequest` and `HybridSearchRequest` both
carry `geo_filters` and a CEL `filter`, and every fusion mode applies them to
both legs. The one shape still not represented by the table —
arbitrary nested boolean search — remains unsupported until its
ordinary engine path exists. The public RPC must return `INVALID_ARGUMENT` or
`FAILED_PRECONDITION` for such a shape; compatibility never authorizes a
heuristic substitute.

## Paging

`QueryRequest.cursor` / `QueryResponse.next_cursor` implement
search-after paging (landed with increment 1). The token embeds the
boundary hit's (absolute rank, exact score bits, doc id); the rest of
the request must repeat the original query verbatim. Resumption
re-finds the boundary hit bitwise — search here is deterministic, so
exact equality is the corpus-state check — and refuses with
FAILED_PRECONDITION when the boundary is gone or its score moved.
Documents ingested after a page that rank before the boundary are
skipped, as search-after semantics require.

Depth: a single-leaf query pages by fetching deeper (its order is
depth-independent by the exact top-k prefix property), capped by
`max_k`. A composite — and any query carrying the composite scorer,
whose normalization statistics move with the pool — pages within its
fixed `selection_k` pool: the fusion strategies' orders are
depth-dependent (RRF ranks, blend normalization, the cascade gate), so
the pool is never silently deepened; exhaustion refuses and names
`selection_k`. A full page
always mints `next_cursor`; a short page provably has nothing after it
at the served depth and mints none.

## Response requirements

Every hit needs:

- stable product identity and optional chunk/parent identity, not only a local
  positional slot;
- final score and deterministic rank;
- raw score per matched search and boost query id;
- normalized value and weighted contribution per scorer dimension;
- which boolean clauses matched;
- the strategy and defaults actually selected;
- optional profile timings that do not alter results.

Filters appear in matched-clause provenance but never in score dimensions.
Unknown query ids, duplicate ids, non-finite weights, impossible strategy
combinations, analyzer drift, missing columns, mixed calibration, stale stats,
or incomplete shards are request failures.
