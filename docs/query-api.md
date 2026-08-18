# Public query contract

Status: design contract for the future public `Query` RPC. The existing
`Search`, `Bm25Search`, and `HybridSearch` RPCs remain the implemented surface
until this contract can delegate to them without weakening their semantics.

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
    Select -->|"candidate_k candidates"| Boost["Candidate-scoped boost queries"]
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
selection's top `candidate_k` documents, never the corpus. Its query relevance
becomes another named signal. The selected candidate set is immutable during
this phase.

`candidate_k` and output `k` are deliberately separate:

\[
k \leq candidate\_k
\]

The final response is the best `k` documents under the post-boost scorer from
that candidate set. It is not represented as the global top-k of the boosted
formula unless the selection strategy itself included the boost signal and
proved that stronger claim.

This is the honest form of the existing rescore-window behavior. A caller that
needs more recall under a strong boost increases `candidate_k`; the server does
not silently over-fetch by an undocumented factor.

## Composite scorer

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
  uint32 candidate_k = 3;
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
  CandidateScoreStrategy scoring = 3;
}

enum SelectionOperator {
  SELECTION_OPERATOR_UNSPECIFIED = 0;
  SELECTION_OPERATOR_AND = 1;
  SELECTION_OPERATOR_OR = 2;
}

message CandidateScoreStrategy {
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

The current implementation proves these lower-level routes:

| Public concept | Existing implementation |
|---|---|
| One dense search | `Search` |
| One lexical search plus CEL/geo filters | `Bm25Search` |
| Dense plus lexical composite scoring | `HybridSearch` and `FusionMode` |
| One candidate-scoped lexical boost | `BoostRescore` |
| Bounded value functions during lexical selection | `ScoreStage` |
| Named raw leg provenance | `HybridHit.vector_score`, `bm25_score`, and `boost_score` |

The adapter must execute those ordinary paths rather than fork their scoring
logic. Shapes not represented by the table, including vector-plus-CEL,
filter-only browse, arbitrary nested boolean search, multiple boost queries,
and the generic named-dimension response, remain unsupported until their
ordinary engine paths exist. The public RPC must return `INVALID_ARGUMENT` or
`FAILED_PRECONDITION` for such a shape; compatibility never authorizes a
heuristic substitute.

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
