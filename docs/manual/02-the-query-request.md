# The query request

`SearchService.Query` takes a `QueryRequest`. It has three parts that are easy
to confuse, so the contract keeps them separate:

1. **Selection** determines which documents are candidates and produces the base
   relevance signals.
2. **Boosts** rescore that fixed candidate set. A boost can reorder, but it
   cannot add or remove a document.
3. **The scorer** combines named signals into the final order.

## Selection shapes

`QueryRequest.selection` is a tree of four node kinds.

**A lexical leaf** (`SearchQuery.lexical`) is BM25 over analyzed text. It has
the query text and the analysis options, and optionally a phrase constraint,
term prefixes, synonym rules, and a score-function chain.

**A dense leaf** (`SearchQuery.dense`) is vector relevance. It has the query
vector, a score mode, and an execution mode.

**A filter leaf** (`FilterQuery`) determines membership and produces no relevance
score. On its own, or ANDed with other filters, it is a **browse**: the matching
documents in global id order, with no scoring. Under OR a filter is rejected,
because a document admitted with no relevance has no defensible position in a
ranked list.

**A composite** (`CompositeSearchStrategy`) is AND or OR over child nodes plus
an explicit strategy saying how the scoring leaf nodes establish the base order.
The AND/OR is about membership; the strategy is about score combination. The two
are stated separately on purpose.

**A boolean tree** (`BooleanQuery`) is the recursive form: `must`, `should`,
`must_not`, and `minimum_should_match`, nestable to 64 levels. Every clause may
itself be a search, a filter, or another boolean node. Membership is resolved
with exact shard bitmaps, and only the documents that pass get scores. Relevance
is the sum of the matching positive scoring clauses unless a scorer replaces the
combination. `minimum_should_match` of 0 resolves to 1 when there are SHOULD
clauses and no MUST clause, and to 0 otherwise; a value above the SHOULD count
is rejected.

## Hybrid composites and their fusion modes

A composite of one dense leaf and one lexical leaf under OR is the hybrid shape.
`SelectionScoreStrategy` picks how the two combine:

- **`rrf`** (global-rank reciprocal rank fusion). Each side is merged across
  shards by raw score into a global ranking, then fused once with
  `w / (rrf_k + rank)`. `rrf_k` defaults to 60. For `k` at or below the per-side
  depth this returns what a single-machine index would.
- **`score_blend`**. Each side is truncated tie-complete to the per-side depth,
  its retained scores are normalized (`MIN_MAX` by default, `Z_SCORE`, or
  `NONE`), and the normalized values combine (`ARITHMETIC` by default,
  `GEOMETRIC`, or `HARMONIC`). Score gaps stand, where RRF flattens every gap
  to the distance between adjacent ranks.
- **`decomposed`**. The exact top-k over the raw weighted sum
  `dense_weight * v(d) + lexical_weight * b(d)`, with no truncation of either
  side. Both weights must be positive. Every document in the corpus is accounted
  for, including documents that are mediocre on each side but strong combined.
- **`cascade`**. The gate side selects its tie-complete top `selection_k`, and
  the other side reranks that pool. `CascadeScore.gate_id` names the gate, which
  must be the dense leaf. The composite's operator must be left unspecified,
  because membership belongs to the gate, and AND and OR both describe
  something else.
  Note the trade: a keyword-strong but vector-weak document does not enter the
  pool and does not appear.
- **`single`**. One scoring leaf; its raw relevance is the order.

Per-side weights follow one rule everywhere: absent means 1.0, an explicit 0
disables that side. Disabling both is rejected.

## k and selection_k

`k` is how many hits come back. `selection_k` is how deep the selection phase
goes: the per-side depth for the composites, the gate depth for cascade, the
pool a boost or a scorer works over. `selection_k` defaults to `k`, and
`k <= selection_k <= max_k` is enforced. `max_k` is a coordinator setting
(`--max-k`, default 10000); a `k` above it is an error, not a clamp.

A `selection_k` that no phase would use is rejected as a silent no-op.

## Paging

Set `QueryRequest.cursor` to a previous response's `next_cursor`, and repeat the
rest of the request unchanged. The token embeds the boundary hit's rank, exact
score bits, and doc id. Resumption re-finds that hit bit for bit; if it is
missing or its score moved, the request fails with FAILED_PRECONDITION and you
start again from the first page. Documents ingested after a page that would rank
before the boundary are skipped, which is what search-after paging means.

How depth grows differs by shape. A single leaf pages by fetching deeper, capped
by `max_k`, because its order does not depend on depth. A composite, or any
query with a scorer, pages inside its fixed `selection_k` pool, because RRF
ranks, blend normalization, and the cascade gate all move with the pool. An
exhausted pool gives an error naming `selection_k` as the knob to raise.

A full page always returns a `next_cursor`. A short page returns none, because
no hit can follow it at the served depth. A token issued by one shape is
rejected on another.

## The streaming query

`SearchService.QueryStream` wraps the same `QueryRequest` and adds a query-wide
`timeout_ms`. It sends **complete replacement snapshots**, not patches: each
`QueryStreamRevision` has a strictly increasing revision number and a full
ordered hit list, so a slow client can skip intermediate revisions and apply the
highest one it has.

The last message is one `QueryStreamCompletion`. Its `response` field is
the same `QueryResponse` the unary route would return, and it is usable only
when `completed` is true. A timeout, a shard failure, analyzer drift, an
incomplete provider certificate, or client cancellation produces
`completed = false` with a gRPC code and message, so a short result is not
mistaken for a finished one. Clients must not infer completeness from
end-of-stream.

Reference: `docs/query-api.md`.
