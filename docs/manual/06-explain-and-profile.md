# Explain and profile

Two flags on `QueryRequest` open up what happened. Both leave the hits and their
order unchanged, bit for bit.

## explain

`explain = true` gives every hit an `Explanation` tree:

```proto
message Explanation {
  double value = 1;
  string description = 2;
  repeated Explanation details = 3;
}
```

One rule at every node: the description states what the value is and how it
follows from the child nodes. The top node's value is the served score
(`QueryHit.score`, as an f64 of the f32). Inner arithmetic is f64, and the f32
rounding at the end is the only difference a reader will find.

No score is recomputed to build the tree. Every number is one the engine already
computed on the path that produced the hit, assembled afterwards.

What each shape reports:

- **A single lexical leaf.** Every (field, term) pair the document contains,
  with `tf`, `doc_length`, `avgdl`, `k1`, `b`, `tf_norm`, `doc_count`, `df`,
  `idf`, the field weight, and the product `weight * idf * tf_norm`. Prefix and
  synonym expansions are grouped under a node naming the prefix or the source
  term, so a term you typed is not placed under a rule. Above the sum, one node
  per score stage in evaluation order: the column value read, the factor or
  addend applied, and the score after it. A document with no value for a stage's
  column shows that stage as identity.
- **A single dense leaf.** The provider's native similarity. Under
  `DENSE_SCORE_MODE_FP32_RERANK` the top node is the exact FP32 dot product,
  with the candidate score that selected the document beneath it, marked as not
  part of the served score.
- **A composite.** One node per side with the fusion arithmetic: `weight /
  (rrf_k + rank)` for RRF, `weight * normalized` for the blend with the
  normalization named, `weight * raw` for decomposed, and for cascade the rerank
  score as the top node with the gate score next to it and not added. Each
  branch is reported at branch level, since the composite protocols include
  scores and ranks and not per-term inputs.
- **A boolean tree.** The top node is the sum of the positive scoring clauses,
  one leaf node per clause id. Filter clauses appear in `matched` and not in the
  tree.
- **With a scorer.** The top node becomes the operation over its dimension
  nodes, each with its contribution, raw signal, and normalized value, and
  skipped or missing dimensions marked. The selection tree is kept underneath as
  provenance, labeled as not a term of the operation.

Explain is rejected by name on shapes that compute no score: a browse, a column
sort over a lexical leaf, and `QueryStream` (a revision's score may be replaced
by a later revision, so a tree over it would explain a score that was not
served). Use the unary route for those.

Cost: one extra pass per lexical shard over the returned hits, plus the size of
the trees in the response.

## profile

`profile = true` fills `QueryResponse.profile` with wall-clock phase timings in
milliseconds:

- `selection_ms`: the delegated selection route (analysis, statistics, fan-out,
  fusion).
- `boost_ms`: the candidate-scoped boost rescores.
- `values_ms`: the stored-value fetch for scorer dimensions.
- `scorer_ms`: the composite scorer arithmetic.
- `projection_ms`: the post-selection projection fetch for the page.
- `rerank_ms`, plus `rerank_rows`, `rerank_logical_bytes`, `rerank_pages`, and
  `rerank_tasks`, the FP32 rerank's time and physical work.
- `collapse_ms`: key resolution and grouping.
- `total_ms`: the entire call.

The same request with the flag off returns identical hits.

`HybridSearchRequest.debug` is the equivalent on the hybrid route: fusion mode,
effective per-side depth, analyzed terms, coordinator phase timings, and a
per-shard breakdown with wall time, hits per side, and the vector scan counters.

Reference: `docs/explain.md`.
