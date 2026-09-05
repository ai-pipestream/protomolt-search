# The explain tree

Landed 2026-09-05 (H6). `QueryRequest.explain = true` hands each hit an
`Explanation` tree: the arithmetic that produced its score, leaf by leaf
and term by term. Results, order, and paging are unchanged with the flag
on; the response is larger and the lexical shards do one extra walk.

```proto
message Explanation {
  double value = 1;
  string description = 2;
  repeated Explanation details = 3;
}
```

The tree keeps one rule at every node: the description states what the
value is and how it follows from the children, and the root's value is
the hit's served score (`QueryHit.score`, as an f64 of the f32). The
inner arithmetic is f64, as the engine's is; the root says so and the
f32 rounding is the only gap a reader will find.

Nothing in the tree is re-scored. Every number is one the engine
computed on the path that produced the hit — the shard's per-term
inputs, the fused hit's ranks and raw scores, the scorer's dimension
reports — assembled after the fact. That is what makes the claim "same
hits, bitwise" testable rather than asserted (`tests/explain.rs`).

## What each shape reports

**A single lexical leaf.** The shard carries back, for each returned
document, the (field, term) pairs it contains with their inputs
(`Bm25Hit.explain`, a `Bm25Explain`). The tree under the leaf's root:

```
lexical leaf "lex": BM25 relevance, served as f32 of the arithmetic below
└─ score stage 0 on column year: input 3 gives contribution 0.375 ...   (flat route only)
   └─ BM25 sum over 2 (field, term) contributions
      ├─ term "zebra" in field body: weight * idf * tf_norm
      │  ├─ tf_norm = tf * (k1 + 1) / (tf + k1 * (1 - b + b * dl / avgdl)) with tf=2, dl=4, ...
      │  ├─ idf = ln(1 + (N - df + 0.5) / (df + 0.5)) with N=8, df=4
      │  └─ field weight
      └─ expansions of prefix "zeb" in field body: sum of the expansions' contributions
         └─ term "zebras" in field body: weight * idf * tf_norm
            └─ ...
```

A term node's value is `weight * idf * tf_norm` in that operand order,
which is the scorers' order, so it is bit-equal to the shard's own
product. Prefix expansions (`docs/prefix-terms.md`) and synonym
expansions (`docs/synonyms.md`) are grouped under a node that names the
prefix or the source term; a term the user typed is never filed under a
rule. The sum node's value is the pre-stage BM25 sum recomposed in
accumulation order (field legs in request order, terms in leg order),
and on the fused route with a phrase leg it is the base fields' sum plus
the phrase group's maximum, each phrase term marked as such. Score
stages (`docs/score-functions.md`) wrap the sum one node per stage in
evaluation order: the column value read (a distance in meters for the
geo ops), the factor or addend applied, and the score after the stage; a
document with no value for the column shows the stage as identity.

**A single dense leaf.** One leaf: the provider's native similarity for
the stored vector. With `DENSE_SCORE_MODE_FP32_RERANK` the root is the
exact FP32 dot product and its one child is the candidate score that
selected the document for the rerank, marked as not part of the served
score.

**A composite selection.** One node per leg with the fusion's
arithmetic (`docs/hybrid-retrieval.md`):

- reciprocal rank fusion: `weight / (rrf_k + rank)` per leg the document
  is in, the leg's raw score beneath it as the value that fixed the rank;
  the root is their sum;
- score blend: `weight * normalized` per leg, the normalized value and
  the raw score beneath it, the normalization named (min-max, z-score,
  identity); the root states the combination (arithmetic over the total
  weight, geometric, harmonic). The normalized values are the ones the
  combination consumed: the hybrid route reports them on
  `HybridHit.vector_normalized` / `bm25_normalized` for the blend mode;
- decomposed: `weight * raw` per leg, the root their sum;
- cascade: the root is the rerank leg's BM25 score (the served score),
  with the phase-1 dense score beside it as the gate that admitted the
  document to the pool, not an addend.

A legacy hybrid boost (`BoostRescore`) appears as a leaf carrying the
boost's BM25 score and the ordering key `base_weight * score +
boost_weight * boost` it produced; the served score is the fused score
and the leaf says so.

Composite legs are reported at leg granularity. The lexical leg's
term-level breakdown is served on the single lexical leaf, where the
shard's breakdown is on the path; a composite's lexical leg travels
through the leg protocols, which carry scores and ranks only.

**A boolean root.** The root is the sum of the positive scoring clauses;
each clause is a leaf under its id with its relevance for the document.
Filter clauses contribute membership and appear in `matched`, never in
the tree.

**Boosts and the composite scorer.** A request boost without a scorer
reorders its window and leaves the served score alone; the root keeps
the selection's value and gains one leaf per boost signal stating the
window's ordering key. With a composite scorer (`docs/query-api.md`
"Composite scorer") the root becomes the operation over its dimension
nodes — each dimension's contribution, with the raw signal and the
normalized value beneath it, a skipped or missing dimension marked — and
the selection tree is kept under the root as the provenance of the
selection signal, labeled as not a term of the operation.

## Refusals

The tree explains a score. A shape that computes none refuses by name
rather than returning hits without trees:

- a browse (a filter-only or empty selection);
- a column sort over a lexical leaf, which walks the leaf's exact
  membership and computes no relevance;
- `QueryStream`: a stream's revisions carry candidate hits without
  trees, and a tree over a revision a later one replaces would explain
  a score that was never served. The unary route serves it.

A lexical hit that arrives at the coordinator without its breakdown is
an internal error, not a hit without a tree.

## Cost

The flag adds one pass per lexical shard over the returned hits only:
per query term, the impact cursor (or the posting stream) advances
through the sorted hit ids once, the same walk the cascade rescore
uses. The fused and blend paths add nothing but the copy of numbers
they already held. The response grows by the tree; a term node is about
five lines of text per (field, term) pair per hit.

## Wire

- `QueryRequest.explain` (15), `QueryHit.explain` (11), `Explanation`.
- `Bm25SearchRequest.explain` (21) and `Bm25QueryRequest.explain` (22)
  ask the lexical routes for the breakdown; `Bm25Hit.explain` (6) is a
  `Bm25Explain` of `Bm25TermExplain` rows and `ScoreStageExplain` rows.
- `HybridHit.vector_normalized` (9) and `bm25_normalized` (10), set for
  the blend mode.
