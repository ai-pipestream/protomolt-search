# Relevance tuning

Four layers, applied in this order: score functions inside selection, boosts
over the candidate pool, the composite scorer over named signals, and A/B
comparison across entire requests.

## Score functions

A score-function chain transforms a lexical score using stored column values.
Each stage must be monotone non-decreasing in the incoming score and must ship a
bound rule valid over the column's entire domain, absence included. That is the
admission bar, because block-max pruning lifts its bounds through the chain.

| Operation | Effect | Rules |
|---|---|---|
| `MULT_EXP_DECAY` | `score *= exp(-abs(x - origin) / scale)` | `scale` finite and above 0 |
| `MULT_LOG` | `score *= 1 + weight * ln(1 + max(x, 0))` | `weight >= 0`; a negative weight could make the factor negative |
| `ADD_LINEAR` | `score += weight * x` | any finite weight |
| `MULT_GEO_DECAY_HAVERSINE` | `score *= exp(-great_circle_meters / scale)` | `origin_lat`, `origin_lon`, `scale` in meters above 0 |
| `MULT_GEO_DECAY_MANHATTAN` | the same with local Manhattan distance | city-scale, not a globe-scale distance |

`SCORE_OP_UNSPECIFIED` is rejected: a stage must say what it does.

`column` names an f64 or i64 column; with `key` set it names a map-numeric
column and reads that key; for the geo operations it names a geo-point column.
A document with no value for the column passes through unchanged. That is exact,
not degraded, and it is why a shard lacking the column entirely is still
correct. A column that **no** shard knows is rejected by the coordinator, because
a misspelled chain would otherwise be a silent no-op.

The chain runs in request-list order on every shard, because multiplication and
addition are not associative in floating point, and the coordinator sends the
same list everywhere. Hits, `min_score`, and `kth_best` are all on the final
chained scale.

One caveat with `ADD_LINEAR`: negative contributions can push scores to or below
zero. Correctness is unaffected, but the wire convention that `min_score = 0`
means unseeded makes cutoff seeding stop working for non-positive values. A
chain that keeps scores positive keeps that optimization.

Score stages are served on the flat lexical route (setting them together with
fused `fields` is rejected), on the single lexical leaf of `Query`, and on
candidate-scoped lexical clauses in a boolean tree.

## Boosts

`QueryRequest.boosts` runs a scoring query against the selection's top
`selection_k` documents and no others. A boost cannot add a document. Its
relevance becomes another named signal under its query id.

A boost can be lexical or dense, on any ranked selection. A browse has no base
order to boost and is rejected.

Without a scorer, one boost is served, and its `base_weight` and
`boost_weight` reorder is the combination: `base_weight * base + boost_weight *
boost`. `window` states how many top candidates are rescored; 0 means all
`selection_k`. Both weights default to 1.0. Several boosts require the composite
scorer.

With a scorer present a boost is signal-only: `window`, `base_weight`, and
`boost_weight` must be left unset, because each would be a silent no-op, and
every candidate in the pool is ranked. The scorer owns combination.

A lexical boost inherits the selection's lexical leaf's analysis when there is
one: term identity must match the index the leaf searched, so a spec of its own
is rejected there. On a dense-only selection it brings its own.

A boost is what makes `selection_k` meaningful on a single-leaf shape: it is the
pool the boost rescores, and a boosted query pages within that pool.

## The composite scorer

`QueryRequest.scorer` combines named signals into the final order over the fixed
`selection_k` pool, after selection and after boost signals are computed, so it
cannot invalidate a first-stage pruning certificate. It requires a ranked
selection.

`operation` is required; there is no default, because defaulting silently would
misreport what ran. Over the **active** dimensions:

| Operation | Formula |
|---|---|
| `WEIGHTED_SUM` | `sum(w * n)`, the one operation admitting a negative weight |
| `WEIGHTED_MEAN` | `sum(w * n) / sum(w)` |
| `MAXIMUM` | `max(w * n)` |
| `PRODUCT` | `product(w * n)` |
| `GEOMETRIC_MEAN` | over positive values, weights renormalized over those |
| `HARMONIC_MEAN` | over positive values; the most AND-flavored, dominated by the weakest dimension |

A dimension is active when it is not disabled, not skipped by its missing
policy, and not skipped by the positive-value rule of the geometric and harmonic
operations. A document with no active dimension scores 0 under every operation.

Each `ScoreDimension` has:

- `id`: non-empty and unique among the scorer's dimensions. It may equal the
  query id it sources; the two provenance surfaces are separate.
- `weight`: absent means 1.0. An explicit 0 is an explicit disable: the
  dimension is still evaluated and reported but excluded from the combination.
  Finite; negative is admitted only under `WEIGHTED_SUM`.
- `source`: the first-stage base score, a search or boost query's raw relevance
  by id, or a bounded stored-value score function. A filter is not a source: it
  contributes no relevance.
- `normalization`: over the pool's present raw values. Unset resolves to
  `MIN_MAX`; `NONE` is the explicit raw identity. A degenerate pool (every
  present value equal, or one present value) maps to 1.0 under `MIN_MAX` and 0.0
  under `Z_SCORE`.
- `missing`: unset resolves to `ZERO`: a normalized zero with the weight still
  active, so under `WEIGHTED_MEAN` the weight remains in the denominator and
  penalizes the document proportionally. `SKIP` removes the weight from every
  sum and renormalization. `ERROR` rejects the entire request with
  FAILED_PRECONDITION naming the document and the dimension, because the client
  declared that every candidate has this signal. Policies apply to disabled
  dimensions too, since those are still evaluated for reporting.

At least one dimension, and at least one that is not disabled: an all-disabled
scorer would order every document at 0 with no sign of it.

Every hit reports `dimensions`, aligned with the request in order, every
dimension included: the raw value (absent when, and only when, the signal was missing),
the normalized value, this dimension's term in the combination, and whether it
was skipped. Those are the exact f64 terms the combination consumed, so a client
can recompute the final score without reimplementing the server's arithmetic.
`QueryHit.score` is their combination cast to f32. Raw per-signal scores also
ride `QueryHit.signals`; the scales are per signal and the contract does not
pretend they share one.

Scorer arithmetic is f64 in dimension list order. Final order is composite score
descending, then doc id ascending. Because normalization statistics move with the
pool, a ranked query pages inside its fixed `selection_k` pool.

## A/B variants and interleaving

`SearchService.VariantSearch` runs two or more complete query configurations over
the same corpus in one request and reports how far their rankings differ.

This belongs in the engine because the comparison is only meaningful when the
runs are otherwise identical. Search here is deterministic and layout-invariant,
so two variants of one query differ only by the variant. There is no recall
noise to average out, and a one-query diff is already a real observation instead
of a sample. Each variant is a complete ordinary request and takes the
ordinary code path, so what is tested is what is served.

- At least two variants. The first is the reference for every diff.
- `label` non-empty and unique; the diffs are unreadable when two variants share a
  name.
- An arm's own `k` and `request_id` are ignored. Depth is shared across the
  request, because rankings truncated at different depths are not comparable.
- `rbo_p` defaults to 0.9, which weights roughly the first 10 results.
- Variants run one after another, not concurrently, so each `elapsed_ms` is that
  configuration's own cost instead of its share of a contended fleet.
- Results are ids and scores only. Re-issue the winning variant as an ordinary
  request for the full hit detail.

The diffs measure difference, not quality. No number here states which variant is
better; that needs labels, or the interleaving's selections.

- `overlap` and `overlap_fraction`: the shared set; 1.0 means the same set in
  any order.
- `kendall_tau`: Kendall tau-b over the union of both rankings. Documents one
  arm omitted are ranked past the end of its list and not dropped, so
  omission counts as the difference it is.
- `rbo`: truncated rank-biased overlap, top-weighted, so a swap at rank 1 costs
  far more than one at rank 50. Truncated means it assumes no more about the
  unseen tail and is therefore a lower bound.
- `score_regret`: mean reference score given up per compared rank, both branches
  read on the reference's yardstick. It measures the set and not the order,
  so any reordering of the same documents cancels to 0. Read it with
  `kendall_tau`: a large tau change at zero regret is the near-duplicate shuffle
  signature, while real regret means the variant reached for lower-scoring
  documents. Its sign is meaningful only when `regret_unscored` is 0.
- `regret_counted` and `regret_unscored`: variant documents the reference did not
  score are reported and not averaged in, because the measure cannot tell
  whether they are better. A high count means the variants diverged past what regret
  can judge.
- `top1_flipped`: whether the variants differ about the single best result, the
  difference a user is most likely to notice and the one most easily invisible
  in an aggregate.

Set `interleave` to merge two variants into the one list a user is shown. Team-draft
interleaving alternates which arm contributes the next result, so both get equal
exposure within a single query and a click is evidence about the ranking instead
of about the position. A document both variants returned is contributed once and
credits no variant beyond that: agreement is no evidence. It requires
two variants; an "interleaving" of three variants would be a different
algorithm wearing the same name. `interleave_seed` of 0 derives a stable seed
from the first variant's query text, so a re-run of the same query is
reproducible while still varying across queries.

## Dense execution policy

`DenseQuery.execution_mode` chooses the candidate traversal, separately from how
candidates get scores.

- Unset preserves exact traversal, so a later backend change cannot silently
  weaken an existing request's quality contract.
- `EXACT` states so explicitly and is rejected if the live provider cannot prove
  an exhaustive traversal and completion certificate.
- `ANN` accepts a provider's configured approximate traversal. It does not claim
  corpus-wide top-k.
- `AUTO` lets the coordinator choose, and it chooses only on evidence.

An exhaustive provider resolves `AUTO` to `EXACT`, bit for bit the same response
as `EXACT`, and no policy is consulted. A configured approximate provider
resolves to `ANN` only through a generation-bound policy installed with
`--dense-execution-policy`. The policy's identity (provider kind, scoring
fingerprint, corpus generation, corpus row count, dimensions) must match the
live cluster field by field, and the request must match a recorded point on `k`,
the filter's live selectivity, and the candidate depth. The first
mismatch is rejected naming the field and both values. No policy, a mismatched
identity, or a key no measurement covered is rejected by name. No value is interpolated.

`k = 0` is rejected under a policy: a recorded point needs a number, not the
coordinator's default.

Every selection with a dense leaf returns `dense_execution` whether or not you
you requested a profile, because it is semantic provenance and not timing data:
the requested and resolved modes, provider kind, quality contract, scoring
fingerprint, whether every provider advertised and completed an exhaustive
traversal, a stable planner reason, and, when a policy was consulted, its id and
fingerprint, the point matched, the live filter selectivity, and the candidate
depth the providers were given.

Mixed provider kinds, scoring spaces, dimensions, quality contracts, or
completion capabilities across shards are rejected at preflight, so an
approximate response cannot be mistaken for a corpus-wide exact one.

Today the provider in the box is exhaustive, so `AUTO` resolves to `EXACT` in
production.

## Dense quality profile

`DENSE_SCORE_MODE_FP32_RERANK` selects `selection_k` candidates with the
provider, then replaces their scores with product-owned FP32 dot products over
the original vectors and reorders that fixed pool. It is exact within the pool.
It is not a corpus-wide exact claim unless the pool covers the corpus, so
candidate depth is the recall knob instead of a hidden expansion factor.

`--dense-quality-profile` installs a table built by measurement that answers
"how deep must the pool be for this recall target". It is strict TOML with unknown keys
rejected, and its fingerprint is the SHA-256 of the file. It records the
embedding model, corpus generation and row count, dimensions, provider kind, and
scoring fingerprint; a set of measurement rows with mean and lowest per-query recall
and per-phase median latency; a set of points mapping `(k, target)` to a
candidate depth; and a default target.

Recall is in parts per million so common targets are exact on the wire: 950000,
990000, 999000, 1000000.

Every point must be justified by a measurement row at the same `k` and depth,
and that row's **lowest per-query** recall must meet the target. A point at an
unmeasured depth, or one promising 100% where the weakest query recovered
99.98%, is rejected at load. The mean is recorded but does not decide: a point
promises every reserved query, not the average one.

How the depth resolves:

| Request | Depth |
|---|---|
| `quality` set, `selection_k = 0` | the profile point |
| `quality` set and `selection_k` set | rejected as competing depth authorities |
| `AUTO` + FP32, no `quality`, `selection_k = 0`, profile with a default | the point at the default target |
| `AUTO` + FP32, no `quality`, `selection_k = 0`, no profile or no default | rejected by name |
| `AUTO` + FP32 with `selection_k` set | your depth |
| `EXACT` or unset + FP32, `selection_k = 0` | `k` |

`DenseQualityPolicy.max_candidates` is an optional request-local upper bound applied
after resolution: a point above it is rejected, not clamped.
`required_profile_fingerprint`, when set, must equal the loaded profile's
fingerprint. `QueryResponse.dense_quality` records which recorded contract chose
the depth.

What the profile does not promise:

- Recall was taken against the exhaustive FP32 order on the
  generation the run covered, for the reserved queries the measurement table
  used. A different query distribution can do less well.
- A tombstone invalidates it: the FP32 rows and the quantized order no longer
  describe the same live set. Compact, then remeasure.
- A `(k, target)` the profile did not measure is rejected, not interpolated.
- The table was built unfiltered. A policy on a filtered dense leaf resolves
  the same depth, and the measurement states no result about recall inside the
  filtered set.
- It is bound to one generation. A rebuild, a reshard changing the row count or
  topology generation, or a recalibration changing the scoring fingerprint
  rejects the profile until it is remeasured.

`--max-rerank-mib` (256 MiB by default) bounds the pool's row bytes before
fan-out, and `--rerank-parallel` bounds the worker pool. Each dot product keeps
scalar accumulation order, so parallel and serial score bits match. The query
profile reports rows, logical bytes, mapped pages, and tasks, which is how you
see that a resolved depth did not silently fall back to `k`.

Reference: `docs/score-functions.md`, `docs/query-api.md`,
`docs/dense-quality-profile.md`.
