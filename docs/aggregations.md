# Aggregations: exact folds over the filtered corpus

Status: implemented (increment 1, 2026-08-27) — COUNT / SUM / MIN /
MAX / MEAN / VARIANCE / STDDEV of one CEL value expression over the
filter-admitted document set, per-shard partials merged exactly.
Group-by-facet, histograms, and exact percentiles are the next
increments (`docs/plans/roadmap-2026-08.md`).

`SearchService.Aggregate` is the analytics half of the value layer:
the same compiled expression language that filters
(`docs/cel-filters.md`), scores (`docs/score-functions.md`), and
projects (`docs/cel-values.md`) now also folds. A request carries a
CEL filter (empty = the whole corpus), geo filters, and up to 32 named
aggregations, each `(name, expression, op)` where the expression is
exactly the projection dialect — arithmetic, the conditional layer,
`math.*` / `engine.*` functions, map reads, everything.

## 1. The contract: exact, deterministic, loud

Search engines traditionally treat aggregations as estimates
(t-digest percentiles, sketch cardinalities) and float sums as
whatever the reduction order produced. This engine's stance is the
one it takes everywhere else — **exactness is the argument**:

- **Int sums are exact.** Shards accumulate i128; the coordinator
  merges in i128. A total outside i64 REFUSES naming `double()` as
  the fix — never a silent wrap, never a quiet float. (Summing an
  epoch-micros column over a large corpus genuinely overflows i64;
  the refusal is reachable, and tested.)
- **Double sums are Neumaier-compensated**, folded in doc order per
  shard; the coordinator folds each shard's (sum, compensation) pair
  — in that order — with its own running compensation.
- **Mean and variance use Welford per shard** (doc order) **and the
  Chan parallel merge across shards** (shard order). VARIANCE is the
  population variance M2/n; STDDEV its square root.
- **Deterministic bit-for-bit.** Shard responses merge IN SHARD
  ORDER, never arrival order, so the same request over the same index
  answers the same bits every run. The reference algorithms are
  replicated verbatim in `tests/aggregate.rs` and every assertion is
  bitwise equality, not epsilon.
- **MIN/MAX are type-preserving**; a double NaN propagates, the same
  rule `math.least`/`math.greatest` take.

## 2. Absence and typing

Kleene, as everywhere: a document whose expression evaluates absent
is SKIPPED — counted in `matched` (it passed the filter) but not in
`present`, and it feeds no fold. Absence is never a fabricated zero;
a selection holding no values answers `present = 0` and an unset
value (COUNT answers the honest 0).

Typing refuses by name, naming the aggregation:

- COUNT serves every expression type — a bare facet read counts
  documents carrying the value.
- SUM/MIN/MAX serve int and double expressions in their own type.
- MEAN/VARIANCE/STDDEV serve doubles only; an int expression refuses
  naming `double()`.
- A boolean expression aggregates nowhere: filter on it and read
  `matched`.
- Shards vote the expression's resolved type; shards disagreeing
  (column families diverging across the fleet) is a
  failed-precondition refusal, not a coerced merge.

The filter rules carry over exactly: geo and filter-leaf typo flags,
and expression column leaves under the projection contract — a column
NO shard knows refuses by name; a column one shard lacks is absent
there and exact.

## 3. The shard pass

One pass in doc order per shard (`NodeService.AggregateShard`): the
resolved filter gates admission (the same `DocFilter` every route
uses), and each admitted document evaluates every aggregation's
resolved expression once. All statistics for the resolved type fold
in that single pass — count, i128 or compensated sum, extrema,
Welford moments — a handful of flops per value; the partial carries
them all and the coordinator reads what the op needs. The walk is
exhaustive by construction, so the exactness certificate is trivial,
the same argument sorted browse makes.

## 4. What this is not (yet)

No group-by, no histograms, no percentiles — next increments. No
text-scoped aggregation: the scope is the FILTER's admitted set, not
a BM25 result set (facet counts already serve the search routes).
And deliberately no approximate anything: when an exact answer needs
a different request shape, the refusal says which.
