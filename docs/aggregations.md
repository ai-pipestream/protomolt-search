# Aggregations: exact folds over the filtered corpus

Status: complete (2026-08-27) — COUNT / SUM / MIN / MAX / MEAN /
VARIANCE / STDDEV of CEL value expressions over the filter-admitted
document set, per-shard partials merged exactly; group-by-facet
(every aggregation folded per facet value); fixed-interval
histograms; and EXACT percentiles (nearest-rank order statistics via
count-below binary search, never a sketch). Extended 2026-09-05 with
the same folds over a query's candidate pool (§8), an exact
CARDINALITY (§9), and calendar date histograms (§10).

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

## 4. Group-by-facet

`group_by` names one facet column: every aggregation folds once
fleet-wide (`results`) and once per distinct facet value (`groups`),
with the SAME exactness contract — per-shard, per-group partials
merged in shard order, groups joined across shards by value and
returned ascending by value (deterministic, like everything else
here). Per group: `matched` (admitted documents carrying the value)
and the full result list in request order.

Absence stays honest: an admitted document WITHOUT the group_by value
joins no group — absence is not a value — and is counted in
`ungrouped`; the fleet-wide totals still cover it. A shard whose
tables lack the column groups nothing and counts all its admitted
documents ungrouped; a column NO shard resolves as a facet refuses by
name.

Cardinality is capped (`max_groups`, default 1000) and the cap
REFUSES loudly — never a silent top-N truncation, because a top-N cut
would need an ordering the request did not state. The refusal names
the cap and the fixes: tighten the filter or raise it.

## 5. Histograms

`histograms` carries up to 8 `(name, expression, interval)` specs
over double expressions (int converts with `double()`; the names
share one namespace with the aggregations). Bucketing is ES-shaped
and exact: bucket index = `floor(value / interval)`, the bucket's
inclusive lower bound is `index * interval`, and only OCCUPIED
buckets return, ascending — sparse, no gap filling. Shards fold
sparse (index, count) maps; the coordinator sums counts by index.

A present value no bucket can hold honestly — NaN, an infinity, or a
bucket index outside i64 (an interval too fine for the magnitude) —
is counted in `unbucketable`, reported, never silently dropped.
Bucket cardinality is capped per histogram (`max_buckets`, default
1024), refusing loudly with the honest fixes: a coarser interval or a
tighter filter.

## 6. Exact percentiles

The headline. Every mainstream search engine answers percentiles with
a sketch (t-digest, HDR) and calls the error acceptable. This engine
answers the EXACT nearest-rank order statistic — for percentile p
over n present values, the k-th smallest with k = max(1, ceil(p/100
n)) — a value some admitted document actually holds, never an
interpolation, never an estimate.

The algorithm is a coordinator-driven binary search over the
ORDER-BITS domain (the same order-preserving u64 keys sorted browse
uses: offset-binary for i64, sign-flip for f64), which makes
convergence exact and bounded: at most 64 count-below rounds close
every window, no epsilon anywhere. Phase 1 rides the ordinary
AggregateShard fan-out (per-expression type vote, rankable count,
global min/max bits); then every requested (spec, percentile) target
converges SIMULTANEOUSLY — one `QuantileCounts` round per iteration
carries all still-open thresholds, each shard answers them in a
single admitted-set pass with each expression evaluated once per
document. Cost is O(rounds) admitted-set scans, bounded by 64 total
regardless of how many percentiles are asked: exactness paid in
scans, not in memory or error.

Typing and absence: int expressions answer ints, double expressions
doubles (the type vote refuses cross-shard divergence); p = 0 is the
minimum, p = 100 the maximum, p = 50 the lower median on even
counts, and every answer reports its rank k. A computed NaN is
`unrankable` — reported, excluded from ranking, never dropped
silently; infinities rank at the ends. An empty selection answers
rank 0 with no value. Percentiles are fleet-wide only (they ignore
`group_by`); up to 8 specs of up to 16 percentiles each, one name
namespace with everything else.

## 7. What this is not (yet)

No per-group histograms or per-group percentiles, no nested
grouping. No aggregation over a sorted lexical leaf's membership
(§8 names the shapes that do fold). And deliberately no approximate
anything: when an exact answer needs a different request shape, the
refusal says which.

## 8. Aggregating a query's pool

`QueryRequest.aggregate` carries the same `AggregateRequest` the
Aggregate route takes and folds it over the candidate pool the page
was drawn from, on the public Query route (`docs/query-api.md`). The
scope is the pool, exactly:

- **A single lexical or dense leaf, a composite (RRF, blend,
  decomposed, cascade), a scorer pool, a boost pool.** The fold runs
  over the `selection_k` candidates, the set the page is the top `k`
  of. Naming an aggregation makes the request a POOLED one: the depth
  is fixed at `selection_k` (default `k`), paging moves inside it, and
  a cursor past it refuses with the fix, the same rule a composite or
  a boost already follows. A hybrid result page therefore shows the
  facet counts of the candidates it ranked, and page two shows the
  same counts, bitwise.
- **Under a collapse** the fold covers the pool before grouping (the
  documents, not the groups); a collapse over a single leaf that
  deepens its pool folds over the pool it settled on.
- **A browse** (a filter-only root, sorted or not) has no pool: its
  membership is the filter's exact match set, and the fold runs over
  that set on the shards directly, whatever page was asked for.
- **A boolean root** aggregates on `BooleanQuery.aggregate` over its
  exact match set (`docs/query-api.md`); setting
  `QueryRequest.aggregate` there refuses by name.
- **A sorted lexical leaf** refuses: its term membership is walked
  page by page and never held as a set.

`AggregateResponse.matched` is the pool's size (the match set's, for
a browse). The request's own `filter` and `geo_filters` must be
empty, because the selection owns membership. Group-by, histograms,
percentiles, and CARDINALITY all fold over the pool. The fan-out is
the explicit-id allowlist the boolean root uses, so a pool fold and
a filter fold over the same documents answer the same bits. The page
itself is unchanged by the aggregation: same hits, same scores, same
`executed`. The streaming Query serves the same request through the
same planner, so its completion carries the aggregate too.

## 9. Cardinality

`AGGREGATE_OP_CARDINALITY` answers the EXACT number of distinct
values an expression takes over the admitted set, never a HyperLogLog.
Each shard folds the distinct values it saw and ships them in its
partial; the coordinator unions them in a deterministic order. The
cost is the distinct set's size, which is why the op carries a loud
cap: `Aggregation.max_distinct` (default 100000) refuses on the shard
that alone exceeds it and again at the merge when the union does,
naming the aggregation and the fixes (raise the cap, tighten the
filter). A nonzero `max_distinct` on any other op refuses.

Typing: CARDINALITY admits every type, booleans included (an
expression like `year > 1993` has at most two values). Strings union
by dictionary term, so shards with different dictionaries agree.
Doubles are compared by canonical bits: `-0.0` and `0.0` are one
value and every NaN payload is one value. Absent rows are not values.
The count is an int result; an empty selection answers zero, like
COUNT. CARDINALITY folds per group under `group_by`, each group's
distinct set unioned across shards on its own.

## 10. Date histograms

`HistogramSpec.calendar` names a calendar unit (MINUTE, HOUR, DAY,
WEEK, MONTH, QUARTER, YEAR) and buckets an int expression in epoch
MICROSECONDS, the unit the timestamp column stores
(`TimestampValue`), at civil boundaries: the first microsecond of the
minute, hour, day, ISO week (Monday), month, quarter (January, April,
July, October), or year that holds the value, in the fixed zone
`utc_offset_minutes` names (zero is UTC; the bound is +-1080). The
calendar is proleptic Gregorian, computed with exact integer
arithmetic (`src/calendar.rs`, no calendar dependency); leap years
and month lengths are what the civil calendar says, before 1970
included.

A bucket's key IS its start instant: `HistogramBucket.lower_int`
carries it in micros and `lower` carries the same number as a double.
Shards fold sparse `(start, count)` maps and the coordinator sums
counts by start, the merge fixed-interval histograms already use, so
the answer is bitwise deterministic in shard order. Only occupied
buckets return, ascending; `max_buckets` caps them loudly as before.
A value whose bucket start leaves i64 (an instant within hours of the
representable range's ends) is `unbucketable`, reported, never
dropped.

Shapes that refuse by name: a calendar with a nonzero `interval`, an
offset outside the bound, an offset without a calendar, an unknown
unit, and a double or string expression (convert nothing; name the
timestamp column).
