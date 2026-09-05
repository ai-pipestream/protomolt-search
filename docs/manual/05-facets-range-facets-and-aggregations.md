# Facets, range facets, and aggregations

## Facet counts on a lexical search

`Bm25SearchRequest` takes three counting requests, all served in one traversal
over one shared match bitmap:

- `facet_fields`: plain facet columns.
- `map_facet_fields`: `{column, key}` pairs. The shape is structured instead
  of a `"column[key]"` string, so keys need no escaping. These come back
  after the plain entries, with `key` set.
- `range_facet_fields`: `{column, key, edges}`, returned positionally.

The counts are count-then-rank: every document matching at least one query term
**and** passing every filter enters the count, whatever `k` and `min_score` are. A
score cutoff bounds what is shown; a filter bounds what matched. Counts are
per-shard shares and additive, so the merge is a plain sum.

A query that wants any of the three costs one full traversal of each query
term's document run on every shard, once. Asking for none costs no extra work.

Two more, over the same filtered match set and flat-route only:

- `stats_fields`: per column, the count of documents holding a value plus min,
  max, and sum, with mean computed at the coordinator.
- `cardinality_fields`: exact distinct counts. Each shard reports the value
  strings in its match set, because ordinals are shard-local and values are the
  only currency a union can use, and the coordinator unions them. The cost is
  proportional to the per-shard distinct counts and is your explicit choice.

### Range facet edges

At least 2 edges, all finite, strictly ascending. Anything else is
INVALID_ARGUMENT naming the column; a silently repaired edge list would answer a
question no one requested. Buckets are half-open `[edges[i], edges[i+1])`, so a
value on an interior edge goes in the upper bucket. There are no
implicit underflow or overflow buckets: a value below the first edge or at or
above the last falls in no bucket. If you want the tails, ask for them.

## The Aggregate route

`SearchService.Aggregate` combines CEL value expressions over the filtered
corpus.
The filter is the same CEL the search routes take; empty means every document.
Each aggregation names an expression and one operation.

At most 32 aggregations, 8 histograms, and 8 percentile specs per request. Names
are non-empty and unique across all three: they share one name space.

### The operations

| Operation | Serves | Notes |
|---|---|---|
| `COUNT` | every type, strings and bools included | reports `present` |
| `SUM` | int and double, in their own type | ints accumulate in 128 bits; a total outside i64 is rejected naming `double()` as the fix. Doubles are Neumaier-compensated |
| `MIN`, `MAX` | int and double | a double NaN propagates |
| `MEAN` | doubles only | Welford per shard, Chan merge across shards |
| `VARIANCE` | doubles only | population variance |
| `STDDEV` | doubles only | square root of the variance |
| `CARDINALITY` | every type | exact distinct count, not a sketch |

An int expression under `MEAN`, `VARIANCE`, or `STDDEV` is rejected naming
`double()`. A boolean expression has no aggregate: filter on it and read
`matched`.

Everything is exact and deterministic. Shard partials merge in shard order, not
arrival order, so the same request over the same index answers the same bits
every run. Absence follows the three-valued rule: a document with no value for
the expression counts in `matched` (it passed the filter) but not in `present`,
and feeds no aggregate. It is not a zero the engine made up. `AggregateResult.value` is
unset when no document has a value.

Shards vote on the expression's resolved type. A difference across the fleet is
rejected with FAILED_PRECONDITION and not merged by coercion.

### Group by

`group_by` names one facet column. Every aggregation runs once fleet-wide into
`results` and once per distinct value into `groups`, ascending by value. An
admitted document with no value for that column joins no group (absence is not a
value) and counts in `ungrouped`; the fleet-wide totals still cover it.

`max_groups` defaults to 1000. Exceeding it is rejected naming the cap, not
truncated: a top-N cut would need an ordering the request did not state. The
message names the fixes.

Percentiles ignore `group_by`. There are no per-group histograms or percentiles,
and no nested grouping.

### Histograms

A `HistogramSpec` counts a double expression's present values into fixed
intervals. Bucket index is `floor(value / interval)`, the inclusive lower bound
is `index * interval`, `interval` must be positive and finite. Buckets are
sparse: only occupied ones come back, ascending, with no gap filling.
`max_buckets` defaults to 1024 and exceeding it is rejected; the fixes are a
coarser interval or a tighter filter. A present value no bucket can hold (NaN,
an infinity, or an index outside i64) is reported in `unbucketable`
and not dropped.

**Calendar histograms.** Set `calendar` to one of `MINUTE`, `HOUR`, `DAY`,
`WEEK`, `MONTH`, `QUARTER`, `YEAR` and the expression is bucketed at civil
boundaries instead. The expression must be int-typed in epoch microseconds, the
unit a timestamp column stores. `utc_offset_minutes` gives a fixed offset from
UTC, bounded at plus or minus 1080 minutes; 0 is UTC. ISO weeks start Monday;
quarters start in January, April, July, and October. Arithmetic is proleptic
Gregorian and exact, with leap years and month lengths as the civil calendar has
them, before 1970 included. Each bucket reports its start instant as
`lower_int` in microseconds, with `lower` giving the same number as a double.
A nonzero `interval` alongside a calendar, an offset outside the bound, an
offset with no calendar, and a double or string expression are all rejected by
name.

### Percentiles

A `PercentileSpec` names an expression and 1 to 16 percentiles, each finite in
[0, 100]. The answer is the exact nearest-rank order statistic, a value some
admitted document actually has: for percentile p over n present values, the
k-th smallest with `k = max(1, ceil(p/100 * n))`. So 0 is the minimum, 100 the
maximum, and 50 the lower median on even counts. Every answer reports its rank.

It is found by a coordinator-driven binary search over the order-bits domain,
at most 64 count-below rounds for the entire request however many percentiles you
ask for, each round one pass over the admitted set per shard. There is no
sketch and no interpolation; exactness is paid in scans instead of in error.

Int expressions answer ints and doubles answer doubles. A computed NaN is
reported in `unrankable`, excluded from ranking, and not dropped.
Infinities rank at the ends. An empty selection answers rank 0 with no value.

### Cardinality

`max_distinct` defaults to 100,000 and applies per shard and again at the merge.
Exceeding it is rejected naming the aggregation and the fixes. A nonzero
`max_distinct` on any other operation is rejected. Strings union by dictionary
term, so shards with different dictionaries agree; doubles compare by canonical
bits, so `-0.0` and `0.0` are one value and every NaN payload is one value.

## Aggregating a query's own pool

`QueryRequest.aggregate` runs the same `AggregateRequest` over the candidates
the page came from, instead of over the entire corpus.

- On a single leaf, a composite, a scorer pool, or a boost pool, the aggregate covers
  the `selection_k` candidates. Naming an aggregation fixes the depth at
  `selection_k`, so paging moves inside that pool and every page reports the
  same aggregate, bit for bit. A cursor past it is rejected with the fix.
- Under a collapse, the aggregate covers the pool before grouping: the documents, not
  the groups.
- A browse has no pool. Its membership is the filter's exact match set, and the
  aggregate runs over that set whatever page was requested.
- A boolean root puts its aggregation on `BooleanQuery.aggregate` instead,
  over its exact match set. Setting `QueryRequest.aggregate` on a boolean root is
  rejected by name.
- A sorted lexical leaf is rejected: its term membership is traversed page by
  page and not kept as a set.

In both pooled and boolean forms the aggregation's own `filter` and
`geo_filters` must be empty, because the selection already owns membership.
`AggregateResponse.matched` is the pool's size. The page itself is unchanged:
same hits, same scores.

Reference: `docs/aggregations.md`.
