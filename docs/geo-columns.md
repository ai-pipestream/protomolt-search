# Geo columns: coordinates, region filters, and distance decay

Landed 2026-08-03 (track 1, increment 3 of the column plane).
"Within 50 km of this courthouse" as a FILTER that removes documents
exactly, and "closer is better" as a score stage that prunes bitwise
identically to the exhaustive oracle. Also the first filter family in
the engine, which is why half this document is about where a filter is
allowed to touch a block-max loop.

## A point is one value

Kind 5 is a per-slot `(lat f64, lon f64)` pair at a fixed 16 B stride,
not two f64 columns with a naming convention:

```text
kind 5 (geo point) table entry:
  u16 name_len | name | u8 kind=5
  u64 min_lat_bits | u64 max_lat_bits | u64 min_lon_bits | u64 max_lon_bits
  u64 vals_off
section: n_slots x (f64 lat | f64 lon), BOTH NaN = absent
```

Two columns would let a latitude survive a lost longitude, and there is
no honest thing to do with half a coordinate. One column makes that
state unrepresentable in the schema, and the absence sentinel closes
the rest of the gap: absence is the pair `(NaN, NaN)`, and a pair with
exactly ONE NaN is refused at open as corruption. Not repaired, not
treated as absent — refused, because "which half do you believe" has no
answer and guessing is how a coordinate silently becomes a different
place. Ingest refuses non-finite coordinates, latitude outside
`[-90, 90]`, and longitude outside `[-180, 180]`, so a half-NaN pair on
disk can only mean the bytes are not what the writer wrote.

The bounding box in the table entry is `(min_lat, max_lat, min_lon,
max_lon)` over present points, all four NaN when the column holds none
(kind 1's empty convention). It is re-derived from a full scan at open
and compared, like every other kind's metadata: metadata is checked,
never trusted. Kind 5 appends LAST in table order, so kinds 0 through 4
keep byte-for-byte the geometry they already had — the fourth time the
kinded table has absorbed a new kind without a new magic.

## Filters: where a filter is allowed to touch pruning

`docs/score-functions.md` drew the line: **CEL selects, function chains
score**, and the reason a filter is the easy half is that *a filter only
REMOVES documents*. Every block-max bound is an upper bound over a set
of documents; removing members of that set cannot make the bound stop
dominating what is left. So geo filters need no new pruning math at
all — no bound rule, no admission theorem, nothing.

What they do need is discipline about WHERE the test goes. In the
pruned scorer the filter gates exactly one thing:

```rust
if passes(filter, doc) && score >= floor && (!heap_full || score > kth) {
```

and nothing else. Not the skip tests, not the MaxScore partition, not
the cursor advances. A filtered document is still selected as a
candidate, still fully evaluated, still counted in `candidates_evaluated`,
and every cursor still advances past it exactly as before — the
wavefront must not depend on the predicate, or the doc-order invariant
the scorer's soundness proof rests on would break. The only consequence
is that the heap's k-th best now tracks the k-th best SURVIVOR, which
rises no faster than the unfiltered k-th best, so the floor stays
conservative and the skip tests stay sound.

The exhaustive oracle applies the same predicate at the same point
(before ranking), which is what makes "pruned == exhaustive bitwise"
a statement about one predicate rather than two.

Rules, stated once and pinned in tests:

- **AND semantics.** Every filter must pass. An empty filter list
  passes everything, and the filtered scorers are then bit-identical to
  the unfiltered ones (the additions are gated, not forked).
- **Edges are INSIDE.** All four bbox edges are inclusive, and
  `distance <= meters` is inside the disc. Half-open is the right rule
  for BUCKETS, which must partition without double-counting
  (`docs/range-facets.md`); a filter partitions nothing, so the
  surprising rule would be the exclusive one.
- **A document without a value fails every filter.** No location is
  inside no region. This is exact, not degradation — and it is the same
  sentence that makes a shard with NO geo column correct: all of its
  documents genuinely have no location. The known flag rides down to
  the column, and the coordinator REFUSES a column no shard knows,
  naming it and `--geo-fields`. That refusal matters more here than
  anywhere else in the column plane: a typo'd facet returns zero counts
  and a typo'd chain is a no-op, but a typo'd FILTER removes every
  document on every shard and hands back an empty result set that looks
  exactly like an honest "nothing matched".
- **Facet counting is narrowed — since the CEL increment.** At this
  increment's landing, geo filters did NOT narrow facet counts; that
  was deliberately deferred so the semantics could be defined once at
  the CEL layer. `docs/cel-filters.md` landed that definition: all
  three facet kinds now count the filtered match set, for the compiled
  filter tree and for these standalone geo filters alike.
- **Routes.** Flat and fused both carry filters (a fused query's match
  set is the union over every leg's terms, and a filter narrows that
  union exactly as it narrows a single leg's). Hybrid could not carry
  them when this was written — `HybridSearchRequest` had no filter
  field at all, because the vector leg had no filter machinery and
  filtering only the lexical half would have misdescribed the result
  set. Making the combination unrepresentable was stronger than
  refusing it at runtime. That gap is now closed
  (`docs/vector-filters.md`): `HybridSearchRequest` carries
  `geo_filters` and `filter`, and both legs resolve the same predicate,
  so the combination is representable precisely because it is no longer
  a half-truth. Hybrid still carries no facets.

## The antimeridian is refused, not guessed

`min_lon > max_lon` is INVALID_ARGUMENT naming the column.

It has an obvious "intended" reading — a box that wraps across 180
degrees — and that is exactly why it is dangerous. The two readings of
`[179, -179]` differ by the whole planet: one is a two-degree sliver
near Fiji, the other is everything else. A producer that sent the pair
by accident (swapped arguments, a sign error, a bounding box computed
from points that themselves straddle the line) gets a loud refusal that
names the problem, instead of a confident answer about the wrong
hemisphere. The workaround is one sentence in the error message: send
two boxes, one each side of 180, and union the results.

A point AT longitude 179.9 needs none of this — an ordinary
`[179, 180]` box reaches it, and that case is pinned. Wraparound boxes
are future work with a decision to make (does the region wrap, or does
the column?), not a default to guess.

## Distances, pinned

**Haversine** on a sphere of the WGS84 mean radius **6 371 008.8 m**
(R1 = (2a + b) / 3), pinned as `geo::EARTH_RADIUS_M`. The constant is a
constant and not a per-call-site opinion, because distributed results
are only bitwise equal to the monolith's if every node computes the
same bits. The `asin` form is used with `sqrt(a)` clamped at 1: the
clamp costs nothing and stops an antipodal pair from rounding past 1
and returning NaN.

**Manhattan** is `|dlat| * M_PER_DEG_LAT + |dlon| * M_PER_DEG_LON *
cos(origin_lat)`, pinned exactly so, where both per-degree constants
are `R * pi / 180`. It is a **local, city-scale approximation** and is
documented as one in the proto: it takes a single cosine (the origin's)
rather than integrating along the path, and it does NOT wrap around the
antimeridian — a longitude difference of 359 degrees measures as 359
degrees, not 1. Both are fine for "how far across town" and wrong for
"how far across the Pacific". Haversine is the one that is right
everywhere; Manhattan exists because grid-city travel is not
great-circle travel and a caller who knows that should be able to say
so.

## Decay stages, and the bound that stays at 1

`MULT_GEO_DECAY_HAVERSINE` and `MULT_GEO_DECAY_MANHATTAN` join the
`docs/score-functions.md` vocabulary with stage fields `origin_lat`,
`origin_lon`, and `scale` (METERS, finite, > 0):

```text
eval:  score * exp(-distance_meters(origin, point) / scale)
bound: ub * 1
```

The multiplier lies in (0, 1], so the stage is monotone non-decreasing
in the incoming score and absence is exact identity — the admission
rule is satisfied by the same argument `MULT_EXP_DECAY` makes, one
dimension up.

**The bound lift is 1.0, and the column's bounding box cannot tighten
it.** This is worth spelling out because it looks like it should. The
tempting lift is `exp(-d_min(origin, bbox) / scale)`, where `d_min` is
the shortest distance from the origin to the column's box (zero when
the origin is inside). That quantity is a correct upper bound on the
multiplier *for documents that have a point*. But the bound must
dominate every document in the block, including the ones with NO point,
whose factor is exactly 1. The honest lift is therefore
`max(1, exp(-d_min / scale))`, and since `exp(-d) <= 1` for every
non-negative distance, that is **1 for every origin and every box**.

So the tight bound is not implemented — not as a fallback, but because
under the current stage contract there is no tighter sound bound to
implement. Tightening below 1 needs per-block PRESENCE counts (to know
a block has no absent documents) and then per-block boxes; that is the
same future optimization `docs/score-functions.md` already names for
`MULT_EXP_DECAY`, and it is a performance item, not a correctness gap.
The box metadata is still written, still validated against a full scan,
and still exposed on the reader — it is what a selective-filter index
will be built on (below), and validating it now is what keeps a future
bound honest.

Getting a min-distance-to-bbox function subtly wrong (the lon-delta
clamp, the closer of the two clamped points) is a silent-true-hit-loss
bug, which is the one unforgivable failure here. Not needing the
function at all is a better outcome than needing it and proving it.

## Configuration

`--geo-fields=courthouse` / `PIPESTREAM_SEARCH_GEO_FIELDS` / `geo_fields` in the
TOML, same rules as every other column table: declared per shard,
immutable once a shard is built, and sharing ONE name space with the
facet, numeric, map, and integer tables (the v7 column table holds one
column per name, so the config refuses a collision early and by name).

Values arrive as `AddDocumentsRequest.geo_points` (field 11):
`GeoPointValue { field, lat, lon }`. The WAL persists the request
verbatim, so durability and reshard replay come free — reshard derives
the child's geo table from the records and re-applies the points, like
every other kind.

## What waits

- **A spatial index for selective filters.** Today a geo filter is a
  16 B column read per candidate, which is the right cost when the
  filter is broad and the wrong one when it keeps 0.1% of the corpus.
  The 2-D analog of the value-sorted `(value, doc)` section
  `docs/range-facets.md` names is a **static Morton- or Hilbert-ordered
  `(cell, doc)` section**: shards are immutable per generation, so a
  sorted array does what an R-tree does, a bbox becomes a small set of
  cell ranges, and a radius becomes the bbox that contains it plus an
  exact re-check. It joins the kinded table as another kind when a
  measurement asks for it, not before — and the column bounding box is
  already there to decide whether a shard can be skipped whole.
- **Wraparound bboxes**, with the decision named above made
  deliberately.
- **Geo facets** ("how many results per cell / per distance band").
  Distance bands are `docs/range-facets.md`'s edge list over a derived
  value, so the interesting question is whether the derived value is
  worth a column or a per-query computation.
- **CEL unification — arrived** (`docs/cel-filters.md`).
  `within_bbox(col, ...)`, `within_radius(col, ...)`, and
  `within_radius_manhattan(col, ...)` compile to these same GeoFilter
  predicates as leaves of the filter tree, and the "filters narrow
  facet counts" question was answered there once for every filter
  family. Distance-as-a-value (`geo.distance(origin) < x`) remains
  future work with the same answer as geo facets: derive a value,
  then it is ordinary.

## The routing seam

Road-network semantics — travel time, energy — are an enrichment
sidecar's job, not an index's (`docs/plans/routing-enrichment.md`).
This increment's job was to make coordinates first-class so that
pipeline has something to read. Reading routee-compass (NREL, BSD-3, in
`/work/reference-code`) for its INTERFACE shapes, with no dependency
taken and nothing copied, sharpened two things about where the seam
goes.

First, **the coordinate-to-graph projection belongs to the routing
side, and it is a real, failable step** — not a detail. Compass makes
it explicit (a `MatchingType` that accepts a point, a vertex id, or an
edge id), tolerant (a metric snap distance in config that hard-errors
past it rather than silently matching something far away), and
cacheable (it writes the resolved vertex id back into the request so a
caller can skip the snap next time). The index cannot own any of that:
the nearest-neighbour structure is derived from the road graph, so it
would have to duplicate the network to hold it. What the index owes the
sidecar is exactly what kind 5 provides — an exact `(lat, lon)` per
document, retrievable without a scoring pass — and what it must NOT
grow is a notion of vertex ids.

Second, **the batch shape matters and their boundary gets it wrong**,
which is a useful thing to learn for free. Compass's public surface is
JSON in, JSON out, and its batch API is "N independent point-to-point
queries fanned across a thread pool". The one-to-many primitive exists
internally (a single-origin shortest-path tree, then one backtrack per
destination) but is unreachable through the JSON boundary, so a cost
matrix costs N x M searches where N would do. A sidecar contract here
should therefore speak SETS — `(origins, destinations) -> cost matrix`,
snapping amortized once per set — rather than mimicking a per-request
envelope. It is also a reminder that lat/lon ordering should be
ASSERTED, not documented: their types are `geo::Coord<f32>` with x/y
ordering, and the only thing that actually enforces "y is latitude" is
a range check inside the haversine function. Ours refuses out-of-range
coordinates at ingest, at filter parse, and at open, for that reason.

And the punchline for the engine side: **precomputed routing costs are
just columns.** Travel time from a fixed anchor set lands as
map-numeric columns keyed by anchor (`travel_min["scotus"]`), where
range facets, score chains with per-key bounds, and future CEL filters
all already work. Coordinates plus anchor-keyed map-numeric columns are
the whole contract; the sidecar needs no new engine machinery, which is
the outcome the boundary was drawn to get.
