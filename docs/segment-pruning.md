# Segment pruning from summaries

A sealed segment records, per integer and per double column, the least and
greatest stored value and how many rows carry one (`docs/immutable-segments.md`,
"Segment summaries"). When a request's filter cannot be satisfied by any value
in a segment's range, no row of that segment can pass it, and the shard leaves
the segment unopened. The answer is the same as with pruning off; only the
work changes, and the response says how much.

This is the vector-side counterpart of block-max on the lexical side: a bound
stored next to the data, tested before the data is read. Block-max skips a
posting block whose best possible score cannot beat the floor; segment pruning
skips a segment whose column range cannot meet the filter.

## What is pruned

Pruning is decided once per request per shard, from the resolved filter tree
and the segment summaries, by `segment_prune::no_row_can_pass`. The rules are
conservative; each one answers "no row can pass" or "maybe", never "some row
passes".

- An integer or double range leaf is impossible on a segment whose summary
  range for that column is disjoint from the predicate, and on a segment where
  no row carries the column (`present == 0`): a missing value never satisfies
  a range. Half-open and inclusive edges are compared exactly, with the same
  edge arithmetic the per-row evaluator uses, so a bound that touches the range
  is "maybe" and one past it is impossible.
- A presence test (`has(column)`) is impossible only when every family the
  name resolved to is known empty in the segment, and only for the integer and
  double families; a name that also resolves to a facet or geo column is
  "maybe", because those tables are not summarized.
- `AND` is impossible when any child is. `OR` is impossible when every child
  is. `NOT` is always "maybe": a range summary can say that no row passes a
  leaf, but not that every row does, and rows without the value are Unknown
  under the leaf and Unknown under the negation.
- Facet, map, geo, string range, and unresolved-number leaves are "maybe".
- A segment sealed before summaries existed has none, and is never pruned.
- A partitioned compaction records each output's value range on the
  partition column; the pruner reads it as one more bound on that column.

Columns are matched by name. The filter carries the shard's table indices,
which are per image; a segment's summary names its columns.

## Where it applies

On a segmented shard, a pruned segment is left out of:

- **The vector scan.** The slot allowlist is filled `false` over the segment's
  slot range without a per-row evaluation, and the segmented provider skips
  an image whose slice of the allowlist is all `false`, so the segment's
  vector image is not opened. This holds on `SearchShard` (both the solo and
  the coalesced kernel path), `StreamSearch`, and the hybrid shard routes.
- **The postings walks.** BM25 scoring under a filter, count-then-rank facets,
  and the fused multi-field route take a reader over the admitted parts and
  the heaps (`SegmentedShard::masked`, `field_masked`): the walks over
  postings, impacts, and dictionaries skip the pruned parts, while reads
  addressed by document id (text, offsets, positions, columns) still cover
  the whole shard, so a surviving hit resolves as usual. Block-max pruning
  chains only the admitted parts.
- **The slot loops.** A browse under a filter, `AggregateShard`, and
  `QuantileCounts` iterate the admitted slots and never evaluate the filter
  on a pruned segment's rows.
- **The boolean planner.** `ResolveFilterBitmap` prunes as the vector scan
  does. `ResolveLexicalBitmap` skips every sealed part in which none of the
  clause's terms occur, from one dictionary lookup per term per part; an
  intersection with an empty membership is empty, so a required lexical
  clause rules those parts out of the group exactly. A dense clause in a
  boolean group is scored over the survivors through `VectorRescore`,
  which a node answers with one masked scan of its index: the survivors
  are the allowlist, a sealed part in which none of them sits is not
  opened, and a SIMD block with none is not read
  (`docs/query-api.md`, "Recursive boolean execution").

The vector kernel still reads every row of a segment it opens: pruning removes
whole segments, not rows. Within an opened segment the allowlist masks rows
and the kernel short-circuits fully masked blocks, as before
(`docs/vector-filters.md`).

## What is not pruned, and why

- Anything under a `NOT`, an `OR` with an unbounded branch, or a leaf over a
  facet, map, geo, or string column: the summary cannot bound it.
- The heap parts (the tail and a frozen tail): they have no summary until
  they seal.
- The phrase route, the hybrid legs' response counters, and the
  `QueryStream` completion: the pruning applies there, and the counts do not
  ride those responses yet.
- Segments of a shard that has no segment catalog (the single-image layout
  and the heap builder): there are no segments to skip, and the counters
  report zero.

## The flag

`--segment-pruning` (default `true`; `TURBOVEC_SEGMENT_PRUNING`, or
`segment_pruning` in the config file) turns pruning off on a node, for A/B
runs and for bisecting a disagreement. With it off every segment is
evaluated, the `segments_total` counters still report the shard's sealed
segments, and `segments_skipped` is zero. The result set and every score are
identical either way; `tests/segment_pruning.rs` asserts that bitwise on each
route.

## The counters

Every route that prunes reports, per shard, `segments_total` (sealed segments
in the snapshot the request took) and `segments_skipped` (the ones ruled out
without being opened): `ShardScanStats` on the shard search stream,
`StreamSearchSummary`, `Bm25QueryResponse`, `BrowseShardResponse`,
`AggregateShardResponse`, `FilterBitmapResponse`, and
`MembershipBitmapResponse` for the lexical bitmap. The coordinator sums them
into `SearchResponse` and `Bm25SearchResponse`, and, when a Query sets
`profile`, into `QueryProfile.segments_total` / `segments_skipped` for the
shape that ran: a dense or lexical leaf under a filter, a browse, or a boolean
root, where each membership resolution counts its shards' segments once, so a
filter clause and a lexical clause together consult the segments twice. The
counts describe work, never results: two requests with the same hits may
report different skips, and a count of zero says nothing about the answer.

## Tests

`tests/segment_pruning.rs`: a persisted segmented shard whose rows arrive in
`year` order under a seal bound of four, so each sealed segment covers one
year. A `year` predicate reports the expected skips on the dense, lexical,
browse, and boolean routes through the public Query with `profile`, on the
BM25 route with facets, on the shard's aggregate, browse, and streaming routes
directly; a column no row carries prunes every sealed segment; `OR` with a
facet branch and `NOT` prune none and return the rows a naive rule would
lose; a segment whose summary is stripped from the manifest is never pruned;
and the same shard reopened with `--segment-pruning=false` returns the same
hits and scores on every case. `src/segment_prune.rs` carries the boundary
unit tests for inclusive, exclusive, and integer-versus-double edges.
