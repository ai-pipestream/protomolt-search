# Relay folds: work in progress (paused 2026-09-06)

Branch `feat/relay-folds-2026-09` from main 5fdedf3. Paused on the
coordinator's request while relay receipts and authorization are
reconciled on main. No merge, no push.

## Done

- `src/coordinator.rs`: `AggMerge` and `PctMerge` (the per-aggregation
  and per-percentile fold states) are `pub(crate)` with `new`, `fold`,
  and a new `partial()` that renders the merged state back as one
  shard's `AggregatePartial` / `PercentilePartial` (exact int sum split
  into halves, compensated double sum as the pair, distinct sets as
  sorted lists, `vtype` Absent when no child typed the expression).
  This is the piece a relay needs to fold child shares in child order
  and answer the parent as one shard. `cargo check` clean; no behavior
  change on the root.

## Findings that shape the remaining work

- `HybridShard`: still called by `fanout_hybrid_two_level` (the
  FUSION_MODE_TWO_LEVEL path, coordinator.rs ~6030). That mode RRF-fuses
  one list per shard at the root and is documented as partition
  dependent, so a relay presenting children as one shard would change
  the answer by design. Keep the refusal; the fused routes (`ShardLegs`,
  cascade) are the composed path. Only the refusal text needs to say so.
- The public search route does not fetch documents through
  `GetDocuments` (callers: console.rs, compaction.rs); `FetchValues`
  (projections, collapse keys, stored-value stages), `ResolveParents`
  (lineage collapse keys), and `BrowseShard` (filter-only query with
  sort and cursor, query.rs ~1935) are the follow-ups the public Query
  route sends a relay child today.
- Fold exactness: a relay covering a prefix of the root's shard order
  (one relay over all children, or nested chains) reproduces the root's
  fold bit for bit: Neumaier folding of (sum, compensation) from a zero
  state is exact, Chan's Welford merge into an empty state copies, ints
  and counts are exact. Side-by-side relays over doubles (sum, mean,
  variance) can differ in the last bit from the flat fold; ints, counts,
  min/max, histograms, percentile partials, and quantile counts are
  exact in any grouping. Say so in docs/relay-coordinators.md.

## Remaining, per route (src/relay.rs; none started)

- `GetDocuments`, `ResolveParents`: route ids by child slot range
  (`children_health` + `child_ranges` + `route_ids`, as `Bm25Rescore`
  does), skip children with no ids, reassemble in the caller's id order
  with a per-id queue so repeated ids come back once per occurrence as
  a node answers them; `still_current` before answering.
- `FetchValues`: same routing, but ask every child (an empty id list
  still answers the known flags the root's typo rule runs on); rows in
  caller order; `stage_columns_known` and `projection_leaves_known`
  ORed through `merge_known`.
- `AggregateShard`: forward the request to every child (foreign
  `doc_ids` are ignored by a node); fold shares in child order with a
  new `merge_aggregate_shares(aggregations, group_by, max_groups,
  histograms, percentiles, shares)`: partials via `AggMerge::fold` then
  `partial()`, groups joined by value in a BTreeMap with the
  `max_groups` cap, histograms summed by bucket index with the
  `max_buckets` cap, percentile partials via `PctMerge`, `matched` and
  `ungrouped` added with a check, geo/filter/expr-leaf known flags ORed
  through `merge_known`, segment counts added.
- `QuantileCounts`: every child; when `boolean` is set translate its
  `expected_stats_epoch` per child through `child_claims`; counts
  summed with a check, list lengths must agree.
- `EvaluateBoolean` with `aggregate`: drop the refusal; fold each
  child's `aggregate` (a missing one is a protocol break) with the same
  `merge_aggregate_shares` and attach to the merged response.
- `BrowseShard`: every child gets the request unchanged (the boundary
  is per row and applies on each child); merge rows with
  `sortkeys::cmp_rows` over `Key::from_pb` (id order when unsorted),
  truncate to `k`, re-emit the original `SortKeyRow`s in the merged
  order; geo/filter/sort known flags ORed; segment counts added.
- `stats_fields` / `cardinality_fields` on the keyword routes: the
  refusal stands unless the fold-state approach above is extended to
  `ColumnStats`; not analyzed further.
- Module doc, `refused()` text, docs/relay-coordinators.md ("What is
  not composed yet" shrinks, reference list grows), README entry.

## Tests (not written)

- tests/relay.rs: the lexical fixture needs an integer column (`year`)
  and a numeric column (`pages`) on `add_faceted_documents` for
  histograms and percentiles. Planned: fetches by id through one and
  two relay levels equal the flat answers (ids across children, foreign
  ids, repeated ids, caller order); browse pages with and without sort
  and with a cursor through a relay root equal the flat root; an
  aggregate (count/sum/min/max on ints, group-by facet, histogram,
  exact percentile) through relay roots equal the flat root, doubles
  under a relay over all children; the boolean aggregate through a
  relay equals the direct root (replaces
  `a_boolean_aggregate_through_a_relay_refuses_by_name`); the
  `HybridShard` refusal remains in `unsupported_routes_refuse_by_name`
  (GetDocuments moves out of it).
