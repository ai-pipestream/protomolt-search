# Placement trees

Predicate partitioning with constraint exclusion. An ordered chain of
predicates at each level assigns every document to a leaf; a leaf is a
shard group with its own shard count and node set. The choice is stored
on the row as an `i64` column holding the root-to-leaf path as a prefix
code, so the layout is a materialized projection of the document's
columns, a proposed tree is the same projection unmaterialized, and a
subtree is one integer range the segment pruner already reasons about.

## Terms

The names here are the database ones, so the reader carries the proof
with the word:

- **Predicate partitioning** (also expression partitioning): the
  partition function is a predicate over the row's columns, written in
  the filter dialect. A multi-level tree is subpartitioning.
- **Constraint exclusion** (also partition pruning): the planner skips a
  partition whose predicate cannot hold together with the query filter.
  The coordinator applies it to shards and the shard applies it to
  segments, with the same sound rules.
- **Logical partition**: a node of the tree that changes no file; its
  predicate is resolved at query time as a bitmap, like a view.
- **Materialized partition**: a node cut into its own segments by a
  partitioned compaction on the placement column, like a materialized
  view. Compaction materializes; a merge makes it logical again.
- **Shard** keeps its meaning: the unit of the stable-key hash split. A
  leaf is a set of shards; a partition is a predicate.

Status, 2026-09-06: the contract, the tree, ingest evaluation, fan-out
pruning, the dry run, the offline placement split by code, the
re-placement split under a new tree, and the node-side check of direct
rows exist. The hitless leaf reshard still keys by hash; its section
below says so.

## The tree

```toml
[placement]
column = "placement"          # i64 column every row carries
level_bits = 9                # 512 children per level, 7 levels in 63 bits

[[placement.nodes]]
name = "large"
cel = "body_bytes >= 65536"
shards = 6
nodes = ["192.168.1.195:19300"]

[[placement.nodes]]
name = "recent"
cel = "year >= 2020"
  [[placement.nodes.children]]
  name = "scotus"
  cel = 'court == "scotus"'
  shards = 1
  [[placement.nodes.children]]
  name = "rest"               # no cel: this level's default
  shards = 2

[[placement.nodes]]
name = "other"                # the root default, mandatory
shards = 2
```

Rules (`Placement::validate` refuses by name):

- A chain is ordered and first match wins. A node with no `cel` is its
  level's default and must be last; the root chain and every node with
  children end with one. A document on which every predicate of a level
  is false or UNKNOWN takes the default, the same rule a partitioned
  compaction applies to rows without the key.
- Predicates are in the filter dialect (`docs/cel-filters.md`), so the
  pruner can reason about them. A rule that needs arithmetic, such as
  document length, is a materialized column first (`docs/cel-values.md`)
  and a comparison on it in the leaf.
- Predicates read stored columns only, so replay derives the same leaf
  and a query filter over the same columns can rule a leaf out.
- A node with children carries no `shards` or `nodes`; leaves do.
  `shards` of 0 selects 1.
- The tree is part of the topology generation and immutable within it.
  Renaming a node is free; reordering or inserting is a new generation.

## The code

Each level owns a field of `level_bits` bits, the root highest, the
sign bit unused, and the leaf's code is the concatenation of the chosen
index per level (`placement::encode`). The descendants of a node share
its prefix, so a subtree is one contiguous range
(`placement::subtree_range`), which is a range predicate on the column.
No bitwise operator enters the filter dialect: `&` in a predicate is
not monotone, while a level field under a fixed prefix is a range.

The column stores the chosen index, not a mask of predicate outcomes:
first match already picked the child, and an outcome mask gives the
pruner nothing.

A leaf's rows satisfy every non-default predicate on its path
(`Leaf::own`). That set is a superset of the rows the leaf holds, which
is what makes it sound for pruning: a filter that cannot hold together
with the path's predicates cannot match a row in the leaf. The "and not
an earlier sibling" part of membership is realized by first match at
ingest and is never needed for pruning.

## Ingest

`RoutedIngestMapped` under a placed topology evaluates the tree once
per source document: the bind's plan extracts the columns, the value
dialect derives the materialized columns, and the chain runs over the
document's own values with the shard's three-valued rules
(`placement::eval_document`). The stable key then hashes inside the
leaf's shard set: under a tree the hash ranges tile the space per leaf
(`route_stable_key_in`), and a plain `route_stable_key` on a placed
topology is refused by name. The rows of one source document (one per
chunk on a chunked plan) go to one shard, so they must agree on the
leaf; a placement predicate reads parent-scope columns. Quality and
geography columns are derived on the node after analysis, so a
predicate on one is UNKNOWN at routing time and falls through.

The leaf's shards fill the column. A node started with
`--placement-column=<name>` declares the column (it joins the integer
table) and `--placement-leaf=<code>` pins the leaf: a document without
a value takes the code, one with the same code passes, one with another
code is refused naming both. A node with the column declared and no
leaf pinned refuses a direct `AddDocuments` without the value, naming
the column and the flag. The log stores the value in place, so replay
evaluates no CEL and a reopened shard answers the same browse. The
flags read `TURBOVEC_PLACEMENT_COLUMN`, `TURBOVEC_PLACEMENT_LEAF`, and
`TURBOVEC_PLACEMENT_TREE`, and the config-file keys of the same names;
the layout diagnostics report the pinned code and whether the shard's
segments carry more than one (`placement_mixed`), and the fixed knob
`placement_tree` names the pinned leaf when a tree is given.

The code alone does not make the leaf's predicates true of the rows: a
direct row with any values takes the code. `--placement-tree=<file>`
gives the node the tree, from the coordinator's shard map (its
`[placement]` table) or a file holding just that table, and the node
then checks each direct row before it derives anything
(`placement::PinnedLeaf`). At startup the pinned code must name a leaf
of the tree, the tree's column must be the declared one, and a tree
without a pinned leaf is refused, each by name. Per row the chain is
evaluated the way the coordinator routes, over the values the row
arrived with plus its value-dialect materialized columns and none of
the quality or geography columns analysis adds later, so a routed row
and a direct row are judged on the same values. A row the tree routes
elsewhere is refused naming the node whose predicate sent it there (an
earlier sibling that is true on the row, or a node on the pinned path
that is false or unknown on it) and the leaf the row belongs to, with
or without the pinned code on it. A logged record already passed, so
replay and the compaction shadow do not evaluate again. With the tree
on every shard, shard pruning and implied-clause dropping rest on
something the shard enforces, not on the routing having been the only
writer.

## Query

Every shard in the map carries one code, and its leaf's own predicates
(`Leaf::own`, plus the placement column pinned to the code) bound what
its rows can hold. Before every filtered fan-out the coordinator tests
the request filter against each shard's bounds
(`placement::impossible_under`) and skips the shards where no row can
pass: an AND with an impossible child, an OR whose branches are all
impossible, a number range outside the leaf's interval, a facet
equality, string range, or prefix no admitted value satisfies. `NOT`,
map, geo, and presence predicates never skip on their own. Skipped
shards are not sent the request on the vector fan-outs, the streaming
scan, the BM25 legs, hybrid, browse, aggregation, percentiles, and
boolean membership; a skipped shard contributes no candidate and no
floor, as a shard with no matching row does. One shard is always
consulted, so a filter that excludes every leaf still runs the known
handshake and returns the shape a consulted fleet returns. The filter
leaves that excluded a shard count as resolved for the typo rule,
because the leaf predicate that excluded it named the same column.

`--shard-pruning` (`TURBOVEC_SHARD_PRUNING`, default on) is the A/B
switch, live afterwards as the coordinator's `shard_pruning` knob
(`docs/diagnostics.md`). The profile reports `shards_total` and
`shards_skipped` for the plan's filter next to the segment counters; a
boolean root resolves each clause on its own shard set and reports no
plan-level skip. The answer is identical with pruning off, and
`tests/placement.rs` holds that on every shape.

### Implied clauses

The same bounds work the other way on a shard that is consulted. A row
in a leaf made every predicate on the leaf's path true, so a request
clause the path implies is true for every row the shard holds: a number
range that contains the leaf's interval on that column (`year >= 2010`
on a leaf pinned to `year >= 2015`, edges compared exactly across the
integer and float domains), a facet list that contains every value the
leaf admits (`court in ["scotus", "ca9"]` on `court == "scotus"`), or a
presence test on a column the leaf constrains. Such a clause is removed
from the tree that shard receives (`placement::implied_under`,
`placement::without_leaves`, `ShardMask::filter_for`), so the shard
resolves one bitmap less; an `AND` left with one child becomes that
child, and a tree implied whole becomes no filter on that shard. Only
clauses reachable from the root through `AND` are considered: under
`OR` or `NOT` a clause's value is part of a larger one and stays. The
leaves a shard was spared count as resolved for the typo rule, and the
shard's known flags, which cover only the tree it received, are mapped
back to the request's leaves by the coordinator. The switch is the same
`shard_pruning` knob, the answer is identical either way, and
`tests/placement.rs` holds it on the shapes that drop one clause, drop
the whole tree, or keep an implied clause under `OR`.

## The dry run

`SearchService.PlanPlacement` takes a proposed tree and reports, per
shard and per leaf, the rows that would land there and the rows that
would move from the code they carry now, plus totals and the rows that
took a default. It reads only; no row moves. An optional `filter`
(filter-dialect CEL) restricts the rows considered.

The counts are exact and come from the filtered counts shards already
answer, with no per-row evaluation on the coordinator. A row lands on a
node when the node's predicate is TRUE and no earlier sibling's
predicate at any level of its path is TRUE (an UNKNOWN sibling falls
through, like FALSE). Under the three-valued rules that is a difference
of two counts: `count(A) - count(A && B)`, where `A` ANDs every
non-default predicate on the path and `B` ORs every earlier sibling's
predicate along it; `A && B` is TRUE exactly when `A` is TRUE and some
earlier sibling is TRUE. A default's count is its parent's minus its
non-default siblings'. The rows that would stay are the same counts
restricted to `placement == code`; on a shard without the column that
predicate is absent, so all of its rows count as moving. Counts are
memoized per node, so a default's subtraction reuses its siblings'.

A predicate naming a column no shard holds is refused by name, the rule
every filtered route applies to a typo; a tree that fails validation is
refused before any shard is asked. The arithmetic is in
`src/placement_plan.rs`; the fan-out is the aggregate route's, one
`AggregateShard` request per count.

## Changing the tree

A leaf edit is a reshard of that leaf only. The offline path exists:
`reshard::split_placement_logs` (and `reshard --placement-column=...
--placement-ranges=lo..=hi,...[,default]`) replays a shard's full
history and assigns each logged row to a child by the code its
placement column carries. No CEL runs at replay; the code was written
at ingest and the log holds it. A child takes one code or a prefix
range (a subtree); the child named `default` takes rows with no value
and rows whose code no range covers, and without one any such row
refuses the split by id. Ranges may not overlap, and only the default
child may be rangeless. Every child keeps the parent's stable-key hash
coverage, because routing under a tree is by code first and by hash
inside the group. The written shard map carries `placement = <code>`
for a child with one code; a child holding a range is left for the
operator to place, since the map names one code per shard.

A NEW tree is a re-placement split: `reshard::split_placement_tree_logs`
(and `reshard --log=<gen> --placement-tree=<map or table> --out-dir=...
--slot-base=B --slot-stride=S`, or `--logs=a,b,...` for the union of
several shards) validates the tree, evaluates it on each live
document's stored values the way the coordinator routes at ingest,
rewrites the placement column to the code the tree assigns, and writes
one child per leaf shard in leaf order (`reshard::tree_children`): a
leaf with `shards = n` gets `n` children tiling the stable-key hash
space as the coordinator's per-leaf routing does, `n` a power of two,
and a row routed into such a leaf must carry a stable key or the split
refuses by id (give the leaf one shard or rebuild through routed
ingest). A vector row with no document cannot be evaluated and refuses
by id. Rows are routed one source WAL bucket at a time into per-child
spill logs under `<out>/spill` (removed at the end), written with the
widest bucket count among the sources (`--spill-bucket-bits` overrides
it), and each child is then built from its spill one bucket at a time:
every non-empty bucket seals as one segment of the child's catalog
(`<out>/shard-<i>.tv.segments`, `docs/immutable-segments.md`), so
memory is one bucket's rows plus one segment build, never a child. On
the fleet's archive (six sources of 64 buckets, 11M rows per band) that
is about 170,000 rows per replay instead of 11M; the first run of this
split wrote one-bucket spills and held a band at once, 50 GB on a 61 GB
machine. A child with no rows is an empty catalog. The written
`shard-map.toml` carries the new tree under `[placement]` and one
`[[shards]]` per child with its `placement` code and hash range,
addresses left to fill in; each child serves under
`--index=<out>/shard-<i>.tv`, `--placement-column`,
`--placement-leaf=<its code>`, and `--placement-tree=<that map>`, and a
child's code that is no leaf of the old tree is refused by name if it
is started under the old map. Documents are re-analyzed through the
sidecar as in every replay, unless `--from-segments` names the sealed
segments beside each log as the source of every document's analyzed
fields, columns, text, vectors and identities
(`docs/replay-from-segments.md`): the analyzer is not called, the
sources must be flushed (an unsealed tail is refused by name), and they
must share one field table, one analysis fingerprint per field, and one
set of column tables. The split is offline: no live cutoff is recorded,
so the sources must be quiescent. `--single-image=<max child rows>`
writes one image per child instead, the shape the other splits write,
and refuses a child above the bound before writing anything. Under the
hash cut a segment covers one hash bucket of the band, not a year range,
so the year cut inside a leaf is `CompactShard` with `partition_column`
on the served child (`docs/segment-pruning.md`); `--cut-column=year
--cut-rows=<n>` cuts each child's spill by the year instead, so the
child's segments come out in year order with partition summaries and
the catalog names the partition key, the layout a compaction would have
left.

The hitless flow (tail while the parent serves, then freeze, catch up,
publish) still partitions by the stable-key hash; keying its catch-up
by the placement value is the next step, on the same
`LiveReshardState` the hash flow uses.

## Segments as leaves: logical and materialized partitions

A segment sealed under one code has a summary of `min = max = code`, so
its tree position is its summary and one walk serves coordinator,
shard, and segment. A node in the tree is a logical partition by
default: a predicate costs a bitmap and changes no file. It becomes a
materialized partition, a real segment cut by a partitioned compaction
on the column, when it is a sharding boundary or when the recent-query
ring and route counters show a cut would pay. A merge makes it logical
again.

Design note with the full argument: sea-of-slop
`design-notes/placement-trees-2026-09-05.md`.
