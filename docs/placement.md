# Placement trees

CEL as the partition function. An ordered chain of predicates at each
level assigns every document to a leaf; a leaf is a shard group with
its own shard count and node set. The choice is stored on the row as an
`i64` column holding the root-to-leaf path as a prefix code, so the
layout is a materialized projection of the document's columns, a
proposed tree is the same projection unmaterialized, and a subtree is
one integer range the segment pruner already reasons about.

Status, 2026-09-05: the contract, the tree, ingest evaluation, and
fan-out pruning exist; the dry run and the leaf reshard follow on their
own branch, and their sections below say so.

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
evaluates no CEL and a reopened shard answers the same browse. Both
flags read `TURBOVEC_PLACEMENT_COLUMN` and `TURBOVEC_PLACEMENT_LEAF`
and the config-file keys of the same names; the layout diagnostics
report the pinned code and whether the shard's segments carry more than
one (`placement_mixed`).

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

## The dry run (planned)

`SearchService.PlanPlacement` takes a proposed tree and reports, per
shard and per leaf, the rows that would land there and the rows that
would move from the code they carry now. It reads only. Today it
validates the tree and refuses by name.

## Changing the tree (planned)

A leaf edit is a reshard of that leaf only, on the hitless split path
keyed by the placement value instead of the stable-key hash: tail while
the parent serves, then freeze, catch up, publish. The freeze and
publish handshake and the generation checks on routed ingest are the
ones in use now.

## Segments as leaves

A segment sealed under one code has a summary of `min = max = code`, so
its tree position is its summary and one walk serves coordinator,
shard, and segment. A node in the tree is logical by default: a
predicate costs a bitmap and changes no file. It becomes physical, a
real segment cut by a partitioned compaction on the column, when it is
a sharding boundary or when the recent-query ring and route counters
show a cut would pay. Physical to logical is a merge.

Design note with the full argument: sea-of-slop
`design-notes/placement-trees-2026-09-05.md`.
