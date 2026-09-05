# Placement trees

CEL as the partition function. An ordered chain of predicates at each
level assigns every document to a leaf; a leaf is a shard group with
its own shard count and node set. The choice is stored on the row as an
`i64` column holding the root-to-leaf path as a prefix code, so the
layout is a materialized projection of the document's columns, a
proposed tree is the same projection unmaterialized, and a subtree is
one integer range the segment pruner already reasons about.

Status, 2026-09-05: the contract is reserved (proto messages, the
`[placement]` table of the shard map, `src/placement.rs` with
validation and the code arithmetic, `SearchService.PlanPlacement`
refusing by name). Ingest evaluation, fan-out pruning, the dry run, and
the leaf reshard follow on their own branches; each section below says
which parts exist.

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

## Ingest (planned)

`RoutedIngestMapped` derives the materialized columns, evaluates the
chain over the document's own columns, writes the code into the
placement column, and hashes the stable key into that leaf's shard set.
The log stores the value in place, so replay evaluates no CEL. A node
with a placement column declared rejects a direct `AddDocuments` that
carries no value, unless started with `--placement-leaf=<code>`, in
which case it fills the value and rejects a document that evaluates to
another leaf.

## Query (planned)

Every shard in the map carries one code, which is a shard-wide summary
of `min = max = code`. Before fan-out the coordinator walks the tree
top-down against the request filter with the segment pruner's rules,
descending only into children that survive. A skipped shard offers no
floor and contributes no candidate, as a shard with no matching row
does today. A clause the path's predicates imply resolves no bitmap on
that shard. `--shard-pruning` mirrors `--segment-pruning`; the profile
reports `shards_total` and `shards_skipped` next to the segment
counters. The answer is identical with pruning off.

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
