# Range facets: i64 columns, timestamps, and bucket counts

Landed 2026-08-03 (track 1, increment 2 of the column plane).
"How many results per year band, per citation band" — over a column
kind that can hold an integer without rounding it.

## Why i64 is a kind and not an f64 with a note

An f64 holds every integer up to 2^53 exactly and none above it:
`2^53 + 1` rounds to `2^53`, silently. That is a small number by the
standards of the things a metadata plane holds — opinion ids, docket
numbers, epoch microseconds, anything minted by a counter — and this
engine's entire argument is that its results are exact. A column that
quietly returns a neighbouring integer is the same failure as a
coordinator that quietly returns a neighbouring ranking.

So integers get kind 4: a true i64 column, same fixed stride and same
per-slot shape as the f64 column, no rounding anywhere in storage.

```text
kind 4 (i64) table entry:
  u16 name_len | name | u8 kind=4 | u64 min_bits | u64 max_bits | u64 vals_off
section: n_slots x i64, i64::MIN = absent
```

`min_bits`/`max_bits` are the i64 two's-complement bits, validated at
open against a full scan of the section, exactly as the f64 column's
are. i64 groups tile after the map-numeric groups and before EOF; the
kinded table meant this needed no new magic and touched no existing
kind, which is now the third time that has paid off (`docs/facets.md`
called the shot, `docs/map-columns.md` collected once already).

**`i64::MIN` is the absence sentinel and ingest refuses it by name.**
An i64 column has no NaN to spend, so absence costs one value of the
domain. The alternative — a presence bitmap — is 1 bit per slot per
column plus a branch on every read, to buy back one integer nobody
sends. Spending the value is right; hiding the cost would not be, so
`IntegerValue.value == INT64_MIN` is INVALID_ARGUMENT, and a column
that no document valued folds its metadata to the empty range
(`min = i64::MAX`, `max = i64::MIN`) rather than to a value.

Score stages read i64 columns transparently: `ScoreStage.column` with
no `key` resolves against the f64 table first and the i64 table second
(one name space across all kinds, so at most one answers). Values and
the column's min/max both reach the stage arithmetic through `as f64`.
That cast rounds above 2^53 but it is monotone non-decreasing, and a
bound only needs order to be preserved, so `bound >= eval` still holds
for every document. What the cast costs is precision INSIDE the score
arithmetic, which was always f64; what the kind buys is that the value
comes back intact from the column. Those are different claims and the
code keeps them separate.

## Timestamps: sugar, not a kind

`TimestampValue { field, google.protobuf.Timestamp value }` on
`AddDocumentsRequest`. The NODE converts the instant to epoch
MICROSECONDS and writes it into the named i64 column. There is no
timestamp kind, no timestamp read path, and nothing downstream that
knows a column was fed by a clock: range facets and score stages over
it work in epoch micros like any other integer.

Micros because i64 micros spans roughly year -290307 to +294247, which
covers every court date with six orders of magnitude to spare, while
i64 nanos would run out in 2262. The sub-microsecond remainder is not
representable and is dropped — `nanos` is non-negative in a valid
Timestamp, so the drop always floors toward negative infinity and the
unit contract reads the same on both sides of the epoch. A producer
that needs nanosecond identity sends an `IntegerValue` and owns the
unit itself. Everything that could make the stored number a lie
refuses instead: `nanos` outside `[0, 1e9)`, a conversion that
overflows i64, and a result that would land on the absence sentinel.

The WAL keeps the request verbatim, so replay redoes the conversion
from the same instant rather than copying a copy — and reshard derives
the child's integer table from BOTH `integers` and `timestamps`,
because they name the same columns. Which is also why "repeats in one
document" spans the two lists: a document holds one value per column,
and a field valued by both is a producer that has not decided.

## Counting: explicit edges, half-open, no tails

`RangeFacetField { column, key, edges }` rides the same count-then-rank
pass as `docs/facets.md` describes, over the same FULL match set,
independent of `k` and the floor. Plain facets, map facets, and range
facets share ONE match bitmap per query: the traversal is the
expensive half, and asking for two kinds of facet must not pay for it
twice.

Per matched document: one value read (a 4/8 B fixed-stride read for
plain columns, a binary search of the document's pair list for a map
key) and one binary search of the edge list. That is `O(log buckets)`
on top of the walk facets already pay for.

The bucket rules are stated once and enforced everywhere:

- **Half-open `[edges[i], edges[i+1])`.** A value sitting exactly on an
  interior edge lands in the UPPER bucket. Half-open is the only rule
  that partitions without double-counting, and every histogram that
  chose otherwise regrets it at the boundary.
- **No implicit underflow or overflow buckets.** A value below
  `edges[0]`, or at or above the last edge, is counted in NO bucket.
  Silent tail buckets are how a histogram grows a spike nobody asked
  for; if you want the tails, ask for them with `-inf`-adjacent edges
  of your choosing. The counts a caller gets back are the intervals a
  caller named.
- **Edges are at least two finite strictly ascending values**, or the
  request is INVALID_ARGUMENT naming the column. Fewer than two
  describes no interval, a non-finite edge makes the comparison
  meaningless, and an unsorted list would answer for intervals that
  were never requested. Nothing here is repaired, sorted, or deduped.
  Edge validation needs no shard state, so the coordinator runs it
  BEFORE its zero-term/k=0 early return and the nodes run it again: a
  malformed list refuses even when there is no match set to count.

Column resolution follows the same order the score stages use: no
`key` means the f64 table then the i64 table; a `key` means a
map-numeric column and that key. i64 values are compared as `as f64`
against the edges, which is what the edges are anyway — a bucket test
uses order alone, and the cast preserves it.

## Distribution

Bucket counts are additive: bucket *i* means the same interval on every
shard because the coordinator forwards one edge list, so the merge is
the positional per-bucket sum. There is no analog of the global-df
trap, exactly as for plain facets.

`known` is per (column, key) per shard: a shard that cannot resolve the
column answers `known: false` with no buckets, which is legitimate for
a heterogeneous fleet (its documents genuinely hold no values — exact,
not degraded). A column NO shard knows is REFUSED, naming the column
and `--numeric-fields / --integer-fields / --map-numeric-fields`,
because a typo'd histogram would otherwise read as "zero results in
every band" — the same typo rule as fields, facets, and chains.

Both the flat and the fused route carry range facets (a fused query's
match set is the union over every leg's terms). Hybrid does not, for
the reason facets do not: the vector leg matches the whole corpus, so
"counts over the matches" has no honest answer there until filters
land.

## Configuration

`--integer-fields=citations,filed_at` / `TURBOVEC_INTEGER_FIELDS` /
`integer_fields` in the TOML, same rules as every other column table:
declared per shard, immutable once a shard is built, and sharing ONE
name space with the facet, numeric, and map tables (the v7 column table
holds one column per name, so the config refuses a collision early and
by name). Timestamps land in these columns; there is no separate knob.

## What waits

- **Range FILTERS** ("year >= 1990 narrows the result set") wait for
  CEL, with facets and filters: a filter must apply BEFORE the floor
  check to keep pruning sound, and CEL is the syntax that unifies
  scalar and map predicates. Counting needs none of that machinery,
  which is why it lands first.
- **A value-ordered index**, if selective range filters ever dominate.
  Lucene reaches for a BKD tree; in one dimension the tree buys
  nothing a sorted array does not, and shards here are immutable per
  generation, so the 1-D analog is a static value-sorted `(value, doc)`
  section per column — two binary searches for a range, no rebalancing,
  no tree at all. It joins the kinded table as another kind when a
  measurement asks for it, not before.
- **Per-block column ranges**, which would let a decay stage tighten
  its bound below 1 (`docs/score-functions.md` names this as the known
  optimization). Unrelated to range facets; the same metadata would
  serve both.
