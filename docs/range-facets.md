# Range facets: i64 columns, timestamps, and bucket counts

Landed 2026-08-03 (track 1, increment 2 of the column plane).
"How many results per year band, per citation band" — over a column
kind that can hold an integer without rounding it.

## Why i64 is a kind and not an f64 with a note

An f64 holds every integer up to 2^53 exactly, but not every integer above it:
`2^53 + 1` rounds to `2^53`, silently. That is a small number by the
standards of the things a metadata plane holds — opinion ids, docket
numbers, epoch microseconds, anything minted by a counter — and this
engine's entire argument is that its results are exact. A column that
quietly returns a neighbouring integer is the same failure as a
coordinator that quietly returns a neighbouring ranking.

Integers use a true i64 column, with no rounding in storage. New writers use
kind 10, which stores presence separately from the value bits:

```text
kind 10 (i64 with presence) table entry:
  u16 name_len | name | u8 kind=10 | u64 min_bits | u64 max_bits | u64 vals_off
sections:
  n_slots x little-endian i64
  ceil(n_slots / 8) presence bytes; bit (slot % 8) of byte (slot / 8)
```

A set bit means present, including zero and `i64::MIN`. Unset slots have
canonical zero value bytes; unused bits in the last byte must also be zero.
Both readers validate these rules, exact section extents and min/max over
present values. The inverted range `(i64::MAX, i64::MIN)` denotes an empty
column, while `(i64::MIN, i64::MIN)` describes a column whose only value is
that minimum. Values and presence receive separate v8 CRC entries.

Legacy kind 4 has the same table entry width but only a values section, with
`i64::MIN` meaning absent. Its interpretation is unchanged. Loading it into a
heap store recovers explicit presence; the next write emits kind 10. Previously
stored numeric values need no reindex. Older binaries refuse kind 10, so retain the
old binary with its old shard generation for rollback and upgrade readers
before producing new files. Stores without integer columns keep their format.
The public `IntegerValue` wire shape and existing mapped-plan fingerprints are
unchanged; the schema report no longer advertises the obsolete sentinel limit.
An existing mapped binding with I64 materialized outputs is different: the old
implementation could silently omit a computed `i64::MIN`. Its materialization
hash now includes a semantic version, so new appends refuse the old binding.
Rebuild that projection from original documents. Resharding or compaction of
already-derived values cannot recover a missing materialized value. F64-only
materialization hashes keep their existing semantics and remain unchanged.

Presence costs one bit per row per integer column. It preserves the full
protobuf signed domain without making a valid numeric value disappear. The
separate unsigned column/query representation remains under development.

The unsigned storage layer uses kind 11, with the same table and section
widths as kind 10 but unsigned min/max and little-endian u64 values. Its empty
range is `(u64::MAX, 0)`; zero and `u64::MAX` are ordinary present values.
Unsigned payloads follow the existing column and positional sections and
precede the source archive. Both writers and both readers preserve their
bits, validate presence and bounds, and keep signed and unsigned column
ordinals separate. Legacy v4/v5 serializers refuse unsigned columns.

Segmented reads retain unsigned values through the writable tail, frozen
seal, publication and reopen. Tail replacement and segment opening require
identical ordered field and column tables. Sealed summaries carry a separate
`uint_columns` list with unsigned bounds and present counts; old summaries
omit that list and provide no unsigned pruning information.

On the unsigned-numeric feature branch, `AddDocumentsRequest.unsigned_integers`
carries `UnsignedIntegerValue { field, value }` entries. Declare the columns
with `--unsigned-integer-fields`, `PIPESTREAM_SEARCH_UNSIGNED_INTEGER_FIELDS`,
or `unsigned_integer_fields` in TOML. Rust uses
`NodeConfig.unsigned_integer_fields`; the mobile protobuf bridge exposes the
same list in `MobileShardConfig.unsigned_integer_fields`. Omit the entry for
absence; an entry containing zero is present. Duplicate entries and unknown
or signed-only column names refuse before applying the row. Column-name
collisions refuse for CLI, Rust and embedded configurations.

The WAL carries the u64 values directly. Flush/reopen, online compaction and
offline WAL splitting preserve their bits. Online compaction now retains the
live column tables on both layouts, including columns without any surviving
value. Offline splitting without supplied column tables still derives its
schema from the records present; it cannot reconstruct an entirely absent
column declaration from those records. Flush remains the ordinary shard's
durability boundary; this does not strengthen AddDocuments receipts.

CEL comparisons and presence tests now resolve u64 columns using exact typed
bounds, including values beyond the signed range. Placement evaluation and
both topology and segment pruning use the same numeric meaning; see
[CEL filters](cel-filters.md#numbers-compare-exactly-across-domains).
Unhinted unsigned protobuf fields now map to the u64 family; explicitly signed
hints retain their checked i64 conversion. [Value projections and materialized
columns](cel-values.md) also retain uint values. Unsigned range facets
and aggregation are not yet connected to the u64 family. The schema
report records the mapped query type and remaining restrictions. Use matching server and
client builds for this feature: older protobuf readers can ignore the new
request field, and older storage readers refuse kind 11.

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
overflows i64.

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

`--integer-fields=citations,filed_at` / `PIPESTREAM_SEARCH_INTEGER_FIELDS` /
`integer_fields` in the TOML, same rules as every other column table:
declared per shard, immutable once a shard is built, and sharing ONE
name space with the facet, numeric, and map tables (the v7 column table
holds one column per name, so the config refuses a collision early and
by name). Timestamps land in these columns; there is no separate knob.

## What waits

- **Range FILTERS — arrived** (`docs/cel-filters.md`): `year >= 1990`
  narrows the result set through the CEL surface, applied BEFORE the
  floor check exactly as this section required, and facet counts are
  narrowed by it. Counting landed first because it needed none of
  that machinery; the machinery has now caught up.
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
