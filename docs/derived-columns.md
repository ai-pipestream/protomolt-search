# Derived columns: the index-time computed column

Status: implemented 2026-09-07 (`src/derived.rs`, `tests/derived_columns.rs`,
the routed-ingest case in `tests/placement.rs`). The general case of the
year cut in `docs/benchmarks/fleet-placement-2026-09.md`: a column whose
value is a CEL expression over the document, computed once at index
time, stored as an ordinary typed column, declared on the index, and
usable as the physical shard key.

## The contract

A `DerivedColumns` message (`proto/ai/protomolt/search/v1/search.proto`)
declares the columns: each has a `name`, a CEL value `expression`
(`docs/cel-values.md`), an explicit `kind` (`f64`, `i64`, `u64`), and an
explicit `disclosure` rule. The TOML form configures the message field
for field, in a node config or in the coordinator's shard map:

```toml
[[derived]]
name = "year_d"
kind = "i64"
expression = "calendar.year(decided)"
disclosure = "inputs"

[[derived]]
name = "court_hash"
kind = "u64"
expression = "hash.fnv64(court)"
disclosure = "inputs"

[[derived]]
name = "key_bucket"
kind = "u64"
expression = "hash.fnv64(stable_key()) % 64u"
disclosure = "inputs"
```

A node takes it with `--derived-columns=<file>` (the shard map, or a
file with just the table; `TURBOVEC_DERIVED_COLUMNS`, config key
`derived_columns`) or as the config file's own `[[derived]]` table,
never both. The coordinator takes the shard map's table.

The **fingerprint** is SHA-256 over a domain separator and the
canonical protobuf encoding, so spelling differences in TOML do not
matter and a changed expression, kind, name, order, or disclosure does.
It identifies what the index means, and it travels: the store file
(the kind-15 column-table entry, `derived-columns`), every sealed
segment, the write-ahead log's manifest (with the `[[derived]]` table
and the complete column table, empty columns included), the layout
diagnostics (`derived_columns`, `derived_fingerprint`), and the reshard
tool's shard map.

## What an expression reads

The document's own values: numerics, integers, unsigned integers, map
numerics by name, **timestamps** as epoch-micro integers under their
column names, **facet strings** through `hash.fnv64(s)` and `==`/`!=`
against a string literal, and the **stable routing key** through
`hash.fnv64(stable_key())`. The vocabulary is the value dialect's plus:

- `calendar.year(t)`, `calendar.month(t)`, `calendar.day(t)`: the
  proleptic Gregorian UTC civil date of an int of epoch microseconds
  (`src/calendar.rs`). Ints; usable at query time too.
- `hash.fnv64(x)`: FNV-1a 64 over the UTF-8 bytes of a facet string or
  over the stable key's bytes, the same function the coordinator routes
  by (`coordinator::stable_routing_hash`, `values::fnv1a64_bytes`). A
  uint; `% 64u` buckets it (unsigned literals carry the `u` suffix).
  Ingest-only: at query time the derived column is the read, and the
  function refuses by name.

The WAL bucket hash is a different contract (it hashes little-endian
row ids and selects high bits) and stays separate. `hash(stable_key) %
n` is a versioned scheme of its own: the expression text is the
version, covered by the fingerprint; it is not the coordinator's
range routing and does not claim to be.

Outputs are not inputs: a column naming an earlier derived column is
refused at declaration time. Every input must be a source column of
the index (the declaration is checked against the tables at startup,
a name the index does not declare refuses by name, so no expression
is absent on every document by accident), the static type must match
the kind, and a derived name may not collide with a source column. A
string or bool result refuses naming the fix (hash it, compare it,
wrap it in a ternary). Absence propagates: a document lacking an input
stores no value.

The names join the column tables of their kinds after the declared
source columns, in declaration order, whether or not any row carries
a value: a node declares them the way the tables are written.

## Ingest

The node is the only writer of derived values. A request that carries
a value under a derived name, or a `derived_fingerprint`, is refused
as forged before anything else looks at it. The node evaluates the
declaration over the document's own values, pushes the results into
the ordinary value lists, stamps `derived_fingerprint`, and logs that:
a WAL record replays with its recorded values and is never evaluated
twice. A logged record whose fingerprint is not the node's declaration
is refused naming both. A column reading the stable key refuses a
document that arrived without one instead of storing absence.

Under a placement tree the pinned shard checks a direct row on its
derived values (they are computed before the tree is evaluated), and
the coordinator's routed mapped ingest computes them per source
document for routing only; the shard computes and stores its own. A
tree predicate may name a derived column; a predicate naming a column
the shard declares in no table, source or derived, is refused at
startup, and the coordinator refuses a shard map reload that changes
the declaration (a rebuild, not a reload).

A request's own `MaterializeSpec` keeps its contract for other
columns; one that names a declared derived column is refused (the
index computes it), and its expressions read source columns only.

## Storage and attach

The store persists the declaration as the kind-15 entry (kind 16 is
allocated and unused). On attach a store, catalog, or installed
snapshot written under another declaration is refused naming both
fingerprints and the fix (a rebuild through the reshard tool); one
written under none is refused unless it is empty; one carrying a
declaration this node does not declare is refused. Inside a catalog
every sealed segment must carry the tail's declaration, checked before
the table comparison so the cause is named first. The log's manifest
records the fingerprint, the `[[derived]]` table, and the complete
column table; a resumed log that disagrees with the node refuses
naming the difference, and a manifest from before either is adopted.

## Rebuild: backfill and recompute

A changed declaration is a rebuild, never a mutation in place. The
reshard tool's re-placement split takes `--derived-columns=<file>` (the
declaration the children are written under; without it the children
keep the sources' own, from the log manifest) and `--derive=a,b` (the
columns to compute on every row). Per row, the fingerprint decides:

- the declaration's: nothing to recompute, `--derive` must be empty;
- none (the row was never derived): `--derive` must name every
  declared column;
- another's: the named columns are recomputed in place of the values
  they carry, the rest are carried through.

Every path restamps. Rows come from the logs (which carry stable keys)
or, with `--from-segments`, from the sealed segments (which do not, so
a column reading the stable key cannot be derived there, refused by
name); the inputs are the sources' column tables, which the segments
carry and a log manifest written by a node that records them does.
The children's tables are the sources' with the derived names
appended, every child's segments carry the kind-15 entry, and the
shard map the tool writes carries the `[[derived]]` table. The cut
column (`--cut-column`) may be a derived column that the sources store;
one being derived in the same run is refused (derive first, cut in a
second split). Compaction and hash splits keep the declaration the
log manifest records.

`tests/derived_columns.rs` shows the backfill storing, row for row,
what direct ingest under the same declaration stores.

## Disclosure

Hashing a field does not make it public. A column declared
`disclosure = "inputs"` is usable or disclosable only by a scope that
holds the same action on the column and on every column it reads, and
on the document identity when it reads the stable key. A column
declared `own` is judged on its own grant, a deliberate operator
choice for a value that discloses nothing its inputs' owners mind.
The rule lives in `src/field_permissions.rs` and applies wherever
field grants do: filters, facets, sorts, aggregations, projections,
explanations.

## Not in this feature

A same-run cut on a column being derived; query-time `hash.fnv64()`
over a stored string (read the derived column); string-valued derived
columns; a derived column as an input to another (write the
expression inline). Each refuses by name.
