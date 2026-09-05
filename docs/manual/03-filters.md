# Filters

A filter determines membership. It removes documents and adds no relevance. Every
filtered route takes two independent inputs, ANDed together:

- `geo_filters`: typed geo predicates, no parsing involved.
- `filter`: one string in CEL surface syntax.

On the public `Query` route a filter is a `FilterQuery` leaf with a
request-unique `id` and either `cel` or `geo`.

The coordinator parses and compiles the CEL once into a predicate tree and sends
that tree to the shards. Shards do not see CEL text; they resolve column names
and string values against their own dictionaries.

## What the CEL surface accepts

Grammar and precedence are stock CEL's. Some examples:

```
court == "scotus" && year >= 1990 && "color" in tags
cites["410 U.S. 113"] >= 3 || within_radius(courthouse, 38.9, -77.0, 25000.0)
decided >= timestamp("2015-01-01T00:00:00Z") && !(court in ["ca5", "ca9"])
```

| Written | Resolved as |
|---|---|
| `court == "scotus"`, `!=` | facet equality |
| `court in ["ca5", "ca9"]` | facet membership |
| `year >= 1990`, `<`, `<=`, `>`, `==`, `!=` | a numeric bound (i64 table, then f64) |
| `tags["color"] == "red"` | one key of a map-facet column |
| `cites["410 U.S. 113"] >= 3` | one key of a map-numeric column |
| `"color" in tags` | key presence |
| `has(court)` | value presence in any scalar family |
| `decided >= timestamp("2015-01-01T00:00:00Z")` | an i64 bound in epoch microseconds |
| `court < "b"`, `>=` and both bounds | one range over the byte-sorted dictionary |
| `court.startsWith("ca")` | one prefix range over that dictionary |
| `within_bbox(col, s, n, w, e)` | a geo box |
| `within_radius(col, lat, lon, meters)` | a geo disc, great-circle distance |
| `within_radius_manhattan(col, lat, lon, meters)` | a geo disc, local Manhattan distance |
| `&&`, `\|\|`, `!`, parentheses | boolean algebra |

Notes on syntax:

- Dots are ordinary characters. `meta.court` is the column named `meta.court`,
  not a traversal.
- A map key must be a string literal. `tags[k]` and `tags["a"+"b"]` are
  rejected: computed keys do not compile.
- The literal's type picks the column family. `year == "1990"` against an
  integer column is rejected as "no facet column year".
- `timestamp()` takes one RFC 3339 string. Sub-microsecond digits truncate away,
  the same direction ingest truncates, so a bound and a stored value truncate the
  same way. It is a value, not a predicate: a bare `timestamp(...)` is rejected
  and names the comparison form.

## Absence is a third value

Evaluation is three-valued, the SQL rule. A comparison on a document that has no
value for the column is UNKNOWN. `&&` is dominated by false, `||` by true, and
`!` swaps true and false and passes UNKNOWN through. Only a document with a
tree that evaluates true is kept.

Negation cannot turn absence into a match: a document with no `court` fails
`!(court == "scotus")` just as it fails the positive form.

The presence tests are total: `has(col)` and `"k" in m` answer true or false for
every document. They are the one thing absence can pass, and `!has(col)` is how
you select for absence.

This departs from stock CEL, which errors on a missing field. Erroring a
fleet-wide scan on its first absent value is unusable, and adopting proto3's
zero values instead would make an unset string equal `""`.

## Geo filters

`GeoFilter` names a geo-point column and one region, and one only.

- `GeoBbox`: all four edges are inclusive. Latitudes in [-90, 90] with
  `min_lat <= max_lat`, longitudes in [-180, 180] with `min_lon <= max_lon`. A
  box with `min_lon > max_lon` would describe an antimeridian crossing; it is
  rejected naming the column, because the two readings differ by the entire
  planet. Send two boxes and union them instead.
- `GeoRadius`: `distance <= meters` is inside. Meters must be finite and above
  zero. `GEO_METRIC_HAVERSINE` is great-circle distance on a sphere of the WGS84
  mean radius; `GEO_METRIC_MANHATTAN` is meters along the meridian plus meters
  along the parallel at the origin's latitude, a city-scale approximation that
  does not wrap the antimeridian. An unspecified metric is rejected.

Geo filters AND with each other and with the CEL filter. A document with no
point fails every geo filter: no location is inside no region.

## The vector branch

On the dense side a filter is an allowlist, not a post-filter. Each shard
resolves the predicates once over its slots and hands the resulting mask to the
scan kernel, which does not score a removed slot. A chunk in which no slot passes
skips the kernel call entirely.

Because a filter only removes documents, every pruning bound remains a valid
bound, and no pruning arithmetic changes. The shard's published cutoff becomes
the filtered k-th best, which is higher, so the fleet prunes sooner.

One cost to know: a masked scan takes the kernel's serial path, so it gives up
the multi-query batch path. Queries are grouped by identical allowlists; an
unfiltered query takes a path identical to its pre-filter form.

Filters reach both branches on every hybrid mode. In cascade they tighten phase 1;
phase 2 reranks that pool and does not widen it.

## What is rejected, and why

Anything that does not compile to dictionary-resolved predicates plus boolean
algebra is rejected by name at INVALID_ARGUMENT. It does not run slowly; it does
not run.

- Arithmetic (`+ - * / %`). The message names the fix: precompute the value and
  compare the column against a constant.
- The ternary `? :`.
- `matches()`. A regular expression engine is a dependency this engine does not
  link.
- `endsWith()` and `contains()`. A byte-sorted dictionary resolves prefixes and
  ranges, not suffixes or substrings. `startsWith()` is accepted.
- The comprehension macros `all`, `exists`, `exists_one`, `filter`, `map`, and
  `size()`.
- The type conversions `int() uint() double() string() bool() bytes() dyn()
  type()`, and `duration()`.
- Comparing two columns, or two constants.
- A bare column, literal, map access, or list in boolean position. `court` on its own
  is rejected with the fix: write `has(court)` or compare it against a value.
- `true`, `false`, and `null`. There are no boolean columns; a facet holding
  `"true"` compares as that string.
- Uint (`123u`), raw (`r"..."`), and bytes (`b"..."`) literals; a leading-dot
  float (`.5`).
- Unknown functions, named, with the accepted vocabulary listed.
- A single `=`; the message states equality is `==`.
- `has()` on a map entry, with the fix `'key' in column`.
- `startsWith("")`, which matches every value; presence is `has(column)`.
- An empty membership list, a list mixing strings and numbers, and nested
  relations such as `a < b < c`.
- String ordering against a shard with a dictionary written in an older
  first-seen order.

Structural caps: a tree deeper than 32 levels or holding more than 256 leaf
nodes is rejected. A filter is a predicate, not a program. An `and` or `or` node
with no children is rejected and not resolved to a truth value no one
requested. Validation runs at the coordinator before fan-out and again on every
node.

## Typos versus true empty results

Each shard reports, per leaf node, whether it can resolve that leaf. The
coordinator rejects a leaf that **no** shard can resolve, naming the leaf's table
and the flag to check. Without that rule, a misspelled column would remove every
document everywhere and look like a correct empty result.

A shard that lacks a column evaluates every comparison on it as the absent case,
which is exact: its documents have no such value.

A **value** the corpus does not hold is not a typo. `court == "scotsu"` returns
zero results, and zero is the true answer.

A shard still bulk-building is rejected instead of answering empty, because
"no columns yet" is a transient state and reporting it as a result would be
wrong.

Reference: `docs/cel-filters.md`, `docs/vector-filters.md`.
