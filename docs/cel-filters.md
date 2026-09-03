# CEL filters: the selection language

`Bm25SearchRequest.filter` is a CEL expression:

```
court == "scotus" && year >= 1990 && "color" in tags
cites["410 U.S. 113"] >= 3 || within_radius(courthouse, 38.9, -77.0, 25000.0)
decided >= timestamp("2015-01-01T00:00:00Z") && !(court in ["ca5", "ca9"])
```

This lands the selection half of the two-language split
`docs/score-functions.md` pinned: **CEL selects, function chains
score.** A filter is a predicate over the column plane — facet, f64,
i64, map, geo — compiled once and executed as dictionary-resolved
ordinal tests. Scoring stays in the fixed stage vocabulary, because
pruning needs a derivable upper bound per stage and an arbitrary
expression has none. Nothing in this feature touches a score.

## The pipeline: compile once, resolve per shard, never interpret

```
CEL text ──(coordinator: src/cel.rs)──▶ FilterExpr IR ──(every shard:
   resolve names/values against ITS dictionaries)──▶ ResolvedFilter
   ──(per candidate, at the heap gate)──▶ pass / fail
```

- The COORDINATOR parses and compiles the text exactly once
  (`cel::compile_filter`) into the `FilterExpr` proto tree — and/or/not
  over seven leaf kinds — then forwards the same tree to every shard.
  Shards never see CEL text.
- Each SHARD resolves the tree against its own tables
  (`Bm25Shard::resolve_filter`): column names to table indices, string
  values to dictionary ordinals, number bounds into the resolved
  family's exact domain. Ordinals are shard-local, like every
  dictionary in this engine, so they never travel.
- Evaluation is array reads and integer compares per candidate
  document, at the ONE place a filter belongs: immediately before the
  floor test and heap insertion, in the pruned scorer and the
  exhaustive oracle identically (`bm25::FilterCtx`, the seam the geo
  increment built). A filter only REMOVES documents, so every
  block-max bound stays a valid upper bound with no new pruning math.
- Nothing is ever interpreted per document. A construct that does not
  compile to dictionary predicates plus boolean algebra is refused BY
  NAME at INVALID_ARGUMENT — it does not run slowly, it does not run.

The parser is hand-rolled recursive descent (`src/cel.rs`): no regex,
no parser generator, no new dependency in the serving binary. The
grammar and precedence are stock CEL's (`||` < `&&` < relations <
unary < member), held to agreement with a reference interpreter by the
differential oracle below.

## The compiled vocabulary

| CEL | Compiles to | Resolution |
|---|---|---|
| `court == "scotus"`, `!=` | `FacetPredicate` (`!=` wraps NOT) | facet table; value → ordinal per shard |
| `court in ["a", "b"]` | `FacetPredicate` with several values | same |
| `year >= 1990`, `<`, `==`, ... | `NumberPredicate` bounds | i64 table first, then f64 |
| `year in [1990, 1995]` | OR of point ranges | same |
| `tags["color"] == "red"`, `in [...]` | `MapFacetPredicate` | map-facet column + key ordinal |
| `cites["k"] >= 3` | `MapNumberPredicate` | map-numeric column + key ordinal |
| `"k" in tags` | `MapKeyPredicate` (total) | map-facet table first, then map-numeric |
| `has(court)` | `HasPredicate` (total) | any scalar family under the name |
| `timestamp("RFC3339")` | an i64 bound in epoch micros | exact civil-date integer math |
| `within_bbox(col, s, n, w, e)` | `GeoFilter` bbox leaf | geo table |
| `within_radius(col, lat, lon, m)` (+ `_manhattan`) | `GeoFilter` radius leaf | geo table |
| `court < "b"`, `<=`, `>`, `>=` | `StringRangePredicate` | one ordinal range of the byte-sorted dictionary (`docs/prefix-terms.md`) |
| `court.startsWith("ca")`, `tags["k"].startsWith("re")` | `StringPrefixPredicate` | the dictionary's prefix range |
| `&&`, `\|\|`, `!`, `(...)` | and / or / not nodes | Kleene three-valued |

Dots are ordinary characters in this engine's flat column namespace:
`meta.court` is the column named "meta.court", not a traversal. Map
access requires a string-literal key; computed keys do not compile.

Refused by name, with the reason in the message: arithmetic (`+ - * /
%`), the ternary, `matches()` (a regex engine is a CVE class this
codebase deliberately does not link), `endsWith`/`contains` (a
byte-sorted dictionary resolves prefixes and ranges, not suffixes or
substrings), the comprehension macros (`all`/`exists`/`filter`/`map`),
`size()`, type conversions, `duration()`, cross-column comparisons,
constant comparisons, bare columns and literals in boolean position,
uint/raw/bytes literals, and unknown functions. String ordering and
`startsWith` compile since 2026-09-02 (`docs/prefix-terms.md`): every
dictionary is written in byte order at flush, and a file whose
dictionary predates that refuses them by name rather than walking
strings per document.

## Three-valued semantics: the one deliberate deviation

Stock CEL ERRORS on a missing field; proto-default semantics would
make an unset string equal `""`. Either is unacceptable over a corpus
where absence is normal — the first fails a fleet-wide scan on its
first absent value, the second lies. This engine uses the SQL rule,
pinned in `src/filter.rs` and its tests:

- A comparison on a document that LACKS the value is UNKNOWN — never
  true, never false.
- `&&`/`||`/`!` are Kleene: `False` dominates AND, `True` dominates
  OR, and NOT swaps True/False while leaving Unknown alone. Negation
  cannot launder absence into a match: a document without a court
  fails `!(court == "scotus")` exactly as it fails the positive form.
- Only a document whose WHOLE tree evaluates True survives.
- The presence tests are TOTAL — `has(col)` and `"k" in m` answer
  True or False for every document (`"k" in {}` is false in CEL, not
  an error) — so they are the one thing absence can pass, and
  `!has(col)` is the escape hatch that deliberately selects it.

A shard that lacks a column entirely evaluates every comparison on it
as the absent case, which is exact: its documents genuinely hold no
value. The same argument as geo filters, one level up.

## Numbers compare exactly, across domains

An i64 bound against an f64 column is compared AS THE INTEGER IT SAYS
(`filter::cmp_f64_i64`: piecewise over the 2^63 edges, integer part,
fraction tiebreak) — never rounded through f64, where anything above
2^53 can cross the bound. An f64 bound against an i64 column is
normalized to exact integer edges with the exclusivity folded in
(`year > 1989.5` is `year >= 1990`), in i128 so a huge float's `+1`
cannot round away. `-0.0` normalizes to `+0.0` so `total_cmp` agrees
with IEEE equality on the one pair where they differ. `min > max` is a
legal empty range that answers empty, honestly.

## The typo rule: structure refuses, data answers

Per-leaf `filter_columns_known` flags ride `Bm25QueryResponse`,
positional over the tree's depth-first leaf order (one shared walk,
`filter::walk_leaves`, so the two sides cannot disagree). The
coordinator ORs them across shards and REFUSES any leaf NO shard can
resolve, naming the leaf's table and the knob to check:

- facet leaf: the facet table has the column — so `year == "1990"`
  (string literal, integer column) refuses as "no facet column year",
  which is the kind-mismatch story: the literal's TYPE picks the
  table.
- number leaf: the i64 or f64 table has it.
- map value leaves: column AND key (a key no shard ingested is
  drill-down structure spelled wrong — the map-facet counting rule).
- `map_has_key`: column only. The key is the QUESTION being asked,
  and refusing unseen keys would make `!("k" in m)` unanswerable.
  The one deliberate departure from the counting rule.
- `has`: any family under the name.
- geo leaf: the geo table (the existing rule).

A VALUE the corpus never held is not a typo: `court == "scotsu"`
answers zero results because zero documents hold it — the true
answer, not degradation. The typo rule guards structure, never data.

A partially-known column is the heterogeneous fleet and is exact.

## Facet counts are narrowed by filters — geo included

This increment answers the question every column doc deferred: all
three facet kinds (plain, map, range) now count the FILTERED match
set. `count_facets` masks its one shared match bitmap with the same
`DocFilter::passes` the scorers gate the heap with — one filter
definition, one truth — so a drill-down never counts a document the
result set cannot contain. This narrowing applies to the standalone
`geo_filters` family too, which previously did not narrow counts;
defining the semantics once at the CEL layer instead of once per
filter kind was the reason geo waited.

## Routes

Flat and fused Bm25 both carry `filter` (forwarded verbatim, ANDed
with `geo_filters` when both are set). `VariantSearch` carries whole
requests, so A/B over filters works for free.

The HYBRID route carried no filters when this document was written,
because its vector leg had no filter machinery and silently filtering
only the lexical half would have lied about the result set. That
increment has since landed (`docs/vector-filters.md`): the vector scan
takes the same resolved predicate as a slot allowlist, so
`SearchRequest` and `HybridSearchRequest` both carry `geo_filters` and
`filter`, every fusion mode filters both legs, and — by the same
removal-only argument, with no new bounds math — hybrid facets are now
well-defined (though counting them is still its own increment).

The debug console follows: with the vector leg on, a filter rides the
hybrid route; with it off, the lexical route still wins, because it
also carries facet counts.

## Tests

- `src/filter.rs`: Kleene tables, absence semantics per leaf, exact
  cross-domain comparison at the 2^53/2^63 edges, bound
  normalization, validation refusals, walk order.
- `src/cel.rs`: every compiled construct against hand-built trees,
  CEL precedence pins, every refusal by name, RFC 3339 to
  epoch-micros at the epoch, leap day, offsets, fraction flooring.
- `tests/cel_filters.rs`: the distributed selection matrix over a
  three-shard fleet (shard 2 column-less), per-shard dictionary
  divergence, the honest-empty value typo, floor seeding under
  filters, facet narrowing (scalar AND geo), loud refusals through
  the public route, pruned == exhaustive bitwise on an impacted
  reader under a compound tree, and the **differential oracle**: on
  fully-populated documents — the domain where stock CEL is defined —
  the compiled ordinal path must agree with the `cel-interpreter`
  reference crate (a DEV dependency; the serving binary never links
  it) on every (expression, document) pair, through the full wire
  stack. Absence semantics are our documented deviation and are
  pinned by our own tests, never hidden inside the oracle.

## What this increment deliberately does not do

- **Hybrid filters** — the vector-leg allowlist was its own increment
  and has since landed (`docs/vector-filters.md`).
- **Filter-only browse** — the BM25 routes still require query terms;
  a match-all + filter route is public-API work.
- **String ranges** (`court < "b"`) — implemented on branch 2026-09-02 with the
  sorted dictionary layout (`docs/prefix-terms.md`).
- **Filter caching** — compiled trees are per-request; caching
  resolved predicates by (filter, stats_epoch) is a measurement away
  if it ever shows up in a profile.
