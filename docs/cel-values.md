# First-class CEL values: projections and materialized columns

Status: implemented (increment 1). CEL selects (`docs/cel-filters.md`)
and function chains score (`docs/score-functions.md`); this document
covers the third and fourth uses of the same compiler: expressions that
PRODUCE values. Query-time **projections** compute named values per
returned hit; ingest-time **materialized columns** compute derived
values once per document and store them as ordinary typed columns.
Neither is new evaluation machinery — both compile once, resolve per
shard (or per document at ingest), and never interpret anything.

## 1. The dialect

A value expression is:

- **Column reads.** A bare or dotted identifier (`price`,
  `meta.page_count` — dots are just characters in the flat column
  namespace) reads the f64, i64, or facet column of that name. Map
  entries read with a string-literal key: `prices["usd"]`.
- **Literals.** Integer and double literals, with CEL's types: `7` is
  an int, `7.0` a double. NaN and infinity do not lex.
- **Arithmetic.** `+ - * / %` with CEL precedence, plus unary minus and
  parentheses. `%` is integer-only, as in CEL.
- **`double(x)`.** The one conversion: int to double (identity on
  double). There is no `int()`, no `uint()`, no string conversion.

Everything else refuses **by name** at compile: comparisons, `&&`/`||`,
`!`, the ternary, `has()`, `in`, string functions, `matches()`, unknown
functions, string literals, lists. The refusal names the construct and,
where one exists, the supported alternative.

## 2. Typing: stock CEL's, finished per shard

CEL does not coerce: int arithmetic with a double is an error, so it is
a refusal here — `price + year` refuses naming `double()`; write
`price + double(year)`. The engine checks this in two stages, because
the coordinator does not know column kinds:

- **Compile time** (coordinator, or the node for materialization):
  conflicts literals already pin down (`1 + 2.0`, `3.5 % 2.0`) refuse
  immediately.
- **Resolution time** (each shard, per request): column reads take the
  kind of the table they resolve in — i64 columns are ints, f64 and
  map-numeric columns are doubles, facet and map-facet columns are
  strings — and the remaining checks finish there. A string column
  joins no arithmetic and does not convert; a bare facet read is legal
  as the WHOLE expression and projects the value string.

A name that resolves in more than one family on a shard is refused as
ambiguous — a value read has no predicate context to disambiguate with.

## 3. Absence, and the two documented deviations

Values inherit the engine's Kleene rule (`docs/cel-filters.md`): an
absent input makes the result absent, and an absent result is the unset
oneof on the wire — never a fabricated zero, never proto3's default.

Two places deliberately deviate from stock CEL, both pinned in
`tests/cel_values.rs`:

- **Missing inputs.** Stock CEL errors on an unbound variable. The
  engine answers ABSENT: over a corpus where absence is normal, a
  per-document error would make every projection partial-failure.
- **Integer arithmetic errors.** Stock CEL errors on i64 overflow,
  division by zero, and the `i64::MIN` edge cases. The engine's checked
  arithmetic answers ABSENT.

Everywhere stock CEL yields a VALUE, the engine yields the same value —
bit-for-bit on doubles. `tests/cel_values.rs` runs the `cel-interpreter`
reference (a dev-dependency the serving binary never links) over an
(expression × document) matrix through the full wire stack and asserts
exactly that. Double division follows IEEE on both sides: `x / 0.0` is
a signed infinity and `0.0 / 0.0` is NaN — values, not errors.

## 4. Query-time projections

`Bm25SearchRequest.projections` carries `(name, expression)` pairs;
names are request-unique and non-empty. The coordinator compiles each
expression once into the `ValueExpr` IR and fans the compiled trees out
(`Bm25QueryRequest.projections`); shards resolve against their own
tables and evaluate per RETURNED hit — k evaluations, after selection,
never per candidate, so projections cannot perturb pruning, floors, or
any completion certificate. Values ride each hit
(`Bm25Hit.projected`, aligned with the request order) through the
ordinary merge.

The filter rules for missing columns carry over exactly:

- A column a SHARD lacks reads absent for every document there — exact,
  its documents hold no such value.
- A column NO shard knows is a typo: the coordinator refuses it by
  name (`projection_leaves_known`, the same contract as filter leaves,
  reported positionally over column-read leaves in expression order,
  depth-first within each expression).

Projections are served on the flat (single-field) Bm25Search route;
the fused multi-field route refuses them by name, like stats and
cardinality. The public `Query` adapter (`docs/query-api.md`) carries
`QueryRequest.projections` on the single-lexical-leaf shape — the
Bm25Search delegate — and refuses other shapes until their ordinary
route serves values; hits return them as `QueryHit.projected`.

## 5. Ingest-time materialized columns

`AddDocumentsRequest.materialize` declares derived columns:
`(name, expression, kind)`, kind one of `MATERIALIZE_KIND_F64` /
`MATERIALIZE_KIND_I64` — **explicit, never inferred**, so the column's
family cannot drift with the data. The expression's evaluated type must
match per document: an int result into an F64 column refuses naming
`double(...)` as the fix.

Materialization is materialize-then-ordinary-path, the same shape the
quality and geography layers use (`docs/quality-columns.md`):

1. The node compiles the spec's expressions once per spec CHANGE
   (cached against spec equality), never per document.
2. Per document — after the quality and geography layers, so their
   derived columns are readable inputs — each expression evaluates
   against the document's OWN values: its `numerics`, `integers`, and
   `map_numerics` by name. Facet strings are not inputs;
   materialization computes numbers.
3. Results are pushed into the request's ordinary `numerics` /
   `integers` lists, so name resolution, the duplicate-column refusal,
   the declared-table check, the apply, and the WAL record all take the
   one path they already took. The target name must be a declared
   column (`--numeric-fields` / `--integer-fields`), like any other.
4. The spec is cleared before the WAL logs the document: the logged
   request carries the values, so **replay never evaluates twice** and
   a later spec change cannot silently rewrite history.

An absent result stores nothing (Kleene again). One storage edge: a
computed i64 equal to `i64::MIN` — the i64 column's absence sentinel —
stores as absent, the same edge the checked arithmetic already maps to
absence. A name the document does not carry is absent, not a typo:
at ingest there is no fleet-wide table to check a spelling against, and
the declared-column check on the OUTPUT name is where typos surface.

Changing a materialization expression changes what an index means, so
it is an index compatibility event — rebuild, not mutate. (Binding
expressions to a fingerprinted mapping is the descriptor-mappings
integration, `docs/descriptor-mappings.md` section 4, which this
increment's machinery was built to serve.)

## 6. What this is not

- Not a scripting engine: the vocabulary is arithmetic and one
  conversion, extended deliberately or not at all.
- Not a scoring path: projections annotate hits; they cannot change
  rank. Scoring stays with function chains, whose bounds math is
  argued in `docs/score-functions.md`.
- Not interpreted: `cel-interpreter` remains a test-only oracle. The
  serving binary compiles, resolves, and runs array reads.
