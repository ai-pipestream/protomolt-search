# First-class CEL values: projections and materialized columns

Status: implemented; uint values added on the unsigned feature branch
(2026-09-05). CEL selects (`docs/cel-filters.md`)
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
  namespace) reads the f64, i64, u64, or facet column of that name. Map
  entries read with a string-literal key: `prices["usd"]`.
- **Literals.** Integer and double literals, with CEL's types: `7` is
  an int, `7u` a uint, `7.0` a double. Decimal and hexadecimal uint literals
  cover zero through `18446744073709551615u`. NaN and infinity do not lex.
- **Arithmetic.** `+ - * / %` with CEL precedence, plus parentheses and
  unary minus for signed numbers. Unary minus refuses uint. `%` is
  integer-only, as in CEL.
- **`double(x)`.** The one conversion: int or uint to double (identity on
  double). Integer values above 2^53 can lose precision during this explicit
  conversion. There is no `int()`, no `uint()`, no string conversion.
- **The conditional layer** (2026-08-27). Comparisons
  (`== != < <= > >=`), Kleene `&&`/`||`/`!`, bool literals, and CEL's
  ternary `cond ? a : b` — the full conditional grammar, at CEL's
  precedence (`?:` lowest and right-associative, then `||`, `&&`,
  relations, arithmetic). Comparisons follow the arithmetic typing
  rule (int with int, uint with uint, double with double, mixed refused naming
  `double()`); doubles compare IEEE, so every comparison with NaN is
  false except `!=`. `==`/`!=` also compare a DIRECT facet or
  map-facet read against a string literal — resolved per shard to a
  dictionary-ordinal check, so a literal the dictionary lacks compares
  FALSE against every present value (and stays absent for absent
  ones). String ordering, bool ordering, comparing two string columns,
  and `in` refuse by name. A bool is a first-class projected value;
  the ternary's branches must agree on one type, and only the TAKEN
  branch's value and absence matter.

- **The function vocabulary** (2026-08-27). `math.*` carries the
  official CEL math extension's names and semantics, so stock CEL
  tooling with that extension agrees with the engine: `math.abs`,
  `math.sign` (type-preserving, NaN stays NaN), `math.greatest` /
  `math.least` (n-ary, type-preserving, a double NaN propagates),
  `math.ceil` / `math.floor` / `math.trunc`, `math.round` (half away
  from zero, CEL's rule), `math.sqrt`, and the three predicates
  `math.isNaN` / `math.isInf` / `math.isFinite`. `engine.*` is this
  engine's own transcendental namespace — `engine.ln`, `engine.exp`,
  `engine.log10`, `engine.pow(x, y)` — deliberately OUTSIDE `math.*`
  so the official namespace stays exactly the official extension.
  Typing is the house rule: `abs`/`sign`/`greatest`/`least` preserve
  one agreed numeric type (int, uint or double); everything else takes doubles only (integers
  convert with `double()`). Results are IEEE values, never errors —
  `math.sqrt(-1.0)` is NaN, `engine.ln(0.0)` is -inf — with one
  integer edge: `math.abs(i64::MIN)` overflows, so it evaluates
  ABSENT where stock CEL errors, the checked arithmetic's own
  deviation. Absence is Kleene through every argument. The `math` and
  `engine` names are reserved as call receivers; columns of those
  names still read (a bare read has no parentheses).

Everything else refuses **by name** at compile: `has()`, `in`, string
functions, `matches()`, unknown functions (`math.foo()` refuses naming
`math.foo`), bare string literals (a string literal is legal only as a
`==`/`!=` operand), lists. The refusal names the construct and, where
one exists, the supported alternative.

## 2. Typing: stock CEL's, finished per shard

CEL does not coerce: int arithmetic with a double is an error, so it is
a refusal here — `price + year` refuses naming `double()`; write
`price + double(year)`. The engine checks this in two stages, because
the coordinator does not know column kinds:

- **Compile time** (coordinator, or the node for materialization):
  conflicts literals already pin down (`1 + 2.0`, `3.5 % 2.0`) refuse
  immediately.
- **Resolution time** (each shard, per request): column reads take the
  kind of the table they resolve in — i64 columns are ints, u64 columns are uints, f64 and
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
- **Integer arithmetic errors.** Stock CEL errors on integer overflow, unsigned underflow,
  division or remainder by zero, and the `i64::MIN` edge cases. The engine's checked
  arithmetic answers ABSENT.

The conditional layer needs no third deviation, because Kleene logic
IS stock CEL's logic with absence in the error role: CEL's `&&`/`||`
are commutative and absorb an error when the other operand determines
the answer, so `false && absent` is false and `true || absent` is true
— and only an undetermined absent operand makes the result absent. An
absent ternary condition makes the result absent, and `!` of absent
stays absent, matching CEL's error propagation the same way. These are
pinned in `tests/cel_values.rs` alongside the two deviations.

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
ordinary merge. Each shard also reports `Bm25QueryResponse.projection_types`,
one resolved scalar type per requested projection, even for zero hits or k=0.
The same metadata travels in the streaming completion response. Coordinators
and relays join an absent type with a known type, refuse conflicting concrete
types, and validate each returned value against its originating shard's type.
An unsigned column on one shard and a signed or double column on another cannot
silently produce a mixed projection. Explicit `double()` remains a conversion.
Queries whose analysis produces no terms still validate requested projections.

Regenerate clients and use matching node, relay and coordinator builds. Older
nodes omit the new metadata and projected queries refuse with
`FAILED_PRECONDITION`; queries without projections retain their wire behavior.
The change does not alter stored index formats.

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
`QueryRequest.projections` on EVERY shape (2026-08-26): the
single-lexical-leaf shape rides the Bm25Search delegate natively, and
every other shape — dense, composite, browse — fetches the paged hits'
values post-selection through the candidate-scoped `FetchValues` seam
(`NodeService.FetchValues`), where the selection is already fixed and
no pruning certificate is involved. Semantics are identical on both
paths (one compile, one resolve rule, the same typo refusal), and
`tests/cel_values.rs` holds the browse path to the lexical route's
values; hits return them as `QueryHit.projected`.

## 5. Ingest-time materialized columns

`AddDocumentsRequest.materialize` declares derived columns:
`(name, expression, kind)`, kind one of `MATERIALIZE_KIND_F64` /
`MATERIALIZE_KIND_I64` / `MATERIALIZE_KIND_U64` — **explicit, never inferred**, so the column's
family cannot drift with the data. The expression's evaluated type must
match per document: an int result into an F64 column refuses naming
`double(...)` as the fix.

Materialization is materialize-then-ordinary-path, the same shape the
quality and geography layers use (`docs/quality-columns.md`):

1. The node compiles the spec's expressions once per spec CHANGE
   (cached against spec equality), never per document.
2. Per document — after the quality and geography layers, so their
   derived columns are readable inputs — each expression evaluates
   against the document's OWN values: its `numerics`, `integers`, `unsigned_integers`, and
   `map_numerics` by name. Facet strings are not inputs;
   materialization computes numbers.
3. A BOOL result never stores — the refusal names the ternary
   (`cond ? 1 : 0`) as the fix. The conditional layer's use at ingest
   is bucketing (`year >= 1994 ? 1 : 0`) into an ordinary numeric
   column.
4. Numeric results are pushed into the request's ordinary `numerics` /
   `integers` / `unsigned_integers` lists, so name resolution, duplicate-column refusal,
   the declared-table check, the apply, and the WAL record all take the
   one path they already took. The target name must be a declared
   column (`--numeric-fields` / `--integer-fields` / `--unsigned-integer-fields`),
   like any other.
5. The spec is cleared before the WAL logs the document: the logged
   request carries the values, so **replay never evaluates twice** and
   a later spec change cannot silently rewrite history.

An absent result stores nothing (Kleene again). Every computed i64 value,
including `i64::MIN`, is stored with explicit presence. Arithmetic overflow
still evaluates absent; a valid minimum integer does not. A name the document
does not carry is absent, not a typo:
at ingest there is no fleet-wide table to check a spelling against, and
the declared-column check on the OUTPUT name is where typos surface.

The corrected I64 presence behavior has a versioned materialization hash.
Mapped binds with an I64 output refuse their old hash, even for the same
expression; rebuild from original documents to recompute the previously
omitted values. F64-only specs retain their old hash. Replaying already-derived
WAL records preserves their recorded values; it does not repair old omissions.

Changing a materialization expression changes what an index means, so
it is an index compatibility event — rebuild, not mutate. Mapped
ingest carries the spec on its bind (`MappedBind.materialize`,
`docs/descriptor-mappings.md` section 4a), so derived columns compute
from a protobuf document's own mapped values with the same contract.

## 6. What this is not

- Not a scripting engine: the vocabulary is arithmetic, the
  conditional layer, the math functions, and one conversion, extended
  deliberately or not at all.
- Not a scoring path: projections annotate hits; they cannot change
  rank. Scoring stays with function chains, whose bounds math is
  argued in `docs/score-functions.md`.
- Not interpreted: `cel-interpreter` remains a test-only oracle. The
  serving binary compiles, resolves, and runs array reads.

## Unsigned value contract (2026-09-05, feature branch)

`ValueExpr.uint_literal` (field 15), `ProjectedValue.uint_value` (field 5),
and `MATERIALIZE_KIND_U64` (enum value 3) are additive. Existing field numbers
and wire types are unchanged. An unset projected oneof means absence; a
present `uint_value: 0` is zero. Unsigned materialized outputs use the existing
u64 storage/WAL family and survive reopen and compaction without narrowing.

The node checks materialization expressions against declared numeric families
before storing a binding or accepting documents, including when an input is
absent. The shared evaluator also checks the actual document's types, including
untaken branches, before evaluation. Untaken arithmetic overflow remains local
to that branch. Outputs must match their declared family; there is no implicit
signed/unsigned conversion. Inputs are the original request values; expressions
do not consume earlier outputs from the same materialization spec.

The uint forms of `math.abs` and `math.sign` follow the
[CEL math extension](https://github.com/cel-expr/cel-go/blob/master/ext/math.go).
The engine retains its documented single-type rule for comparisons, ternaries
and `greatest`/`least`, and requires explicit double conversion for the other
math functions. Unsigned sorting and collapse are supported with typed keys and results
([query contract](query-api.md#sorting)). Unsigned aggregate accumulators and exact percentiles preserve uint results
([aggregation contract](aggregations.md#11-unsigned-aggregates-2026-09-05-feature-branch));
range facets and scoring remain separate work.

`tests/unsigned_values.rs` compares arithmetic against an independent u128
oracle and stock CEL where CEL produces values. It covers numeric boundaries,
wire presence, typing, branch evaluation and distributed projection reads across
reopen and compaction. `tests/unsigned_mapping.rs` also exercises materialized
uint outputs from descriptor-mapped ingest while preserving original source bytes.

`tests/bm25_projection_types.rs` exercises the same projected queries through
flat, one-level and nested relay topologies, with unary and streamed shard
responses. It checks exact full-range uint values, missing columns, conflicting
int/uint/double declarations even with empty match sets, and malformed child
metadata and row widths. Candidate-scoped fetches use the same type and row
validators. Fused multi-field projections remain explicitly unsupported.
