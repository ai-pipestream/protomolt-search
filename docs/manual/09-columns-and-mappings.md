# Columns and mappings

Every value in a document that is not analyzed text goes into a typed
column. There is one flat name space: a name belongs to one column kind, and
declaring the same name twice is rejected at startup.

## The column kinds

| Kind | Type | Declared with | Absence |
|---|---|---|---|
| facet | dictionary-encoded string | `--facet-fields` | no ordinal |
| numeric | f64 | `--numeric-fields` | NaN |
| integer | i64 | `--integer-fields` | `INT64_MIN` |
| map facet | `map<string,string>` | `--map-facet-fields` | key missing |
| map numeric | `map<string,f64>` | `--map-numeric-fields` | key missing |
| geo point | `(lat, lon)` degrees | `--geo-fields` | both parts NaN |

Columns are declared per shard and fixed once the shard is built. Growing an
existing corpus a new column means a reshard replay, not an in-place migration.

**Facet values are opaque.** They are not analyzed; a count uses the exact string
you sent. Ingesting a value for a field no node declared is INVALID_ARGUMENT naming
the field and the flag that would declare it.

**Integers get their own kind** because an f64 silently rounds past 2^53.
`INT64_MIN` is rejected: it is the absence sentinel. `TimestampValue` is ingest
sugar over an integer column: the node converts the instant to epoch
microseconds and stores it in the named integer column, so everything
downstream works in microseconds. Sub-microsecond precision is dropped. A field
valued by both `integers` and `timestamps` in one document is rejected.

**Map columns** stay one column no matter how many distinct keys the corpus
has. Wire access is structured (`MapFacetField { column, key }`), so keys need
no escaping. At most one value per document, field, and key; a repeat in one
document is rejected and not resolved last-write-wins.

**Geo points** are one column, not two. A pair with one NaN coordinate is
rejected at open as corruption. Coordinates must be finite, latitude in
[-90, 90] and longitude in [-180, 180].

## Quality columns

`AddDocumentsRequest.quality` tells the node to derive columns from the analysis
sidecar's noise and artifact layers in the same pass that produces the terms:

- `noise_column` (f64): the lowest finding's score in [0, 1], or 0.0
  when there were no findings.
- `noise_chars_column` (i64): characters covered by the union of the finding
  spans, so overlaps count once.
- `artifact_column` (i64): how many text artifacts were flagged. Detection
  only flags; the stored text is not modified.

The values are appended to the document's ordinary numeric and integer lists
before it is applied, so filters, facets, and score chains read them with no new
new mechanism. The write-ahead log keeps the derived values with the spec cleared,
so replay recreates them without calling the sidecar again.

A clean reading is a value: a document ingested under a spec always gets numbers
written. Absence means ingested without a spec. The two states are distinct on
purpose. There is no quality score op; use `SCORE_OP_MULT_EXP_DECAY` with
`origin = 0` over the noise column.

## Geography columns

`AddDocumentsRequest.geography` derives, in the same pass:

- `point_column` (geo): the highest-confidence resolved location, ties broken
  by first mention.
- `country_column` (facet): the top region vote's ISO country code, a
  document-level aggregate that may differ from the point for good reason.
- `confidence_column` (f64): that location's confidence in [0, 1].

Absence here is a real measurement. A document that mentions no resolvable place
writes no point, because there is no neutral coordinate: (0, 0) is a place in
the Gulf of Guinea. Geo filters skip it and CEL over its columns reads UNKNOWN.

A session asking for geography checks the sidecar's NER capability up front and
is rejected when no NER model is configured, because "no model" and "no places
in this document" look the same per response and would ingest an entire corpus as
place-less.

The quality spec and the geography spec do not enter the analysis fingerprint:
they change no term.

## Mappings derived from a descriptor

`SearchService.PlanIndex` takes a serialized `FileDescriptorSet` and a fully
qualified message type and returns the index plan it would bind, without
creating anything. The same descriptor set and type produce the same fields, in
the same order, with the same fingerprint, on every node and every run.

Each `MappedField` reports its dotted proto `path`, its engine `name` (the name
filters and projections use), its `kind`, whether it is repeated, its structural
`role`, and the `family` it is stored in.

- Kinds: TEXT, KEYWORD, INT32, INT64, FLOAT, DOUBLE, BOOLEAN, DATE, BINARY,
  VECTOR, OBJECT, NESTED.
- Families: TEXT_FIELD for analyzed text; FACET for keyword and boolean
  ("true"/"false"); I64 for INT32, INT64, and DATE as epoch microseconds; F64
  for float and double; VECTOR for the dense side.
- `COLUMN_FAMILY_NONE` is recorded in the plan, not silently dropped. Object,
  nested, and binary fields and repeated scalars land there, because the column
  planes hold one value per document and the engine does not guess a collapse
  rule.
- Roles: one `DOC_ID`, at most one `CHUNKS` scope, and a `CHUNK_ID`
  inside it. An integer document id is used as it is; a string id is reduced to
  the first 8 bytes of its SHA-256, big-endian, so any client can compute the
  same id.

Absence follows the engine's three-valued rule, not proto3's default-value rule:
an unset scalar stores no value, a comparison over it is UNKNOWN, and negation
cannot turn absence into a match.

Derivation rejects, it does not guess. No resolvable vector field, no
resolvable document id, an ambiguous candidate set, or a hint that contradicts
the field it is set on all fail INVALID_ARGUMENT naming the field and the hint
that would fix it.

Fields may include explicit hints as descriptor options, using the ProtoMolt
index-hint extension from the platform's `ai.protomolt` package family. A
message already annotated for ProtoMolt's indexers is understood here without
modification. The engine's own gRPC surface keeps its existing package,
`ai.protomolt.search.v1`, which is what a client generates stubs from.

The plan's `fingerprint` is a SHA-256 over a canonical encoding of the plan.
Two engines agree on their mapping when, and only when, the fingerprints agree. Mapped
ingest requires you to pass the fingerprint you reviewed: dry-run first, then
bind what you saw.

## Projections and materialized columns

Both use one CEL value dialect: column reads (a dotted name is one name, map access by
string-literal key), int, double, bool, and string literals, `+ - * / %`, unary
minus, `double()`, comparisons, three-valued `&& || !`, the ternary
`cond ? a : b`, and the `math.*` and `engine.*` functions.

**Projections** are `QueryRequest.projections` (or
`Bm25SearchRequest.projections`): named expressions computed per returned hit,
after selection, over that hit's own columns. They are compiled once at the
coordinator and resolved per shard. A projection does not select and does not rank;
it annotates. Values come back on each hit in request order, and an unset value
means absence.

**Materialized columns** are `MaterializeSpec` on the ingest stream: derived
columns computed once from the document's own values and stored as ordinary
typed columns. Each names a target family (`MATERIALIZE_KIND_F64` or
`MATERIALIZE_KIND_I64`) and the expression's evaluated type must match, so a
column's family cannot drift with the data. Write `double(...)` to land an
integer expression in the f64 family. String-typed expressions are rejected: a
bare column copy stores no new value. A name colliding with a column the request
already sends rejects the document and does not overwrite it.

The write-ahead log stores the document with the derived values already in place
and the spec cleared, so replay does not re-evaluate.

Reference: `docs/descriptor-mappings.md`, `docs/cel-values.md`.
