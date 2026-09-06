# Descriptor-derived mappings and the descriptor exchange contract

The decoder and fingerprint were revised after `PRE_ASTRA`; see
[Search foundations](search-foundations.md) for migration requirements and the
remaining preservation, indexing and query contracts. In particular, the older
reference description below is historical evidence, not complete shape support.

Status: **increments 1 and 2 implemented** (2026-08-25) — dry-run
derivation, and bind + protobuf-native ingest (section 4a).
`SearchService.PlanIndex` derives the deterministic, fingerprinted plan
for one message type inside a serialized FileDescriptorSet
(`src/mapping.rs`, `tests/descriptor_mappings.rs`): kinds inferred with
protomolt's rules, explicit `(index)` hints read off field options (the
raw descriptor bytes are walked by hand for the extension payloads,
because prost drops extensions — the same hand-rolled-parser posture as
the CEL front-end), each field mapped onto the engine column family it
would land on (`ColumnFamily` — repeated scalars and OBJECT/NESTED/
BINARY fields visibly map to FAMILY_NONE, never silently dropped), and
the whole plan identified by a lowercase-hex SHA-256 over a canonical
fixed-layout encoding (compatibility tag `pipestream-search.plan.v3`,
including the reachable wire schema; hash from
the hand-rolled `src/sha256.rs`, pinned to the NIST vectors). Every
refusal in section 2 is implemented and pinned by tests: ambiguous or
missing vector/doc-id candidates, contradictory hints, chunk-scope
violations, range/TREE_PATH/chunking-policy hints, conflicting
extension declarations. Increment 2 binds and ingests (section 4a);
the stored format is a later increment. The ingest-time CEL machinery
the bind carries landed with `docs/cel-values.md`.

The exchange contract is now drafted and vendored (section 5). Increment 2 landed the same day: binding and
protobuf-native ingest (`NodeService.IngestMapped`, section 4a) — the
documents stream as the serialized protobuf messages they already are
and reduce, by walking their wire bytes against the plan, onto the
ORDINARY ingest path. The durable shard-level binding landed with it
(the first bind pins a shard to its plan across restarts), and chunk
scopes followed: a chunked plan ingests one engine row per chunk, with
parent fields denormalized and the engine's existing parent-collapse
keyed by the reduced parent id (section 4a). The original framing,
kept:

The ownership move is decided (descriptor-derived mappings belong to
pipestream-search, not turbovec-grpc), and the reference implementation
is frozen in turbovec-grpc git history.

## Scalar wrappers (2026-09-05, feature branch)

Singular `DoubleValue`, `FloatValue`, `Int64Value`, `UInt64Value`, `Int32Value`,
`UInt32Value`, `BoolValue` and `StringValue` messages project their scalar value
at the containing field's declared path. For example, `counter` projects to a
u64 column rather than `counter.value` to `counter_value`. Type, name and
identity hints belong on the containing field. String kind inference uses that
field's name, so a wrapped `status` is a keyword while a wrapped `body` is text.
Numeric keyword hints retain decimal rendering and integer key reduction.
Boolean wrappers use the existing `true`/`false` string facet representation.
TEXT wrappers index analyzed terms; scalar presence and value expressions need a
KEYWORD projection. Original bytes distinguish absent and empty text regardless
of whether either produces terms. Floating wrappers use finite f64 columns.
`BytesValue`, repeated wrappers, maps and explicit OBJECT/NESTED/BINARY hints
remain source-only under the current column contract. SKIP retains the original
without a projected value. These are the [standard protobuf wrappers](https://protobuf.dev/reference/protobuf/google.protobuf/).

Absence of the wrapper is missing. A present empty wrapper projects the scalar
default, including zero, false and the empty string. Message merging and oneof
selection finish before extraction. Empty facet strings are now accepted and
remain distinct from absent values through filtering, projection, counting,
WAL replay and compaction; facet storage already encodes absence separately.
The original protobuf retains wrapper presence, bytes, unknown fields and
unindexed members. Scalar wrappers must have the expected `value = 1` component,
scalar type, default and optional cardinality without a oneof. Incompatible
named descriptors refuse projection while remaining available to DescribeSchema.
Hints on wrapper or Timestamp component fields refuse rather than being ignored.

DOC_ID and CHUNK_ID require keyword or integer value projections. A string
identity role with an unspecified kind infers KEYWORD; explicitly requesting
TEXT or a source-only kind refuses during planning. Wrapped identities work in
flat and chunked plans, with exact unsigned bits and string-key hashing.
This does not supply catalog identity or publication receipts to legacy ingest.

This changes wrapper paths, names and sometimes kinds, so their canonical v3
plan fingerprints change. Rebuild existing wrapper bindings from original
sources and use the new plan's column names. No stored format changes are needed,
and unrelated plans retain their fingerprints. Report version 2 identifies
wrapper and Timestamp components as INPUT paths, with `value_path` identifying
the consuming query value. See [schema reports](schema-report.md).

Analysis-name hints remain recorded rather than resolved. MappedBind currently
provides an explicit AnalysisSpec only for the body. Native embedded analysis
therefore supports the wrapped body and scalar columns but refuses populated
non-body text without an explicit specification; the current mapped API cannot
supply one. Per-field analysis configuration remains required for complete
mobile mapped indexing. The lifecycle conformance test uses the supported
sidecar path for nested text; a separate embedded test exercises native body
analysis and wrapper scalar defaults.

## Timestamp projection validation (2026-09-05, feature branch)

A DATE projection validates the descriptor's `seconds` field as int64 number 1
and `nanos` as int32 number 2, both singular optional fields with default zero
and no oneof membership. A matching type name alone is insufficient. Compatible
proto2 descriptors are accepted; filenames, language options and extra source
fields do not alter these requirements. Source-only description remains
available for incompatible schemas. The extractor no longer substitutes zero
for missing or incorrectly typed descriptor components.

Mapped and direct timestamp ingest enforce protobuf's years 0001 through 9999
and nonnegative nanos below one billion. An absent Timestamp remains absent;
an explicitly present empty message is the epoch. Components merge before
validation and projection. Queries retain the existing floor-to-microseconds
contract; original source bytes retain nanoseconds.

Valid projections, stored formats and plan fingerprints are unchanged. New
planning refuses incompatible DATE descriptors. Previously accepted out-of-range
instants in images are not rewritten; WAL replay containing them now refuses.
Correct such source data and rebuild its generation instead of normalizing it
silently. Unaffected generations do not need a rebuild for this validation fix.

## Unsigned numeric mapping (2026-09-05, feature branch)

Unhinted `uint32`/`fixed32` fields derive `UINT32`; `uint64`/`fixed64` derive
`UINT64`. Singular values land in the separate `U64` column family, including
zero and the full unsigned maximum. Declare those columns with
`--unsigned-integer-fields` (or the corresponding Rust/mobile configuration).
The extractor preserves optional presence, oneof selection, implicit defaults
and nested-message presence. Parent and chunk ID reduction retains unsigned
bits. Repeated scalars and maps remain source-only unless their existing
structural contract provides a projection; unsigned support does not invent a
reduction rule.

Explicit `INT32`/`INT64` hints retain the existing signed i64 column contract;
a value above `i64::MAX` refuses. Explicit keyword hints continue to produce
exact decimal strings. Shared ProtoMolt hints are unchanged. This change is in
the search product's inference and column contracts.

Kind and family are already included in the canonical v3 hash, so an inferred
unsigned projection acquires a different fingerprint. Existing signed and
explicit keyword projections retain their hashes when no inferred unsigned
field changes. Old inferred-unsigned bindings require a new generation built
from original messages and reviewed against the new plan. The node refuses
both the old fingerprint and a configuration that declares only signed columns.
The legacy fingerprint fixture comes from the mapper at `d0a1716`, and the
existing signed plan's pinned fingerprint remains a regression gate.

Planning also refuses two projected fields that land the same column name,
including flattened paths and parent/chunk collisions. Set distinct indexing
`name` hints to resolve the error; the planner names both paths. Source-only
schema description remains available for these descriptors. Previously bound
ambiguous plans need corrected hints and a new generation.

CEL unsigned filters, presence tests, typed projections, checked arithmetic and
`MATERIALIZE_KIND_U64` outputs preserve the full unsigned domain. `double()` is
an explicit, potentially lossy conversion. Materialization validates declared
input types before documents arrive, so a uint input assigned to an I64 output
refuses even when the input is absent. Unsigned sorting and collapse are
supported, as are exact unsigned scalar aggregates and percentiles. The schema
report names the remaining unsigned range-facet and scoring limitations and the
explicit conversion required by statistical folds. See [the value dialect](cel-values.md)
and [aggregations](aggregations.md). Legacy
`IngestMapped` retains original bytes but does not publish catalog
`DocumentIdentity` or the source-authority write receipts. The mapped lifecycle
test verifies unsigned key values, filtering, and byte-preserving source
storage across compaction; it does not establish those remaining identity and
publication contracts.

## 1. The layering rule

Three things are easy to conflate and must not be:

1. **Descriptors are vocabulary.** A `google.protobuf.FileDescriptorSet`
   says what fields a message type has and what their proto types are.
   The shared gRPC descriptor-exchange contract (protomolt's
   `ai.protomolt.proto.schema.registry.v1`, `descriptor_exchange.proto`)
   is only a way to move those bytes around: nouns `DescriptorSet` and
   `DescriptorSetVersion`, verbs Register/Get/List plus a bidirectional
   Sync, content-addressed by SHA-256, descriptor bytes opaque.
2. **Mappings are policy.** Which fields become columns, which field is
   the document id, what a chunk is, which analyzer a text field uses:
   that is engine-local derivation, owned here, versioned here, and never
   dictated by the exchange contract. The contract ships vocabulary; it
   does not ship decisions.
3. **CEL is the engine's own compiled dialect.** The selection and
   projection language on the serving path is the hand-rolled compiler in
   `src/cel.rs`, which compiles expression text once into the
   `FilterExpr` IR and never interprets anything per document. Stock CEL
   is a conformance reference the test suite holds us to, not a library
   we link.

The dependency consequence is the point of the layering: pipestream-search
has **no dependency on ProtoMolt code**, compile-time or runtime. It vendors
three ProtoMolt vocabulary protos (section 5), exactly as it vendors the
OpenNLP sidecar's `analysis.proto`, and it consumes the exchange service as
one gRPC client among any. ProtoMolt is a future client of pipestream-search,
and any other system that can register a FileDescriptorSet is equally a
client. The product's own protocol and ranking path remain independent of
ProtoMolt implementation code.

## 2. What the reference implementation provides

turbovec-grpc carried a transitional protobuf `Documents` service that
was removed from its `main` when the product boundary was corrected
(commit `68910cb`; the full implementation is preserved in history
immediately before it). It is the reference for this port. What it
proves:

- **Deterministic plan derivation.** From a serialized FileDescriptorSet
  plus a message type name it derives an `IndexSchema`: an ordered list
  of `PlannedField`s with a resolved `FieldKind` (TEXT, KEYWORD, INT32,
  INT64, FLOAT, DOUBLE, BOOLEAN, DATE, BINARY, VECTOR, OBJECT, NESTED),
  structural `FieldRole`s (DOC_ID, CHUNKS, CHUNK_ID), and a lowercase
  SHA-256 fingerprint over the canonical plan encoding. Two schemas are
  the same schema exactly when their fingerprints agree — the same
  discipline this engine already applies to analysis fingerprints.
  Derivation refuses by name when it cannot resolve a vector field or a
  document id without guessing; it never picks one of several candidates
  silently.
- **Protobuf-native ingest.** `AddDocuments` is a client stream of
  serialized protobuf messages of the bound type. The node decodes each
  document against the bound descriptor (via `prost-reflect`), extracts
  vector, id, and planned scalars, and fails the stream by position on
  the first document that does not decode. Nothing is transcoded to JSON
  or any intermediate document model, so there is no mapping layer for
  field types to drift in. The document-id reduction (integer verbatim;
  string reduced to the first 8 bytes of SHA-256, big-endian) is part of
  the contract, so any client can compute the same id.
- **Chunk scopes and parent collapse.** A repeated message field marked
  CHUNKS plans its children as their own fields; chunk rows denormalize
  parent scalars so a filter sees parent and chunk fields together with
  no query-time join. `SearchDocuments` can collapse to top-k parents,
  and `GetParents` resolves parent membership per shard for the
  coordinator's union.
- **A durable persisted format.** `stored_documents.proto` writes one
  `StoredDocumentSet` (and `StoredParentSet` for chunked schemas) per
  shard generation, values keyed by planned-field ordinal, verified by
  size and CRC against the generation manifest on restore, and bound to
  the schema fingerprint: a set only ever pairs with the plan it was
  written under, and a derivation drift on restart is an index
  compatibility event, not a warning.

## 3. What changes in the port

The reference is migration material, not a drop-in. The differences are
deliberate:

- **CEL dialect.** The reference linked the `cel` crate (v0.14) and
  *interpreted* each filter expression per document against stored
  values. This engine's serving path does the opposite: the coordinator
  compiles the expression once (`cel::compile_filter`) into the
  `FilterExpr` IR, every shard resolves names and values against its own
  dictionaries (column names to table indices, strings to ordinals), and
  evaluation is array reads and integer compares at the heap gate
  (`docs/cel-filters.md`). The port targets that compiler. The `cel`
  crate does not enter this repository's dependency tree; what does not
  compile does not run slowly, it does not run.
- **Presence semantics.** Explicit presence follows the descriptor: unset
  optional fields and inactive oneof members remain missing, while explicitly
  present defaults are projected. Proto3 implicit-presence scalars project their
  defaults whether omitted or encoded. They cannot distinguish an unset value
  from a deliberately assigned default. Missing values still use the engine's
  Kleene three-valued comparisons (`src/filter.rs`).
- **Storage target.** Mapped fields land on the existing typed column
  plane — facet, i64, f64, map, and geo families with their shard-local
  dictionaries — rather than a parallel per-document value store. Where
  a planned kind has no existing family (DATE arrives as epoch micros in
  the i64 family; repeated scalars arrive as map or list columns), the
  mapping names the family it writes. The ordinal-keyed persistence idea
  from `stored_documents.proto` survives in spirit: stored bytes pair
  with one schema fingerprint, covered by the same section-CRC integrity
  table every other section carries.
- **Distributed shape.** Compilation happens once on the coordinator and
  the resolved IR fans out, like every CEL filter today. The schema
  fingerprint rides ingest and query the way analysis fingerprints
  already do: a mismatch is an error, not degraded search.
- **Exactness invariants are untouched.** A filter or projection only
  removes or annotates documents; floors, completion certificates, and
  the pruned-versus-exhaustive bitwise equivalence all stand, by the
  same removal-only argument the geo increment used.

Chunk scopes landed by reusing what the engine had (resolved as the
design note wanted): chunk rows carry ordinary lineage records whose
`parent_id` is the REDUCED parent id, so the existing parent-collapse
scans group mapped chunks with no new machinery and no imported parent
tables. The reference's id-reduction contract (integer verbatim;
string reduced to the first 8 bytes of SHA-256, big-endian) is what
makes the parent key computable by any client.

## 4. First-class CEL: the extension surface

CEL today is a filter language: it selects, and function chains score
(`docs/score-functions.md`). Descriptor-derived mappings extend the same
compiler in two directions. Both are new *uses* of compiled CEL, not new
evaluation machinery:

- **Ingest-time materialization.** A mapping may declare a derived
  value: a CEL expression over the document's own fields, computed once
  per document at ingest and stored as an ordinary typed column. Because
  the result is an ordinary column, filters, facets, and score-function
  chains over it need no new machinery and no new bounds math. The
  expression text is part of the mapping, so it is covered by the
  mapping's fingerprint the way analyzer identity is covered by the
  analysis fingerprint: changing a materialization expression is an
  index compatibility event — a rebuild, never a silent behavior change.
- **Query-time projections.** A request may ask for computed values per
  hit, compiled per request by the same front-end and evaluated over
  resolved columns after selection. Projections produce values rather
  than predicates, which admits a new leaf family in the IR (column
  reads plus pure scalar functions over them), but the serving rule is
  unchanged: compile once, resolve per shard, never interpret per
  document. A construct that does not compile is refused by name.

Both directions inherit the compiler's refusal list (arbitrary string
functions, regex, comprehension macros, and the rest, per
`docs/cel-filters.md`) until a construct is deliberately added, and both
are held to stock-CEL agreement by the existing differential oracle:
`tests/cel_filters.rs` runs the `cel-interpreter` reference crate — a
dev dependency the serving binary never links — over every (expression,
document) pair on fully-populated documents, through the full wire
stack. The oracle extends to materialization and projection expressions
with the same split: wherever stock CEL is defined, the compiled path
must agree with it; the three-valued absence semantics remain our
documented deviation, pinned by our own tests, never hidden inside the
oracle.

## 4a. Bind and ingest, as landed

`NodeService.IngestMapped` is a client stream: the first message is a
`MappedBind`, every later message is one serialized protobuf document
of the bound type. The contract, piece by piece:

- **Binding is agreement on the plan.** The bind carries the
  descriptor set, the message type, and the REQUIRED
  `expected_fingerprint` — the fingerprint the client reviewed on its
  PlanIndex dry run. The node re-derives the plan locally (derivation
  is deterministic, so both sides compute it independently) and
  refuses a mismatch naming both fingerprints. An empty expected
  fingerprint refuses too: dry-run first, then bind what you saw.
- **The bind stands or nothing streams.** Landing columns are checked
  against the shard's declared tables up front — every gap named in
  one message with its flag (`--facet-fields`, `--integer-fields`,
  `--numeric-fields`, `--bm25-fields`) — along with the body choice:
  `body_path` picks which TEXT field is the stored body (optional
  exactly when the plan has one), and the remaining TEXT fields index
  as ordinary multi-field columns. A chunked plan, a plan with no TEXT
  field, and a TEXT-kind document id all refuse at bind.
- **Decode before projection.** `prost-reflect` validates the descriptor and
  the merge adapter in `src/protobuf.rs` decodes the whole message before the
  compiled field-number projection reads
  values. Oneof alternatives replace each other, repeated values concatenate,
  and singular submessages merge before indexing. Invalid wire data, including
  malformed unindexed known fields, refuses the document. Explicitly present
  empty strings are retained. Required fields are checked after merging,
  including unindexed fields and present extensions. Closed-enum unknowns do
  not replace existing values or select oneofs. Open-enum unknowns project as
  decimal facet strings; declared numbers use the first declared alias.
  Singular proto2 groups expand like messages. Unsigned values above i64::MAX
  still refuse in signed columns.
  Timestamp and vector projections retain their existing units and narrowing.
  The decoder does not make source preservation or every shape queryable; see
  the foundation completion requirements.
- **The ordinary path does the rest.** Each decoded document becomes an
  ordinary `AddDocumentsRequest` — the bind's `analysis` and CEL
  `materialize` specs attached as session properties — and enters the
  same streaming analysis session, the same column validation, the
  same apply, the same WAL record. Replay never needs the descriptor:
  the log carries the reduced values.
- **A chunked plan ingests one engine row per chunk.** The searchable
  rows are the chunks: the body must be a TEXT field inside the CHUNKS
  scope (`body_path` picks it when the scope has several), each chunk
  carries its own vector, and chunk-scope scalars land under their
  unprefixed plan names. Parent scalars AND parent TEXT fields
  denormalize onto every chunk row — parent text as ordinary
  multi-field columns, the same shape the production corpus uses for
  case names — so a filter sees parent and chunk fields together with
  no query-time join. A declared CHUNK_ID is required per chunk; a
  document with ZERO chunks is a legitimate empty document and yields
  zero rows (the response's `parents` count keeps it visible). Each
  row's lineage carries the reduced parent id as `DocLineage.parent_id`
  (the field once named for court opinions, now generic), which
  is exactly the key the engine's parent-collapse scans group by:
  `collapse_parents` works over mapped chunks unchanged. (One caveat,
  inherited from the self-parent tag: rows ingested WITHOUT lineage
  are their own parents under a high-bit-tagged id, so mixing mapped
  chunk rows and lineage-less rows in one shard can collide parent
  keys in the tagged range; mapped corpora carry lineage on every row
  and never meet it.) Chunk refusals name the document position, the
  chunk ordinal, and the field.
- **Ids stay positional; the two legs append in lockstep.** This
  engine's ids are server-assigned slots shared by both legs, so the
  mapped document takes the next id and its vector applies at the SAME
  id under the same lock, with the same WAL record `AddVectors`
  writes. A shard whose document leg ran ahead refuses by name (the
  vector would land below its document and silently corrupt every
  hybrid result). The document id FIELD lands on its planned family —
  a keyword id on the facet plane, an integer id on the i64 plane —
  exact and filterable; the reference's 8-byte SHA-256 reduction
  remains the contract for id-KEYED features (upserts, chunk-parent
  joins) when an increment needs one, but storing the exact value
  loses nothing today.

- **The binding is durable: an index only ever pairs with the plan it
  was written under.** The FIRST bind pins the shard to the triple
  (plan fingerprint, body path, materialize-spec hash — the spec is
  part of the identity because changing a materialization expression
  changes what an index means). Every later bind must match exactly or
  refuses naming what differs; changing the mapping is a rebuild,
  never a rebind. Durability follows the store: the binding persists
  as the kind-6 entry of the kinded column table — inside the v8
  integrity envelope, so it lives and dies with the columns it
  describes and cannot vanish separately — written at flush, adopted
  from the file at startup. The bind is also a WAL record (in
  `markers.wal`), so reshard replay carries the binding onto rebuilt
  children and refuses inputs bound to different plans. A snapshot
  install replaces the binding along with everything else (the image's
  own, usually none): a wholesale replace replaces the plan identity
  too. A bind that never flushed evaporates with the columns it never
  wrote — consistency with the store is the invariant, not the bind
  ceremony.

What remains deliberately left out: per-field analyzer resolution
(the plan records analyzer NAMES; non-body text fields analyze under
sidecar defaults, and analysis identity is enforced by the analysis
fingerprint as everywhere).

## 5. Vendoring and the BYO-descriptor flow

The exchange contract is vendored, never depended on:

- The file is copied from the owning protomolt repository into this
  repo's vendored tree (package `ai.protomolt.proto.schema.registry.v1`,
  import path following the same convention as
  `proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`), and it must
  remain **byte-identical** to the source. Copy it from the owning repo;
  never edit the vendored copy independently.
- Byte identity is enforced by a check, not by convention:
  `scripts/check-vendored-protos.sh` pins the SHA-256 of each vendored
  file (descriptor_exchange.proto, its validate.proto import, and
  indexing_hints.proto) against ProtoMolt rev `75ae2c60`, and diffs
  byte-for-byte against a checkout when `PROTOMOLT_DIR` names one. Run
  locally today; wire into CI when CI exists.
- The `ai.pipestream.proto.index.hints.v1` declaration is retired. A
  descriptor set that declares it is refused by name rather than treated as
  an alias: recompile the source schema against
  `ai.protomolt.proto.index.hints.v1`, then bind a new generation and rebuild
  from retained original sources. Do not rewrite an existing plan fingerprint
  or replay reduced columns to make it appear compatible.
- Descriptor bytes are opaque to the ranking path. The engine consumes
  Register/Get/List/Sync as a client; a descriptor set is bytes plus a
  SHA-256 until the mapping layer derives a plan from it.

The bring-your-own-descriptor flow, end to end:

1. A client registers a complete `FileDescriptorSet`
   (`protoc --include_imports`, or any registry that stores complete
   sets) with the exchange service and gets back its content address.
2. The client asks pipestream-search to plan an index over a message type
   in that set — first as a dry run, then bound. Derivation is local,
   deterministic, and fingerprinted (section 2). Two engines agree on
   their mapping exactly when their fingerprints agree.
3. Documents ingest as the serialized protobuf messages they already
   are, decoded against the bound descriptor, extracted per the plan.
4. Queries filter and project in the engine's compiled CEL dialect over
   the mapped columns. Selection, pruning, fusion, floors, and
   completion behave exactly as they do over hand-built columns, because
   the columns are the same columns.

A client that never touches the exchange service loses nothing: the
descriptor set can equally arrive inline in the plan request, as the
reference implementation did. The exchange service exists so that a set
registered once is addressable by every consumer without re-shipping
bytes.

## 6. Non-goals

- **No protomolt build coupling.** No crate, no generated code, no
  service dependency beyond one vendored proto file verified
  byte-identical. If protomolt disappeared tomorrow, this engine's
  build, tests, and ranking would not notice.
- **No interpreted CEL on the serving path.** Not for filters, not for
  materialization, not for projections, not behind a flag. The `cel`
  crate stays out of the dependency tree; `cel-interpreter` stays a
  test-only oracle. What does not compile does not run.
- **No mapping policy inside the descriptor exchange contract.** The
  contract stays a byte mover: content-addressed, opaque, policy-free.
  Derivation rules, kind vocabularies, fingerprints, and refusal
  behavior are this engine's own versioned surface.
- **No heuristic derivation.** A descriptor that does not resolve to one
  vector field and one document id is refused with the reason named, as
  the reference implementation already did. Silent candidate-picking is
  the failure mode this feature refuses to ship.
