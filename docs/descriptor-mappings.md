# Descriptor-derived mappings and the descriptor exchange contract

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
fixed-layout encoding (version tag `turbovec-search.plan.v1`, hash from
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
ORDINARY ingest path. The durable shard-level binding landed with it:
the first bind pins a shard to its plan across restarts (section 4a).
Still later: chunked plans. The original framing, kept:

The ownership move is decided (descriptor-derived mappings belong to
turbovec-search, not turbovec-grpc), and the reference implementation
is frozen in turbovec-grpc git history.

## 1. The layering rule

Three things are easy to conflate and must not be:

1. **Descriptors are vocabulary.** A `google.protobuf.FileDescriptorSet`
   says what fields a message type has and what their proto types are.
   The shared gRPC descriptor-exchange contract (protomolt's
   `ai.pipestream.proto.schema.registry.v1`, `descriptor_exchange.proto`)
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

The dependency consequence is the point of the layering: turbovec-search
has **no dependency on protomolt**, compile-time or runtime. It vendors
one proto file (section 5) exactly as it already vendors the OpenNLP
sidecar's `analysis.proto`, and it consumes the exchange service as one
gRPC client among any. Protomolt is a future client of turbovec-search,
and any other system that can register a FileDescriptorSet is equally a
client. Nothing in the engine's build, wire contract, or ranking path
names protomolt.

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
- **Absence semantics.** The reference gave unset proto3 fields their
  proto3 defaults (empty string, 0, the epoch). This engine uses the
  documented Kleene three-valued rule: a comparison on a document that
  lacks the value is UNKNOWN, and negation cannot launder absence into a
  match (`src/filter.rs`). Mapped fields inherit the engine's rule, not
  the reference's; over a corpus where absence is normal, proto-default
  semantics lie.
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

Chunk scopes wait (resolved with increment 2): a chunked plan derives
and fingerprints, but binding one for ingest refuses by name. The
engine already has lineage records and a parent-collapse mode in
hybrid fusion; the chunk increment should reuse those rather than
import the reference's parent tables unchanged.

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
- **Extraction is the same hand-rolled wire discipline as the hint
  pass.** The bind compiles a trie over descriptor field NUMBERS; each
  document's bytes walk it once — unknown fields skip, repeated
  occurrences follow protobuf merge semantics, malformed bytes refuse
  by position ("document 17: ..."). Values land by planned family:
  strings, bools ("true"/"false"), enums (the value NAME from the
  descriptor; an undeclared number refuses — schema drift, not a
  value), integers (every proto encoding; a uint above i64::MAX
  refuses), Timestamps (as `TimestampValue`, so the ordinary
  epoch-micros conversion and its refusals apply), floats and doubles.
  A double vector narrows to the engine's f32 plane — the one lossy
  landing, stated here. An empty wire string is proto3 absence and
  lands nothing; the body, the id, and the vector are required and
  refuse when absent.
- **The ordinary path does the rest.** Each decoded document becomes an
  ordinary `AddDocumentsRequest` — the bind's `analysis` and CEL
  `materialize` specs attached as session properties — and enters the
  same streaming analysis session, the same column validation, the
  same apply, the same WAL record. Replay never needs the descriptor:
  the log carries the reduced values.
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

What this increment deliberately leaves out: chunked plans (refused at
bind) and per-field analyzer resolution (the plan records analyzer
NAMES; non-body text fields analyze under sidecar defaults, and
analysis identity is enforced by the analysis fingerprint as
everywhere).

## 5. Vendoring and the BYO-descriptor flow

The exchange contract is vendored, never depended on:

- The file is copied from the owning protomolt repository into this
  repo's vendored tree (package `ai.pipestream.proto.schema.registry.v1`,
  import path following the same convention as
  `proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`), and it must
  remain **byte-identical** to the source. Copy it from the owning repo;
  never edit the vendored copy independently.
- Byte identity is enforced by a check, not by convention:
  `scripts/check-vendored-protos.sh` pins the SHA-256 of each vendored
  file (descriptor_exchange.proto, its validate.proto import, and
  indexing_hints.proto) against protomolt rev `74d172d9`, and diffs
  byte-for-byte against a checkout when `PROTOMOLT_DIR` names one. Run
  locally today; wire into CI when CI exists.
- Descriptor bytes are opaque to the ranking path. The engine consumes
  Register/Get/List/Sync as a client; a descriptor set is bytes plus a
  SHA-256 until the mapping layer derives a plan from it.

The bring-your-own-descriptor flow, end to end:

1. A client registers a complete `FileDescriptorSet`
   (`protoc --include_imports`, or any registry that stores complete
   sets) with the exchange service and gets back its content address.
2. The client asks turbovec-search to plan an index over a message type
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
