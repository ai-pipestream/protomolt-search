# Descriptor-derived mappings and the descriptor exchange contract

Status: design note. Nothing here is implemented. Two inputs are
settled outside this repository and one is not: the ownership move is
decided (descriptor-derived mappings belong to turbovec-search, not
turbovec-grpc), the reference implementation is frozen in turbovec-grpc
git history, and the descriptor exchange contract is still being drafted
in the protomolt repository. Sections that depend on the draft carry a
TODO rather than a guess.

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

TODO: whether chunk scopes land in the first increment or wait. The
engine already has lineage records and a parent-collapse mode in hybrid
fusion; the port should reuse those rather than import the reference's
parent tables unchanged.

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

## 5. Vendoring and the BYO-descriptor flow

The exchange contract is vendored, never depended on:

- The file is copied from the owning protomolt repository into this
  repo's vendored tree (package `ai.pipestream.proto.schema.registry.v1`,
  import path following the same convention as
  `proto/ai/pipestream/opennlp/analysis/v1/analysis.proto`), and it must
  remain **byte-identical** to the source. Copy it from the owning repo;
  never edit the vendored copy independently.
- Byte identity is enforced by a check, not by convention. This repo
  currently has no CI workflow, so the first increment lands a check
  script that diffs the vendored copy against a pinned upstream rev, run
  locally and wired into CI when CI exists. TODO: the script and the
  pinned rev land with the vendor commit.
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
