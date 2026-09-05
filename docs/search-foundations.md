# Search foundations

Implementation branch: `feat/search-foundations`, based on `PRE_ASTRA`.
This tracks the full requested foundation. Individual passing increments do not
establish completion of the three workstreams.

## Completion requirements

| Requirement | Required evidence | Current state |
|---|---|---|
| Faithful protobuf decoding and compatible index binding | Generated-runtime differential fixtures for presence, oneofs, merges, scalar encodings, unknown values and schema evolution | Oneof, presence, merged messages, int32, enum openness, required fields and groups corrected; v3 includes reachable extensions; coverage expanding |
| Every protobuf shape has an explicit preservation, indexing and query disposition | Typed index definition and exhaustive descriptor/field support report; no silent omission | Accepted mapped plans return an exhaustive reachable schema graph and exact projection/query dispositions; standalone reporting of unbindable schemas and configurable index definitions remain |
| Original payload and descriptor identity survive storage and replay | Byte equality after restart, snapshots, replication, compaction and resharding, including unknown fields | Row-bearing mapped sources retained byte-for-byte through images, WAL, replicas, snapshots, compaction and resharding; zero-row logical documents remain |
| Complete scalar, repeated, map, nested and well-known-type semantics | Projection and query conformance across supported syntax/edition and shape combinations | Incomplete; existing column-family restrictions remain |
| Workspace and collection grants separate read, ingest and administration | Denial tests on every public and node entry point, default collection resolution and direct access | Public routes enforce revisioned protobuf capabilities; direct node/cluster-control policy enforcement remains |
| Document and field grants cover retrieval and disclosure | Selection, statistics, suggestions, facets, highlights, projections, source fetch, caches and cursors tested under distinct and revoked policies | Not implemented |
| Stable document and chunk identity | Exact key lookup and returned identity unchanged through compaction, replay and resharding | Not implemented; public row IDs remain positional |
| Conditional writes and persistent idempotency | Concurrent version conflicts, repeated requests, key reuse with different payload, disconnected acknowledgment and restart tests | Collection-wide local source authority implemented; server routing and projection transactions remain |
| Accepted, searchable and durable receipts | API states tied to actual transaction publication and persisted recovery boundaries, crash tests at each boundary | Local source acceptance has durable/volatile receipts and abrupt-process-exit coverage; searchable publication remains |

## Design constraints

The public contracts belong in the product's protobuf package. Keep descriptor
vocabulary owned by ProtoMolt, product projection and policy owned by search,
and vector distribution owned by the interchangeable backends. The embedded
library must retain its no-network dependency boundary.

Preservation, projection and querying are separate contracts. An original message
must not lose bytes because a field is not indexed. The index definition must
record every field's disposition and reject unsupported requested operations.
Descriptor content identity, decoder semantics and projection identity must be
separately inspectable. Physical postings and vector blocks can remain packed
and memory mapped.

Authorization uses the ecosystem's workspace authority through a replaceable
provider, not a second user directory. A resolved request context binds subject,
workspace, collection, action and policy revision. Mandatory document selection
and field grants cannot be overridden by user CEL. Cache/cursor identity and
stream revocation must use that context. A trusted internal connection alone
must not permit a public principal to bypass the policy through a node route.

The write transaction owns the exact stable key, source version, conditional
precondition, original payload, projection, idempotency record and receipt.
Publishing a replacement must expose one logical version, not both the appended
row and its predecessor. Compaction may change storage slots but cannot change
this identity or discard the deduplication history required by the contract.
Define acknowledgment and retention rules explicitly; fsync and replicated
acknowledgment are separate capabilities. A phone retaining sole ownership never
promises durability from a remote copy.

## Decoder increment

`src/mapping.rs` now validates descriptors with `prost-reflect` and decodes the
whole message before projecting into columns. This resolves oneof selection and
submessage merging before any leaf lands, including a oneof alternative that has
no indexed column. Projection retains explicitly present empty strings and zero
values. Implicit-presence scalars project their protobuf defaults consistently
whether omitted or explicitly encoded; an absent explicit-presence field remains
missing. An absent enclosing message does not invent child values.

The v3 plan fingerprint includes the reachable wire schema as well as the
projection. Field numbers, scalar encodings, cardinality, syntax, defaults,
oneof membership, enum declarations and map-entry shape participate. Descriptor
file order, unrelated files and source comments do not. The original descriptor
content hash remains separate.

This is an index compatibility change. Existing v1/v2 mapped generations remain
readable, but a new bind derives v3 and refuses to append into a v1/v2 binding.
Rebuild mapped data from original protobuf sources into a new generation. Do not
rewrite stored fingerprints or replay reduced columns as proof of corrected
extraction. Unmapped generations do not acquire a new mapping or require a
rebuild from this change alone.

Remaining protobuf work includes zero-row original retention, standalone shape
reporting for unbindable schemas, unsigned columns, repeated/nested correlation, extension indexing,
well-known types and Editions. Reachable MessageSet types are explicitly refused.
The decoder dependency does not itself prove those contracts. Its behavior must
be covered or adapted by the conformance suite before support is claimed.

`tests/protobuf_semantics.rs` uses generated prost messages as differential
oracles and adversarial wire encodings. `tests/descriptor_mappings.rs` pins the
new fingerprint. `tests/mapped_ingest.rs` exercises binding, column landing,
restart, routed ingest, replication and resharding through the real handlers.

`src/protobuf.rs` intercepts closed-enum values before the reflected message
mutates, so an unknown number cannot erase a known value or change oneof
selection. Unknown closed-enum map values omit the entire entry from projection.
Open enums retain unknown numbers, rendered as decimal facet strings; declared
numbers retain the first alias name. Openness follows the enum's defining file,
including proto3 enums imported by proto2 messages. Protobuf framing and recursion
limits remain in prost; no parallel wire parser was added.

Required fields are validated after all message fragments merge. The check
includes unindexed repeated/map messages, groups and present extensions, while
inactive oneof members and absent optional messages do not invent requirements.
Singular groups now expand and project like singular messages. Registered
extensions and their reachable message/enum types participate in the v3 hash;
reordering files or extension declarations does not change it.

The checked-in fixtures under `tests/fixtures/protobuf-semantics/` compare merged
values and required-field validity against Google's protobuf 6.33.5 runtime.
They cover decoding, not original-source preservation: the private projection
decoder intentionally excludes closed-enum unknowns from its value tree. Its
output must never substitute for the original payload in future source storage.

## Capability increment

`authorization.proto` and `src/authorization.rs` supply exact workspace-bound
collection grants, an ecosystem authority adapter, revisioned decisions and
stream revocation. `CollectionSet` enforces separate search, ingest and admin
capabilities on every public RPC. Bearer configuration now requires an explicit
policy; credentials without grants deny access. [Security](security.md) records
the route table, migration, tests and remaining direct-node/document/field gaps.
This does not complete authorization or the overall foundation.

## Source storage increment

[Original protobuf storage](protobuf-source-storage.md) records the archive and
WAL formats, byte-preservation evidence and remaining zero-row catalog gap.
Mapped rows retain original producer bytes separately from projected values.
Descriptors and source payloads are interned within each image and WAL
generation; spill builders put payloads on disk. This does not yet establish
logical identity, complete shape support, source disclosure permissions or
transactional durability.

## Schema report increment

[Schema and projection report](schema-report.md) describes every reachable field
and registered extension for accepted plans, including skipped and recursive
shapes. Exact projection paths distinguish source-only occurrences, traversed
containers and query values; constraints expose current value-domain losses.
This does not supply the remaining scalar/nested/extension query implementations
or an independently configurable index definition.

## Logical source authority increment

[Document writes](document-writes.md) records the collection-wide catalog,
conditional versions and persistent retry decisions. Embedded Rust and mobile
calls retain originals without requiring any index rows. Its path is independent
of shard geometry, and source acceptance never claims search visibility.
Legacy ingest, public result identity, atomic projection replacement, catalog
backup/migration, workspace binding and document/field grants remain unfinished.
