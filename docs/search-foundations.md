# Search foundations

Implementation branch: `feat/search-foundations`, based on `PRE_ASTRA`.
This tracks the full requested foundation. Individual passing increments do not
establish completion of the three workstreams.

## Completion requirements

| Requirement | Required evidence | Current state |
|---|---|---|
| Faithful protobuf decoding and compatible index binding | Generated-runtime differential fixtures for presence, oneofs, merges, scalar encodings, unknown values and schema evolution | Oneof, explicit presence, implicit defaults, merged messages and int32 projection corrected; coverage expanding |
| Every protobuf shape has an explicit preservation, indexing and query disposition | Typed index definition and exhaustive descriptor/field support report; no silent omission | Not implemented |
| Original payload and descriptor identity survive storage and replay | Byte equality after restart, snapshots, replication, compaction and resharding, including unknown fields | Not implemented; current mapped WAL stores reduced columns |
| Complete scalar, repeated, map, nested and well-known-type semantics | Projection and query conformance across supported syntax/edition and shape combinations | Incomplete; existing column-family restrictions remain |
| Workspace and collection grants separate read, ingest and administration | Denial tests on every public and node entry point, default collection resolution and direct access | Public routes enforce revisioned protobuf capabilities; direct node/cluster-control policy enforcement remains |
| Document and field grants cover retrieval and disclosure | Selection, statistics, suggestions, facets, highlights, projections, source fetch, caches and cursors tested under distinct and revoked policies | Not implemented |
| Stable document and chunk identity | Exact key lookup and returned identity unchanged through compaction, replay and resharding | Not implemented; public row IDs remain positional |
| Conditional writes and persistent idempotency | Concurrent version conflicts, repeated requests, key reuse with different payload, disconnected acknowledgment and restart tests | Not implemented |
| Accepted, searchable and durable receipts | API states tied to actual transaction publication and persisted recovery boundaries, crash tests at each boundary | Not implemented |

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

The v2 plan fingerprint includes the reachable wire schema as well as the
projection. Field numbers, scalar encodings, cardinality, syntax, defaults,
oneof membership, enum declarations and map-entry shape participate. Descriptor
file order, unrelated files and source comments do not. The original descriptor
content hash remains separate.

This is an index compatibility change. Existing v1 mapped generations remain
readable, but a new bind derives v2 and refuses to append into a v1 binding.
Rebuild mapped data from original protobuf sources into a new generation. Do not
rewrite stored fingerprints or replay reduced columns as proof of corrected
extraction. Unmapped generations do not acquire a new mapping or require a
rebuild from this change alone.

Remaining protobuf work includes original-source retention, complete shape
reporting, unsigned columns, repeated/nested correlation, enum openness,
required-field validation, extensions/groups, well-known types and Editions.
The decoder dependency does not itself prove those contracts. Its behavior must
be covered or adapted by the conformance suite before support is claimed.

`tests/protobuf_semantics.rs` uses generated prost messages as differential
oracles and adversarial wire encodings. `tests/descriptor_mappings.rs` pins the
new fingerprint. `tests/mapped_ingest.rs` exercises binding, column landing,
restart, routed ingest, replication and resharding through the real handlers.

## Capability increment

`authorization.proto` and `src/authorization.rs` supply exact workspace-bound
collection grants, an ecosystem authority adapter, revisioned decisions and
stream revocation. `CollectionSet` enforces separate search, ingest and admin
capabilities on every public RPC. Bearer configuration now requires an explicit
policy; credentials without grants deny access. [Security](security.md) records
the route table, migration, tests and remaining direct-node/document/field gaps.
This does not complete authorization or the overall foundation.
