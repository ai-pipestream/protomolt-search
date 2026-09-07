# Explicit protobuf indexing policy

This increment introduces `IndexDefinition` in `ai.protomolt.search.v1`.
It separates the producer's schema from the consumer's choice of searchable
columns. The definition accompanies `PlanIndexRequest` and `MappedBind`; the
derived `MappedPlan` acknowledges the canonical definition.

## Contract

A present definition selects exactly its listed projections. Descriptor hints
and field-name heuristics do not add columns or select roles. Unlisted fields
remain in the original source and the exhaustive schema report, with no value
projection. An absent definition retains the existing inferred mapping contract.
An empty definition is an error for the current row index, rather than a request
to fall back to inference. Source-only acceptance remains the document catalog's
separate operation.

Each projection identifies an occurrence by a root-relative sequence of protobuf
field numbers. Reusing a message under two parents permits different projections
for each occurrence. The engine derives readable source paths from the descriptor
and validates the entire path (one through nine components). A field's kind, physical column name, structural
role and vector dimension are explicit. Unknown kinds/roles, duplicate paths,
colliding column names, missing fields, and impossible projections refuse at
planning. Definition order does not affect the plan or fingerprint.

The current index requires one explicit DOC_ID and one explicit VECTOR, with a
positive vector dimension. Bind still selects a TEXT body and validates explicit
analysis and node column declarations. An explicit CHUNKS container enables
traversal into that repeated message; arbitrary repeated or map traversal cannot
silently flatten element relationships. A projection can end at a map field:
KEYWORD/BOOLEAN values use the map-facet plane and FLOAT/DOUBLE values use the
map-numeric plane. [Map projection](map-projection.md) defines keys, defaults,
duplicate entries and supported value types. Other unsupported shapes remain
source-only until their storage and query semantics are implemented.

Explicit policy uses a separate fingerprint domain, even if its resolved columns
match an inferred plan. A node that ignores the new bind field derives a different
fingerprint and refuses before applying rows. Clients must inspect the returned
definition acknowledgment when planning against mixed-version services.

## Preservation and lifecycle

The descriptor is not patched or reserialized to carry configuration. Its exact
content address and the original source bytes remain separate from projection
identity. The canonical plan covers the reachable wire schema and resolved
columns. Existing durable binding checks must reject a changed policy across
reopen, replication and compaction; replay uses the already materialized columns.
No source or index transfer is required for a phone to plan locally.

This is an indexing-policy contract, not authorization or publication. Grants,
conditional source writes, idempotency and searchable receipts retain their own
requirements. In particular, adding a projection to an existing schema still
requires building and publishing a compatible index generation.

## Status

The planner and durable storage integration implement the contract described
here. This increment builds on main `af51a1d`; it includes no fleet rollout.

The planner, extractor, coordinator, node bind and embedded/mobile protobuf
entry points use the same definition. Tests cover occurrence-specific types,
canonical ordering, hint independence, distinct inferred/explicit fingerprints,
named refusal, explicit chunk scopes, over-the-wire planning and ingest, stored
bindings after reopen, and preservation of unindexed and unknown source fields
after compaction renumbers rows. The mobile ABI test plans and ingests using the
same explicit policy. A projected DOC_ID still does not manufacture catalog
identity for legacy mapped ingest.

This does not complete repeated/nested value storage, protobuf editions, remote
authorization, or transactional index publication. Existing inferred bindings
remain readable and keep their fingerprints. Using an explicit definition
requires a new binding even when its columns equal the inferred plan's columns.

For example, this policy selects a numeric key, a text body and a vector from
root fields 1, 2 and 3. Supply the descriptor set and root type alongside it in
`PlanIndexRequest`, then reuse the acknowledged policy and fingerprint in
`MappedBind`:

```textproto
index_definition {
  projections {
    field_numbers: 1
    kind: MAPPED_KIND_UINT64
    column_name: "document_key"
    role: MAPPED_ROLE_DOC_ID
  }
  projections {
    field_numbers: 2
    kind: MAPPED_KIND_TEXT
    column_name: "body"
  }
  projections {
    field_numbers: 3
    kind: MAPPED_KIND_VECTOR
    column_name: "semantic"
    vector_dims: 256
  }
}
```

This example assumes the declared types are `uint64`, `string` and
`repeated float` respectively. The first column is a projected source value;
its name does not turn it into the catalog's opaque document key. A nested path
such as `field_numbers: [5, 1]` selects field 1 inside root field 5.

## Durable policy and compatibility

`MappedIndexContract` format 1 retains the canonical explicit definition, root
message type and bound plan fingerprint. `StoredBinding.index_contract` carries
its encoded bytes independently of analysis and vector declarations. Original
source descriptors remain in the source/catalog contract; an empty bind does
not accept a source document or invent catalog identity.

The contract survives image flush and reopen, WAL recovery, segment-catalog
publication, snapshot transfer, replication, compaction and reshard replay.
`ApplyWalBinding` echoes the installed bytes; the sender rejects a missing or
different acknowledgment before advancing its replication cursor. Acceptance
of a bind alone is not a durable receipt: node `Flush` remains the persistence
boundary. Catch-up flushes every replayed mutation prefix, including a retry
whose binding or rows were already accepted in memory. It rejects
`Flush.written=false` instead of advancing a durable cursor on a volatile node. A WAL-only recovery test explicitly syncs the writer before reopening
without an image.

Image column-table kind **14** extends the kind-13 payload with a little-endian
u32 byte length followed by the canonical policy. Heap readers, mapped readers,
spill writers and integrity scanners all recognize it. Older readers reject the
unknown kind. Segment catalogs retain their existing binding-required format 2
gate and the extended canonical `LoggedBinding`;
an older decoder drops its unknown policy field and fails the existing exact
re-encoding check, including when the catalog has no segments.

The first explicit-policy WAL binding durably upgrades the manifest to **format
6 before appending**. Inferred named-vector bindings still require only format
5, explicit analysis requires 4, and older binding encodings remain unchanged.
Readers whose maximum format is 5 refuse a policy-bearing WAL generation.

Loading or applying a nonempty contract checks its version, canonical encoding,
field-number paths, kinds, roles, scope constraints, containing plan fingerprint,
and agreement with the vector declaration. Malformed, unknown-version and
noncanonical contracts refuse. Planning additionally checks the source
descriptor and type compatibility; structural validation of stored metadata does
not independently establish that relationship.

Tests cover empty recovery, incompatible rebinds with correctly derived
fingerprints, synced WAL-only recovery, image writer parity, truncated payloads,
segment catalogs, compaction followed by reshard replay, and real network
replication to a receiver that omits its acknowledgment. Snapshot receivers with
no WAL recover the policy in both layouts. Invalid WAL appends leave both clock
and format unchanged. A lost/missing acknowledgment followed by an already-bound
retry must leave the binding on disk before returning an advanced cursor; a
receiver without persistence must refuse that durable acknowledgment.

## Validation

Against main `af51a1d`, 507 library tests, 716 integration tests across 122
targets, 12 embedded tests and two IVF evaluation tests passed (1,237 total).
The existing live OpenNLP conformance test remains ignored. All five iOS/Android
Rust target checks passed, retaining three existing relay dead-code warnings.
Locked tests/examples compilation, formatting, vendored-proto and whitespace
checks passed. A descriptor comparison confirms exactly six additive fields and
three new messages, with all prior wire declarations unchanged.

The new regressions first reproduced lost policy in image storage, cursor
advancement after an accepted-but-unflushed retry, and cursor advancement on a
volatile receiver. They pass with the durable contract and catch-up checks.
Lifecycle tests cover actual network replication, no-WAL snapshot receivers,
WAL-only recovery, compaction and reshard replay, not only message round trips.

These are local results, not hosted CI, device execution or deployment. Build
concurrency was two and test concurrency four. The supported projection kinds
remain bounded as described above; these results do not establish completion of
all protobuf shapes, remote authorization or transactional source publication.
