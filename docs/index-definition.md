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
silently flatten element relationships. Unsupported value shapes must remain
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

This checkpoint is on `feat/explicit-index-definition-2026-09`, based on main
`af51a1d`. It is not ready for main until the durable policy work below is
implemented. No fleet changes are part of this work.

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

## Required durable integration before merge

The current durable binding retains the plan fingerprint, body path, analysis
contract and vector binding. It does **not** retain the explicit policy. Restart
and compaction preserve materialized columns and reject a changed fingerprint,
but that is insufficient to recover the policy for a later rebuild. Do not merge
this checkpoint to main on the strength of those tests alone.

The next implementation must retain a canonical, versioned protobuf contract
containing the explicit definition, root message type and bound plan fingerprint.
Source descriptors remain in the source/catalog contract; policy retention must
not rewrite them or assert source acceptance for an empty bind.

Carry that contract as its own `StoredBinding` member, not inside the vector or
analysis declaration. Propagate it through `LoggedBinding`, segment catalog
bindings, `ApplyWalBinding`, node recovery, compaction and resharding. Receivers
must echo the installed contract and senders must reject a missing or different
acknowledgment. Validate version, canonical encoding, structural definition and
the containing fingerprint before binding, writing, replaying or opening it.

Use a distinct binding table kind (14 is currently unused) so older image
readers refuse rather than discard policy. The current WAL maximum is format 5;
explicit policy needs a new format, advanced durably before its first record.
Existing inferred bindings must continue using their existing encodings and
remain readable. Update every heap, mapped-reader, spill and integrity-scanner
path; changing only the primary writer is insufficient.

Required evidence includes an empty bind followed by restart, canonical-policy
recovery, a correctly fingerprinted incompatible rebind, actual WAL replay,
heap/spill/mapped image parity, segmented catalogs, compaction/resharding that
preserve the contract, and replication to a peer that drops its acknowledgment.
Malformed/unknown/noncanonical contracts must fail without appending a record or
changing the existing binding. Re-run the combined suite and mobile checks after
the storage integration, and refresh main before the eventual merge.

## Checkpoint validation

Against main `af51a1d`, 507 library tests, 710 integration tests across 121
targets, 12 embedded tests and two IVF evaluation tests passed (1,231 total).
The existing live OpenNLP conformance test remains ignored. All five iOS/Android
Rust target checks passed, retaining three existing relay dead-code warnings.
Locked tests/examples compilation, formatting, vendored-proto checks and
whitespace checks also passed. A descriptor comparison confirms exactly three
additive fields and two new messages, with all prior wire declarations unchanged.
The documentation's textproto example encodes successfully with protoc.

These are local results for the planning/binding checkpoint, not hosted CI,
device execution, deployment, or evidence of durable policy retention. Source
files were unchanged throughout final validation. Build concurrency was two and
test concurrency four.
