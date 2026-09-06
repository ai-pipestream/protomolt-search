# Vector field identity in indexing plans

`MappedPlan.vector_binding` describes the one vector plane derived from the
protobuf descriptor. The version-1 `MappedVectorBinding` names its exact indexed
column, source path, declared dimension and containing plan fingerprint. A
hinted name is used verbatim; it is not replaced by the source path or an
implicit body-field alias. Dimension zero retains the existing meaning that the
first document supplies the dimension.

The binding is derived after vector inference. Planning now checks column-name
collisions after that inference as well: previously a repeated float field could
start with no column family, become the vector later, and share its flattened
name with a scalar column. Explicitly hinted vector fields were also skipped by
the old collision check. For example, `metadata.embedding` and a scalar named
`metadata_embedding` now refuse instead of identifying two indexed fields with
one grant name.

Vector fields also cannot take the built-in `body`, `parent_id` or `group_id`
names. Mapped ingest rejects a vector name found in the node's configured or
active stored text, scalar, map or geo tables, including columns outside the
mapped descriptor. This prevents a configuration change or extra landing column
from bypassing the plan's uniqueness check.

`mapped_vector::from_plan` checks that the repeated vector field, path, kind,
column family and declared dimension agree. The canonical codec rejects wrong
versions or fingerprints, empty or reserved names, malformed data, duplicate
wire fields and unknown fields. Empty bytes mean no binding declaration, never
an implicit field grant.

## Fingerprint and compatibility

The new message is a derived explanation of already-fingerprinted inputs.
The plan's canonical hash already covers the indexed field name, source path,
kind, column family, declared dimension and reachable schema. Its algorithm is
unchanged; the derived message is not hashed recursively. Valid existing plans
keep their fingerprints. A previously ambiguous plan must choose distinct
indexing names; that intentional mapping change produces a different fingerprint.

The initial planning increment added one field and one message to the public
protobuf contract without changing storage. The subsequent
[stored binding integration](vector-binding-storage.md) adds an index metadata
kind and WAL version gate.

## Required runtime integration

A plan response is not a credential, physical read claim or proof that a server
is serving that plan. `StoredBinding` now retains this vector name alongside the
plan fingerprint and body/analysis metadata. See the stored binding integration
for replay, compaction and replica evidence, and the remaining rowless runtime
generation publication gap when WAL is disabled.
Read requests must name the field and nodes must verify it against that durable
binding before a field grant can authorize vector selection or scoring.

Raw vector-only indexing also needs an explicit field definition. Do not guess
that its vector plane is `body`, a materialized scalar or a source-path alias.
Field-restricted vector membership and the remaining restricted public query
routes stay gated until this runtime contract and their other selection and
disclosure boundaries are complete. This increment does not complete any of the
three foundation objectives.

## Evidence

`tests/protobuf_semantics.rs` checks inferred-name collisions and the typed
binding's canonical validation. The checked-in vector-binding descriptor fixture
covers explicit ProtoMolt naming/dimensions, scalar aliases and all three
reserved names. `tests/mapped_ingest.rs` checks collisions in all eight configured
column families. A node unit test checks active store tables independently of
startup declarations. Existing mapping, unsigned-type and ingest tests exercise
the unchanged extraction and fingerprint rules.

Validation of this change passed 464 library tests, 639 integration tests across
111 targets, and 12 embedded tests (1,115 passed, one existing ignored test).
The descriptor fixture regenerates byte-for-byte from its checked-in source.
All five Android/iOS target checks, the tests/examples compile check, formatting,
vendored-proto identity and diff checks passed. Descriptor comparison against
`4a58c9e` confirms exactly the new plan field and vector-binding message; every
existing protobuf declaration is unchanged.
