# Stored vector field bindings

`StoredBinding.vector_binding` retains the canonical `MappedVectorBinding`
derived during mapped ingest. The exact bytes travel with the plan, body,
materialization and analysis identities. The field name is not reconstructed
from the first source document, a row position or a scalar column.

Initial mapped ingest requires an empty unbound shard, or an exactly matching
existing binding. A populated unbound shard cannot acquire a descriptor claim
over old rows. Legacy bindings with no vector declaration remain readable, but
cannot accept the new named binding through an implicit upgrade. Rebuild from
original sources to change a binding. The plan fingerprint algorithm itself is
unchanged.

## Index and WAL formats

The BM25 column table uses kind 13 for bindings with a vector declaration. Its
payload contains the three legacy strings, the analysis digest string, a
length-prefixed analysis contract and a length-prefixed vector binding. Analysis
may use the legacy body default while the vector name is explicit. Kind 13
requires a nonempty, canonical vector binding with the same plan fingerprint.
Kind 6 and kind 12 remain readable with their original encodings. The normal v8
container covers this inline metadata with its header CRC. Both heap and mmap
readers validate the payload, and both heap and spill writers preserve it.

`LoggedBinding.vector_binding = 6` carries the same canonical bytes. Appending a
named vector binding persists WAL manifest version 5 before writing the record,
so an older reader refuses instead of dropping an unknown protobuf field and
replaying an unnamed binding. Malformed or contradictory declarations refuse
before the manifest or record clock changes. Analysis-only bindings continue to
require version 4; source and logical-row-identity gates remain at 2 and 3.

Compaction writes the binding before rows in the rewritten WAL and preserves it
in each output image. Offline reshard replay validates it and carries it to
child images. Existing image-based snapshot transfer carries the binding inside
the BM25 artifact. This adds no independent sidecar that could be separated from
the image it describes.

## Replica acknowledgement

`ApplyWalBindingRequest.vector_binding = 7` supplies the canonical binding.
The node validates it, checks its name against configured and active non-vector
columns, and compares the complete binding before returning
`ApplyWalBindingResponse.vector_binding = 3`. Catch-up requires an exact echo
before proceeding. An older receiver that ignores the request field cannot
acknowledge it. Empty legacy declarations do not gain an implicit vector name.
If an older receiver installed only the legacy subset before this check failed,
upgrade it and reinstall the matching base snapshot before catch-up. A retry
must not erase its binding or reinterpret that partial installation as the
complete named contract.

The response acknowledges installation in the node's current state. It is not a
new durability receipt; existing WAL append and flush semantics still apply.

## Remaining integration

An empty generation with WAL enabled recovers the binding from the log, even
when no source document or BM25 image exists. With WAL disabled, a rowless
runtime shard still has no generation-level metadata artifact: its binding is
only in memory until a document-bearing image is written. Zero-row snapshot
and generation publication need a common durable metadata contract. The empty
store codec tests do not prove that this runtime publication exists.

Read requests still need to name the vector field, and nodes must compare it
with the binding under the same read guard used for selection and scoring.
Raw vector-only collections need an explicit field definition. The remaining
restricted query routes stay gated. These storage changes do not complete the
protobuf, permission or identity/durability objectives.

## Evidence

- `tests/mapped_vector_storage.rs` checks byte-identical heap/spill images for
  empty and populated stores, with legacy and explicit analysis; heap/mmap
  reopen; integrity verification; truncation, malformed payloads and old-kind
  reinterpretation; and refusal of a binding from another plan.
- `src/wal.rs` checks that invalid binds leave the version and clock untouched,
  that version 5 is durable before flush, and that replay preserves exact bytes.
- `tests/mapped_ingest.rs` checks initial-bind refusals, exact acknowledgement,
  empty WAL-backed reopen in both layouts, populated restart, replica catch-up,
  offline resharding, and binding retention in both compaction layouts after
  tombstone reclamation. It also transfers those compacted generations over
  `StreamSnapshot`, then reopens receivers with WAL disabled and requires an
  exact installed-binding acknowledgement from the image alone.

Validation passed 465 library tests, 644 integration tests across 112 targets,
and 12 embedded tests: 1,121 passed, with one existing ignored test. All five
Android/iOS target checks, tests/examples compilation, formatting, vendored-proto
identity and diff checks passed. Descriptor comparison against `cac7831` confirms
exactly the three additive binding fields described above, with existing
declarations unchanged. Two older fixtures were updated for the intentional
binding migration: materialization hashes are tested independently of missing
vector metadata, and named mapped WAL bindings now require version 5.
