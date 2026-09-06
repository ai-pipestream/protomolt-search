# Binding publication without source rows

A mapped generation keeps its complete `StoredBinding` after `Flush`, even
before its first document and with WAL disabled. This covers the plan
fingerprint, body path, materialization fingerprint, analysis fingerprint and
contract, and canonical vector declaration. Binding acknowledgement alone
continues to use the existing append semantics; call `Flush` before relying on
image durability.

## Publication and recovery

Single-image shards write an empty BM25 image with the same binding metadata
entry used by populated images. Segmented shards publish the binding inside
`segments.json`, using catalog format 2. The payload is canonical protobuf
`LoggedBinding` bytes plus their SHA-256. This reuses the six-field WAL
vocabulary; it adds no public protobuf declaration. The checksum detects
corruption and is not an authorization credential.

Format 1 catalogs remain readable and may derive a binding from their segments.
Format 2 requires an explicit binding; format 1 cannot declare one. Every
segment must agree with the generation and the other segments, including
whether it is bound at all. Opening validates the canonical encoding, checksum,
analysis contract and vector declaration. Unknown or duplicate protobuf fields
in this persisted declaration are refused rather than silently discarded.

Publication uses the catalog's existing atomic manifest replacement. Repeating
the same binding is idempotent and does not advance its epoch. A conflicting
binding, or a request to label populated unbound segments, refuses. A reopened
heap tail adopts the generation binding. Removing the final deleted segment
preserves the binding by publishing format 2; removing live rows without
replacement outputs refuses.

Node compaction retains the binding in the rewritten WAL and resulting image
or catalog even when every row is deleted. The closing flush replaces the
retired FP32 sidecar with an empty one and saves the empty provider image, so
its dimensions and calibration survive a WAL-free restore.

## Snapshots

A zero-row, uncalibrated generation can transfer through `StreamSnapshot` and
`InstallSnapshotFrom` without manufacturing a provider image. For single-image
repositories, this requires a bound, zero-row BM25 image, zero vector/document/
live counts and dimension, no scoring fingerprint, and no FP32 artifact.
Populated document-only repository installs are not introduced by this change.
Segment repositories carry the binding in their catalog. Installed BM25-only
snapshot directories are recognized by startup and generation-swap recovery.

Export first flushes, then takes the seal mutex and shard read lock before
checking that files still represent the current state. Mutations, including
provider configuration and raw vector appends, invalidate an internal
`files_current` flag before changing state. A WAL must also be clean. A write arriving
between flush and copy, or a new tail accumulated during sealing, causes a
bounded retry; after eight attempts the export refuses. Holding both locks
through copy keeps catalog publication and state mutation out of the copied
generation, including on shards with WAL disabled. This internal readiness
flag is not a new accepted/searchable/durable receipt.

Segment snapshot installation verifies whole-shard FP32 dimensions and row
count against the provider images before replacing live files. A sealed
provider takes precedence over an earlier standalone calibration image,
matching normal startup. Export omits that obsolete image and copies only the
published catalog's named artifacts, excluding unreferenced compaction staging
files and retired segments. Artifact hashes alone cannot prove row alignment.

## Compatibility and limits

Pin compatible binaries for writers, readers and snapshot receivers. Older
catalog readers refuse format 2. Older single-image startup code may ignore a
BM25-only `.snap` directory because it recognizes installed generations only
by a vector image; downgrading such a shard is unsupported. Keep the prior
binary and generation together for rollback. Existing BM25 binding-kind and
WAL version gates remain as documented in [stored vector bindings](vector-binding-storage.md).

This closes the rowless runtime binding publication gap. It does not finish
field-named vector query enforcement, authorization across all public routes,
atomic source/catalog publication, conditional writes, persistent idempotency,
or public durability receipts. Those remain part of the foundations objective.

## Evidence

- `tests/segment_binding.rs`: canonical encoding and corrupt declarations,
  repeated publication, conflicting or missing segment bindings, legacy
  derivation, final deleted-segment removal and refusal to discard live rows.
- `tests/mapped_ingest.rs`: both layouts with WAL on/off, binding-only flush,
  restart and real peer snapshot transfer, first-row ingest after restore;
  partial and complete deletion followed by compaction and WAL-free restore,
  with exact binding and provider identity checks, then new ingest and a second
  snapshot without refitting calibration.
- Node publication tests: a real mutation after flush while copy waits for the
  seal mutex; malformed metadata-only snapshot claims leave the receiver
  untouched; interrupted generation-swap recovery retains the empty binding.
- `tests/snapshot_repository.rs`: mismatched FP32 rows and dimensions with
  recomputed artifact hashes refuse without changing live query results;
  unpublished staging files are omitted, and older repositories with a
  redundant standalone image retain the catalog's full query results.

Validation on 2026-09-06 passed 468 library tests, 648 integration tests across
113 targets, and 12 embedded tests: 1,128 passed, with one existing ignored
integration test. Cargo ran with two build jobs; library and integration tests
used four test threads, with integration targets in groups of six. All five
Android/iOS target checks, the tests/examples compile check, formatting,
vendored-proto identity and diff checks passed. The complete search descriptor
set is byte-identical to `a1e1881`. No fleet deployment, fleet benchmark or
physical-device runtime test ran.
