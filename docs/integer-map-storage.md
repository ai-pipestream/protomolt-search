# Exact integer map storage

Status: storage and segmented-read integration in progress on
`feat/integer-map-storage-2026-09`. Public protobuf ingestion, WAL replay,
transplant/compaction, mapping, and typed query integration are not complete.
Do not treat the low-level storage API as an end-to-end supported map shape.
`docs/map-projection.md` remains the public mapping contract.

Signed and unsigned map values use distinct column kinds, 15 and 16, in the
kinded BM25 column table. The value never passes through `double`. A present
zero, `i64::MIN`, and `u64::MAX` are ordinary entries. An omitted key is absent.
An empty string is a valid key. The low-level setter rejects a second value
for a document/key pair; protobuf duplicate-entry resolution belongs to the
projection layer before this setter.

Each table entry contains the column name, kind, section offset, and section
length. Integer map sections follow scalar unsigned columns and precede the
original-source archive. Existing floating-point map columns retain their
format. The v8 envelope covers the complete new sections with integrity
checks. An older reader encounters an unknown column kind and rejects the
file. The v4/v5 writers reject integer map columns before writing the payload.

Section version 1 uses little-endian integers:

- `u32` version, document-slot count, and key count.
- Keys sorted by UTF-8 bytes. Each key has a `u32` byte length, bytes, a one-byte
  bound-presence flag, and two 64-bit values holding the exact minimum and
  maximum in the column's signedness. No bounds use flag zero and zero bits;
  present bounds use flag one, including `(0, 0)`.
- One `u64` pair offset per row plus a final offset. Offsets count pairs.
- Pairs of `u32` key ordinal and 64-bit value, strictly ordered by key within
  each row. Signed values retain their two's-complement bits.

Offsets use 64 bits so pair cardinality is not limited to the row-id domain.
Key lengths use 32 bits, including keys longer than 65,535 bytes. Writers
remap one document's pairs at a time. Heap bounds update on insertion; sealed
bounds come from checked metadata. The reader verifies section geometry,
UTF-8, dictionary ordering, offsets, pair ordinals, duplicate keys, and exact
agreement between stored bounds and values before exposing reads. This
currently requires scanning entries at open, consistent with existing map
column validation; it is not a lazy-open format.

Segmented shards compare these column tables during open and tail replacement.
Their key dictionaries translate ordinals separately for sealed segments, the
frozen tail, and the mutable tail. Bounds combine exact typed minima/maxima;
missing keys contribute no bound. Sealing does not change values or entry
presence.

Tests in `tests/integer_maps.rs` cover heap/spill byte agreement, reload and
rewrite, source archive coexistence, full-width values, absent and empty keys,
long UTF-8 keys, empty files/columns, rejected duplicate writes, legacy writer
rejection, and segment lifecycle transitions. The section tests in
`src/integer_map.rs` cover truncation and forged geometry, keys, and bounds.

## Remaining integration

1. Add typed document entries to the search protobuf and a WAL version gate
   persisted before any record containing them. Older readers must reject a
   generation they cannot replay exactly. Replication must establish typed-map
   support before accepting such writes.
2. Preserve column tables and entries through rebuild, transplant, compaction,
   snapshot/recovery, and mutable tail construction, including empty columns.
3. Bind descriptor-driven signed/unsigned map projections and report their
   preservation, indexing, and query capabilities separately. Keep the default
   and duplicate-key rules already established for protobuf map extraction.
4. Add exact typed map selectors to filters, values, sorting and aggregations.
   Scoring conversions need an explicit numeric contract; do not use a rounded
   floating-point filter as an exact integer predicate.
5. Exercise the public routes across both layouts and relays with permissions,
   original source bytes, stable identity, retries and durability receipts.

## Validation, 2026-09-07

On base `3d0f109`, the storage changes passed 508 library tests, 746 integration
tests across 128 targets, 12 embedded tests and two IVF-provider tests: 1,268
passed, zero failed. The existing live OpenNLP conformance test remains ignored.
All five Android/iOS compile targets, tests/examples compilation, formatting,
vendored-proto verification and whitespace checks passed. Protobuf definitions
and dependency manifests/locks are unchanged.

The integer map lifecycle test additionally writes a new key to the fresh tail
while a prior tail is frozen and verifies both before and after publication.
That strengthened test and duplicate-write assertions were rerun after the full
suite. Production sources remained unchanged during validation. These results
cover the storage APIs and existing behavior; they do not establish typed-map
support for the unfinished public and replay routes listed above.
