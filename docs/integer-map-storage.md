# Exact integer map storage

Status: storage, document ingestion, WAL replay and segment transplant are
implemented on `feat/integer-map-storage-2026-09`. Descriptor-driven mapping
and typed query integration are not complete.
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

## Document transport and rebuild

`AddDocumentsRequest.map_integers` (tag 27) and `map_unsigned_integers`
(tag 28) carry named column, string key and exact signed/unsigned value.
Configure new builders with `--map-integer-fields` and
`--map-unsigned-integer-fields`, their `PROTOMOLT_MAP_INTEGER_FIELDS` and
`PROTOMOLT_MAP_UNSIGNED_INTEGER_FIELDS` environment equivalents, or the
matching configuration arrays. MobileShardConfig exposes the same arrays at
29 and 30. These names share the existing column namespace. Unknown columns,
wrong column families and duplicate document/key entries fail before row
allocation. Low-level ingestion does not apply protobuf last-entry-wins rules.

WAL format 7 gates these entries. A writer resuming an older generation
publishes the new manifest version before appending a typed-map record or its
source blob. A failed manifest update leaves the record clock unchanged.
Existing index bindings still require format 6, independently of typed maps.
Older decoders reject the newer generation instead of dropping map fields.

Receiver document contract version 1 is exposed by HealthResponse tag 22 and
AddDocumentsResponse tag 5. Replication requires it before sending records
containing integer maps, unsigned scalar values, original source or identity,
and checks the write response as well. A legacy peer's successful row count
is insufficient. The acknowledgement describes accepted field semantics;
replication still requires Flush before advancing its durable cursor. As with
the existing positional replica protocol, retrying an accepted prefix assumes
that the receiver retains the same history. This version field is not a
content digest or a persistent per-write receipt.

Both image layouts retain typed values and source/identity through Flush,
reopen and compaction. Explicit WAL rebuild and segment transplant copy them
without a floating-point conversion. Compaction supplies the complete column
tables, retaining configured columns with no entries. WAL-only split paths
that infer tables from observed entries cannot recover an entirely absent
column declaration. Node open does not automatically replay the document WAL.

## Remaining integration

1. Persist complete column definitions for standalone WAL-only recovery,
   including columns with no entries, and define automatic recovery separately.
2. Bind descriptor-driven signed/unsigned map projections and report their
   preservation, indexing, and query capabilities separately. Keep the default
   and duplicate-key rules already established for protobuf map extraction.
3. Add exact typed map selectors to filters, values, sorting and aggregations.
   Scoring conversions need an explicit numeric contract; do not use a rounded
   floating-point filter as an exact integer predicate.
4. Exercise public typed-map query routes across both layouts and relays with
   permissions, original source bytes, stable identity, retries and durable
   receipts. These query routes are not yet implemented.

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
support for the document transport additions or unfinished query routes described above.

### Document transport validation

The transport, WAL and transplant additions passed 508 library tests, 750
integration tests across 130 targets, 12 embedded tests and two IVF-provider
tests: 1,272 passed, zero failed, with the existing live OpenNLP test ignored.
Android/iOS compilation passed on all supported targets, along with
compilation of tests/examples, formatting, vendored-proto and whitespace checks.
A descriptor comparison with `969a204` confirms that existing declarations
are unchanged: two map-entry messages and six fields were added.

The complete validation ran in one systemd scope with `MemoryMax=8G` and
`MemorySwapMax=0`. The cgroup peak, including build-file cache, was 8 GiB;
source inputs remained unchanged during the run. Targeted tests also cover
failed WAL-version publication, missing receiver capability before dispatch,
missing write acknowledgement followed by a retry, invalid map entries before
row allocation, original bytes and stable identity through compaction, and
exact map values across log rebuild, segment transplant and reordered year cuts.
