# Original protobuf storage

Mapped ingest retains the producer's exact descriptor-set bytes, root message
name and payload for each source that produces index rows. It does not encode a
new message from the projection decoder. Unknown fields, unknown enum values,
wire ordering and noncanonical encodings therefore survive unchanged. The
projection contract still determines which fields can be indexed and queried.

This is an increment toward the [search foundations](search-foundations.md),
not a completed logical document store. A mapped document with zero chunks still
produces no storage row and is counted by the existing ingest response without
being retained. The standalone archive can hold originals without rows, but the
node needs a logical document catalog independent of its segment row geometry
before that capability is exposed. Stable keys, versioned writes, idempotency and
per-write durability receipts remain separate, outstanding work.

## Index images

`ProtobufSource` is the transport envelope. `SourceRecord` stores the root type
and exact payload with the SHA-256 of the original descriptor bytes. A source
archive interns descriptors and source records independently; chunk rows carry a
source ordinal and their original chunk ordinal. Content addresses identify
bytes within storage, not public logical document identity.

The current TVBM2508 writer adds a kind-9 column-table entry named
`protobuf-sources`. Its offset and length address the final payload section.
The archive begins with `PMSOURCE`, a little-endian u64 protobuf-index length,
and the index's 32-byte SHA-256. The encoded `SourceArchiveIndex` is followed by
the descriptor and source-record blobs it addresses. Blob ranges tile the
section without gaps or trailing bytes. The enclosing v8 CRC table covers the
section; source lookup also verifies the referenced blob hashes.

Readers validate the archive index when opening an image and borrow source
bytes from the memory map until lookup. Heap builders retain source bytes in
memory. Spill builders write them to `sources.spill` and retain only addresses
and row references in memory. Index sealing copies those bytes into the image.
Sealed segments each own their archive, so a parent crossing segment boundaries
can occur once in each affected segment. Legacy image writers refuse to discard
retained sources. Older readers reject the new column kind; source-free images
keep their existing representation.

## Write log and lifecycle

WAL format 2 adds `sources.wal` within each generation directory. It begins with
`PMSWAL01` and contains the same length/CRC-framed protobuf blobs as the record
logs. Descriptors and original sources are each interned once per generation.
Chunk records contain `SourceReference` addresses instead of repeating the
parent's payload and descriptor. A legacy format-1 manifest is upgraded before
the writer appends its first source reference; this build reads both versions.
Old binaries reject format 2.

Source blobs are written before referencing row frames and fsynced before the
row logs at Flush. The generation directory is also synced. An incomplete,
unreferenced source tail is removed on the next source append. A row referring
to missing, truncated, corrupt or wrongly addressed source bytes is a hard
replay error. Unreferenced blobs left by interrupted or truncated writes remain
until the generation is reclaimed.

`RecordReader` resolves references into complete `AddDocumentsRequest` values.
Replica tails are self-contained even when they start after a parent's first
chunk. Offline resharding and online compaction use the same reader and retain
source bytes for surviving rows. A compaction's new WAL interns the surviving
sources again, so deleting the first chunk cannot invalidate another chunk's
source. Snapshot installation carries source archives inside the image.

This does not change the existing acknowledgment contract: applying a row is
volatile until Flush, and WAL failures still use the existing degraded-mode
behavior. It does not introduce transactional writes or remote durability.

## Access and verification

Source lookup is currently a trusted local library operation. There is no new
public source-fetch RPC or automatic source field in search responses. Document
and field authorization must cover any future disclosure API. The embedded
library continues to keep source, WAL and index files on the phone and has no
network transport dependency.

Tests cover byte equality through heap/spill/mapped images, snapshots, replica
catch-up, resharding and both compaction layouts; archive truncation and bit
corruption; WAL deduplication across buckets and restart; source corruption;
and incomplete unreferenced WAL tails. These are local correctness checks,
not fleet performance or mobile device measurements.
