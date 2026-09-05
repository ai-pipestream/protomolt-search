# Original protobuf storage

Mapped ingest retains the producer's exact descriptor-set bytes, root message
name and payload for each source that produces index rows. It does not encode a
new message from the projection decoder. Unknown fields, unknown enum values,
wire ordering and noncanonical encodings therefore survive unchanged. The
projection contract still determines which fields can be indexed and queried.

This is an increment toward the [search foundations](search-foundations.md),
not a completed logical document store. A mapped document with zero chunks still
produces no storage row and is counted by the existing ingest response without
being retained. The separate [logical catalog](document-writes.md) now retains
accepted source versions without any rows and supplies local conditional writes,
retry history and acceptance receipts. Connecting that authority to atomic index
publication and every public identity path remains unfinished.

## Index images

`ProtobufSource` is the transport envelope. `SourceRecord` stores the root type
and exact payload with the SHA-256 of the original descriptor bytes. A source
archive interns descriptors and source records independently; chunk rows carry a
source ordinal and their original chunk ordinal. Content addresses identify
bytes within storage, not public logical document identity.

Rows may also carry a `DocumentIdentity`: an exact collection-local document key,
a positive source version, and optional chunk ordinal. `AddDocumentsRequest`
retains this metadata with `original_source`; its chunk ordinal must agree with
`source_chunk_ordinal`. The legacy route imports the metadata but does not enforce
conditional version writes or persistent retries.

The archive interns each key/version pair once in `SourceArchiveIndex.identities`.
Rows reference that entry and retain their own chunk ordinal. A key/version pair
cannot refer to different source bytes within one archive. The metadata resides
in the archive index, so identity lookup does not load the original payload.
This keeps long keys from being repeated on every chunk row. Archive format 2
marks this extension; source-only archives continue to write format 1, and this
reader accepts both. Older format-1 readers refuse identified archives.

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

## Immutable identity views

`identity_snapshot()` on the heap store, spill builder and image reader captures
row-to-identity bindings. The segmented store and `Bm25Shard` expose the same
view across sealed parts, a frozen seal and the mutable tail. Segment ranges
are captured with the metadata and checked for overlap/order/overflow; a lookup
cannot fall through a gap into the wrong part. Missing legacy identities remain
absent, including when a later append fills a previously empty row.

Capture the view while holding the state lock used to select/score rows. It
remains usable after that state is released, moved into a sealed segment,
replaced or dropped. Taking a view after an unfenced query cannot recover the
query's earlier generation. The view contains no live-row filter, policy decision
or document-catalog head and does not itself certify search eligibility.

Heap and spill metadata use shared pages of 1,024 entries. Creating a view
clones the page-directory handles; a subsequent write detaches the directory
and touched pages, sharing unchanged key bytes. Directory copying costs one
pointer per page, so it is not a constant-cost write guarantee. Image readers
share their parsed archive index. A segmented capture visits its parts; cloning
the resulting view is constant-time. Views retain metadata and exact keys, but
no original payload blobs, frozen stores or mapped index files. Tests verify
that those owners can be dropped and their files removed while old bindings
remain readable.

A comparison with the writer at checkpoint `b9c99ea`, using sparse rows across
page boundaries with and without logical identities, produced byte-identical
format-1 and format-2 archives.

This changes no protobuf fields, archive format or persisted identity meaning.
It is the metadata retention seam for query generation handling; dense and
other currently unsupported result routes do not gain identity merely because
a view exists. Their scan, completion and result propagation still need to
carry the matching view or bindings end to end.

## Write log and lifecycle

WAL format 2 adds `sources.wal` within each generation directory. It begins with
`PMSWAL01` and contains the same length/CRC-framed protobuf blobs as the record
logs. Descriptors and original sources are each interned once per generation.
Chunk records contain `SourceReference` addresses instead of repeating the
parent's payload and descriptor. A legacy format-1 manifest is upgraded before
the writer appends its first source reference; this build reads both versions.
Pre-source-storage binaries reject format 2. A document carrying logical identity
upgrades the WAL manifest to format 3 before the referencing record is appended,
so a format-2 reader cannot silently discard that metadata during replay. This
build reads formats 1 through 3; ordinary new node logs start at format 2 and
advance only when identity is used. The source-blob framing remains unchanged.

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
The same paths retain logical identity while compaction renumbers physical rows.

This does not change the existing acknowledgment contract: applying a row is
volatile until Flush, and WAL failures still use the existing degraded-mode
behavior. It does not introduce transactional writes or remote durability.

## Access and verification

Source lookup is currently a trusted local library operation. There is no new
public source-fetch RPC or automatic source field in search responses. Document
and field authorization must cover any future disclosure API. The embedded
library continues to keep source, WAL and index files on the phone and has no
network transport dependency.

`StoredDocument.identity` exposes imported identity on the existing node fetch
route. `GetDocuments.doc_ids` still addresses current-generation physical rows;
it is not a stable-key lookup or a way to recover the identity of an earlier
unfenced search after compaction. Query/stream hit propagation and versioned
publication remain outstanding. Oversized physical IDs are rejected from lookup
instead of narrowing to another row's u32 slot.

Tests cover byte equality through heap/spill/mapped images, snapshots, replica
catch-up, resharding and both compaction layouts; archive truncation and bit
corruption; WAL deduplication across buckets and restart; source corruption;
and incomplete unreferenced WAL tails. These are local correctness checks,
not fleet performance or mobile device measurements.
Compaction tests additionally compare exact keys, source versions and chunk
ordinals in image readers and node fetches after renumbering, reopen and replay.
