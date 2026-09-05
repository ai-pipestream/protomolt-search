# Logical document acceptance

`document_write.proto` defines the source/version transaction independently of
physical index rows. `src/document_catalog.rs` implements the local authority;
`EmbeddedSearch::accept_document` and the C/JNI mobile bridge use it directly.
This is the source acceptance stage of the write lifecycle. Index publication,
server routing and document/field authorization remain unfinished.

## Identity and retry rules

An exact byte `document_key` identifies a document within one catalog's bound
collection. It is neither a hash nor a physical row ID. A catalog covers the
whole collection; its explicit path stays fixed across changes in shard count,
slot offsets and index generations. Embedded configuration requires every shard
to name that collection. A second writer cannot open the same catalog file.
When a catalog is configured, storage parents must exist. Canonical paths reject
placing the catalog within known index artifacts, snapshot/segment trees, BM25
spill directories, WAL trees or default compaction paths. Custom compaction work
directories must be empty under the existing compaction preflight.

Every successful mutation increments the document version and the catalog's
accepted sequence. Version zero means no prior version. A deletion creates a
tombstone version; it never resets the version counter or removes history.
`expected_version` has protobuf presence:

| Value | Precondition |
|---|---|
| Absent | Unconditional write |
| Zero | No prior version, including no tombstone |
| Positive | Exact current version, including a tombstone |

An operation ID is also exact bytes, scoped to the entire catalog. A transaction
first checks whether that ID was already accepted, before checking the version.
An identical retry returns the original receipt with `replayed=true`, even if a
later replacement or deletion changed the head. Reusing an accepted ID for a
different request returns `ALREADY_EXISTS`. A failed version precondition returns
`ABORTED` and consumes neither a version nor an operation ID. Thus an unaccepted
operation can be corrected and submitted again.

Contract version 1 compares the SHA-256 of the generated request encoding. It
includes the exact source and descriptor bytes, identity, mutation and presence
of the precondition. Alternate outer protobuf wire encodings of the same request
have the same meaning. Changes that give additional request fields meaning must
use a new contract version; unknown outer fields do not acquire semantics here.
Keys are limited to 16 KiB and operation IDs to 1 KiB; both must be nonempty.

## Preservation and receipts

The source envelope retains the producer's descriptor set, message type and
payload bytes. Acceptance requires nonempty descriptor/type fields; it treats
their content as opaque. It does not assert that a schema can be planned, that
the payload is valid for that schema, or that a projection can be built. Empty
payloads and documents producing no index rows can be retained. Validation and
projection must occur before any searchable acknowledgment.

One transaction stores the new head, immutable version, original source,
descriptor and retry receipt. Source and descriptor blobs are content-interned;
the exact document key remains the identity. Deletes retain prior sources and
retry history. No automatic history expiration or garbage collection exists.

| Receipt | Meaning in the current implementation |
|---|---|
| `accepted=true` | The source/history transaction committed |
| `durable=true` | The persistent local transaction completed its sync boundary |
| `durable=false` | The caller explicitly selected volatile storage |
| `searchable=false` | No index projection was published by this operation |

Errors return an error status, not a successful receipt with guessed flags. If a
commit or its acknowledgment is interrupted, retry the same operation ID and
request. A positive durable receipt promises local storage, not another device's
copy, a search snapshot, or protection against loss of the phone.

The store pins redb 4.1.0, uses `Durability::Immediate` for commits, and syncs the
catalog's parent directory before opening succeeds. The parent must already
exist. The cache is explicitly 8 MiB; redb's
[builder default is 1 GiB](https://docs.rs/redb/4.1.0/redb/struct.Builder.html#method.new).
A separate exclusive file lock covers backends where redb does not acquire one.
Failure to acquire that lock fails opening. Existing empty files, missing tables,
unknown catalog formats and collection mismatches fail instead of creating fresh
retry history. Content hashes are checked during source reads.

## Embedded and mobile use

Configure `EmbeddedSearchConfig.document_catalog` with the collection and a
stable private path. Use `path=None` only for explicitly volatile storage.
Omitting the configuration disables `accept_document`; legacy search and ingest
still work. `create` refuses an existing catalog; `open` loads it.

The mobile equivalent is `MobileOpenRequest.document_catalog`. Its `path` must
be nonempty for persistent storage and empty exactly when `in_memory=true`.
The bridge binds the runtime's shards to the configured collection. Invoke
`nativeAcceptDocument` on Android or `acceptDocument` in the Swift facade with
encoded `AcceptDocumentRequest` bytes. The `MobileResponse` payload is an encoded
`DocumentWriteReceipt`. Version conflicts retain the distinct mobile `ABORTED`
error code. Calls block through commit and should run off the UI thread.

The source store adds no networking. The Rust `accepted_document` lookup is for
the trusted local application. No network source-fetch service is exposed.

## Ordered source history

`ReadAcceptedDocumentsRequest` and `ReadAcceptedDocumentsResponse` let a local
projection worker consume original versions in acceptance order. The same
transaction that accepts the source, version and retry decision also appends its
sequence-to-version entry. Reading history does not mark a version searchable.
It includes replaced versions and tombstones, independently of the current head.

Start with `after_sequence=0`, omit `through_sequence`, and supply `limit` and
`max_bytes`. The response pins the current upper sequence. For subsequent pages,
reuse that fence and the returned `next_sequence`; later concurrent writes are
excluded. `complete=true` means the fence was reached. To tail subsequent writes,
omit the fence again while retaining the last sequence. These cursors belong to
the same catalog and are not portable to a newly created authority.

Each page reads one database snapshot. Limits are 1 to 1000 versions and 1 byte
to 64 MiB of summed encoded `AcceptedDocumentVersion` values. Metadata framing
of the outer response is outside that byte count. A first version exceeding the
budget returns `RESOURCE_EXHAUSTED`, with no cursor advancement; otherwise the
page ends before that version. This bounds returned data, not all allocations
inside the database. A source too large for a page remains available through the
trusted single-version Rust lookup. Missing sequence/version references fail as
data loss; the reader never skips a gap.

The Rust entry is `EmbeddedSearch::read_accepted_documents`. Android exposes
`nativeReadAcceptedDocuments`, and Swift exposes `readAcceptedDocuments`, both
using protobuf bytes through the local bridge. These operations return original
sources to the owning application; they introduce no transport. The bridge
preserves `RESOURCE_EXHAUSTED` and `DATA_LOSS` as distinct mobile error codes.

Catalog format 2 adds the ordered index. Opening a format-1 catalog rebuilds it
from immutable versions in one immediate transaction, validates sequence
uniqueness/completeness, and advances the header only with the complete index.
A failed migration leaves format 1 intact. Original source bytes, keys, versions
and retry receipts are unchanged. Older binaries refuse format 2. This is a
source-authority migration; it neither changes physical index formats nor asks
the application to discard and rebuild its source catalog.

The feed is publisher input, not a complete catalog backup: it does not expose
operation IDs and cannot reconstruct the persistent idempotency authority.

## Remaining lifecycle work

This API does not feed legacy `IngestMapped` automatically. Its accepted deletes
do not remove existing legacy search rows. Legacy ingest still lacks these
transactional receipts and still loses mapped parents that produce no rows.
Next, projection publication must consume accepted versions, replace all chunks
atomically, and return stable document/chunk identity through every result path.
The searchable state must reflect that publication, including recovery.

The catalog is outside current index snapshots, replica bootstrap and row
resharding. Those operations do not constitute a backup or migration of this
authority. Until a coordinated backup/export protocol exists, retain the catalog
at its configured path. A phone must keep that authority local. Multi-host
authority routing, workspace binding, document/field grants and coordinated
source/projection recovery remain part of the foundation goal.

`tests/document_catalog.rs` covers concurrent compare-and-set, concurrent retries,
restart after replacement/delete, exact original bytes, empty source payloads,
exclusive opening, refusal of incomplete catalogs, and embedded reopening with a
different shard layout. A subprocess exits immediately after its durable receipt,
skipping database destruction; its retry and source survive recovery. This is
process-exit evidence, not a physical power-loss or device-runtime test. The
mobile bridge test exercises the C ABI and conflict mapping; mobile compilation
and the no-network dependency gate remain separate required checks.
History tests cover fixed-fence pagination during new writes, exact byte budgets,
replaced/deleted sources, successful format-1 upgrades and rollback of an
incomplete-history migration.
