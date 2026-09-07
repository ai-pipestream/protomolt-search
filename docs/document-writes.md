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

Each catalog has a persistent, randomly generated 16-byte `history_id`. Moving
or reopening the file preserves it; creating another catalog, even for the same
collection, creates another identity. It identifies source lineage and must be
stored with projection checkpoints. It is not an authorization credential or a
lease fence, and does not distinguish divergent copies of the same catalog file.
Workspace binding and replicated authority remain separate unfinished work.

New clients discover the identity with a history request at `after_sequence=0`
and `through_sequence=0` (with valid positive limits). This returns an empty page
and the identity even when the first source is larger than the page budget.
They then write with `contract_version=2` and that `history_id`. The catalog
checks it before retry lookup or mutation; a foreign history returns
`FAILED_PRECONDITION` and consumes no sequence or operation ID. Version 1 remains
legacy unpinned local acceptance and requires an empty identity field. It must
not be used to claim protection against authority replacement. Older receivers
reject write version 2 rather than silently ignoring its identity precondition.

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
An identical retry returns the original acceptance decision with `replayed=true`, even if a
later replacement or deletion changed the head. Reusing an accepted ID for a
different request returns `ALREADY_EXISTS`. A failed version precondition returns
`ABORTED` and consumes neither a version nor an operation ID. Thus an unaccepted
operation can be corrected and submitted again.

Both write versions compare the SHA-256 of the generated request encoding. It
includes the exact source and descriptor bytes, identity, mutation and presence
of the precondition; version 2 also includes the pinned history identity. Alternate outer protobuf wire encodings of the same request
have the same meaning. Changes that give additional request fields meaning must
use a new contract version; unknown outer fields do not acquire semantics here.
Retry a pending operation with its original contract version and request.
Changing an existing operation from version 1 to version 2 changes its retry
hash and returns `ALREADY_EXISTS`; the identity precondition does not rewrite
an older operation. Keys are limited to 16 KiB and operation IDs to 1 KiB;
both must be nonempty.

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
Failure to acquire that lock fails opening. `open` requires an existing file:
missing files or parent directories return `NOT_FOUND` without creating storage.
Only an explicit `create` can initialize a new authority. This prevents a missing
mount or moved catalog from resetting versions and idempotency history.
Existing empty files, missing tables,
unknown catalog formats and collection mismatches fail instead of creating fresh
retry history. Content hashes are checked during source reads.

## Embedded and mobile use

Configure `EmbeddedSearchConfig.document_catalog` with the collection and a
stable private path. Use `path=None` only for explicitly volatile storage.
Omitting the configuration disables `accept_document`; legacy search and ingest
still work. `create` refuses an existing catalog; `open` requires and loads it.
The catalog is opened before any shard storage, so missing authority fails
without initializing replacement shard artifacts.

The mobile equivalent is `MobileOpenRequest.document_catalog`. Its `path` must
be nonempty for persistent storage and empty exactly when `in_memory=true`.
The bridge binds the runtime's shards to the configured collection. Invoke
`nativeAcceptDocument` on Android or `acceptDocument` in the Swift facade with
encoded `AcceptDocumentRequest` bytes. The `MobileResponse` payload is an encoded
`DocumentWriteReceipt`. Version conflicts retain the distinct mobile `ABORTED`
error code. A persistent mobile reopen with missing history returns mobile
`NOT_FOUND`; applications must restore or locate that history rather than
silently retrying as a create. Calls block through commit and should run off
the UI thread.

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
reuse that fence, the returned `next_sequence`, and `history_id`; later concurrent writes are
excluded. `complete=true` means the fence was reached. To tail subsequent writes,
omit the fence again while retaining the last sequence and identity. A nonzero
cursor or positive fence without an identity returns `INVALID_ARGUMENT`; a
foreign identity returns `FAILED_PRECONDITION`, including for an empty or complete
page. Clients must check the returned identity: an older reader can ignore the
request field and return no identity. Such a response cannot advance a bound
checkpoint. A zero-sequence discovery starts a history, never resumes one.

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

Catalog format 3 adds the history identity and a boundary for older retry
receipts. Opening format 2 commits those header fields once without rewriting
sources, versions, change entries or retry records. Opening format 1 also rebuilds
the ordered index from immutable versions, validating sequence uniqueness and
completeness in that same immediate transaction. A failed migration leaves the
original format intact. Old receipts are returned with the newly assigned
identity, while their stored bytes and acceptance decisions stay unchanged.
Only receipts through the recorded migration boundary may omit a stored identity;
new receipts with missing or foreign identities fail as data loss. Reopening
format 3 with a missing or malformed identity fails without generating one.
Older binaries refuse format 3. This is a source-authority migration; it does
not change physical index formats or require source reindexing. Preserve the
migrated authority in backups: restoring a pre-migration copy assigns a new
identity and requires reconciling any previously bound projection checkpoints.

The feed is publisher input, not a complete catalog backup: it does not expose
operation IDs and cannot reconstruct the persistent idempotency authority.

## Complete projection preparation

`DocumentCatalog::prepare_projection` and
`EmbeddedSearch::prepare_document_projection` read one exact accepted version
through `PrepareDocumentProjectionRequest`. This is trusted local source access,
with no network RPC or mobile entry point. The request pins the catalog history,
exact key and positive version; a later replacement cannot retarget it. The
catalog checks the version's accepted-history entry and source hashes in one
read snapshot, then derives the reviewed plan from its stored descriptor and
optional `IndexDefinition`. A missing or different fingerprint refuses.

The protobuf `PreparedDocumentProjection` carries the original source once and
a complete list of mapped, unanalyzed rows and vectors. Every row carries the
catalog's exact key and version. Flat identities have no chunk ordinal; chunked
identities use their zero-based source ordinal, independently of a payload's
legacy id or chunk-id hint. Source-only and unknown bytes remain in the original.
The resolved body path accompanies the plan fingerprint. Legacy mapped lineage
remains mapped payload data; it is not proof of catalog-authorized parent grouping.

A source with zero chunks returns a nondeleted batch with its original bytes,
reviewed mapping and no rows. An accepted deletion returns a tombstone batch
without a source, mapping or rows; requesting mapping options for a deletion
refuses. Neither case disappears from the version lifecycle.

`max_rows` is 1 to 65536 and is checked before per-chunk slot allocation.
`max_bytes` is 1 byte to 64 MiB of the entire encoded result, including metadata,
source, row identities, vectors and protobuf framing. Source sizes are checked
before copying large blobs. Rows are assembled one chunk at a time into a
private batch; per-chunk projection slots are released before assembling the
next chunk. An over-budget result or invalid later chunk returns an error, never a partial
batch. These are data budgets, not a hard bound on decoder/database heap usage.

Preparation is read-only. It does not analyze text, compute derived columns,
validate the complete runtime index binding, ingest rows, advance a publication
cursor or change any searchable receipt. The returned protobuf is data, not an
authorization or commit proof. Publication must still stage and verify the full
version, then atomically replace its visible chunk set under the index's binding
and authorization rules. It must not trust a caller-constructed batch as evidence
that the source authority accepted that version.

## Remaining lifecycle work

Imported row identities now accompany `Bm25Hit` from flat and fused lexical
search and lexical rescoring. Each node reads the identity under the same
shard-state guard that produces the score. Coordinator merging preserves it,
and simple lexical selection carries it into `QueryHit`, including the final
`QueryStream` response. Keys, versions and optional chunk ordinals survive
compaction and reopening; `doc_id` remains a generation-local locator.
Rows ingested without an identity report absence rather than a fabricated key.

The product-owned dense paths also retain scored identities through classic and
streaming scans; see [Dense identity](dense-identity.md). Remote-provider,
streaming parent-collapse and remaining legacy route identities remain,
while [provisional revisions](query-stream-identity.md) now carry imported keys
under the same admitted version and authority view. [Final Query results](query-result-identity.md)
now resolve identity across hybrid, Boolean and browse adapters, including collapse
inner hits, under the same selection version and authority view.
These paths report imported row metadata;
it does not certify that the catalog accepted that version or that it is the
current authorized version. Publication and authorization must supply those
guarantees before this becomes the complete document-facing search contract.

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

## Missing-authority recovery regression

Checkpoint `e658840` still recreated a catalog when its file was missing on
`open`. Two regression tests reproduced that behavior: direct catalog reopen
silently initialized fresh history, and embedded reopen continued into shard
startup. The corrected path opens an existing file without a create fallback.
Tests move an accepted catalog away, verify refusal without replacement files,
restore it, and verify that the original operation returns its original receipt
as a retry. The mobile ABI carries the missing-history error as `NOT_FOUND`
and also verifies the restore/retry sequence. Explicit create and explicitly
volatile catalogs retain their existing behavior.

This fixes source-authority recovery. It does not implement the index projection
publication and target WAL application proofs described above.

Validation for the missing-authority fix passed 566 local tests: 540 unit tests,
13 catalog integration tests and 13 embedded/mobile/dependency tests. All five
Android/iOS target checks, test/example compilation, formatting, vendored-proto
checks and existing-field descriptor comparisons passed. The two new catalog
regressions failed before the fix and passed afterwards. These were the shared
unit suite and affected integration targets, not a rerun of every integration
target. All builds/tests ran under an 8 GiB memory cap with swap disabled and
two Cargo build jobs. No fleet rollout or hosted-CI result is claimed.
