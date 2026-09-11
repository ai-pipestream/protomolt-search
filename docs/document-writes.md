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
| `write_epoch` | The owner's write epoch the write committed under on an activated managed source; zero elsewhere ("Write outcomes" below) |

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

## Write outcomes

On an activated managed source every write is admitted under a lease
([admission under Raft](raft-admission.md)), and the lease is judged
twice: at the final check immediately before the commit, and again when
the commit returns durable. The second judgement is what the storage
records, on the operation record and on the version, as the write's
outcome (`WriteOutcome` in the storage proto, catalog format 11):

| Outcome | Meaning |
|---|---|
| `ACCEPTED` | The lease was still open when the commit returned; the write was admitted under a right current at durability. Every record written before outcomes existed decodes as this, which is what it was. |
| `UNCONFIRMED` | The commit returned after the lease lapsed. The row is durable; whether it was admitted is not yet decided. Marked in a durable transaction of its own, before the caller learns of it. |
| `FENCED` | Settled under a fresh lease and the actor's right was gone: the grant revoked, or the owner no longer ACTIVE under the write's epoch. Marked with the control revision this replica had applied. Final. |

An unconfirmed write is settled by `ActiveManagedCatalog::settle` under a
fresh leased admission for the same actor: the entry check run again.
Right current, the record becomes ACCEPTED and the receipt is returned;
right gone, the record becomes FENCED and the settlement is a
`FAILED_PRECONDITION` naming the version, the sequence, the epoch and the
revision. An admission that lapsed again settles nothing, by name, and
the record stays unconfirmed for the next attempt. A retry of an
operation whose record is unconfirmed is answered from the record before
the entry check, and settled under the retry's own admission, which is
fresh at entry; a retry of a fenced operation replays the rejection. A
fenced version stays in the history at the sequence it took, marked
(`AcceptedDocumentVersion.fenced`, `fenced_at_revision`), and remains the
head of its key, so the next version of that key counts on from it; a
reader that applies history treats a fenced version as never admitted.
Every version carries the epoch it committed under (`write_epoch`, zero
on a catalog that is not an activated managed source).

The hosted write service settles on the same call
([hosted owner writes](raft-hosting.md#hosted-owner-writes)): a write
returned unconfirmed takes a fresh lease under the call's permits and
answers with the settlement; when no lease can be had, the call is
`UNAVAILABLE` naming the durable version as unconfirmed, and the exact
retry settles it. What this closes, and the residual it leaves, are in
[admission under Raft](raft-admission.md), "Source write boundary".

Fault injection (`fault-injection`): `arm_postcommit_pause` holds a write
after its commit returned and before its outcome is judged, the window
the outcomes exist for.

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

## Hosted owner writes

A Raft member serves the `DocumentWriteService` proto
(`GetDocumentWriteTarget`, `AcceptDocument`) over its activated managed
catalogs through `HostedDocumentWriteService`
(`src/document_write_service/hosted.rs`), instead of the
single-authority `DocumentWriteServiceImpl` over explicitly provisioned
local catalogs. Clients do not change; a process serves one variant or
the other, never both, and the two share the `Route` rows.

Every admission on the hosted path comes from the member's Raft host:
the transport gate (`Principals::authenticate` and
`authorize(.., Ingest)`) stays first and the authority admission is
authoritative, with the lease actor equal to the transport principal
name. The grant runs on a blocking worker and the commit carries the
admission's final check; the commit's return is judged against the lease
once more and recorded as the write's outcome ("Write outcomes" above),
settled on the same call under a fresh lease when the first lapsed. The
lease, recovery and failure mapping are in [hosted owner
writes](raft-hosting.md#hosted-owner-writes) and the lease argument in
[admission under Raft](raft-admission.md).

## Ordered source history

`ReadAcceptedDocumentsRequest` and `ReadAcceptedDocumentsResponse` let a local
projection worker consume original versions in acceptance order. The same
transaction that accepts the source, version and retry decision also appends its
sequence-to-version entry. Reading history does not mark a version searchable.
It includes replaced versions and tombstones, independently of the current head,
and fenced versions marked as such ("Write outcomes"), with every version's
write epoch.

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

An explicit `DocumentCatalog::seal_history` retirement commits format 4 with a
terminal seal at an exact accepted watermark. It preserves the history identity
and all retry/source records. All subsequent source and index journal writes
refuse, including after reopen or copying a sealed backup. Active catalogs stay
format 3; there is no automatic retirement on lease expiry and no unseal API.
Historical reads remain available. See [source writer retirement](source-index-backup.md#terminal-retirement-of-the-source-writer)
for the pending-work rule and the separate replacement-activation boundary.

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

## Private analyzed candidates

`StageDocumentProjectionRequest` adds the next preparation step. The trusted
Rust owner calls `EmbeddedSearch::stage_document_projection(shard, request)` or
`NodeServiceImpl::stage_document_projection(catalog, request)`. It names an exact
accepted projection, a complete explicit `field_analysis` contract, optional
materialization and a positive final-file byte budget. Source bytes, document
keys, versions and chunk identities come from the catalog; callers cannot submit
replacement rows. The catalog's immutable collection must match the target node.

The candidate uses a private node with the target's column and derived-column
declarations, phrase configuration, provider calibration and configured analyzer.
Its input joins the same analysis stream, field analysis, validation and
materialization pipeline as ordinary ingest. The existing target's mapping,
analysis and materialization binding must match. Empty sources also require a
configured analyzer; native aliases validate every explicit field specification
before staging. A populated unbound target is refused. Source values, full-width unsigned values and presence, exact source
bytes and catalog-owned row identities reach the ordinary segment artifacts.
`hash.fnv64(stable_key())` receives the exact catalog key. Private work does not
update the target's vocabulary observations, live rows, WAL or read version.

The returned `StagedDocumentCandidate` owns its private files and exposes a
protobuf `DocumentProjectionStage` description and read-only segment access to
the trusted owner. It also retains the exact original source, including for zero
rows. An empty source has an empty segment set with its reviewed binding; a
deletion has neither a source nor a segment set. The target statistics
incarnation/epoch in the description identifies the state used for preparation;
it must be rechecked before publication. The description is not a credential or
a commit certificate. Neither it nor staging changes the original acceptance
receipt, and no searchable receipt is issued.

The target needs a persistent index path with an existing parent. Preparation
creates a unique private sibling directory (mode 0700 on Unix); it does not
rewrite the target's catalog. Source preparation keeps its existing row and byte
limits. `max_staged_bytes` accepts 1 byte to 1 GiB and limits the final private
files before a candidate is returned. It is not a disk quota or a bound on all
analysis/decoder allocations. Automatic tail seals are disabled for the single
bounded source; the closing flush seals its complete candidate. Blocking file
work retains directory ownership through completion. Normal drop and failed
preparation remove private files; abrupt process exit can leave orphan stage
directories, which are never committed publication evidence.

There is no new network RPC or mobile ABI command. The embedded Rust entry uses
its existing native analyzer and opens no socket. Server-side staging uses only
the node's configured analysis backend. Local activation is described below;
visibility across shards and later searchable receipts remain lifecycle work.
A successful private build cannot advance a
source-history publication cursor by itself. `tests/document_staging.rs` covers
the accepted-source-to-segment path, late row failures, output budgets, collection
and analysis refusals, empty sources and deletions, and the embedded entry.

## Durable projection decisions

The trusted Rust owner can call `DocumentCatalog::prepare_index_publication`
with a private candidate and validated before/after segment snapshots while
holding the target publication fence. The catalog derives the version and source
hash from its immutable accepted history. It verifies the candidate's exact source
bytes and history identity, the appended artifacts, and retirement of every live
previous row of that document key. Unrelated rows, index bindings and partition
settings cannot change in the same certified transition.

The optional journal extension stores protobuf records in two redb tables:
per-index state and immutable decisions keyed by index identity plus accepted
sequence. Its format-1 header and tables are created together with the first
intent under immediate durability. Reopen rejects an unsupported extension,
foreign history or missing tables. Source catalog format 3 and existing receipts
are unchanged; older source acceptance writers preserve these additional tables.
An in-memory source catalog cannot prepare a durable publication intent.

Each logical index processes accepted sequences consecutively starting at 1
from an empty segment set. The index key is an exact binary logical identity,
1 to 1024 bytes, not a filesystem path or node address. An intent records the
source version, row count, adjacent manifest epochs and SHA-256 digests of both
typed manifests. Format 1 hashes their `serde_json` encoding, including artifact
hashes and the binding, independent of the manifest file's whitespace. The
intent ID hashes its protobuf encoding with that ID field empty. There is at
most one pending intent per index. An exact retry returns the original intent;
a different transition must first resolve the pending one.

`recover_index_publication` holds the segment catalog's update fence, checks that
the on-disk manifest matches its opened snapshot, and syncs the manifest and its
directory and the link from its existing application container before committing
a source-journal decision. Matching the intended
after-manifest commits the decision and cursor atomically. Matching the
before-manifest aborts only the projection intent, leaving source acceptance
unchanged. A third manifest refuses recovery and retains the intent. Uncertain
manifest publication requires reopen first. Even with no pending intent, the
durable manifest must match the committed cursor, which must match its immutable
decision. Empty sources and deletions both need explicit sequence transitions,
including a new manifest epoch when there are no physical rows to change.

This is artifact transaction evidence, not proof of active serving state.
`index_publication_decision` returns historical decisions without claiming that
their source versions are still searchable. The original acceptance/retry receipt
is never rewritten. There is no new network RPC, mobile ABI command or positive
searchable receipt. The local activation path below joins the journal to the
serving view. Source activation now installs persistent ownership in the segment
manifest and fences legacy writers as described below. Source-aware compaction,
coherent backups and coordination of documents spanning shards remain unfinished;
legacy maintenance cannot silently advance the source cursor. Regression tests in
`document_catalog::publication::tests` exercise actual staged source rows,
replacement and retirement checks, exact retries, empty/deleted sources, cursor
ordering, foreign histories, missing tables and interrupted manifest commits.

## Local source activation

`NodeServiceImpl::publish_document_projection_blocking` takes the source catalog,
an exact logical index key, and the private candidate. It derives artifact paths
and complete retirements itself; callers do not submit arbitrary rows or source
identities. The collection must match and the read version captured at staging
must still hold. The target must have a persistent, sealed segment catalog with
WAL disabled and no active compaction. A configured WAL remains a refusal even
if a failed writer was removed. Runtime deletions absent from the catalog also
refuse certification. The application supplies existing durable container
directories for the source authority and index, as for source-catalog creation.

The node holds its ingest, mutation and seal gates through the transaction.
It prepares both search legs and exact-vector views before taking the final
serving-state write lock. Under that lock it rechecks the read version and
records the source intent, publishes the manifest, activates the prepared view,
advances the statistics epoch, and resolves the journal decision. A zero-row
source still publishes its reviewed binding and a new epoch. Deletions retire
all prior live chunks; repeated deletions also advance the source sequence
without creating dummy rows.

Success returns a protobuf `DocumentProjectionActivation` identifying the source
history/key/version, accepted sequence, logical index, artifact decision, row
count, catalog epoch, and active shard read version. This is a local observation
at that read version, not a collection-wide or perpetual visibility promise.
Original acceptance/retry receipts remain unchanged. If manifest publication is
uncertain, the old runtime remains active and ingest is fenced. If the runtime
has activated but the journal decision cannot be acknowledged, the method returns
an error and fences ingest; it does not fabricate a successful observation.

After reopen, `recover_document_projection_blocking` resolves pending artifact
transactions and verifies that the current journal decision matches the durable
catalog and the active, sealed serving state. It returns that decision with the
new lifetime's read version, or no activation when none has committed. Stale
candidates must be discarded; recovery observes an already committed transition
without appending another copy. The embedded Rust owner exposes asynchronous
`publish_document_projection` and `recover_document_projection` methods; its
blocking worker retains ownership of candidate files even if the caller stops
awaiting it. The same socket-free search path observes the published rows.

The first source activation atomically installs `SourceIndexOwner`, including
on an empty source or deletion-only index. It identifies the source history,
logical index and collection; it contains no path, node address or credential.
The segment manifest stores canonical protobuf bytes and their SHA-256 under
format 3. Formats 1 and 2 remain readable for unowned indexes; older readers
refuse format 3. A later source transition must preserve the same owner, and
recovery verifies that it belongs to the supplied source catalog and index key.
There is no separate mutation that adopts an arbitrary populated legacy index.

Owned indexes refuse legacy row, vector, delete, replacement, binding and backend
mutations at their commit boundaries. Generic catalog publication and staged
compaction cannot remove or replace the owner. Startup requires the original
collection, segmented layout, WAL disabled and no competing snapshot generation.
The local attachment methods reject replacement exact-vector data and extra
runtime tombstones. As with other local persistence, this is not protection from
an actor who can rewrite the files directly; independent writers must not share
a root without the owning application's serialization.

Index-only snapshot export and import are unavailable for source-managed indexes:
copying index files without the authoritative source catalog is not a coherent
backup. Legacy compaction is also refused. Both need journal-aware lifecycle
transactions before support can be enabled. No public document-write RPC, mobile
ABI publication command or collection-wide searchable receipt is added here.
Tests cover actual lexical/vector reads and identities, stale candidates, empty
sources, deletions, embedded restart, rejected unjournaled mutations, persisted
ownership and both interruption windows with the source catalog and serving node
reopened.

Ownership checkpoint validation: 588 library tests, 831 integration tests across
142 targets, 13 embedded tests and two IVF adapter tests passed. The existing
live OpenNLP comparison remains ignored. All five Android/iOS compile targets,
examples, formatting, vendored contracts and the search-protobuf descriptor
comparison against `3c5c7ed` passed. The combined run used an enforced 8 GiB
cgroup limit with swap disabled and no OOM kills. This is local validation;
no fleet deployment or source-aware backup/compaction proof is claimed.

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
The local publisher now consumes accepted versions and replaces their chunks
atomically. Next, exclusive source ownership and collection-wide publication
must make those guarantees part of the authenticated document-write contract,
with stable document/chunk identity through every result path.

### Runtime publication constraints

The foundation branch can prepare a complete accepted projection, resolve an
exact key to sealed rows, and commit new segments with old-row retirements in
one segment manifest. The local publisher joins those components for one shard.
A document spanning shards still needs a collection publication decision;
independent shard acknowledgments cannot establish an atomic visible version.

`NodeServiceImpl::publish_segment_rows_blocking` activates a prepared segment
transaction as one coherent runtime view. It requires both the current shard
statistics epoch/incarnation and the segment catalog epoch. It claims the ingest,
mutation and sealing gates, then constructs the complete BM25, vector-provider,
exact-vector and tombstone state before publishing any manifest. It checks the
node's column/derived declaration and preserves its mapped binding. Queries can
read the old state during preparation; a final shard write guard covers manifest
sync, runtime activation, parent-cache invalidation and the read-version advance.
Old version claims are rejected after activation, and held immutable segment
snapshots retain their previous rows and tombstones.

This trusted local, blocking operation requires a persistent segmented layout
with no mutable/frozen tail, WAL, installed single-image generation or active
compaction. It refuses incompatible states instead of losing pending rows or
bypassing replay. Call it off Tokio workers. It introduces no public RPC,
authentication boundary or document receipt: its returned statistics claim only
identifies the activated runtime. An owner must fence independent catalog writers.

Exact-vector views share sealed FP32 files, including catalogs with document-only
segments, without copying the corpus into a sidecar; see
[exact-vector storage](exact-vector-storage.md). Existing global deletions remain
in the runtime mask. The operation does not certify those earlier deletions as
durable, or certify source identities supplied in segment artifacts.

If consumer preparation fails, the manifest and serving state stay unchanged.
If manifest publication fails after preparation, the node retains the old runtime
and fences ingest until recovery. An uncertain catalog publication also blocks
flush, seal and snapshot copying, including an export whose initial flush happened
before the failure. Reopening validates the manifest actually on disk; an
unacknowledged publication must never be interpreted as a successful document
receipt. Tests in `node::publication::tests` inject failure after manifest rename;
`tests/exact_segment_view.rs` verifies row replacement and read-version activation.

Local recovery joins immutable accepted history to the actual committed index
manifest and active read version, including empty projections and deletions.
Collection-wide publication still needs its own decision. A cursor advanced
after an ingest or flush is not that evidence, and the original persistent
acceptance/retry decision remains distinct from later publication status.

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
