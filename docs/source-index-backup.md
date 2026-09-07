# Coherent source and index backup

A recoverable backup must join accepted source history, immutable retry
receipts, the publication/maintenance journal, and every index manifest and
artifact referenced by that journal. An index-only snapshot cannot establish
this relationship. Existing snapshot routes continue to refuse source-owned
catalogs.

## Implemented source capture

`DocumentCatalog::capture_checkpoint(max_metadata_bytes)` pins one redb read
transaction. Its header includes all accepted versions, including versions that
have not reached any index. Its index states record each index's committed
source sequence, physical manifest hash and maintenance cursor. Capture refuses
pending source or maintenance decisions; the owner must recover them first.
Unknown or missing tables also refuse instead of silently disappearing from the
copy. This operation does not mutate the live catalog or repair its journal.

`CatalogCheckpoint::write_to(path, limits)` copies the tables from that held
transaction into a new database. It preserves raw keys and values, including
original descriptor and source bytes, unknown protobuf fields, legacy retry
records, deletions and sources that produce zero index rows. It does not replay
acceptance or generate replacement history IDs. Even empty declared tables are
retained. The returned storage protobuf `DocumentCatalogCheckpoint` records the
captured header and all index anchors, copied record count, output size and
SHA-256. It is metadata for the source half of a backup, not a serving receipt
or authorization to replace a live authority.

The output file is created exclusively, with mode 0600 on Unix. Existing
files are untouched. Database commits use immediate durability; success follows
database close, file sync, checksum and parent-directory sync. Failed copies
remove only their own file. The enclosing backup owner must supply a private
staging directory and publish its completion manifest only after the source and
all referenced index artifacts are complete. A raw checkpoint file left by a
process crash must not be mistaken for a completed backup.

The copy uses a separate database with an 8 MiB page cache. Each output
transaction accounts key/value bytes plus 64 bytes per record and is limited by
`batch_bytes` (1–64 MiB) and 65,536 records. An oversized single record refuses
without truncation. `max_file_bytes` is checked after every commit and after
close; it is a completed-file bound, not a filesystem quota. The checksum uses
a 64 KiB buffer. Metadata is limited separately to 1–64 MiB. These limits do
not include every allocator or database bookkeeping cost and are not process
memory quotas. Operational builds and copies still need the 8 GiB cgroup limit
with swap disabled.

Acceptance can continue while a checkpoint is held. redb retains pages needed
by that read transaction, so long-lived captures can increase the source
file's disk footprint under ongoing writes. The bundle owner must drop the
capture promptly on cancellation or failure. Copying an old read transaction
must never fall back to newer records from the live source.

## Implemented coherent bundle capture

`DocumentCatalog::capture_backup(indexes, limits)` captures the source read
transaction first, then pins every journaled index under its live catalog's
publication fence. The caller supplies the actual live catalogs, which share
those fences with publication and compaction. Missing, duplicate, extra,
foreign-owner or journal-mismatched indexes refuse the capture. A concurrent
publication may cause a refusal; capture never substitutes a newer source view.
`NodeServiceImpl::backup_documents_blocking` supplies this operation for a
source authority with exactly one journaled local index.

The capture holds open files for immutable artifacts and retains the captured
manifest bytes. Compaction may retire the original paths after capture; the
copy reads the held files instead of reopening those paths. Catalog fences are
released before bulk copying. Metadata, file count, completed bundle bytes and
source transaction bytes have explicit protobuf budgets. OS descriptor limits
may refuse a capture below its requested file count.

`CapturedBackup::write_to(destination)` creates a private directory exclusively
(mode 0700 on Unix), outside every captured live catalog. Existing destinations
are untouched. It writes the captured source database, releases the source read
transaction, then streams artifacts with a 64 KiB buffer and checks every length
and SHA-256. Complete generation declarations survive even for an empty index.
Files and directories are synced before the final `source-backup.pb` manifest
is written and synced. That additive storage protobuf binds source metadata,
index keys and epochs, relative artifact paths, sizes and checksums. Its own
SHA-256 covers the canonical message with its digest field cleared.

Before copying source records, bundle creation audits the pinned accepted
history. Versions and ordered changes must form a complete sequence with valid
per-document predecessors and latest heads. Every source and descriptor blob
must match its content address, including blobs used only by older versions.
Each accepted write needs exactly one immutable retry receipt with the correct
history, version, sequence and acceptance flags. Legacy receipts may omit the
history ID only through the persisted migration boundary; their bytes are
preserved. Original descriptor and payload bytes are hashed without parsing or
rewriting the application message.

Receipt uniqueness uses a temporary redb index with an 8 MiB cache and at most
1024 entries per transaction. Individual decoded records must fit
`source_batch_bytes`; `max_bytes` also caps the scratch file independently,
checked after every transaction. The audit deletes scratch before source copy
starts. It performs read-only checks against the captured source transaction,
so later acceptance cannot alter the audited history. This adds a full source
history scan to backup creation. The same pinned read also validates the full
publication and maintenance journal. Per index, accepted source decisions and
physical maintenance steps must form one uninterrupted epoch/hash chain ending
at the captured tip. Each step binds the correct accepted version, source
decision and previous maintenance cursor. Exact table counts reject extra
records outside those chains. Reads hold one decision at a time under the
record budget; historical artifact files are not required. These checks also
apply to histories containing empty publications and unpublished source backlog.
Incoming bundle staging is described below; restore activation remains separate
work.

A failed write removes only its own output directory. A process crash can leave
a partial directory. Presence of the completion file alone is insufficient:
restore must verify its digest and its entire inventory. These local owner
operations grant no remote export, permissions change or serving activation.

## Implemented incoming bundle staging

`DocumentCatalog::stage_backup_restore(bundle, destination, request)` verifies
one incoming local bundle against the digest, history ID and exact collection
supplied independently in `SourceRestoreRequest`; the empty collection is
valid. The trusted owner chooses both the input root and private destination
parent. The request requires explicit metadata, file, total-byte and
source-record budgets. Before protobuf decoding allocates repeated entries, the
verifier bounds the outer artifact and index envelopes, including nested
checkpoint index counts. It then requires the canonical completion manifest,
its expected digest, and one sorted, unique, checksummed inventory containing
exactly the source and physical index artifacts referenced by the journal.
Unlisted files present on disk are ignored and never copied; listed artifacts
that are missing, changed, duplicated, noncanonical or unreferenced refuse.

On Unix, every input component is opened relative to a held directory
descriptor with symlink following disabled and must be a regular file below
real directories. Linux, Android and iOS use this path; other platforms refuse
explicitly until they provide equivalent descriptor-relative reads. This makes
`libc` an embedded as well as network dependency. The owner-selected destination
must be outside the incoming bundle and must not exist.
Staging creates it with mode 0700 and copies only named files into new mode-0600
files, checking each length and SHA-256 while streaming. The original bundle is
left byte-for-byte unchanged.

The copied redb source is opened read-only under an explicit shared source-file
lock. Its checkpoint metadata and record count must match the completion
manifest, and the full accepted-source, retry, publication and maintenance
journal audit runs against that private copy. Each physical segment is reopened;
BM25 and exact-vector payload integrity are checked in addition to the manifest
and artifact hashes. After files and directories are durable, the original
canonical `source-backup.pb` is written last into staging. This completion
marker identifies a verified bundle, not an activated authority. Staging does
not re-evaluate CEL, reanalyze text, regenerate embeddings or replay accepted
writes to reconstruct the captured state.

The returned `VerifiedSourceRestore` retains the private directory, read-only
database handle and shared lock. Dropping it releases those handles and attempts
to remove only its owned staging directory; filesystem cleanup is best-effort.
It creates no writer or serving state, restores no grants and permits no
off-phone or other remote export.

## Terminal retirement of the source writer

`DocumentCatalog::seal_history(SourceSealRequest)` is the trusted local owner's
durable retirement operation. The request names the source history, its exact
expected accepted sequence and an operation ID of 1..1024 bytes. It requires
a durable catalog. It does not accept a lease-expiry observation or an index
epoch as a substitute for the accepted watermark.

The seal takes the same redb writer used by acceptance and by all projection
and maintenance journal mutations. Before changing anything it captures the
last committed source checkpoint, with a 64 MiB metadata bound. Pending source
or maintenance intents refuse sealing; the owner must resolve them against
their actual durable segment manifests first. A new preparation either commits
first, leaving an intent that blocks sealing, or waits and observes the seal.
A concurrent accepted write either advances the expected sequence and makes
sealing refuse, or observes the committed seal and refuses itself.

This operation never takes node or segment locks while holding the database
writer. Publishers acquire those locks before the source transaction, so the
reverse order would deadlock. The pending journal intent covers the interval
between preparation and the final artifact decision, including filesystem work
outside a database transaction. A private staged candidate has no reservation:
if sealing wins first, its later publication must refuse at preparation.

The header and `SourceHistorySeal` are committed together with immediate
durability. Active catalogs remain format 3. A sealed catalog becomes format 4,
which older writers refuse; a format-3 header carrying a seal or a format-4
header missing its seal is corrupt. The seal binds the unchanged history ID,
final accepted sequence and operation ID. Retrying the exact request returns
the stored seal after restart. Another operation ID refuses. No unseal API or
automatic expiry exists.

After sealing, acceptance (including retries), publication preparation and
recovery, maintenance enablement, preparation and recovery all refuse at the
database write boundary. Historical reads and backup capture remain available.
Source bytes, versions, idempotency records and index journal state are retained;
sealing does not claim that an accepted backlog was indexed. Backup and incoming
staging preserve the seal, and reopening that copied source keeps it sealed.

This is evidence about the retired **local store**, not authorization for a
replacement writer and not a fence on an earlier independent copy. Raw file
copies, a stale backup, a shared history ID or a matching completion digest do
not prove exclusive ownership. The collection authority must bind its current
workspace/collection grants, action, owner, incarnation and generation to any
future activation. Phones may retire and recover locally; this operation
introduces no transfer or off-device replica path.

## Restore work remaining

Activation belongs to the collection authority. A restore must not create two
active writers for one source history or turn a verified completion marker into
a current serving receipt. Activation and rollback must enforce current
permissions, write fencing, ownership, generation checks and a single active
authority, including an explicit rule for replacing newer accepted history
with an older checkpoint. Mobile activation must retain local-device residency.
The staging verifier and local terminal seal do not supply this distributed
activation contract. The seal implements the old-store retirement primitive;
committed prepare/activate decisions, replacement catch-up proof, rollback
policy and recovery of that authority transaction remain to be implemented.

The bundle must be usable without the original source paths or a running
analysis service. It must retain accepted-but-unpublished writes and expose
that backlog separately from searchable state. Mobile and remote wrappers must
use the same owner operation; they must not assemble independent source and
index snapshots in application code.

## Validation

At `2546712`, the complete local source-authority run passed 658 library tests,
848 integration tests across 144 targets, 13 embedded tests and 2 IVF-provider
tests: 1,521 passed, 0 failed. The existing live OpenNLP test remains ignored.
All 5 Android/iOS targets, tests/examples compilation, formatting,
vendored-proto identity and diff checks passed. The descriptor comparison with
`44276d8` preserves existing field numbers/types, enum values and RPCs;
retirement adds a header field and two messages. All 629 tracked source hashes
matched before and after validation. The scope had an 8 GiB hard limit, peaked
at 8 GiB, used no swap and recorded 0 OOM, OOM-kill or OOM-group-kill events.
This is local validation; hosted CI, fleet rollout and merge to `main` were not
performed. Explicit server routing and current-authority activation remain
unfinished.

The test-only follow-up `2214e48` adds an abrupt-exit regression. The complete
20-test `document_catalog` integration target and formatting passed with runtime
source hashes unchanged. Together this covers 1,522
passing tests, but the full gate was not rerun at `2214e48`.

### Historical validation at `7538f96`

At `7538f96`, the incoming-staging checkpoint passed 651 library tests, 848
integration tests across 144 targets, 13 embedded tests and 2 IVF-provider
tests: 1,514 passed, 0 failed. The existing live OpenNLP test remains ignored.
All 5 Android/iOS targets, tests/examples compilation, formatting,
vendored-proto identity and diff checks passed. Existing search/storage wire
descriptors from `14ef9cf` are unchanged, and all 627 tracked source hashes
matched before and after validation. The scope had an 8 GiB hard limit, peaked
at 8 GiB, used no swap and recorded 0 OOM, OOM-kill or OOM-group-kill events.
This is local validation; hosted CI, fleet rollout and merge to `main` were not
performed. Incoming private-staging verification is implemented as described
above; collection-authority activation remains unfinished.

### Historical validation at `9cee9a3`

At `9cee9a3`, the reconciled branch passed 640 library tests, 848 integration
tests across 144 targets, 13 embedded tests and 2 IVF-provider tests: 1,503
passed, 0 failed. The existing live OpenNLP test remains ignored. All 5
Android/iOS targets, tests/examples compilation, formatting, vendored-proto
identity and diff checks passed. Existing search/storage descriptors from
`daec858` are unchanged, and all 625 tracked source hashes matched before and
after validation. The 8 GiB scope used no swap and recorded 0 OOM, OOM-kill or
OOM-group-kill events. This is local validation; hosted CI, fleet rollout and
merge to `main` were not performed.

This checkpoint reconciles the local `main` correction in `1a74f97`, the
Boolean fixes in `92bfe79`, and the complete journal audit in `a749925`.
`9cee9a3` additionally rejects `math.abs(i64::MIN)` when evaluating a declared
column, while missing input, per-request materialization absence and untaken
ternary branches retain their existing behavior.

### Historical validation at `92bfe79`

At `92bfe79`, the source-history audit and Boolean reconciliation passed
636 library tests, 848 integration tests across 144 targets, 13 embedded
tests and 2 IVF-provider tests: 1,499 passed, 0 failed. The live OpenNLP
test remains ignored. Android/iOS compilation passed on all 5 targets;
tests/examples, formatting, vendored protos and whitespace checks passed.
Search/storage descriptors from `d3c4534` are unchanged. Input hashes matched
before and after validation. The 8 GiB scope used no swap and had 0 OOM or
OOM-kill events. These results apply to that historical local checkpoint.

The source audit regressions cover older history gaps, blob corruption, stale
heads, invalid retry flags, migration boundaries, disk and record budgets,
exclusive scratch ownership and duplicate receipts across 1024-entry batches.
The Boolean fix in `92bfe79` checks shared filter references under direct
SHOULD/MUST_NOT and complete candidate provenance, with document visibility.

### Historical bundle capture validation

The bundle implementation has eight focused passing regressions: capture across
compaction and path retirement with unpublished source backlog, multiple owned
indexes, empty declarations and source tombstones, source-only histories,
index/owner and budget refusals, changed artifact bytes, exclusive output
ownership, and refusal of destinations nested in live catalogs.

Local validation at `afcc03c`, including the merge of `ba71981`, passed:
628 library tests, 842 integration tests across 142 targets, 13 embedded tests
and 2 isolated IVF-provider tests. Total: 1,485 passed, 0 failed. The existing
live OpenNLP test remains ignored. All 5 Android/iOS compile targets,
tests/examples compilation, formatting, vendored-proto identity and whitespace
checks passed. Existing search/storage descriptors from `bdb54c4` are preserved;
the backup messages are additive. Source hashes matched before and after the
run. The scope used `MemoryMax=8G` and `MemorySwapMax=0`, peaked at 8 GiB,
with 0 OOM and OOM-kill events.

### Historical source-only checkpoint validation

The checkpoint implementation passed 615 library tests, 837 integration tests
across 142 targets, 13 embedded tests and two isolated IVF-provider tests:
1,467 passed, zero failed. The existing live OpenNLP conformance test remains
ignored. All five Android/iOS compile targets, tests/examples compilation,
formatting, vendored-proto identity and whitespace checks passed. All existing
search and storage protobuf declarations from `a0b3949` are preserved; the two
checkpoint messages are additive. Mobile builds retain the three existing relay
dead-code warnings.

The eight focused regressions cover exact source/retry bytes through a held
read snapshot and later acceptance, empty tables, bounded-copy failures and
exclusive destination ownership, unknown tables, multiple indexes in one
source history, pending source and maintenance decisions, and a maintenance
anchor with accepted-but-unpublished source backlog. Removing an accepted
version while retaining its otherwise valid journal tip reproduced a failing
capture test; capture now refuses that detached anchor against the pinned
version and ordered-change tables.

The final validation source stayed unchanged throughout the run. The systemd
scope used `MemoryMax=8G` and `MemorySwapMax=0`, peaked at 8 GiB, and recorded
zero OOM or OOM-kill events. This is local validation; hosted CI, a fleet rollout
and corpus-scale backup measurements were not performed.

## Restore boundaries still to implement

An unpublished index at committed sequence zero needs its recorded initial
manifest retained without inventing a later source decision. Restore must keep
that index fenced until source publication establishes its owned serving state.
The enclosing authority must use current permissions; restoring source/index
artifacts must never reinstate an older set of grants. Device-shard maintenance
and backup stay inside the owning phone's storage policy. A remote request must
not turn the local checkpoint primitive into off-device source or index export.
