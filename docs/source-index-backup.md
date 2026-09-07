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
history scan to backup creation. It supplies the source-table check for future
restore work; historical publication/maintenance chain validation and restore
activation are still separate work.

A failed write removes only its own output directory. A process crash can leave
a partial directory. Presence of the completion file alone is insufficient:
restore must verify its digest and its entire inventory. These local owner
operations grant no remote export, permissions change or serving activation.

## Restore work remaining

1. Restore into a new private destination. Verify the bundle and artifact
   inventory, open the copied source database, validate every journal anchor
   against the copied manifests, and validate each segment with the configured
   provider before producing an activatable result. Do not re-evaluate CEL,
   analyze text, regenerate embeddings, or replay writes to approximate the
   captured state.
2. Activation belongs to the collection authority. A restore must not create
   two active writers for one source history or turn a recovered artifact into
   a current serving receipt. Existing write fencing, ownership and generation
   checks remain mandatory. Define recovery from an older checkpoint explicitly
   before allowing it to replace a newer accepted history. A local restore
   verifier does not supply this distributed activation contract.

The bundle must be usable without the original source paths or a running
analysis service. It must retain accepted-but-unpublished writes and expose
that backlog separately from searchable state. Mobile and remote wrappers must
use the same owner operation; they must not assemble independent source and
index snapshots in application code.

## Validation

At `92bfe79`, the source-history audit and Boolean reconciliation passed
636 library tests, 848 integration tests across 144 targets, 13 embedded
tests and 2 IVF-provider tests: 1,499 passed, 0 failed. The live OpenNLP
test remains ignored. Android/iOS compilation passed on all 5 targets;
tests/examples, formatting, vendored protos and whitespace checks passed.
Search/storage descriptors from `d3c4534` are unchanged. Input hashes matched
before and after validation. The 8 GiB scope used no swap and had 0 OOM or
OOM-kill events. These results apply to this local checkpoint, ahead of the
pending derived-evaluation fixes.

The source audit regressions cover older history gaps, blob corruption, stale
heads, invalid retry flags, migration boundaries, disk and record budgets,
exclusive scratch ownership and duplicate receipts across 1024-entry batches.
The Boolean fix in `92bfe79` checks shared filter references under direct
SHOULD/MUST_NOT and complete candidate provenance, with document visibility.

### Bundle capture validation

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

This checkpoint is local and unpushed pending the derived-evaluation fixes.
Those future edits need their own validation.

### Source-only checkpoint validation

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
