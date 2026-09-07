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

## Completing the bundle

The source capture is implemented; the following bundle and restore work is
still required.

1. Capture the source transaction first. For every journal index, obtain the
   exact committed manifest under that catalog's publication fence. The index
   key, owner history/collection, typed manifest hash and physical generation
   must agree with the captured state. Missing, extra or mismatched indexes
   refuse the entire backup. Accepted backlog remains source backlog.
2. Pin each referenced immutable artifact while its catalog fence is held, then
   release the fence before bulk I/O. An ordinary query's mapped reader is not
   sufficient protection for a later reopen by pathname: compaction can retire
   that pathname. The pinning mechanism needs explicit file-descriptor and
   metadata budgets. A changed manifest refuses capture rather than mixing
   generations.
3. Copy artifact bytes from the held files, verify lengths and checksums, retain
   complete generation declarations even for empty indexes, and write the
   captured source database. Record only validated relative bundle paths.
   Create the final protobuf bundle manifest last, after syncing its contents
   and directories. Partial outputs are private and are not restorable bundles.
4. Restore into a new private destination. Verify the bundle and artifact
   inventory, open the copied source database, validate every journal anchor
   against the copied manifests, and validate each segment with the configured
   provider before producing an activatable result. Do not re-evaluate CEL,
   analyze text, regenerate embeddings, or replay writes to approximate the
   captured state.
5. Activation belongs to the collection authority. A restore must not create
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
