# Deletes, replacements, and compaction

Provider vectors, postings, columns, and exact FP32 rows remain immutable
within a generation. `NodeService.DeleteDocuments` records idempotent global-id
tombstones in one packed live-row overlay. Every lexical, vector, hybrid,
browse, facet, aggregation, fetch, parent, and exact-rerank path consults that
same overlay. `TermStats` subtracts deleted postings and lengths, so distributed
BM25 still uses global statistics for the live corpus.

An update is append-then-retire:

1. Append the new document and aligned vector, receiving its new positional id.
2. Call `CommitReplacements(old_id, new_id)` on the owning product shard.
3. The node validates that both rows exist in every active artifact and the
   replacement is live, then atomically tombstones the old row under the shard
   write lock.

The appended row is queryable before step 2, so callers that require no
temporary overlap must keep it outside their application-visible selection
until the retirement commits. The RPC makes the retirement batch atomic; it
does not stage or publish the prior append.

Retries are idempotent. A committed replacement never mutates old postings or
provider bytes in place. Health reports live rows, deleted rows, and the overlay
revision. `Flush` persists `<index>.live`; snapshot generations use
`live-docs.bin`. Delete and replacement records also enter the bucketed WAL.

Deletion order does not affect earlier tombstones. The bitmap grows when a
newly deleted slot needs another word; deleting a lower slot cannot truncate
higher words. Regression coverage deletes rows across three words in descending
order, replaces another lower row, retries those deletes and verifies live
statistics, lexical results, browse, fetch and exact rescoring before and after
reopening. Older binaries could discard higher tombstones on a lower-row delete;
the fix preserves existing bits but cannot reconstruct bits already lost from
a persisted overlay. Such an overlay requires recovery from retained deletion
history.

Positional ids are scoped to a WAL generation. A compaction or a snapshot
install renumbers a shard's rows, so a client that holds ids across one and
then deletes or replaces by id would name whichever rows carry those ids
now. Every ingest response therefore reports the `wal_generation` its ids
belong to, and `DeleteDocuments` / `CommitReplacements` take an optional
`expected_wal_generation`: a claim that names another generation refuses
with `FAILED_PRECONDITION` ("stale WAL generation") instead of applying. An
absent claim is accepted as before; the guard needs a WAL, since an unlogged
shard has no generation to move.

## Compaction

`NodeService.CompactShard` reclaims a shard's tombstones online, on both
layouts: writes and reads continue for the whole run, and the cutover holds
the shard's write lock for the last few records only. It is unary and
long-running (the build re-analyzes every live document), so clients set a
deadline in minutes; the same entry point is `NodeServiceImpl::compact_shard`
for an in-process control-plane worker.

The shape is the live reshard's, in one process:

1. **Cutoff.** Under the read lock the node fixes `(WAL generation, high
   watermark)` and the row and tombstone counts, then fsyncs the log through
   that clock. Writes keep landing on the live shard. The cut sits at a row
   boundary: on the segment layout a legacy two-RPC append (AddDocuments,
   then AddVectors) can be halfway through at that instant, and a replay
   through such a cut would build a bucket with one document more than
   vectors, which the layout refuses to seal; the preflight waits for the
   row to complete (up to two seconds), then refuses naming the counts.
2. **Build.** `reshard::compact_log` replays the generation through the
   cutoff with its deletes and replacements applied and writes the dense
   all-live image under the work directory (`<index>.compact/` by default;
   `work_dir` overrides it; it must be empty and on the index's filesystem).
   For the segment layout the output is one sealed segment per non-empty WAL
   bucket, so the build holds one bucket's rows at a time; for the
   single-image layout it is one image. The replay also yields the old-to-new
   id map (`ChildImage::row_parent_ids`) and hands every live row to a sink
   that writes the **rewritten WAL generation**: the same records with dense
   new ids, tombstoned rows gone, the binding first.
3. **Shadow.** The image opens as a second shard state whose WAL is that new
   generation: for the single-image layout the image laid out as a
   generation directory; for the segment layout the outputs staged (hashed,
   fsynced, unpublished) under the live catalog root with a fresh heap tail,
   plus the dense FP32 sidecar assembled from the segments' rows.
4. **Tail.** With no lock held, the node fsyncs the live log, reads the
   records after the applied clock (incrementally: only new frames), and
   applies them to the shadow through the SAME functions ingest uses —
   `apply_batch_locked`, `apply_document_locked`, `delete_documents_locked`,
   `commit_replacements_locked`, `apply_binding_locked`. Appends get shadow
   ids and extend the id map; deletes and replacements map through it, and an
   id the map does not know is an error, never a skip; documents are analyzed
   through the node's own analysis backend in one batch per pass. The loop
   repeats until a pass consumes fewer than `tail_bound` records (default 256).
5. **Cutover.** Reserve the commit gate, then wait for any seal in flight
   to finish. Ingest commits, deletes, replacements, binding changes and
   public flushes await shared permits asynchronously. Reads and analysis
   can continue. Prepare the remaining tail without the live shard's write lock.
   Acquire the write lock, fsync the live WAL, and verify its generation and
   high watermark still match the prepared tail. If writes advanced the WAL,
   release the lock and prepare the new records before retrying (16 attempts,
   then refuse with "writes outpace compaction"). A generation change aborts.
   Once caught up, fsync the shadow, write the commit marker, move the rewritten
   generation into `<index>.wal/`, swap the files (the single-image layout takes
   the same
   rename dance a snapshot install takes, `adopt_generation`; the segment
   layout swaps `segments.json`), swap the state, bump the stats epoch, and
   release both locks and the reservation. Analysis never runs under the live
   shard's write lock. Queries that
   snapshotted the old state finish on it.
6. **Closing flush.** The call then runs `Flush`, which writes the new
   generation's images, removes the marker, and retires the replaced files:
   `<index>.snap-old` and the legacy single-image files, or the replaced
   segment directories. Any `Flush` completes a pending cutover the same way.
   On the segment layout a flush that meets a legacy two-RPC append mid-row
   (documents one ahead of vectors) refuses to seal by the layout's rule;
   the closing flush waits that out for up to two seconds, then returns the
   refusal and names the pending cutover.

The response reports rows before and after, tombstones reclaimed, tail
records applied (and how many under the lock), the write-lock hold and the
closing flush in milliseconds, the new WAL generation, the cutoff clock, the
layout, and the stats epoch. `dry_run` runs the preflight only and reports
what a compaction would work from, writing nothing.

Refused by name: an in-memory shard; a shard without a WAL; a generation
with legacy unclocked records; a generation that began with preexisting
state its log does not hold (a snapshot install's; compaction would drop the
image); a compaction already running on the shard; a bulk BM25 build in
progress; no analysis backend; a non-empty work directory or a leftover
staged segment; a work directory on another filesystem; a compacted store
whose field table or analyzer fingerprints differ from the shard's; a
snapshot installed during the run (the WAL rotates under the tail, and the
compaction aborts with the live shard untouched); a pending cutover whose
closing flush has not run; a segment-layout tail still mid-row after the
wait.

### Why the log is rewritten

A snapshot install rotates the WAL to a generation whose manifest records
the image as `preexisting_*`: partial history, which `reshard` refuses. Had
compaction rotated the same way, a shard could be compacted exactly once,
ever, and never split, merged, or tailed by a replica afterwards. The
rewritten generation is full history — `reshard --split=1` over it rebuilds
the compacted shard, pinned by test — so every later compaction, reshard,
and catch-up keeps working. The cost is one pass over the live rows written
as records, and the superseded generation stays on disk as history until the
operator archives it (`docs/resharding.md`, retention).

### Crash safety

The cutover writes `<index>.compact-commit` before it renames anything and
removes it only after the closing flush has put the new generation's images
on disk; the old generation is retired after that. A marker found at open
(`recover_generation`) means the closing flush never ran, and the node rolls
back: it restores `<index>.snap-old` or removes the compacted generation
directory, restores `segments.json` from the copy the cutover kept and
removes the staged segments, removes the rewritten WAL generation, and
removes the marker — every rename undone where it happened and skipped
where it did not. The generation it returns to is the one the shard was on,
which holds everything that was ever flushed: the product's durability point
is `Flush`, and the closing flush is the first flush of the new generation.
Rows acked between the cutover and that flush are lost by a crash in that
window exactly as un-flushed ingest always is. The work directory is left
for inspection after a rollback and refused by name until removed.

### Costs

- The build reads the whole log and re-analyzes every live document (one
  batch session per spec on the node's analysis backend); the tail's first
  pass reads the log's frames again to find the cutoff, later passes read
  only new frames.
- Peak disk is the old generation plus the new: the work directory holds
  the dense image (or staged segments) and the rewritten WAL generation
  until cutover, and the old image, catalog entries, and WAL generation stay
  until the closing flush retires them (the WAL generation stays until
  archived).
- The write-lock window includes the live WAL fence, the shadow WAL fsync,
  the marker, three or four renames, and the state swap. No records or analysis run under that
  lock, so `locked_tail_records` is zero. Earlier measurements of 13–39 ms
  with 18–90 records applied under the lock describe the retired algorithm;
  it could stall when analysis and a blocked ingest shared a runtime.
  The concurrent-write fixture still refuses lock holds above 500 ms.
- The closing flush costs what any flush on the layout costs: a segmented
  flush seals the tail rows into one small segment and rewrites the
  whole-shard FP32 sidecar; a single-image flush rewrites the image. It is
  reported apart from the lock hold.
- Tombstones written after the cutoff are carried into the new generation
  as tombstones; the next compaction takes them. A quiescent compaction ends
  with `deleted_docs = 0`.
- Global ids change across the generation (positional). A cursor minted
  before the cutover refuses by name on resumption, and the
  `expected_wal_generation` claim above guards id-addressed mutations. An
  `AddDocuments` and `AddVectors` pair that straddles the cutover is
  answered with an old-generation id and a new-generation one; the row
  itself sits at the vector's id in the new generation.

`tests/compaction.rs` pins all of it on both layouts: a concurrent writer
whose calls complete in a fraction of the run, the lock bound, tail records
applied, the work directory and the old generation gone, a second quiescent
compaction ending with no tombstones, every read path (lexical, dense,
hybrid, facets, sorted browse, fetch, parents) bitwise equal by stable text
to a shard built fresh from the final set, a reopen from disk, the rewritten
generation replaying to the same shard, the cursor and stale-generation
refusals, distributed equal to monolithic beside an untouched shard, the
refusal table, the snapshot-during-compaction abort, the dry run, and the
rollback of an interrupted cutover.

The unit regression `cutover_analysis_allows_reads_and_includes_writes_during_preparation`
injects a committed write during final analysis, requires the live state to
remain readable during every analysis call, and verifies the cutover includes
the late write after retrying its watermark fence.
The persistent-write regression forces every attempt to receive another write
and verifies refusal leaves the live generation and every committed row intact.
Those writes deliberately bypass the commit gate to exercise the WAL fence.
A single-thread runtime test verifies a reserved cutover yields a public writer,
allows health reads, and resumes the writer after the reservation is released.
