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

Compaction is a one-child reshard (`--split=1`). Replay drops tombstoned rows
and writes a dense all-live generation, including rebuilt provider, BM25, and
exact-vector artifacts. Install it atomically with `InstallSnapshot`. Until WAL
tail catch-up exists, stop writes before fixing the replay point and keep them
stopped through install; reads may continue while the replacement image builds.
