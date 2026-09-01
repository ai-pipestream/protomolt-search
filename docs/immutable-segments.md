# Immutable aligned segments

Status: the segment catalog, exact cross-segment vector/BM25 collectors,
atomic publication, and WAL-backed compaction API are implemented in
`src/segments.rs`. The historical single-image `NodeService` layout remains
the default. A node worker opts into the catalog and reports the resulting
generation to `ClusterControl`; startup does not silently convert an index.

## Unit of publication

One segment contains four row-aligned artifacts:

- a provider-owned vector image;
- product-owned exact FP32 rows;
- a BM25 image containing postings, columns, stored text, and lineage;
- a live-document bitmap.

`segment.json` records the generation, positional range, row counts, backend,
scoring fingerprint, analysis fingerprints, and full SHA-256 of every
artifact. `segments.json` names one immutable set of those segments. Opening a
set verifies every hash and row count before it can serve.

All segments in a set must use the same exhaustive, higher-is-better vector
contract and the same scoring and analysis fingerprints. Positional ranges may
not overlap. Stable product identity remains in document lineage rather than
in those generation-local positions.

## Query exactness

A query snapshots one `Arc<OpenedSegmentSet>`, so an in-flight query cannot
cross an append or compaction publication.

BM25 first sums live document count, live total length, and live per-term df
across the complete snapshot. Every segment then scores with those same global
statistics and a tombstone filter. Vector search uses a per-segment live-row
allowlist. Both paths merge into one global heap ordered by score descending,
then document id ascending.

Deleted documents are removed from both membership and BM25 statistics. A
segment set never merges scores derived from local df or local average length.

## Append and compaction

`SegmentCatalog::append` copies artifacts to a temporary directory, hashes and
opens them, fsyncs them, renames the segment, writes the new set manifest
atomically, and only then swaps the live snapshot.

`replace_many_for_compaction` accepts one or more dense outputs. Their shared
generation must be newer than every input and their combined row count must
equal the inputs' live row count. This permits a large compaction to remain a
set of bounded physical segments rather than rebuilding one large heap image.

`compact_wal_generations` is the blocking worker entry point. It:

1. replays one WAL hash bucket at a time;
2. applies deletes and replacements from the complete WAL;
3. seals each partition as an all-live provider/BM25/FP32 segment;
4. publishes all outputs with one manifest swap.

The node logs one vector or document row per routed WAL record. Replay rejects
a foreign record whose rows straddle buckets, so additions, replacements, and
deletions for an id remain in the same bounded replay unit. WAL bucket count is
the memory-control knob: more buckets create smaller physical segments without
changing query semantics.

Old segment directories are not deleted during publication. Reclaim them only
after old snapshots have drained and the accepted generation has passed its
soak and backup policy.

## Control-plane execution

The authority schedules `COMPACT_SHARD`, `SPLIT_SHARD`, and `MERGE_SHARDS`
actions. A node worker reads its pending actions from `GetClusterPlan`, builds
and verifies the output, then calls `CompletePlacementAction`. The authority
checks generation, fingerprints, live-row conservation, and exact hash-range
tiling before it replaces the old replica records or publishes a topology.

The worker may run in the server process with `spawn_blocking` or in the same
private embedded/mobile process. Neither form requires another network hop for
the search data path.
