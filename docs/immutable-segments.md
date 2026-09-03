# Immutable aligned segments

Status (2026-09-02): the segment catalog is the **default layout of a new
persisted shard**. `NodeService` serves a `SegmentedShard`
(`src/segmented.rs`): the catalog's sealed segments plus one heap tail, read
as one shard, with the vector side as a `SegmentedProvider`
(`src/segmented_vectors.rs`) over the segments' images plus a tail image.
The single-image layout remains available (`--layout=single-image`) and
every existing single-image shard keeps it: a shard has the layout its files
have, and nothing converts on open — a path with both a single image and a
catalog refuses by name.

## The node's segmented shard

Local document ids are one positional space: segment `i` covers
`[base_i, base_i + rows_i)` in catalog order and the tail starts where the
last segment ends. Every read routes a document id to its part; every
aggregate (document count, total length, df) sums the parts, so global BM25
statistics are exact across segments. Column dictionaries are the one place
a union needs state of its own: each segment's facet, map-key, and map-value
ordinals are local to its file, so the shard keeps one global dictionary per
column (byte-sorted over the sealed parts, with the tail's new values after
it) and a remap from each part's ordinals. Callers see global ordinals
everywhere, so filters, facets, projections, highlights, and prefix
expansion work unchanged over a segmented shard.

Block-max pruning survives sealing: a term's impacts across the sealed
segments chain into one `ImpactCursor` with cumulative block and level-1
numbering and per-part document bases. A term the tail holds has no impacts
until the next seal; the scorer takes its exact heap path for that query and
regains pruning on the next flush.

**Sealing.** A seal turns the tail into one new segment in three steps.
Under the shard's write lock the tail freezes into a read-only part and a
fresh tail starts after it (a cheap swap). With no lock held, the frozen
part's documents, live-bitmap slice, and — when it has vectors — its vector
image and FP32 rows are written, hashed, fsynced, and appended to the catalog
with one manifest swap; the FP32 rows are copied out of the exact-vector
sidecar under a read lock first, a sequential copy. Under the write lock
again the shard adopts the published snapshot. Queries serve the frozen part
throughout (it answers as the tail it was: exact heap path, global
dictionaries, positions, sentences) and ingest continues into the new tail.
Seals on one shard run one at a time; a seal that failed after freezing is
finished by the next attempt. The WAL is untouched by a seal: it changes the
on-disk layout, not the log's history, so reshard, replication catch-up, and
compaction read the same full-history generation they always did.

A flush seals whatever the tail has. `--seal-tail-docs` (default 500,000)
bounds a segment: the ingest loop checks the tail between documents and
seals a full tail before applying the next one, so one long AddDocuments
stream produces segments of at most that many rows without waiting for the
stream's end (a vectors-only stream checks between batches). 0 seals on
flush only. A tail whose document and vector rows disagree refuses to seal
by name (a segment's artifacts cover the same rows); mapped ingest keeps
them aligned. A documents-only shard seals documents-only segments (no
vector artifact); the catalog and its collectors accept them.

A sealed segment's vector image is served from its file through a memory
map by default (`docs/mmap-vectors.md`; `--vector-mmap=false` loads it into
memory instead); the tail image is owned, because it takes writes. A vector
index the shard created before its first document — a calibration, or
vectors ingested first — becomes the segmented provider's tail when the
catalog appears, so its rows seal with the documents.

The node's whole-shard live bitmap and exact-vector sidecar remain the
serving copies; each sealed segment carries its own slice of both for
compaction, split, and merge. Deletes after sealing go to the node bitmap
and the WAL, and compaction replays the WAL, so a segment's own bitmap is its
state at seal time by design.

`tests/segment_layout.rs` pins: a fresh shard writes a catalog and no single
image; two sealed segments plus a tail answer every probe (df, live bitmap
after a delete, facets, CEL ranges over the union dictionary, sentence
highlights, prefix expansion, phrases) bitwise as one image of the same rows,
before and after reopening from disk; an old single-image fixture serves
under the default and is never converted; both layouts on one path refuse;
the tail auto-seals at the bound; one stream longer than twice the bound
seals as it goes and every segment stays within it; and the union read
surface (df, postings, positions, sentences, the chained cursor,
dictionaries) equals one store, through a frozen part included.

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
