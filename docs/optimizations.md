# Engine optimizations

How turbovec-search keeps memory bounded and latency low, organized by
pipeline stage. Numbers cited were measured on the CourtListener corpus:
9,740,254 opinions chunked into 86,633,399 passages (dim-256 vectors,
8 shards of 10,829,174 chunks each) on one 121 GB build machine.

## Indexing

**One analysis pass per opinion.** The chunker sends each opinion to the
analysis sidecar once and receives sentence spans, token spans, and
per-sentence embeddings together. Chunk boundaries are planned client-side
from the spans. Because the embedding model is linear (a static table
lookup with mean pooling), a chunk's vector is the token-weighted mean of
its sentences' vectors — computed exactly, with no second embedding call,
when sentences are packed into chunks. Measured throughput: ~3,500
opinions/s (~31,000 chunks/s) through one JVM sidecar at 24 concurrent
streams.

**Resumable, fail-fast chunking.** The chunker writes chunks and
embeddings through an ordered writer that only ever appends a contiguous
prefix of input lines. On any analysis failure it stops at the last
contiguous line instead of buffering past the gap, so a rerun resumes at
exactly the right line and the output can never silently skip a document.
A final reconciliation refuses to exit cleanly unless every fed opinion is
accounted for.

**Two files, one order, no join state.** Chunks (NDJSON) and embeddings
(fixed-stride binary records) are written in the same order. The ingest
driver therefore joins them by walking both files in lock step and
asserting key equality at each position — and because embedding records
have fixed stride, any shard's block is a direct seek. The driver holds
tens of MB regardless of corpus size; an earlier version that materialized
one shard's block held 27 GB.

**Spill-and-merge BM25 construction.** Building postings in a heap map
costs roughly 5x the raw data in allocator and pointer overhead: a
10.8M-chunk shard peaked at 105 GB RSS. The spill builder keeps a 256 MB
buffer of `(term, doc, tf, offsets)` entries; when full, the buffer is
sorted by `(term, doc)` and written as a run, and document texts stream to
a spill file at add time already in their final section encoding. Flush
k-way merges the runs in one sequential pass. Doc ids only grow, so runs
never overlap within a term and the merge is concatenation in run order.
Output is byte-identical to the in-memory builder (enforced by test).
Measured build memory: under 1 GB per shard.

**Memory-mapped serving.** A flushed shard reopens as an mmap over the
postings/doc-store file and the vector index; the OS page cache is the
buffer pool. The transition is in-process (builder dropped, reader
opened) — no restart. A resident 10.8M-document shard holds only per-doc
tables in heap; node RSS at startup is a few MB against a 48.6 GB
postings file and 1.4 GB vector index.

**Seeded, locked calibration.** TQ+ quantization parameters are fitted
once from a stride sample of the corpus (~289k vectors), broadcast to all
shards before ingest, and locked for the index lifetime. Every shard
quantizes identically, so scores are comparable across shards and across
experimental cluster builds.

**Write-ahead log, born partitioned.** Every ingested document and vector
batch is logged before acknowledgment, hashed into 64 buckets by id. A
shard's history can be replayed offline to split one shard into N, merge
several into one, or redistribute N shards into M — without re-embedding
and without a live cluster. Split children reconstruct the parent's top-k
bitwise (verified by `reshard_verify`).

**One writer per shard.** Nodes refuse a second concurrent ingest stream
(`FAILED_PRECONDITION`) rather than interleave positional ids from two
writers. Parallel ingest is expressed by giving each driver a disjoint
shard range over the same node list; block offsets are computed globally,
so any partition of the ranges reproduces the same cluster.

## Coordination

**Share-nothing coordinators.** A query is owned end to end by one
coordinator; the only cross-request state is static topology (the shard
map). Calibration lives on shards, floors are per-query. Coordinators
scale horizontally behind a load balancer with no shared memory, and a
coordinator tree (coordinators implementing the node search interface)
composes for very wide clusters because top-k merge is associative.

**Collaborative floor sharing.** During a query, each node streams its
running k-th-best score to the coordinator; the coordinator aggregates the
maximum and pushes it back down; nodes prune the rest of their scan
against it. The pruning is lossless: a score below the global k-th best
cannot appear in the final top-k. Sharing is toggleable per node
(`share_floors`) so A/B comparisons run on identical data.

**Seeded floors (`initial_threshold`).** The fork's search-kernel patch
lets a caller seed the top-k floor before a scan begins, so a shard
starting late — or a coordinator with a prior from sibling shards —
prunes from the first block instead of only after its heap fills. Every
heap initialization site in the kernels honors the seed; a chunked-scan
test enforces this against upstream changes.

**Global ids without coordination.** Shards are assigned disjoint slot
offsets (stride 25M); a global document id is `slot_offset + local id`.
No id allocator, no consensus, and any subset of shards can be queried
independently.

**Comparable BM25 scores.** The coordinator gathers corpus-wide document
frequencies and lengths (`TermStats` fan-out) and sends the global stats
with each shard query, so per-shard BM25 scores merge correctly instead
of reflecting shard-local statistics.

**Resharding as replay.** Because ingest is logged, routing experiments
are derivable rather than re-ingested: the same 8 block-routed shard logs
replay into a hash-uniform 8-shard cluster (`reshard --logs=... --split=8`)
for measuring block vs uniform routing under identical data.

## Searching

**Quantized SIMD scan.** Vector scoring uses upstream turbovec's 4-bit
TurboQuant kernels: lookup-table dot products over interleaved codes,
with AVX-512/NEON paths and block-level pruning against the current heap
minimum.

**Chunked scans with mid-scan floor injection.** Nodes scan in chunks of
SIMD blocks and re-read the shared floor between chunks, so coordinator
updates take effect mid-query. In the 100k-opinion canary sweep, floor
sharing reduced vector candidates collected per query by roughly 6x, with
a hit-signature gate confirming identical results with sharing on and off.

**BM25 over the map.** Term lookup binary-searches a fixed-stride
directory (18-byte entries plus a term blob); postings decode lazily from
the map. Occurrence offsets stored per posting slice highlights directly
out of the stored original text.

**Hybrid fusion.** Vector and BM25 legs run per shard and fuse with
reciprocal-rank fusion, either shard-local (one round trip) or
coordinator-level with a rescore pass for exact lexical scores on the
fused candidate set.

**Measured, gated experiments.** `cluster_sweep` sweeps k across a live
cluster twice — floor sharing on and off — asserting bitwise-identical
hits before reporting `candidates_collected`, so pruning claims are
backed by a correctness gate rather than assumed.
