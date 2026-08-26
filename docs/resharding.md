# Resharding design: log, split, merge, compaction

> **Note: AI-generated. Human review needed.**

This document describes how turbovec-search grows a live cluster: how
shards split when they get too big, merge when they get too small, and
how the log underneath makes both possible without re-indexing from the
source pipeline. It records the design lineage, the invariants, what is
built today, and what is deliberately deferred.

## Lineage

Live resharding is a well-trodden problem with one dominant solution
shape, visible in Solr's SPLITSHARD, Vitess resharding, CockroachDB
range splits, and HBase region splits:

1. **Snapshot** the parent shard at a known sequence point.
2. **Build children in the background** from that snapshot, partitioned
   by a deterministic doc-to-shard function, while the parent keeps
   serving reads and accepting writes. Children tail the write log from
   the snapshot point, each applying only the writes it owns.
3. **Swap atomically** by flipping metadata — a generation-numbered
   shard map at the coordinator — never by moving data mid-query. The
   parent drains and retires.

The key insight: **the write log is the mechanism; the swap is just
metadata.** Systems where the index is the source of truth struggle to
reshard. Systems where the index is a *materialized view of a log* get
split, merge, rebuild, and disaster recovery from the same primitive.

### Our structural ace

For most vector indexes (HNSW graphs, IVF centroids) split/merge is
painful because the structure is entangled. With seeded calibration, a
TurboQuant index is **row-separable**: rotation and codebook are pure
functions of (dim, seed) and (bit_width, dim), calibration is pinned,
and every vector's packed codes are an independent row. A split is row
filtering, a merge is concatenation — byte-identical to a from-scratch
build, with no re-encoding and no quality drift. Doc-partitioned BM25
postings behave the same way. The hard part for us is orchestration,
not index surgery.

## The pieces

### Write-ahead log (the mechanism)

Every persisted shard keeps an append-only log at `<index>.wal/`. The
log is a **folder of hash-bucketed segment files**, not one file:

```
<index>.wal/
  gen-000000/
    manifest.toml        # dim, bit_width, calibration, slot_offset,
                         # generation, bucket_bits, format_version
    bucket-000.wal       # framed records: [len][crc32][prost]
    ...
    bucket-063.wal
    markers.wal          # flush / snapshot markers
  gen-000001/            # created by InstallSnapshot rotation
    ...
```

Every vector/document record is routed to
`bucket = fnv1a64(id) >> (64 - bucket_bits)` — the *same* function the
resharder partitions by. Each bucket file is therefore a
pre-partitioned slice of the shard's history. This is log-level
pre-sharding: splitting does not re-partition the log because the log
was born partitioned.

Writes are applied to the in-memory indexes first and logged
immediately after, under one lock — a failed apply must never reach the
log, or its assigned ids would be reused and the duplicate would poison
every replay. Durability is unaffected by the ordering: both sides are
volatile until `Flush`, and `Flush` fsyncs the log BEFORE it writes the
index images, so every durable index state has its log superset on disk.
Per-file sequence numbers with CRC framing; a torn tail from a crash is
truncated on resume without affecting other buckets.

Recovery reconciles the log against the applied state at open: buffered
appends can outlive a process crash (kernel-cached pages need no fsync),
leaving the on-disk log ahead of the on-disk indexes, so records at or
above the applied tip are truncated — they were never durable-acked
(`Flush` is the durability point). If an append fails at runtime, the
shard degrades loudly to unlogged: the generation is renamed `.broken`
and serving continues (an unlogged shard serves fine; it just can only
be rebuilt, never resharded).

### Split and merge (replay)

The `reshard` tool replays the log offline and builds install images
(`.tv` + `.tv.bm25`) plus an updated shard map:

- **Split 1→N** (N ≤ bucket_count): child *i* owns a contiguous range
  of buckets. No repartitioning pass — the child's input is literally a
  set of bucket files. N > bucket_count falls back to re-hashing within
  buckets (correct, slower; bucket_count caps *cheap* split
  granularity).
- **Merge N→1**: replay the inputs' buckets together. Requires
  identical calibration and identical bucket geometry — enforced, not
  documented.

Children are built with the parent's seeded calibration, so scores are
byte-comparable with the parent and with each other. BM25 postings are
rebuilt from the logged document records; global df/avgdl re-aggregate
from `TermStats` at query time as usual.

### Atomic swap (already existed)

`InstallSnapshot` was designed for bulk load and is exactly phase 3 of
the skeleton: stream an image, validate calibration, atomically rename
generation directories, recover interrupted swaps at startup. A node
that installs a resharded image rotates its WAL to a fresh generation
and logs on from there.

### Shard map (the metadata)

The coordinator can load a versioned map (`--shard-map=file.toml`):
a generation number plus per-shard address, slot range, and hash range.
The map is the id→shard authority; `--nodes` remains as the implicit
generation 0.

## Invariants

1. **Split/merge only within one (engine version, calibration) pair.**
   Mixed-calibration merges are hard errors. Engine-version upgrades
   are a different mechanism (blue/green: rebuild all shards from the
   log, swap the whole map) — pleasingly, the same machinery applied
   cluster-wide.
2. **Ids are generation-scoped.** A reshard reassigns contiguous slot
   ranges per child. Global vector ids are internal addressing, valid
   within one map generation; external identity lives in the documents
   themselves (e.g. the court pipeline's opinion id + ordinal).
3. **The shard map is the id→shard authority.** Queries and snapshots
   are stamped with a generation; a flip is a metadata change, never a
   data move.
4. **Policy rule: a shard without a WAL can serve but can never be
   split or merged — only rebuilt from source.** Logging is therefore
   required for any shard that will ever be resharded, and defaults on
   for persisted shards.
5. **Reshard requires full history.** A generation that began with
   state its log does not contain — the image an `InstallSnapshot`
   rotation superseded it with, or the shard's contents when logging
   was enabled after data existed — records that state as
   `preexisting_vectors`/`preexisting_documents` in its manifest, and
   the reshard tool refuses it: a log-only replay would silently drop
   the preexisting state. Such shards serve normally; resharding them
   means rebuilding from source (or, later, an image-aware reshard
   that partitions the base image plus the log).

## Compaction

Classic WAL truncation does not apply — the log is full history, kept
for replay. "Compaction" means collapsing history into a fresh base
image:

- **Live compaction = self-snapshot.** Dump the live index to an image,
  `InstallSnapshot` it back onto the same node (atomic generation swap,
  crash-safe), rotate to an empty log generation, archive the old one.
  Reads never stop; ingest pauses only for the final swap.
- **Natural triggers:** after a bulk-load phase completes, after any
  `InstallSnapshot` (rotation already happens; retiring the old
  generation is the compaction), after a reshard retires its parent,
  and later when removes exist (rewrite dropping removed ids).
- Bucket files keep compaction units small and independent.

## Cost

The log stores raw float32 vectors inline: roughly 1x the raw embedding
size on top of the quantized index (which is ~8x smaller at 4-bit).
This is deliberate — the space buys bookkeeping, not serving capacity.

### Bulk load recipe

Because of that 1x cost, a full-corpus build should NOT ingest through
the logged AddVectors path — logging ~100M raw vectors writes more WAL
than index. The intended shape for an initial build:

1. Build shard images offline (the harness / reshard machinery), with
   the one seeded calibration.
2. `InstallSnapshot` the images onto the nodes. The WAL rotates to a
   fresh generation whose manifest records the image as
   `preexisting_*` — cheap, honest, and the log stays near-empty.
3. Live ingest from then on goes through the logged path; only the
   incremental tail costs WAL space.

The tradeoff is explicit: a post-install shard is not log-reshardable
(invariant 5) until image-aware reshard exists. For a bulk-built corpus
that is the right trade — resharding a freshly built corpus is better
done by re-running the offline build with a different N, which is
cheaper than any replay.

### Retention

Superseded generations (after a snapshot install or a completed
reshard) are history, not garbage. Policy: archive, never auto-delete —
a generation directory is self-contained (manifest + buckets), so
`mv gen-000000 <archive>/` is a complete archival. `wal_inspect`
reports per-generation record counts and bytes so the operator can see
what a generation is worth before archiving it. (Systems with an
acknowledged-prefix WAL truncate instead; our full-history model
archives whole generations at rotation boundaries.)

## Status

Built and tested:

- Bucketed WAL with manifest, CRC framing, per-file seq, torn-tail
  recovery, generation rotation on InstallSnapshot.
- Crash reconciliation (truncate at the applied tip on open),
  log-before-images fsync ordering at Flush, apply-then-log under the
  shard lock, degrade-to-unlogged on append failure, and the
  full-history guard (`preexisting_*` manifest fields, refused by the
  reshard tool).
- Offline `reshard` tool: cheap split by bucket range, fallback
  re-partitioning, merge with calibration/geometry checks, shard-map
  emission. Split-then-search reconstructs the parent top-k bitwise;
  merge reproduces the monolithic index including BM25 identity.
- Versioned shard-map config at the coordinator.

Deliberately deferred:

- **Image-aware reshard** — splitting a post-InstallSnapshot shard
  means partitioning the base image (row-filtering the `.tv`, splitting
  the BM25 store by doc) plus its log; today such shards are refused
  rather than mis-resharded.

- **Live catch-up** — children tailing the parent's log while it serves
  (phase 2 of the skeleton). Today split/merge is offline replay +
  atomic swap; the log format already carries everything catch-up needs.
- **Generation flip at query time** — the coordinator reads the map at
  startup; hot map reload and query-stamping are future work.
- **Hash-based ingest routing** — writes currently go to explicitly
  addressed shards; routing by `hash(doc_id)` range comes with the map.
- **Replication** — the log + InstallSnapshot are the right substrate
  (a replica is a node that tails a log and installs images), but no
  replication protocol exists yet. WAL record fields 6–7 are reserved
  for the multi-writer pieces — clock tags and recovery points, the
  generalization of per-file seq that answers "what exactly am I
  missing" across writers — so old logs stay readable when they
  arrive.
- **Streaming reshard** — the replay buffers vectors in memory per
  child; spilling to disk is a follow-up if very large shards need it.
