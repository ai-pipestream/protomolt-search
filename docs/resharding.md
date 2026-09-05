# Resharding design: log, split, merge, compaction

> **Note: AI-generated. Human review needed.**

This document describes how pipestream-search grows a live cluster: how
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

### Provider implications

For most vector indexes, such as HNSW graphs and IVF structures, native image
split/merge is painful because the structure is entangled. The current
implementation instead rebuilds provider images from raw vectors retained in
the WAL. The embedded TurboVec backend also has row-separable encoded data,
which can support a future encoded-row path without making that property part
of the product contract. Doc-partitioned BM25 postings are rebuilt from logged
documents. The shared requirement is reproducible provider configuration and
one scoring identity across the resulting shards.

## The pieces

### Write-ahead log (the mechanism)

Every persisted shard keeps an append-only log at `<index>.wal/`. The
log is a **folder of hash-bucketed segment files**, not one file:

```
<index>.wal/
  gen-000000/
    manifest.toml        # dimension, provider configuration, slot offset,
                         # generation, bucket geometry, format version
    bucket-000.wal       # framed records: [len][crc32][prost]
    ...
    bucket-063.wal
    markers.wal          # flush / snapshot markers
  gen-000001/            # created by InstallSnapshot rotation
    ...
```

Every vector/document record is routed to
`bucket = fnv1a64(id) >> (64 - bucket_bits)`. Each bucket file is therefore a
pre-partitioned slice of the shard's history. This is log-level
pre-sharding: splitting does not re-partition the log because the log
was born partitioned.

Each record also carries one generation-wide logical clock. `ReadWal` merges
the bucket and marker files by that clock and exports an immutable flushed
prefix, which gives replica and child catch-up one exact resume point despite
the physical files. Routed mapped ingest additionally records the opaque stable
product key beside every vector and document row. Live resharding partitions by
`fnv1a64(stable_key)`, never by a generation-local slot id.

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
(`.vector` plus a BM25 sidecar) and an updated shard map:

- **Split 1→N** (N ≤ bucket_count): child *i* owns a contiguous range
  of buckets. No repartitioning pass — the child's input is literally a
  set of bucket files. N > bucket_count falls back to re-hashing within
  buckets (correct, slower; bucket_count caps *cheap* split
  granularity).
- **Merge N→1**: replay the inputs' buckets together. Requires identical
  provider configuration and identical bucket geometry, enforced in code.

Children are built with the parent's provider configuration, so their native
scores share one identity. BM25 postings are
rebuilt from the logged document records; global df/avgdl re-aggregate
from `TermStats` at query time as usual.

- **Split by placement code** (`docs/placement.md`): `reshard
  --placement-column=<col> --placement-ranges=lo..=hi,...[,default]`
  assigns each logged row to the child whose code range holds the value
  its placement column carries, with no CEL at replay. Ranges may not
  overlap; the `default` child takes rows with no value or an uncovered
  code, and without one such a row refuses the split by id. Children keep
  the parent's full hash coverage, because routing under a tree is by
  code first and by hash inside the group.

### Snapshot installation

`InstallSnapshot` was designed for bulk load and is exactly phase 3 of
the skeleton: stream an image, validate its provider and scoring fingerprint, atomically rename
generation directories, recover interrupted swaps at startup. A node
that installs a resharded image rotates its WAL to a fresh generation
and logs on from there.

Since 2026-09-04 a node can also fetch an image itself
(`docs/snapshots.md`): `ExportSnapshot` publishes a shard's generation
to a directory with a hashed manifest that records the WAL cutoff the
image contains, and `InstallSnapshotFrom` pulls such a repository from a
directory, an HTTP(S) URL, or a peer node's `StreamSnapshot`, verifies
every artifact, and runs the same install. The cutoff in the manifest is
the clock replica catch-up (`replication::sync_once`) resumes from.

### Hot shard map and write barrier

The coordinator loads a versioned map (`--shard-map=file.toml`): a generation
number plus per-shard primary, optional replica, slot base, and stable-key hash
range. It polls complete newer maps and swaps one immutable `Arc`; an in-flight
query keeps the old snapshot, and every public `Query` response reports the
generation it served. A client may require an exact generation and receives a
precondition failure rather than silently crossing a cutover.

Routed ingest requires the old generation explicitly. Final cutover takes a
write barrier only on that routed ingest RPC, catches children through the
parent's last durable clock, verifies counts and scoring identity, writes the
new map durably, swaps it in memory, and releases writes. Queries never take
the barrier. If publication fails after the map is durable, writes remain
frozen; restart from the durable map or retry publication rather than reopening
the old generation and losing a tail.

## Invariants

1. **Split/merge only within one provider scoring configuration.** Inputs with
   different backend state are hard errors. Engine-version upgrades
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
image AND a fresh full-history log:

- **Online, per shard: `NodeService.CompactShard`** (`docs/mutations.md`).
  The node fixes a clock cutoff, replays the log through it into a dense
  all-live generation (one image, or one sealed segment per WAL bucket),
  writes the rewritten WAL generation from the same replay, tails the live
  log into the new generation through the ingest apply functions, and cuts
  over under a write lock that holds for the last `tail_bound` records.
  Writes and reads continue throughout. The superseded generation stays on
  disk as history.
- **Offline, one-child reshard.** `reshard --split=1` over a generation
  applies its delete and replacement records and writes a dense
  provider/BM25/FP32 image; install it with `InstallSnapshot`. That rotation
  records the image as `preexisting_*`, so the result is not
  log-reshardable and cannot be compacted again from its log; it is the
  bulk-load shape, not the maintenance one.
- The rewritten generation is what keeps the shard reshardable: `reshard`
  over it rebuilds the compacted shard (pinned in `tests/compaction.rs`), so
  a compacted shard can be split, merged, compacted again, or tailed by a
  replica from its new generation. A replica compacts on its own; its
  primary's ids do not map onto a compacted replica, so a pair re-baselines
  after either side compacts.
- **Natural triggers:** when the live-row overlay crosses an operator-selected
  threshold (the control plane's `--control-compact-tombstone-ppm`), after a
  bulk-load phase completes, and after a reshard retires its parent.
- Bucket files keep compaction units small and independent.

## Cost

The log stores raw float32 vectors inline: roughly 1x the raw embedding
size on top of the quantized index (which is ~8x smaller at 4-bit).
This is deliberate — the space buys bookkeeping, not serving capacity.

### Bulk load recipe

Because of that 1x cost, a full-corpus build should NOT ingest through
the logged AddVectors path — logging ~100M raw vectors writes more WAL
than index. The intended shape for an initial build:

1. Build shard images offline (the harness / reshard machinery) with one
   provider configuration and scoring identity. Retain the original FP32 rows
   as each child's exact-vector sidecar when FP32 candidate reranking is part
   of the serving contract.
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

## Hitless split runbook

This path is intentionally limited to one append-only routed source. Legacy
rows without stable keys, deletes/replacements, a rotated WAL, incomplete hash
ranges, or mismatched scoring fingerprints fail loudly.

1. Build the stable-key baseline while the source keeps serving:

   ```bash
   cargo run --release --example reshard -- \
     --log=/data/source.tv.wal --split=2 --stable-routing \
     --out-dir=/data/next --slot-base=0 --slot-stride=25000000 \
     --analysis-addr=http://analysis:50051
   ```

   This emits child images and `live-cutoff.toml`. The source generation must
   have full history, a fully clocked WAL, and one stable key per live row.

2. Start the child images on disjoint ports. Copy the emitted shard map to a
   staging path and replace each `addr = "TODO"`; do not place this staging
   map at the coordinator's live `--shard-map` path.

3. Capture child baselines and run background catch-up as often as needed:

   ```bash
   cargo run --release --example live_reshard -- init \
     --source=http://old:50051 --cutoff=/data/next/live-cutoff.toml \
     --old-generation=7 --children-map=/data/next/staging-map.toml \
     --state=/data/next/live.toml
   cargo run --release --example live_reshard -- catch-up \
     --state=/data/next/live.toml
   ```

4. Cut over. This freezes only generation-7 routed writes, applies and verifies
   the final prefix, atomically writes the live map, publishes generation 8,
   and releases writes:

   ```bash
   cargo run --release --example live_reshard -- cutover \
     --state=/data/next/live.toml \
     --coordinator=http://coordinator:50050 \
     --publish-map=/etc/protomolt-search/shard-map.toml
   ```

Keep the parent generation through soak and backup. A cutover error after the
new map is durable deliberately leaves writes frozen; restart the coordinator
from that map before accepting writes.

## Status

Built and tested:

- Bucketed WAL with manifest, CRC framing, per-file sequence, a gapless
  generation-wide logical clock, torn-tail recovery, generation rotation, and
  server-streamed exact prefix reads.
- Crash reconciliation (truncate at the applied tip on open),
  log-before-images fsync ordering at Flush, apply-then-log under the
  shard lock, degrade-to-unlogged on append failure, and the
  full-history guard (`preexisting_*` manifest fields, refused by the
  reshard tool).
- Offline `reshard` tool: cheap split by bucket range, fallback
  re-partitioning, merge with provider-configuration/geometry checks, shard-map
  emission. Split-then-search reconstructs the parent top-k bitwise;
  merge reproduces the monolithic index including BM25 identity.
- Bucket-bounded segmented replay and atomic aligned-segment publication.
  Peak replay holds one WAL bucket rather than a whole child; global BM25
  statistics and vector/BM25 heaps still merge exactly. See
  [immutable-segments.md](immutable-segments.md).
- Stable-key baseline partitioning, incremental child tailing, durable
  checkpoints, final write barrier, count/fingerprint verification, and atomic
  live map publication.
- Versioned hot shard maps with request snapshots and generation requirements;
  stable-key routed mapped ingest.
- Automatic primary-to-replica WAL catch-up with durable per-pair cursors and
  idempotent crash-window reconciliation.
- Control-plane `SPLIT_SHARD` executed by the node worker
  (`docs/cluster-control.md`, "Shard split"): the ranged stable split of
  the source's own WAL, placed children, the live tail, an ingest fence for
  the final drain, completion with the children as primaries, and the
  source's retirement — the hitless split runbook above as one durable
  action rather than three tool invocations.

Deliberately deferred:

- **Image-aware reshard** — splitting a post-InstallSnapshot shard means
  provider-specific partitioning or rebuilding the base vector image, splitting
  the BM25 store by document, and applying its log; today such shards are refused
  rather than mis-resharded.

- **Delete/replacement migration during live split** — child catch-up
  refuses these records instead of inventing an unverified cross-generation
  tombstone mapping. Node-local compaction maps them through the replay's
  id map (`docs/mutations.md`); the N-way split does not yet.
