# turbovec-search

Distributed top-k search over [turbovec](https://github.com/ai-pipestream/turbovec)
(TurboQuant) shard indexes, with **collaborative mid-query floor sharing**:
shard nodes publish their current k-th best score while they scan, the
coordinator aggregates the maximum and pushes it back, and nodes prune the
remainder of their scan against it — losslessly.

Phase 1: one crate, one binary, three roles (`node`, `coordinator`, `both`),
tonic gRPC + tokio, static cluster membership.

## Design

```
                        ┌──────────────────────┐
                        │     coordinator      │
   client ──Search──▶   │   (SearchService)    │
                        │                      │
                        │  FloorTracker: max   │◀──┐ k-th best per shard
                        │  over shard floors   │   │ (once heap fills)
                        └───┬───────┬───────┬──┘   │
              Start+FloorUpdate │       │       │  │ FloorUpdate
            ┌───────────────────┘       │       └───────────────┐
            ▼                           ▼                       ▼
     ┌─────────────┐             ┌─────────────┐         ┌─────────────┐
     │   node 0    │             │   node 1    │   ...   │   node N    │
     │ (NodeService)│            │ (NodeService)│         │ (NodeService)│
     │ shard index │             │ shard index │         │ shard index │
     │ chunked scan│             │ chunked scan│         │ chunked scan│
     └─────────────┘             └─────────────┘         └─────────────┘
```

Floor flow for one query:

1. The coordinator opens a bidi `SearchShard` stream to every node and
   sends `StartShardSearch { query, k, request_id }`.
2. Each node scans its shard **in chunks** of `chunk_blocks` SIMD blocks
   (default 64 blocks = 2048 vectors). Each chunk is a
   `search_with_options` call restricted to that chunk's slot range by an
   allowlist mask, seeded with the best floor known at that moment
   (`initial_threshold`).
3. Once a node's running top-k heap is full, it publishes its k-th best
   after each chunk (`FloorUpdate` node → coordinator). The coordinator
   tracks the max over all shards and broadcasts every raise to all nodes
   (`FloorUpdate` coordinator → node). Nodes apply the raised floor to the
   next chunk.
4. Each node ends its stream with `SearchShardDone { hits, stats }` — its
   local top-k plus scan counters. The coordinator merges the shard lists
   (score descending; ties by shard index, then vector id) and answers the
   client.

Chunking exists because turbovec's scan is a single synchronous call with a
call-time-fixed floor; scanning in masked chunks gives the floor flow
intra-query reactivity without patching the kernel. The union of the masked
chunk ranges is exactly the whole shard, so chunking alone changes nothing
about results.

## The lossless invariant, and why it holds

**Claim.** Pruning candidates that score below the max published floor can
never drop a true global top-k hit.

**Why.** A floor published by a shard is that shard's current k-th best,
emitted only once the shard holds k candidates. The k-th best of any subset
of the corpus is a lower bound on the k-th best of the whole corpus: the
global top-k picks the best k of the union, so its k-th entry scores at
least as high as the k-th entry of any shard's top-k. Therefore every
published floor ≤ the true global k-th best, and so is the max over
published floors. Any candidate scoring strictly below that max also scores
below the global k-th best — at least k other candidates beat it — so it
cannot belong to the global top-k. Candidates scoring exactly at the floor
are kept (turbovec's threshold is inclusive and the k-th-best seeding keeps
boundary ties), so tie scenarios are safe too.

The same argument covers the node's *local* floor (its own heap's k-th
best): it is a lower bound on the shard's final k-th best, hence on the
global one.

Empirically: `tests/lossless.rs` builds a 20k-vector corpus (dim 128,
4-bit), fits calibration on a sample, builds 3 shard indexes plus one
monolithic index with the same seeded calibration, and asserts the
coordinator's top-10 equals the monolithic top-10 **exactly** — same ids,
bitwise-same scores, same order — for several queries, with floor sharing
on and off. `tests/node_loopback.rs` injects a floor mid-scan over real
gRPC and asserts identical results.

## Why scores are comparable across shards at all

Quantized scores are only comparable across separately built indexes if
every index encodes vectors identically. turbovec's seeded TQ+ calibration
provides this: fit the per-coordinate `(shift, scale)` once on a
representative sample (build a throwaway index from the sample, read
`calibration()`), then construct every shard with
`TurboQuantIndex::new_with_calibration`. Same calibration ⇒ byte-identical
codes for the same vector ⇒ per-slot scores are pure functions of the
vector, so shard scores can be merged directly. `NodeService.GetCalibration`
exposes a shard's calibration so deployments can verify uniform seeding.

## Running

```bash
cargo build --release

# Single-process demo: both roles, random demo corpus (calibration fitted
# on a 20% sample and seeded), one self-issued search at the end.
./target/release/turbovec-search --role=both \
    --demo-vectors=20000 --dim=128 --bit-width=4 \
    --nodes=127.0.0.1:50051 --demo-query

# A real shard node over a persisted .tv index.
./target/release/turbovec-search --role=node \
    --index=/data/shard-0.tv --slot-offset=0 --node-listen=0.0.0.0:50051

# A coordinator over three nodes.
./target/release/turbovec-search --role=coordinator \
    --coord-listen=0.0.0.0:50050 \
    --nodes=node0:50051,node1:50051,node2:50051
```

Every flag has a `TURBOVEC_*` env fallback (`--role`/​`TURBOVEC_ROLE`,
`--nodes`/`TURBOVEC_NODES`, `--chunk-blocks`/`TURBOVEC_CHUNK_BLOCKS`,
`--floor-sharing`/`TURBOVEC_FLOOR_SHARING`, `--slot-offset`, `--index`,
`--demo-vectors`, `--dim`, `--bit-width`, `--node-listen`, `--coord-listen`).

## Testing and benchmarking

```bash
cargo test            # unit + integration (lossless, loopback, benchmark)
cargo test --release --test bench_sharing -- --nocapture   # with numbers
```

The benchmark (`tests/bench_sharing.rs`) runs 50 queries against a 60k
corpus on 3 shards, with and without sharing, and reports
`candidates_collected` (every candidate that survived the floors in effect
when its chunk ran — the kernel-visible proxy for skipped work, since the
kernel exposes no block-skip counter) plus wall-time medians. It asserts
identical hit sequences in both modes and strictly fewer collected
candidates with sharing.

## Layout

- `proto/turbovec/search/v1/search.proto` — the wire API (heavily
  commented), codegen via `build.rs` + tonic-build.
- `src/chunked.rs` — the chunked scan (mask per chunk, floor seeding,
  running heap, publish/poll points). Pure and unit-tested.
- `src/merge.rs` — global top-k merge (total order: score desc, shard, id)
  and the coordinator's floor tracker.
- `src/node.rs` / `src/coordinator.rs` — the two gRPC services.
- `src/config.rs` / `src/main.rs` — CLI/env config and the one-binary
  process wiring.
- `tests/` — lossless e2e, NodeService loopback with mid-scan injection,
  and the skipped-work benchmark.

## Limitations (phase 1)

- **Static membership.** The coordinator's node list is fixed at startup;
  no discovery, no re-sharding, no node failure handling beyond surfacing
  the error.
- **No replication.** Each shard lives on exactly one node.
- **Calibration is manual.** Nodes must be constructed with the same
  seeded calibration out of band (the tests do it in-process);
  `GetCalibration` only verifies, it does not distribute.
- **Per-query streams.** Each query opens a fresh channel + `SearchShard`
  stream per node (no pooling), and vector ids are `slot_offset + slot`
  with contiguous disjoint offsets assigned out of band.
- **Skipped-work metric is a proxy.** `candidates_collected` is countable
  through the public API; a true per-block prefilter-skip counter needs a
  small patch to the turbovec kernel.
