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

### Cluster configuration file

For real deployments the binary reads a TOML file (`--config cluster.toml`,
or `TURBOVEC_CONFIG`). Precedence: **CLI flag > env var > config file >
default**. Every flag takes `--key=value` or `--key value`.

```toml
role = "both"                                  # node | coordinator | both
coord_listen = "0.0.0.0:50050"
nodes = ["host-a:50051", "krick-1:50051"]      # fan-out order = tie-break order
chunk_blocks = 64                              # scan chunk size (SIMD blocks)
floor_sharing = true
max_message_mib = 64                           # gRPC message cap (both directions)

[[shards]]                                     # shards this process serves
listen = "0.0.0.0:50051"                       # one NodeService listener per shard
index = "/data/turbovec/shard-0.tv"
slot_offset = 0                                # global id base for this shard

[[shards]]
listen = "0.0.0.0:50052"
index = "/data/turbovec/shard-1.tv"
slot_offset = 20000
```

Membership is **static**: the coordinator's `nodes` list and each node's
`[[shards]]` set are fixed at startup. Changing topology means editing
configs and restarting — deliberate for this phase. Single-shard shorthand
(`--index`, `--demo-vectors`, `--node-listen`, `--slot-offset`) overrides
the file's `[[shards]]` entirely.

## k-sweep benchmark harness

`sweep` is a second binary that builds a deterministic corpus, serves it as
N shards on loopback (real gRPC), and sweeps k with floor sharing on and
off, reporting candidates collected and wall medians/p90 per mode — the
harness for measuring how sharing's payoff varies with k. It also asserts
sharing never changes results at any k.

```bash
cargo run --release --bin sweep -- \
    --vectors=60000 --dim=128 --shards=3 \
    --k=10,100,1000,10000 --queries=20 \
    --chunk-blocks=64 --modes=on,off
```

`--write-indexes DIR` additionally persists the shards as `.tv` files and
prints ready-to-paste `[[shards]]` config entries — this is how the indexes
for a real deployment are produced (shared calibration baked in).

## Ingest flow (write path)

Shards ingest over gRPC; prebuilt `.tv` files are no longer required.
Deployment order for a from-scratch cluster is **fit → seed → ingest →
search**:

1. **Fit** a calibration on a representative sample (any tool that can run
   turbovec: build a throwaway index from the sample, read `calibration()`).
2. **Seed** every shard with it via `NodeService.SetCalibration` — or let
   the CLI do it: start one seeded node (demo or loaded index), then
   `turbovec-search calibrate --fit-from=node0:50051 --apply-to=node1:50051,node2:50051`.
   SetCalibration is accepted only while a shard is empty; calibration is
   locked for the index's lifetime (turbovec's own rule), so a retry of the
   same calibration is an idempotent no-op and anything else is rejected.
3. **Ingest** with `NodeService.AddVectors` (client-streaming, flat
   batches). Batches apply under the shard's write lock; searches hold the
   read lock for their whole scan, so no search observes a half-applied
   batch. Ids are server-assigned: the i-th vector of a shard is
   `slot_offset + i` (positional; turbovec's id-mapped index does not
   support the masked, floor-seeded scan this service uses).
4. **Search** as before — the lossless invariant holds for ingested data
   exactly as for prebuilt indexes (proven by `tests/multiprocess.rs`).

**Persistence**: `NodeService.Flush` writes the shard to its config
`index` path (atomic `.tv` write), and `save_on_shutdown = true` (the
default) flushes on SIGINT/SIGTERM. A shard whose index path does not
exist at startup starts empty; after ingest + flush (or graceful
shutdown), a restart with the same config comes back with all vectors
and the locked calibration (`.tv` persists it).

## Two-machine runbook

Topology: host A (this host) runs coordinator + shard 0; host B (`krick-1`)
runs shard 1. Static membership — both configs list the same node set.

1. **Build and produce shard indexes** on host A:

   ```bash
   cargo build --release
   ./target/release/sweep --vectors=100000 --shards=2 --k=10 --queries=1 \
       --modes=off --write-indexes=/data/turbovec
   # writes /data/turbovec/shard-0.tv, shard-1.tv (same seeded calibration)
   ```

   (Any source of `.tv` files works as long as every shard was built with
   the SAME seeded calibration — that is what makes scores mergeable.
   Verify with `NodeService.GetCalibration` if in doubt. Alternatively,
   skip files entirely: point each node's `index` at a fresh path, start
   empty, then seed + ingest over gRPC per "Ingest flow" above.)

2. **Copy the binary and shard 1 to krick-1:**

   ```bash
   scp target/release/turbovec-search krick-1:/usr/local/bin/
   scp /data/turbovec/shard-1.tv krick-1:/data/turbovec/
   ```

3. **Config on krick-1** (`/etc/turbovec/krick-1.toml`):

   ```toml
   role = "node"
   [[shards]]
   listen = "0.0.0.0:50051"
   index = "/data/turbovec/shard-1.tv"
   slot_offset = 50000          # = vectors in shard 0 (contiguous offsets)
   ```

   Start: `turbovec-search --config /etc/turbovec/krick-1.toml`

4. **Config on host A** (`/etc/turbovec/host-a.toml`):

   ```toml
   role = "both"
   coord_listen = "0.0.0.0:50050"
   nodes = ["host-a:50051", "krick-1:50051"]

   [[shards]]
   listen = "0.0.0.0:50051"
   index = "/data/turbovec/shard-0.tv"
   slot_offset = 0
   ```

   Start: `turbovec-search --config /etc/turbovec/host-a.toml`

5. **Verify.** From host A (or any host that can reach `host-a:50050`),
   issue a real search. The binary's built-in check does one:

   ```bash
   turbovec-search --role=coordinator --nodes=host-a:50051,krick-1:50051 \
       --coord-listen=127.0.0.1:59999 --demo-query --query-dim=128
   ```

   (spins a throwaway coordinator against the running nodes and prints the
   merged top-10). Or call `SearchService.Search` with any gRPC client
   against `host-a:50050` — proto at `proto/turbovec/search/v1/search.proto`.

6. **The large-k two-machine experiment** (manual, not a CI gate): run the
   sweep in-process for baseline numbers (`--k=10,100,1000,10000`), then
   repeat against the 2-machine cluster by pointing a sweep-style client at
   `host-a:50050`. Watch `candidates_collected` and wall medians per k per
   mode.

## Testing and benchmarking

```bash
cargo test            # unit + integration (lossless incl. k=1000, loopback, benchmark)
cargo test --release --test bench_sharing -- --nocapture   # with numbers
```

The benchmark (`tests/bench_sharing.rs`) runs 50 queries against a 60k
corpus on 3 shards, with and without sharing, and reports
`candidates_collected` (every candidate that survived the floors in effect
when its chunk ran — the kernel-visible proxy for skipped work, since the
kernel exposes no block-skip counter) plus wall-time medians. It asserts
identical hit sequences in both modes and strictly fewer collected
candidates with sharing. `tests/lossless.rs` additionally proves exact
losslessness at k=1000 over a 24k corpus.

## Layout

- `proto/turbovec/search/v1/search.proto` — the wire API (heavily
  commented), codegen via `build.rs` + tonic-build.
- `src/chunked.rs` — the chunked scan (mask per chunk, floor seeding,
  running heap, publish/poll points). Pure and unit-tested, including
  k=1000.
- `src/merge.rs` — global top-k merge (total order: score desc, shard, id)
  and the coordinator's floor tracker.
- `src/node.rs` / `src/coordinator.rs` — the two gRPC services. The node
  owns the shard state machine (empty → seeded → live) behind a write
  lock: chunked scans under the read lock, adds/calibration under the
  write lock, flush on demand or shutdown.
- `src/config.rs` / `src/main.rs` — TOML/env/CLI config and process wiring
  (multi-shard, multi-role, graceful shutdown, `calibrate` subcommand).
- `src/harness.rs` — corpus generation, calibration fitting, shard building
  and loopback server startup shared by tests and the sweep binary.
- `src/bin/sweep.rs` — the k-sweep benchmark harness.
- `tests/` — lossless e2e (k=10 and k=1000), NodeService loopback with
  mid-scan injection, ingest/calibration rules, a multi-process
  ingest-and-restart acceptance test, and the skipped-work benchmark.

## Limitations

- **Static membership.** The coordinator's node list is fixed at startup;
  no discovery, no re-sharding, no node failure handling beyond surfacing
  the error.
- **No replication.** Each shard lives on exactly one node.
- **Calibration distribution is manual-trigger.** `SetCalibration` (or the
  `calibrate` subcommand) pushes a fitted calibration; nothing fits or
  verifies automatically, and shards with mismatched calibrations produce
  incomparable scores without warning beyond `GetCalibration` inspection.
- **Positional ids only.** Ingested vectors are identified by
  `slot_offset + slot` in insertion order; client-chosen ids would need
  turbovec's `IdMapIndex`, which lacks the masked, floor-seeded scan.
  Deletes/updates are not supported (append-only).
- **Durability is flush-based.** Vectors are durable after `Flush` or a
  graceful shutdown; an ungraceful kill loses everything since the last
  flush (no WAL, no save interval).
- **Per-query streams.** Each query opens a fresh channel + `SearchShard`
  stream per node (no pooling).
- **Skipped-work metric is a proxy.** `candidates_collected` is countable
  through the public API; a true per-block prefilter-skip counter needs a
  small patch to the turbovec kernel.
