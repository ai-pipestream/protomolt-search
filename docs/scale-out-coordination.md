# Scale-out coordination: a tree of coordinators and bandwidth as the budget

A proposal for review before anything is built. It designs the two
remaining items of the 2026-09-05 roadmap goal on top of the placement
tree (`placement.md`) and the partitioned layout with segment pruning
(`segment-pruning.md`, `benchmarks/partition-pruning-2026-09.md`), both
on main.

Status: proposal, 2026-09-05, reviewed. The restricted, read-only relay
the review cleared exists (`relay-coordinators.md`: `StreamSearch`,
`TermStats`, `Health`, a map interface with pinned revisions), and the
keyword leg now runs through it (`Bm25Query`, `Bm25PhraseQuery`,
`Bm25QueryStream`, `Bm25Rescore`, `ShardLegs`, with the root's
statistics forwarded unchanged and the epoch token translated per
child); the rest of item 2 and item 3 are not built. The contract
it would need first (a scan-rate field on `NodeCapacity` and a
`ClusterControl.PlanBalance` dry run, both refusing by name) sits on the
branch `feat/scale-out-reservation-2026-09` so the review can change it
before it reaches main. Two decisions are the operator's and are named at
the end, with the questions a reviewer is asked to answer.

Review: [changes requested, 2026-09-05](scale-out-coordination-review-2026-09-05.md).
The review qualifies the composition, epoch, telemetry and standby claims
below. It identifies a restricted query relay and scan instrumentation that
can proceed independently; full relay compatibility, the final balance
contract, automatic failover and segment movement are not cleared. Phone-owned
shards remain on their originating device. Read the review before implementing
this proposal.

## What exists, and what it costs at scale

The coordinator holds the only top-k heap in a query. Nodes stream
candidates and prune against a floor the coordinator raises; floor raises
and advisory cancels travel on a signed UDP fast lane beside the gRPC
stream, every datagram with a gRPC twin, and a query completes only when
every shard's completion frame has arrived (`docs/architecture.md` §4).
Global BM25 statistics come from `NodeService.TermStats` per shard, cached
by epoch (`src/stats_cache.rs`). The control plane (`ClusterControl`) is one
process with a durable state file: leases, capacity reports, placement
decisions, topology history, publication (`docs/cluster-control.md`).

Per query, the coordinator's work is linear in the number of shards it
talks to, three times over:

| Cost | Per shard | Why it matters at thousands |
|---|---|---|
| Open streams and UDP targets | one each | file descriptors and per-query setup |
| Floor raise | one datagram plus one gRPC twin | every raise fans out to N; raises are frequent early in a scan |
| Statistics fetch on a cache miss | one `TermStats` | a cold term costs N round trips before scoring starts |
| Completion | one frame to wait for | tail latency is the slowest of N, and hedging is per shard |

A few hundred shards is fine. Thousands need a level in between, and the
placement tree already says where that level is: a leaf, or a subtree, is a
node set with one code range, so it is the natural unit for an intermediate
coordinator to own.

## Item 2: a tree of coordinators

Built so far: the restricted relay of `relay-coordinators.md`, with the
vector stream and the keyword leg through it. What follows is the
design it grows toward; the review names the gates.

### The relay coordinator

A relay is a coordinator that presents itself to its parent as one shard.
It serves the node-facing surface the parent already speaks
(`StreamSearch`, `Bm25QueryStream`, `TermStats`, `Health`, the browse and
aggregation shard routes) by fanning out to its own children and merging.
Nothing new is spoken between levels; the parent does not know it is
talking to a relay. Composition is what keeps the exactness argument
intact: each level's completion frame is issued only when every child's
has arrived, so the root's rule "answer after all frames" is unchanged.

What the relay does at each level:

- **Candidates.** Forwards its children's candidates upward as one stream,
  filtered by the floor it holds. It may keep a local heap of its own
  subtree's top-k: the subtree's k-th best is a valid floor for its
  children (the global k-th best can only be higher), so a relay tightens
  its children before the parent's raise arrives. Exactness holds because
  a floor is only ever a lower bound on the final cutoff.
- **Floors.** A parent raise is forwarded to every child on the same UDP
  lane with the same signing, and the gRPC twin follows. The relay never
  lowers a floor.
- **Statistics.** `TermStats` sums document frequencies and lengths over
  its children and reports a composite epoch (a hash of child epochs), so
  the root's stats cache treats a subtree as one shard. The typo rule
  (a leaf no shard can resolve) composes: the relay reports "unresolvable"
  only when every child does.
- **Placement pruning.** The relay owns one code range, so the root skips
  it by the same rule it skips a shard. Inside, the relay applies the tree
  walk to its own children. One walk, three levels, no new protocol; this
  is the same statement as the segment level.
- **Hedging and deadlines.** The relay hedges its children; the parent
  hedges relays. A relay with a replica is two relays over the same
  children.

Fan-out per level of 64 to 256 gives two levels for 4k to 65k shards.

### What is unchanged

The wire contract. A relay is `--role=coordinator` with a shard map whose
entries are other coordinators, plus one flag that makes it serve the
node-facing surface. The root's map lists relays as shards with their code
ranges. Routed ingest goes through the root, which evaluates the placement
tree and hands the stream to the relay owning the leaf, which hashes into
its children.

### The control plane: one process today

`ClusterControl` is one process with a durable file. Its decisions are
correct because they are serialized in that process and fenced by lease
tokens and control revisions. That model does not survive the process:
a coordinator restart is a control-plane outage, and two coordinators
cannot both hold the file.

The tree makes the question concrete: relays need the map too, and they
need to agree on the generation.

Options, with a recommendation:

1. **Raft over the control state, coordinators as voters.** The durable
   file becomes the state machine of a replicated log; `PublishTopology`
   is a committed entry; relays are learners that receive the map by log
   replication instead of file polling. Three or five voters. This is the
   shape every system in the reference landscape ended up with. The cost
   is either a dependency (`openraft` is the mature Rust one) or a
   hand-rolled Raft, which is a project of its own and a place where
   subtle bugs live.
2. **An external store** (etcd, Consul) for the map and leases. Least
   code, but a runtime dependency the phone-class and Pi deployments do
   not want, and a second system to operate.
3. **Keep one process, add a standby** that tails the durable file over
   `StreamSnapshot`-style replication and takes over on lease expiry.
   Smallest change; the failover window is the lease, and split-brain is
   prevented by the same fencing tokens nodes already check.

Recommendation: 3 first, because it is a week of work inside the existing
fencing model and covers the restart case, then 1 when the tree is real,
with `openraft` unless the user prefers to own the implementation. Option
2 is not recommended.

## Item 3: bandwidth as the budget

Status, 2026-09-05: the measurement and the balance dry run exist
(`bandwidth-budget.md`); execution of a move and segment-subset
ownership do not.

An unfiltered vector query reads every 4-bit vector of every shard it
touches. There is no per-chunk upper bound to stop early on, so the cost
is bytes read divided by the node's scan rate, and the slowest node sets
the query's latency. The fix is not a smarter scan; it is placing bytes
where the rate is.

### Measure the rate, not the core count

`NodeCapacity` reports disk, memory, threads, and failure domain. None of
those is the number that matters. The telemetry now on main has it: the
scan-work counters and the per-route latency histograms give bytes
scanned per second per node under real load, and the recent-query ring
gives it per query. A node's **scan rate** (bytes per second, smoothed)
becomes a capacity field the lease renewal carries, measured, never
declared. A Pi and a workstation then differ by their measured rate, and
a phone by its own.

### Balance bytes by rate

The control plane's placement decision becomes: assign leaves (and, within
a leaf, segments) so that the largest `bytes_on_node / rate_of_node` is
as small as possible. That is the makespan of the unfiltered query, and
minimizing it is a greedy assignment recomputed on each reconciliation
tick, with hysteresis so a transient slow-down does not move data.

### The fast node takes segments from the slow one

Segments are immutable, hash-verified artifacts, and a shard can serve a
subset of them (`SegmentedShard::masked`). So segment ownership can be
split from shard ownership: a fast node installs a slow node's sealed
segments through the existing snapshot repository path
(`ExportSnapshot` / `InstallSnapshotFrom`, digest-verified), and the
shard map names, per shard, which node serves which segment range. The
coordinator fans out a shard's query to each owner with its segment
subset; the union is exact because segments are disjoint by row range and
the tail stays with the shard's primary. A moved segment is a copy, not a
move, until the map says otherwise, so the slow node keeps serving until
the cutover.

What this needs, in order:

1. The measured scan rate on the lease (node agent reads its own
   counters; `NodeCapacity` gains a field).
2. Segment-subset ownership in the shard map and the fan-out (the mask
   exists; the map entry and the merge do not).
3. The greedy balancer in `ReconcileCluster`, with the dry-run shape
   `PlanPlacement` already has: report what would move and by how many
   bytes before moving it.
4. The copy path over the snapshot repository, then the map flip.

The placement tree is what makes this bounded: a leaf's node set is the
pool a balancer moves within, so a phone-class node holding one small leaf
is never asked to take a workstation's segments.

## Questions for the reviewer

1. The relay presents itself as one shard through the existing node-facing
   surface. Is there a route on that surface whose merge across children
   is not exact by composition (identity carry-through, cursors bound to a
   generation, catalog-backed publication)? Name it and the invariant it
   would need.
2. `TermStats` summed across children with a composite epoch: does the
   stats cache's epoch check tolerate a hash of child epochs, or does it
   need a monotone number per relay?
3. The relay owns one placement code range. Is anything in the
   authorization or collection layer keyed on a shard address in a way a
   relay address would break?
4. Segment-subset ownership: the map entry would name several servers for
   disjoint segment ranges of one shard. Does the identity or catalog
   work assume one server per shard anywhere?
5. The scan-rate measurement: bytes read per second from the scan
   counters, smoothed. Is there a better signal already collected on the
   node side, or a reason the lease renewal is the wrong carrier?

## Decisions for the operator

1. **Consensus.** Standby-with-fencing first, then Raft: agree, or go to
   Raft now, and if Raft, `openraft` or hand-rolled?
2. **Segment ownership split from shard ownership.** It changes the shard
   map's meaning (a shard can have several servers for disjoint segment
   ranges). Agree to that model before the map format changes.

## Order of work, once decided

1. Relay coordinator serving the node-facing surface over children, with
   composite `TermStats` and forwarded floors; the exactness test is the
   existing bitwise-equivalence suite run through one relay level.
2. Measured scan rate on the lease; the balancer's dry run.
3. Standby control plane with fencing.
4. Segment-subset ownership and the copy path.
5. Raft, when the tree has more than one relay in production.

## Response to the review, 2026-09-05

The [review](scale-out-coordination-review-2026-09-05.md) is accepted in
full. Choices it leaves open, decided here so the branches can start:

- **No relay heap.** `StartStreamSearch` carries no k, and the heap was an
  optimization. The relay forwards candidates and the parent's floor and
  nothing else; ties and the score scale are preserved.
- **Aggregation is outside the first relay scope.** When it enters, the
  relay forwards ordered leaf partials without folding them, so the root
  folds in shard order as today and the bits do not change. The variance
  counterexample becomes a test.
- **Statistics ceiling.** Checked sums with a refusal by name now. A wider
  contract adds `uint64` fields beside the `uint32` ones, never in place of
  them.
- **Epoch token.** A nonzero durable allocation bound to the recorded
  tuple (collection, child identities, incarnations, ranges, child epochs),
  translated per child, `FAILED_PRECONDITION` with the stale-epoch prefix on
  an unknown token, bounded retention. Not a hash.
- **Field capabilities.** The first relay requires homogeneous child field
  capabilities and refuses phrase and fused requests otherwise.
- **First relay topology.** One relay per placement leaf, children with
  contiguous slot ranges so `Health` stays representable, a dedicated
  collection endpoint per relay, separate signed UDP sessions per hop.
- **Scan measurement.** Encoded bytes processed are known from the index
  geometry (rows scanned times encoded row bytes), counted once per
  batched pass in the node's scan path with active scan time, so the
  provider is unchanged. Freshness travels with the rate (fields 7 to 9 on
  `NodeCapacity`); a stale rate makes its node unmeasured.
- **Device residency** is a capacity field (`NodeResidency`). A DEVICE node
  is excluded from every plan and every executor before any capacity logic
  runs, with the reason reported.
- **Balancer.** Whole-shard moves only, within a leaf's node set, bounded
  move budget, deterministic tie-break, `min_gain` validated in [0, 1],
  provenance (`control_revision`) on the plan, exclusions with reasons.
  Cluster trust, not a public route.
- **Control plane.** The single authority stays through relay development.
  Failover comes with a Raft library, never a standby promoted on node
  leases. The operator's two decisions (when Raft, and whether movable
  server collections may split segment ownership) remain open and block
  nothing in this scope.

The reserved contract on `feat/scale-out-reservation-2026-09` carries these
changes; `bandwidth-budget.md` is the measurement and balancer design.

### Raft ownership and review handoff

The foundations work owns the OpenRaft state machine, storage, transport and
recovery integration. Fable owns relay consumers of the published map.
[The Raft design](raft-control-design.md) is ready for Fable's review before
implementation, which follows the budget branch merge. It defines the complete
map envelope and distinguishes trusted control learners from narrower map
subscribers. The current single authority remains during relay development.
