# Scale-out coordination review, 2026-09-05

Review of the [proposal](scale-out-coordination.md) at main
`1a0d9461daca11b188338289c0756ad7db88fac0` and reservation
`a29beb9e55f60ae65e893a460cb36fd5125d98c7` on
`feat/scale-out-reservation-2026-09`. Both refs were verified on Forgejo and
GitHub. The additional identity-view work at `47182bc` was inspected separately;
it is not yet on main. This review does not merge the reservation or authorize
a fleet change.

## Verdict and work that can proceed

**Changes requested before freezing the contract.** A relay is feasible, but
the complete current node surface does not compose transparently through an
arbitrary tree. Keep the search-owned namespace `ai.protomolt.search`.

The relay implementation can start with an explicitly restricted, read-only
surface: forwarding vector candidates and parent floors, preserving global
IDs, bounding queues, propagating cancellation/deadlines, and requiring every
child to complete. Use a fixed, disjoint participant set and compatible provider
state. Refuse unsupported routes by name. Do not advertise full NodeService
compatibility, local relay heaps, recursive ingest, or aggregate equivalence yet.

The telemetry implementation can start independently. The balancer may start
as a whole-shard dry run after its measurement and eligibility semantics below
are defined. Executing moves, split segment ownership, and automatic control
failover remain separate gates. Phone-owned shards are ineligible for export,
replication, relocation, or segment copying, regardless of measured rate.

## 1. Which routes need more than composition?

### Vector and lexical candidates

`StreamSearch` can forward all qualifying child candidates and the parent's
monotone floor. However, `StartStreamSearch` has **no k field**
([search.proto](../proto/ai/protomolt/search/v1/search.proto),
`StartStreamSearch`). The proposed subtree top-k heap cannot derive its size
from this request. Defer that optimization or add an explicit, versioned heap
contract. A collapsed query needs k distinct parents, not k chunks; a floor
from duplicate chunks or overlapping replicas is not a valid parent cutoff.
Keep boundary ties and the original score scale. Do not prune an unfinished
hybrid computation using a floor from a different score or fusion stage.

BM25 must forward the **root's global** document counts, lengths, term
frequencies, field order and score stages unchanged, while translating the
per-target epoch claim as described below. Calling a relay's public
`Bm25Search`, `HybridSearch`, or `Query` and merging its final local rankings
does not implement those node contracts. Global RRF ranks and candidate pools
are root-level computations. Preserve hit identity and explain metadata rather
than reconstructing hits from only IDs and scores.

Child errors or missing terminal certificates fail the attempt. A restarted or
hedged relay must not continue a heap/floor derived from a different participant
set or generation. Separate UDP tokens and signed sessions are needed on each
hop; propagate meaning, not a datagram addressed to a different stream.

### Statistics and field capabilities

Term frequencies currently use `uint32` in `TermStatsResponse`, `FieldStats`,
and global BM25 request legs. The coordinator sums them into `Vec<u32>`
(`CoordinatorServiceImpl::body_stats` and `fused_stats` in
[coordinator.rs](../src/coordinator.rs)). Two valid child counts of
3,000,000,000 and 2,000,000,000 exceed that wire domain. This is an existing
collection-size ceiling that adding relays cannot remove. Use checked sums
and explicit refusal immediately; plan a compatible wider statistics contract
before claiming collections beyond that bound. Audit the associated scorer
types and other aggregate counters, not just the protobuf field spelling.

The typo rule is an OR across children's known flags, but phrase planning
also asks whether a field/positions exist **everywhere** (`known_shards`,
`positions_shards`, and phrase checks in `coordinator.rs`). One boolean
`FieldStats.known` cannot preserve both any-child and all-child knowledge.
Initially require homogeneous child field capabilities or refuse affected
phrase/fused requests; a general relay needs separate coverage information.

### Aggregations, dictionaries and bounded results

Integer counts, checked integer sums, extrema, histogram counts, and quantile
probe counts can combine over disjoint, consistently pinned inputs. Exact
cardinality requires unioning distinct values, never summing cardinalities.
Group keys and dictionary entries merge by value, not local ordinal.

**Floating-point aggregate reduction is not tree invariant.** The current
contract folds leaf partials in shard order (`AggMerge::fold` in
`coordinator.rs`; [aggregations.md](aggregations.md), section 1). Applying its
Chan/Welford merge to singleton leaves `[1, 1, 2, 1]` gives population variance
`0x1.7ffffffffffffp-3` in a flat fold and `0x1.8000000000000p-3` when the first
two and last two leaves are folded at relays first. Preserve ordered leaf
partials through a suitable envelope, or define and test a different numerical
contract. A combined `AggregatePartial` alone cannot promise the flat result's
bits. Compensated sums also require a reduction-order audit.

`ExpandTermPrefix` and `SuggestTerms` report exact counts but omit entries once
their bounds are exceeded. A relay cannot infer an exact distinct union from
two omitted overlapping dictionaries by adding their counts. Specify whether
budgets are per leaf, per subtree or per query; preserve overflow/refusal
semantics. The same issue applies to globally bounded group/distinct sets.

### IDs, follow-up fetches, health and browse

`GetDocuments`, `ResolveParents`, `FetchValues`, `Bm25Rescore`, vector rescoring,
and query-pool aggregation must route each original ID to the correct child
and preserve request/result association. Bitmap responses represent one
`base_label` plus one contiguous bit domain. A relay over sparse child ranges
needs bounded composition or a range-aware contract, not a potentially huge
zero-filled span.

`HealthResponse` describes one slot range and one WAL generation/watermark.
`product_label_ranges` in `coordinator.rs` derives one interval per endpoint
from `slot_offset` and `max(num_vectors, bm25_docs)` and rejects overlaps.
Summing counts while reporting the first child's base invents ownership for
gaps and loses later ranges. Do not fabricate a subtree WAL watermark.
Either restrict the first relay topology to representable ranges and supported
health uses, or introduce an explicit logical-shard/range capability.

Browse top-k is composable under the same total sort order and original IDs.
Pagination is a separate limitation: the current `Cursor` in
[query.rs](../src/query.rs) stores rank, score/sort keys and document ID; it
does **not** pin a topology or index generation. Do not claim that adding a
relay makes existing cursors safe across compaction or ownership changes.

### Placement, ingest and administration

`TopologyRoute.placement` is one leaf code, and topology validation requires
that code to name a leaf. It is not an arbitrary subtree interval. Start with
one relay per leaf or extend routing/pruning metadata before covering internal
subtrees. A subtree may be skipped only when no descendant can match.

Routed ingest currently calls child `NodeService.IngestMapped` with
`IngestMappedRequest` batches (`routed_ingest_mapped_bound` in
`coordinator.rs`), not child `SearchService.RoutedIngestMapped`. Recursive
ingest therefore needs a real adapter with pinned routing, response accounting
and retry behavior. Treat it as a later feature. Snapshot install/export,
WAL replay, compaction, calibration and backend administration are explicit
ownership operations, not generic reductions that a relay may impersonate.

## 2. Composite epochs and the stats cache

The cache itself uses equality, not ordering (`StatsCache::store` in
[stats_cache.rs](../src/stats_cache.rs)). It can accept an opaque nonzero
relay token; it does not require monotonicity to perform a lookup.

However, a hash of child epoch numbers alone is insufficient. Bind the token
to the collection, child identities, child incarnations/generations, selected
ownership ranges and each child's statistics epoch. Pin that exact mapping
through scoring and all related membership/rescore phases. Translate the
parent's token into the recorded child claims. Nodes enforce those claims
under the same read guard used for scoring (`ShardState::check_stats_epoch`
in [node.rs](../src/node.rs)). A separate health check followed by unclaimed
scoring has a mutation race.

An expired/unknown token or changed child must return the recognized
`FAILED_PRECONDITION` with the `stale stats epoch` prefix so the parent retries.
Never substitute zero, which explicitly disables checking. Do not reuse a
token after restart or on a replica for different state. Current leaf epochs
are process-local, so incarnation reuse needs attention there as well.
Bound the retained token mappings. A nonzero durable allocation tied to the
recorded tuple is one option; a hash requires a documented collision and
restart strategy. Merely making a counter monotone inside one process does
not solve these problems.

## 3. Authorization and collection addresses

Product grants key on principal, collection, workspace and action, with a
policy revision, not a network address
([authorization.rs](../src/authorization.rs)). Preserve that boundary and
stream revocation checks; never replace a user's decision with a relay's
cluster credential.

There **is** an address restriction in `CollectionSet::named`
([collections.rs](../src/collections.rs)): one node endpoint cannot appear
under two collections. Many node read messages have no collection selector.
A shared relay listener cannot silently multiplex several collections under
today's wire shape. Use a dedicated collection endpoint initially or introduce
authenticated resource routing before sharing one.

Node/control routes use cluster trust, and `ClusterControlService::membership`
requires a verified client certificate when configured. `PlanBalance` has that
same internal boundary, not public Admin-grant enforcement. Do not expose it
to public clients by assuming its collection string is authorization. A phone
bridge must allowlist query operations and apply its own result-release policy;
it must never become a generic proxy for NodeService administration.

## 4. Segment owners, identity and the catalog

The stable identity is collection + exact document key + version + optional
chunk ordinal. It contains no server address. The local `DocumentCatalog`
is one exclusive authority for the **whole collection**, not one database
per shard ([document-writes.md](document-writes.md)). Creating independent
catalog writers at relays would break conditional writes and operation-ID
deduplication. Server routing and atomic searchable publication are unfinished;
copying index segments does not copy or transfer that authority.

The physical query/control implementation currently has one primary and one
optional identical replica per `TopologyRoute`. Different segment owners are
not replicas and cannot occupy the existing replica slot. Reconciliation keys
replica records by `(shard_id, node_id)`; it has no segment ownership version.

`SegmentedShard::masked` is a **query pruning view**, not an ownership view
([segmented.rs](../src/segmented.rs)). `MaskedShard::doc_count` and
`total_doc_length` return whole-shard totals, heap/frozen data remain included,
and reads by document ID reach the whole shard. Summing several such views
double-counts statistics and can serve rows outside the purported ownership.
Build a separate serving subset with all stores and routes aligned.

Before activation, define an atomic ownership manifest covering logical shard,
index generation, immutable segment identities/ranges, and exactly one owner
for each frozen/tail range. Validate disjointness and complete coverage. Carry
the matching live bitmap/tombstone revision, analyzer/provider state, lineage,
columns, original source and identity metadata; an immutable segment alone
does not contain subsequent deletes. Retain old readers while pinned queries
finish. Compaction renumbers rows, so row intervals without a generation are
not durable ownership identifiers. Whole-shard snapshot APIs are reusable
transport material, not proof that subset install/cutover already exists.

The identity views on `47182bc` retain row-to-source metadata without retaining
index files. They do not pin liveness, policy, catalog heads, or a network
owner. Follow-up identity resolution must remain attached to the originating
query generation/session; fetching today's row after compaction can alias a
different document. Dense identity stream extensions remain pending and should
be coordinated with the relay protocol, not assumed present.

The existing [device-shard contract](device-shards.md) is binding here:
iOS/Android document, index, vector, WAL and snapshot bytes stay on the phone.
Exclude device-owned data from copy/move plans and executors before capacity
optimization. Losing a phone means unavailable coverage, not replica creation.
This restriction does not await the operator's server-segment decision.

## 5. Scan-rate measurement and the reservation

Lease renewal is a reasonable transport for a bounded measurement summary.
The claimed measurement is **not already available**: `ShardScanStats`,
`SCAN_COUNTERS` in [metrics.rs](../src/metrics.rs), and `RecentFigures` in
[diagnostics.rs](../src/diagnostics.rs) count chunks, candidates, floors and
segments, not scanned bytes or active scan time. Route latency includes work
other than scanning. Candidates depend on k and floor selectivity, so dividing
them or nominal collection bytes by request latency is not scan bandwidth.

Add measurement at the provider scan boundary. Define effective encoded bytes
processed versus physical memory/disk traffic, the eligible execution mode,
sample window/count, observation age and smoothing. Count a shared batch's
physical pass once; do not count it once per query. Keep queueing, backpressure,
filtered/pruned scans, cancellation, warm/cold residency and concurrent work
distinguishable. Scope the estimate to comparable provider/index settings.
Phones also need current availability and thermal/energy constraints. A scalar
is an estimate for a named workload, not a universal node capacity.

Before freezing the reserved messages:

- Keep `NodeCapacity.scan_bytes_per_second = 6`, the search namespace, and
  `PlanBalance` appended after `DescribeSchema` (route index 64; 65 entries).
  The `#[serde(default)]` correctly gives old stored capacities a zero value.
- Specify measurement freshness and applicability, either with additive sample
  metadata or a documented server-side contract that prevents stale rates from
  remaining eligible merely because leases renew. Zero remains unknown. Never
  produce infinity or a fabricated zero duration for an unknown rate.
- Define `NodeLoad.bytes` in the same units as the rate. Treat
  `seconds_before/after` as estimates. Account for aggregate load when several
  shards share a node; a faster measured scan does not establish spare disk,
  memory, ingest capacity or safe replica placement.
- Keep the first `BalanceMove` explicitly whole-shard. Validate collection,
  stable topology generation, eligibility/residency and failure domains. Add
  control/measurement provenance sufficient to explain/reject a stale plan,
  plus reasons for pinned/unmeasured/ineligible nodes. A future segment plan
  needs its own segment/generation identity; do not reinterpret this field set.
- Validate finite `min_gain` in a documented range, retain the zero-default
  semantics explicitly, and use deterministic tie-breaking and bounded move
  budgets. Hysteresis and copy cost belong in the execution policy.
- Replace references to nonexistent `docs/bandwidth-budget.md` with the
  proposal or create the actual design document before merging. Preserve
  the current named `UNIMPLEMENTED` response until a real dry run exists.

## Control-plane decision

Do not implement automatic standby promotion using the existing node leases
as its safety argument. Those leases are validated against each process's
private `StoredState`; `next_token` and `revision` also live there
([control_plane.rs](../src/control_plane.rs)). Two partitioned copies can
independently renew leases and issue conflicting decisions. Tailing a file and
waiting for lease expiry does not elect or fence a unique authority.

Recommendation: retain the single authority while building read-only relays,
then use an established Raft library for automatic failover. Manual recovery
requires the former authority to be stopped/fenced and an explicit recovery
point. If HA is required in this phase, design the replicated state machine
now. Avoid a hand-written consensus implementation. Raft's election and commit
rules rely on quorum agreement, not merely a timeout
([Raft paper](https://raft.github.io/raft.pdf)). OpenRaft is a candidate library,
but still requires application storage, state-machine, network and recovery
integration ([OpenRaft guide](https://docs.rs/openraft/latest/openraft/docs/getting_started/index.html)).

Operator decisions are pending: when to introduce Raft, and whether movable
server collections may adopt split segment ownership. Neither decision is
needed to start the restricted query relay or scan instrumentation.

## Acceptance and handoff

The relay gate must compare flat, one-level and two-level execution with fixed
leaf identities, under permuted arrival order and topology grouping. Include
ties, shared parents, partial fields/positions, epoch changes and restart,
child errors/cancellation, bounded dictionaries, sparse IDs, follow-up fetches,
and floating-point aggregate counterexamples. Prove each enabled route; an
unsupported route must refuse. Add a synthetic statistics test beyond u32
without building billions of documents.

The balance gate needs unknown/stale samples, batched scans, shared-node load,
eligibility, no-capacity destinations and deterministic plans. Before any data
movement, add generation/tombstone/copy-failure/compaction races and a test
proving phone-owned bytes can never enter export or placement actions. HA needs
partition, stale leader, restart and acknowledged-state recovery tests.

Validation of the reserved branch: `cargo check --locked --tests --examples`
passed in an isolated detached worktree. The `stats_cache`, `collections` and
`aggregate` integration suites passed all 27 tests; the `control_plane::`
library tests passed all 9 tests. The numeric counterexample above was also
reproduced with the current merge equations. These are focused review checks,
not a full-suite run. No relay, scan measurement,
balancer execution, consensus or segment transfer implementation was tested by
this review; none exists on the reviewed reservation.
