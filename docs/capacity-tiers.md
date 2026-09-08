# Capacity tiers over declared hash partitions

Status: design for operator review, 2026-09-07. Nothing in this document
is built. It is the contract to review before any ownership change is
implemented: it names partitions, observations, the plan, the move
shape, and the refusal behavior, and it embeds draft protocol blocks as
text. No field number below is allocated; every one is marked DRAFT and
becomes contract only when the number is reserved in
`proto/ai/protomolt/search/v1/search.proto`.

The documents this one extends: the measurement and the dry run in
[bandwidth-budget.md](bandwidth-budget.md), the predicate tree and its
codes in [placement.md](placement.md), the declared hash column and its
fingerprint in [derived-columns.md](derived-columns.md), and the durable
control plane in [cluster-control.md](cluster-control.md).

## 1. Terms

- **Declared hash partition** (here, *partition*): the set of rows
  whose declared hash column holds one ordinal, e.g. every row with
  `key_bucket == 7` under the declaration
  `key_bucket = hash.fnv64(stable_key()) % 64u`. A partition is a row
  predicate, not a file, a shard, or a node assignment.
- **Observation**: a node's measured statement about one partition it
  holds, with a freshness. Measured, never declared, the rule the scan
  rate already follows.
- **Capacity tier** (here, *tier*): a named residency policy —
  required node residency kind, minimum replica count, and an
  observed-workload band — that a partition is planned into. A tier is
  a policy record, not a place.
- **Plan**: the output of the dry run. It computes, and never executes,
  exactly as `PlanBalance` does.
- **Residency floor**: the minimum live copies a tier guarantees. A
  plan that would take a partition below its floor at any point of its
  own move sequence is refused, and so is each move in it.

Shard keeps its meaning (the stable-key hash split of the topology);
leaf keeps its meaning (a placement-tree node with a node set). A
partition is orthogonal to both, and §2 and §7 say how.

## 2. Partition identity

A partition is identified by the triple:

```
(derived_fingerprint, column name, bucket ordinal)
```

- **derived_fingerprint** is the SHA-256 over the canonical
  `DerivedColumns` encoding, the fingerprint the store file, every
  sealed segment, and the WAL manifest already carry
  ([derived-columns.md](derived-columns.md)). The expression text is in
  that encoding, so the modulus is inside the fingerprint:
  `% 64u` and `% 128u` are different declarations, different
  fingerprints, and therefore different partition sets. A partition set
  never re-buckets in place; a new modulus is a new identity and a
  rebuild, which is the derived-column rule applied to tiers.
- **column name** is carried beside the fingerprint although the
  fingerprint already covers it, so a refusal text and an operator's
  log name the column instead of a digest alone. Two columns of one
  declaration are two partition sets.
- **bucket ordinal** is the `u64` value the column stores. Ordinals are
  dense in `[0, n)` only by the expression's own arithmetic; the
  declaration does not promise every ordinal occurs.

Why not node-relative identity. The alternative — identifying a
partition by the shard or node that holds it — fails the two events
this contract exists for: a reshard re-splits shards, and a node
replacement re-homes them, and neither event changes which rows carry
`key_bucket == 7`. Identity is a function of the declaration, which is
why it survives both.

**Membership is a pure function of the stored column.** A row is in
partition `(f, c, b)` exactly when its stored value under `c` is `b`,
written by the node at ingest under declaration `f`. Any replica
verifies membership locally: it holds the same fingerprint on attach
(the store refuses a mismatch by name already), reads the stored
column, and evaluates one equality. No coordinator, no routing table,
and no replay is involved. The count of a partition on a shard is the
filtered count of `c == b`, the exact primitive `PlanPlacement`
already fans out, so a partition's size is auditable at any replica
without moving a row.

**Relation to the WAL bucket hash.** The WAL bucket hash is a
different contract ([derived-columns.md](derived-columns.md) says so):
it hashes little-endian row ids and selects high bits, and a compaction
renumbers rows in WAL-bucket order, so WAL bucket membership is not
stable across compaction and identifies nothing a tier can keep. The
declared hash column is stable precisely because it hashes the stable
key, which a compaction, a reshard, and a node replacement all
preserve. The two never mix: a partition observation or move that
arrives keyed by WAL bucket is refused by name, and nothing here
changes the WAL's own bucketing.

**Relation to placement-leaf codes.** The placement column is an `i64`
prefix code assigned by a predicate tree; the partition column is a
`u64` hash ordinal assigned by a hash. They are different columns with
different writers and different invariants, and one declaration may
carry both. A partition's rows may span several leaves (the tree split
on `year`, the hash did not), and a leaf holds many partitions. A
partition predicate is a point range on its column, so it composes
with the pruner's rules the way any equality does; nothing in the leaf
bounds changes. Where the two meet is movement scope: a tier move, like
a `PlanBalance` move, stays inside the placement leaf's node set that
holds the partition's rows, and a partition whose rows span several
leaves is planned per leaf, one move list per (partition, leaf) pair —
the leaf's node set is the pool, and the plan never proposes a
destination outside it.

## 3. Capacity observations

Each node reports, per partition it holds rows of, one observation. The
carrier is `ReportShard` (per shard, so per partition-in-shard), the
route whose discipline the scan rate already uses on the lease; the
authority aggregates to per (partition, node).

```proto
// DRAFT. Not allocated. One node's measured statement about one
// partition in one shard. Carried as
// `repeated PartitionObservation partitions` on ReportShardRequest
// (DRAFT field, number unassigned).
message PartitionObservation {
  PartitionIdentity partition = 1;      // DRAFT
  // Rows and resident encoded bytes of the partition in this shard.
  uint64 rows = 2;                      // DRAFT
  uint64 resident_bytes = 3;            // DRAFT
  // Attributed scan throughput for scans confined to this partition,
  // same window discipline as NodeCapacity.scan_bytes_per_second.
  // Zero means unknown; a scan that touched other partitions is not
  // attributed here (see the note below).
  uint64 scan_bytes_per_second = 4;     // DRAFT
  // Queueing: the p50 and p99 queue wait the shard's scans of this
  // partition observed inside the window, microseconds. Zero with
  // samples zero means unknown, never "fast".
  uint64 queue_wait_p50_us = 5;         // DRAFT
  uint64 queue_wait_p99_us = 6;         // DRAFT
  // Warmth: when this shard last served a scan touching the partition,
  // unix ms. Zero means never observed since the node started; warmth
  // is a last-touch time, not a cache probe.
  uint64 last_scanned_unix_ms = 7;      // DRAFT
  // Freshness triple, same meaning as the scan rate's: the newest
  // sample's observation time, the sample count, the window length.
  uint64 observed_unix_ms = 8;          // DRAFT
  uint32 samples = 9;                   // DRAFT
  uint64 window_ms = 10;                // DRAFT
}

// DRAFT. Identity as §2 defines it.
message PartitionIdentity {
  string derived_fingerprint = 1;       // DRAFT
  string column = 2;                    // DRAFT
  uint64 bucket = 3;                    // DRAFT
}
```

Freshness and epoch discipline follow the scan-rate lease reporting:
zero rate is unknown, never zero; a window under the minimum sample
count reports unknown; the authority judges staleness from
`observed_unix_ms` against a policy bound, never from lease recency.
One addition: the authority keeps an **observation epoch**, a monotone
counter it advances when the observation set it would plan from
changes (a report lands, an observation crosses staleness, a lease
appears or expires). The epoch is the plan's input identity in §5.

**Stale means refused, not skipped.** This is deliberately stricter
than `PlanBalance`, which lists a stale node in `excluded` and plans
without it. A tier decision re-homes bytes on the strength of observed
workload; planning it from a stale observation moves bytes toward a
rate that may no longer exist. The rule: if any observation the policy
requires is stale or unknown — for a partition the policy covers, from
a node in that partition's pool — the plan refuses by name, e.g.
`plan_tiers: partition key_bucket=7 on node pi5v1 was last observed
812s ago, past the 600s bound`. A policy may narrow the set it covers
(only partitions above a resident-byte floor, only named tiers); what
it covers must be fresh. A plan is never computed on stale data, and
there is no partial plan.

**Honest scope, same as the rate's.** A partition observation
describes this node's provider under its recent workload for these
rows. It is not disk, memory, ingest, or replica capacity, and the
plan's fields say so by name.

## 4. Deterministic planning

The plan is a pure function of three inputs, and names all three:

```
plan = f(topology generation, observation set, policy)
```

Same inputs, same plan, bitwise. The inputs are made canonical before
the function runs, so map iteration order is never an input:

- partitions are ordered by (`derived_fingerprint` bytes, column name,
  bucket ordinal);
- nodes are ordered by node id;
- within a step, the candidate partition is the one whose move lowers
  the objective most, ties broken by partition identity, then by
  destination node id, then by tier name — the same shape as
  `PlanBalance`'s node-id-then-shard-id rule;
- floating-point estimates are computed in a fixed evaluation order and
  compared exactly only to detect a tie, which the deterministic
  tie-break then resolves; an estimate is never a map key.

The response carries the three inputs' identities (§5) so a receiver
checks the plan against what it would have planned from, not against
the moves.

```proto
// DRAFT. Not allocated. On ClusterControl, beside PlanBalance:
//   rpc PlanTiers(PlanTiersRequest) returns (PlanTiersResponse);  // DRAFT
message PlanTiersRequest {
  string collection = 1;                // DRAFT
  // The policy: tiers in precedence order, with each tier's residency
  // floor and workload band. Absent selects the control plane's stored
  // policy; the response always carries the policy identity it used.
  TierPolicy policy = 2;                // DRAFT
  uint64 max_moves = 3;                 // DRAFT; 0 selects the policy default
  uint64 max_observation_age_ms = 4;    // DRAFT; 0 selects 10 minutes
}

message TierPolicy {                    // DRAFT
  repeated CapacityTier tiers = 1;      // DRAFT
  // SHA-256 over the canonical encoding, the policy's identity in
  // every plan and refusal. The authority computes it; a client
  // carrying one that differs from its tiers is refused naming both.
  string policy_fingerprint = 2;        // DRAFT
}

message CapacityTier {                  // DRAFT
  string name = 1;                      // DRAFT, e.g. "hot", "warm", "archive"
  NodeResidency residency = 2;          // DRAFT; SERVER or unspecified-refused
  uint32 min_replicas = 3;              // DRAFT; the residency floor, >= 1
  // The observed band this tier covers: scans per second per resident
  // byte, [lo, hi), plus a warmth bound. A partition outside every
  // band is reported unclassified and is not moved.
  double scans_per_byte_lo = 4;         // DRAFT
  double scans_per_byte_hi = 5;         // DRAFT
  uint64 max_seconds_since_scan = 6;    // DRAFT; 0 = no warmth bound
}

message TierMove {                      // DRAFT
  PartitionIdentity partition = 1;      // DRAFT
  string from_tier = 2;                 // DRAFT; "" when unclassified
  string to_tier = 3;                   // DRAFT
  string leaf = 4;                      // DRAFT; the bounding placement leaf
  uint64 bytes = 5;                     // DRAFT
  // The revision triple of §5, repeated per move so a move checked
  // alone refuses correctly.
  uint64 topology_generation = 6;       // DRAFT
  uint64 observation_epoch = 7;         // DRAFT
  string derived_fingerprint = 8;       // DRAFT
}

message PlanTiersResponse {             // DRAFT
  uint64 topology_generation = 1;       // DRAFT
  uint64 control_revision = 2;          // DRAFT
  uint64 observation_epoch = 3;         // DRAFT
  string derived_fingerprint = 4;       // DRAFT
  string policy_fingerprint = 5;        // DRAFT
  repeated TierMove moves = 6;          // DRAFT
  // Per partition: the tier the observations classify it into, before
  // any move, with the observation figures the plan saw. Unclassified
  // partitions appear here with an empty tier and the reason.
  repeated PartitionPlacement placements = 7; // DRAFT
}
```

**Proof obligation.** The contract's test, not its hope: two
independent planner instances — in the test, two control planes built
from the same stored state — given the same topology generation,
observation set, and policy must emit byte-identical serialized
responses. The test holds a golden plan for a fixed input, permutes the
insertion order of nodes, partitions, and observations, and asserts
the bytes do not change. A plan that depends on iteration order, wall
clock beyond the declared staleness bound, or process identity fails
it. This is the tier-plan analogue of the placement suite's
pruning-on/pruning-off answer identity.

## 5. Revision checks

Every plan and every move carries four identities: the topology
generation, the control revision, the observation epoch, and the
declaration fingerprint (plus the policy fingerprint on the plan). A
receiver compares what a message carries against what it holds, per
field, and refuses a mismatch by name. The refusal text names the
route, the field, the carried value, and the local value, in the shape
the plane already uses:

```
plan_tiers: the request's observation epoch 118 is not this authority's 121
plan_tiers: shard s6 carries derived_fingerprint 9f2c…, the plan's is 41aa…; a rebuild, not a reload
apply_tier_move: move carries topology_generation 41, this node serves 39
apply_tier_move: policy fingerprint 7be1… is not the stored policy's c304…
```

A receiver never reinterprets a plan from another revision: a
generation behind or ahead is a refusal, not a partial application,
and a fingerprint mismatch is a refusal naming the fix, the same
stance attach takes on a store written under another declaration. The
observation epoch is what makes a plan stale between issue and use:
the authority advanced its observation set since it planned, so the
plan's premise is gone.

## 6. Residency constraints

- **Device exclusion by declaration, extended.** The `PlanBalance`
  rule — a `DEVICE` node is excluded from every export, replication,
  relocation, and segment copy before any capacity logic runs, with
  the exclusion reported — applies to tiers unchanged and per
  partition: a device node is never a move destination and never a
  tier's node, whatever the partition's observed rate there. An
  `UNSPECIFIED` residency is reported and never assumed movable, and a
  tier whose `residency` is `DEVICE` is refused at policy validation
  by name.
- **The residency floor is per tier.** `min_replicas` is the count of
  ready copies the tier guarantees. A plan validates each move against
  the floor the partition is leaving and the floor it is entering.
- **Never below the floor mid-move.** A tier move is copy-before-drop,
  sequenced inside the plan: the copy at the destination must exist
  and be ready before the plan's own move list drops the source copy.
  A plan whose move sequence would leave a partition below its floor
  at any intermediate step — including the step where the source's
  last copy is the only ready one — is refused, naming the partition,
  the tier, and the floor: `plan_tiers: tier "archive" floor is 2
  copies, this move sequence leaves partition key_bucket=7 with 1 at
  step 3`. A floor the cluster cannot currently satisfy (fewer
  eligible nodes than `min_replicas` in the leaf's pool) is reported,
  not planned around.
- **Failure domains carry over.** The per-move rule from the balance
  dry run — a destination in the failure domain of one of the
  partition's ready copies is skipped — applies unchanged, checked at
  every intermediate step of the sequence, not only at the endpoints.

## 7. Tiers

A capacity tier is a named residency policy (§4's `CapacityTier`):
which residency kind may hold the partition, how many copies must
exist, and which observed workload band the tier covers. Tiers are
ordered by precedence; the classification of a partition is the first
tier whose band its observation falls in. The names are the operator's
("hot", "warm", "archive" are examples, not vocabulary); the semantics
are the floor and the band, nothing else.

**A partition changes tier only by a planned move.** Classification is
derived from observations and reported on every plan; residency is
changed only by the move list a plan emits and an operator or executor
applies under §5's revision checks. There is no implicit tier change:
an observation that reclassifies a partition changes its reported
tier, and moves nothing. Conversely a partition never drifts into a
tier because a node happened to hold it — tier is a property the plan
assigns, and the plan is the only writer of that assignment.

**Interaction with the placement tree.** The leaf's node set remains
the pool every move stays inside, as it does for `PlanBalance`; a tier
further narrows the pool by residency kind and policy, never widens
it. A partition spanning leaves is planned per (partition, leaf), so
the same bucket ordinal can sit in "warm" in one leaf and "archive" in
another — the classification is over the rows the leaf holds, and the
plan says which leaf each move is bounded by. A leaf edit or a new
tree is a new topology generation, which §5 turns into refusals of
every outstanding tier plan: a re-placement and a tier move never
compose silently.

**Interaction with pruning.** A partition is a logical partition in
the placement document's sense: a predicate, costing a bitmap,
changing no file. Shard and segment pruning are untouched — the
partition column's equality is one more prunable predicate where a
segment's summary bounds it, and `placement == code` bounds are
unaffected. Nothing in a tier plan marks a shard skippable; a pruned
shard still contributes its observation and its resident bytes to the
plan, because pruning is a per-query decision and capacity is not.

## 8. What this contract does not do

- **No ownership change.** Who serves a partition's rows stays the
  topology's decision (shards and, where it lands, segment-subset
  ownership). This contract names partitions, classifies them, and
  plans their tier moves as a dry run, exactly the stance
  `PlanBalance` takes for whole shards. The map flip, the copy path,
  and the cutover are the separate execution gate that
  [scale-out-coordination.md](scale-out-coordination.md) already
  fences off.
- **No automatic execution.** No reconciler applies a tier plan. A
  plan is computed on request and carried under its revision checks;
  anything that one day applies it is a client of §5, not a part of
  this contract.
- **No consensus.** The single control authority computes plans. Raft
  and standby-with-fencing are the control-plane track
  ([raft-control-design.md](raft-control-design.md)) and are unchanged
  here; the determinism obligation of §4 is what makes a later
  replicated authority able to check its members' plans against each
  other, and it is specified now for that reason.
- **No new hash, no re-bucketing.** The partition function is the
  declared column's own expression. A changed modulus is a new
  declaration and a rebuild, never a migration in place.
- **Dependencies on the foundations track.** The partition column must
  be a declared derived column under the in-flight descriptor-mapping
  and authorization work on `feat/search-foundations`: stable keys
  must exist on every row the expression reads (a column reading the
  stable key already refuses a document without one), and the
  disclosure rules of [derived-columns.md](derived-columns.md) apply
  to every observation, plan, and refusal text that names the column —
  a plan is cluster-trust, like the other control routes, and is not a
  public route. Observations name partitions, never stable keys or
  document identities.

## 9. Review checklist

The questions the operator answers before implementation:

1. Is the triple (derived_fingerprint, column, bucket ordinal) the
   partition identity, or should identity also bind the topology
   generation the partition set was first planned under? (§2 argues it
   must not: resharding is the event identity must survive.)
2. Is per-(partition, shard) reporting on `ReportShard` the right
   carrier, or do observations belong on the lease like the node scan
   rate? The lease is per process; the partition is per shard.
3. The scan-rate window today is one per process, and per-partition
   scan attribution exists only for scans confined to one partition.
   Is that attribution rule acceptable for v1, or must the observer
   record which partitions each scan touched?
4. Is refuse-on-stale (§3) right, where `PlanBalance` skips-and-plans?
   The argument is that tier moves re-home bytes on observed workload;
   the cost is that one stale node blocks every plan.
5. Are the tier band's units — scans per second per resident byte,
   plus a warmth bound — the signal to classify on, or should the band
   be the scan rate alone?
6. Is per-(partition, leaf) planning the right movement scope, given
   the leaf's node set is the pool, or should a tier name its own node
   subset inside the leaf in the policy?
7. The residency floor is per tier. Should any floor be per
   (partition, tier), e.g. a named partition pinned to three copies
   regardless of band?
8. The determinism test asserts byte-identical plans across planner
   instances. Is the golden-plan fixture maintained per release, or
   regenerated and only the equality assertion kept?
9. Which refusal texts of §5 must the executor's log preserve
   verbatim, for the fleet runbook to diagnose a stale plan?
10. The policy fingerprint identifies the tier policy the way the
    derived fingerprint identifies the declaration. Should the policy
    live in the shard map beside `[placement]` and `[[derived]]`, so
    one file carries all three identities?
