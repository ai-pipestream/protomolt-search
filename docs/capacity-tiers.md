# Capacity tiers over declared hash partitions

Status: amended design for operator review, 2026-09-08. The 2026-09-07
draft was reviewed; this revision applies the review's corrections:
resource-scoped identity, an observation signal that the reports
actually carry, a frozen planning snapshot, a canonical policy encoding
with pinned fixtures, an explicit advisory/execution boundary, and
worked examples with literal inputs and expected outputs (§10). Nothing
in this document is built. It is the contract to review before any
ownership change is implemented: it names partitions, observations, the
plan, the move shape, and the refusal behavior, and it embeds draft
protocol blocks as text. No field number below is allocated; every one
is marked DRAFT and becomes contract only when the number is reserved in
`proto/ai/protomolt/search/v1/search.proto`.

The documents this one extends: the measurement and the dry run in
[bandwidth-budget.md](bandwidth-budget.md), the predicate tree and its
codes in [placement.md](placement.md), the declared hash column and its
fingerprint in [derived-columns.md](derived-columns.md), the durable
control plane in [cluster-control.md](cluster-control.md), and the
authority boundary in
[raft-control-design.md](raft-control-design.md#single-authority-convergence-boundary-2026-09-08).

## 1. Terms

- **Declared hash partition** (here, *partition*): the set of rows of
  one collection whose declared hash column holds one ordinal, e.g.
  every row with `key_bucket == 7` under the declaration
  `key_bucket = hash.fnv64(stable_key()) % 64u`. A partition is a row
  predicate, not a file, a shard, or a node assignment.
- **Fragment**: a partition bounded to one placement leaf at one
  topology generation — the unit a move copies. A partition spanning
  leaves is one partition and several fragments.
- **Observation**: a node's measured statement about one
  partition-in-shard it holds, bound to the reporting process's
  incarnation and the shard's source generation, with an explicit
  window. Measured, never declared, the rule the scan rate already
  follows.
- **Capacity tier** (here, *tier*): a named residency policy —
  required node residency kind, minimum replica count, and an
  observed-workload band — that a partition is planned into. A tier is
  a policy record, not a place.
- **Plan**: the output of the dry run. It computes, and never executes,
  exactly as `PlanBalance` does. In this revision the plan is
  *advisory only*: no execution route exists, and §6 lists what an
  execution contract must add before one may.
- **Residency floor**: the minimum live copies a tier guarantees,
  counted as *complete replicas* (§2). A plan that would take a
  partition below its floor at any point of its own move sequence is
  refused, and so is each move in it.

Shard keeps its meaning (the stable-key hash split of the topology);
leaf keeps its meaning (a placement-tree node with a node set). A
partition is orthogonal to both, and §2 and §8 say how.

## 2. Partition, fragment, and copy identity

A partition is identified inside an explicit resource by the quintuple:

```
(workspace, collection, derived_fingerprint, column name, bucket ordinal)
```

- **workspace and collection** bind the resource. Two collections with
  byte-identical declarations produce byte-identical
  `derived_fingerprint` values and do **not** authorize combining their
  rows: the triple `(fingerprint, column, bucket)` is a partition
  identity only inside one (workspace, collection). Every request,
  stored observation key, response, and plan digest carries the pair,
  and a message whose pair does not match the receiver's resource is
  refused by name. (Example E1 works this case literally.)
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

**Fragment identity.** Movement never names a bare partition; it names
a fragment:

```
fragment = (partition, topology_generation, leaf)
```

The topology generation binds the tree the leaf belongs to; a leaf
label or a network address alone is not a stable owner and never
appears in an identity. A partition whose rows span several leaves is
planned per fragment, one move list per (partition, leaf) pair — the
leaf's node set is the pool, and the plan never proposes a destination
outside it (§8).

**Copy identity.** Each concrete copy of a fragment is identified by:

```
copy = (fragment, node_id, process_incarnation, storage_incarnation)
```

- **process_incarnation** is the 128-bit identifier the node process
  mints at startup and presents on registration; a restart is a new
  incarnation.
- **storage_incarnation** is the 128-bit identifier the node mints when
  it installs one copy of the fragment's bytes (ingest, transplant, or
  replica bootstrap); it survives process restart attached to the
  bytes' manifest and dies with those bytes. A node that re-ingests or
  re-bootstraps the same fragment mints a new storage incarnation, so a
  stale plan can never address bytes installed after the plan's
  snapshot.

**Complete replica, defined.** One *complete replica* of fragment F at
source version V is a copy whose verified coverage evidence — the
manifest digest and per-section integrity proof over F's full row set
at V — matches V, held by a distinct eligible owner in a distinct
failure domain. Consequences:

- Two nodes holding disjoint rows of one bucket are two **partial
  copies**, together zero complete replicas; a floor counts only
  complete replicas (example E2).
- Two relay routes to the same owner are one copy, not two; relays are
  transport, not residency.
- Node `ready` flags and row counts alone do not establish a complete,
  matching copy: the evidence is the coverage proof at the source
  version, the same stance replica bootstrap already takes.
- Replica floors therefore need coverage and source-version evidence
  for the same fragment, from unique eligible owners in unique failure
  domains. A dry run states these as captured-state assumptions (§7);
  execution, when it exists, must revalidate them against the
  foundations track's verified owner/readiness contract before any
  drop.

Why not node-relative identity. The alternative — identifying a
partition by the shard or node that holds it — fails the two events
this contract exists for: a reshard re-splits shards, and a node
replacement re-homes them, and neither event changes which rows carry
`key_bucket == 7`. Identity is a function of the declaration and the
resource, which is why it survives both.

**Membership is a pure function of the stored column.** A row is in
partition `(ws, coll, f, c, b)` exactly when its stored value under `c`
is `b`, written by the node at ingest under declaration `f`. Any
replica verifies membership locally: it holds the same fingerprint on
attach (the store refuses a mismatch by name already), reads the
stored column, and evaluates one equality. No coordinator, no routing
table, and no replay is involved. The count of a partition on a shard
is the filtered count of `c == b`, the exact primitive `PlanPlacement`
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
bounds changes.

## 3. Capacity observations

Each node reports, per partition-in-shard it holds rows of, one
observation per window. The carrier is `ReportShard` (per shard, so per
partition-in-shard), the route whose discipline the scan rate already
uses on the lease; the authority aggregates to per (partition, node)
and per partition under the rules below.

```proto
// DRAFT. Not allocated. One node's measured statement about one
// partition in one shard. Carried as
// `repeated PartitionObservation partitions` on ReportShardRequest
// (DRAFT field, number unassigned).
message PartitionObservation {
  PartitionIdentity partition = 1;      // DRAFT; includes workspace+collection
  ReporterIdentity reporter = 2;        // DRAFT
  ShardRef shard = 3;                   // DRAFT
  // Rows and resident encoded bytes of the partition in this shard.
  uint64 rows = 4;                      // DRAFT
  uint64 resident_bytes = 5;            // DRAFT
  // The band signal's numerator: scans served by this reporter that
  // were confined to this partition, inside the window below. Zero is
  // a measured value (measured idle), not unknown; unknown is the
  // absence of a current report, never a zero.
  uint64 scans_observed = 6;            // DRAFT
  // Attributed payload bytes of those scans, same window; diagnostic,
  // never an input to classification.
  uint64 scan_bytes = 7;                // DRAFT
  // Queueing: the p50 and p99 queue wait this shard's scans of this
  // partition observed inside the window, microseconds. Unknown when
  // samples is zero; percentiles are per-reporter values and are never
  // merged across reporters (see the aggregation rules below).
  uint64 queue_wait_p50_us = 8;         // DRAFT
  uint64 queue_wait_p99_us = 9;         // DRAFT
  uint32 samples = 10;                  // DRAFT; queue-wait sample count
  // Warmth: when this reporter last served a scan touching the
  // partition, unix ms. Zero means never observed since this
  // incarnation started; warmth is a last-touch time, not a cache
  // probe.
  uint64 last_scanned_unix_ms = 11;     // DRAFT
  // The exact window: [window_start_unix_ms, window_end_unix_ms).
  // window_end is the observation time; both are the reporter's clock.
  uint64 window_start_unix_ms = 12;     // DRAFT
  uint64 window_end_unix_ms = 13;       // DRAFT
}

// DRAFT. Identity as §2 defines it: resource-scoped.
message PartitionIdentity {
  string workspace = 1;                 // DRAFT
  string collection = 2;                // DRAFT
  string derived_fingerprint = 3;       // DRAFT
  string column = 4;                    // DRAFT
  uint64 bucket = 5;                    // DRAFT
}

// DRAFT. A node process, pinned to one run of it.
message ReporterIdentity {
  string node_id = 1;                   // DRAFT
  bytes process_incarnation = 2;        // DRAFT; 16 bytes, minted at startup
}

// DRAFT. A shard at one source generation.
message ShardRef {
  string shard = 1;                     // DRAFT
  uint64 source_generation = 2;         // DRAFT
}
```

**The signal the band consumes.** The tier band is scans per second
per resident byte, and the observation now carries its inputs
directly: `scans_observed` over the explicit window, and
`resident_bytes`. Classification computes the rate in fixed point
under §5's rules. The v1 attribution rule is the scan rate's existing
one: a scan is attributed to a partition only when it was confined to
that one partition; a scan that touched several partitions is
attributed to none of them and appears only in the node-level rate.

**Zero, idle, unknown — three different values.**

- `scans_observed == 0` in a current report is **measured idle**: the
  reporter was alive, the window is fresh, and nothing scanned the
  partition. Its rate is exactly 0 and it classifies into a band whose
  `lo` is 0.
- **Unknown** is the absence of a current report from the committed
  owner's current incarnation. Unknown is never rendered as zero, never
  classifies, and under the coverage rule below refuses the plan.
  (Example E3 works the pair literally.)
- `resident_bytes == 0` with `rows > 0`, or `scans_observed > 0` with
  `resident_bytes == 0`, is an impossible state — a scan of zero
  resident bytes is attribution corruption — and the report is refused
  by name. A shard holding no rows of a partition sends no report for
  it at all.

**Report binding.** The stored observation key is the full tuple
`(workspace, collection, derived_fingerprint, column, bucket, shard,
source_generation, node_id, process_incarnation)`. Rules:

- **Restart supersedes.** When node N's incarnation J lands its first
  report, every stored observation keyed to `(…, N, J′)` with `J′ ≠ J`
  is dropped in the same committed transition. An old incarnation's
  freshness never counts, and a report from a superseded incarnation is
  refused by name (example E5).
- **Reshard refuses.** A report keyed to a source generation behind
  the committed one is refused by name; it is never silently absorbed
  into the new generation's set.
- **Retries are idempotent; conflicts refuse.** A report whose key and
  window match a stored report: identical payload is a no-op (the set
  is unchanged, the epoch does not advance); a differing payload under
  the same key and window is refused by name — the reporter must move
  to a new window (example E6).
- **Overlapping windows.** One stored observation per key: the one
  with the greatest `window_end`. A report whose window overlaps but is
  not equal to the stored one supersedes it if its `window_end` is
  greater, and is discarded as superseded otherwise. Windows are
  half-open, so disjoint windows never overlap.

**Aggregation from partition-in-shard to partition.** The planner's
partition figures come from the stored per-shard reports under
coverage and deduplication rules, not from any merged summary:

- **Rows and resident bytes** sum over the reports of each shard's
  *committed owner* only. Shards partition the rows, so owner reports
  are disjoint and the sum is exact coverage; a replica's report is
  coverage evidence for §2's replica test and is never added again.
  If the committed owner of a covered shard has no current report, the
  partition's bytes are unknown, not zero.
- **Scans** sum over every current reporter (owner and serving
  replicas): each reported scan happened once, at that node.
- **Warmth** is the maximum `last_scanned_unix_ms` over current
  reporters; zero participates as never.
- **Percentiles are never aggregated.** Averaging or adding p50/p99
  values across reporters is not a percentile of anything, so the plan
  keeps them per reporter and never classifies on them; they appear in
  the response's placements as per-reporter diagnostics. A future
  mergeable distribution (a sketch with a stated merge rule) may
  replace them without changing the rest of this contract.

**Freshness and the observation epoch.** The authority judges
staleness from the frozen planning instant (§4), never from lease
recency: an observation is stale when `planning_instant −
window_end_unix_ms > max_observation_age_ms` (strictly greater; the
boundary is fresh — example E4), with checked arithmetic and the
clock-skew refusal of §4. The authority keeps an **observation epoch**,
a monotone counter that advances only on committed transitions that
change the stored set: a report lands and changes it, a restart drop,
a reshard refusal does *not* (the set is unchanged), and a committed
expiry transition. Expiry transitions are ordered control events the
authority commits from its scheduler when an observation crosses the
staleness bound; they are persisted in order wherever they affect
committed state. Reading the wall clock — including to plan — never
advances the epoch and never mutates the stored set.

**Stale means refused, not skipped.** This is deliberately stricter
than `PlanBalance`, which lists a stale node in `excluded` and plans
without it. A tier decision re-homes bytes on the strength of observed
workload; planning it from a stale observation moves bytes toward a
rate that may no longer exist. The rule: if any observation the policy
requires is stale or unknown — for a partition the policy covers, from
the committed owner or a serving replica in that partition's pool —
the plan refuses by name, e.g.
`plan_tiers: partition key_bucket=7 on node pi5v1 was last observed 812s ago, past the 600s bound`.
A policy may narrow the set it covers (only partitions above a
resident-byte floor, only named tiers); what it covers must be fresh.
A plan is never computed on stale data, and there is no partial plan.

**Honest scope, same as the rate's.** A partition observation
describes this node's provider under its recent workload for these
rows. It is not disk, memory, ingest, or replica capacity, and the
plan's fields say so by name.

## 4. Deterministic planning over a frozen snapshot

The plan is a pure function of a frozen input snapshot, and the
response names the snapshot in full:

```
plan = f(plan context)        where the context is:
  ( planning_instant_unix_ms,
    resolved limits,                       # every one, defaults included
    authority identity + incarnation,
    control revision, policy revision, policy fingerprint,
    observation epoch, observation-set digest,
    workspace, collection, derived fingerprint,
    topology generation, placement-tree digest,
    provider-geometry digest )
```

Same snapshot, same plan, bitwise. The rules:

- **Time is frozen.** The authority reads its clock once, at plan
  start, and records the value as `planning_instant_unix_ms`. Every
  freshness and warmth judgment inside the plan uses that recorded
  value. Planning never reads the clock a second time, never advances
  the observation epoch, and never commits an expiry transition; those
  are §3's ordered committed events, applied before or after the plan,
  never inside it.
- **Every limit is resolved and recorded.** `max_moves`,
  `max_observation_age_ms`, and the `clock_skew_bound_ms` are recorded
  in the context after defaults are applied, so two requests with
  different stated or defaulted limits are different inputs and
  produce different plan digests.
- **Clock skew refuses loudly.** An observation with
  `window_end_unix_ms > planning_instant + clock_skew_bound_ms` is
  refused by name (`plan_tiers: observation from node pi5v1 ends
  23,004 ms in the future, past the 5,000 ms skew bound`). Within the
  skew bound, future-dated values clamp to the planning instant. All
  time subtraction is checked; an underflow outside the skew rule is a
  refusal, not a wrap.
- **No hidden inputs.** The context binds the authority that planned
  (identity and incarnation), both revisions, the observation set by
  digest, the full topology and tree by generation and digest, the
  collection declaration, and the provider geometry. A topology
  generation alone does not bind those values; per the
  [authority convergence boundary](raft-control-design.md#single-authority-convergence-boundary-2026-09-08),
  the planner consumes the committed snapshot and performs no live
  coordinator lookups — a live lookup would be an unrecorded input and
  is forbidden.

The snapshot is made canonical before the function runs, so map
iteration order is never an input:

- partitions are ordered by (`derived_fingerprint` bytes, column name,
  bucket ordinal) inside (workspace, collection);
- observations are ordered by the §3 stored-key tuple;
- nodes are ordered by node id;
- within a step, the candidate partition is the one whose move lowers
  the objective most, ties broken by partition identity, then by
  destination node id, then by tier name — the same shape as
  `PlanBalance`'s node-id-then-shard-id rule;
- all arithmetic is the fixed-point integer arithmetic of §5; nothing
  floating point is computed, compared, or serialized anywhere in the
  plan.

**plan_digest.** The plan's identity is
`SHA-256("protomolt.capacity-plan-input.v1" ‖ 0x00 ‖ u32le(1) ‖ the context fields in the order above)`,
each field in §5's canonical encoding. Because the context includes the resolved limits and
the planning instant, two runs that differ only in a defaulted limit
or in when they planned have different digests — a plan can always be
checked against exactly what it planned from.

```proto
// DRAFT. Not allocated. On ClusterControl, beside PlanBalance:
//   rpc PlanTiers(PlanTiersRequest) returns (PlanTiersResponse);  // DRAFT
message PlanTiersRequest {
  string workspace = 1;                 // DRAFT
  string collection = 2;                // DRAFT
  // The policy: tiers in precedence order, with each tier's residency
  // floor and workload band. Absent selects the control plane's stored
  // policy; the response always carries the policy identity it used.
  TierPolicy policy = 3;                // DRAFT
  uint64 max_moves = 4;                 // DRAFT; 0 selects the policy default
  uint64 max_observation_age_ms = 5;    // DRAFT; 0 selects 10 minutes
}

message TierPolicy {                    // DRAFT
  // Precedence order is semantic: classification is the first tier
  // whose band matches. The list order is part of the policy's
  // identity (§5); two policies differing only in tier order are
  // different policies.
  repeated CapacityTier tiers = 1;      // DRAFT
  // SHA-256 over the canonical encoding of this message with this
  // field excluded (§5). The authority computes it; a client carrying
  // one that differs from its tiers is refused naming both.
  string policy_fingerprint = 2;        // DRAFT
}

message CapacityTier {                  // DRAFT
  string name = 1;                      // DRAFT, e.g. "hot", "warm", "archive"
  NodeResidency residency = 2;          // DRAFT; SERVER or unspecified-refused
  uint32 min_replicas = 3;              // DRAFT; the residency floor, >= 1
  // The observed band this tier covers, in fixed-point nanos: scans
  // per second per resident byte, times 10^9, [lo, hi). Integer edges,
  // exact classification (§5). A partition outside every band is
  // reported unclassified and is not moved.
  uint64 scans_per_byte_nanos_lo = 4;   // DRAFT
  uint64 scans_per_byte_nanos_hi = 5;   // DRAFT
  uint64 max_seconds_since_scan = 6;    // DRAFT; 0 = no warmth bound
}

// DRAFT. The fragment a move copies, and the two copies it names.
message FragmentIdentity {
  PartitionIdentity partition = 1;      // DRAFT
  uint64 topology_generation = 2;       // DRAFT
  string leaf = 3;                      // DRAFT
}

message CopyIdentity {                  // DRAFT
  string node_id = 1;                   // DRAFT
  bytes process_incarnation = 2;        // DRAFT
  bytes storage_incarnation = 3;        // DRAFT
}

message TierMove {                      // DRAFT
  FragmentIdentity fragment = 1;        // DRAFT
  CopyIdentity source = 2;              // DRAFT
  CopyIdentity destination = 3;         // DRAFT; the reserved identity
  string from_tier = 4;                 // DRAFT; "" when unclassified
  string to_tier = 5;                   // DRAFT
  uint64 bytes = 6;                     // DRAFT
  // The identity of the plan that emitted this move, so a move checked
  // alone refuses correctly. plan_digest binds every context input,
  // the resolved limits included.
  string plan_digest = 7;               // DRAFT
  uint64 control_revision = 8;          // DRAFT
  uint64 observation_epoch = 9;         // DRAFT
  string policy_fingerprint = 10;       // DRAFT
}

message PlanTiersResponse {             // DRAFT
  // The frozen context, in full.
  uint64 planning_instant_unix_ms = 1;  // DRAFT
  uint64 max_moves = 2;                 // DRAFT; as resolved
  uint64 max_observation_age_ms = 3;    // DRAFT; as resolved
  uint64 clock_skew_bound_ms = 4;       // DRAFT; as resolved
  string authority_id = 5;              // DRAFT
  bytes authority_incarnation = 6;      // DRAFT
  uint64 control_revision = 7;          // DRAFT
  uint64 policy_revision = 8;           // DRAFT
  string policy_fingerprint = 9;        // DRAFT
  uint64 observation_epoch = 10;        // DRAFT
  string observation_set_digest = 11;   // DRAFT
  uint64 topology_generation = 12;      // DRAFT
  string placement_tree_digest = 13;    // DRAFT
  string workspace = 14;                // DRAFT
  string collection = 15;               // DRAFT
  string derived_fingerprint = 16;      // DRAFT
  string provider_geometry_digest = 17; // DRAFT
  string plan_digest = 18;              // DRAFT
  repeated TierMove moves = 19;         // DRAFT
  // Per partition: the tier the observations classify it into, before
  // any move, with the observation figures the plan saw (aggregate
  // rate, warmth, per-reporter percentiles unmerged). Unclassified
  // partitions appear here with an empty tier and the reason.
  repeated PartitionPlacement placements = 20; // DRAFT
}
```

**Proof obligation.** The contract's test, not its hope: two
independent planner instances — in the test, two control planes built
from the same stored state — given the same frozen snapshot must emit
byte-identical serialized responses. The test holds golden plans for
the fixed literal inputs of §10, permutes the insertion order of
nodes, partitions, and observations, and asserts the bytes do not
change (example E8). A plan that depends on iteration order, wall
clock beyond the frozen instant, or process identity fails it. The
golden bytes are produced once, reviewed, and pinned per
implementation — never regenerated to match a change — and the same
golden test runs on the ARM and x86 builds; with §5's all-integer
arithmetic and little-endian canonical encoding, the bytes are
architecture-independent by construction, and the cross-arch run
proves it. This is the tier-plan analogue of the placement suite's
pruning-on/pruning-off answer identity.

## 5. Canonical encodings, policy identity, and numeric rules

**Canonical encoding `C`.** Every hashed value in this contract uses
one encoding: a domain tag, a version, then fields in specification
order. Strings are their UTF-8 bytes with a `u32le` length prefix;
integers are little-endian; digests are their 32 raw bytes; lists are
a `u32le` count followed by the elements *in their semantic order* —
for tiers that order is precedence, not map insertion order. Names are
compared bytewise: there is no case folding or Unicode normalization,
and a policy containing two bytewise-equal tier names is refused at
validation.

**policy_fingerprint.** The fingerprint of a `TierPolicy` is
`SHA-256(C(policy))` where `C` here is:

```
"protomolt.capacity-tier-policy.v1" ‖ 0x00 ‖
u32le(1) ‖                       # encoding version
u32le(tier_count) ‖
for each tier in declared (precedence) order:
  u32le(name_len) ‖ name ‖
  u8(residency) ‖                # 1 = SERVER; DEVICE is invalid in a tier
  u32le(min_replicas) ‖
  u64le(scans_per_byte_nanos_lo) ‖
  u64le(scans_per_byte_nanos_hi) ‖
  u64le(max_seconds_since_scan)
```

The `policy_fingerprint` field itself is excluded from its own hash —
a fingerprint computed over a message that contains the fingerprint is
recursive and undefined, so the hashed form is the versioned policy
*definition* with the computed field removed. Example E7 pins the
literal bytes and digest of one policy and shows that permuting tier
order changes the fingerprint, because order is precedence.

**Fixed-point bands, exact classification.** The band's unit is the
*nano*: scans per second per resident byte, times 10⁹, as a `u64`.
There are no doubles anywhere in the contract — no NaN, no infinity,
no signed zero, no serialization ambiguity, and no
architecture-dependent evaluation. Given a partition's aggregate
`scans_observed` S over a window of W milliseconds and aggregate
`resident_bytes` B (both §3's rules), the classification rate is

```
rate_nanos = floor( S · 10¹² / (W · B) )
```

computed in `u128` with checked conversions. A window longer than
2⁴⁰ ms is refused at report ingest, so `W · B < 2¹⁰⁴` and
`S · 10¹² < 2¹⁰⁴`: the arithmetic cannot overflow, and any violation
of the bounds is a named refusal, never a wrap. Because the band edges
are integers, floor division is exact: the true rational rate lies in
`[lo, hi)` if and only if `rate_nanos` does, so no partition is
misclassified by rounding, and a true rate exactly equal to `hi`
lands outside the band as the half-open rule requires. A partition
classifies into the first tier in precedence order whose band
contains `rate_nanos` *and* whose warmth bound it satisfies; warmth
with `max_seconds_since_scan = 0` is no bound, a `last_scanned` of
zero fails any nonzero warmth bound, and warmth subtraction uses the
frozen instant under §4's checked rules.

**Band validity.** A tier with `lo >= hi` is refused at policy
validation by name. Bands may overlap across tiers; overlap is
resolved by precedence order and is not an error, because the order
is part of the policy's identity and meaning. A partition outside
every band — including one whose rate exceeds every `hi` — is
reported unclassified with its figures and is never moved.

## 6. Revision checks and the advisory boundary

Every plan carries the frozen context in full, and every move carries
the plan's identity: `plan_digest`, `control_revision`,
`observation_epoch`, and `policy_fingerprint`, plus the concrete
`source` and `destination` copy identities of §2. A receiver compares
what a message carries against what it holds, per field, and refuses
a mismatch by name. The refusal text names the route, the field, the
carried value, and the local value, in the shape the plane already
uses:

```
plan_tiers: the request's observation epoch 118 is not this authority's 121
plan_tiers: shard s6 carries derived_fingerprint 9f2c…, the plan's is 41aa…; a rebuild, not a reload
plan_tiers: workspace "ws-court" collection "briefs" is not this plan's "ws-court"/"cases"
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

**Advisory only.** This revision of the contract defines no execution
route, and a dry-run digest is not an execution credential. The
reason is structural: a multi-step plan is computed against one
snapshot, and if its first committed move advances the control
revision, every later step of the same plan is stale under strict
per-step revision comparison (example E9 works this literally).
Rather than weaken the checks, v1 keeps plans advisory. Before any
execution contract is introduced it must define, as its own reviewed
design:

1. a retained operation/step identity that survives the revision
   advances the operation itself causes;
2. actor-scoped retries — which actor may re-present a step, and what
   idempotency key makes a retry safe;
3. the expected state before each step, checked at apply time;
4. capacity reservations on the destination, with their own identity
   and expiry;
5. revalidation rules: which fields a step rechecks against current
   committed state (verified unique copies, failure domains,
   reservations, the current authority and its incarnation) and which
   it takes from its snapshot, so unrelated observation or topology
   changes never silently reinterpret an accepted step.

Until that contract exists, anything that acts on a plan is an
operator reading a report, and §7's execution-time revalidation
duties are stated as obligations on that future contract, not on this
one.

## 7. Residency constraints and floors

- **Device exclusion by declaration, extended.** The `PlanBalance`
  rule — a `DEVICE` node is excluded from every export, replication,
  relocation, and segment copy before any capacity logic runs, with
  the exclusion reported — applies to tiers unchanged and per
  partition: a device node is never a move destination and never a
  tier's node, whatever the partition's observed rate there. An
  `UNSPECIFIED` residency is reported and never assumed movable, and a
  tier whose `residency` is `DEVICE` is refused at policy validation
  by name. The exclusion binds at *both* boundaries: planning never
  proposes a device-local source for export, replication, or server
  replacement, and the future execution contract must re-check it at
  apply time. Neither collection Admin nor cluster trust overrides
  source residency; residency is the node's declaration about itself.
- **The residency floor is per tier, counted in complete replicas.**
  `min_replicas` is the count of §2-complete ready copies the tier
  guarantees.
- **Old-tier and new-tier obligations, and the transition point.**
  During a move the partition is leaving one tier's floor and entering
  another's, and applying both floors throughout would make every
  departure impossible. The rule: the *leaving* tier's floor binds
  until the *entering* tier's floor is satisfied by verified complete
  copies at the destination; the transition point is the step in the
  plan's own sequence at which the entering floor is met, and from
  that step the leaving floor lapses. A plan whose sequence leaves the
  partition below the binding floor at any intermediate step is
  refused, naming the partition, the tier, and the floor:
  `plan_tiers: tier "archive" floor is 2 copies, this move sequence leaves partition key_bucket=7 with 1 at step 3`.
  A floor the cluster cannot currently satisfy (fewer eligible nodes
  than `min_replicas` in the leaf's pool) is reported, not planned
  around.
- **Floors are proven under stated assumptions.** A dry run proves
  that its own proposed sequence preserves the binding floor *under
  the captured snapshot*: it assumes no concurrent node failure, no
  topology change, and no observation drift beyond the frozen set, and
  it says so in the response's context. It cannot and does not
  guarantee survival of unrelated concurrent failures; that guarantee
  belongs to execution, which must revalidate unique verified copies,
  failure domains, capacity reservations, and the current authority
  before dropping any source (§6's deferred contract).
- **Failure domains carry over.** The per-move rule from the balance
  dry run — a destination in the failure domain of one of the
  partition's ready copies is skipped — applies unchanged, checked at
  every intermediate step of the sequence against the complete-replica
  set, not only at the endpoints.

## 8. Tiers

A capacity tier is a named residency policy (§4's `CapacityTier`):
which residency kind may hold the partition, how many complete copies
must exist, and which observed workload band the tier covers. Tiers
are ordered by precedence; the classification of a partition is the
first tier whose band its observation satisfies. The names are the
operator's ("hot", "warm", "archive" are examples, not vocabulary);
the semantics are the floor and the band, nothing else.

**A partition changes tier only by a planned move.** Classification is
derived from observations and reported on every plan; residency is
changed only by the move list a plan emits and an operator or executor
applies under §6's revision checks. There is no implicit tier change:
an observation that reclassifies a partition changes its reported
tier, and moves nothing. Conversely a partition never drifts into a
tier because a node happened to hold it — tier is a property the plan
assigns, and the plan is the only writer of that assignment.

**Interaction with the placement tree.** The leaf's node set remains
the pool every move stays inside, as it does for `PlanBalance`; a tier
further narrows the pool by residency kind and policy, never widens
it. A partition spanning leaves is planned per fragment, so the same
bucket ordinal can sit in "warm" in one leaf and "archive" in another
— the classification is over the rows the leaf holds, and the plan
says which leaf each move is bounded by. A leaf edit or a new tree is
a new topology generation, which §6 turns into refusals of every
outstanding tier plan: a re-placement and a tier move never compose
silently.

**Interaction with pruning.** A partition is a logical partition in
the placement document's sense: a predicate, costing a bitmap,
changing no file. Shard and segment pruning are untouched — the
partition column's equality is one more prunable predicate where a
segment's summary bounds it, and `placement == code` bounds are
unaffected. Nothing in a tier plan marks a shard skippable; a pruned
shard still contributes its observation and its resident bytes to the
plan, because pruning is a per-query decision and capacity is not.

## 9. What this contract does not do

- **No ownership change.** Who serves a partition's rows stays the
  topology's decision (shards and, where it lands, segment-subset
  ownership). This contract names partitions, classifies them, and
  plans their tier moves as a dry run, exactly the stance
  `PlanBalance` takes for whole shards. The map flip, the copy path,
  and the cutover are the separate execution gate that
  [scale-out-coordination.md](scale-out-coordination.md) already
  fences off.
- **No execution, advisory plans only.** No route applies a tier plan,
  and no reconciler consumes one. §6 states the five obligations an
  execution contract must meet before one is reviewed; until then a
  plan is a report an operator reads.
- **No consensus.** The single control authority computes plans. Raft
  and standby-with-fencing are the control-plane track
  ([raft-control-design.md](raft-control-design.md)) and are unchanged
  here; the determinism obligation of §4 is what makes a later
  replicated authority able to check its members' plans against each
  other, and it is specified now for that reason.
- **No new hash, no re-bucketing.** The partition function is the
  declared column's own expression. A changed modulus is a new
  declaration and a rebuild, never a migration in place.
- **No transport tag allocation.** Every protocol block above is
  DRAFT text; field numbers are reserved in
  `proto/ai/protomolt/search/v1/search.proto` only after this amended
  contract is accepted.
- **Dependencies on the foundations track.** The partition column must
  be a declared derived column under the in-flight descriptor-mapping
  and authorization work on `feat/search-foundations`: stable keys
  must exist on every row the expression reads (a column reading the
  stable key already refuses a document without one), and the
  disclosure rules of [derived-columns.md](derived-columns.md) apply
  to every observation, plan, and refusal text that names the column —
  a plan is cluster-trust, like the other control routes, and is not a
  public route. Observations name partitions, never stable keys or
  document identities. The verified owner/readiness contract that §2's
  replica test and §6's execution revalidation rely on is likewise
  foundations work; this contract names the dependency rather than
  bypassing it.

## 10. Worked examples, with literal fixtures

These examples are the review fixtures: complete inputs and the
expected classifications, plans, and refusals, with the digests
pinned. When the planner is implemented, §4's proof-obligation test
pins these literals as golden expectations; a behavior change must
change the fixture by deliberate reviewed edit, never by regenerating
goldens to match new output.

**Common fixture.** Unless an example says otherwise:

- workspace `ws-court`, collection `cases`.
- Declaration D: `key_bucket = hash.fnv64(stable_key()) % 64u`. For
  these examples the declaration constant is computed as
  `SHA-256("key_bucket = hash.fnv64(stable_key()) % 64u")` — an
  example-local construction; production fingerprints use the
  canonical `DerivedColumns` encoding of derived-columns.md. The
  fixture value:
  `34386b557959dd356964202d4a1fd062671e5ffac646dca4fe48421df09826d0`.
- Policy P, three tiers in precedence order, bands in nanos:

  | name | residency | min_replicas | band [lo, hi) | warmth bound |
  |---|---|---|---|---|
  | hot | SERVER | 3 | [100, 1 000 000 000 000) | none |
  | warm | SERVER | 2 | [1, 100) | 86 400 s |
  | archive | SERVER | 2 | [0, 1) | none |

- Nodes: `krick-1`, `pi5v1`, `pi5v3` (SERVER; failure domains `krick`,
  `pi5`, `pi5` respectively — the two Pis share a domain), and `pi5v2`
  (DEVICE). Tree generation 9; leaf `L4` = {krick-1, pi5v1, pi5v3}.
- Frozen planning instant `T = 1788868800000` (2026-09-08T12:00:00Z).
  Default window: `[1788868200000, 1788868800000)` (600 s). Resolved
  limits unless stated: `max_moves = 16`, `max_observation_age_ms =
  600000`, `clock_skew_bound_ms = 5000`. Authority `control-0`,
  incarnation `00…01`; control revision 41, policy revision 7,
  observation epoch 118.
- Incarnations: `A = 00…0a`, `B = 00…0b` (16 bytes each).
- Base observation set O, two reports for partition
  `(ws-court, cases, 34386b55…26d0, key_bucket, 7)`:

  | shard (gen) | reporter | rows | resident bytes | scans | last scanned | samples |
  |---|---|---|---|---|---|---|
  | s6 (3) | krick-1 / A | 1 000 000 | 268 435 456 | 3 000 000 | T−5000 | 3000 (p50 1200 µs, p99 9400 µs) |
  | s7 (3) | pi5v1 / B | 500 000 | 134 217 728 | 0 | T−3 600 000 | 0 |

  The s6 reporter also attributes `scan_bytes = 786 432 000 000`; the
  s7 report's percentiles are unknown (`samples = 0`), not fast.

  Aggregates under §3: rows 1 500 000, resident bytes 402 653 184,
  scans 3 000 000, warmth T−5000. Rate:
  `floor(3 000 000 · 10¹² / (600 000 · 402 653 184)) = 12417` nanos
  (exact rational 12 417.6343…; floor is exact per §5). 12 417 ∈
  [100, 10¹²) → **hot**.

  Canonical observation-set digest (`C` over the §3 stored-key order,
  domain `protomolt.capacity-observations.v1`):
  `67e90291160053b7bd8602114bc9a9f93345f12d5b4bd337a5df6a234cbb377b`.

  Plan digest for the full frozen context (tree digest
  `SHA-256("example placement tree: gen 9, leaf L4 = {krick-1, pi5v1, pi5v3}")`,
  provider-geometry digest `SHA-256("example provider geometry: turbovec 64-shard mapped image")`):
  `1ffd7ca10bd031449688debc5c493de4ea8928195641cf31e7bc57027380725d`.

### E1. Cross-collection identical declarations

Collection `briefs` declares the same expression text
`key_bucket = hash.fnv64(stable_key()) % 64u`; its derived fingerprint
is therefore the same `34386b55…26d0`. The two partition sets are
still distinct identities:

```
cases:  (ws-court, cases,  34386b55…26d0, key_bucket, 7)
briefs: (ws-court, briefs, 34386b55…26d0, key_bucket, 7)
```

A `PlanTiersRequest{workspace: "ws-court", collection: "briefs"}`
plans only `briefs` rows; the stored observation keys for the two
collections never collide, and a report or move carrying
`collection: "briefs"` presented against the `cases` plan refuses:

```
plan_tiers: workspace "ws-court" collection "briefs" is not this plan's "ws-court"/"cases"
```

Equal fingerprints identify equal declarations, never combinable rows.

### E2. Partial copies are not replicas

Bucket 9 in leaf L4: pi5v1 holds one disjoint subset of its rows
(storage incarnation `00…91`), pi5v3 holds the rest (`00…92`); no node
holds the fragment's full row set at one source version. Complete
replica count of the fragment: **0** (two partial copies). Tier
`archive`'s floor of 2 cannot even be evaluated as satisfied, and any
move touching the fragment refuses:

```
plan_tiers: fragment (key_bucket=9, gen 9, L4) has 0 complete replicas (2 partial); tier "archive" floor is 2
```

The remedy the plan reports is replication to completeness, never a
move that treats two halves as two copies. Likewise two relay routes
to pi5v1's copy are one copy.

### E3. Measured idle versus missing data

- Bucket 7's s7 report (fixture O): `scans_observed = 0` in a fresh
  window from the current incarnation — **measured idle**. Its
  contribution to the rate is exactly 0, the report classifies
  normally, and had the aggregate rate fallen in [0, 1) the partition
  would classify `archive`.
- Bucket 11 is covered by policy P, but the committed owner of its
  shard has sent no current report — **unknown**, not zero. The plan
  refuses:

```
plan_tiers: partition key_bucket=11 on node krick-1 has no current observation; unknown is not idle
```

Turning missing telemetry into a zero rate would classify precisely
the unobserved cold partitions this feature exists to describe, so the
two cases never share a code path.

### E4. Staleness boundary

With `max_observation_age_ms = 600000` and planning instant T:

- `window_end = T − 600000` → age 600 000 ms, not strictly greater
  than the bound → **fresh**.
- `window_end = T − 600001` → age 600 001 ms → **stale**, and the
  plan refuses:

```
plan_tiers: partition key_bucket=7 on node pi5v1 was last observed 600001 ms ago, past the 600000 ms bound
```

And the skew rule: `window_end = T + 23004` exceeds the 5 000 ms skew
bound → the report is refused by name (`ends 23,004 ms in the
future`); `window_end = T + 3000` is within the bound and clamps to T
for freshness arithmetic.

### E5. Restarted reporters

krick-1 (incarnation A) has a fresh stored report for (bucket 7, s6)
with `window_end = T − 60000`. krick-1 restarts; incarnation B lands
its first report at `window_end = T − 30000` with `scans_observed = 0`
(measured idle for the window it can speak for). In the same committed
transition the authority drops every observation keyed to
`(…, krick-1, A)`; the epoch advances once. Consequences:

- The A report's freshness never counts, although its window is only
  60 s old.
- Classification now uses B: the partition's s6 scans for planning are
  0, not 3 000 000.
- A late-arriving A report — a retry that outlived its process — is
  refused:

```
report_shard: node krick-1 incarnation 00…0a is superseded by 00…0b
```

### E6. Duplicate reports and window conflicts

- The s6 report of fixture O is retried byte-identically (same key,
  same window): idempotent no-op. The stored set is unchanged, the
  observation-set digest stays `67e90291…377b`, and the epoch stays
  118.
- The same key and window arrives with `scans_observed = 2 999 999`:
  refused, naming the conflict — the reporter must emit a new window:

```
report_shard: observation for (key_bucket=7, s6, gen 3) window [1788868200000, 1788868800000) from krick-1/00…0a conflicts with the stored report
```

- A report with window `[1788868201000, 1788868801000)` (overlapping,
  greater end) supersedes the stored one; one with window
  `[1788867600000, 1788868200000)` (earlier, disjoint) is discarded as
  superseded by the newer stored window.

### E7. Policy hashing, pinned

Policy P's canonical bytes are exactly 155 bytes:

```
"protomolt.capacity-tier-policy.v1" 0x00
u32le(1) u32le(3)
  u32le(3) "hot"     0x01 u32le(3) u64le(100) u64le(1000000000000) u64le(0)
  u32le(4) "warm"    0x01 u32le(2) u64le(1)   u64le(100)          u64le(86400)
  u32le(7) "archive" 0x01 u32le(2) u64le(0)   u64le(1)            u64le(0)
```

```
policy_fingerprint(P) = 5858b135aed48c2bbc1cc5bd0accec509eee415ea29929c2dd29908e76e708eb
```

Pinned consequences:

- Moving `warm` ahead of `hot` (precedence is semantic) changes the
  identity:
  `449751c3e2d50ec0177958554e92c6ea3403a82139b6e6aecc3223f45973a2e7`.
- The fingerprint field is excluded from its own hash: a client that
  sends P with `policy_fingerprint = "5858b135…"` is accepted; the
  same tiers carrying `policy_fingerprint = "0000…"` is refused naming
  both values.
- A tier with `lo >= hi` (e.g. warm edited to [100, 100)) is refused
  at validation, not hashed.

### E8. Permuted input order, identical plan

The authority applies the two fixture reports in the order (O_s6,
O_s7) and, on a second instance, (O_s7, O_s6). Canonicalization orders
the stored set by the §3 key tuple either way, so:

- observation-set digest is `67e90291…377b` in both;
- the plan bytes are identical in both, with plan digest
  `1ffd7ca1…725d`.

Expected plan output for the fixture, stated literally:

```
PlanTiersResponse {
  planning_instant_unix_ms: 1788868800000
  max_moves: 16  max_observation_age_ms: 600000  clock_skew_bound_ms: 5000
  authority_id: "control-0"  authority_incarnation: 00…01
  control_revision: 41  policy_revision: 7
  policy_fingerprint: "5858b135…08eb"
  observation_epoch: 118
  observation_set_digest: "67e90291…377b"
  topology_generation: 9
  workspace: "ws-court"  collection: "cases"
  derived_fingerprint: "34386b55…26d0"
  plan_digest: "1ffd7ca1…725d"
  placements: [
    { partition: (ws-court, cases, 34386b55…26d0, key_bucket, 7),
      classified_tier: "hot",
      rate_nanos: 12417, rows: 1500000, resident_bytes: 402653184,
      last_scanned_unix_ms: 1788868795000,
      reporters: [
        { node: "krick-1", shard: "s6", p50_us: 1200, p99_us: 9400, samples: 3000 },
        { node: "pi5v1",  shard: "s7", samples: 0 } ] }
  ]
  moves: []    # bucket 7 already sits on three SERVER nodes in L4; hot's floor of 3 is met
}
```

The percentiles appear per reporter, unmerged. A partition with
`scans_observed = 60000` on the same bytes would classify at 248 nanos
→ `warm`; the fixture's 3 000 000 scans is what puts it in `hot`.

### E9. Multi-step plans and revisions

Suppose plan P (digest `1ffd7ca1…725d`, control revision 41, epoch
118) carries two moves, m1 then m2, for two fragments. Under strict
per-step revision comparison, if an executor commits m1, the control
revision advances to 42 — and m2, which carries 41, refuses:

```
apply_tier_move: move carries control_revision 41, this authority is at 42
```

That is the intended v1 outcome: the plan is advisory, no executor
exists, and the refusal demonstrates why a dry-run digest cannot act
as an execution credential. The future execution contract (§6's five
obligations) must give the operation a retained identity that survives
the revision advances its own steps cause, must revalidate each step's
preconditions against current committed state — verified unique
copies, failure domains, reservations, the current authority — and
must refuse when an unrelated observation or topology change would
reinterpret an accepted step. Until that contract is reviewed, m1 and
m2 are lines in a report an operator reads.

## 11. Review dispositions

The 2026-09-08 operator review of the 2026-09-07 draft resolved the
original checklist as follows; the section numbers are this revision's.

1. **Identity** — superseded: the partition identity is the
   resource-scoped quintuple of §2, and movement names fragments and
   copies, never bare partitions, leaves, or addresses.
2. **Carrier** — retained: per-(partition, shard) reporting on
   `ReportShard`, now bound to reporter incarnation and source
   generation, with explicit aggregation rules (§3).
3. **Attribution** — retained for v1 (confined scans only), and the
   band's signal is now a carried counter (`scans_observed` over an
   explicit window), not a derivative of byte throughput (§3, §5).
4. **Refuse-on-stale** — retained, with the measured-idle/unknown
   distinction made explicit so cold-but-measured partitions still
   classify (§3, E3).
5. **Band units** — scans per second per resident byte, kept, now in
   fixed-point nanos with exact integer-edge classification (§5).
6. **Movement scope** — retained: per (partition, leaf) fragments
   inside the leaf's node set (§2, §8).
7. **Floors** — per tier, counted in complete replicas, with the
   old-tier/new-tier transition point defined (§7).
8. **Golden fixtures** — pinned literals, reviewed expectations,
   proven on both architectures; never regenerated to match a change
   (§4, §10).
9. **Refusal texts** — the §3, §5, §6, §7 texts are the set the
   executor's log must preserve verbatim.
10. **Policy location** — still the operator's call: whether the tier
    policy lives in the shard map beside `[placement]` and
    `[[derived]]` so one file carries all three identities.

**Next bounded task (per the review's handoff).** With these semantics
fixed: implement bounded observation collection and a pure dry-run
planner against committed input snapshots, with §10's literals as the
golden tests. Move execution, ownership changes, transport tag
allocation, and further fleet operations stay out of that task;
source-authority activation remains on the foundations track.
