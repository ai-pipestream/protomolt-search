# Capacity tiers over declared hash partitions

Status: second amendment for operator review, 2026-09-08. The
2026-09-07 draft was reviewed; the first amendment applied its
corrections (resource-scoped identity, carried observation signal,
frozen planning snapshot, canonical policy encoding, advisory/execution
boundary, worked examples). The review of that amendment found six
contradictions in the examples and remaining semantic gaps; this
revision repairs them: three-domain replica evidence in the fixtures, a
corrected warm literal, cohort-aligned windows, per-fragment
attribution, owner/history/storage binding on reports and copies, an
advisory (never minted) move target, a defined v1 move objective with
capacity limits, checked aggregation bounds, and an executable fixture
verifier (`scripts/verify_capacity_tier_fixtures.py`) that recomputes
every pinned literal in §10. Nothing in this document is built. It is
the contract to review before any ownership change is implemented: it
names partitions, observations, the plan, the move shape, and the
refusal behavior, and it embeds draft protocol blocks as text. No field
number below is allocated; every one is marked DRAFT and becomes
contract only when the number is reserved in
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
copy = (fragment, node_id, process_incarnation, storage_incarnation,
        ownership_epoch)
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
- **ownership_epoch** is the authority's committed ownership-history
  counter for the shard the copy serves: it advances on every committed
  ownership transition (attach, bootstrap, handoff, replacement), and
  the authority's record of it is the logical owner/history the
  fragment tuples name. A copy, a report, or a coverage claim whose
  ownership epoch is not *equal* to the committed one — older or
  newer — is refused by name; an integer generation comparison that
  only rejects older values cannot distinguish replacement bytes, so
  equality is the rule everywhere in this contract.

**Complete replica, defined.** One *complete replica* of fragment F at
source version V is a copy whose verified coverage evidence — the
manifest digest and per-section integrity proof over F's full row set
at V — matches V, presented under the committed ownership epoch, held
by a distinct eligible owner in a distinct failure domain.
Consequences:

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

Each node reports, per (partition, shard, leaf) triple it holds rows
of, one observation per reporting cohort. The carrier is `ReportShard`
(per shard, so per partition-in-shard, split per leaf when the shard
spans leaves), the route whose discipline the scan rate already uses
on the lease; the authority aggregates to per fragment under the rules
below.

```proto
// DRAFT. Not allocated. One node's measured statement about one
// partition in one shard, attributed to one placement leaf. Carried as
// `repeated PartitionObservation partitions` on ReportShardRequest
// (DRAFT field, number unassigned).
message PartitionObservation {
  PartitionIdentity partition = 1;      // DRAFT; includes workspace+collection
  ReporterIdentity reporter = 2;        // DRAFT
  ShardRef shard = 3;                   // DRAFT; generation, storage, owner epoch
  // The fragment this report's figures describe. The authority checks
  // the pair against committed topology: a (shard, leaf) pair that is
  // not committed coverage at this topology generation is refused by
  // name. A shard whose rows of the partition span several leaves
  // produces one report per leaf, with disjoint row/byte attribution —
  // never one report copied wholesale into every leaf.
  uint64 topology_generation = 4;       // DRAFT
  string leaf = 5;                      // DRAFT
  // Rows and resident encoded bytes of the fragment in this shard.
  uint64 rows = 6;                      // DRAFT
  uint64 resident_bytes = 7;            // DRAFT
  // The band signal's numerator: scans served by this reporter that
  // were confined to this partition, inside the window below. Zero is
  // a measured value (measured idle): it means zero *qualifying*
  // scans — it says nothing about broad queries that touched the
  // partition without being confined to it. Unknown is the absence of
  // a current report, never a zero.
  uint64 scans_observed = 8;            // DRAFT
  // Attributed payload bytes of those scans, same window; diagnostic,
  // never an input to classification.
  uint64 scan_bytes = 9;                // DRAFT
  // Queueing: the p50 and p99 queue wait this shard's scans of this
  // partition observed inside the window, microseconds. Unknown when
  // samples is zero; percentiles are per-reporter values and are never
  // merged across reporters (see the aggregation rules below).
  uint64 queue_wait_p50_us = 10;        // DRAFT
  uint64 queue_wait_p99_us = 11;        // DRAFT
  uint32 samples = 12;                  // DRAFT; queue-wait sample count
  // Warmth: when this reporter last served a scan touching the
  // partition, unix ms. Zero means never observed since this
  // incarnation started; warmth is a last-touch time, not a cache
  // probe.
  uint64 last_scanned_unix_ms = 13;     // DRAFT
  // The cohort window: [window_start_unix_ms, window_end_unix_ms),
  // aligned to the committed reporting cohort (below). window_end is
  // the observation time; both are the reporter's clock.
  uint64 window_start_unix_ms = 14;     // DRAFT
  uint64 window_end_unix_ms = 15;       // DRAFT
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

// DRAFT. A shard's bytes as one committed installation: the source
// generation, the storage incarnation of this copy, and the committed
// ownership-history epoch. All three are equality-checked against the
// authority's committed record; older *or newer* values are refused by
// name, because a behind-or-ahead value names bytes other than the
// committed ones (a replacement mints a new storage incarnation).
message ShardRef {
  string shard = 1;                     // DRAFT
  uint64 source_generation = 2;         // DRAFT
  bytes storage_incarnation = 3;        // DRAFT; 16 bytes
  uint64 ownership_epoch = 4;           // DRAFT
}
```

**The signal the band consumes.** The tier band is scans per second
per resident byte, and the observation carries its inputs directly:
`scans_observed` over the cohort window, and `resident_bytes`.
Classification computes the rate in fixed point under §5's rules. The
v1 attribution rule is the scan rate's existing one: a scan is
attributed to a partition only when it was confined to that one
partition; a scan that touched several partitions is attributed to
none of them and appears only in the node-level rate. So
`scans_observed == 0` asserts "no qualifying confined scan in this
window", never "no query touched these rows".

**The reporting cohort.** Windows are not per-reporter choices. The
authority commits one **reporting cohort** — a window length `L` and a
phase — and every reporter emits windows aligned to it:
`[phase + k·L, phase + (k+1)·L)`. Rules:

- A report whose window is not exactly one cohort window is refused at
  ingest by name (example E10). There is no per-reporter window
  normalization to define, because unaligned windows never enter the
  stored set.
- Aggregation combines only reports of the *same* cohort window: the
  plan selects the latest cohort window fully closed at the frozen
  planning instant (the *current cohort*), and only reports aligned to
  it participate. Two reports with different windows — 60 scans in 60
  seconds and 60 scans in 600 seconds — are never summed into one
  rate.
- A reporter whose latest report is for an earlier cohort window is
  judged by the ordinary freshness rule against the frozen instant; a
  fresh-but-previous-cohort report is stored, marked non-current, and
  does not satisfy coverage.

**Zero, idle, unknown — three different values.**

- `scans_observed == 0` in a current report is **measured idle**: the
  reporter was alive, the window is the current cohort, and no
  qualifying scan was served. Its rate is exactly 0 and it classifies
  into a band whose `lo` is 0.
- **Unknown** is the absence of a current-cohort report from the
  committed owner's current process and storage incarnation. Unknown
  is never rendered as zero, never classifies, and under the coverage
  rule below refuses the plan. (Example E3 works the pair literally.)
- `resident_bytes == 0` with `rows > 0`, or `scans_observed > 0` with
  `resident_bytes == 0`, is an impossible state — a scan of zero
  resident bytes is attribution corruption — and the report is refused
  by name. A shard holding no rows of a partition in a leaf sends no
  report for that pair at all.

**Report binding.** The stored observation key is the full tuple
`(workspace, collection, derived_fingerprint, column, bucket,
topology_generation, leaf, shard, source_generation, ownership_epoch,
node_id, process_incarnation, storage_incarnation)` — the same rows
the §2 copy identity names, so an observation and the copy it
describes can never disagree about which bytes were measured. Rules:

- **Restart supersedes.** When node N's incarnation J lands its first
  report, every stored observation keyed to `(…, N, J′, …)` with
  `J′ ≠ J` is dropped in the same committed transition. An old
  incarnation's freshness never counts, and a report from a superseded
  incarnation is refused by name (example E5). A restart changes the
  process incarnation only; the storage incarnation stays with the
  bytes.
- **Generation and epoch equality.** A report whose source generation,
  ownership epoch, or topology generation is not *equal* to the
  committed value — behind or ahead — is refused by name; it is never
  silently absorbed into another generation's set. A refusal does not
  change the stored set.
- **Retries are idempotent; conflicts refuse.** A report whose key and
  window match a stored report: identical payload is a no-op (the set
  is unchanged, the epoch does not advance); a differing payload under
  the same key and window is refused by name — the reporter must wait
  for the next cohort window (example E6).
- **One stored observation per key.** Because every stored window is a
  cohort window, the cohort rule replaces any overlap handling: the
  stored report for a key is the one for the latest cohort window the
  reporter has landed; an older-cohort report arriving later is
  discarded as superseded.

**Aggregation from reports to a fragment.** Classification is per
fragment (§8), and each report is attributed to exactly one fragment
by its (partition, topology generation, leaf), so the planner's
fragment figures come from the stored reports under coverage and
deduplication rules, not from any merged summary:

- **Rows and resident bytes** sum over the reports of each shard's
  *committed owner* only. Shards partition the rows, so owner reports
  are disjoint and the sum is exact coverage; a replica's report is
  coverage evidence for §2's replica test and is never added again.
  If the committed owner of a covered shard has no current report, the
  fragment's bytes are unknown, not zero.
- **Scans** sum over every current reporter of the fragment (owner and
  serving replicas): each reported scan happened once, at that node.
- **Warmth** is the maximum `last_scanned_unix_ms` over the fragment's
  current reporters; zero participates as never.
- **Percentiles are never aggregated.** Averaging or adding p50/p99
  values across reporters is not a percentile of anything, so the plan
  keeps them per reporter and never classifies on them; they appear in
  the response's placements as per-reporter diagnostics. A future
  mergeable distribution (a sketch with a stated merge rule) may
  replace them without changing the rest of this contract.
- **Aggregate bounds are checked, not assumed.** At most 2²⁰ reports
  may feed one fragment's aggregate (a larger fan-in refuses by name);
  S and B accumulate in `u128`. With per-report `u64` fields and the
  §5 window bound, the aggregates satisfy `S < 2⁸⁴` and `B < 2⁸⁴`,
  which is what makes §5's `u128` rate argument hold for the
  aggregates rather than only for single reports.

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
  collection declaration, and the provider geometry — which includes
  each node's committed capacity report (total and currently resident
  bytes), the only capacity figure destination selection may use. A
  topology generation alone does not bind those values; per the
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
- all arithmetic is the fixed-point integer arithmetic of §5; nothing
  floating point is computed, compared, or serialized anywhere in the
  plan.

**The v1 move algorithm is violation repair, not optimization.** A
plan emits moves only to eliminate violations of the *classified*
tier's constraints, and it does so in a fully specified order:

1. **Classify** every covered partition's fragments (§5). A fragment
   whose current copies already satisfy its classified tier's
   residency kind and floor produces no moves; reclassification alone
   never moves bytes, because all v1 tiers share residency kind
   SERVER — the tier label changes what the floor demands, not where
   bytes sit.
2. **Collect violations** in deterministic order: fragments by
   partition identity, then (topology generation, leaf); within a
   fragment, floor violations before residency violations. A floor
   violation needs `min_replicas − current_complete` added copies; a
   residency violation (a complete copy on a node of the wrong
   residency kind) needs one replacement copy per offending copy.
3. **Build the candidate list** for each needed copy: nodes in the
   fragment's leaf pool, residency SERVER, not already holding a
   complete copy of the fragment, failure domain distinct from every
   current complete copy, and with projected free bytes — committed
   capacity minus committed resident bytes minus bytes already planned
   onto that node earlier in *this* plan — greater than or equal to the
   fragment's bytes. Candidates are ordered by projected free bytes
   descending, ties by node id ascending.
4. **Emit the move** to the first candidate, or record a capacity
   refusal naming the fragment, the tier, and every candidate's
   shortfall (example E13), then continue with the next violation.
5. **Stop** when no violations remain or `max_moves` moves have been
   emitted. Violations left unplanned because of the limit are
   reported by name in the response; the emitted prefix is still a
   complete plan for what it covers, since every emitted move is
   independently floor-checked (§7).

The objective this minimizes is bytes copied while eliminating every
classified-tier violation. Each violation forces a fixed number of
copies of a fixed fragment, so byte minimization reduces to
destination choice, which step 3's ordering fixes. The complete
tie-break key for any move is: (partition identity tuple, topology
generation, leaf, violation kind, source node id, source storage
incarnation, destination node id). Two planners given the same
snapshot emit the same moves because every choice above is a function
of the snapshot; example E12 is the golden nonempty case.

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
  uint64 ownership_epoch = 4;           // DRAFT; committed owner history
}

// DRAFT. An advisory destination description — deliberately not a
// CopyIdentity. A dry run never mints the destination's storage
// incarnation: no copy exists there, so there is nothing to name.
// The future execution protocol assigns the reservation its
// identities when it actually creates them.
message MoveTarget {
  string node_id = 1;                   // DRAFT
}

message TierMove {                      // DRAFT
  FragmentIdentity fragment = 1;        // DRAFT
  CopyIdentity source = 2;              // DRAFT; fully bound, §2
  MoveTarget destination = 3;           // DRAFT; advisory, never reserved
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
implementation — never regenerated to match a change. Because §5's
arithmetic is all-integer and the canonical encoding is little-endian,
the bytes are architecture-independent by construction; the executed
cross-architecture proof is **pending**: it exists only when the
planner is implemented and the same golden test has run on the ARM and
x86 builds, with the exact commands, outputs, and tested
implementations recorded here. Until then the claim is a design
property, not a measured result; `scripts/verify_capacity_tier_fixtures.py`
is today's executable evidence, and it verifies the fixtures against
the rules, not any planner binary. This is the tier-plan analogue of
the placement suite's pruning-on/pruning-off answer identity.

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

**Where the policy lives.** The tier policy is declared in the shard
map beside `[placement]` and `[[derived]]`, and that declaration is a
*bootstrap or explicit update input*, not a live source: the authority
reads it at bootstrap or on an explicit policy-update route, validates
it (band validity, residency kinds, unique names), and commits the
effective policy with a new `policy_revision`. Every frozen context
carries the committed policy's revision and fingerprint. Reloading or
editing the local shard-map file never overrides committed policy
silently — a restarted authority whose file disagrees with its
committed policy refuses to serve `PlanTiers` until an explicit update
or a restored commit resolves the difference by name. This keeps one
writer (the authority) and one committed value, so the file's role is
configuration input, not a second live authority.

**Fixed-point bands, exact classification.** The band's unit is the
*nano*: scans per second per resident byte, times 10⁹, as a `u64`.
There are no doubles anywhere in the contract — no NaN, no infinity,
no signed zero, no serialization ambiguity, and no
architecture-dependent evaluation. Given a fragment's aggregate
`scans_observed` S over the current cohort window of W milliseconds
and aggregate `resident_bytes` B (both §3's rules), the classification
rate is

```
rate_nanos = floor( S · 10¹² / (W · B) )
```

computed in `u128` with checked conversions. A window longer than
2⁴⁰ ms is refused at report ingest, and §3's aggregation bounds cap a
fragment's fan-in at 2²⁰ reports, so the *aggregate* S and B satisfy
`S < 2⁸⁴` and `B < 2⁸⁴`, giving `W · B < 2¹²⁴` and
`S · 10¹² < 2¹²⁴`: the arithmetic cannot overflow for single reports
or for the aggregates the planner actually uses, and any violation of
the bounds is a named refusal, never a wrap. Because the band edges
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
pinned. Every pinned literal in this section is recomputed from the
stated encodings by `scripts/verify_capacity_tier_fixtures.py`, an
executable, stdlib-only verifier with all inputs and widths specified;
run it with

```
python3 scripts/verify_capacity_tier_fixtures.py
```

and it exits nonzero unless every recomputed value matches the value
pinned here. When the planner is implemented, §4's proof-obligation
test pins these same literals as golden expectations; a behavior
change must change the fixture by deliberate reviewed edit, never by
regenerating goldens to match new output.

**Common fixture A.** Unless an example says otherwise:

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

- Nodes: `krick-1` (SERVER, failure domain `krick`), `pi5v1` (SERVER,
  domain `pi5-west`), `pi5v3` (SERVER, domain `pi5-east`) — three
  distinct domains — and `pi5v2` (DEVICE). Tree generation 9; leaf
  `L4` = {krick-1, pi5v1, pi5v3}, leaf `L7` = {krick-1, pi5v1}.
- Committed capacity reports (provider geometry): free bytes
  krick-1 = 26 843 545 600 (25 GiB), pi5v1 = 16 106 127 360 (15 GiB),
  pi5v3 = 0.
- Frozen planning instant `T = 1788868800000` (2026-09-08T12:00:00Z).
  The committed reporting cohort is `L = 600000 ms`, phase 0, so
  cohort windows are the 600-second unix-aligned intervals; the current
  cohort at T is `[1788868200000, 1788868800000)`. Resolved limits
  unless stated: `max_moves = 16`, `max_observation_age_ms = 600000`,
  `clock_skew_bound_ms = 5000`. Authority `control-0`, incarnation
  `00…01`; control revision 41, policy revision 7.
- Process incarnations: krick-1 `A = 00…0a`, pi5v1 `B = 00…0b`,
  pi5v3 `C = 00…0c` (16 bytes each). Storage incarnations: krick-1's
  s6 install `00…a1`, krick-1's s7 install `00…a2`, pi5v1's s6 install
  `00…b1`, pi5v1's s7 install `00…b2`, pi5v3's s6 install `00…c1`.
  Committed ownership epochs: shard s6 → 5, shard s7 → 2.

Bucket 7 spans two leaves, so it is two fragments, each with its own
observation reports (§3's per-leaf attribution):

- **Fragment (bucket 7, gen 9, L4)** lives in shard s6 (source
  generation 3, ownership epoch 5): 1 000 000 rows, 268 435 456
  resident bytes. Three complete copies, each verified against the
  same coverage digest at source generation 3 — the example's
  coverage-evidence constant is
  `0dbf795457d54641dc05c4e7d679f8a1988de734d6f45594079ed463e02ca681`:

  | copy | node | proc inc. | storage inc. | domain |
  |---|---|---|---|---|
  | owner | krick-1 | 00…0a | 00…a1 | krick |
  | replica | pi5v1 | 00…0b | 00…b1 | pi5-west |
  | replica | pi5v3 | 00…0c | 00…c1 | pi5-east |

  Reports (window = current cohort): krick-1 — scans 3 000 000,
  scan_bytes 786 432 000 000, p50 1200 µs, p99 9400 µs, samples 3000,
  last_scanned T−5000. pi5v1 and pi5v3 — scans 0 (measured idle),
  last_scanned 0 (never served since this incarnation), samples 0.

- **Fragment (bucket 7, gen 9, L7)** lives in shard s7 (source
  generation 3, ownership epoch 2): 500 000 rows, 134 217 728 resident
  bytes. Two complete copies, coverage digest
  `c06e53ec1642080f0acda4e03305040364fd96cbbeb37cd3106f2adc99ac18e0`:
  owner pi5v1 (proc 00…0b, storage 00…b2, domain pi5-west) and replica
  krick-1 (proc 00…0a, storage 00…a2, domain krick). Reports: pi5v1 —
  scans 0, last_scanned T−3 600 000, samples 0; krick-1 — scans 0,
  last_scanned 0, samples 0.

Aggregates under §3 (fixture A observation epoch 118):

- Fragment (7, L4): owner-only bytes B = 268 435 456 (the two replica
  reports are coverage evidence, never re-added); scans S = 3 000 000
  over all three reporters; warmth T−5000.
  Rate: `floor(3 000 000 · 10¹² / (600 000 · 268 435 456)) = 18626`
  nanos. 18 626 ∈ [100, 10¹²) → **hot**.
- Fragment (7, L7): B = 134 217 728, S = 0 (measured idle), warmth
  T−3 600 000. Rate 0 ∈ [0, 1) → **archive**.

Canonical observation-set digest (domain
`protomolt.capacity-observations.v1`, encoding version 1, entries
ordered by the §3 stored-key tuple):
`33d9878d3941dbc85ffddd22d4b739162bae935dd5476521a5f0a1439ab965e3`.

Plan digest for the full frozen context (tree digest
`SHA-256("example placement tree: gen 9, leaf L4 = {krick-1, pi5v1, pi5v3}, leaf L7 = {krick-1, pi5v1}")`,
provider-geometry digest
`SHA-256("example provider geometry: turbovec 64-shard mapped image; committed free bytes: krick-1=26843545600, pi5v1=16106127360, pi5v3=0")`):
`2a4ba9094445438e21bbf888a5e22569c5cfbbada571cfd856823edb20102a4f`.

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

- Fragment (7, L7) in fixture A: `scans_observed = 0` in current-cohort
  reports from current incarnations — **measured idle**. Its rate is
  exactly 0 and it classifies `archive`. The zero asserts "no
  qualifying confined scan in this window"; a broad query that swept
  the fragment without being confined to it leaves the zero untouched.
- Bucket 11 is covered by policy P, but the committed owner of its
  shard has sent no current-cohort report — **unknown**, not zero. The
  plan refuses:

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

krick-1 (process incarnation A, storage incarnation 00…a1) has a
fresh stored report for fragment (7, L4). krick-1 restarts: new
process incarnation `00…1a`, *same* storage incarnation — the bytes
survived. Its first report lands for the current cohort with
`scans_observed = 0`. In the same committed transition the authority
drops every observation keyed to `(…, krick-1, 00…0a, …)`; the epoch
advances once. Consequences:

- The old process's freshness never counts, although its window was
  current.
- Classification now uses the new incarnation's report: the
  fragment's scans for planning drop to the serving replicas' 0, and
  the fragment reclassifies from hot toward its measured rate — the
  restart changed the *measurement*, which is correct: the process
  that served 3 000 000 scans is gone.
- A late-arriving report from process incarnation `00…0a` — a retry
  that outlived its process — is refused:

```
report_shard: node krick-1 incarnation 00…0a is superseded by 00…1a
```

Had krick-1 instead re-installed the fragment's bytes, the storage
incarnation would change (`00…a1` → new), the authority's committed
ownership epoch would advance, and every observation keyed to the old
storage incarnation would be refused as naming replaced bytes.

### E6. Duplicate reports and window conflicts

- The krick-1 L4 report of fixture A is retried byte-identically (same
  key, same cohort window): idempotent no-op. The stored set is
  unchanged, the observation-set digest stays `33d9878d…65e3`, and the
  epoch stays 118.
- The same key and window arrives with `scans_observed = 2 999 999`:
  refused, naming the conflict — the reporter must wait for the next
  cohort window:

```
report_shard: observation for (key_bucket=7, gen 9, L4, s6, gen 3) window [1788868200000, 1788868800000) from krick-1/00…0a conflicts with the stored report
```

- A report for the previous cohort window arriving late is discarded
  as superseded by the stored current-cohort report for the same key.

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
- An operator who edits the policy in the shard-map file changes
  nothing committed: the authority keeps serving the committed policy
  at revision 7 until an explicit update commits a new revision (§5).

### E8. Permuted input order, identical plan

The authority applies fixture A's five reports in two different
orders on two planner instances. Canonicalization orders the stored
set by the §3 key tuple either way, so:

- observation-set digest is `33d9878d…65e3` in both;
- the plan bytes are identical in both, with plan digest
  `2a4ba909…2a4f`.

Expected plan output for fixture A, stated literally:

```
PlanTiersResponse {
  planning_instant_unix_ms: 1788868800000
  max_moves: 16  max_observation_age_ms: 600000  clock_skew_bound_ms: 5000
  authority_id: "control-0"  authority_incarnation: 00…01
  control_revision: 41  policy_revision: 7
  policy_fingerprint: "5858b135…08eb"
  observation_epoch: 118
  observation_set_digest: "33d9878d…65e3"
  topology_generation: 9
  workspace: "ws-court"  collection: "cases"
  derived_fingerprint: "34386b55…26d0"
  plan_digest: "2a4ba909…2a4f"
  placements: [
    { fragment: (ws-court, cases, 34386b55…26d0, key_bucket, 7, gen 9, L4),
      classified_tier: "hot",
      rate_nanos: 18626, rows: 1000000, resident_bytes: 268435456,
      last_scanned_unix_ms: 1788868795000,
      complete_replicas: [
        { copy: (krick-1, 00…0a, 00…a1, epoch 5), domain: "krick",
          coverage: "0dbf7954…a681" },
        { copy: (pi5v1, 00…0b, 00…b1, epoch 5), domain: "pi5-west",
          coverage: "0dbf7954…a681" },
        { copy: (pi5v3, 00…0c, 00…c1, epoch 5), domain: "pi5-east",
          coverage: "0dbf7954…a681" } ],
      reporters: [
        { node: "krick-1", shard: "s6", p50_us: 1200, p99_us: 9400, samples: 3000 },
        { node: "pi5v1",  shard: "s6", samples: 0 },
        { node: "pi5v3",  shard: "s6", samples: 0 } ] },
    { fragment: (…, key_bucket, 7, gen 9, L7),
      classified_tier: "archive",
      rate_nanos: 0, rows: 500000, resident_bytes: 134217728,
      last_scanned_unix_ms: 1788865200000,
      complete_replicas: [
        { copy: (pi5v1, 00…0b, 00…b2, epoch 2), domain: "pi5-west",
          coverage: "c06e53ec…18e0" },
        { copy: (krick-1, 00…0a, 00…a2, epoch 2), domain: "krick",
          coverage: "c06e53ec…18e0" } ],
      reporters: [
        { node: "pi5v1",  shard: "s7", samples: 0 },
        { node: "krick-1", shard: "s7", samples: 0 } ] }
  ]
  moves: []
}
```

`moves: []` because both classified tiers' constraints are satisfied
by *proven* copies: hot's floor of 3 is met by three complete replicas
in three distinct failure domains, each with coverage evidence at the
committed source generation and ownership epoch; archive's floor of 2
is met by two. Node count plays no part — delete the coverage digest
from any copy and the floor check fails (E2's refusal shape), even
though the node still holds the rows.

The warm literal: had the L4 reporters served `scans_observed =
12 000` in the same window on the same bytes, the rate would be
`floor(12 000 · 10¹² / (600 000 · 268 435 456)) = 74` nanos ∈ [1, 100)
→ `warm`. (The fixture's 3 000 000 scans at 18 626 nanos is `hot`;
an earlier draft of this example used 60 000 scans — 248 nanos —
which is hot under these bands, not warm. 12 000 is the corrected
literal.)

### E9. Multi-step plans and revisions

Suppose plan P (digest `2a4ba909…2a4f`, control revision 41, epoch
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

### E10. Unequal windows never aggregate

krick-1 emits a report with window `[1788868140000, 1788868740000)` —
660 seconds, offset from the cohort grid. It is refused at ingest:

```
report_shard: window [1788868140000, 1788868740000) is not a cohort window (length 600000 ms, phase 0)
```

The rule the cohort replaces: without alignment, 60 scans in 60
seconds from one reporter and 60 scans in 600 seconds from another
could be summed into "120 scans" over an unspecified duration — a
rate of nothing. Under the cohort rule both reports either name the
same 600-second window and aggregate, or name different cohort
windows, in which case only the current cohort participates in the
plan and the earlier one is staleness-judged, never blended.

### E11. One partition, two leaves, two tiers

Fixture A is the multi-leaf example. Bucket 7's rows were split by
the placement tree on `year`: the year≤2020 rows live in shard s6
under leaf L4, the rest in shard s7 under leaf L7. The s6 node does
**not** copy its report into L7: it reports (bucket 7, s6, L4) with
exactly the rows and bytes that leaf holds, and the s7 node reports
(bucket 7, s7, L7) with its own. Classification runs per fragment
with per-fragment aggregates, so the same bucket ordinal classifies
**hot** in L4 (18 626 nanos) and **archive** in L7 (0 nanos,
measured idle) in one plan — E8's response shows both placements side
by side. A move the plan might one day emit for one fragment is
bounded by that fragment's leaf pool and says nothing about the
other.

### E12. A nonempty move plan, fully specified

Fixture M: same world as fixture A, plus two cold partitions whose
rows sit in shard s6 under leaf L4, each with exactly one complete
copy on pi5v3 (proc 00…0c, storage 00…c1, epoch 5):

| bucket | rows | resident bytes | scans (cohort) | last scanned |
|---|---|---|---|---|
| 5 | 80 000 | 21 474 836 480 (20 GiB) | 0 | T−7 200 000 |
| 6 | 72 000 | 19 327 352 832 (18 GiB) | 0 | T−10 800 000 |

Both classify `archive` (rate 0), floor 2, one complete copy each —
two floor violations. Observation epoch 119; fixture M's
observation-set digest is
`236d6740a5d5eaec435bb2b6c4b85644ed437ff87b40fd6b9150579ead21f6e1`
and its plan digest is
`96f514b75b2c29dcf708ddcdb558d69628f01e1280191bae947aa9182e81d48e`
(fixture M's frozen context is fixture A's with epoch 119 and the M
observation set; note fixture M *replaces* fixture A's bucket-7
reports for this example — its observation set is exactly the two
reports above).

The repair loop (§4):

1. Violations in partition-identity order: bucket 5 before bucket 6.
2. Bucket 5 needs one copy. Candidates in L4's pool: krick-1
   (projected free 25 GiB ≥ 20 GiB, domain `krick` ≠ `pi5-east`),
   pi5v1 (15 GiB < 20 GiB — ineligible). Ordered by projected free
   descending: krick-1.
3. Emit:

```
TierMove {
  fragment: (ws-court, cases, 34386b55…26d0, key_bucket, 5, gen 9, L4)
  source:      { node_id: "pi5v3", process_incarnation: 00…0c,
                 storage_incarnation: 00…c1, ownership_epoch: 5 }
  destination: { node_id: "krick-1" }    # advisory; no incarnation minted
  from_tier: "archive"  to_tier: "archive"
  bytes: 21474836480
  plan_digest: "96f514b7…d48e"  control_revision: 41
  observation_epoch: 119  policy_fingerprint: "5858b135…08eb"
}
```

`from_tier == to_tier` is correct: the move repairs the floor, the
classification is unchanged. The destination is an advisory target
description; the storage incarnation of the copy it proposes does not
exist and is not named.

### E13. Capacity refusal

Fixture M continues: bucket 6 needs one copy of 18 GiB. After the
bucket-5 move is planned onto krick-1, projected free bytes are
krick-1 = 25 − 20 = 5 GiB and pi5v1 = 15 GiB; both are below 18 GiB.
No candidate remains, so the plan records a refusal instead of a move:

```
plan_tiers: tier "archive" floor is 2 for partition key_bucket=6, but no eligible destination in leaf L4 has capacity: krick-1 has 5368709120 free after earlier planned moves, pi5v1 has 16106127360 free, the fragment needs 19327352832
```

The plan still contains the bucket-5 move; the refusal is reported
per violation, and the emitted prefix is a complete plan for what it
covers (§4 step 5). Re-running the same snapshot emits the same move
and the same refusal, byte-identically.

## 11. Review dispositions

The 2026-09-08 operator review of the 2026-09-07 draft, and its
second pass over the first amendment, resolved the original checklist
as follows; the section numbers are this revision's.

1. **Identity** — superseded: the partition identity is the
   resource-scoped quintuple of §2; movement names fragments and
   copies; copies and reports bind the committed owner/history
   (`ownership_epoch`) and storage incarnation with equality checks
   in both directions.
2. **Carrier** — retained: per-(partition, shard, leaf) reporting on
   `ReportShard`, bound to process incarnation, storage incarnation,
   source generation, and ownership epoch, with explicit aggregation
   rules (§3).
3. **Attribution** — retained for v1 (confined scans only), with zero
   defined as zero *qualifying* scans; the band's signal is a carried
   counter (`scans_observed`) over a cohort-aligned window, not a
   derivative of byte throughput (§3, §5).
4. **Refuse-on-stale** — retained, with the measured-idle/unknown
   distinction explicit so cold-but-measured partitions still
   classify (§3, E3).
5. **Band units** — scans per second per resident byte, kept, in
   fixed-point nanos with exact integer-edge classification and
   checked aggregate bounds (§5).
6. **Movement scope** — retained: per (partition, leaf) fragments
   inside the leaf's node set (§2, §8, E11).
7. **Floors** — per tier, counted in complete replicas with coverage
   and source-version evidence, with the old-tier/new-tier transition
   point defined (§7, E8).
8. **Golden fixtures** — pinned literals in §10, recomputed by the
   committed executable verifier
   (`scripts/verify_capacity_tier_fixtures.py`); the fixtures are
   reviewed expectations, never regenerated to match a change. The
   cross-architecture ARM/x86 golden run is **pending**: it is a
   property to demonstrate on the implemented planner, with exact
   commands and outputs recorded here once they exist, not a claimed
   result.
9. **Refusal texts** — the §3, §5, §6, §7 texts are the set the
   executor's log must preserve verbatim.
10. **Policy location** — resolved: the policy is declared in the
    shard map beside `[placement]` and `[[derived]]` as bootstrap or
    explicit-update input; the effective policy is committed with its
    `policy_revision` in the authority and returned in every frozen
    context; reloading the local file never silently overrides
    committed policy (§5).

**Next bounded task (per the review's handoff).** With these semantics
fixed and the fixtures verified by the committed script: implement
bounded observation collection (cohort-aligned reports on
`ReportShard`, the stored-set rules of §3) and a pure dry-run planner
against committed input snapshots, with §10's literals as the golden
tests. Move execution, ownership changes, transport tag allocation,
and further fleet operations stay out of that task; source-authority
activation remains on the foundations track.
