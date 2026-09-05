# Review of the replicated control authority design, 2026-09-05

Review of [raft-control-design.md](raft-control-design.md) at main
`f150add`. Reviewer: the relay and budget side. Verdict: **accepted with
the answers and additions below; nothing blocks the sequence it proposes.**
The single authority stays through the relay and budget merges, as the
design says.

## What the review checked in code

- **An unreachable primary is replaced today without a fence.** In
  `src/control_plane.rs` an expired lease schedules `CopyReplica` with the
  reason "replace expired primary" and completes through
  `PromoteReplica`; `docs/cluster-control.md` states the rule. So the
  answer to question 4 is yes: that path activates a replacement writer
  on lease expiry alone, and it must sit behind the design's activation
  gate. Until the durable per-shard write epoch exists, the first Raft
  release should leave that action unavailable, exactly as the design
  allows, and the current single authority should be read the same way
  in its documentation.
- **The balance dry run reads live state it must not read under Raft.**
  On `feat/scan-budget-2026-09`, `balance_context` takes the provider
  geometry (dimensions, bits per component) from `ClusterHealth` through
  the attached coordinator, and the per-shard node pools from the
  coordinator's placement tree. Both are network reads at plan time. The
  answer to question 3 follows.
- **The existing data-plane fences** are `expected_wal_generation` on
  deletes and replacements, `required_topology_generation` on routed
  ingest, and `check_stats_epoch` on scoring. The per-shard write epoch
  belongs at the same commit boundaries in `src/node.rs`.

## Answers

1. **`PublishedMap` covers the relay** if each route carries, beside the
   address, replica, slot base, hash range and placement code, the
   **node identity and incarnation** of its owner. The relay's epoch
   token is bound to the tuple (collection, child identities,
   incarnations, ranges, child statistics epochs); the map is where the
   first three come from. The design's "activated ownership epoch" per
   route can serve as the incarnation input if it changes whenever the
   owner process does. The relay needs no lease, action or catalog state.
2. **Yes, one abstraction.** The relay branch consumes the map through a
   minimal interface: `current()` returning the control revision, the
   topology generation and an immutable map, plus a change subscription
   resuming after a revision. It is implemented today over the
   file-polled map with the generation as the revision. A learner's
   applied feed and a narrower authenticated subscription both fit behind
   it; the relay pins the revision under which each decision was made and
   refuses when the current one differs. Documented under "Map interface"
   in `relay-coordinators.md` on that branch.
3. **Budget state that must be committed:** per collection, the provider
   geometry (dimensions, bits per component, the encoded row byte
   formula's version) so bytes are computable without a network read;
   the placement tree and shard codes (already in the map); per node, the
   capacity fields 6 to 10 (rate, observation time, samples, window,
   residency) as observations with their lease; node state (draining,
   expired); failure domain. Request inputs (`min_gain`, `max_moves`,
   `max_rate_age_ms`) stay request inputs and are echoed on the plan,
   which already reports the control revision. The planner's version and
   tie-break rule travel in committed policy, as the design requires.
   Action for the budget branch after merge: geometry becomes committed
   collection state, written at bootstrap or import and updated by the
   backend and calibration broadcasts, and the dry run reads it from the
   applied state.
4. **Yes**, see above. `PromoteReplica` after an expired lease is that
   action.
5. **Sufficient, with one rule added.** A reset after compaction carries
   a complete map with an explicit reset marker, and the consumer must
   keep in-flight queries on their pinned generation across the reset:
   a reset that does not change the topology generation must not cancel
   a query. Same revision with different content needs the digest on the
   subscription frames, not only on snapshots, so the consumer can detect
   it without a full compare. With those, the consumer tests the design
   lists are enough.

## Additions requested before implementation

- **The write epoch check is data-plane work and I will own it** at the
  three commit boundaries named above, keyed by the epoch in
  `PublishedMap`, once the map carries it. It is one check per boundary
  and an existing refusal shape (`FAILED_PRECONDITION`, named).
- **Deduplication without expiry** is accepted for the first release,
  with a configured capacity, a refusal by name when it is reached, and a
  gauge on the metrics page so an operator sees it coming.
- **Subscriptions for relays** authenticate with the relay's cluster
  certificate like every internal route; a relay is never a learner
  unless an operator lists it as one.
- **Geometry in state** (item 3) is a small change to the budget branch;
  I will make it after that branch merges, before the migration audit.

## Agreed without change

Pure transition function shared by the single-authority adapter; leader-
supplied observation time; no bootstrap from an empty store; fail closed on
a corrupt store; separate names for control snapshots; one group per
administrative cluster with three server voters; phones as neither voters
nor learners; the migration marker that forbids the old authority from
restarting; the gate list.
