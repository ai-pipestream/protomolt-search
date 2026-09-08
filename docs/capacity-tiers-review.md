# Capacity-tier contract review

Reviewed 2026-09-08 against `docs/capacity-tiers.md` on main `02150a7`,
which is contained in the foundations checkout at `60525ae`. This is a source
and design review, not a repeat fleet measurement or an implemented tier API.

The separation of logical partitions, placement pools and capacity policy is
sound. Retain explicit device exclusion, refusal of unknown residency, immutable
declaration identity, copy-before-drop intent, and the dry-run-only boundary.
The draft needs the corrections below before allocating protocol fields or
implementing the planner. None requires a fleet rollout.

## Resource and copy identity

The triple (declaration fingerprint, column, bucket) is a partition identity
*inside an explicit resource*. Equal declarations in two collections produce
equal triples but do not authorize combining their rows. Bind workspace and
collection in the request, stored observation key, response and plan digest.
A movement fragment must additionally name its logical owner/history, leaf
within the relevant tree generation, and source/target storage incarnations.
A tree's leaf label or a network address is not a globally stable owner.

Define what constitutes one complete replica of the planned fragment. Two
nodes holding disjoint rows of a bucket are not two replicas of that bucket;
two relay routes to the same owner are not two copies. Replica floors need
coverage and source-version evidence for the same fragment, with unique
eligible owners/failure domains. Node `ready` flags and row counts alone do
not establish complete, matching copies. A dry run can describe its assumptions;
execution needs the foundations track's verified owner/readiness contract.

## Observations must supply the proposed signal

The proposed tier band uses scans per second per resident byte, but the
observation carries throughput in bytes per second and a generic sample count.
These do not yet define that rate. Specify a scan counter and exact window,
or choose a signal actually carried by the observations. State the treatment
of zero resident bytes, zero observed scans, overlapping windows and retries.

Distinguish a measured idle partition from an unobserved partition. Otherwise
the strict stale/unknown rule can prevent classifying precisely the cold
partitions the feature should describe. Preserve explicit unknown values; do
not turn missing telemetry into zero workload.

Aggregation from partition-in-shard reports into partition/node/leaf planning
inputs needs explicit rules. Counts and bytes need coverage/deduplication rules;
p50/p99 values cannot be merged by simply adding or averaging their percentiles.
Retain per-shard measurements or define a mergeable distribution. Bind reports
to node/process incarnation and shard/source generation so a restarted process
or reshard cannot refresh an obsolete observation. The draft's replica and
observation identities must agree about which rows each measurement describes.

## Freeze time and all effective planning inputs

The stated three-input function omits request limits and the time used for
freshness/warmth. Record a planning instant and all resolved limits in the input
snapshot and returned context, including defaults. Planning reads that frozen
snapshot; it must not mutate an observation epoch as a side effect of reading
the local wall clock. Persist ordered time observations/expiry transitions
where they affect committed state. Define future timestamps and clock-skew
refusals with checked arithmetic.

The context also includes the authority identity/incarnation, committed control
and policy revisions, observation-set digest, full topology/tree, collection
declaration and provider geometry. A topology generation alone does not bind
all of those values. This follows the
[authority convergence boundary](raft-control-design.md#single-authority-convergence-boundary-2026-09-08);
live coordinator lookups are not an extra hidden planning input.

## Canonical policy and deterministic numeric rules

`TierPolicy.policy_fingerprint` cannot be included recursively in its own hash.
Hash a versioned policy definition with that computed field excluded, use a
specified domain and encoding, and define normalization. Preserve tier order
because it is precedence, not map insertion order. Pin literal hash fixtures.

Fixed evaluation order alone is an incomplete numeric contract. Define finite
bands, invalid/overlapping intervals, signed-zero handling and overflow behavior.
Use specified fixed-point/rational units where practical; if doubles remain,
specify their evaluation and serialization rules and reject unsupported values
by name. Prove golden plan bytes on both the ARM and x86 implementations.
Golden fixtures are reviewed expectations, not regenerated to match a change.

## Advisory plans and future execution are different contracts

The draft says every move carries a control revision, but `TierMove` omits it.
It also lacks concrete source/destination copy identities and the policy
identity needed to check a move independently. Reconcile the prose and draft.
Include the effective request inputs in the plan's identity; otherwise different
limits may yield different plans with the same stated input identities.

Strictly comparing every step to the original control revision would make a
multi-step plan stale after its own first committed move. Keep v1 advisory.
Before introducing execution, specify a retained operation/step identity,
actor-scoped retries, expected step state, reservations and revalidation rules.
A dry-run digest is not an execution credential. Unrelated observation or
topology changes must not silently reinterpret an accepted step.

## Floors are checked against explicit assumptions

A dry run can prove that its own proposed sequence preserves a floor under
the captured state. It cannot guarantee survival of unrelated concurrent
failures. Define old-tier versus new-tier floor obligations and their transition
point; otherwise applying both floors throughout can make every departure
impossible. Execution must revalidate unique verified copies, failure domains,
capacity reservations and current authority before dropping a source.

Device-local sources remain excluded from export, replication and server
replacement at both planning and execution boundaries. Collection Admin or
cluster trust must not override source residency.

## Handoff

The next bounded task is to amend the contract and create literal examples of
the complete inputs and expected classifications/plans. Include cross-collection
identical declarations, partial copies, measured idle versus missing data,
staleness boundaries, restarted reporters, duplicate reports, policy hashing,
permuted input order and multi-step revision changes.

After those semantics are fixed, implement bounded observation collection and
a pure dry-run planner against committed input snapshots. Keep move execution,
ownership changes, transport tag allocation and further fleet operations out of
that preparatory task. Source-authority activation remains on the foundations
track. This review does not approve the current draft as an execution protocol.
