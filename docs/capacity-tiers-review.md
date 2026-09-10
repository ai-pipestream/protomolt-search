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

## Review of amendment `3456c20` (2026-09-08)

The amendment improves resource identity, replica evidence, recorded planning
inputs and the advisory boundary. Independent recomputation confirms both
155-byte policy hashes (`5858b135…08eb` and `449751c3…e2e7`), the base rate of
12,417 nanos and the stated UTC planning instant. The earlier review is not a
request to undo those changes. The following contradictions still prevent
using the current examples as accepted planner goldens.

1. **E8 violates its own replica floor.** The fixture assigns the three server
   nodes to only two failure domains (`krick`, `pi5`, `pi5`), while `hot` requires
   three distinct-domain complete copies. Its `moves: []` explanation says the
   floor is met merely because three nodes hold the partition. It also omits
   complete-copy evidence and the serving-replica observations required by its
   own coverage rule. Provide a complete valid fixture with three domains and
   verified copies, or expect an unsatisfied-floor refusal under the original
   fixture. Node count must not stand in for proven replicas.

2. **The warm example is hot under the written bands.** At 60,000 scans,
   `floor(60000 * 10^12 / (600000 * 402653184)) = 248`. The policy's hot band is
   `[100, 10^12)`; warm is `[1, 100)`. Correct the expected tier or choose a
   different literal input. Preserve the boundary rules and recompute affected
   fixtures deliberately.

3. **Windows and fragment aggregation remain underspecified.** Reports allow
   independent window starts/ends, but the aggregate formula sums all counts
   and divides by one `W`. For example, 60 scans in 60 seconds and 60 scans in
   600 seconds cannot be combined as 120 scans over an unspecified common
   duration. Require an aligned window cohort, or define exact per-reporter
   normalization and its coverage/freshness rules. Also reconcile per-partition
   aggregation in section 3 with per-leaf classification in section 8: reports
   need enough committed coverage information to attribute bytes, scans and
   warmth to the planned fragment. A report for a shard spanning leaves cannot
   be copied wholesale into every leaf. Add unequal-window and multi-leaf
   examples. Zero confined scans must mean zero *qualifying* scans, not a claim
   that no broad query touched the partition.

4. **Concrete ownership remains part of the foundations interface.** The new
   fragment/copy tuples omit the logical owner/history requested in the original
   review. Observation keys omit storage incarnation even though copy identity
   includes it. Bind observations and coverage evidence to the exact current
   owner/history/storage and source generation; comparing only a node/process
   and an integer generation is insufficient to distinguish replacement bytes.
   Require equality with the committed generation, including refusal of future
   generations, rather than only rejecting older ones. A dry run must not mint
   the destination's purportedly reserved storage incarnation: use an advisory
   target description until a future execution protocol actually reserves it.

5. **A deterministic move algorithm still needs a defined objective.** Section 4
   says to choose the move that lowers “the objective” most, but never defines
   that objective, candidate costs or destination capacity constraints. All
   example tiers use SERVER, so changing a tier name alone says nothing about
   which server to prefer. Specify the v1 optimization target, eligibility and
   byte/work limits, stopping rule, and complete tie-break key (including
   fragment/source where necessary). Add at least one fully specified nonempty
   move plan and a capacity refusal. Otherwise different reasonable planners
   can satisfy the prose and emit different bytes.

6. **Separate computed fixtures from cross-architecture proof.** Nothing is
   implemented according to the document's opening, yet section 11 and the
   handoff say golden plans were proven on ARM and x86. The explicit policy
   encoding is independently reproducible; the full observation/context byte
   transcripts and verifier are not supplied alongside their hashes. Commit
   those transcripts or a deterministic fixture verifier with all inputs and
   widths specified. Describe the architecture run as pending until exact
   commands, outputs and tested implementations are available. A design's
   all-integer arithmetic does not by itself establish an executed cross-arch
   result. Define checked aggregation bounds as well as per-report bounds so
   the `u128` argument applies to aggregate `S` and `B` too.

The next bounded assignment is to repair these semantics and fixtures and add
an executable, independent fixture verifier. Sol can write that verifier;
Terra runs it under the agreed memory cap and Astra reviews the literal output.
After the corrected examples agree with the rules, proceed with bounded
observation collection and the pure planner. Keep transport allocation,
movement execution, ownership changes and fleet work outside this assignment.

For policy placement, use a declaration beside placement/derived configuration
as the bootstrap or explicit update input. The effective policy must then be
committed with its revision in the authority and returned in the frozen context;
reloading a local shard-map file must not silently override committed policy.
This resolves the configuration role without creating a second live authority.
