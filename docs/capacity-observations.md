# Capacity observations as committed state

Status: implemented 2026-09-08 in `src/source_authority/capacity.rs` (store
format 3), tests in `src/source_authority/capacity_tests.rs`. This is the
persistence side of the [capacity-tier planner](capacity-tiers.md): the
planner's frozen snapshot (`TierSnapshot`) is built from one consistent read
of committed rows, and `TierSnapshot::validated` stays the only validator of
that input. The adapter transcribes; it repairs nothing.

## Rows (store format 3)

Format 3 adds four tables and a `capacity` header to the format-2 store and
is adopted at open from format 1 or 2 in one transaction that creates only
the missing tables and headers. Any other table set or header refuses.

| Table | Key | Value |
|---|---|---|
| `capacity_configure_operations` | actor-scoped operation key (shared `command_id` namespace with control and import commands) | `CapacityConfigureOperation`: command, digest, decision |
| `capacity_state` | resource | `CapacityConfiguration`, observation epoch, configuring revision, counts |
| `capacity_reporters` | resource + node id | current process incarnation |
| `capacity_observations` | resource + the planner's canonical identity tuple | the `CapacityObservation` exactly as validated |

Recovery decodes every row, recomputes counts against both headers, checks
every stored observation's key against its value, requires every reporter and
observation row to belong to a configured resource, requires each resource's
state to equal its latest accepted configure decision, and rebuilds Kimi's
`ObservationStore` from the rows under the committed view: every retained
observation must land again under the same rules, or the open refuses
(`DataLoss`). A later command that changes the committed view (topology,
ownership) must drop the observations that no longer validate in the same
transaction; this is an invariant the map feed work must keep.

## Commands

`configure_capacity(principal, CapacityConfigureCommand)` is a control
command: current resource Admin, revision CAS, a retained retry decision,
advances the control revision. It requires applied control state for the
resource, refuses an invalid tier policy or bounds outside `1..65536`
records, `1..64 MiB`, `1..4096` reporters (envelope refusals, never
decisions), records a `FailedPrecondition` when the cohort would change while
observations are retained or when the new bounds are below what is held.

`capacity_transition(principal, CapacityTransition)` applies one observation
transition — `Register { node_id, process_incarnation }`, `Report {
observation }`, `Expire { now, max_age }` — under the committed configuration
and view. Transitions advance the observation epoch, never the control
revision, and are idempotent by content: an identical report is `UNCHANGED`,
a re-registered incarnation is a no-op, a repeated expiry drops nothing. They
carry no retry record; a refusal is returned with Kimi's text (route, field,
carried value, committed value) and stores nothing. Admission is current
resource Admin for every transition in this release; node-authenticated
reporting is a later step. A transition that changes nothing commits nothing.

The rules are Kimi's, applied by rebuilding the observation store from the
resource's rows inside the transaction and running the transition through
it; the rows then record the outcome (a landed report replaces the row under
its key; a new incarnation drops the node's older rows; an expiry drops rows
older than the bound). The rebuild is bounded by the configured limits.

## The committed view

`derive` transcribes the applied control rows of the resource into the
planner's `CommittedView`:

- shards from the committed replica rows; the primary replica's generation
  is the shard's source generation; a shard with no primary or several is
  refused by name, not repaired;
- the shard's leaf from the current topology: the route whose hash range is
  the primary's names a placement code, resolved against the committed tree;
  a resource with `no_placement` has no leaf coverage, so every report
  refuses as "covers no rows in leaf";
- pool = the shard's replica nodes; copies = one per replica with the node's
  committed failure domain, `complete = false`, zero storage incarnation and
  coverage digest: legacy replicas carry no verification evidence, and the
  planner counts them as partial;
- ownership epoch 0: no committed ownership transition has happened since
  the import;
- derived fingerprint from the committed declaration (`no_derived` is the
  zero fingerprint), placement tree and provider geometry digests over their
  canonical bytes (zero when explicitly absent).

Node records: residency as committed (the legacy node's own declaration),
eligible iff active, server-resident and the committed lease is unexpired at
the planning instant, failure domain and capacity from the committed report.
Imported leases are observations; eligibility here grants nothing.

## Planner input

`planner_input(principal, key, PlanningContext) -> TierSnapshotInput` reads
the headers, policy, capacity state, control rows, reporters and observations
in one read transaction and transcribes them with the caller's planning
instant, bounds and fragment requests. Identical committed inputs give an
identical input, plan digest and plan bytes after reopen (tested), after a
crash on either side of a configure or report commit (tested), and the plan
digest changes when the access policy revision or the tier policy changes
(tested).

## What this does not decide

- Node-authenticated reporting and per-node grants.
- Verified complete copies (coverage digests, storage incarnations) as
  committed facts; they arrive with publication receipts.
- Ownership epochs above zero; the map feed publishes them.
- A cached observation store; the per-command rebuild is bounded by the
  configured limits and is the simplest faithful form.
