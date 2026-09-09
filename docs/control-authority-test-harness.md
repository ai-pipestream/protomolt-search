# Control-authority adversarial test harness (slices 1, 2 and 3)

Coordination record and design for the adversarial integration-test harness
around the source-authority control plane. This is test-only work: it adds
`tests/control_authority_adversarial.rs` (slice 1),
`tests/control_authority_model.rs` (slice 2a),
`tests/control_authority_planner.rs` (slice 2b, replaced by slice 3b) plus
the shared kit under `tests/control_adversarial/` and this document. No
production source, proto, or Cargo manifest changes.

## Checkpoint and branch

- Worktree: `psearch-wt-adversarial` (task worktree of `protomolt-search`).
- Branch: `test/control-authority-adversarial`.
- Base commit: `43596c9` (merge of `70bcbf8`, capacity tiers), the agreed
  checkpoint with Fable, who owns all production authority, storage-contract
  and Raft code. The harness builds only on the public API at that commit;
  production files are untouched here.

## Ownership boundary

- Fable owns: production authority code (`src/source_authority/*`,
  `src/control_plane/*`), storage contracts, Raft,
  `VerifiedOwnerCompletion` construction, fault-injection internals
  (the crate-private `Fault` / `StateWriteFault` hooks and the
  `#[cfg(test)]` unit suites such as `import_tests.rs`,
  `retirement_tests.rs`, `admission_tests.rs`). None of that is modified or
  depended on here.
- This harness uses only the public interfaces (below) and exercises them
  from a separate integration-test target. In-transaction fault injection and
  the READY execution path stay crate-internal and remain covered by Fable's
  own unit tests.

## Agreed public test surface

All confirmed against commit `43596c9`:

- `pipestream_search::source_authority::SourceAuthorityStore`
  - `create(path, &SourceAuthorityIdentity, &AccessPolicy, &SourceAuthorityLimits)`
  - `open(path, &expected_identity)` — recovery; refuses while a live clone
    holds the file lock (drop all clones first).
  - `execute(principal, &SourceAuthorityCommand) -> Result<SourceAuthorityDecision, Status>`
    — refuses `ConfirmReady` actions with `PermissionDenied` before any
    validation.
  - `decision(principal, key, command_id)`, `owner(principal, key)`,
    `policy(principal, workspace, collection)`,
    `authorize(principal, collection, action)`,
    `admission(principal)`, `confirm_owner_ready(principal, &command, &verified)`
    (the last requires a `VerifiedOwnerCompletion`, which only the hosting
    adapter can construct — a compile-time boundary this harness cannot and
    does not cross).
  - `retire_legacy_control(principal, &LegacyControlRetirementRequest, &DurableControlPlane)`
  - `recover_legacy_retirement(principal, &request, path)`
  - `control_snapshot(principal, key) -> ControlCollectionSnapshot` whose
    `digest` is the public determinism statement (32 bytes).
- Staged import (`pipestream_search::source_authority`, re-exported from
  `src/source_authority/import.rs`): free fns `payload_digest`, `chunk_digest`,
  `retirement_digest`, `plan_chunks`, `chunk_capacity`, `MIN_CHUNK_CAPACITY`
  (4096); store methods `begin_control_import` (Begin only; Begin through
  `execute_control_import` is `PermissionDenied`), `execute_control_import`
  (Chunk/Commit/Abort), `control_import_decision`, `control_import_workflow`.
  Retry semantics: exact (actor, key, command_id) retry with identical bytes
  returns the stored decision with no new revision; the same id with changed
  content is `FailedPrecondition` (not recorded). Current permission is
  checked before the retry lookup, so a revoked actor's retry is
  `PermissionDenied`; a regrant returns the old stored decision without
  reapplying. A new id restaging identical content under an already-staged
  ordinal is a recorded `AlreadyExists`; differing content under a staged
  ordinal is a recorded `FailedPrecondition`. Workflow ids are single-use
  tombstones: reuse is a recorded `AlreadyExists`.
- Legacy plane (`pipestream_search::control_plane`): `ControlPolicy` (pub
  fields), `DurableControlPlane::open(path, policy)`,
  `open_existing(path, policy)`, `.with_collection(name)`,
  `.bootstrap_topology(generation, &[TopologyRoute])` (pristine planes only;
  every route needs `hash_range`), `checkpoint_for_import()`.
  `pipestream_search::coordinator::TopologyRoute { addr, replica, hash_range: Option<(u64, u64)>, placement }`.
  `LegacyControlCheckpoint::decode(&bytes)` with `.policy()`, `.state()`,
  `.sha256()`. After retirement, `open_existing` on the retired path fails
  with "legacy control authority is durably retired for import".
  `RetiredLegacyControl` (not Clone/Debug): `.record()`, `.checkpoint_bytes()`;
  holding it keeps the legacy file lock.
- Wire types: `pipestream_search::pb::storage::*` (commands, decisions,
  receipts, `LegacyControlRetirementRequest`, `ControlImportPayload`,
  `LegacyControlImportSupplement`, ...); `pipestream_search::pb::{AccessAction,
  AccessPolicy, CollectionGrant, CollectionResource}`.
- `pipestream_search::sha256::digest(&[u8]) -> [u8; 32]`, `to_hex`.
- Capacity adapter and transitions (added for slice 3b, confirmed against
  `331c18f` + `b1bbe51`): `SourceAuthorityStore::configure_capacity(principal,
  &CapacityConfigureCommand) -> CapacityConfigureDecision` (a retained
  control command over applied state: it advances `control_revision` on
  acceptance and returns refusals as recorded decisions with a nonzero gRPC
  code, e.g. a cohort shift with observations retained is a recorded
  `FailedPrecondition`), `capacity_transition(principal, &CapacityTransition)
  -> CapacityTransitionReceipt` (Register/Report/Expire; idempotent by
  content — an exact report retry is `UNCHANGED`, a re-registration repeats
  the same receipt, a repeat expiry drops 0 — refusals are `Status`, never
  stored, and no transition moves `control_revision`), and
  `planner_input(principal, key, &PlanningContext) -> TierSnapshotInput`, the
  real persisted adapter: every input field except the planning context
  (instant, move/age/skew bounds, fragment requests) is derived from
  committed authority rows. `capacity_state(principal, key)` exposes the
  reporter/observation counts and the observation epoch.
- Cluster fixture path (slice 3b): node registration and shard reports are
  cluster routes, not source-authority commands. The kit drives them through
  `pipestream_search::control_plane::ClusterControlService::new(plane)` and
  the `pipestream_search::pb::cluster_control_server::ClusterControl` trait
  (`register_node`, `report_shard`) on a short-lived in-process tokio
  runtime, then reopens the plane with `open_existing` for retirement.
  `lease_ms` is clamped to 300,000 ms by the plane; each `report_shard`
  reconciles and publishes, so the committed topology generation after the
  import is read back from the snapshot, never assumed.

## Harness design

### Kit (`tests/control_adversarial/kit.rs`)

Mirrors the in-crate `src/source_authority/import_tests.rs` helpers
(`legacy`, `store`, `retire`, `payload`, `supplement`, `command`,
`run_import`) through the public API only:

- `TestDir` — hand-rolled temp dir (`std::env::temp_dir()`, pid + nanos in
  the name), `create_dir_all`, `Drop` removes it. No tempfile crate.
- Fixed fixtures: identity seed 7; policy = alice+bob `Admin` on workspace
  `test`, collection `books`; `limits` with an 8 KiB `max_command_bytes`
  (chunk capacity ~6 KiB); a 2-route legacy plane bootstrapped at generation 1
  (routes tile the u64 hash space); minimal valid supplement mirroring
  `import_tests.rs` (`NoPlacement`, `NoDerived` with empty fingerprint,
  `Geometry` provider, `ControlPlannerPolicy` carrying the checkpoint's
  captured policy, `history_placement_unavailable: true`).
- Import plan: `plan_three_chunks` declares `chunk_bytes =
  ceil(payload / 3)` so even the small 2-route payload stages exactly three
  chunks (the protocol only requires `chunk_bytes <= chunk_capacity` and
  `chunk_count = ceil(payload_bytes / chunk_bytes)`).
- Process injection: `spawn_worker` re-runs the same test binary with
  `--exact <test> --nocapture --test-threads=1` and env vars (the
  `tests/multiprocess.rs` idiom); `kill9` sends `/bin/kill -9` and asserts
  the child died by SIGKILL; `wait_marker` polls a marker file with a bounded
  timeout and fails loudly.

### Worker/parent SIGKILL injection (suite test g)

The worker (`sigkill_import_worker`, a `#[test]` in the same target that
early-returns unless `PSEARCH_ADV_WORKER` is set) builds the store and the
2-route legacy plane, retires it, then stages three chunks and commits. It
writes marker files `m0..m5` (after retire, begin, each chunk, commit) and,
when armed (`PSEARCH_ADV_ARM = k`), busy-waits for a `go` file (up to 120 s,
then exits cleanly so a lost parent never leaks a process — a missed kill
fails the parent's SIGKILL assertion loudly). The parent therefore controls
the exact kill point between durable steps.

For each `k in 0..=5`: fresh dir, spawn the worker armed at `k`, wait for
`mk`, `kill -9`, reap, then recover through the public API:
`SourceAuthorityStore::open` (revalidates all accounting), decode the
retirement request the worker persisted, `recover_legacy_retirement`, and
re-issue the whole command sequence (same ids, same revisions). Exact
retries return stored decisions; missing steps apply fresh. Assertions: the
commit succeeds exactly once with a receipt; the final `control_snapshot`
digest equals a no-crash baseline computed once in the parent; the exact
retry of the final commit returns the stored receipt decision; and the
retired legacy path refuses `open_existing` with the durable-retirement
message. For `k = 0` (killed after retire, before begin) the begin simply
applies fresh.

### Determinism via snapshot digest

Every assertion that matters is reduced to the 32-byte
`ControlCollectionSnapshot.digest`: reopen stability (test a), permuted chunk
staging (test c), and crash recovery at every boundary (test g) all compare
digests, so the harness proves state equivalence without depending on
internal encodings. All inputs are fixed seeds and byte-exact command
sequences; the test output records the operation trace (markers and
revisions) for post-mortem replay. There is deliberately no randomness; if a
future slice needs pseudo-randomness, hand-roll a xorshift64 in the kit (no
new dependencies).

## Evidence scope: local, not Raft

This harness is LOCAL single-process replay/recovery evidence: one redb
authority file, one retired legacy JSON plane, real SIGKILL between durable
steps, recovery through the public API. It is not multi-node Raft evidence —
there is no leader change, log replication, or snapshot installation across
processes here. When Fable lands the Raft interfaces, this harness extends:
worker set per replica, kill the leader mid-import, and assert the imported
collection's snapshot digest on the new leader equals the local baseline.

## Current limitations

- The READY execution path (`confirm_owner_ready`) and in-transaction fault
  injection are crate-internal; this harness covers only the public
  refusals (`execute` with `ConfirmReady` → `PermissionDenied`) and the
  crash boundaries at durable-step granularity. Mid-transaction kills are
  Fable's `Fault::ExitBefore/AfterCommit` unit tests.
- Kill granularity is one marker per completed call; a kill cannot land
  inside a single `execute_control_import` call from here.
- The supplement is the minimal valid 2-route shape (`NoPlacement`,
  `NoDerived`, `Geometry` provider); placement-tree and derived-column
  imports are covered by Fable's unit suite, not duplicated.

## Slice 2a: differential model + seeded fuzzer

`tests/control_adversarial/model.rs` is an independent reference model of
the documented control rules, written from `docs/source-authority-storage.md`
and `docs/control-import.md` ONLY — it is deliberately not derived from
`src/source_authority/import.rs`. State: Admin set, policy/control revisions,
a retry journal keyed by (actor, owner id, command id, namespace) with content
bytes and the recorded code/revisions, owner preparations (workflow,
generation, prepared/cancelled), used owner-workflow ids, import workflows
(staging ordinal → bytes, or terminal), and an applied-import flag per
resource. `apply()` implements the documented decision order: current
permission (unrecorded `PermissionDenied`, checked before retry disclosure) →
Begin holder-actor binding (unrecorded) → cross-namespace id reuse (unrecorded
`FailedPrecondition`) → retry lookup (verbatim stored decision; changed
content is an unrecorded `FailedPrecondition`) → recorded CAS on the
control/policy/ownership-generation revisions → per-action transition rules,
with a self-revoking `ReplaceGrants` modeled as `Suppressed` (decision
recorded, revisions advance, the caller sees `PermissionDenied`). Every code
choice the docs do not name exactly is marked `AMBIGUOUS` with the chosen
reading and a doc-line citation in a comment.

`tests/control_authority_model.rs` is the differential fuzzer over that
model, deterministic via a hand-rolled xorshift64
(`tests/control_adversarial/rng.rs`). Pinned seeds 1..=24 (`0x5EED_0000 + n`,
printed per seed; `PSEARCH_ADV_SEEDS=<end>` extends the range;
`PSEARCH_ADV_TRACE_DIR=<dir>` keeps per-seed op traces). Per seed: fresh
`TestDir`, store, 2-route legacy plane, retire (alice), payload; 60 ops at
roughly 65% model-guided / 35% adversarial (a non-admin actor, stale
revisions ±1, ids reused from a 24-id pool, out-of-phase import steps,
flipped chunk bytes), plus exact (~45%) and mutated (~55%) retries of earlier
ops at about 15% of steps. Every op runs against the real store and the
model, comparing `Err(code)` vs `Status`/`Suppressed` and the decision code
plus both revisions. Mid-trace recovery at op 20 and op 40 (drop store and
holder, reopen, `recover_legacy_retirement`; a current admin regrants alice
first as a real compared op if she was revoked). After each reopen and at
trace end: sampled `decision()` / `control_import_decision()` checks
(verbatim codes+revisions for admins, `PermissionDenied` for revoked or
never-granted actors) and deferred verification that suppressed decisions
reappear verbatim after a regrant. A mismatch panics with the seed, op index,
and the full serialized trace.

Finding of the slice, resolved per protocol: seed `0x5eed0004` op #15 —
Cancel on a never-prepared owner. The model said recorded
`FailedPrecondition`; the real store records `NotFound` (and uses
`FailedPrecondition` only for a wrong-workflow cancel on an existing
preparation, `src/source_authority/transition.rs:120-131`). The docs name no
code for this case; `NotFound` for a missing resource is the canonical
reading and no doc contradicts it, so the model was fixed (comment cites the
doc line and the distinction). After the fix the pinned 24-seed set passes,
and an extended env-only run of 600 seeds (~37k ops, ~26 s) passed with zero
further mismatches — evidence the remaining `AMBIGUOUS` readings all agree
with the implementation. No reportable production issue; no
doc-implementation contradiction found. Runtime headroom is large (24 seeds ≈
1 s), so the pinned seed count can grow cheaply in a later slice.

Gate: model target passes under the 8 GiB, swap-disabled scope in ~1.0 s;
the slice-1 target still passes; rustfmt clean; no production changes.

## Slice 3a: administrative recovery and activation refusals (post-Raft rebase)

Rebased onto the frozen foundations checkpoint `331c18f` (Raft hosting);
the capacity-planner leg is untouched (`src/capacity_tiers.rs` is unchanged
since `43596c9`). Two new vocabulary items from the task list land in the
model and the fuzzer, plus three focused tests in
`tests/control_authority_adversarial.rs`.

Model rules added (`tests/control_adversarial/model.rs`), docs-first:

- `Recover` (import namespace, so its retry/journal keys stay separate from
  owner commands): docs/control-import.md "Administrative recovery" —
  "any current Admin of the resource may terminate a staging workflow into
  phase RECOVERED under the same revision CAS and actor-scoped retry rules".
  Unlike stage/commit/abort there is deliberately NO initiator check. A
  concurrent Commit and Recover "serialize ...: exactly one becomes the
  terminal decision, the other is a recorded refusal" — AMBIGUOUS (doc names
  no code): FailedPrecondition, the implementation's "import workflow is
  already terminal" (`src/source_authority/import.rs:887-919`, which also
  shows Recover skipping the initiator check that gates Abort/Chunk/Commit).
  Unknown workflow: NotFound, as for the other steps. The model keeps the
  terminal row with its initiating principal, never sets `applied_import`,
  and treats RECOVERED like ABORTED/COMMITTED for every later step.
- `Activate` (owner namespace): docs/source-owner-admission.md "Activation:
  the committed fence" — activation "follows READY through
  `ActivateSourceOwner { workflow_id }`, a control command any current Admin
  may issue through `execute`" and records `SourceOwnerActivation {
  write_epoch = ownership_generation, activated_control_revision }`. Refusal
  codes are unnamed in the doc: a missing owner is NotFound (AMBIGUOUS, as
  Cancel's canonical reading) and a non-READY owner or wrong workflow is
  FailedPrecondition (AMBIGUOUS; the implementation agrees,
  `src/source_authority/transition.rs:168-192`). The success branch is
  unreachable from tests — READY requires `VerifiedOwnerCompletion`, which
  is pub(crate) — so the model only holds PREPARED/CANCELLED owners and the
  fuzzer exercises the refusal paths; the READY→ACTIVE happy path is a later
  slice through the managed-catalog bridge.

Fuzzer (`tests/control_authority_model.rs`): the op mix is rebalanced —
Prepare 12, ReplaceGrants 13, Begin 8, Cancel 12, Activate 8, Chunk 18,
Commit 10, Recover 10, Abort 9 — and Recover ops carry a plausible expected
policy revision via the new `kit::import_command_policy` (the other import
steps keep the slice-1 pinning of the bootstrap revision). Mutated retries
of Activate perturb the workflow id. The seed values are unchanged
(`0x5EED_0000 + n`, pinned 1..=24); the streams shifted with the new ops,
which is expected. The extended run is re-pinned at 200 seeds
(`PSEARCH_ADV_SEEDS=200`, ~12.4k ops) with zero mismatches.

Focused tests (`tests/control_authority_adversarial.rs`):

- `recover_terminates_a_stranded_import_without_transfer` — alice begins and
  stages two chunks; bob (the other Admin, no holder) Recovers. Assertions:
  phase RECOVERED; the row keeps alice as initiator while the terminal step
  names bob; reservation released (`reserved_bytes == reserved_decisions ==
  0`); no applied control state (`control_snapshot` → NotFound); alice's
  exact retry of her last chunk returns the stored decision verbatim; the
  workflow id is spent (re-Begin → recorded AlreadyExists); second
  Recover/Abort/Commit → recorded FailedPrecondition without receipts; and a
  fresh workflow imports the resource end-to-end afterwards.
- `commit_and_recover_serialize_exactly_once` — both orderings on separate
  stores: Commit-then-Recover leaves the resource imported (Recover refuses,
  a second Begin records AlreadyExists); Recover-then-Commit leaves it
  unimported (Commit refuses with no receipt, `control_snapshot` → NotFound).
- `activate_refusals_before_ready` — mallory is refused with
  PermissionDenied before anything is recorded; Activate on a PREPARED
  owner, with the wrong workflow id, on a CANCELLED owner, and on a missing
  owner are recorded FailedPrecondition/FailedPrecondition/FailedPrecondition
  /NotFound respectively; `owner()` shows the phase unchanged and
  `activation` absent after every refusal.

No model/implementation mismatches arose in the re-pinned 24-seed set or
the 200-seed extended run — the Recover and Activate refusal codes the
implementation gives match the docs-first readings above.

Gate: `cargo check --tests --features fault-injection,raft` clean; the
model and adversarial targets pass both with `--features fault-injection,raft`
and without; the planner target passes unchanged; rustfmt clean; no
production changes, no commit.
## Slice 3b: the planner leg on the real persisted adapter

Slice 2b mapped an imported `ControlCollectionSnapshot` to the capacity
planner's input by hand: the kit built the node registry, the committed
view, the policy and the observations, and only the revisions, the resource
triple and the supplement digests came from the snapshot. That leg is
replaced (the slice-2b and slice-2c sections are removed with it). The
hand-built `kit::planner_input`, `planner_committed_view`,
`planner_observation_store`, `feed_reports` and their helpers are deleted
from the kit. `tests/control_authority_planner.rs` now feeds
`SourceAuthorityStore::planner_input` — the real persisted adapter at
`src/source_authority/capacity.rs` — and everything the planner sees is
authority-derived: the committed control and policy revisions, the authority
identity, the resource triple, the topology generation, the placement tree
digest, the provider geometry digest, the node records with eligibility,
the committed shards/leaves/copies, the retained observations and the
observation epoch. The kit supplies only the `PlanningContext` (a fixed
planning instant, move/age/skew bounds, and the fragment requests) and the
observation values.

The fixture chain, all public API:

1. `kit::legacy_plane_with_cluster(dir)` — a pristine 2-route plane (routes
   tile the u64 hash space) with two server nodes registered through the
   `ClusterControl` routes (`node-a`/rack-a, `node-b`/rack-b, the plane's
   300,000 ms lease maximum, `NodeResidency::Server`) and one ready primary
   replica reported per route: shard-a on node-a with route 0's exact hash
   range, shard-b on node-b with route 1's. Registration and reports run on
   a short-lived in-process tokio runtime; the plane is then reopened with
   `open_existing` for retirement. Each `report_shard` reconciles and
   publishes, so the committed topology generation is whatever the plane
   published — the kit reads it back from the snapshot
   (`kit::committed_generation`) instead of assuming 1.
2. Retirement and import with the placement supplement
   (`kit::placement_supplement`): the in-crate two-leaf tree ("old" with
   `year < 2020`, "rest") and one route code per route, so derive() names
   shard-a's leaf "old" and shard-b's leaf "rest" through each route's
   committed placement code. Copies derive as partial legacy evidence
   (`complete = false`, zero storage incarnation and coverage).
3. `configure_capacity` at control revision 6 (the import commits at
   revision 6: begin 1, three chunks, commit), three server-resident tiers
   with single-replica minima — every shard has exactly one committed copy —
   then Register transitions for both reporters and one Report per shard in
   the current cohort window. The planning instant is fixed at twenty
   cohorts past the epoch (phase 0), so node eligibility (committed lease
   expiry must exceed the instant) is deterministic even though the imported
   leases were captured at wall clock.

Tests (`tests/control_authority_planner.rs`, nine including the env-gated
worker):

- `adapter_input_and_plan_digest_stable_across_reopen` — the adapter input
  binds `control_revision: 7`, `policy_revision: 1`,
  `observation_epoch: 2` and the committed topology generation; reopening
  the store reproduces the committed snapshot digest and the plan view
  (input render, `TierSnapshot::plan_digest`, plan canonical bytes) exactly.
- `adapter_plan_digest_stable_across_chunk_permutation` — forward vs
  reverse chunk staging on twin stores: byte-identical adapter inputs and
  plan digests, because nodes, replicas and placement codes all come from
  the committed import, not the staging order.
- `adapter_transitions_are_idempotent_and_epoch_scoped` — an exact report
  retry is `UNCHANGED` with the epoch still; re-registration repeats the
  same receipt; a repeat expiry drops 0; transitions never move the control
  revision; a report whose leaf the committed shard does not cover refuses
  with "covers no rows in leaf" and stores nothing.
- `adapter_plan_digest_moves_with_policy_and_tier_changes` — a
  `ReplaceGrants` command advancing the access-policy revision moves the
  digest; a reconfigure with different tier thresholds (same cohort) moves
  it again; a cohort shift with observations retained is a recorded
  `FailedPrecondition` decision that moves nothing; a reopen reproduces the
  final view exactly.
- `adapter_expiry_is_deterministic_and_epoch_scoped` — the Expire
  transition (this replaces the slice-2c `commit_expiry` test, re-expressed
  through the committed path) drops both reports when `now - window_end`
  strictly exceeds `max_age`, moves the epoch once, repeats as a no-op, and
  leaves the adapter serving zero observations; planning the evicted leaves
  refuses "no current observation" instead of planning from memory. Twin
  stores produce equal receipts, equal counts and equal digests.
- `adapter_supersession_drops_old_incarnation_and_is_deterministic` — the
  Register transition (replacing the slice-2c `register_incarnation` test)
  supersedes the old process incarnation: its rows drop in the same
  committed transition (`dropped == 1`), its late report is refused naming
  the new incarnation, the replacement process reports and lands, and twin
  stores end with equal receipts and equal plan views.
- `adapter_refuses_reports_outside_the_committed_resource` — a report of
  another collection refuses with "is not this resource" and a report a
  topology generation ahead refuses with the identity error, both through
  the committed transition path; a corrected report for the next cohort
  window lands.
- `plan_digest_stable_across_sigkill_recovery` — kept from slice 2b on a
  new worker, `kit::sigkill_capacity_worker_body` (same marker schedule as
  the slice-1 import worker, plus the cluster registration and the
  placement supplement): killed at marker 2, recovered through the public
  API, then configure/register/report run deterministically and the adapter
  plan view equals the no-crash baseline. Baseline and recovery are
  different directories registered at different wall-clock times; the
  comparison is the plan view, not the snapshot digest, because node
  eligibility is judged at the fixed planning instant and the imported
  lease bytes legitimately differ.

Gate: all three targets (`control_authority_adversarial` 11 tests,
`control_authority_model` 1, `control_authority_planner` 9) pass under the
8 GiB, swap-disabled systemd scope, with and without
`--features fault-injection,raft`; `cargo check --tests --features
fault-injection,raft` is clean; rustfmt clean; no production changes, no
new dependencies, no commit. The leg remains local single-process evidence:
no Raft leader change, log replication, or snapshot installation across
processes is claimed.

## Slice 3c: raft-host regressions for review findings R1, R2, R4

A fourth target, `tests/control_raft_regressions.rs` (11 tests), pins the
checkpoint-review findings against the real single-node `RaftHost`. It runs
only under `--features raft` and is gated with
`cargo test --test control_raft_regressions --features raft,fault-injection
-- --test-threads=1`. The tests pin REQUIRED behavior quoted from the review;
at the frozen checkpoint `331c18f` eight of them FAIL, and each failure is a
minimized reproduction for Fable — assertions are deliberately not weakened
to make a finding pass. Positive coverage (`r1_positive_...`,
`active_fence_...`, `recover_...`) passes there.

Kit additions: `tests/control_adversarial/raft_kit.rs` (compiled under the
`raft` feature; `#![allow(dead_code)]` because the other targets link it
without using every helper) mirrors the in-crate `src/raft/tests.rs`
single-node recipe — `HostConfig` 50/150/300 ms heartbeats/elections,
snapshots on demand only — and exposes `bootstrap_host`, `bootstrap_host_with`
(explicit policy; the ACTIVE fence test needs the Ingest grant the kit
policy omits), `start_host`, `wait_leader`, `last_applied`, `wait_applied`
(polls raft metrics: a `propose` resolves on commit, and the apply — plus
the metrics watch — lands a tick later, so a bare metrics read races the
state machine), `raw_proposal`, `snapshot_image` (decodes the prost
`RaftSnapshotMeta`; its `last_log_id.index` is the durable applied position,
the R2 observable, since `raft_applied` is otherwise pub(crate)), and
`install_image` (drives `begin_receiving_snapshot`/`install_snapshot` on a
standalone `ControlStateMachine`, the only install path a single node can
exercise — `replace_from` is pub(crate) and the host cannot drive an install
through its own raft core). `kit::prepare_action_history(workflow,
history_id)` adds a Prepare variant carrying a real catalog history id for
the managed-bind leg. Two load-bearing facts the tests encode: raft's
`initialize` commits a membership entry at log index 1, so after N proposals
the applied index is N+1; and a `SourceAdmission` is a read guard on the
store's admission lock, so it must not span a propose (the raft apply takes
that lock exclusively — holding one deadlocks the apply) nor may any store
clone outlive `shutdown` if the group is to reopen (redb's exclusive file
lock would block).

Results at `331c18f` (3 passed, 8 failed; wall time under a second for the
whole target):

- `r1_raw_propose_begin_without_holder_is_refused` — FAILED (repro). A raw
  Begin envelope through `host.propose` and through `raft().client_write`
  applies with no retirement holder; both workflows exist afterwards. The
  admitted path with the holder (`propose_import(..., Some(&retired))`) works
  and is exercised first as the positive control.
- `r1_raw_propose_confirm_ready_without_binding_is_refused` — FAILED
  (repro). A raw ConfirmReady through both raw surfaces turns the owner
  READY (phase 3) with no managed binding ever presented.
- `r1_read_handle_cannot_mutate` — FAILED (repro). Every pub replay surface
  on a handle advertised for reads by `host.store()` applies mutations with
  no committed log entry: `replay_control_import` (begin, three chunks,
  commit), `replay_capacity_configure`, `replay_capacity_transition`, and
  `replay_command` (Prepare at the post-import revision); the read handle
  then shows the staged workflow, capacity state and PREPARED owner. The
  direct `execute` path already refuses (a positive guard inside the same
  test). One run reports every violated surface.
- `r2_exact_retry_advances_durable_applied_position` — FAILED (repro).
  After prepare (index 2), a changed-content retry (consumed refusal, index
  3) and an exact retry last (index 4, stored decision verbatim), the
  snapshot meta bound index 3: `assertion failed: left: 3, right: 4` — the
  retry path returns the stored decision before `write_pending_applied`. The
  retry is deliberately last: any fresh entry after it would overwrite the
  lagging position, which is exactly the masking the review warned about.
  After the (future) fix, the restart leg replays the log, recovers at index
  4, and serves the PREPARED owner.
- `r2_import_chunk_retry_advances_durable_applied_position` — FAILED
  (repro). Begin (2), chunk-0 (3), changed-content chunk-0 (consumed refusal,
  4), exact chunk-0 retry last (5, identical stored decision); the snapshot
  bound 4: `left: 4, right: 5`.
- `r2_capacity_retries_and_observation_transition` — FAILED (repro). The
  full placement flow (retire, begin, three chunks, commit, configure,
  changed-content configure refusal, both reporter registrations, a landed
  report and the review's positive unchanged report) commits indices 2..=12,
  then the exact configure retry last (13, identical stored decision); the
  snapshot bound 12: `left: 12, right: 13`. The unchanged report itself is
  pinned positive: it consumes index 12 and `wait_applied(12)` observes it.
- `r4_install_refusal_preserves_the_live_handle` — FAILED (repro). With a
  reader clone outstanding, `install_snapshot` refuses (replace_from's
  "outstanding handles" check) and the machine's shared store slot is None:
  `state_machine` took the store out before `replace_from` and the refusal
  path never put it back. The reader held across the refused install still
  serves (pinned positive), and with it dropped the same image installs
  cleanly (pinned positive).
- `r4_subscription_rebinding_after_install` — FAILED (repro). A
  `subscribe_applied` receiver taken before a clean install never observes
  the post-install revision (a hand-applied entry at image index+1); it
  stays attached to the retired database: "the pre-install receiver never
  observed the new applied revision 2". Managed owner access on the
  installed store reflects the image (pinned positive).
- `r1_positive_replay_through_the_host`, `active_fence_through_the_host`,
  `recover_through_the_host` — PASS. The trusted entry points admit the same
  command families the raw paths accept holder-free; Prepare → managed bind
  → ConfirmReady → Activate commits the write fence (epoch 1 admits, epoch 2
  refuses, phase ACTIVE) and survives a host restart; bob's Recover keeps
  alice as initiator with bob as the terminal step and turns a later Commit
  into a recorded refusal.

Observation limits: single-node groups only — no leader change, log
replication or multi-node install is claimed; the durable-position
observable is the snapshot meta because `raft_applied` is pub(crate);
snapshot installs are exercised on a standalone state machine for the same
pub(crate) reason; and the admission-guard/file-lock discipline above is
test-shape, not product behavior. Gate: the three pre-existing targets pass
with and without `--features fault-injection,raft` (adversarial 11, model 1,
planner 9); `cargo check --tests --features raft,fault-injection` is clean;
rustfmt clean; no production changes, no new dependencies, no commit.

## Slice 3d: snapshot-protocol reproductions (R3, R5) and the crash matrix

Two final targets complete the build. `tests/control_raft_snapshots.rs`
(10 tests, `--features raft`) pins the checkpoint-review findings R3
(snapshot generation publication) and R5 (receive identity, bounded
buffering) against a standalone `ControlStateMachine` over a kit store —
never a `RaftHost` — so installs are exercised exactly as a follower
receiver sees them: `begin_receiving_snapshot` → write → `install_snapshot`.
`tests/control_crash_faults.rs` (7 tests, `--features fault-injection`)
re-drives the durable-commit crash windows (capacity configure/report,
import administrative recovery, managed bind/activate) through the public
worker/parent fault-injection surface. Both are gated together with 3c:
`cargo test --test control_raft_snapshots --features raft,fault-injection --
--test-threads=1` (about 13 s) and `cargo test --test control_crash_faults
--features raft,fault-injection -- --test-threads=2` (under a second).

Parallel gate: both raft targets take a target-wide `SERIAL` mutex for
each test's whole body, so the combined release gate
(`cargo test --release --features raft,fault-injection --no-fail-fast --
--test-threads=4`) may run them alongside everything else. The lock is
load-bearing in both: every host in `control_raft_regressions` bootstraps
on the kit's fixed `127.0.0.1:9917`, and `r5_install_hashing_is_bounded`
resets and re-records its process-wide allocation watermark inside the
lock so no other test's allocations can overlap the measurement window.

Kit additions: `raft_kit.rs` gains `control_entry` (a committed
`(term 1, node 1, index)` entry), `build_current` (publishes a generation
through the real snapshot builder and returns image + openraft meta),
`meta_for` (a meta naming an index, image bytes, and voter set — the
snapshot id format the receiver verifies), `install_received` (receive +
install preserving the receive file identity), and `image_of`. The crash
target reuses the kit's worker/parent split (`spawn_worker`, `KillOnDrop`,
`TestDir::from_env` via `PSEARCH_ADV_DIR`) with its own env selection
(`PSEARCH_CRASH_WORKER/FAMILY/FAULT`). Two load-bearing facts: an
`SourceAdmission` guard must not span a control command (the apply takes
the admission lock exclusively — holding one deadlocks), and a worker that
survives an armed exit fault must panic so the parent's exit-code assertion
stays honest.

### Snapshot results at `331c18f` (2 passed, 8 failed; 12.5 s)

Every failure is a minimized reproduction for Fable; assertions pin the
REQUIRED behavior and are not weakened.

- `r3_wrong_group_valid_image_is_refused_and_previous_survives` — FAILED
  (repro). The foreign-group image passes length/digest verification, is
  copied over `current.redb` in place, and `current.meta` is published
  before the group check runs; the install then errors naming the group,
  but `get_current_snapshot` serves the foreign bytes (snapshot id
  `1-1056768-c7c06cb9…`), `current.redb` on disk no longer equals the
  previous generation, and the live handle's slot was taken and never
  restored (`control_snapshot` → NotFound "resource has no applied control
  state").
- `r3_correct_index_wrong_membership_is_refused` — FAILED (repro). The
  machine's own image back at its real applied index but with voters `{9}`
  installs cleanly: only the applied index is compared and the in-memory
  membership is then set from the meta.
- `r3_rejected_install_leaves_previous_usable_after_restart` — FAILED
  (repro). After the refused wrong-group install, reopening the store and
  rebuilding the machine serves the foreign image, not the previous
  generation.
- `r3_interrupted_publication_boundaries` — FAILED at case (d2) only;
  (a) image-swapped-under-old-meta, (b) leftover `current.meta.tmp`, (c)
  leftover `staged.redb`, and (d1) meta with a bumped index under old
  bytes all behave as required (serve the complete previous generation or
  refuse loudly). (d2) — old image bytes under a self-consistent
  recomputed newer-index meta, the forged generation pointer — is served
  silently: the reader pairs names, it does not validate content before
  publication.
- `r3_reader_held_across_publication` — FAILED (repro). A reader held on
  the served generation observes the new bytes after a successful install:
  the copy over `current.redb` is in place, so the open fd's contents
  change mid-read.
- `r5_install_uses_the_actual_receive_not_the_newest_filename` — FAILED
  (repro). With an abandoned `incoming-<later>.redb` planted, the install
  selects it (`newest_incoming` is a lexicographic filename max) and
  refuses with DataLoss "snapshot image length 4096 and digest differ from
  the announced 557056" — or would install a valid foreign file.
- `r5_interrupted_receive_leaves_no_selected_artifact` — FAILED at (ii)
  only; (i) a plain earlier-named leftover is accidentally harmless (the
  fresh receive wins by name). (ii) a later-named planted partial is
  selected: DataLoss "snapshot image length 8192 and digest differ from
  the announced 557056".
- `r5_install_hashing_is_bounded` — FAILED (repro). A `#[global_allocator]`
  peak tracker (watermark reset immediately before the install, so growth
  is attributable to it) measures a peak live growth of **8,847,524 bytes
  for an 8,847,360-byte image** — `image_signature` reads the whole file
  into one Vec.
- `r5_oversize_and_truncated_images_refuse_before_replacement` — PASS.
  Announced-length and digest verification happen before any copy, so both
  shapes refuse loudly and the previous generation is intact. (Positive
  self-check.)
- `r5_announced_length_is_checked_only_at_install` — PASS (API-gap
  documentation). The receive sink is an unbounded `File`; bytes beyond
  the announcement are accepted while receiving and the refusal happens
  late, at install. The REQUIRED "enforce announced lengths WHILE
  receiving" is unreachable through the current surface — there is no
  announced length at `begin_receiving_snapshot` at all.

### Crash-matrix results at `331c18f` (7 passed; 0.2 s)

The control group: every window the review documents already behaves,
proving the worker/parent fault machinery before Fable touches the
snapshot path. Parents compute a no-crash baseline in a twin directory
first, then assert the documented recovery window and convergence through
the public reopen/retry API only; every worker exits 87 (the armed
`ExitFault::{BeforeCommit,AfterCommit}`), and a worker that survives its
fault panics so the parent's exit-code assertion stays honest.

- `crash_capacity_configure_before_and_after_commit` — PASS ×2. Before:
  no configuration committed (NotFound), a fresh configure lands. After:
  `configured_control_revision` equals the reopened revision and an exact
  retry (previous revision) returns the stored decision. Both converge to
  the baseline planner view digest and final control revision (11).
- `crash_capacity_report_before_and_after_commit` — PASS ×2. Before:
  `observation_count == 0`, the re-issued report reports Landed. After:
  `(observation_count, observation_epoch) == (1, 1)`, the exact repeat
  reports Unchanged. Both converge to the baseline view and revision.
- `crash_recover_before_and_after_commit` — PASS ×2. Before: the workflow
  is still Staging with its reservation held and no recovery decision
  recorded. After: Recovered with the reservation released. Both accept
  the exact recovery retry (control revision 5, stored decision) and
  record a non-zero refusal for a later Commit attempt without disturbing
  the terminal workflow; the state survives a reopen.
- `crash_managed_bind_and_activate_boundaries` — PASS ×4. Bind before:
  the catalog is still format 8 (`AccessControlledCatalog::open`) and the
  owner is still Prepared on the authority. Bind after: a committed
  format-9 binding recovered through `PreparedManagedCatalog::recover`
  (`bound_at_sequence == 1`). Activate before: the authority owner is
  ACTIVE on the control side but the catalog is still format 9 — active
  recovery refuses FailedPrecondition, prepared recovery works and
  activates. Activate after: format 10 recovered directly through
  `ActiveManagedCatalog::recover`. All four serve the next write at
  sequence 2. (A fresh import after recovery is covered by the slice-3b
  SIGKILL recovery matrix, which re-runs the whole import.)

Gate: the four pre-existing targets are unchanged (adversarial 11, model
1, planner 9 pass with and without features; raft-regressions keeps its
3-pass/8-repro tally); `cargo check --tests --features raft,fault-injection`
is clean; rustfmt clean; no production changes, no new dependencies, no
commit.

## Integration onto the repaired branch (Fable, 2026-09-08)

The harness at `611ceb0` (on `331c18f`) is integrated onto the repaired
branch `feat/integer-map-storage-2026-09`. The sections above are kept as
written: they are the original failing evidence at `331c18f`. This section
accounts for each of the 16 reproductions against the repaired production
code, with no assertion weakened.

Where the repair deliberately made an unsafe surface private, the runtime
reproduction is replaced by a compile-fail accessibility test (the
`compile_fail` doctests on `pipestream_search::raft`, nine of them:
`RaftHost::propose`, `RaftHost::raft`, the four `replay_*` store paths,
`ControlStateMachine::new`, `ControlStateMachine::shared_store`,
`RaftLogStore::create`) and the positive coverage is retained through the
supported host API. No production API was reopened to compile the old
tests.

### The rebuilt kit

`tests/control_adversarial/raft_kit.rs` no longer reaches `host.raft()`,
`host.propose`, the replay paths, the log store or the state machine. It
keeps the single-node helpers (`bootstrap_host`, `start_host`,
`wait_applied`, and `durable_position`, which reads
`RaftHost::applied_position`), reads snapshot generations from the
documented layout (`raft-snapshots/current` naming
`generations/<n>/{image.redb,meta}`), and adds a two-member group over the
tonic transport (`Cluster`: node 1 by `bootstrap_cluster`, node 2 by
`prepare_member` + `start_member`, seeded either by `add_learner` or by a
raw push) and a raw registered peer (`peer_client`, `install_chunks`,
node 3's fixture certificate under `tests/certs/raft`) that speaks the
transport's `InstallSnapshot` RPC. That peer is the adversary model for R3
and R5: a member of the cluster CA that is registered but not in the
membership, which is stronger than an in-process handle.

Two library facts shape the fixtures. openraft drops a snapshot whose
position is not newer than the receiver's applied position before the
receiver validates anything, so crafted images name a newer index and are
pushed at a detached replica (seeded by a raw push and never added to the
group). And openraft treats a refused install as a fatal storage error and
stops the receiving core; each test records that observation
(`core_running`) and restarts the member where a later step needs it. The
safety assertions never depend on it.

### Accounting, one row per reproduction

| # | Original reproduction at 331c18f | Repaired behavior | Regression evidence on the repaired branch |
|---|---|---|---|
| 1 | R1 `r1_raw_propose_begin_without_holder_is_refused`: `host.propose` and `raft().client_write` applied a holder-free Begin; both workflows staged. | `propose` is private, `raft()` is gone; `propose_import` admits a Begin against the holder before it submits. | `compile_fail` doctests (`propose`, `raft`); `r1_holder_free_begin_and_binding_free_confirm_refuse_at_proposal`: a Begin without the holder is refused at proposal, no workflow staged, no log entry consumed, the durable position unchanged; the held Begin still works. |
| 2 | R1 `r1_raw_propose_confirm_ready_without_binding_is_refused`: the raw paths turned the owner READY with no binding. | Same closure; `propose_command` refuses ConfirmReady (`PermissionDenied`), only `propose_confirm_ready` with the verified binding submits it. | Same test: ConfirmReady through the general path refused, owner stays PREPARED. `active_fence_through_the_host` keeps the positive path (bind, confirm, activate, fence, restart) through `with_admission`. |
| 3 | R1 `r1_read_handle_cannot_mutate`: `replay_control_import`, `replay_capacity_*` and `replay_command` on `host.store()` mutated with no log entry. | The four replay paths are crate-private; every public direct command on a hosted store refuses "propose through the host"; `admission()` refuses on a hosted store. | `compile_fail` doctests (four replay paths); `r1_hosted_read_handle_refuses_every_direct_mutation`: `execute`, `begin_control_import`, `execute_control_import`, `configure_capacity`, `capacity_transition`, `admission` all refuse by name; no owner, workflow or capacity state appears; position and log length unchanged. |
| 4 | R2 `r2_exact_retry_advances_durable_applied_position`: the snapshot bound index 3 after the exact retry at 4. | The retry lookup and the applied position share one transaction in every command family. | Same test on `durable_position` and the published meta: 4 after the retry, 4 after restart. |
| 5 | R2 `r2_import_chunk_retry_advances_durable_applied_position`: bound 4, required 5. | Same. | Same test: durable position 5, snapshot meta 5. |
| 6 | R2 `r2_capacity_retries_and_observation_transition`: bound 12, required 13. | Same; the unchanged observation transition consumes its entry (12). | Same test: 12 after the unchanged report, 13 after the configure retry, snapshot meta 13. |
| 7 | R4 `r4_install_refusal_preserves_the_live_handle`: a refused install left the shared store slot `None`. | The store swaps its database in place under its own locks; a held handle neither blocks nor refuses an install and serves the installed state afterwards; nothing is ever taken out of the slot. | `r4_install_refusal_preserves_the_live_handle` through the transport: the install proceeds with a handle held, the core keeps running, the held handle and `host.store()` both serve the installed owner, the applied position is the image's. |
| 8 | R4 `r4_subscription_rebinding_after_install`: a pre-install `subscribe_applied` receiver never fired again. | The policy and applied watch channels move to the reopened store. | `r4_subscription_rebinding_after_install`: the pre-install receiver observes the image's revision, and keeps waking on entries replicated after the member joins. |
| 9 | R3 `r3_wrong_group_valid_image_is_refused_and_previous_survives`: the foreign image replaced `current.redb` and `current.meta` before the group check. | Install verifies the checksum, probes a copy as a store of this group and checks position and membership before any publication; generations are immutable. | Same test through the transport: an honest foreign meta is refused by the transport (`PermissionDenied`, names the group) before the library sees it; a forged this-group meta over the foreign bytes is refused by the probe (names the group or incarnation); the member's published generation is byte-identical and its owner row unchanged. |
| 10 | R3 `r3_correct_index_wrong_membership_is_refused`: membership never compared, then taken from the meta. | The probe's stored membership must equal the meta. | Same test: refused naming the membership; the genuine newer image installs afterwards. |
| 11 | R3 `r3_rejected_install_leaves_previous_usable_after_restart`: the previous generation was gone after restart. | Generations behind one pointer; a refused install removes only its receive. Integration also found and fixed: a purge must not pass the store's applied position (see "Found during integration"). | Same test: after the refusal and a restart the generation, owner and applied position are intact, the group takes the member in and a later change lands on it. |
| 12 | R3 `r3_interrupted_publication_boundaries`: (a)-(d2) mixed image/meta pairs served or pointer forgeries served. | Startup sweeps unpublished generations, `current.tmp` and `staged.redb`; a meta whose id disagrees with its image refuses to start; a generation claiming a position past the store's applied position refuses to start (found during integration). | Same test: (a) swept, (b) and (c) untouched pair served, (d1) start refused naming the mismatch, (d2) start refused naming the position; with the genuine meta back the member starts at its own position and is seeded from the leader's real image. |
| 13 | R3 `r3_reader_held_across_publication`: `current.redb` overwritten in place under an open reader. | Immutable generation directories; publication moves a pointer and removes the old directory. | Same test: a file held open on the old generation reads the same bytes after the member published a newer one. |
| 14 | R5 `r5_install_uses_the_actual_receive_not_the_newest_filename`: the newest `incoming-*` name was selected. | Each receive is its own directory identified by handle; only that receive's artifacts are consumed; the sweep removes the rest at start. | Same test: a planted later-named directory is left alone, the real receive installs at its own position, the planted directory goes at restart. |
| 15 | R5 `r5_interrupted_receive_leaves_no_selected_artifact` and `r5_oversize_and_truncated_images_refuse_before_replacement`: leftover receives selected; lengths checked late. | A superseded receive is dropped with its bytes; length and digest are verified on the received bytes before any replacement. | Both tests: an abandoned half receive is gone once superseded, a planted directory is not this receive's; an oversize announcement and a truncated receive refuse naming the length with the previous generation intact, and the genuine image installs afterwards. |
| 16 | R5 `r5_announced_length_is_checked_only_at_install` (the documented API gap) and `r5_install_hashing_is_bounded` (peak 8,847,524 bytes for an 8,847,360-byte image). | The transport refuses a chunk past the announced length before the library sees it (integration closed the gap); hashing streams through a bounded buffer. | `r5_announced_length_is_enforced_while_receiving`: the excess chunk is `InvalidArgument` naming the announced length, the core keeps running, the correct final chunk completes the receive. `r5_install_hashing_is_bounded`: a 12 MiB image seeded through `add_learner` grows live allocation by 14,827,276 bytes for a 42,274,816-byte image. |

### Found during integration

Re-expressing the reproductions through the supported path surfaced three
defects in the repaired branch that the in-crate tests, which drove the
state machine directly, could not see. All three are fixed on the branch:

- A refused install left the log purged past the store. openraft purges
  the log for an incoming snapshot before the state machine has installed
  it, and treats the state machine's refusal as fatal. The member could
  then not restart (a debug build trips the library's
  `purge_upto <= snapshot` invariant; a prepared member fails with "no
  applied position; nothing to snapshot"). `RaftLogStore` now bounds every
  purge by the hosted store's applied position and completes the deferred
  part when the store catches up, so the log and the store agree after a
  refusal and after a successful install alike.
- A local generation claiming a position past the store's applied
  position passed startup and tripped the library's
  `snapshot <= committed` invariant. `ControlStateMachine::new` now
  refuses it as data loss, naming both positions.
- A registered peer could stop a receiving core with a debug assertion
  by sending a snapshot or entries under an uncommitted vote. The transport
  refuses appends and snapshot chunks whose vote is not committed, since
  both come only from a leader.

The liveness observation recorded at first integration (a refused install
stopped the receiving core, openraft's contract) is closed by the snapshot
admission checkpoint: the transport stages, binds and validates the
complete image before the library sees it, so invalid incoming data is
refused with the core running (`docs/raft-hosting.md`, "Snapshot
admission"; the acceptance test
`snapshot_admission_refuses_invalid_transfers_without_stopping_the_core`
in the snapshot target, which now asserts the core keeps running after
each refusal). The one local condition that stayed fatal at that checkpoint, a swap
refused for an outstanding store handle, is gone as well: the store now
swaps its database in place under its own locks, so a held handle neither
blocks nor refuses an install and serves the installed state afterwards
(`r4_install_refusal_preserves_the_live_handle` pins it with the core
running). Only a real storage failure inside the library's install path
is fatal.

### Found in review of the swap

Kimi's review of the swap checkpoint (b5c6f48) found the claim "no install
path stops a core except a real storage failure" earned for the install
path and asked for it to be scoped, or for the serve/publish race to be
taken as the next finding. Both are done. `docs/raft-hosting.md` ("Log and
store agreement") now names the three snapshot paths (install, build,
serve) and what stops the core on each. And the race is fixed: the serve
path (`get_current_snapshot`, on the state machine worker) read the
pointer and then the generation with no lock between them, while a build,
in its own task, published the next generation and removed the rest; a
publish landing between the two reads removed the generation under the
serve, the read failed as a storage error and the core stopped, with no
invalid data and no storage fault anywhere. A serve is now one step under
the pointer lock and every publish takes it.

`a_serve_under_a_concurrent_publish_returns_the_generation_it_read`
(snapshot target, `fault-injection`) pins the interleaving with two new
gates, `RaftHost::arm_snapshot_build_gate` (the build paused before it
publishes) and `arm_snapshot_serve_gate` (the serve paused after it read
the pointer), released build first, and reads the served snapshot through
`RaftHost::serve_snapshot`. Against the unlocked serve it fails with the
pointer moved to generation 2, generation 1 gone and the core stopped on
"No such file or directory"
(`/tmp/psearch-import-evidence/serve-publish-race-unfixed.log`); with the
lock the serve returns generation 1's meta and bytes, the pointer has not
moved under it, the build publishes generation 2 afterwards and the core
keeps committing (`serve-publish-race-fixed.log`).

The other non-storage condition on the build path, a snapshot trigger at
a prepared member that has applied nothing, is refused by
`RaftHost::trigger_snapshot` by name before the library sees it
(`a_snapshot_trigger_with_nothing_applied_is_refused_before_the_library`:
without the guard the member core stops on the builder's refusal; with it
the trigger is `FailedPrecondition`, the core runs, and the member is
seeded and snapshots afterwards).

### Found in the default gate of the serve checkpoint: the fork window

The default `cargo test --lib` gate for 945b484 failed once, in
the retirement test for corrupt frames (its "checksum" case), with the control ownership lock unavailable right after
the test dropped its holder: "lock acquisition failed because the operation
would block" (`/tmp/psearch-import-evidence/serve-lock-lib-default.log`).
The lock code was not at fault. `flock` locks belong to an open file
description, and a child spawned by another test thread keeps duplicates of
the parent's descriptors between fork and exec, so a lock released in that
window stays in force until the child's close-on-exec sweep. The crate already
guarded this (`src/test_support.rs`: spawns under the write side of one
RwLock, lock takes under the read side) on the assumption that
`Command::spawn` returns after the sweep. It does not. std spawns with
`clone3(CLONE_VM|CLONE_VFORK)` (`flock-guard-race-strace.txt`), and the
kernel resumes a vfork parent when the child gives up the shared address
space (`exec_mmap`), which is before `do_close_on_exec`. A probe in the
test's shape (hold, spawn under the guard, drop, re-take through a fresh
open; `flock-guard-race.rs`, `flock-guard-race-probe.txt`) saw the parent's
file still in the child's table after `spawn` returned and the re-take
rejected: 6 rejections in 120,000 rounds.

The fix keeps the guard and adds the second layer it needed: the child
closes its inherited regular-file descriptors before it execs
(`close_regular_files_after_fork`, a pre-exec hook that walks
`/proc/self/fd` with raw system calls and makes no allocation, since it runs
in the forked child of a multithreaded process). With the hook a returned
`spawn` means the descriptions are closed however the parent was
resumed. Regular files only: locks live on them, and the pipes std reports
an exec failure over must survive. The probe with the hook: 0 rejections in
160,000 rounds. Two tests in `test_support::tests`:
`a_spawned_child_has_no_inherited_regular_file` (the child lists its own
descriptors while the parent keeps a locked file open across the spawn,
and no regular file is among them) and
`a_lock_dropped_during_spawns_is_free_at_once` (take, drop and re-take in
a loop while a second thread spawns two hundred children under the guard).
Without the hook the second test fails within a few runs
(`fork-hook-sensitivity.txt`); with it the full default lib gate and the
`raft,fault-injection` lib gate pass (`fork-hook-lib-default.log`,
`fork-hook-lib-raft.log`).

What the fix covers: the lib test binary. Its 14 worker spawns go
through `ForkGuarded` and its production lock takes call
`lock_handoff` under `cfg(test)`. The six spawn sites in the integration
binaries (`tests/document_catalog.rs`, `tests/replay_journal.rs`,
`tests/source_access.rs`, `tests/control_adversarial/kit.rs`) have no layer of the two: the library they link is compiled without `cfg(test)`, so the
handoff is not in their lock code, and `test_support` is crate-private.
Giving them the same protection means compiling the handoff into the
library under a feature and exposing the module; that is a design choice
for the owner of the storage contracts and is left open here, named.
