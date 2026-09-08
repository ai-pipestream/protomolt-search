# Control-authority adversarial test harness (slices 1 and 2)

Coordination record and design for the adversarial integration-test harness
around the source-authority control plane. This is test-only work: it adds
`tests/control_authority_adversarial.rs` (slice 1),
`tests/control_authority_model.rs` (slice 2a), and
`tests/control_authority_planner.rs` (slice 2b) plus the shared kit under
`tests/control_adversarial/` and this document. No production source, proto,
or Cargo manifest changes.

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

## Slice 2b: capacity-planner integration leg

`tests/control_authority_planner.rs` closes the loop from the committed
authority state to the capacity planner's public input
(`pipestream_search::capacity_tiers::TierSnapshotInput`,
`TierSnapshot::validated`, `plan_tiers`). The kit maps a real imported
`ControlCollectionSnapshot` to a planner input (`kit::planner_input`) with an
explicit split:

- Authority-derived fields come from the snapshot itself: the authority id
  (hex of the SHA-256 of the identity's `group_id`) and incarnation, both
  revisions, the workspace/collection key, the topology generation from the
  imported collection state, the derived fingerprint (SHA-256 of the
  supplement's `derived_fingerprint` string), and the provider geometry
  digest (SHA-256 of the supplement's canonical prost encoding).
- Harness-built fields mirror the in-crate planner fixtures
  (`src/capacity_tiers.rs` test module): the three-node registry, the
  three-tier policy, the two fragment requests (bucket 7, leaves L4/L7), the
  cohort configuration (600 s, phase 0, planning instant
  2026-09-08T12:00:00Z), and the five fixture observations. The committed
  view (`kit::planner_committed_view`) carries the fixture-A shard/leaf/copy
  shape (s6/s7, leaves L4/L7, five copy specs) bound to the snapshot's
  resource triple and topology generation. Hand-building is expected: the
  imported 2-route plane registers no nodes, exactly as the in-crate
  fixtures hand-build theirs.

Tests:

- `plan_digest_stable_across_replay_reopen_and_permutation` — four-way plan
  digest equality (`plan_tiers(...).plan_digest`): forward vs reverse chunk
  staging (the protocol keys chunks by ordinal and accepts any staging
  order), committed store vs reopened store, and forward vs reverse
  observation ingest order (the observation set and its digest are canonical,
  so the plan cannot move). Also pins snapshot-digest equality across the
  reopen and observation-set-digest equality across the permutation.
- `plan_digest_stable_across_sigkill_recovery` — the shared
  `kit::sigkill_import_worker_body` worker (refactored out of the slice-1
  target; both targets now carry a thin env-gated `#[test]` wrapper, and the
  slice-1 test names and behavior are unchanged) is armed at marker 2
  (killed right after its first staged chunk), recovered through the public
  API, and the recovered import's plan digest equals the no-crash baseline.
- `observation_bound_to_committed_resource` — the observation store's
  committed view serves one resource: a report of another collection is
  refused with "is not this resource" and a report one topology generation
  ahead is refused with the identity error, even though the shard, leaf, and
  node all exist in the committed view; the committed-matching twin lands.

Gate: all three targets (`control_authority_adversarial` 8 tests,
`control_authority_model` 1, `control_authority_planner` 4) pass under the
8 GiB, swap-disabled systemd scope; rustfmt clean; no production changes, no
new dependencies. The planner leg remains local single-process evidence the
same way slice 1 is: no Raft leader change, log replication, or
snapshot installation across processes is claimed.

## Slice 2c: observation expiry and incarnation supersession

Two focused tests in `tests/control_authority_planner.rs` pin the
observation store's two committed lifecycle transitions, verified against
`src/capacity_tiers.rs` (`commit_expiry` at line 533, `register_incarnation`
at line 488, and the in-crate boundary test at line 3141):

- `expiry_is_deterministic_and_excludes_the_expired` — expiry ages
  observations on `window_end_unix_ms`, never `last_scanned_unix_ms`, and
  expires one only when `now - window_end` is STRICTLY greater than
  `max_age` (age == max_age survives; the boundary is inclusive). A repeat
  `commit_expiry` with the same arguments is a no-op (`Ok(0)`, digest
  unchanged), and a `now` at or before every window end expires nothing.
  Two identical stores fed the same reports (three in the current cohort,
  two advanced into the next one so a strict subset can expire) return the
  same drop count, end with equal `observation_set_digest()` values that
  differ from the pre-expiry digest, and no longer carry the expired shard's
  reports. A plan built at the survivors' cohort instant classifies the
  surviving fragment field-equal (placements, refusals, policy fingerprint)
  to a store that only ever held the survivors; the fully expired fragment
  refuses loudly ("no current observation; unknown is not idle") instead of
  planning from memory.
- `supersession_drops_the_old_incarnation_and_is_deterministic` —
  re-registering a node drops its reports at every other process
  incarnation (epoch advances once, only when the set changes). A late
  duplicate report from the superseded process is refused naming the current
  incarnation ("... is superseded by ..."); the replacement process reports
  with the committed storage incarnation and lands. Two stores taken through
  the identical sequence end with equal set digests and equal
  `TierSnapshot::plan_digest()` values.

Also: the pre-existing dead-code warnings from `model.rs`/`rng.rs` (each
target uses only part of the shared module) are silenced with the same
`#![allow(dead_code)]` rationale used in `kit.rs`.

Gate: all three targets pass under the 8 GiB, swap-disabled scope in
`--release` with zero warnings; rustfmt clean; no production changes, no
commit.
