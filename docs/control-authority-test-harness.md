# Control-authority adversarial test harness (slice 1)

Coordination record and design for the adversarial integration-test harness
around the source-authority control plane. This slice is test-only: it adds
`tests/control_authority_adversarial.rs` plus the shared kit under
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
