//! Capacity-planner integration leg (slice 3b;
//! `docs/control-authority-test-harness.md`).
//!
//! Everything the planner sees here comes from the real persisted adapter:
//! `SourceAuthorityStore::planner_input`, fed by `configure_capacity` and
//! `capacity_transition` (Register/Report/Expire) on top of a legacy control
//! plane that registered its nodes and shard replicas through the public
//! `ClusterControl` routes before retirement. The plan digest is pinned
//! across chunk staging order, store reopen, observation identity refusals,
//! idempotent transitions, expiry, supersession, and SIGKILL recovery at a
//! chunk boundary.

mod control_adversarial;

use std::time::Duration;

use control_adversarial::kit;
use pipestream_search::capacity_tiers::{plan_tiers, TierSnapshot};
use pipestream_search::pb::storage::{CapacityTransitionOutcome, LegacyControlRetirementRequest};
use pipestream_search::pb::AccessAction;
use pipestream_search::sha256;
use prost::Message;
use tonic::Code;

const MARKER_TIMEOUT: Duration = Duration::from_secs(60);
const T: u64 = kit::PLANNING_INSTANT_UNIX_MS;
const COHORT: u64 = kit::CAPACITY_COHORT_MS;

/// The adapter binds the input to the authority identity, the committed
/// control and policy revisions, the resource, the topology generation and
/// the observation epoch; reopening the store recomputes every one of them
/// identically and the plan digest does not move.
#[test]
fn adapter_input_and_plan_digest_stable_across_reopen() {
    let fixture = kit::adapter_fixture("adapter-reopen");
    let before = kit::planner_view(&fixture.store, T);
    assert!(before.0.contains("control_revision: 7"), "{}", before.0);
    assert!(before.0.contains("policy_revision: 1"), "{}", before.0);
    assert!(before.0.contains("observation_epoch: 2"), "{}", before.0);
    assert!(
        before
            .0
            .contains(&format!("topology_generation: {}", fixture.generation)),
        "{}",
        before.0
    );

    let snapshot_before = fixture
        .store
        .control_snapshot("alice", &kit::key())
        .unwrap();
    let kit::AdapterFixture {
        dir,
        authority,
        store,
        ..
    } = fixture;
    drop(store);
    let reopened = kit::open_store(&dir, &authority);
    let snapshot_after = reopened.control_snapshot("alice", &kit::key()).unwrap();
    assert_eq!(
        snapshot_before.digest, snapshot_after.digest,
        "reopen changed the committed snapshot digest"
    );
    let after = kit::planner_view(&reopened, T);
    assert_eq!(after, before, "reopen changed the adapter's plan view");
    eprintln!(
        "adapter reopen: plan digest {} stable",
        sha256::to_hex(&after.1)
    );
}

/// Forward and reverse chunk staging produce byte-identical adapter inputs
/// and plan digests: the protocol keys chunks by ordinal, and every
/// authority-derived field — nodes, replicas, placement codes — comes from
/// the committed import, not from the staging order.
#[test]
fn adapter_plan_digest_stable_across_chunk_permutation() {
    let forward = kit::adapter_fixture_staged("adapter-perm-fwd", false);
    let reverse = kit::adapter_fixture_staged("adapter-perm-rev", true);
    let forward_view = kit::planner_view(&forward.store, T);
    let reverse_view = kit::planner_view(&reverse.store, T);
    assert_eq!(
        forward_view, reverse_view,
        "reverse chunk staging changed the adapter's plan view"
    );
    eprintln!(
        "adapter chunk permutation: plan digest {} stable",
        sha256::to_hex(&forward_view.1)
    );
}

/// Transitions are idempotent by content and scoped to the observation
/// epoch: an exact report retry is UNCHANGED, a re-registration repeats the
/// same receipt, a repeat expiry drops nothing, and no transition ever moves
/// the control revision. A report whose leaf the committed shard does not
/// cover refuses with the identity error and stores nothing.
#[test]
fn adapter_transitions_are_idempotent_and_epoch_scoped() {
    let fixture = kit::adapter_fixture("adapter-idempotent");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let generation = fixture.generation;
    let input = store
        .planner_input("alice", &kit::key(), &kit::planning_context(T, generation))
        .unwrap();
    assert_eq!(input.control_revision, 7);

    // Exact retry of the scanned report: UNCHANGED, epoch still 2.
    let again = store
        .capacity_transition(
            "alice",
            &kit::report_transition(
                authority,
                kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
            ),
        )
        .unwrap();
    assert_eq!(again.outcome, CapacityTransitionOutcome::Unchanged as i32);
    assert_eq!(again.observation_epoch, 2);
    assert_eq!(again.control_revision, 7);

    // Re-registration of the same incarnation repeats the same receipt.
    let first = store
        .capacity_transition(
            "alice",
            &kit::register_transition(authority, kit::NODE_A, kit::INC_A),
        )
        .unwrap();
    let second = store
        .capacity_transition(
            "alice",
            &kit::register_transition(authority, kit::NODE_A, kit::INC_A),
        )
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(second.observation_epoch, 2);

    // A repeat expiry with a wide horizon drops nothing and moves nothing.
    let repeat = store
        .capacity_transition(
            "alice",
            &kit::expire_transition(authority, T + 2 * COHORT, 10 * COHORT),
        )
        .unwrap();
    assert_eq!(repeat.dropped, 0);
    assert_eq!(repeat.observation_epoch, 2);

    // The control revision is still 7: transitions never move it.
    let input = store
        .planner_input("alice", &kit::key(), &kit::planning_context(T, generation))
        .unwrap();
    assert_eq!(input.control_revision, 7);
    assert_eq!(input.policy_revision, 1);

    // A report whose leaf the committed shard does not cover refuses with
    // the identity error; nothing is stored and the epoch does not move.
    let mut foreign_leaf = kit::scanned_observation(kit::NODE_A, kit::INC_A, generation);
    foreign_leaf.leaf = "elsewhere".into();
    let error = store
        .capacity_transition("alice", &kit::report_transition(authority, foreign_leaf))
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("covers no rows in leaf"),
        "{}",
        error.message()
    );
    assert_eq!(
        store
            .capacity_state("alice", &kit::key())
            .unwrap()
            .observation_epoch,
        2
    );
    eprintln!("adapter transitions: idempotent by content, epoch-scoped, revision-static");
}

/// The plan digest binds the access-policy revision and the tier policy: a
/// ReplaceGrants command that advances the policy revision moves the digest,
/// a reconfigure with different tier thresholds (same cohort) moves it
/// again, a cohort shift while observations are retained refuses as a
/// recorded FailedPrecondition decision without moving anything, and a
/// reopen reproduces the final view exactly.
#[test]
fn adapter_plan_digest_moves_with_policy_and_tier_changes() {
    let fixture = kit::adapter_fixture("adapter-digest-moves");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let base = kit::planner_view(store, T);

    // (a) An access-policy change advances the revision the plan binds.
    let mut alice = kit::grant("alice", kit::COLLECTION);
    alice.actions.push(AccessAction::Search as i32);
    let decision = store
        .execute(
            "alice",
            &kit::source_command(
                authority,
                &kit::key(),
                "grants",
                7,
                1,
                0,
                kit::replace_grants_action(vec![alice, kit::grant("bob", kit::COLLECTION)]),
            ),
        )
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let after_grants = kit::planner_view(store, T);
    assert_ne!(
        after_grants.1, base.1,
        "policy revision did not move the digest"
    );
    assert!(
        after_grants.0.contains("policy_revision: 2"),
        "{}",
        after_grants.0
    );

    // (b) A tier-policy change is a retained control command too; same
    // cohort, different thresholds.
    let decision = store
        .configure_capacity(
            "alice",
            &kit::configure_command(
                authority,
                "cfg-2",
                8,
                2,
                kit::capacity_configuration(100_000_000_000),
            ),
        )
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    let after_policy = kit::planner_view(store, T);
    assert_ne!(
        after_policy.1, after_grants.1,
        "tier thresholds did not move the digest"
    );
    assert!(
        after_policy.0.contains("observation_epoch: 2"),
        "the reconfigure touched the observations: {}",
        after_policy.0
    );

    // (c) A cohort shift with observations retained refuses as a recorded
    // decision and moves nothing.
    let mut shifted = kit::capacity_configuration(100_000_000_000);
    shifted.cohort_phase_unix_ms = 5;
    let refused = store
        .configure_capacity(
            "alice",
            &kit::configure_command(authority, "cfg-3", 9, 2, shifted),
        )
        .unwrap();
    assert_eq!(
        refused.code,
        Code::FailedPrecondition as u32,
        "{}",
        refused.message
    );
    let after_refusal = kit::planner_view(store, T);
    assert_eq!(
        after_refusal, after_policy,
        "the refused cohort shift moved the plan view"
    );

    // Reopen reproduces the final view exactly.
    let kit::AdapterFixture {
        dir,
        authority,
        store,
        ..
    } = fixture;
    drop(store);
    let reopened = kit::open_store(&dir, &authority);
    assert_eq!(
        kit::planner_view(&reopened, T),
        after_policy,
        "reopen changed the plan view"
    );
    eprintln!("adapter digest moves: policy revision and tier thresholds bound, refusal inert");
}

/// Expiry through the Expire transition drops aged rows identically across
/// twin stores: `now - window_end` strictly greater than `max_age` expires,
/// the epoch moves once, a repeat expiry is a no-op, and planning the
/// evicted leaves refuses loudly instead of planning from memory.
#[test]
fn adapter_expiry_is_deterministic_and_epoch_scoped() {
    let run = |name: &str| {
        let fixture = kit::adapter_fixture(name);
        let store = &fixture.store;
        let generation = fixture.generation;
        let before = kit::planner_view(store, T);
        let expired = store
            .capacity_transition(
                "alice",
                &kit::expire_transition(&fixture.authority, T + 2 * COHORT, COHORT),
            )
            .unwrap();
        assert_eq!(expired.dropped, 2, "both reports are one cohort old");
        assert_eq!(expired.observation_epoch, 3);
        let repeat = store
            .capacity_transition(
                "alice",
                &kit::expire_transition(&fixture.authority, T + 2 * COHORT, COHORT),
            )
            .unwrap();
        assert_eq!(repeat.dropped, 0, "a repeat expiry must drop nothing");
        assert_eq!(repeat.observation_epoch, 3);

        let state = store.capacity_state("alice", &kit::key()).unwrap();
        assert_eq!(
            (
                state.reporter_count,
                state.observation_count,
                state.observation_epoch
            ),
            (2, 0, 3)
        );
        let input = store
            .planner_input("alice", &kit::key(), &kit::planning_context(T, generation))
            .unwrap();
        assert!(
            input.observations.is_empty(),
            "expired observations are still visible to the adapter"
        );
        let snapshot = TierSnapshot::validated(input).expect("adapter input must validate");
        let error = plan_tiers(&snapshot).expect_err("the evicted leaves must not plan");
        assert!(error.contains("no current observation"), "{error}");
        (before, expired, snapshot.plan_digest())
    };
    let first = run("adapter-expiry-a");
    let second = run("adapter-expiry-b");
    assert_eq!(first, second, "identical twin stores expired differently");
    eprintln!("adapter expiry: dropped 2, epoch 3, repeat inert, twins identical, plan refuses");
}

/// A restarted reporter supersedes its old incarnation through the Register
/// transition: the old process's rows drop in the same committed transition,
/// its late report is refused naming the new incarnation, the new process
/// reports with the committed storage incarnation and lands, and twin stores
/// taken through the sequence end with equal receipts and equal plan views.
#[test]
fn adapter_supersession_drops_old_incarnation_and_is_deterministic() {
    const NEW_INC: u8 = 0x0d;
    let run = |name: &str| {
        let fixture = kit::adapter_fixture(name);
        let store = &fixture.store;
        let generation = fixture.generation;
        let before = kit::planner_view(store, T);
        let restarted = store
            .capacity_transition(
                "alice",
                &kit::register_transition(&fixture.authority, kit::NODE_A, NEW_INC),
            )
            .unwrap();
        assert_eq!(restarted.dropped, 1);
        assert_eq!(restarted.observation_epoch, 3);

        // The old process's late report is refused, never matched.
        let late = kit::report_transition(
            &fixture.authority,
            kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
        );
        let error = store.capacity_transition("alice", &late).err().unwrap();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert!(
            error.message().contains("is superseded by"),
            "{}",
            error.message()
        );

        // The new process reports and lands.
        let landed = store
            .capacity_transition(
                "alice",
                &kit::report_transition(
                    &fixture.authority,
                    kit::scanned_observation(kit::NODE_A, NEW_INC, generation),
                ),
            )
            .unwrap();
        assert_eq!(landed.outcome, CapacityTransitionOutcome::Landed as i32);
        assert_eq!(landed.observation_epoch, 4);
        (before, restarted, landed, kit::planner_view(store, T))
    };
    let first = run("adapter-supersession-a");
    let second = run("adapter-supersession-b");
    assert_eq!(first, second, "identical supersession sequences diverged");
    assert_ne!(
        (first.0).1,
        (first.3).1,
        "supersession and the replacement report did not move the plan view"
    );
    eprintln!(
        "adapter supersession: old incarnation dropped and refused, replacement landed, twins equal"
    );
}

/// The adapter serves one committed resource: a report of another
/// collection or another topology generation refuses with the identity
/// error, never silently matched by shard and leaf; the committed-matching
/// twin lands.
#[test]
fn adapter_refuses_reports_outside_the_committed_resource() {
    let fixture = kit::adapter_fixture("adapter-resource-binding");
    let store = &fixture.store;
    let authority = &fixture.authority;
    let generation = fixture.generation;

    // Another resource is refused even though the shard, leaf and node all
    // exist in the committed view: identity is resource equality first.
    let mut wrong_resource = kit::scanned_observation(kit::NODE_A, kit::INC_A, generation);
    wrong_resource.partition.as_mut().unwrap().collection = "other-books".into();
    let error = store
        .capacity_transition("alice", &kit::report_transition(authority, wrong_resource))
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("is not this resource"),
        "{}",
        error.message()
    );

    // A report ahead of the committed topology generation refuses in the
    // same committed transition path.
    let mut wrong_generation = kit::scanned_observation(kit::NODE_A, kit::INC_A, generation);
    wrong_generation.topology_generation = generation + 1;
    let error = store
        .capacity_transition(
            "alice",
            &kit::report_transition(authority, wrong_generation),
        )
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("topology_generation"),
        "{}",
        error.message()
    );

    // Nothing was stored; a corrected report for the next cohort window
    // lands and advances the epoch.
    assert_eq!(
        store
            .capacity_state("alice", &kit::key())
            .unwrap()
            .observation_epoch,
        2
    );
    let mut next_window = kit::scanned_observation(kit::NODE_A, kit::INC_A, generation);
    next_window.window_start_unix_ms = T;
    next_window.window_end_unix_ms = T + COHORT;
    next_window.last_scanned_unix_ms = T + 100;
    let landed = store
        .capacity_transition("alice", &kit::report_transition(authority, next_window))
        .unwrap();
    assert_eq!(
        landed.outcome,
        CapacityTransitionOutcome::Landed as i32,
        "the corrected report did not land"
    );
    assert_eq!(landed.observation_epoch, 3);
    eprintln!(
        "adapter resource binding: foreign resource and generation refused, corrected twin landed"
    );
}

/// Kill the capacity worker with SIGKILL right after its first staged chunk
/// (marker 2), recover the import through the public API, run the capacity
/// steps deterministically, and pin the adapter's plan view to the no-crash
/// baseline. The baseline and the recovered store are different directories
/// registered at different wall-clock times; the comparison is the plan
/// view, because node eligibility is judged at the fixed planning instant,
/// not against the wall clock.
#[test]
fn plan_digest_stable_across_sigkill_recovery() {
    let baseline = kit::adapter_fixture("adapter-sigkill-baseline");
    let baseline_view = kit::planner_view(&baseline.store, T);
    eprintln!(
        "adapter sigkill baseline: plan digest {}",
        sha256::to_hex(&baseline_view.1)
    );

    let dir = kit::TestDir::new("adapter-sigkill-k2");
    let mut worker = kit::KillOnDrop(kit::spawn_worker(
        "sigkill_capacity_worker",
        &[
            (kit::WORKER_ENV, "1"),
            (kit::WORKER_DIR_ENV, dir.path().to_str().unwrap()),
            (kit::WORKER_ARM_ENV, "2"),
        ],
    ));
    kit::wait_marker(&dir, 2, MARKER_TIMEOUT);
    kit::kill9(worker.child());

    // Recovery through the public API only: the durable retirement request
    // was written before the kill, and the recovered holder re-admits it.
    let authority = kit::identity(kit::SEED);
    let store = kit::open_store(&dir, &authority);
    let request =
        LegacyControlRetirementRequest::decode(std::fs::read(dir.request()).unwrap().as_slice())
            .unwrap();
    let retired = store
        .recover_legacy_retirement("alice", &request, &dir.legacy())
        .unwrap();
    let payload = kit::import_payload_placement(&retired);
    kit::run_import_staged(&store, &authority, &retired, &payload, false);
    let generation = kit::committed_generation(&store);

    let decision = store
        .configure_capacity(
            "alice",
            &kit::configure_command(
                &authority,
                "cfg",
                6,
                1,
                kit::capacity_configuration(1_000_000_000_000),
            ),
        )
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    for (node, incarnation) in [(kit::NODE_A, kit::INC_A), (kit::NODE_B, kit::INC_B)] {
        store
            .capacity_transition(
                "alice",
                &kit::register_transition(&authority, node, incarnation),
            )
            .unwrap();
    }
    store
        .capacity_transition(
            "alice",
            &kit::report_transition(
                &authority,
                kit::scanned_observation(kit::NODE_A, kit::INC_A, generation),
            ),
        )
        .unwrap();
    store
        .capacity_transition(
            "alice",
            &kit::report_transition(
                &authority,
                kit::silent_observation(kit::NODE_B, kit::INC_B, generation),
            ),
        )
        .unwrap();

    let recovered = kit::planner_view(&store, T);
    assert_eq!(
        baseline_view, recovered,
        "crash recovery changed the adapter's plan view"
    );
    eprintln!("adapter sigkill k=2: recovered, plan digest matches baseline");
}

/// Worker for `plan_digest_stable_across_sigkill_recovery`; the body lives
/// in `kit::sigkill_capacity_worker_body` (same marker schedule as the
/// slice-1 import worker, plus the cluster registration and the placement
/// supplement). Early-returns unless `PSEARCH_ADV_WORKER` is set.
#[test]
fn sigkill_capacity_worker() {
    kit::sigkill_capacity_worker_body();
}
