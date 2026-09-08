//! Capacity-planner integration leg (slice 2b;
//! `docs/control-authority-test-harness.md`).
//!
//! Maps real imported `ControlCollectionSnapshot`s to the capacity planner's
//! public `TierSnapshotInput` and pins the plan digest across chunk replay
//! order, store reopen, observation ingest permutation, and SIGKILL recovery
//! at a chunk boundary. Also pins the observation store's resource binding:
//! a report of any other resource or topology generation is refused, never
//! silently matched by shard and leaf.

mod control_adversarial;

use std::time::Duration;

use control_adversarial::kit;
use pipestream_search::capacity_tiers::{plan_tiers, IngestOutcome, TierSnapshot};
use pipestream_search::pb::storage::{ControlCollectionSnapshot, LegacyControlRetirementRequest};
use prost::Message;

const MARKER_TIMEOUT: Duration = Duration::from_secs(60);

/// Derive the capacity plan digest from a committed snapshot: build the
/// harness committed view and observations, map the snapshot to the planner
/// input, validate, and plan.
fn plan_digest_of(snapshot: &ControlCollectionSnapshot) -> [u8; 32] {
    let view = kit::planner_committed_view(snapshot);
    let observations = kit::feed_reports(&view, false);
    let input = kit::planner_input(snapshot, &view, &observations);
    let planned = TierSnapshot::validated(input).expect("harness planner input must validate");
    plan_tiers(&planned)
        .expect("plan_tiers must accept the harness input")
        .plan_digest
}

/// Full Begin -> three Chunks -> Commit on a fresh 2-route legacy plane;
/// `reverse` stages the chunk ordinals in reverse order (the protocol keys
/// chunks by ordinal and accepts any staging order). Returns the plan digest
/// and the committed snapshot.
fn import_and_plan(dir: &kit::TestDir, reverse: bool) -> ([u8; 32], ControlCollectionSnapshot) {
    let (store, authority) = kit::create_store(dir);
    let legacy = kit::legacy_plane(dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = store
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "begin",
                1,
                kit::WORKFLOW,
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    let ordinals: Vec<u32> = if reverse {
        (0..chunk_count).rev().collect()
    } else {
        (0..chunk_count).collect()
    };
    let mut revision = 2u64;
    for ordinal in ordinals {
        let decision = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    &format!("chunk-{ordinal}"),
                    revision,
                    kit::WORKFLOW,
                    kit::chunk_action(&payload, chunk_bytes, ordinal),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "chunk {ordinal}: {}", decision.message);
        revision += 1;
    }
    let commit = store
        .execute_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "commit",
                revision,
                kit::WORKFLOW,
                kit::commit_action(),
            ),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);
    let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();
    let digest = plan_digest_of(&snapshot);
    (digest, snapshot)
}

/// The committed snapshot digest and the derived plan digest are identical
/// for forward and reverse chunk staging, identical after a store reopen,
/// and identical when the same observations are ingested in reverse order.
#[test]
fn plan_digest_stable_across_replay_reopen_and_permutation() {
    let dir_a = kit::TestDir::new("planner-replay-a");
    let (digest_a, snapshot_a) = import_and_plan(&dir_a, false);
    let dir_b = kit::TestDir::new("planner-replay-b");
    let (digest_b, _snapshot_b) = import_and_plan(&dir_b, true);
    assert_eq!(
        digest_a, digest_b,
        "reverse chunk staging changed the plan digest"
    );

    // Reopen the committed store and re-derive the planner input from the
    // reopened snapshot.
    let authority = kit::identity(kit::SEED);
    let reopened = kit::open_store(&dir_a, &authority);
    let snapshot_reopened = reopened.control_snapshot("alice", &kit::key()).unwrap();
    assert_eq!(
        snapshot_a.digest, snapshot_reopened.digest,
        "reopen changed the committed snapshot digest"
    );
    let digest_reopened = plan_digest_of(&snapshot_reopened);
    assert_eq!(digest_a, digest_reopened, "reopen changed the plan digest");

    // Same reports, opposite ingest order: the stored set and its digest are
    // canonical, and the plan digest must not move.
    let view = kit::planner_committed_view(&snapshot_a);
    let forward = kit::feed_reports(&view, false);
    let reversed = kit::feed_reports(&view, true);
    assert_eq!(
        forward.observation_set_digest(),
        reversed.observation_set_digest(),
        "ingest order changed the observation set digest"
    );
    let input = kit::planner_input(&snapshot_a, &view, &reversed);
    let planned = TierSnapshot::validated(input).expect("harness planner input must validate");
    let permuted = plan_tiers(&planned)
        .expect("plan_tiers must accept the harness input")
        .plan_digest;
    assert_eq!(
        digest_a, permuted,
        "observation ingest permutation changed the plan digest"
    );
    eprintln!(
        "planner replay/reopen/permutation: plan digest {} stable",
        pipestream_search::sha256::to_hex(&digest_a)
    );
}

/// Kill the import worker with SIGKILL right after its first staged chunk
/// (marker 2), recover the import through the public API, and pin the plan
/// digest to the no-crash baseline.
#[test]
fn plan_digest_stable_across_sigkill_recovery() {
    let baseline_dir = kit::TestDir::new("planner-sigkill-baseline");
    let (baseline_digest, _baseline) = import_and_plan(&baseline_dir, false);
    eprintln!(
        "planner sigkill baseline: digest {}",
        pipestream_search::sha256::to_hex(&baseline_digest)
    );

    let dir = kit::TestDir::new("planner-sigkill-k2");
    let mut worker = kit::KillOnDrop(kit::spawn_worker(
        "sigkill_import_worker",
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
    let payload = kit::import_payload(&retired);
    let (chunk_bytes, chunk_count) = kit::plan_three_chunks(payload.len());
    let begin = store
        .begin_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "begin",
                1,
                kit::WORKFLOW,
                kit::begin_action(&retired, &payload, chunk_bytes, chunk_count),
            ),
            &retired,
        )
        .unwrap();
    assert_eq!(begin.code, 0, "{}", begin.message);
    let mut revision = 2u64;
    for ordinal in 0..chunk_count {
        let decision = store
            .execute_control_import(
                "alice",
                &kit::import_command(
                    &authority,
                    &format!("chunk-{ordinal}"),
                    revision,
                    kit::WORKFLOW,
                    kit::chunk_action(&payload, chunk_bytes, ordinal),
                ),
            )
            .unwrap();
        assert_eq!(decision.code, 0, "chunk {ordinal}: {}", decision.message);
        revision += 1;
    }
    let commit = store
        .execute_control_import(
            "alice",
            &kit::import_command(
                &authority,
                "commit",
                revision,
                kit::WORKFLOW,
                kit::commit_action(),
            ),
        )
        .unwrap();
    assert_eq!(commit.code, 0, "{}", commit.message);

    let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();
    let recovered = plan_digest_of(&snapshot);
    assert_eq!(
        baseline_digest, recovered,
        "crash recovery changed the plan digest"
    );
    eprintln!("planner sigkill k=2: recovered, plan digest matches baseline");
}

/// Worker for `plan_digest_stable_across_sigkill_recovery`; the body lives
/// in `kit::sigkill_import_worker_body` (shared with the slice-1 target).
/// Early-returns unless `PSEARCH_ADV_WORKER` is set.
#[test]
fn sigkill_import_worker() {
    kit::sigkill_import_worker_body();
}

/// The observation store's committed view serves one resource: reports of
/// another collection or another topology generation are refused with the
/// identity error, and the committed-matching report lands.
#[test]
fn observation_bound_to_committed_resource() {
    let dir = kit::TestDir::new("planner-resource-binding");
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    kit::run_import(&store, &authority, &retired, &payload);
    let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();

    let view = kit::planner_committed_view(&snapshot);
    let mut observations = kit::planner_observation_store(&view);
    let reports = kit::planner_reports(&view);

    // Another resource is refused even though the shard, leaf, and node all
    // exist in the committed view: identity is resource equality first.
    let mut wrong_resource = reports[0].clone();
    wrong_resource.partition.collection = "other-books".to_string();
    let error = observations
        .ingest(wrong_resource)
        .expect_err("a cross-resource report must be refused");
    assert!(error.contains("is not this resource"), "{error}");

    // A report ahead of the committed topology generation is likewise
    // refused in both directions.
    let mut wrong_generation = reports[0].clone();
    wrong_generation.topology_generation = view.topology_generation + 1;
    let error = observations
        .ingest(wrong_generation)
        .expect_err("a wrong-generation report must be refused");
    assert!(error.contains("topology_generation"), "{error}");

    // The committed-matching twin of the refused reports lands.
    assert_eq!(
        observations.ingest(reports[0].clone()).expect("ingest"),
        IngestOutcome::Landed
    );
    eprintln!("planner resource binding: foreign resource and generation refused, twin landed");
}

/// Run the canonical import once and return the directory (kept alive) and
/// the committed snapshot, for tests that only need the planner's committed
/// view rather than an import permutation.
fn imported_snapshot() -> (kit::TestDir, ControlCollectionSnapshot) {
    let dir = kit::TestDir::new("planner-lifecycle");
    let (store, authority) = kit::create_store(&dir);
    let legacy = kit::legacy_plane(&dir, 2);
    let (retired, _request) = kit::retire(&store, &legacy, 1);
    let payload = kit::import_payload(&retired);
    kit::run_import(&store, &authority, &retired, &payload);
    let snapshot = store.control_snapshot("alice", &kit::key()).unwrap();
    (dir, snapshot)
}

fn inc_0d() -> [u8; 16] {
    let mut out = [0u8; 16];
    out[15] = 0x0d;
    out
}

/// One full supersession sequence: feed the fixture reports, re-register
/// krick-1 with a new process incarnation, watch the late old-incarnation
/// report refused, then accept the new process's report for (s6, L4).
/// Returns the store, the digest after feeding, and the digest after the
/// supersession.
fn supersession_sequence(
    view: &pipestream_search::capacity_tiers::CommittedView,
) -> (
    pipestream_search::capacity_tiers::ObservationStore,
    [u8; 32],
    [u8; 32],
) {
    let t = kit::PLANNING_INSTANT_UNIX_MS;
    let mut store = kit::feed_reports(view, false);
    let fed = store.observation_set_digest();
    store
        .register_incarnation("krick-1", inc_0d())
        .expect("re-register krick-1");
    let superseded = store.observation_set_digest();
    assert_ne!(
        fed, superseded,
        "supersession did not change the set digest"
    );
    assert!(
        store
            .observations()
            .iter()
            .all(|obs| obs.reporter.node_id != "krick-1"),
        "the superseded node's reports are still present"
    );
    // A late duplicate from the old process is refused, never matched.
    let late = kit::planner_reports(view).remove(0);
    let error = store
        .ingest(late)
        .expect_err("a superseded process's report must be refused");
    assert!(error.contains("superseded"), "{error}");
    // The new process reports with the committed storage incarnation.
    let new_process = kit::PlannerCopySpec {
        node: "krick-1",
        proc_inc: 0x0d,
        stor_inc: 0xa1,
        domain: "krick",
    };
    let fresh = kit::planner_obs(
        view,
        &new_process,
        "L4",
        "s6",
        3,
        5,
        1_000_000,
        268_435_456,
        3_000_000,
        t - 5000,
    );
    assert_eq!(
        store.ingest(fresh).expect("the new process reports"),
        IngestOutcome::Landed
    );
    (store, fed, superseded)
}

/// `register_incarnation` replaces a node's incarnation and drops its
/// reports at every other incarnation (verified against
/// `src/capacity_tiers.rs:488`). A late report from the superseded process
/// is refused naming the current incarnation; the replacement process
/// reports with the committed storage incarnation; two stores taken through
/// the same sequence end with equal set digests and equal plan digests.
#[test]
fn supersession_drops_the_old_incarnation_and_is_deterministic() {
    let (_dir, snapshot) = imported_snapshot();
    let view = kit::planner_committed_view(&snapshot);

    let (first, fed, superseded) = supersession_sequence(&view);
    let first_digest = first.observation_set_digest();
    let (second, fed_2, superseded_2) = supersession_sequence(&view);
    let second_digest = second.observation_set_digest();

    assert_eq!(fed, fed_2);
    assert_eq!(superseded, superseded_2);
    assert_eq!(
        first_digest, second_digest,
        "identical supersession sequences diverged"
    );
    assert_ne!(
        superseded, first_digest,
        "the replacement report did not change the set digest"
    );

    // The plan digest is a pure function of the snapshot: equal snapshots
    // (same observations, same observation epoch) plan digests equal.
    let first_input = kit::planner_input(&snapshot, &view, &first);
    let second_input = kit::planner_input(&snapshot, &view, &second);
    let first_plan = TierSnapshot::validated(first_input)
        .expect("valid")
        .plan_digest();
    let second_plan = TierSnapshot::validated(second_input)
        .expect("valid")
        .plan_digest();
    assert_eq!(
        first_plan, second_plan,
        "plan digest diverged after supersession"
    );
    eprintln!(
        "supersession: old incarnation dropped and refused, replacement landed, digests equal"
    );
}

/// `commit_expiry` ages observations on `window_end_unix_ms` and expires one
/// only when `now - window_end` is STRICTLY greater than `max_age`
/// (verified against `src/capacity_tiers.rs:533` and the in-crate boundary
/// test). Two identical stores expire identically; the expired ids leave the
/// set; a plan over the survivors classifies the affected fragment exactly
/// as a store that only ever held them.
#[test]
fn expiry_is_deterministic_and_excludes_the_expired() {
    let (_dir, snapshot) = imported_snapshot();
    let view = kit::planner_committed_view(&snapshot);
    let t = kit::PLANNING_INSTANT_UNIX_MS;
    let cohort = kit::COHORT_LENGTH_MS;

    // Boundary: age == max_age survives, strictly greater expires.
    let mut bounded = kit::feed_reports(&view, false);
    let digest_full = bounded.observation_set_digest();
    assert_eq!(
        bounded.commit_expiry(t, 0).expect("expiry"),
        0,
        "now == window_end must expire nothing"
    );
    assert_eq!(bounded.observation_set_digest(), digest_full);
    assert_eq!(
        bounded.commit_expiry(t + 599_999, 599_999).expect("expiry"),
        0,
        "age == max_age must survive"
    );
    assert_eq!(bounded.observation_set_digest(), digest_full);
    assert_eq!(
        bounded.commit_expiry(t + 600_000, 599_999).expect("expiry"),
        5,
        "age > max_age must expire"
    );
    let digest_empty = bounded.observation_set_digest();
    assert_ne!(digest_full, digest_empty);
    assert!(bounded.observations().is_empty());
    assert_eq!(
        bounded.commit_expiry(t + 600_000, 599_999).expect("expiry"),
        0,
        "a repeat expiry is a no-op"
    );
    assert_eq!(bounded.observation_set_digest(), digest_empty);

    // Strict-subset expiry: move s7's two reports to the next cohort window
    // so now = T + cohort expires exactly s6's three.
    let mut reports = kit::planner_reports(&view);
    for report in &mut reports[3..] {
        report.window_start_unix_ms = t;
        report.window_end_unix_ms = t + cohort;
    }
    let mut first = kit::planner_observation_store(&view);
    let mut second = kit::planner_observation_store(&view);
    for report in &reports {
        assert_eq!(
            first.ingest(report.clone()).expect("ingest"),
            IngestOutcome::Landed
        );
        assert_eq!(
            second.ingest(report.clone()).expect("ingest"),
            IngestOutcome::Landed
        );
    }
    let digest_before = first.observation_set_digest();
    assert_eq!(first.commit_expiry(t + cohort, 0).expect("expiry"), 3);
    assert_eq!(second.commit_expiry(t + cohort, 0).expect("expiry"), 3);
    let digest_after = first.observation_set_digest();
    assert_eq!(
        digest_after,
        second.observation_set_digest(),
        "identical stores expired differently"
    );
    assert_ne!(
        digest_before, digest_after,
        "expiry did not change the observation set digest"
    );
    let remaining = first.observations();
    assert_eq!(remaining.len(), 2);
    assert!(
        remaining.iter().all(|obs| obs.shard.shard == "s7"),
        "expired s6 observations are still present"
    );
    assert_eq!(
        first.commit_expiry(t + cohort, 0).expect("expiry"),
        0,
        "a repeat expiry must be a no-op"
    );
    assert_eq!(
        first.observation_set_digest(),
        digest_after,
        "the repeat expiry changed the digest"
    );

    // now before every window end expires nothing.
    let mut early = kit::planner_observation_store(&view);
    for report in &reports {
        assert_eq!(
            early.ingest(report.clone()).expect("ingest"),
            IngestOutcome::Landed
        );
    }
    let digest_early_full = early.observation_set_digest();
    assert_eq!(early.commit_expiry(t, 0).expect("expiry"), 0);
    assert_eq!(
        early.observation_set_digest(),
        digest_early_full,
        "expiry before the timestamps changed the set"
    );

    // Plan over the survivors: at instant T + cohort the current cohort is
    // [T, T + cohort), exactly where the s7 survivors report. The L7
    // classification is field-equal to a store that only ever held them.
    let mut input = kit::planner_input(&snapshot, &view, &first);
    input.planning_instant_unix_ms = t + cohort;
    input
        .fragment_requests
        .retain(|request| request.leaf == "L7");
    let plan = plan_tiers(&TierSnapshot::validated(input).expect("valid"))
        .expect("plan over the survivors");
    assert_eq!(plan.placements.len(), 1);

    let mut survivors = kit::planner_observation_store(&view);
    for report in &reports[3..] {
        assert_eq!(
            survivors.ingest(report.clone()).expect("ingest"),
            IngestOutcome::Landed
        );
    }
    let mut survivor_input = kit::planner_input(&snapshot, &view, &survivors);
    survivor_input.planning_instant_unix_ms = t + cohort;
    survivor_input
        .fragment_requests
        .retain(|request| request.leaf == "L7");
    let survivor_plan = plan_tiers(&TierSnapshot::validated(survivor_input).expect("valid"))
        .expect("plan over the survivors only");

    assert_eq!(
        plan.placements, survivor_plan.placements,
        "classification changed with history the plan cannot see"
    );
    assert_eq!(plan.refusals, survivor_plan.refusals);
    assert_eq!(plan.policy_fingerprint, survivor_plan.policy_fingerprint);

    // The expired fragment refuses loudly instead of planning from memory.
    let mut l4_input = kit::planner_input(&snapshot, &view, &first);
    l4_input.planning_instant_unix_ms = t + cohort;
    l4_input
        .fragment_requests
        .retain(|request| request.leaf == "L4");
    let error = plan_tiers(&TierSnapshot::validated(l4_input).expect("valid"))
        .expect_err("the expired fragment must not plan");
    assert!(error.contains("no current observation"), "{error}");
    eprintln!("expiry: boundary inclusive, strict subset expired, survivors classify identically");
}
