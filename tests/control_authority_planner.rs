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
