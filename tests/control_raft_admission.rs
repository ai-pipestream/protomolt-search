//! Slice 4b: deterministic admission-timing tests for the three gaps named
//! in docs/raft-admission.md, against the contract in that document — never
//! against implementation behavior. A deviation is a finding for Fable, not
//! an assertion to weaken.
//!
//! The agreed oracle ("Contract for in-flight work", docs/raft-admission.md):
//! an operation that passed its final check may finish; revocation takes
//! effect for every write whose final check happens after the interval has
//! lapsed, and for every new admission; a refusing final check leaves no
//! durable change and no retry record; the receipt discloses nothing about
//! the lease.
//!
//! Compiled only with both `raft` and `fault-injection`. Leader isolation
//! additionally needs `tls` (a default feature) for the transport and the
//! `isolate`/`heal` hooks; the pause hooks (`arm_grant_gate`,
//! `arm_precommit_pause`) are fault-injection. All timing derives from
//! `raft_kit::cluster_host_config` (lease 100 ms, skew 50 ms, election
//! 150/300 ms); every sleep is computed from those values.
#![cfg(all(feature = "raft", feature = "fault-injection"))]

mod control_adversarial;

use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt as _;

use control_adversarial::{kit, raft_kit};
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::document_catalog::{AccessControlledCatalog, ActiveManagedCatalog};
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, SourceAuthorityIdentity, SourceManagedBinding, SourceOwnerCompletion,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, AccessPolicy,
    DocumentWriteReceipt, ProtobufSource,
};
use pipestream_search::raft::{HostConfig, RaftHost};
use tonic::Code;

use raft_kit::Cluster;

/// Target-wide serialization: the cluster tests share timing-sensitive
/// election windows and loopback mTLS listeners, so tests hold this lock
/// for their whole body (the suite is small; serialized wall time is
/// unchanged in practice).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const WORKFLOW: &[u8] = b"admission-owner-one";

fn lease_ms(config: &HostConfig) -> u64 {
    config.admission_lease_ms
}

fn election_min_ms(config: &HostConfig) -> u64 {
    config.election_timeout_min_ms
}

fn election_max_ms(config: &HostConfig) -> u64 {
    config.election_timeout_max_ms
}

/// The hosted policy: alice and bob Admin, plus the Ingest grant the
/// write-admission entry check requires (mirrors the in-crate fixtures).
fn hosted_policy() -> AccessPolicy {
    let mut policy = kit::policy();
    for grant in &mut policy.grants {
        if grant.principal == "alice" {
            grant.actions.push(AccessAction::Ingest as i32);
        }
    }
    policy
}

/// Revoke every grant alice holds: the surviving quorum's revocation in the
/// isolation scenarios (docs/raft-admission.md, "Failure behaviour").
fn revoke_alice_action() -> pipestream_search::pb::storage::source_authority_command::Action {
    kit::replace_grants_action(vec![kit::grant("bob", kit::COLLECTION)])
}

/// One accepted document under `key` (mirrors `src/document_catalog/managed/
/// tests.rs`); the fixture's first write is what the managed bridge binds to.
fn catalog_write(document_key: &[u8], operation_id: &[u8]) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: document_key.to_vec(),
        operation_id: operation_id.to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

/// Create the managed catalog and return it with the history id the owner
/// preparation must pin (mirrors the slice-3c fixture).
fn catalog_fixture(dir: &kit::TestDir) -> (AccessControlledCatalog, Vec<u8>) {
    let mut policy = kit::policy();
    for grant in &mut policy.grants {
        if grant.principal == "alice" {
            grant.actions.push(AccessAction::Ingest as i32);
        }
    }
    let authorizer: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(policy).unwrap());
    let admin = AccessPermit::acquire(
        authorizer.clone(),
        "alice",
        kit::COLLECTION,
        AccessAction::Admin,
    )
    .unwrap();
    let ingest =
        AccessPermit::acquire(authorizer, "alice", kit::COLLECTION, AccessAction::Ingest).unwrap();
    let catalog = AccessControlledCatalog::create(
        &dir.path().join("catalog.redb"),
        &pipestream_search::pb::storage::SourceResourceBinding {
            format_version: 1,
            workspace: kit::WORKSPACE.into(),
            collection: kit::COLLECTION.into(),
        },
        &admin,
    )
    .unwrap();
    let receipt = catalog
        .accept(&ingest, &catalog_write(b"document-one", b"accept-one"))
        .unwrap();
    (catalog, receipt.history_id)
}

fn confirm_command(
    authority: &SourceAuthorityIdentity,
    control_revision: u64,
    completion: SourceOwnerCompletion,
) -> pipestream_search::pb::storage::SourceAuthorityCommand {
    kit::source_command(
        authority,
        &kit::owner_key(),
        "confirm-ready",
        control_revision,
        1,
        1,
        pipestream_search::pb::storage::source_authority_command::Action::ConfirmReady(
            ConfirmSourceOwnerReady {
                workflow_id: WORKFLOW.to_vec(),
                completion: Some(completion),
            },
        ),
    )
}

/// The full ACTIVE bridge on the cluster's leader: Prepare -> bind ->
/// ConfirmReady -> Activate, then open the activated source. Returns the
/// ACTIVE catalog; the leader's revision tracker has advanced by three.
async fn bridge_active(
    cluster: &mut Cluster,
    leader: u64,
    dir: &kit::TestDir,
    owner_label: &str,
) -> ActiveManagedCatalog {
    let authority = cluster.group.clone();
    let (catalog, history_id) = catalog_fixture(dir);

    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        owner_label,
        cluster.revision,
        1,
        0,
        kit::prepare_action_history(WORKFLOW, history_id),
    );
    let decision = cluster
        .host(leader)
        .propose_command("alice", &prepare)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    cluster.revision += 1;
    let preparation = decision.owner.clone().unwrap();

    let store = cluster.host(leader).store().unwrap();
    let managed = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
    let verified = managed.completion().unwrap();
    assert_eq!(verified.completion().bound_at_sequence, 1);

    let confirm = confirm_command(&authority, cluster.revision, verified.completion().clone());
    let confirmed = cluster
        .host(leader)
        .propose_confirm_ready("alice", &confirm, &managed.completion().unwrap())
        .await
        .unwrap();
    assert_eq!(confirmed.code, 0, "{}", confirmed.message);
    cluster.revision += 1;

    let activate = kit::source_command(
        &authority,
        &kit::owner_key(),
        "activate-owner",
        cluster.revision,
        1,
        1,
        kit::activate_action(WORKFLOW),
    );
    let activated = cluster
        .host(leader)
        .propose_command("alice", &activate)
        .await
        .unwrap();
    assert_eq!(activated.code, 0, "{}", activated.message);
    cluster.revision += 1;

    cluster
        .host(leader)
        .with_admission("alice", |admission| managed.activate(admission))
        .await
        .unwrap()
}

/// Rebuild the managed binding from bytes the worker persisted before the
/// kill: the readiness check is a hash over the exact binding, so the
/// recovery must present the original bytes, not a reconstruction.
fn binding_from_worker(dir: &kit::TestDir) -> SourceManagedBinding {
    use prost::Message;
    let bytes = std::fs::read(dir.path().join(BINDING_FILE)).unwrap();
    SourceManagedBinding::decode(bytes.as_slice()).unwrap()
}

// ---------------------------------------------------------------------------
// Scenario 1: a grant paused after the barrier resumes into a refusal once
// the survivors have elected and revoked — no revocation race is possible.
// ---------------------------------------------------------------------------

/// The lease argument's operational form: a grant paused after its barrier
/// while the survivors elect a successor and commit alice's revocation
/// resumes into a refusal — the election cannot precede the election floor,
/// and the floor is past every lease the old leader issued.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_grant_after_barrier_survives_no_revocation_race() {
    let _serial = serial();
    let config = raft_kit::cluster_host_config();
    let mut cluster = raft_kit::three_voters("admission-race", &hosted_policy()).await;
    let leader = cluster.leader().await;

    let gate = cluster.host(leader).arm_grant_gate();
    let host = Arc::new(cluster.take(leader));
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| {
                admission.authorize(kit::COLLECTION, AccessAction::Ingest)
            })
            .await
        })
    };
    gate.reached().await;

    // Isolate the old leader from both peers, both directions.
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let isolated_at = Instant::now();
    host.isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }

    // No election finishes before the election floor: the pause has already
    // outlasted lease + skew, so the resumed grant must be refused.
    let successor = cluster.leader_other_than(leader).await;
    assert!(
        isolated_at.elapsed() >= Duration::from_millis(election_min_ms(&config)),
        "a revocation preceded the election floor: elected after {:?}",
        isolated_at.elapsed()
    );

    // The surviving quorum commits the revocation of alice.
    let revoked = cluster
        .propose(
            successor,
            "bob",
            "revoke-alice",
            &kit::key(),
            revoke_alice_action(),
        )
        .await;
    let _ = revoked;
    let revoked_at = cluster
        .host(successor)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;

    gate.release();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(
        error.code(),
        Code::FailedPrecondition,
        "the resumed grant must be refused once the interval elapsed: {error}"
    );
    assert!(
        error.message().contains("elapsed before the grant"),
        "the refusal must name the grant-time check: {error}"
    );

    // The old leader grants nothing further: no quorum, no admission.
    let error = host
        .with_admission("alice", |admission| {
            admission.authorize(kit::COLLECTION, AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(
        error.message().contains("linearizable"),
        "the old leader's barrier must fail, not serve a stale view: {error}"
    );

    // Heal: the old leader catches up, applies the revocation, and alice is
    // denied on both sides.
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    let host = Arc::try_unwrap(host).ok().expect("tasks finished");
    cluster.insert(leader, host);
    cluster.wait_applied(leader, revoked_at).await;
    let healed = cluster.host(leader).store().unwrap();
    assert_eq!(
        healed
            .policy("bob", kit::WORKSPACE, kit::COLLECTION)
            .unwrap()
            .revision,
        2,
        "the revocation must be applied on the old leader after catch-up"
    );
    drop(healed);
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize(kit::COLLECTION, AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::Unavailable,
        "a follower has no barrier to grant through: {error}"
    );
    let error = cluster
        .host(successor)
        .with_admission("alice", |admission| {
            admission.authorize(kit::COLLECTION, AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::PermissionDenied,
        "the current leader must deny the revoked principal: {error}"
    );

    cluster.shutdown().await;
}

/// Positive arm: the same pause released within the interval is granted, the
/// deadline is the anchor plus the ttl (elapsed time consumed the interval;
/// it did not extend it), the admission authorizes, and past the deadline it
/// refuses by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_grant_resumed_within_interval_is_granted() {
    let _serial = serial();
    let config = raft_kit::cluster_host_config();
    let lease = Duration::from_millis(lease_ms(&config));
    let mut cluster = raft_kit::three_voters("admission-pause-ok", &hosted_policy()).await;
    let leader = cluster.leader().await;

    let gate = cluster.host(leader).arm_grant_gate();
    let before = Instant::now();
    let host = Arc::new(cluster.take(leader));
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| {
                let lease = *admission
                    .lease()
                    .expect("a raft admission carries its lease");
                admission
                    .authorize(kit::COLLECTION, AccessAction::Ingest)
                    .map(|_| lease)
            })
            .await
        })
    };
    gate.reached().await;
    let paused_at = Instant::now();
    // Release inside the interval: only half of it is gone.
    tokio::time::sleep(lease / 2).await;
    gate.release();
    let granted = task.await.unwrap().unwrap();

    // The anchor precedes the barrier's send, and the deadline is anchored:
    // grant time played no role in it.
    assert!(
        granted.anchor >= before && granted.anchor <= paused_at,
        "the anchor must be taken before the barrier is invoked"
    );
    assert_eq!(
        granted.deadline(),
        granted.anchor + granted.ttl,
        "the deadline must be anchor + ttl, not grant + ttl"
    );
    assert!(
        granted.deadline() <= paused_at + granted.ttl,
        "the deadline must not be extended by the pause"
    );
    let remaining = granted.deadline().saturating_duration_since(Instant::now());
    assert!(
        remaining > Duration::ZERO && remaining <= granted.ttl / 2,
        "about half the interval must remain: {remaining:?}"
    );

    // The granted admission authorizes now and refuses by name once the
    // interval has run out: expiry is scheduled from the anchor, on time.
    host.with_admission("alice", |admission| {
        let lease = *admission.lease().expect("lease");
        admission
            .authorize(kit::COLLECTION, AccessAction::Ingest)
            .expect("the granted admission authorizes");
        std::thread::sleep(lease.ttl + Duration::from_millis(50));
        let error = admission.check_fresh().err().expect("past the deadline");
        assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
        assert!(
            error.message().contains("lease expired"),
            "the refusal must name the lease expiry: {error}"
        );
        Ok(())
    })
    .await
    .unwrap();

    cluster.insert(leader, Arc::try_unwrap(host).ok().expect("tasks finished"));
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Scenario 2: elapsed time alone consumes the interval — no revocation or
// election is needed to defeat a delayed grant.
// ---------------------------------------------------------------------------

/// A grant whose quorum responses (and the pause) consumed 80% of the lease
// is granted with at most 20% of the interval remaining: the deadline is
// anchor + ttl, never grant + ttl. A second grant held past the ttl with a
// healthy quorum is refused at the grant — pure timing, no election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_quorum_response_cannot_extend_the_interval() {
    let _serial = serial();
    let config = raft_kit::cluster_host_config();
    let lease = Duration::from_millis(lease_ms(&config));
    let mut cluster = raft_kit::three_voters("admission-delay", &hosted_policy()).await;
    let leader = cluster.leader().await;

    // First admission: hold the pause for 80% of the lease, then release.
    let gate = cluster.host(leader).arm_grant_gate();
    let host = Arc::new(cluster.take(leader));
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| {
                let lease = *admission
                    .lease()
                    .expect("a raft admission carries its lease");
                admission
                    .authorize(kit::COLLECTION, AccessAction::Ingest)
                    .map(|_| lease)
            })
            .await
        })
    };
    gate.reached().await;
    tokio::time::sleep(lease * 4 / 5).await;
    gate.release();
    let granted = task.await.unwrap().unwrap();

    // The deadline is anchor + ttl exactly: elapsed time was consumed, not
    // forgiven.
    assert_eq!(
        granted.deadline(),
        granted.anchor + granted.ttl,
        "the deadline must be anchored, not grant-extended"
    );
    let remaining = granted.deadline().saturating_duration_since(Instant::now());
    assert!(
        remaining > Duration::ZERO,
        "the grant must still be inside its interval"
    );
    assert!(
        remaining < granted.ttl,
        "the elapsed 80% of the interval must be gone: {remaining:?} left of {:?}",
        granted.ttl
    );
    assert!(
        remaining <= granted.ttl / 5 + Duration::from_millis(20),
        "at most ~20% of the interval may remain: {remaining:?}"
    );

    // Second admission: hold the pause past the whole lease on a healthy
    // quorum. Elapsed time alone defeats the grant.
    let gate = host.arm_grant_gate();
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| {
                admission.authorize(kit::COLLECTION, AccessAction::Ingest)
            })
            .await
        })
    };
    gate.reached().await;
    tokio::time::sleep(lease + Duration::from_millis(50)).await;
    gate.release();
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(
        error.code(),
        Code::FailedPrecondition,
        "a grant past its interval must be refused even with no election: {error}"
    );
    assert!(
        error.message().contains("elapsed before the grant"),
        "the refusal must name the grant-time check: {error}"
    );

    cluster.insert(leader, Arc::try_unwrap(host).ok().expect("tasks finished"));
    cluster.shutdown().await;
}

/// An isolated leader's barrier fails within the read bound (the election
/// ceiling), not after it: `Unavailable`, quickly, naming the linearizable
/// read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_leader_barrier_fails_within_election_max() {
    let _serial = serial();
    let config = raft_kit::cluster_host_config();
    let bound = Duration::from_millis(election_max_ms(&config));
    let cluster = raft_kit::three_voters("admission-barrier", &hosted_policy()).await;
    let leader = cluster.leader().await;

    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    cluster.host(leader).isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }

    let started = Instant::now();
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize(kit::COLLECTION, AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(
        error.code(),
        Code::Unavailable,
        "an isolated leader must refuse admission: {error}"
    );
    assert!(
        error.message().contains("linearizable"),
        "the refusal must name the barrier: {error}"
    );
    assert!(
        elapsed <= bound + Duration::from_millis(100),
        "the barrier must fail within the read bound ({bound:?}), not after: {elapsed:?}"
    );

    for other in &others {
        cluster.host(*other).heal();
    }
    cluster.host(leader).heal();
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Scenario 3: the source write boundary. Entry admits; the final check
// inside the source transaction is where the interval is measured; a
// refusal leaves nothing durable and no retry record; the receipt
// discloses nothing about the lease.
// ---------------------------------------------------------------------------

/// A write paused inside its source transaction past the lease is refused
/// at the final check: re-accepting the same request under a fresh
/// admission is new work (no retry record, no durable change). A pause that
/// ends within the lease commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_paused_past_lease_leaves_no_durable_change() {
    let _serial = serial();
    let config = raft_kit::cluster_host_config();
    let lease = Duration::from_millis(lease_ms(&config));
    let mut cluster = raft_kit::three_voters("admission-write", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("admission-write-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "prepare-owner").await;
    let request = catalog_write(b"document-two", b"accept-two");

    // Entry admits under a fresh lease; the precommit pause outlasts it;
    // the final check refuses by name.
    active.arm_precommit_pause(lease + Duration::from_millis(200));
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| active.accept(admission, &request))
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::FailedPrecondition,
        "the final check inside the source transaction must refuse: {error}"
    );
    assert!(
        error.message().contains("lease expired"),
        "the refusal must name the lease expiry: {error}"
    );

    // No durable change and no retry record: the same request under a
    // fresh admission is accepted as new work at the next sequence.
    let receipt = cluster
        .host(leader)
        .with_admission("alice", |admission| active.accept(admission, &request))
        .await
        .unwrap();
    assert!(
        !receipt.replayed,
        "a refused write must leave no retry record: the same request must be new work"
    );
    assert_eq!(
        receipt.accepted_sequence, 2,
        "the fixture write is sequence 1; the refused write must not have consumed one"
    );

    // Positive control: a pause that ends within the lease commits.
    let mut third = catalog_write(b"document-three", b"accept-three");
    third.expected_version = Some(0);
    active.arm_precommit_pause(lease / 2);
    let receipt = cluster
        .host(leader)
        .with_admission("alice", |admission| active.accept(admission, &third))
        .await
        .unwrap();
    assert_eq!(receipt.accepted_sequence, 3, "the in-lease write commits");

    drop(active);
    cluster.shutdown().await;
}

/// The epoch fence and the revocation together: a stale write epoch is
/// refused by name while the owner is ACTIVE; a write in flight through the
/// old leader while the survivors revoke is refused at its final check; and
/// after the old leader catches up, the revoked principal is denied at
/// every admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_paused_during_revocation_is_refused_and_epoch_fenced() {
    let _serial = serial();
    let config = raft_kit::cluster_host_config();
    let lease = Duration::from_millis(lease_ms(&config));
    let mut cluster = raft_kit::three_voters("admission-revoke", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("admission-revoke-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "prepare-owner").await;

    // The fence exists before anything moves: the committed epoch admits, a
    // different epoch refuses by name (docs/raft-admission.md: "the write is
    // fenced against replacement by the owner's write epoch").
    cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission
                .admit_write(&kit::owner_key(), 1, AccessAction::Ingest)
                .expect("the committed epoch admits");
            let error = admission
                .admit_write(&kit::owner_key(), 2, AccessAction::Ingest)
                .err()
                .expect("a stale epoch must refuse");
            assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
            assert!(
                error.message().contains("the fence has moved"),
                "the refusal must name the epoch fence: {error}"
            );
            Ok(())
        })
        .await
        .unwrap();

    // Start a write paused inside its transaction, then isolate the leader
    // and let the survivors elect and revoke alice while it sleeps.
    active.arm_precommit_pause(lease + Duration::from_millis(300));
    let host = Arc::new(cluster.take(leader));
    let request = catalog_write(b"document-two", b"accept-two");
    let task = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            host.with_admission("alice", |admission| active.accept(admission, &request))
                .await
        })
    };
    // The write is inside its pause before the lease expires; give the
    // entry admit a moment to run first.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    host.isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }
    let successor = cluster.leader_other_than(leader).await;
    cluster
        .propose(
            successor,
            "bob",
            "revoke-alice",
            &kit::key(),
            revoke_alice_action(),
        )
        .await;
    let revoked_at = cluster
        .host(successor)
        .applied_position()
        .unwrap()
        .unwrap()
        .index;

    // The pause ends on its own; the final check finds the lease lapsed.
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(
        error.code(),
        Code::FailedPrecondition,
        "the in-flight write must be refused at its final check: {error}"
    );
    assert!(
        error.message().contains("lease expired"),
        "the refusal must name the lease expiry: {error}"
    );

    // Heal: the old leader applies the revocation; alice is denied on the
    // current leader, and the old leader grants nothing as a follower.
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    let host = Arc::try_unwrap(host).ok().expect("tasks finished");
    cluster.insert(leader, host);
    cluster.wait_applied(leader, revoked_at).await;
    let error = cluster
        .host(successor)
        .with_admission("alice", |admission| {
            admission.authorize(kit::COLLECTION, AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::PermissionDenied,
        "the revoked principal must be denied at the current leader: {error}"
    );
    let error = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            admission.authorize(kit::COLLECTION, AccessAction::Ingest)
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Code::Unavailable,
        "the old leader grants nothing after catch-up: {error}"
    );

    cluster.shutdown().await;
}

/// The receipt from an admitted write discloses nothing about the lease:
/// its field set is exactly the document identity, version, sequence,
/// flags and history — no anchor, deadline, ttl or principal-derived lease
/// material (docs/raft-admission.md: "the receipt discloses nothing about
/// the lease").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receipt_discloses_nothing_about_the_lease() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("admission-receipt", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("admission-receipt-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "prepare-owner").await;
    let request = catalog_write(b"document-two", b"accept-two");

    let receipt = cluster
        .host(leader)
        .with_admission("alice", |admission| active.accept(admission, &request))
        .await
        .unwrap();

    // The complete public field set of `DocumentWriteReceipt` at 0fe7081.
    // A lease field added to this struct is a contract change and must fail
    // this exhaustive destructure at compile time.
    let DocumentWriteReceipt {
        document_key,
        version,
        accepted_sequence,
        accepted,
        searchable,
        durable,
        replayed,
        history_id,
    } = receipt.clone();

    assert_eq!(document_key, request.document_key);
    assert_eq!(version, 1, "the first accept of this document is version 1");
    assert_eq!(accepted_sequence, 2);
    assert!(accepted);
    assert!(!searchable, "searchable is a downstream projection flag");
    assert!(durable);
    assert!(!replayed);
    assert_eq!(history_id.len(), 16);

    // No serialized lease material: the admission's wall-clock anchor must
    // not appear in the encoded receipt, and the debug form must not name
    // the lease. (The lease's monotonic anchor/deadline are `Instant`s and
    // have no serialized form to check for.)
    let encoded = {
        use prost::Message;
        receipt.encode_to_vec()
    };
    let wall_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let probe = wall_millis.to_be_bytes();
    assert!(
        !encoded.windows(8).any(|window| window == probe.as_slice()),
        "the receipt must not carry a wall-clock timestamp of the admission"
    );
    let debug = format!("{receipt:?}");
    for needle in ["lease", "expir", "anchor", "ttl"] {
        assert!(
            !debug.to_lowercase().contains(needle),
            "the receipt debug form must not disclose lease material ({needle:?})"
        );
    }

    drop(active);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Crash arm: a write that passed its final check may finish. The hook
// between the final check and the commit is process-internal and has no
// deterministic external trigger, so this arm kills the process right
// after a successful accept (durability guaranteed by the return) and
// verifies convergence after restart: the contents are durable exactly
// once and the retry resolves to the recorded receipt. Single-node hosted
// by design: a raft member restart with a seeded catalog is the subject of
// slice-3c/4a restart coverage, not of this timing gap.
// ---------------------------------------------------------------------------

const MARKER: &str = "admitted.marker";
const BINDING_FILE: &str = "managed-binding.bin";

/// Worker: build the single-node hosted group and ACTIVE owner, accept one
/// write, drop the marker, and park until killed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_admission_write_worker() {
    if std::env::var_os(kit::WORKER_DIR_ENV).is_none() {
        return;
    }
    let dir = kit::TestDir::from_env();
    let guard = raft_kit::bootstrap_host_with(&dir, &hosted_policy()).await;
    let active = {
        // The bridge against a single-node host reuses the cluster bridge by
        // wrapping the guard in a one-node cluster view is overkill; drive
        // the same proposals directly.
        let authority = kit::identity(kit::SEED);
        let (catalog, history_id) = catalog_fixture(&dir);
        let prepare = kit::source_command(
            &authority,
            &kit::owner_key(),
            "prepare-owner",
            1,
            1,
            0,
            kit::prepare_action_history(WORKFLOW, history_id),
        );
        let decision = guard.propose_command("alice", &prepare).await.unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        let preparation = decision.owner.clone().unwrap();
        let store = guard.store().unwrap();
        let managed = guard
            .with_admission("alice", |admission| {
                catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
            })
            .await
            .unwrap();
        use prost::Message as _;
        let binding = managed.binding("alice").unwrap();
        std::fs::write(dir.path().join(BINDING_FILE), binding.encode_to_vec()).unwrap();
        let confirm = confirm_command(
            &authority,
            2,
            managed.completion().unwrap().completion().clone(),
        );
        let confirmed = guard
            .propose_confirm_ready("alice", &confirm, &managed.completion().unwrap())
            .await
            .unwrap();
        assert_eq!(confirmed.code, 0, "{}", confirmed.message);
        let activate = kit::source_command(
            &authority,
            &kit::owner_key(),
            "activate-owner",
            3,
            1,
            1,
            kit::activate_action(WORKFLOW),
        );
        let activated = guard.propose_command("alice", &activate).await.unwrap();
        assert_eq!(activated.code, 0, "{}", activated.message);
        guard
            .with_admission("alice", |admission| managed.activate(admission))
            .await
            .unwrap()
    };
    let request = catalog_write(b"document-two", b"accept-two");
    let receipt = guard
        .with_admission("alice", |admission| active.accept(admission, &request))
        .await
        .unwrap();
    std::fs::write(
        dir.path().join(MARKER),
        receipt.accepted_sequence.to_string(),
    )
    .unwrap();
    // Park until the parent kills us: durability already happened.
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

/// Verify worker: reopen the killed group, recover the ACTIVE owner, and
/// prove the write is durable exactly once with its retry recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_admission_verify_worker() {
    if std::env::var_os(kit::WORKER_DIR_ENV).is_none() {
        return;
    }
    let dir = kit::TestDir::from_env();
    // The redb file lock and raft teardown can lag a SIGKILL; reopen with a
    // bounded retry (the slice-3c restart recipe).
    let mut guard = None;
    for _ in 0..40 {
        match RaftHost::start(
            dir.path(),
            &kit::identity(kit::SEED),
            raft_kit::NODE_ID,
            &raft_kit::host_config(),
        )
        .await
        {
            Ok(host) => {
                guard = Some(host);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    let guard = guard.expect("hosted group reopens after the crash");
    raft_kit::wait_leader(&guard).await;

    let store = guard.store().unwrap();
    let owner = store.owner("alice", &kit::owner_key()).unwrap();
    assert_eq!(
        owner.phase,
        pipestream_search::pb::storage::PreparedSourceOwnerPhase::Active as i32,
        "the activated owner survives the crash"
    );
    let binding = binding_from_worker(&dir);
    let active = guard
        .with_admission("alice", |admission| {
            ActiveManagedCatalog::recover(
                &dir.path().join("catalog.redb"),
                &store,
                admission,
                &binding,
            )
        })
        .await
        .unwrap();

    let request = catalog_write(b"document-two", b"accept-two");
    let receipt = guard
        .with_admission("alice", |admission| active.accept(admission, &request))
        .await
        .unwrap();
    assert!(
        receipt.replayed,
        "the retried write after a crash must resolve to the recorded receipt"
    );
    assert_eq!(
        receipt.accepted_sequence, 2,
        "the write is durable exactly once at sequence 2"
    );
    assert_eq!(receipt.version, 1);
    guard.shutdown().await.unwrap();
}

/// Parent: kill the worker right after its accept and verify convergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_final_check_converges() {
    let _serial = serial();
    let dir = kit::TestDir::new("admission-crash");
    let mut worker = kit::KillOnDrop(kit::spawn_worker(
        "crash_admission_write_worker",
        &[(kit::WORKER_DIR_ENV, dir.path().to_str().unwrap())],
    ));
    // Wait for the marker: the accept returned and durability is promised.
    let deadline = Instant::now() + Duration::from_secs(60);
    let marker = dir.path().join(MARKER);
    while !marker.exists() {
        assert!(Instant::now() < deadline, "worker never admitted the write");
        std::thread::sleep(Duration::from_millis(50));
    }
    worker.child().kill().unwrap();
    let status = worker.child().wait().unwrap();
    assert!(
        status.signal().is_some(),
        "the worker must die by signal, not exit: {status:?}"
    );
    drop(worker);

    let mut verify = kit::KillOnDrop(kit::spawn_worker(
        "crash_admission_verify_worker",
        &[(kit::WORKER_DIR_ENV, dir.path().to_str().unwrap())],
    ));
    let status = verify.child().wait().unwrap();
    assert!(status.success(), "verify worker failed: {status:?}");
}
