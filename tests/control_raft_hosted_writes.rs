//! Hosted owner writes: the network write service in which every admission
//! comes from the Raft host's lease, over three voters on loopback mTLS.
//!
//! The oracle is docs/raft-admission.md: a write admitted under a current
//! lease commits and stays durable; a lease that lapsed before the grant
//! admits nothing and leaves no row and no retry record; off-leader and
//! isolated members refuse by name.
//!
//! Compiled only with `raft`, `tls` and `fault-injection` together (the
//! `control_raft_admission` convention: the lease scenarios need the pause
//! hook `arm_grant_delay`, and the group needs the mTLS transport). The
//! write service itself serves plain loopback HTTP here with Bearer [REDACTED]
//! in production it registers on the process's secured listener.
#![cfg(all(feature = "raft", feature = "tls", feature = "fault-injection"))]

mod control_adversarial;

use std::sync::Arc;
use std::time::{Duration, Instant};

use control_adversarial::{kit, raft_kit};
use pipestream_search::authorization::{AccessPermit, Authorizer, PolicyAuthority};
use pipestream_search::document_catalog::{AccessControlledCatalog, ActiveManagedCatalog};
use pipestream_search::document_write_service::hosted::HostedDocumentWriteService;
use pipestream_search::pb::document_write_service_client::DocumentWriteServiceClient;
use pipestream_search::pb::storage::{
    ConfirmSourceOwnerReady, SourceAuthorityIdentity, SourceOwnerCompletion,
};
use pipestream_search::pb::{
    accept_document_request::Mutation, AcceptDocumentRequest, AccessAction, AccessPolicy,
    DocumentWriteRequest, DocumentWriteServiceLimits, GetDocumentWriteTargetRequest,
    ProtobufSource,
};
use pipestream_search::raft::RaftHost;
use tonic::Code;

use raft_kit::Voters;

/// Target-wide serialization: the tests share timing-sensitive election
/// windows and loopback listeners, so each holds this lock whole-body.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

const WORKFLOW: &[u8] = b"hosted-owner-one";

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

fn principals() -> pipestream_search::security::Principals {
    principals_with(Arc::new(PolicyAuthority::new(hosted_policy()).unwrap()))
}

/// Transport principals over an authority the test keeps a handle on, so
/// it can replace the policy under a call.
fn principals_with(authority: Arc<PolicyAuthority>) -> pipestream_search::security::Principals {
    use pipestream_search::security::{PrincipalConfig, Principals};
    let authority: Arc<dyn Authorizer> = authority;
    Principals::from_configs(&["alice", "bob"].map(|name| PrincipalConfig {
        name: name.into(),
        token: format!("{name}-token-0123456789"),
        concurrency: 8,
        ..Default::default()
    }))
    .unwrap()
    .with_authorizer(authority)
}

fn auth<T>(message: T, principal: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {principal}-token-0123456789")
            .parse()
            .unwrap(),
    );
    request
}

fn limits() -> DocumentWriteServiceLimits {
    DocumentWriteServiceLimits {
        max_request_bytes: 64 * 1024,
        max_pending_bytes: 256 * 1024,
        max_in_flight: 4,
    }
}

/// One accepted document under `key` (mirrors the managed tests); the
/// fixture's first write is what the managed bridge binds to.
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

/// One network write: contract version 2 pinned to the target history.
fn network_write(
    history_id: Vec<u8>,
    document_key: &[u8],
    operation_id: &[u8],
    expected_version: u64,
) -> DocumentWriteRequest {
    let mut request = catalog_write(document_key, operation_id);
    request.contract_version = 2;
    request.history_id = history_id;
    request.expected_version = Some(expected_version);
    DocumentWriteRequest {
        collection: kit::COLLECTION.into(),
        document: Some(request),
    }
}

/// Create the managed catalog and return it with the history id the owner
/// preparation must pin.
fn catalog_fixture(dir: &kit::TestDir) -> (AccessControlledCatalog, Vec<u8>) {
    let authorizer: Arc<dyn Authorizer> = Arc::new(PolicyAuthority::new(hosted_policy()).unwrap());
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
/// ACTIVE catalog; the leader's revision tracker advances by three.
async fn bridge_active(
    voters: &mut Voters,
    leader: u64,
    dir: &kit::TestDir,
    owner_label: &str,
) -> ActiveManagedCatalog {
    let authority = voters.group.clone();
    let (catalog, history_id) = catalog_fixture(dir);

    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        owner_label,
        voters.revision,
        1,
        0,
        kit::prepare_action_history(WORKFLOW, history_id),
    );
    let decision = voters
        .host(leader)
        .propose_command("alice", &prepare)
        .await
        .unwrap();
    assert_eq!(decision.code, 0, "{}", decision.message);
    voters.revision += 1;
    let preparation = decision.owner.clone().unwrap();

    let store = voters.host(leader).store().unwrap();
    let managed = voters
        .host(leader)
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
    let verified = managed.completion().unwrap();
    assert_eq!(verified.completion().bound_at_sequence, 1);

    let confirm = confirm_command(&authority, voters.revision, verified.completion().clone());
    let confirmed = voters
        .host(leader)
        .propose_confirm_ready("alice", &confirm, &managed.completion().unwrap())
        .await
        .unwrap();
    assert_eq!(confirmed.code, 0, "{}", confirmed.message);
    voters.revision += 1;

    let activate = kit::source_command(
        &authority,
        &kit::owner_key(),
        "activate-owner",
        voters.revision,
        1,
        1,
        kit::activate_action(WORKFLOW),
    );
    let activated = voters
        .host(leader)
        .propose_command("alice", &activate)
        .await
        .unwrap();
    assert_eq!(activated.code, 0, "{}", activated.message);
    voters.revision += 1;

    voters
        .host(leader)
        .with_admission("alice", |admission| managed.activate(admission))
        .await
        .unwrap()
}

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestServer {
    async fn shutdown(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        match tokio::time::timeout(Duration::from_secs(10), &mut self.task).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                panic!("hosted-write server did not release its connections");
            }
        }
    }
}

async fn serve(
    service: HostedDocumentWriteService,
) -> (
    DocumentWriteServiceClient<tonic::transport::Channel>,
    TestServer,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopping) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = stopping.await;
                },
            ),
    );
    let client = DocumentWriteServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    (
        client,
        TestServer {
            shutdown: Some(shutdown),
            task,
        },
    )
}

fn revoke_alice_action() -> pipestream_search::pb::storage::source_authority_command::Action {
    kit::replace_grants_action(vec![kit::grant("bob", kit::COLLECTION)])
}

/// Serve the hosted service on `node`, taken out of the voters. The caller
/// shuts the server down, then returns the host with `insert`.
fn take_host(voters: &mut Voters, node: u64) -> Arc<RaftHost> {
    Arc::new(voters.take(node))
}

fn return_host(voters: &mut Voters, node: u64, host: Arc<RaftHost>) {
    let host = Arc::try_unwrap(host)
        .ok()
        .expect("no task still holds the host");
    voters.insert(node, host);
}

// ---------------------------------------------------------------------------
// A write through the leader's hosted service is admitted and durable.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_through_the_hosted_service_is_admitted_by_the_leader_and_durable() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-write", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-write-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    let catalog = Arc::new(active);
    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;

    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(target.history_id.len(), 16);

    let request = network_write(
        target.history_id.clone(),
        b"hosted-one",
        b"hosted-op-one",
        0,
    );
    let first = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(first.accepted && first.durable && !first.searchable);
    assert_eq!(first.version, 1);
    assert!(!first.replayed);

    // The exact retry answers from the stored decision.
    let replay = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed);
    assert_eq!(
        (replay.version, replay.accepted_sequence),
        (first.version, first.accepted_sequence)
    );

    // The row is in the catalog itself: the fixture's write was the first
    // accepted row, the network write is the second.
    let inspected = host
        .with_admission("alice", |admission| catalog.inspect(admission, 1 << 20))
        .await
        .unwrap();
    assert_eq!(
        inspected
            .header
            .as_ref()
            .map(|header| header.accepted_sequence),
        Some(2),
        "the network write must be the catalog's second accepted row"
    );

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// A write through a follower names the leader and refuses.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_through_a_follower_is_rejected_naming_the_leader() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-follower", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let follower = *cluster
        .nodes()
        .iter()
        .find(|n| **n != leader)
        .expect("three voters");
    let dir = kit::TestDir::new("hosted-follower-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    // The target comes from the leader's service: on a follower even the
    // target read takes a lease and refuses.
    let catalog = Arc::new(active);
    let leader_host = take_host(&mut cluster, leader);
    let leader_service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&leader_host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut leader_client, leader_server) = serve(leader_service).await;
    let target = leader_client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    drop(leader_client);
    leader_server.shutdown().await;
    return_host(&mut cluster, leader, leader_host);

    let host = take_host(&mut cluster, follower);
    let service =
        HostedDocumentWriteService::new(principals(), Arc::clone(&host), vec![catalog], limits())
            .unwrap();
    let (mut client, server) = serve(service).await;
    let error = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-one", b"hosted-op-one", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(
        error
            .message()
            .contains(&format!("not the leader; leader is Some({leader})")),
        "the follower must name the leader it refuses for: {error}"
    );

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, follower, host);
    cluster.shutdown().await;
}

use pipestream_search::config::RaftManagedCatalog;
use pipestream_search::document_write_service::hosted::recover_catalogs;

fn managed_entry(collection: &str, dir: &kit::TestDir) -> RaftManagedCatalog {
    RaftManagedCatalog {
        collection: collection.into(),
        path: dir.path().join("catalog.redb"),
    }
}

// ---------------------------------------------------------------------------
// A lease kept past its interval leaves no durable change.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_kept_past_its_lease_leaves_no_durable_change() {
    let _serial = serial();
    let config = raft_kit::host_config();
    let lease = Duration::from_millis(config.admission_lease_ms);
    let mut cluster = raft_kit::three_voters("hosted-kept-lease", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-kept-lease-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::new(active)],
        limits(),
    )
    .unwrap();
    // The target is fetched before the delay is armed: the target read
    // takes a lease of its own and would expire under the same pause.
    // The clone shares the service state, so arming through it arms the
    // served copy.
    let handle = service.clone();
    let (mut client, server) = serve(service).await;

    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    handle.arm_grant_delay(lease * 3);
    let request = network_write(
        target.history_id.clone(),
        b"hosted-one",
        b"hosted-op-one",
        0,
    );
    let error = client
        .accept_document(auth(request.clone(), "alice"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(
        error
            .message()
            .contains("admission interval elapsed before the grant"),
        "the refusal must name the grant-time check: {error}"
    );

    // The same operation retried after a fresh admission is new work, not
    // a replay: the refused grant left no row and no retry record.
    let second = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(second.accepted && !second.replayed);
    assert_eq!(second.version, 1);

    drop(handle);
    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// An isolated leader refuses through the service within the election
// ceiling; after healing it still refuses as a follower.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_rejects_hosted_writes_within_election_timeout_max() {
    let _serial = serial();
    let config = raft_kit::host_config();
    let election_max = Duration::from_millis(config.election_timeout_max_ms);
    let mut cluster = raft_kit::three_voters("hosted-isolation", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-isolation-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;

    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::new(active)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();

    // Isolate the old leader from both peers, both directions.
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    host.isolate(others.iter().copied());
    for other in &others {
        cluster.host(*other).isolate([leader]);
    }

    // The barrier is bounded by the election ceiling, so the refusal
    // arrives within it however long the isolation lasts; the lease is
    // slack for the local service path around the barrier.
    let lease = Duration::from_millis(config.admission_lease_ms);
    let request_at = Instant::now();
    let error = client
        .accept_document(auth(
            network_write(
                target.history_id.clone(),
                b"hosted-one",
                b"hosted-op-one",
                0,
            ),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(
        request_at.elapsed() < election_max + lease,
        "the refusal must arrive near the read bound, not after a full election: {:?}",
        request_at.elapsed()
    );

    // The surviving quorum elects a successor and commits the revocation
    // of alice while the old side still shows the stale policy.
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

    // Heal: the old leader catches up to the revoked policy and still
    // refuses as a follower, naming the successor.
    host.heal();
    for other in &others {
        cluster.host(*other).heal();
    }
    host.wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(revoked_at), "applied")
        .await
        .unwrap();
    let error = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-two", b"hosted-op-two", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unavailable, "{error}");
    assert!(
        error
            .message()
            .contains(&format!("not the leader; leader is Some({successor})")),
        "the healed old leader must refuse as a follower naming the successor: {error}"
    );

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Restart: every member restarts on its directory, the catalogs recover
// through the same library function the binary uses, and the leader's
// service serves writes again.
//
// Leadership after a restart is not deterministic, so the restart covers
// the whole group (as a fleet rollout would) and the write goes to
// whichever member leads: recovery itself is member-agnostic, running on
// the host's store under a leased admission.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_restarted_with_its_managed_catalogs_serves_writes_again() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-restart", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-restart-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);
    let entry = managed_entry(kit::COLLECTION, &dir);

    // Record every member's bound address, then stop the whole group.
    let mut states = Vec::new();
    for node in cluster.nodes() {
        let addr = cluster.host(node).listen_addr().expect("listening");
        states.push((node, cluster.member_dir(node), addr));
    }
    let mut hosts = Vec::new();
    for node in cluster.nodes() {
        hosts.push(cluster.take(node));
    }
    for host in hosts {
        host.shutdown().await.unwrap();
    }

    // Start every member again on its own directory and address. The first
    // one up recovers the catalogs at once: one member of three is running,
    // so no leader can exist, and recovery reads the applied view.
    let mut recovered_without_a_leader = None;
    for (node, dir, addr) in &states {
        let host = RaftHost::start_member(
            dir,
            &cluster.group,
            *node,
            &cluster.config,
            raft_kit::transport(*node, &cluster.directory, *addr),
        )
        .await
        .unwrap();
        assert!(
            !host.awaiting_snapshot().unwrap(),
            "a restarted member must hold its seeded state"
        );
        if recovered_without_a_leader.is_none() {
            // One member of three: the library may resume the old leader's
            // role from its committed vote, but no quorum is in reach, so
            // no lease can be granted here and a leased recovery could not
            // have run.
            let no_lease = host.lease("alice").await.err().expect("no quorum");
            assert_eq!(no_lease.code(), Code::Unavailable, "{no_lease}");
            let recovered = recover_catalogs(&host, "alice", &[entry.clone()]).unwrap();
            recovered_without_a_leader = Some(recovered.len());
        }
        cluster.insert(*node, host);
    }
    assert_eq!(recovered_without_a_leader, Some(1));
    let leader = cluster.leader().await;

    // Recover through the same function the binary calls at start, then
    // serve one write on the leader.
    let host = take_host(&mut cluster, leader);
    let recovered = recover_catalogs(&host, "alice", &[entry]).unwrap();
    assert_eq!(recovered.len(), 1);
    let service =
        HostedDocumentWriteService::new(principals(), Arc::clone(&host), recovered, limits())
            .unwrap();
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let receipt = client
        .accept_document(auth(
            network_write(target.history_id, b"hosted-after-restart", b"restart-op", 0),
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(receipt.accepted && receipt.durable && !receipt.replayed);
    assert_eq!(receipt.version, 1);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

/// A catalog file bound under a preparation but never activated: recovery
/// names the prepared state and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_a_prepared_but_inactive_catalog_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-prepared", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-prepared-catalog");
    let authority = cluster.group.clone();
    let (catalog, history_id) = catalog_fixture(&dir);
    let prepare = kit::source_command(
        &authority,
        &kit::owner_key(),
        "hosted-prepare",
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
    let preparation = decision.owner.clone().unwrap();
    let store = cluster.host(leader).store().unwrap();
    let prepared = cluster
        .host(leader)
        .with_admission("alice", |admission| {
            catalog.bind_prepared_owner(admission, &store, &preparation, 1 << 20)
        })
        .await
        .unwrap();
    drop(prepared);
    drop(store);

    let host = take_host(&mut cluster, leader);
    let error = recover_catalogs(&host, "alice", &[managed_entry(kit::COLLECTION, &dir)])
        .err()
        .expect("a prepared catalog must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains("prepared, not active"),
        "the rejection must name the prepared state: {error}"
    );

    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

/// A catalog file bound under another authority: recovery names the
/// foreign identity and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_a_foreign_authority_catalog_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-foreign", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-foreign-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);

    // A member of another group: its store carries another identity, so
    // the file's binding names an authority it never served.
    let foreign_dir = kit::TestDir::new("hosted-foreign-member");
    let foreign = RaftHost::bootstrap_single(
        foreign_dir.path(),
        &kit::identity(9),
        1,
        "https://node-1:19291",
        &hosted_policy(),
        &kit::limits(),
        &raft_kit::host_config(),
    )
    .await
    .unwrap();
    let error = recover_catalogs(&foreign, "alice", &[managed_entry(kit::COLLECTION, &dir)])
        .err()
        .expect("a foreign catalog must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains("another source authority"),
        "the rejection must name the foreign authority: {error}"
    );
    foreign.shutdown().await.unwrap();

    cluster.shutdown().await;
}

/// A catalog file whose activation the store does not record: recovery
/// names the fence mismatch and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_with_an_unrecorded_activation_is_rejected_by_name() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-unrecorded", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-unrecorded-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);

    // A fresh store of the same identity holds no owner row at all, so the
    // file's activation is recorded nowhere in it: the fence lookup finds
    // no committed preparation to compare against.
    let fresh_dir = kit::TestDir::new("hosted-unrecorded-member");
    let fresh = raft_kit::bootstrap_host_with(&fresh_dir, &hosted_policy()).await;
    let error = recover_catalogs(&fresh, "alice", &[managed_entry(kit::COLLECTION, &dir)])
        .err()
        .expect("an unrecorded activation must not recover");
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(
        error.message().contains("has no committed preparation"),
        "the rejection must name the unrecorded activation: {error}"
    );
    fresh.shutdown().await.unwrap();

    cluster.shutdown().await;
}

/// Timing under which a write can be parked inside its transaction for a
/// few hundred milliseconds without the lease lapsing: the default
/// production values on the kit's snapshot and chunk settings.
fn slow_lease_config() -> pipestream_search::raft::HostConfig {
    pipestream_search::raft::HostConfig {
        heartbeat_interval_ms: 250,
        election_timeout_min_ms: 1_000,
        election_timeout_max_ms: 2_000,
        admission_lease_ms: 500,
        clock_skew_ms: 250,
        ..raft_kit::host_config()
    }
}

fn one_in_flight() -> DocumentWriteServiceLimits {
    DocumentWriteServiceLimits {
        max_request_bytes: 64 * 1024,
        max_pending_bytes: 256 * 1024,
        max_in_flight: 1,
    }
}

// ---------------------------------------------------------------------------
// One member restarted while the other two keep running: it recovers its
// catalogs at start, before it has any leader, and serves writes again
// once it leads.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_member_restarted_recovers_at_start_and_serves_once_it_leads() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters("hosted-single-restart", &hosted_policy()).await;
    let leader = cluster.leader().await;
    let others: Vec<u64> = cluster
        .nodes()
        .into_iter()
        .filter(|n| *n != leader)
        .collect();
    let dir = kit::TestDir::new("hosted-single-restart-catalog");
    let active = bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await;
    drop(active);
    let entry = managed_entry(kit::COLLECTION, &dir);
    let addr = cluster.host(leader).listen_addr().expect("listening");
    let member_dir = cluster.member_dir(leader);

    // Cut every link, then leave one entry in the leader's log that no peer
    // holds: a command the state machine refuses (a stale expected
    // revision), appended locally while its commit waits for a quorum that
    // cannot answer. With the longer log, the restarted member is the only
    // one its peers can elect, whichever of them asks first.
    cluster.host(leader).isolate(others.iter().copied());
    for other in &others {
        let rest: Vec<u64> = cluster.nodes().into_iter().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    let before = cluster.host(leader).log_entries().unwrap().len();
    let host = Arc::new(cluster.take(leader));
    let pending = {
        let host = Arc::clone(&host);
        let stale = kit::source_command(
            &cluster.group,
            &kit::owner_key(),
            "stale-revision",
            1,
            1,
            0,
            kit::prepare_action_history(WORKFLOW, vec![7; 16]),
        );
        tokio::spawn(async move { host.propose_command("alice", &stale).await })
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while host.log_entries().unwrap().len() == before {
        assert!(
            Instant::now() < deadline,
            "the entry never reached the leader's log"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pending.abort();
    let _ = pending.await;
    let host = Arc::try_unwrap(host)
        .ok()
        .expect("the proposal task released the host");
    host.shutdown().await.unwrap();

    // Start the member again on its directory. It has no leader, and the
    // catalogs recover all the same, through the function the binary calls.
    let host = RaftHost::start_member(
        &member_dir,
        &cluster.group,
        leader,
        &cluster.config,
        raft_kit::transport(leader, &cluster.directory, addr),
    )
    .await
    .unwrap();
    // The library may resume the old leader's role from the committed
    // vote, but both peers still cut this member off: no quorum is in
    // reach, no lease can be granted, and recovery runs all the same.
    let no_lease = host.lease("alice").await.err().expect("no quorum");
    assert_eq!(no_lease.code(), Code::Unavailable, "{no_lease}");
    let recovered = recover_catalogs(&host, "alice", &[entry]).unwrap();
    assert_eq!(recovered.len(), 1);

    // Open the peers to the restarted member while they stay cut from each
    // other, and serve on it.
    for other in &others {
        cluster.host(*other).heal();
        let rest: Vec<u64> = others.iter().copied().filter(|n| n != other).collect();
        cluster.host(*other).isolate(rest);
    }
    let host = Arc::new(host);
    let service =
        HostedDocumentWriteService::new(principals(), Arc::clone(&host), recovered, limits())
            .unwrap();
    let (mut client, server) = serve(service).await;

    // Until it leads the barrier finds no quorum for it and the service
    // rejects; the library elects it once a peer grants the vote.
    let deadline = Instant::now() + Duration::from_secs(30);
    while host.metrics().borrow().state != openraft::ServerState::Leader {
        assert!(
            Instant::now() < deadline,
            "the restarted member never led with the longest log"
        );
        host.trigger_election().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for other in &others {
        cluster.host(*other).heal();
    }

    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let receipt = client
        .accept_document(auth(
            network_write(
                target.history_id,
                b"hosted-after-single-restart",
                b"single-restart-op",
                0,
            ),
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(receipt.accepted && receipt.durable && !receipt.replayed);
    assert_eq!(receipt.version, 1);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// A client that drops its call: the worker finishes on its own terms, the
// permits are released only when it returns, and the write is durable.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_drops_its_call_leaves_the_worker_to_finish_under_its_permits() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters_with(
        "hosted-dropped-call",
        &hosted_policy(),
        &slow_lease_config(),
    )
    .await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-dropped-call-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
    let host = take_host(&mut cluster, leader);
    let service = HostedDocumentWriteService::new(
        principals(),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        one_in_flight(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();

    // Park the next write inside its transaction, after the staging and
    // before the final check, for less than the lease; drop the call while
    // it is parked.
    let pause = Duration::from_millis(300);
    catalog.arm_precommit_pause(pause);
    let request = network_write(
        target.history_id.clone(),
        b"hosted-dropped",
        b"dropped-op",
        0,
    );
    let started = Instant::now();
    let dropped = tokio::time::timeout(
        Duration::from_millis(60),
        client.accept_document(auth(request.clone(), "alice")),
    )
    .await;
    assert!(
        dropped.is_err(),
        "the call must still be parked when the client drops it"
    );

    // The worker holds the only in-flight permit until it returns.
    let refused = client
        .accept_document(auth(
            network_write(target.history_id.clone(), b"hosted-second", b"second-op", 0),
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused}");
    assert!(
        refused
            .message()
            .contains("source execution capacity is full"),
        "{refused}"
    );
    assert!(
        started.elapsed() < pause,
        "the second call must arrive while the first is parked"
    );

    // The worker commits with no client listening: once its permit is
    // free, the exact retry replays the durable row.
    let deadline = Instant::now() + Duration::from_secs(10);
    let replay = loop {
        match client.accept_document(auth(request.clone(), "alice")).await {
            Ok(receipt) => break receipt.into_inner(),
            Err(status) if status.code() == Code::ResourceExhausted => {
                assert!(
                    Instant::now() < deadline,
                    "the permit was never released: {status}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(status) => panic!("the retry failed for another reason: {status}"),
        }
    };
    assert!(
        replay.replayed && replay.durable,
        "the dropped call's write must be durable and replay"
    );
    assert_eq!(replay.version, 1);
    assert!(
        started.elapsed() >= pause,
        "the retry can only answer after the worker's pause"
    );

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// The transport policy changes under a committed write: the row is durable
// and the receipt is withheld by the transport gate, by name.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_receipt_is_withheld_when_transport_access_changes_under_a_committed_write() {
    let _serial = serial();
    let mut cluster = raft_kit::three_voters_with(
        "hosted-receipt-race",
        &hosted_policy(),
        &slow_lease_config(),
    )
    .await;
    let leader = cluster.leader().await;
    let dir = kit::TestDir::new("hosted-receipt-race-catalog");
    let catalog = Arc::new(bridge_active(&mut cluster, leader, &dir, "hosted-prepare").await);
    let host = take_host(&mut cluster, leader);
    let transport_policy = Arc::new(PolicyAuthority::new(hosted_policy()).unwrap());
    let service = HostedDocumentWriteService::new(
        principals_with(Arc::clone(&transport_policy)),
        Arc::clone(&host),
        vec![Arc::clone(&catalog)],
        limits(),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: kit::COLLECTION.into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();

    // Park the write inside its transaction and replace the transport
    // policy without alice while it is parked. The authority policy, which
    // admits the write, is unchanged: the commit goes through, and the
    // transport gate withholds the receipt after the worker returns.
    catalog.arm_precommit_pause(Duration::from_millis(300));
    let request = network_write(
        target.history_id.clone(),
        b"hosted-withheld",
        b"withheld-op",
        0,
    );
    let call = {
        let mut client = client.clone();
        let request = request.clone();
        tokio::spawn(async move { client.accept_document(auth(request, "alice")).await })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    let mut without_alice = hosted_policy();
    without_alice.revision = 2;
    without_alice
        .grants
        .retain(|grant| grant.principal != "alice");
    transport_policy.replace(without_alice).unwrap();
    let withheld = call.await.unwrap().unwrap_err();
    assert_eq!(withheld.code(), Code::PermissionDenied, "{withheld}");
    assert!(
        withheld.message().contains("access policy changed"),
        "the withheld receipt must name the policy change: {withheld}"
    );

    // Durable regardless: with alice granted again, the exact retry replays.
    let mut restored = hosted_policy();
    restored.revision = 3;
    transport_policy.replace(restored).unwrap();
    let replay = client
        .accept_document(auth(request, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed && replay.durable);
    assert_eq!(replay.version, 1);

    drop(client);
    server.shutdown().await;
    return_host(&mut cluster, leader, host);
    cluster.shutdown().await;
}
