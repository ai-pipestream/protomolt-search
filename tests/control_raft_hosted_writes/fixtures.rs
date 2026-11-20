//! Shared Raft voter cluster, mTLS bridge, and hosted document-write fixtures.

use super::*;

/// Target-wide serialization: the tests share timing-sensitive election
/// windows and loopback listeners, so each holds this lock whole-body.
pub(crate) static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) const WORKFLOW: &[u8] = b"hosted-owner-one";

/// The hosted policy: alice and bob Admin, plus the Ingest grant the
/// write-admission entry check requires (mirrors the in-crate fixtures).
pub(crate) fn hosted_policy() -> AccessPolicy {
    let mut policy = kit::policy();
    for grant in &mut policy.grants {
        if grant.principal == "alice" {
            grant.actions.push(AccessAction::Ingest as i32);
        }
    }
    policy
}

pub(crate) fn principals() -> pipestream_search::security::Principals {
    principals_with(Arc::new(PolicyAuthority::new(hosted_policy()).unwrap()))
}

/// Transport principals over an authority the test keeps a handle on, so
/// it can replace the policy under a call.
pub(crate) fn principals_with(
    authority: Arc<PolicyAuthority>,
) -> pipestream_search::security::Principals {
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

pub(crate) fn auth<T>(message: T, principal: &str) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {principal}-token-0123456789")
            .parse()
            .unwrap(),
    );
    request
}

pub(crate) fn limits() -> DocumentWriteServiceLimits {
    DocumentWriteServiceLimits {
        max_request_bytes: 64 * 1024,
        max_pending_bytes: 256 * 1024,
        max_in_flight: 4,
    }
}

/// One accepted document under `key` (mirrors the managed tests); the
/// fixture's first write is what the managed bridge binds to.
pub(crate) fn catalog_write(document_key: &[u8], operation_id: &[u8]) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: document_key.to_vec(),
        operation_id: operation_id.to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: include_bytes!("../fixtures/protobuf-semantics/descriptor.bin")
                .to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 7],
        })),
        ..Default::default()
    }
}

/// One network write: contract version 2 pinned to the target history.
pub(crate) fn network_write(
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
pub(crate) fn catalog_fixture(dir: &kit::TestDir) -> (AccessControlledCatalog, Vec<u8>) {
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

pub(crate) fn confirm_command(
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
pub(crate) async fn bridge_active(
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

pub(crate) struct TestServer {
    pub(crate) shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    pub(crate) task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestServer {
    pub(crate) async fn shutdown(mut self) {
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

pub(crate) async fn serve(
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

pub(crate) fn revoke_alice_action(
) -> pipestream_search::pb::storage::source_authority_command::Action {
    kit::replace_grants_action(vec![kit::grant("bob", kit::COLLECTION)])
}

/// Serve the hosted service on `node`, taken out of the voters. The caller
/// shuts the server down, then returns the host with `insert`.
pub(crate) fn take_host(voters: &mut Voters, node: u64) -> Arc<RaftHost> {
    Arc::new(voters.take(node))
}

pub(crate) fn return_host(voters: &mut Voters, node: u64, host: Arc<RaftHost>) {
    let host = Arc::try_unwrap(host)
        .ok()
        .expect("no task still holds the host");
    voters.insert(node, host);
}

pub(crate) fn managed_entry(collection: &str, dir: &kit::TestDir) -> RaftManagedCatalog {
    RaftManagedCatalog {
        collection: collection.into(),
        path: dir.path().join("catalog.redb"),
    }
}

/// Timing under which a write can be parked inside its transaction for a
/// few hundred milliseconds without the lease lapsing: the default
/// production values on the kit's snapshot and chunk settings.
pub(crate) fn slow_lease_config() -> pipestream_search::raft::HostConfig {
    pipestream_search::raft::HostConfig {
        heartbeat_interval_ms: 250,
        election_timeout_min_ms: 1_000,
        election_timeout_max_ms: 2_000,
        admission_lease_ms: 500,
        clock_skew_ms: 250,
        ..raft_kit::host_config()
    }
}
