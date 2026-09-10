use pipestream_search::{
    authorization::{AccessPermit, AuthorizationGuard, Authorizer, PolicyAuthority},
    document_catalog::AccessControlledCatalog,
    document_write_service::DocumentWriteServiceImpl,
    pb::{
        accept_document_request::Mutation,
        document_write_service_client::DocumentWriteServiceClient,
        document_write_service_server::DocumentWriteService,
        storage::{SourceRecord, SourceResourceBinding},
        AcceptDocumentRequest, AccessAction, AccessDecision, AccessPolicy, CollectionGrant,
        CollectionResource, DocumentWriteRequest, DocumentWriteServiceLimits,
        GetDocumentWriteTargetRequest, ProtobufSource,
    },
    security::{PrincipalConfig, Principals},
};
use prost::Message;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Status};

const COLLECTION: &str = "books";
const WORKSPACE: &str = "workspace-a";

struct Directory(PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "document-write-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn catalog(&self) -> PathBuf {
        self.0.join("source.redb")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn binding() -> SourceResourceBinding {
    SourceResourceBinding {
        format_version: 1,
        workspace: WORKSPACE.into(),
        collection: COLLECTION.into(),
    }
}

fn policy(revision: u64, writers: &[&str]) -> AccessPolicy {
    let mut grants = writers
        .iter()
        .map(|principal| CollectionGrant {
            principal: (*principal).into(),
            workspace: WORKSPACE.into(),
            collection: COLLECTION.into(),
            actions: vec![AccessAction::Ingest as i32],
            ..Default::default()
        })
        .collect::<Vec<_>>();
    grants.extend([
        CollectionGrant {
            principal: "reader".into(),
            workspace: WORKSPACE.into(),
            collection: COLLECTION.into(),
            actions: vec![AccessAction::Search as i32],
            ..Default::default()
        },
        CollectionGrant {
            principal: "administrator".into(),
            workspace: WORKSPACE.into(),
            collection: COLLECTION.into(),
            actions: vec![AccessAction::Admin as i32],
            ..Default::default()
        },
    ]);
    AccessPolicy {
        format_version: 1,
        revision,
        resources: vec![CollectionResource {
            workspace: WORKSPACE.into(),
            collection: COLLECTION.into(),
        }],
        grants,
    }
}

fn principals(authority: Arc<dyn Authorizer>) -> Principals {
    Principals::from_configs(
        &["writer-a", "writer-b", "reader", "administrator"].map(|name| PrincipalConfig {
            name: name.into(),
            token: format!("{name}-token-0123456789"),
            concurrency: 4,
            ..Default::default()
        }),
    )
    .unwrap()
    .with_authorizer(authority)
}

fn auth<T>(message: T, principal: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {principal}-token-0123456789")
            .parse()
            .unwrap(),
    );
    request
}

fn write(history_id: Vec<u8>, operation_id: &[u8], expected_version: u64) -> DocumentWriteRequest {
    DocumentWriteRequest {
        collection: COLLECTION.into(),
        document: Some(AcceptDocumentRequest {
            contract_version: 2,
            history_id,
            document_key: b"source\0one".to_vec(),
            operation_id: operation_id.to_vec(),
            expected_version: Some(expected_version),
            mutation: Some(Mutation::Delete(true)),
        }),
    }
}

fn source() -> ProtobufSource {
    let mut payload = vec![8, 7];
    payload.extend_from_slice(&[0xa0, 6, 0x81, 0]);
    ProtobufSource {
        descriptor_set: include_bytes!("fixtures/protobuf-semantics/descriptor.bin").to_vec(),
        message_type: "semantics.Doc".into(),
        payload,
    }
}

fn source_write(
    history_id: Vec<u8>,
    operation_id: &[u8],
    expected_version: u64,
    source: ProtobufSource,
) -> DocumentWriteRequest {
    let mut request = write(history_id, operation_id, expected_version);
    request.document.as_mut().unwrap().mutation = Some(Mutation::Source(source));
    request
}

fn limits(max_request_bytes: u64, max_in_flight: u32) -> DocumentWriteServiceLimits {
    DocumentWriteServiceLimits {
        max_request_bytes,
        max_pending_bytes: max_request_bytes * u64::from(max_in_flight),
        max_in_flight,
    }
}

fn provision(dir: &Directory, authority: Arc<dyn Authorizer>) -> Arc<AccessControlledCatalog> {
    let admin =
        AccessPermit::acquire(authority, "administrator", COLLECTION, AccessAction::Admin).unwrap();
    Arc::new(AccessControlledCatalog::create(&dir.catalog(), &binding(), &admin).unwrap())
}

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl TestServer {
    async fn shutdown(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        match tokio::time::timeout(Duration::from_secs(5), &mut self.task).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                panic!("document-write server did not release its connections");
            }
        }
    }
}

async fn serve(
    service: DocumentWriteServiceImpl,
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
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = stopping.await;
            }),
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

#[tokio::test]
async fn discovery_acceptance_actor_retries_and_reopen_keep_one_history() {
    let dir = Directory::new("roundtrip");
    let authority = Arc::new(PolicyAuthority::new(policy(1, &["writer-a", "writer-b"])).unwrap());
    let catalog = provision(&dir, authority.clone());
    let service = DocumentWriteServiceImpl::new(
        principals(authority.clone()),
        vec![catalog.clone()],
        limits(64 * 1024, 4),
    )
    .unwrap();
    let (mut client, server) = serve(service).await;
    let target = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: COLLECTION.into(),
            },
            "writer-a",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (target.workspace.as_str(), target.collection.as_str()),
        (WORKSPACE, COLLECTION)
    );
    assert_eq!(target.history_id.len(), 16);

    let original_source = source();
    let first_request = source_write(
        target.history_id.clone(),
        b"same-operation",
        0,
        original_source.clone(),
    );
    let first = client
        .accept_document(auth(first_request.clone(), "writer-a"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (first.version, first.accepted_sequence, first.replayed),
        (1, 1, false)
    );
    assert!(first.accepted && first.durable && !first.searchable);
    let replay = client
        .accept_document(auth(first_request, "writer-a"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.version, first.version);
    assert_eq!(replay.accepted_sequence, first.accepted_sequence);
    assert!(replay.replayed);

    let other_actor = client
        .accept_document(auth(
            write(target.history_id.clone(), b"same-operation", 1),
            "writer-b",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((other_actor.version, other_actor.accepted_sequence), (2, 2));
    drop(client);
    server.shutdown().await;
    drop(catalog);

    let admin = AccessPermit::acquire(
        authority.clone(),
        "administrator",
        COLLECTION,
        AccessAction::Admin,
    )
    .unwrap();
    let reopened =
        Arc::new(AccessControlledCatalog::open(&dir.catalog(), &binding(), &admin).unwrap());
    let (mut client, reopened_server) = serve(
        DocumentWriteServiceImpl::new(principals(authority), vec![reopened], limits(64 * 1024, 4))
            .unwrap(),
    )
    .await;
    let replay_after_reopen = client
        .accept_document(auth(
            source_write(
                target.history_id.clone(),
                b"same-operation",
                0,
                original_source.clone(),
            ),
            "writer-a",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (
            replay_after_reopen.version,
            replay_after_reopen.accepted_sequence
        ),
        (1, 1)
    );
    assert!(replay_after_reopen.replayed);
    let changed = client
        .accept_document(auth(
            write(target.history_id, b"same-operation", 0),
            "writer-a",
        ))
        .await
        .unwrap_err();
    assert_eq!(changed.code(), Code::AlreadyExists);
    drop(client);
    reopened_server.shutdown().await;

    let database = redb::Database::open(dir.catalog()).unwrap();
    let read = database.begin_read().unwrap();
    let sources = read
        .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("sources"))
        .unwrap();
    assert_eq!(sources.len().unwrap(), 1);
    let (_, stored) = sources.iter().unwrap().next().unwrap().unwrap();
    let stored = SourceRecord::decode(stored.value()).unwrap();
    let descriptors = read
        .open_table(redb::TableDefinition::<&[u8], &[u8]>::new("descriptors"))
        .unwrap();
    let descriptor = descriptors
        .get(stored.descriptor_sha256.as_slice())
        .unwrap()
        .unwrap();
    assert_eq!(
        ProtobufSource {
            descriptor_set: descriptor.value().to_vec(),
            message_type: stored.message_type,
            payload: stored.payload,
        },
        original_source
    );
}

#[tokio::test]
async fn authorization_history_contract_and_transport_limits_refuse_before_write() {
    let dir = Directory::new("refusals");
    let authority = Arc::new(PolicyAuthority::new(policy(1, &["writer-a", "writer-b"])).unwrap());
    let catalog = provision(&dir, authority.clone());
    let service =
        DocumentWriteServiceImpl::new(principals(authority), vec![catalog], limits(1024, 2))
            .unwrap();
    let direct = service.clone();
    let (mut client, server) = serve(service).await;
    let history = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: COLLECTION.into(),
            },
            "writer-a",
        ))
        .await
        .unwrap()
        .into_inner()
        .history_id;

    for principal in ["reader", "administrator"] {
        let error = client
            .get_write_target(auth(
                GetDocumentWriteTargetRequest {
                    collection: COLLECTION.into(),
                },
                principal,
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
        let error = client
            .accept_document(auth(write(history.clone(), b"unauthorized", 0), principal))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
    }
    let anonymous = client
        .get_write_target(Request::new(GetDocumentWriteTargetRequest {
            collection: COLLECTION.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(anonymous.code(), Code::Unauthenticated);

    let mut legacy = write(history.clone(), b"legacy", 0);
    legacy.document.as_mut().unwrap().contract_version = 1;
    legacy.document.as_mut().unwrap().history_id.clear();
    assert_eq!(
        client
            .accept_document(auth(legacy, "writer-a"))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        client
            .accept_document(auth(write(vec![7; 16], b"wrong-history", 0), "writer-a"))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut oversized = write(history.clone(), b"oversized", 0);
    oversized.document.as_mut().unwrap().document_key = vec![7; 4096];
    let transport = client
        .accept_document(auth(oversized.clone(), "writer-a"))
        .await
        .unwrap_err();
    assert_eq!(transport.code(), Code::OutOfRange);
    assert!(
        transport.message().contains("length") && transport.message().contains("limit"),
        "{transport}"
    );
    let handler = DocumentWriteService::accept_document(&direct, auth(oversized, "writer-a"))
        .await
        .err()
        .expect("handler must enforce its own request byte limit");
    assert_eq!(handler.code(), Code::ResourceExhausted);
    assert!(handler.message().contains("max_request_bytes"), "{handler}");
    let accepted = client
        .accept_document(auth(write(history, b"valid", 0), "writer-a"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((accepted.version, accepted.accepted_sequence), (1, 1));
    drop(client);
    server.shutdown().await;
}

#[test]
fn invalid_limits_and_duplicate_collections_are_rejected() {
    let dir = Directory::new("construction");
    let authority = Arc::new(PolicyAuthority::new(policy(1, &["writer-a"])).unwrap());
    let catalog = provision(&dir, authority.clone());
    for invalid in [
        DocumentWriteServiceLimits {
            max_request_bytes: 0,
            max_pending_bytes: 1,
            max_in_flight: 1,
        },
        DocumentWriteServiceLimits {
            max_request_bytes: 1024,
            max_pending_bytes: 512,
            max_in_flight: 1,
        },
        DocumentWriteServiceLimits {
            max_request_bytes: 1024,
            max_pending_bytes: 1024,
            max_in_flight: 0,
        },
    ] {
        let error = DocumentWriteServiceImpl::new(
            principals(authority.clone()),
            vec![catalog.clone()],
            invalid,
        )
        .err()
        .unwrap();
        assert_eq!(error.code(), Code::InvalidArgument);
    }
    let duplicate = DocumentWriteServiceImpl::new(
        principals(authority),
        vec![catalog.clone(), catalog],
        limits(1024, 1),
    )
    .err()
    .unwrap();
    assert_eq!(duplicate.code(), Code::InvalidArgument);
}

#[derive(Debug)]
struct GatedAuthorizer {
    inner: Arc<PolicyAuthority>,
    armed: AtomicBool,
    ready: tokio::sync::mpsc::UnboundedSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    timed_out: AtomicBool,
    gate_on_call: std::sync::atomic::AtomicUsize,
}

struct GatedGuard<'a> {
    inner: Box<dyn AuthorizationGuard + 'a>,
    ready: &'a tokio::sync::mpsc::UnboundedSender<()>,
    release: &'a Mutex<mpsc::Receiver<()>>,
    timed_out: &'a AtomicBool,
    decision_calls: std::sync::atomic::AtomicUsize,
    gate_on_call: usize,
}
impl AuthorizationGuard for GatedGuard<'_> {
    fn decision(&self) -> &AccessDecision {
        // AccessPermit::pin performs call one while validating the provider's
        // returned decision, then performs its second revision check. The
        // controlled catalog performs call two after admission, immediately
        // before entering the source transaction.
        if self.decision_calls.fetch_add(1, Ordering::AcqRel) + 1 == self.gate_on_call {
            self.ready.send(()).unwrap();
            if self
                .release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .is_err()
            {
                self.timed_out.store(true, Ordering::Release);
            }
        }
        self.inner.decision()
    }
}
impl Authorizer for GatedAuthorizer {
    fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        self.inner.authorize(principal, collection, action)
    }
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.subscribe()
    }
    fn pin(&self, expected: &AccessDecision) -> Result<Box<dyn AuthorizationGuard + '_>, Status> {
        let guard = self.inner.pin(expected)?;
        if self.armed.swap(false, Ordering::AcqRel) {
            Ok(Box::new(GatedGuard {
                inner: guard,
                ready: &self.ready,
                release: &self.release,
                timed_out: &self.timed_out,
                decision_calls: std::sync::atomic::AtomicUsize::new(0),
                gate_on_call: self.gate_on_call.load(Ordering::Acquire),
            }))
        } else {
            Ok(guard)
        }
    }
}

struct GateRelease(Option<mpsc::Sender<()>>);
impl GateRelease {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_byte_budget_refuses_independently_of_an_available_slot() {
    let dir = Directory::new("pending-bytes");
    let inner = Arc::new(PolicyAuthority::new(policy(1, &["writer-a", "writer-b"])).unwrap());
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate_release = GateRelease(Some(release_tx));
    let gated = Arc::new(GatedAuthorizer {
        inner,
        armed: AtomicBool::new(false),
        ready: ready_tx,
        release: Mutex::new(release_rx),
        timed_out: AtomicBool::new(false),
        gate_on_call: std::sync::atomic::AtomicUsize::new(2),
    });
    let catalog = provision(&dir, gated.clone());
    let (mut client, server) = serve(
        DocumentWriteServiceImpl::new(
            principals(gated.clone()),
            vec![catalog],
            DocumentWriteServiceLimits {
                max_request_bytes: 1024,
                max_pending_bytes: 1024,
                max_in_flight: 2,
            },
        )
        .unwrap(),
    )
    .await;
    let history = client
        .get_write_target(auth(
            GetDocumentWriteTargetRequest {
                collection: COLLECTION.into(),
            },
            "writer-a",
        ))
        .await
        .unwrap()
        .into_inner()
        .history_id;

    gated.armed.store(true, Ordering::Release);
    let mut held_client = client.clone();
    let mut held = write(history.clone(), b"held", 0);
    held.document.as_mut().unwrap().document_key = vec![b'a'; 700];
    let running =
        tokio::spawn(async move { held_client.accept_document(auth(held, "writer-a")).await });
    tokio::time::timeout(Duration::from_secs(5), ready_rx.recv())
        .await
        .expect("first request did not retain its byte budget in the worker")
        .unwrap();

    let mut competing = write(history, b"competing", 0);
    competing.document.as_mut().unwrap().document_key = vec![b'b'; 700];
    let error = client
        .accept_document(auth(competing, "writer-b"))
        .await
        .unwrap_err();
    gate_release.release();
    running.await.unwrap().unwrap();
    assert!(!gated.timed_out.load(Ordering::Acquire));
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().contains("byte"), "{error}");
    drop(client);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revocation_before_the_second_revision_check_refuses_without_accepting() {
    let dir = Directory::new("pre-admission-revocation");
    let inner = Arc::new(PolicyAuthority::new(policy(1, &["writer-a"])).unwrap());
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate_release = GateRelease(Some(release_tx));
    let gated = Arc::new(GatedAuthorizer {
        inner: inner.clone(),
        armed: AtomicBool::new(false),
        ready: ready_tx,
        release: Mutex::new(release_rx),
        timed_out: AtomicBool::new(false),
        gate_on_call: std::sync::atomic::AtomicUsize::new(1),
    });
    let catalog = provision(&dir, gated.clone());
    let service = DocumentWriteServiceImpl::new(
        principals(gated.clone()),
        vec![catalog],
        limits(64 * 1024, 1),
    )
    .unwrap();
    let history = DocumentWriteService::get_write_target(
        &service,
        auth(
            GetDocumentWriteTargetRequest {
                collection: COLLECTION.into(),
            },
            "writer-a",
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .history_id;

    gated.armed.store(true, Ordering::Release);
    let running_service = service.clone();
    let request = write(history.clone(), b"precheck", 0);
    let handler = tokio::spawn(async move {
        DocumentWriteService::accept_document(&running_service, auth(request, "writer-a")).await
    });
    tokio::time::timeout(Duration::from_secs(5), ready_rx.recv())
        .await
        .expect("provider guard validation did not reach its gate")
        .unwrap();

    let mut revisions = inner.subscribe();
    let (replace_tx, replace_rx) = mpsc::channel();
    let replacing = {
        let inner = inner.clone();
        std::thread::spawn(move || {
            replace_tx.send(inner.replace(policy(2, &[]))).unwrap();
        })
    };
    let published = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;
    gate_release.release();
    let refused = tokio::time::timeout(Duration::from_secs(5), handler)
        .await
        .expect("pre-admission request did not finish after gate release")
        .unwrap()
        .unwrap_err();
    let replacement =
        tokio::task::spawn_blocking(move || replace_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap();
    if replacement.is_ok() {
        replacing.join().unwrap();
    }

    assert!(published);
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert!(!gated.timed_out.load(Ordering::Acquire));
    replacement.unwrap().unwrap();
    inner.replace(policy(3, &["writer-a"])).unwrap();
    let accepted = DocumentWriteService::accept_document(
        &service,
        auth(write(history, b"precheck", 0), "writer-a"),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!((accepted.version, accepted.accepted_sequence), (1, 1));
    assert!(accepted.accepted && accepted.durable && !accepted.searchable);
    assert!(!accepted.replayed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_after_admission_suppresses_receipt_but_exact_retry_replays_commit() {
    let dir = Directory::new("receipt-suppression");
    let inner = Arc::new(PolicyAuthority::new(policy(1, &["writer-a"])).unwrap());
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate_release = GateRelease(Some(release_tx));
    let gated = Arc::new(GatedAuthorizer {
        inner: inner.clone(),
        armed: AtomicBool::new(false),
        ready: ready_tx,
        release: Mutex::new(release_rx),
        timed_out: AtomicBool::new(false),
        gate_on_call: std::sync::atomic::AtomicUsize::new(2),
    });
    let catalog = provision(&dir, gated.clone());
    let service = DocumentWriteServiceImpl::new(
        principals(gated.clone()),
        vec![catalog],
        limits(64 * 1024, 1),
    )
    .unwrap();
    let history = DocumentWriteService::get_write_target(
        &service,
        auth(
            GetDocumentWriteTargetRequest {
                collection: COLLECTION.into(),
            },
            "writer-a",
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .history_id;

    gated.armed.store(true, Ordering::Release);
    let running_service = service.clone();
    let request = write(history.clone(), b"suppressed-receipt", 0);
    let handler = tokio::spawn(async move {
        DocumentWriteService::accept_document(&running_service, auth(request, "writer-a")).await
    });
    tokio::time::timeout(Duration::from_secs(5), ready_rx.recv())
        .await
        .expect("accept did not pass both permit revision checks")
        .unwrap();

    let mut revisions = inner.subscribe();
    let (replace_tx, replace_rx) = mpsc::channel();
    let replacing = {
        let inner = inner.clone();
        std::thread::spawn(move || {
            replace_tx.send(inner.replace(policy(2, &[]))).unwrap();
        })
    };
    let published = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;
    let before_release = replace_rx.try_recv();
    let replacement_was_pending = matches!(&before_release, Err(mpsc::TryRecvError::Empty));

    gate_release.release();
    let response = tokio::time::timeout(Duration::from_secs(5), handler)
        .await
        .expect("admitted handler did not finish after gate release")
        .unwrap();
    let replacement = tokio::task::spawn_blocking(move || match before_release {
        Ok(result) => Ok(result),
        Err(mpsc::TryRecvError::Empty) => replace_rx.recv_timeout(Duration::from_secs(5)),
        Err(mpsc::TryRecvError::Disconnected) => Err(mpsc::RecvTimeoutError::Disconnected),
    })
    .await
    .unwrap();
    if replacement.is_ok() {
        replacing.join().unwrap();
    }

    assert!(
        published,
        "revocation was not published during the admitted commit"
    );
    assert!(replacement_was_pending);
    let suppressed = response.err().expect("revoked receipt was disclosed");
    assert_eq!(suppressed.code(), Code::PermissionDenied);
    assert!(!gated.timed_out.load(Ordering::Acquire));
    replacement.unwrap().unwrap();

    inner.replace(policy(3, &["writer-a"])).unwrap();
    let replay = DocumentWriteService::accept_document(
        &service,
        auth(write(history, b"suppressed-receipt", 0), "writer-a"),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!((replay.version, replay.accepted_sequence), (1, 1));
    assert!(replay.accepted && replay.durable && !replay.searchable);
    assert!(replay.replayed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_handler_keeps_admission_and_capacity_until_commit_then_replays_after_regrant() {
    let dir = Directory::new("cancellation");
    let inner = Arc::new(PolicyAuthority::new(policy(1, &["writer-a", "writer-b"])).unwrap());
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate_release = GateRelease(Some(release_tx));
    let gated = Arc::new(GatedAuthorizer {
        inner: inner.clone(),
        armed: AtomicBool::new(false),
        ready: ready_tx,
        release: Mutex::new(release_rx),
        timed_out: AtomicBool::new(false),
        gate_on_call: std::sync::atomic::AtomicUsize::new(2),
    });
    let catalog = provision(&dir, gated.clone());
    let service = DocumentWriteServiceImpl::new(
        principals(gated.clone()),
        vec![catalog],
        limits(64 * 1024, 1),
    )
    .unwrap();
    let history = DocumentWriteService::get_write_target(
        &service,
        auth(
            GetDocumentWriteTargetRequest {
                collection: COLLECTION.into(),
            },
            "writer-a",
        ),
    )
    .await
    .unwrap()
    .into_inner()
    .history_id;

    gated.armed.store(true, Ordering::Release);
    let running_service = service.clone();
    let first_request = write(history.clone(), b"cancel-retry", 0);
    let handler = tokio::spawn(async move {
        DocumentWriteService::accept_document(&running_service, auth(first_request, "writer-a"))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), ready_rx.recv())
        .await
        .expect("accept did not pass both permit revision checks")
        .unwrap();
    handler.abort();
    let cancelled = handler.await;

    let mut revisions = inner.subscribe();
    let (replace_tx, replace_rx) = mpsc::channel();
    let replacing = {
        let inner = inner.clone();
        std::thread::spawn(move || {
            replace_tx
                .send(inner.replace(policy(2, &["writer-b"])))
                .unwrap();
        })
    };
    let published = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;
    let before_release = replace_rx.try_recv();
    let replacement_was_pending = matches!(&before_release, Err(mpsc::TryRecvError::Empty));

    let capacity = DocumentWriteService::accept_document(
        &service,
        auth(write(history.clone(), b"other", 0), "writer-b"),
    )
    .await
    .unwrap_err();

    gate_release.release();
    let replacement = tokio::task::spawn_blocking(move || match before_release {
        Ok(result) => Ok(result),
        Err(mpsc::TryRecvError::Empty) => replace_rx.recv_timeout(Duration::from_secs(5)),
        Err(mpsc::TryRecvError::Disconnected) => Err(mpsc::RecvTimeoutError::Disconnected),
    })
    .await
    .unwrap();
    if replacement.is_ok() {
        replacing.join().unwrap();
    }

    assert!(cancelled.unwrap_err().is_cancelled());
    assert!(
        published,
        "revocation was not published during the admitted commit"
    );
    assert!(replacement_was_pending);
    assert_eq!(capacity.code(), Code::ResourceExhausted);
    assert!(capacity.message().contains("capacity"), "{capacity}");
    assert!(!gated.timed_out.load(Ordering::Acquire));
    replacement.unwrap().unwrap();

    inner.replace(policy(3, &["writer-a", "writer-b"])).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let recovered = loop {
        match DocumentWriteService::accept_document(
            &service,
            auth(write(history.clone(), b"cancel-retry", 0), "writer-a"),
        )
        .await
        {
            Ok(response) => break response.into_inner(),
            Err(error)
                if error.code() == Code::ResourceExhausted
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("retry after regrant failed: {error}"),
        }
    };
    assert_eq!((recovered.version, recovered.accepted_sequence), (1, 1));
    assert!(recovered.accepted && recovered.durable && !recovered.searchable);
    assert!(recovered.replayed);
}
