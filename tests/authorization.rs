mod common;

use pipestream_search::authorization::{
    AccessPermit, AuthorizedStream, Authorizer, PolicyAuthority,
};
use pipestream_search::collections::CollectionSet;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::*;
use pipestream_search::security::{PrincipalConfig, Principals};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

fn policy() -> AccessPolicy {
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: vec![
            CollectionResource {
                workspace: "workspace-a".into(),
                collection: "a".into(),
            },
            CollectionResource {
                workspace: "workspace-b".into(),
                collection: "b".into(),
            },
        ],
        grants: [
            ("reader", AccessAction::Search),
            ("writer", AccessAction::Ingest),
            ("admin", AccessAction::Admin),
        ]
        .into_iter()
        .map(|(principal, action)| CollectionGrant {
            field_permissions: None,
            document_visibility: None,
            principal: principal.into(),
            workspace: "workspace-a".into(),
            collection: "a".into(),
            actions: vec![action as i32],
        })
        .collect(),
    }
}
fn principals() -> Principals {
    Principals::from_configs(&["reader", "writer", "admin"].map(|name| PrincipalConfig {
        name: name.into(),
        token: format!("{name}-token-0123456789012345"),
        ..Default::default()
    }))
    .unwrap()
}
fn request<T>(message: T, principal: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {principal}-token-0123456789012345")
            .parse()
            .unwrap(),
    );
    request
}
fn set(authority: Arc<PolicyAuthority>) -> CollectionSet {
    CollectionSet::named(
        ["a", "b"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    CoordinatorServiceImpl::new(vec![]).with_collection(name),
                )
            })
            .collect(),
        Some("a".into()),
    )
    .unwrap()
    .with_principals(Arc::new(principals().with_authorizer(authority)))
}

#[test]
fn grants_are_exact_and_actions_do_not_imply_each_other() {
    let authority = PolicyAuthority::new(policy()).unwrap();
    for (principal, allowed) in [
        ("reader", AccessAction::Search),
        ("writer", AccessAction::Ingest),
        ("admin", AccessAction::Admin),
    ] {
        for action in [
            AccessAction::Search,
            AccessAction::Ingest,
            AccessAction::Admin,
        ] {
            assert_eq!(
                authority.authorize(principal, "a", action).is_ok(),
                action == allowed
            );
            assert_eq!(
                authority
                    .authorize(principal, "b", action)
                    .unwrap_err()
                    .code(),
                Code::PermissionDenied
            );
        }
    }
    let decision = authority
        .authorize("reader", "a", AccessAction::Search)
        .unwrap();
    assert_eq!(decision.workspace, "workspace-a");
    assert_eq!(decision.policy_revision, 1);
    assert_eq!(
        authority
            .authorize("reader", "a", AccessAction::Unspecified)
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[test]
fn invalid_policy_cannot_replace_the_current_revision() {
    let authority = PolicyAuthority::new(policy()).unwrap();
    for format_version in [0, 4, u32::MAX] {
        let mut unsupported = policy();
        unsupported.format_version = format_version;
        assert!(PolicyAuthority::new(unsupported).is_err());
    }
    let mut wrong_workspace = policy();
    wrong_workspace.revision = 2;
    wrong_workspace.grants[0].workspace = "workspace-b".into();
    assert!(authority.replace(wrong_workspace).is_err());
    let mut unknown_action = policy();
    unknown_action.revision = 2;
    unknown_action.grants[0].actions = vec![99];
    assert!(authority.replace(unknown_action).is_err());
    let mut duplicate = policy();
    duplicate.revision = 2;
    duplicate.resources.push(duplicate.resources[0].clone());
    assert!(authority.replace(duplicate).is_err());
    assert!(authority.replace(policy()).is_err());
    assert_eq!(
        authority
            .authorize("reader", "a", AccessAction::Search)
            .unwrap()
            .policy_revision,
        1
    );
    let mut revoked = policy();
    revoked.revision = 2;
    revoked.grants.clear();
    authority.replace(revoked).unwrap();
    assert!(authority
        .authorize("reader", "a", AccessAction::Search)
        .is_err());
    assert!(authority.replace(policy()).is_err());
}

#[tokio::test]
async fn each_public_unary_route_enforces_its_declared_action() {
    let set = set(Arc::new(PolicyAuthority::new(policy()).unwrap()));
    macro_rules! refuses {
        ($principal:expr; $($method:ident : $request:ident),+ $(,)?) => { $(
            let error = SearchService::$method(&set, request($request { collection: "a".into(), ..Default::default() }, $principal)).await.err().unwrap();
            assert_eq!(error.code(), Code::PermissionDenied, "{}: {}", stringify!($method), error);
        )+ };
    }
    for principal in ["writer", "admin"] {
        refuses!(principal; search: SearchRequest, bm25_search: Bm25SearchRequest,
            phrase_search: PhraseSearchRequest, hybrid_search: HybridSearchRequest,
            variant_search: VariantSearchRequest, query: QueryRequest, aggregate: AggregateRequest, suggest: SuggestRequest, term_suggest: TermSuggestRequest);
        let error = SearchService::query_stream(
            &set,
            request(
                QueryStreamRequest {
                    collection: "a".into(),
                    ..Default::default()
                },
                principal,
            ),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.code(), Code::PermissionDenied);
    }
    for principal in ["reader", "writer"] {
        refuses!(principal; broadcast_vector_backend: BroadcastVectorBackendRequest,
            broadcast_calibration: BroadcastCalibrationRequest, plan_index: PlanIndexRequest,
            describe_schema: DescribeSchemaRequest, plan_placement: PlanPlacementRequest,
            freeze_topology_writes: FreezeTopologyWritesRequest, publish_topology: PublishTopologyRequest,
            abort_topology_cutover: AbortTopologyCutoverRequest, cluster_health: ClusterHealthRequest);
    }
    // An authorized admin reaches descriptor validation, but cannot bypass it.
    let error = SearchService::plan_index(
        &set,
        request(
            PlanIndexRequest {
                collection: "a".into(),
                ..Default::default()
            },
            "admin",
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn schema_description_requires_admin_in_the_resolved_workspace() {
    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let set = set(authority.clone());
    let input = DescribeSchemaRequest {
        descriptor_set: include_bytes!("fixtures/schema-report/source-only.bin").to_vec(),
        message_type: "source_report.Empty".into(),
        collection: String::new(),
    };
    let response = SearchService::describe_schema(&set, request(input.clone(), "admin"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.report.unwrap().root_message, "source_report.Empty");
    let mut other_workspace = input.clone();
    other_workspace.collection = "b".into();
    let error = SearchService::describe_schema(&set, request(other_workspace, "admin"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    let mut revoked = policy();
    revoked.revision = 2;
    revoked.grants.clear();
    authority.replace(revoked).unwrap();
    let error = SearchService::describe_schema(&set, request(input, "admin"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn default_resolution_and_health_listing_do_not_leak_other_workspaces() {
    let set = set(Arc::new(PolicyAuthority::new(policy()).unwrap()));
    // Empty resolves to a before the capability check.
    let health =
        SearchService::cluster_health(&set, request(ClusterHealthRequest::default(), "admin"))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(
        health
            .collections
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    let denied = SearchService::cluster_health(
        &set,
        request(
            ClusterHealthRequest {
                collection: "b".into(),
            },
            "admin",
        ),
    )
    .await
    .unwrap_err();
    let unknown = SearchService::cluster_health(
        &set,
        request(
            ClusterHealthRequest {
                collection: "not-present".into(),
            },
            "admin",
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(denied.message(), unknown.message());
    let error = SearchService::plan_index(&set, request(PlanIndexRequest::default(), "admin"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument); // a's admin is admitted
}

#[tokio::test]
async fn authentication_without_a_policy_does_not_grant_access() {
    let set = CollectionSet::single(CoordinatorServiceImpl::new(vec![]))
        .with_principals(Arc::new(principals()));
    let error =
        SearchService::cluster_health(&set, request(ClusterHealthRequest::default(), "admin"))
            .await
            .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn revision_change_wakes_and_drops_a_pending_stream() {
    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let permit =
        AccessPermit::acquire(authority.clone(), "reader", "a", AccessAction::Search).unwrap();
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<(), tonic::Status>>(1);
    let mut stream = AuthorizedStream::new(
        tokio_stream::wrappers::ReceiverStream::new(receiver),
        Some(permit),
    );
    let (started, waiting) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        // Register the revision and producer wakers before changing the policy.
        std::future::poll_fn(|cx| {
            use tokio_stream::Stream;
            assert!(std::pin::Pin::new(&mut stream).poll_next(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        started.send(()).unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
        assert!(stream.next().await.is_none());
    });
    waiting.await.unwrap();
    let mut next = policy();
    next.revision = 2;
    next.grants.clear();
    authority.replace(next).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(sender.is_closed());
}

#[tokio::test]
async fn routed_ingest_checks_grants_before_descriptor_work() {
    use pipestream_search::pb::search_service_client::SearchServiceClient;
    use tokio_stream::wrappers::TcpListenerStream;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let set = set(Arc::new(PolicyAuthority::new(policy()).unwrap()));
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(set.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let mut client = SearchServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    for principal in ["reader", "admin", "writer"] {
        let bind = RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Bind(
                RoutedMappedBind {
                    collection: "a".into(),
                    ..Default::default()
                },
            )),
        };
        let error = client
            .routed_ingest_mapped(request(tokio_stream::iter([bind]), principal))
            .await
            .unwrap_err();
        if principal == "writer" {
            assert_ne!(error.code(), Code::PermissionDenied);
        } else {
            assert_eq!(error.code(), Code::PermissionDenied);
        }
    }
    server.abort();
}

#[tokio::test]
async fn cached_search_is_not_reachable_after_revocation() {
    use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
    use pipestream_search::node::NodeConfig;
    use pipestream_search::pb::node_service_client::NodeServiceClient;
    let (address, handle) = common::start_empty_node(NodeConfig {
        collection: "a".into(),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    })
    .await;
    let mut node = NodeServiceClient::connect(address.clone()).await.unwrap();
    node.add_documents(tokio_stream::iter([AddDocumentsRequest {
        text: "confidential evidence".into(),
        analysis: Some(body_spec()),
        collection: "a".into(),
        ..Default::default()
    }]))
    .await
    .unwrap();
    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let coordinator = CoordinatorServiceImpl::new(vec![address])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
        .with_collection("a");
    let set = CollectionSet::named(vec![("a".into(), coordinator)], Some("a".into()))
        .unwrap()
        .with_principals(Arc::new(principals().with_authorizer(authority.clone())));
    let query = Bm25SearchRequest {
        text: "evidence".into(),
        analysis: Some(body_spec()),
        collection: "a".into(),
        k: 10,
        ..Default::default()
    };
    for _ in 0..2 {
        assert_eq!(
            SearchService::bm25_search(&set, request(query.clone(), "reader"))
                .await
                .unwrap()
                .into_inner()
                .hits
                .len(),
            1
        );
    }
    let mut next = policy();
    next.revision = 2;
    next.grants.clear();
    authority.replace(next).unwrap();
    let denied = SearchService::bm25_search(&set, request(query, "reader"))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);
    handle.abort();
}

#[test]
fn policy_configuration_is_explicit_and_rejects_typos() {
    let path =
        std::env::temp_dir().join(format!("psearch-access-config-{}.toml", std::process::id()));
    let credentials =
        "[[principals]]\nname = \"reader\"\ntoken = \"reader-test-token-0123456789\"\n";
    std::fs::write(&path, credentials).unwrap();
    assert!(Principals::load(&path)
        .unwrap_err()
        .contains("explicit [policy]"));
    std::fs::write(&path, format!("{credentials}\n[policy]\nformat_version = 1\nrevision = 1\n[[policy.resources]]\nworkspace = \"workspace-a\"\ncollection = \"a\"\n[[policy.grants]]\nprincipal = \"reader\"\nworkspace = \"workspace-a\"\ncollection = \"a\"\nactions = [\"serach\"]\n")).unwrap();
    assert!(Principals::load(&path)
        .unwrap_err()
        .contains("unknown access action"));
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("serach", "search");
    std::fs::write(&path, &text).unwrap();
    let principals = Principals::load(&path).unwrap();
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert(
        "authorization",
        "Bearer reader-test-token-0123456789".parse().unwrap(),
    );
    let principal = principals.authenticate(&metadata).unwrap();
    assert!(principals
        .authorize(&principal, "a", AccessAction::Search)
        .is_ok());
    assert!(principals
        .authorize(&principal, "a", AccessAction::Admin)
        .is_err());
    std::fs::write(&path, text.replace("actions =", "actons =")).unwrap();
    assert!(Principals::load(&path)
        .unwrap_err()
        .contains("unknown field"));
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn replacement_during_producer_poll_cannot_disclose_an_item() {
    struct RevokeOnPoll(Arc<PolicyAuthority>, bool);
    impl tokio_stream::Stream for RevokeOnPoll {
        type Item = Result<&'static str, tonic::Status>;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let mut next = policy();
            next.revision = 2;
            next.grants.clear();
            self.0.replace(next).unwrap();
            std::task::Poll::Ready(Some(if self.1 {
                Err(tonic::Status::invalid_argument("private schema details"))
            } else {
                Ok("private result")
            }))
        }
    }
    for error_item in [false, true] {
        let unchanged = Arc::new(PolicyAuthority::new(policy()).unwrap());
        let unchanged_permit =
            AccessPermit::acquire(unchanged, "reader", "a", AccessAction::Search).unwrap();
        let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
        let permit =
            AccessPermit::acquire(authority.clone(), "reader", "a", AccessAction::Search).unwrap();
        let mut stream = AuthorizedStream::with_permits(
            RevokeOnPoll(authority, error_item),
            vec![unchanged_permit, permit],
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::PermissionDenied
        );
        assert!(stream.next().await.is_none());
    }
}

#[tokio::test]
async fn each_authority_can_wake_a_stream_with_multiple_resource_permits() {
    for revoked in 0..2 {
        let authorities: Vec<_> = (0..2)
            .map(|_| Arc::new(PolicyAuthority::new(policy()).unwrap()))
            .collect();
        let permits = authorities
            .iter()
            .map(|authority| {
                AccessPermit::acquire(authority.clone(), "reader", "a", AccessAction::Search)
                    .unwrap()
            })
            .collect();
        let (sender, receiver) = tokio::sync::mpsc::channel::<Result<(), tonic::Status>>(1);
        let mut stream = AuthorizedStream::with_permits(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
            permits,
        );
        let (started, waiting) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            std::future::poll_fn(|cx| {
                use tokio_stream::Stream;
                assert!(std::pin::Pin::new(&mut stream).poll_next(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            started.send(()).unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap_err().code(),
                Code::PermissionDenied
            );
            assert!(stream.next().await.is_none());
        });
        waiting.await.unwrap();
        let mut next = policy();
        next.revision = 2;
        next.grants.clear();
        authorities[revoked].replace(next).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("every authority must register a revocation waker")
            .unwrap();
        assert!(sender.is_closed());
    }
}

fn policy_revision(revision: u64) -> AccessPolicy {
    let mut next = policy();
    next.revision = revision;
    next
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_publishes_new_admission_before_old_pins_drain() {
    use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};

    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let stale =
        AccessPermit::acquire(authority.clone(), "reader", "a", AccessAction::Search).unwrap();
    let (pinned_tx, pinned_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let admitted = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            let permit =
                AccessPermit::acquire(authority, "reader", "a", AccessAction::Search).unwrap();
            let pin = permit.pin().unwrap();
            pinned_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(pin);
        })
    };
    pinned_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut revisions = authority.subscribe();
    let (replace_tx, replace_rx) = mpsc::channel();
    let replacing = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            replace_tx
                .send(authority.replace(policy_revision(2)))
                .unwrap();
        })
    };
    let published = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;

    let (authorize_tx, authorize_rx) = mpsc::channel();
    let authorizing = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            authorize_tx
                .send(authority.authorize("reader", "a", AccessAction::Search))
                .unwrap();
        })
    };
    let current = authorize_rx.recv_timeout(Duration::from_secs(5));
    let (stale_tx, stale_rx) = mpsc::channel();
    let stale_check = std::thread::spawn(move || {
        stale_tx
            .send(stale.pin().err().map(|error| error.code()))
            .unwrap();
    });
    let stale_pin_code = stale_rx.recv_timeout(Duration::from_secs(5));
    let before_release = replace_rx.try_recv();
    let replacement_was_pending = matches!(&before_release, Err(TryRecvError::Empty));

    release_tx.send(()).unwrap();
    admitted.join().unwrap();
    let replacement = match before_release {
        Ok(result) => Ok(result),
        Err(TryRecvError::Empty) => replace_rx.recv_timeout(Duration::from_secs(5)),
        Err(TryRecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
    };
    if replacement.is_ok() {
        replacing.join().unwrap();
    }
    if current.is_ok() {
        authorizing.join().unwrap();
    }
    if stale_pin_code.is_ok() {
        stale_check.join().unwrap();
    }

    assert!(
        published,
        "revision 2 was not published while the old operation was admitted"
    );
    assert_eq!(current.unwrap().unwrap().policy_revision, 2);
    assert_eq!(stale_pin_code.unwrap(), Some(Code::PermissionDenied));
    assert!(replacement_was_pending);
    replacement.unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_replacements_drain_their_own_epochs_in_order() {
    use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};

    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let (old_ready_tx, old_ready_rx) = mpsc::channel();
    let (old_release_tx, old_release_rx) = mpsc::channel();
    let old_operation = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            let permit =
                AccessPermit::acquire(authority, "reader", "a", AccessAction::Search).unwrap();
            let pin = permit.pin().unwrap();
            old_ready_tx.send(()).unwrap();
            old_release_rx.recv().unwrap();
            drop(pin);
        })
    };
    old_ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut revisions = authority.subscribe();
    let (second_tx, second_rx) = mpsc::channel();
    let second = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            second_tx
                .send(authority.replace(policy_revision(2)))
                .unwrap();
        })
    };
    let published_second = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;

    let mut new_operation = None;
    let mut new_release_tx = None;
    let mut new_ready = None;
    let mut third = None;
    let mut third_rx = None;
    let mut third_started = None;
    if published_second {
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = {
            let authority = authority.clone();
            std::thread::spawn(move || {
                let permit =
                    AccessPermit::acquire(authority, "reader", "a", AccessAction::Search).unwrap();
                let pin = permit.pin().unwrap();
                ready_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                drop(pin);
            })
        };
        new_ready = Some(ready_rx.recv_timeout(Duration::from_secs(5)));
        new_release_tx = Some(release_tx);
        new_operation = Some(worker);

        let (started_tx, started_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let worker = {
            let authority = authority.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                result_tx
                    .send(authority.replace(policy_revision(3)))
                    .unwrap();
            })
        };
        third_started = Some(started_rx.recv_timeout(Duration::from_secs(5)));
        third_rx = Some(result_rx);
        third = Some(worker);
    }
    let second_before_release = second_rx.try_recv();
    let second_was_pending = matches!(&second_before_release, Err(TryRecvError::Empty));
    let third_before_old_release = third_rx.as_ref().map(mpsc::Receiver::try_recv);
    let third_was_pending_during_old_drain =
        matches!(&third_before_old_release, Some(Err(TryRecvError::Empty)));
    let revision_before_old_release = *revisions.borrow();

    old_release_tx.send(()).unwrap();
    old_operation.join().unwrap();
    let second_result = match second_before_release {
        Ok(result) => Ok(result),
        Err(TryRecvError::Empty) => second_rx.recv_timeout(Duration::from_secs(5)),
        Err(TryRecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
    };
    if second_result.is_ok() {
        second.join().unwrap();
    }

    let published_third = if published_second {
        tokio::time::timeout(Duration::from_secs(5), revisions.changed())
            .await
            .is_ok()
            && *revisions.borrow() == 3
    } else {
        false
    };
    let third_before_new_release = match third_before_old_release {
        Some(Err(TryRecvError::Empty)) => third_rx.as_ref().map(mpsc::Receiver::try_recv),
        earlier => earlier,
    };
    let third_waited_for_new_epoch =
        matches!(&third_before_new_release, Some(Err(TryRecvError::Empty)));

    if let Some(release) = new_release_tx.take() {
        release.send(()).unwrap();
        new_operation.take().unwrap().join().unwrap();
    }
    let third_result = match (third_before_new_release, third_rx) {
        (Some(Ok(result)), _) => Some(Ok(result)),
        (Some(Err(TryRecvError::Empty)), Some(rx)) => Some(rx.recv_timeout(Duration::from_secs(5))),
        (Some(Err(TryRecvError::Disconnected)), _) => Some(Err(RecvTimeoutError::Disconnected)),
        (None, _) => None,
        _ => unreachable!(),
    };
    if third_result.as_ref().is_some_and(Result::is_ok) {
        third.take().unwrap().join().unwrap();
    }

    assert!(
        published_second,
        "revision 2 was not published during its old drain"
    );
    assert!(
        second_was_pending,
        "replacement 2 completed before revision 1 released"
    );
    assert_eq!(revision_before_old_release, 2);
    third_started.unwrap().unwrap();
    assert!(third_was_pending_during_old_drain);
    new_ready.unwrap().unwrap();
    second_result.unwrap().unwrap();
    assert!(
        published_third,
        "revision 3 did not publish after replacement 2 completed"
    );
    assert!(third_waited_for_new_epoch);
    third_result.unwrap().unwrap().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unwinding_an_admitted_operation_releases_the_replacement_drain() {
    use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};

    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let (ready_tx, ready_rx) = mpsc::channel();
    let (unwind_tx, unwind_rx) = mpsc::channel();
    let admitted = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            let permit =
                AccessPermit::acquire(authority, "reader", "a", AccessAction::Search).unwrap();
            let _pin = permit.pin().unwrap();
            ready_tx.send(()).unwrap();
            unwind_rx.recv().unwrap();
            panic!("simulated admitted-operation failure");
        })
    };
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut revisions = authority.subscribe();
    let (replace_tx, replace_rx) = mpsc::channel();
    let replacing = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            replace_tx
                .send(authority.replace(policy_revision(2)))
                .unwrap();
        })
    };
    let published = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;
    let before_unwind = replace_rx.try_recv();
    let replacement_was_pending = matches!(&before_unwind, Err(TryRecvError::Empty));

    unwind_tx.send(()).unwrap();
    assert!(admitted.join().is_err());
    let replacement = match before_unwind {
        Ok(result) => Ok(result),
        Err(TryRecvError::Empty) => replace_rx.recv_timeout(Duration::from_secs(5)),
        Err(TryRecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
    };
    if replacement.is_ok() {
        replacing.join().unwrap();
    }

    assert!(
        published,
        "replacement was not published while the operation was admitted"
    );
    assert!(replacement_was_pending);
    replacement.unwrap().unwrap();
    assert_eq!(*revisions.borrow(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn revoked_grant_closes_new_admission_while_its_old_operation_drains() {
    use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};

    let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
    let (ready_tx, ready_rx) = mpsc::channel();
    let (inspect_tx, inspect_rx) = mpsc::channel();
    let (decision_tx, decision_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let admitted = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            let permit =
                AccessPermit::acquire(authority, "reader", "a", AccessAction::Search).unwrap();
            let pin = permit.pin().unwrap();
            ready_tx.send(pin.decision().clone()).unwrap();
            inspect_rx.recv().unwrap();
            decision_tx.send(pin.decision().clone()).unwrap();
            release_rx.recv().unwrap();
            drop(pin);
        })
    };
    let original = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reader operation did not acquire its admission");

    let mut revoked = policy_revision(2);
    revoked.grants.retain(|grant| grant.principal != "reader");
    let mut revisions = authority.subscribe();
    let (replace_tx, replace_rx) = mpsc::channel();
    let replacing = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            replace_tx.send(authority.replace(revoked)).unwrap();
        })
    };
    let published = tokio::time::timeout(Duration::from_secs(5), revisions.changed())
        .await
        .is_ok()
        && *revisions.borrow() == 2;

    let (reader_tx, reader_rx) = mpsc::channel();
    let reader_check = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            reader_tx
                .send(authority.authorize("reader", "a", AccessAction::Search))
                .unwrap();
        })
    };
    let fresh_reader = reader_rx.recv_timeout(Duration::from_secs(5));
    let (writer_tx, writer_rx) = mpsc::channel();
    let writer_check = {
        let authority = authority.clone();
        std::thread::spawn(move || {
            writer_tx
                .send(authority.authorize("writer", "a", AccessAction::Ingest))
                .unwrap();
        })
    };
    let fresh_writer = writer_rx.recv_timeout(Duration::from_secs(5));
    inspect_tx.send(()).unwrap();
    let retained = decision_rx.recv_timeout(Duration::from_secs(5));
    let before_release = replace_rx.try_recv();
    let replacement_was_pending = matches!(&before_release, Err(TryRecvError::Empty));

    release_tx.send(()).unwrap();
    admitted.join().unwrap();
    let replacement = match before_release {
        Ok(result) => Ok(result),
        Err(TryRecvError::Empty) => replace_rx.recv_timeout(Duration::from_secs(5)),
        Err(TryRecvError::Disconnected) => Err(RecvTimeoutError::Disconnected),
    };
    if replacement.is_ok() {
        replacing.join().unwrap();
    }
    if fresh_reader.is_ok() {
        reader_check.join().unwrap();
    }
    if fresh_writer.is_ok() {
        writer_check.join().unwrap();
    }

    assert!(
        published,
        "revocation was not published while the old operation was admitted"
    );
    let denied = fresh_reader
        .unwrap()
        .expect_err("revoked reader gained new admission");
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(fresh_writer.unwrap().unwrap().policy_revision, 2);
    assert_eq!(retained.unwrap(), original);
    assert!(replacement_was_pending);
    replacement.unwrap().unwrap();
}
