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
    for format_version in [0, 2, u32::MAX] {
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
        let authority = Arc::new(PolicyAuthority::new(policy()).unwrap());
        let permit =
            AccessPermit::acquire(authority.clone(), "reader", "a", AccessAction::Search).unwrap();
        let mut stream = AuthorizedStream::new(RevokeOnPoll(authority, error_item), Some(permit));
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            Code::PermissionDenied
        );
        assert!(stream.next().await.is_none());
    }
}
