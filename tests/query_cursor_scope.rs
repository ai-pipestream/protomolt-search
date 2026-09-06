use pipestream_search::{
    analyzer::body_spec,
    authorization::PolicyAuthority,
    collections::CollectionSet,
    coordinator::CoordinatorServiceImpl,
    harness::start_empty_node,
    node::NodeConfig,
    pb::{node_service_client::NodeServiceClient, search_service_server::SearchService, *},
    security::{PrincipalConfig, Principals},
};
use std::sync::Arc;
use tonic::{Code, Request};

fn policy(revision: u64) -> AccessPolicy {
    AccessPolicy {
        revision,
        format_version: 1,
        resources: vec![CollectionResource {
            workspace: "work".into(),
            collection: "docs".into(),
        }],
        grants: ["alice", "bob"]
            .into_iter()
            .map(|name| CollectionGrant {
                principal: name.into(),
                workspace: "work".into(),
                collection: "docs".into(),
                actions: vec![AccessAction::Search as i32],
            })
            .collect(),
    }
}
fn request<T>(body: T, who: &str) -> Request<T> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {who}-01234567890123456789")
            .parse()
            .unwrap(),
    );
    request
}
fn query() -> QueryRequest {
    QueryRequest {
        k: 1,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "text".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "word".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}
struct Fixture {
    set: CollectionSet,
    authority: Arc<PolicyAuthority>,
    coordinator: CoordinatorServiceImpl,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    async fn new() -> Self {
        let (addr, task) = start_empty_node(NodeConfig {
            collection: "docs".into(),
            analysis_addr: Some("native".into()),
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .add_documents(tokio_stream::iter((0..4).map(|_| AddDocumentsRequest {
                collection: "docs".into(),
                text: "word".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })))
            .await
            .unwrap();
        let coordinator = CoordinatorServiceImpl::new(vec![addr])
            .with_collection("docs")
            .with_bm25(Some("native".into()), Default::default());
        let authority = Arc::new(PolicyAuthority::new(policy(1)).unwrap());
        let set = Self::set(&coordinator, authority.clone());
        Self {
            set,
            authority,
            coordinator,
            task,
        }
    }
    fn set(coordinator: &CoordinatorServiceImpl, authority: Arc<PolicyAuthority>) -> CollectionSet {
        let principals = Principals::from_configs(&["alice", "bob"].map(|name| PrincipalConfig {
            name: name.into(),
            token: format!("{name}-01234567890123456789"),
            ..Default::default()
        }))
        .unwrap()
        .with_authorizer(authority);
        CollectionSet::named(
            vec![("docs".into(), coordinator.clone())],
            Some("docs".into()),
        )
        .unwrap()
        .with_principals(Arc::new(principals))
    }
    async fn resume(&self) -> QueryRequest {
        let first = self
            .set
            .query(request(query(), "alice"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.hits[0].doc_id, 0);
        assert!(!first.next_cursor.is_empty());
        QueryRequest {
            cursor: first.next_cursor,
            ..query()
        }
    }
}

#[tokio::test]
async fn another_principal_cannot_resume_the_same_boundary() {
    let fixture = Fixture::new().await;
    let resume = fixture.resume().await;
    let same = fixture
        .set
        .query(request(resume.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(same.hits[0].doc_id, 1);
    let error = fixture
        .set
        .query(request(resume, "bob"))
        .await
        .expect_err("a cursor must bind its principal");
    assert_eq!(error.code(), Code::FailedPrecondition);
}
#[tokio::test]
async fn a_new_policy_revision_cannot_resume_an_old_cursor() {
    let fixture = Fixture::new().await;
    let resume = fixture.resume().await;
    fixture.authority.replace(policy(2)).unwrap();
    let error = fixture
        .set
        .query(request(resume, "alice"))
        .await
        .expect_err("a cursor must bind the policy revision");
    assert_eq!(error.code(), Code::FailedPrecondition);
}
#[tokio::test]
async fn a_different_query_cannot_reuse_an_unchanged_boundary_score() {
    let fixture = Fixture::new().await;
    let mut resume = fixture.resume().await;
    if let Some(selection_query::Node::Search(search)) =
        resume.selection.as_mut().unwrap().node.as_mut()
    {
        if let Some(search_query::Query::Lexical(lexical)) = search.query.as_mut() {
            lexical.text = "word word".into();
        }
    }
    let error = fixture
        .set
        .query(request(resume, "alice"))
        .await
        .expect_err("query identity must not be inferred from a matching boundary score");
    assert_eq!(error.code(), Code::FailedPrecondition);
}
#[tokio::test]
async fn topology_generation_is_part_of_cursor_identity() {
    let fixture = Fixture::new().await;
    let resume = fixture.resume().await;
    let moved = Fixture::set(
        &fixture.coordinator.clone().with_topology_generation(2),
        fixture.authority.clone(),
    );
    let error = moved
        .query(request(resume, "alice"))
        .await
        .expect_err("a cursor must bind topology generation");
    assert_eq!(error.code(), Code::FailedPrecondition);
}

async fn streamed(
    set: &CollectionSet,
    body: QueryRequest,
    who: &str,
) -> Result<QueryResponse, tonic::Status> {
    use tokio_stream::StreamExt;
    let mut events = set
        .query_stream(request(
            QueryStreamRequest {
                query: Some(body),
                ..Default::default()
            },
            who,
        ))
        .await?
        .into_inner();
    while let Some(event) = events.next().await {
        if let Some(query_stream_response::Payload::Completion(completion)) = event?.payload {
            if !completion.completed {
                return Err(tonic::Status::new(
                    Code::from_i32(completion.error_code as i32),
                    completion.error_message,
                ));
            }
            return Ok(completion.response.expect("completed query has a response"));
        }
    }
    panic!("stream ended without a completion");
}

#[tokio::test]
async fn unary_and_streaming_pages_share_the_same_authorization_binding() {
    let fixture = Fixture::new().await;
    let resume = fixture.resume().await;
    let error = fixture
        .set
        .query_stream(request(
            QueryStreamRequest {
                query: Some(resume.clone()),
                ..Default::default()
            },
            "bob",
        ))
        .await
        .err()
        .expect("reject before opening a stream");
    assert_eq!(error.code(), Code::FailedPrecondition);
    let second = streamed(&fixture.set, resume, "alice").await.unwrap();
    assert_eq!(second.hits[0].doc_id, 1);
    assert!(second.next_cursor.starts_with("pqc1:"));
    let third = fixture
        .set
        .query(request(
            QueryRequest {
                cursor: second.next_cursor,
                ..query()
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(third.hits[0].doc_id, 2);

    let first = streamed(&fixture.set, query(), "alice").await.unwrap();
    let resumed = QueryRequest {
        cursor: first.next_cursor,
        ..query()
    };
    assert_eq!(
        fixture
            .set
            .query(request(resumed.clone(), "bob"))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    fixture.authority.replace(policy(2)).unwrap();
    assert_eq!(
        fixture
            .set
            .query_stream(request(
                QueryStreamRequest {
                    query: Some(resumed),
                    ..Default::default()
                },
                "alice"
            ))
            .await
            .err()
            .unwrap()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn altered_paging_and_sort_requests_are_refused_before_fanout() {
    let fixture = Fixture::new().await;
    let resume = fixture.resume().await;
    fixture.task.abort();
    for change in 0..3 {
        let mut changed = resume.clone();
        match change {
            0 => changed.k = 2,
            1 => changed.selection_k = 2,
            2 => changed.sort.push(QuerySort {
                column: "unknown_column".into(),
                descending: true,
            }),
            _ => unreachable!(),
        }
        let error = fixture
            .set
            .query(request(changed, "alice"))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
        assert!(error.message().contains("context changed"));
    }
}

#[tokio::test]
async fn trace_metadata_and_resolved_collection_do_not_change_query_identity() {
    let fixture = Fixture::new().await;
    let mut resume = fixture.resume().await;
    resume.request_id = "page-two".into();
    resume.profile = true;
    resume.collection = "docs".into();
    let response = fixture
        .set
        .query(request(resume, "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits[0].doc_id, 1);
    assert_eq!(response.request_id, "page-two");
    assert!(response.profile.is_some());
}

#[tokio::test]
async fn host_managed_keys_allow_equivalent_hosts_and_refuse_rotation_or_route_changes() {
    let fixture = Fixture::new().await;
    let coordinator = fixture.coordinator.clone().with_cursor_signing_key([9; 32]);
    let source = Fixture::set(&coordinator, fixture.authority.clone());
    let first = source
        .query(request(query(), "alice"))
        .await
        .unwrap()
        .into_inner();
    let resume = QueryRequest {
        cursor: first.next_cursor,
        ..query()
    };
    let peer = CoordinatorServiceImpl::new(coordinator.node_addresses().to_vec())
        .with_collection("docs")
        .with_bm25(Some("native".into()), Default::default())
        .with_cursor_signing_key([9; 32]);
    let response = Fixture::set(&peer, fixture.authority.clone())
        .query(request(resume.clone(), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits[0].doc_id, 1);
    let rotated = Fixture::set(
        &peer.clone().with_cursor_signing_key([10; 32]),
        fixture.authority.clone(),
    );
    assert_eq!(
        rotated
            .query(request(resume.clone(), "alice"))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let moved = CoordinatorServiceImpl::new(vec!["http://127.0.0.1:1".into()])
        .with_collection("docs")
        .with_cursor_signing_key([9; 32]);
    let moved = Fixture::set(&moved, fixture.authority.clone());
    assert_eq!(
        moved
            .query(request(resume, "alice"))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn streaming_rejects_a_nested_query_for_another_collection() {
    let fixture = Fixture::new().await;
    let mut body = query();
    body.collection = "another".into();
    let error = fixture
        .set
        .query_stream(request(
            QueryStreamRequest {
                collection: "docs".into(),
                query: Some(body),
                ..Default::default()
            },
            "alice",
        ))
        .await
        .err()
        .expect("nested collection must agree with the authorized resource");
    assert_eq!(error.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn hot_topology_snapshots_keep_authorization_and_cursor_keys() {
    let fixture = Fixture::new().await;
    let hot = fixture
        .coordinator
        .clone()
        .with_hot_topology(vec![None])
        .unwrap();
    let set = Fixture::set(&hot, fixture.authority.clone());
    let first = set
        .query(request(query(), "alice"))
        .await
        .unwrap()
        .into_inner();
    let resume = QueryRequest {
        cursor: first.next_cursor,
        ..query()
    };
    assert_eq!(
        streamed(&set, resume.clone(), "alice").await.unwrap().hits[0].doc_id,
        1
    );
    assert_eq!(
        set.query(request(resume.clone(), "bob"))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    hot.reload_topology(1, hot.current_topology_routes(), None)
        .unwrap();
    assert_eq!(
        set.query(request(resume, "alice"))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let fresh = streamed(&set, query(), "alice").await.unwrap();
    fixture.authority.replace(policy(2)).unwrap();
    let error = set
        .query_stream(request(
            QueryStreamRequest {
                query: Some(QueryRequest {
                    cursor: fresh.next_cursor,
                    ..query()
                }),
                ..Default::default()
            },
            "alice",
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
}
