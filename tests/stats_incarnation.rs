use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    link::NodeLink,
    node::{NodeConfig, NodeServiceImpl},
    pb::{node_service_server::NodeServiceServer, AddDocumentsRequest},
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tonic::codegen::{http, Service};

type NodeServer = NodeServiceServer<NodeServiceImpl>;

/// Keep the TCP listener and pooled connection fixed while replacing the
/// complete handler. This isolates address reuse from transport retry timing.
#[derive(Clone)]
struct Replaceable<S> {
    current: Arc<Mutex<S>>,
    before_score: Arc<Mutex<VecDeque<S>>>,
    before_fetch: Arc<Mutex<VecDeque<S>>>,
    scoring_calls: Arc<AtomicUsize>,
    before_reads: Arc<Mutex<std::collections::HashMap<String, VecDeque<Option<S>>>>>,
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for Replaceable<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<http::Request<B>> for Replaceable<S>
where
    S: Service<http::Request<B>> + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // The wrapped generated tonic servers are always ready.
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let route = request.uri().path().rsplit('/').next().unwrap();
        if let Some(Some(next)) = self
            .before_reads
            .lock()
            .unwrap()
            .get_mut(route)
            .and_then(VecDeque::pop_front)
        {
            *self.current.lock().unwrap() = next;
        }
        if request.uri().path().ends_with("/FetchValues") {
            if let Some(next) = self.before_fetch.lock().unwrap().pop_front() {
                *self.current.lock().unwrap() = next;
            }
        }
        if [
            "Bm25Query",
            "Bm25QueryStream",
            "Bm25Rescore",
            "ShardLegs",
            "HybridShard",
        ]
        .iter()
        .any(|route| request.uri().path().ends_with(&format!("/{route}")))
        {
            self.scoring_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(next) = self.before_score.lock().unwrap().pop_front() {
                *self.current.lock().unwrap() = next;
            }
        }
        self.current.lock().unwrap().clone().call(request)
    }
}

async fn node(texts: &[&str]) -> NodeServer {
    node_values(texts, false).await
}
async fn node_values(texts: &[&str], varying: bool) -> NodeServer {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            numeric_fields: vec!["boost".into()],
            ..Default::default()
        },
    ));
    NodeLink::Local(node.clone())
        .add_documents(tokio_stream::iter(
            texts
                .iter()
                .enumerate()
                .map(|(index, text)| AddDocumentsRequest {
                    analysis: Some(body_spec()),
                    text: (*text).into(),
                    numerics: vec![pipestream_search::pb::NumericValue {
                        field: "boost".into(),
                        value: if varying { (index + 1) as f64 } else { 1.0 },
                    }],
                    ..Default::default()
                })
                .collect::<Vec<_>>(),
        ))
        .await
        .unwrap();
    node.as_ref().clone().into_server(16 * 1024 * 1024)
}

async fn start(
    server: NodeServer,
) -> (
    String,
    Replaceable<NodeServer>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let service = Replaceable {
        current: Arc::new(Mutex::new(server)),
        before_score: Arc::new(Mutex::new(VecDeque::new())),
        before_fetch: Arc::new(Mutex::new(VecDeque::new())),
        scoring_calls: Arc::new(AtomicUsize::new(0)),
        before_reads: Arc::new(Mutex::new(std::collections::HashMap::new())),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.clone())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (address, service, handle)
}

fn coordinator(address: String) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![address])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
}

fn signature(hits: &[pipestream_search::pb::Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter()
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect()
}

#[tokio::test]
async fn warm_cache_refetches_when_the_same_address_serves_a_new_lifetime() {
    for levels in 0..=2 {
        let (address, service, handle) = start(node(&["rust rust", "rust"]).await).await;
        let mut relays = Vec::new();
        let mut root_address = address;
        for _ in 0..levels {
            let (address, _, relay) =
                pipestream_search::harness::start_relay(vec![root_address]).await;
            root_address = address;
            relays.push(relay);
        }
        let warm = coordinator(root_address.clone());
        warm.fanout_bm25("rust", 10, Some(&body_spec()))
            .await
            .unwrap();
        assert_eq!(warm.stats_cache().fetch_count(), 1);
        *service.current.lock().unwrap() = node(&["rust rust", "other other other"]).await;
        let actual = warm
            .fanout_bm25("rust", 10, Some(&body_spec()))
            .await
            .unwrap();
        let expected = coordinator(root_address)
            .fanout_bm25("rust", 10, Some(&body_spec()))
            .await
            .unwrap();
        assert_eq!(signature(&actual), signature(&expected));
        assert_eq!(
            warm.stats_cache().fetch_count(),
            2,
            "replacement invalidates the old cached share"
        );
        handle.abort();
        for relay in relays {
            relay.abort();
        }
    }
}

#[tokio::test]
async fn a_second_replacement_during_retry_is_refused_instead_of_dropping_the_fence() {
    let (address, service, handle) = start(node(&["rust rust", "rust"]).await).await;
    let warm = coordinator(address);
    warm.fanout_bm25("rust", 10, Some(&body_spec()))
        .await
        .unwrap();
    let second = node(&["rust rust", "other other"]).await;
    let third = node(&["rust", "rust other other other"]).await;
    *service.before_score.lock().unwrap() = VecDeque::from([second, third]);
    let before = service.scoring_calls.load(Ordering::SeqCst);
    let refusal = warm
        .fanout_bm25("rust", 10, Some(&body_spec()))
        .await
        .unwrap_err();
    assert_eq!(refusal.code(), tonic::Code::FailedPrecondition);
    assert!(refusal.message().starts_with("stale stats epoch"));
    assert_eq!(
        service.scoring_calls.load(Ordering::SeqCst) - before,
        2,
        "bounded retry"
    );
    assert_eq!(warm.stats_cache().fetch_count(), 2);
    handle.abort();
}

#[tokio::test]
async fn every_lexical_scoring_route_rejects_another_lifetime() {
    use pipestream_search::pb::*;
    let (address, service, handle) = start(node(&["rust rust", "rust"]).await).await;
    let mut client = node_service_client::NodeServiceClient::connect(address)
        .await
        .unwrap();
    let stats = client
        .term_stats(TermStatsRequest {
            version_only: false,
            terms: vec!["rust".into()],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    *service.current.lock().unwrap() = node(&["rust rust", "other other"]).await;
    let bm25 = Bm25QueryRequest {
        terms: vec!["rust".into()],
        k: 10,
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        expected_stats_epoch: stats.stats_epoch,
        expected_stats_incarnation: stats.stats_incarnation.clone(),
        ..Default::default()
    };
    let unary = client.bm25_query(bm25.clone()).await.unwrap_err();
    let mut fused_request = bm25.clone();
    fused_request.fields = vec![Bm25FieldLeg {
        field: "body".into(),
        terms: bm25.terms.clone(),
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        ..Default::default()
    }];
    let fused = client.bm25_query(fused_request).await.unwrap_err();
    let rescore = client
        .bm25_rescore(Bm25RescoreRequest {
            terms: bm25.terms.clone(),
            candidate_ids: vec![0],
            global_doc_count: stats.doc_count,
            global_total_doc_length: stats.total_doc_length,
            global_doc_frequencies: stats.doc_frequencies.clone(),
            expected_stats_epoch: stats.stats_epoch,
            expected_stats_incarnation: stats.stats_incarnation.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let legs = client
        .shard_legs(ShardLegsRequest {
            terms: bm25.terms.clone(),
            k: 10,
            global_doc_count: stats.doc_count,
            global_total_doc_length: stats.total_doc_length,
            global_doc_frequencies: stats.doc_frequencies.clone(),
            expected_stats_epoch: stats.stats_epoch,
            expected_stats_incarnation: stats.stats_incarnation.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let hybrid = client
        .hybrid_shard(HybridShardRequest {
            terms: bm25.terms.clone(),
            k: 10,
            global_doc_count: stats.doc_count,
            global_total_doc_length: stats.total_doc_length,
            global_doc_frequencies: stats.doc_frequencies.clone(),
            expected_stats_epoch: stats.stats_epoch,
            expected_stats_incarnation: stats.stats_incarnation.clone(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let streaming = client
        .bm25_query_stream(tokio_stream::iter([Bm25QueryStreamRequest {
            payload: Some(bm25_query_stream_request::Payload::Start(bm25)),
        }]))
        .await;
    let streaming = match streaming {
        Err(error) => error,
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Err(error) => break error,
                    Ok(None) => panic!("a stale stream must refuse"),
                    Ok(Some(frame)) => {
                        // No candidates from an incorrectly fenced scoring operation.
                        if let Some(bm25_query_stream_response::Payload::CandidateBatch(_)) =
                            frame.payload
                        {
                            panic!("stale candidates escaped");
                        }
                    }
                }
            }
        }
    };
    for error in [unary, fused, rescore, legs, hybrid, streaming] {
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{error}");
        assert!(error.message().starts_with("stale stats epoch"), "{error}");
    }
    handle.abort();
}

fn public_query() -> pipestream_search::pb::QueryRequest {
    use pipestream_search::pb::*;
    QueryRequest {
        k: 1,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "text".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "rust".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn public_query_value_fetch_keeps_the_preselection_lifetime() {
    use pipestream_search::pb::{search_service_server::SearchService, *};
    let (address, service, handle) = start(node(&["rust", "rust"]).await).await;
    let coordinator = coordinator(address);
    let mut query = public_query();
    query.sort = vec![QuerySort {
        column: "boost".into(),
        descending: false,
    }];
    query.projections = vec![NamedProjection {
        name: "boost".into(),
        expression: "boost".into(),
    }];
    let baseline = SearchService::query(&coordinator, tonic::Request::new(query.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(baseline.hits.len(), 1);
    let replacement = node(&["rust", "rust"]).await;
    service.before_fetch.lock().unwrap().push_back(replacement);
    let error = SearchService::query(&coordinator, tonic::Request::new(query))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        service.before_fetch.lock().unwrap().is_empty(),
        "the test must reach the guarded value fetch"
    );
    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn final_query_validation_rejects_a_retry_that_changed_the_read_generation() {
    use pipestream_search::pb::search_service_server::SearchService;
    for streaming in [false, true] {
        let (address, service, handle) = start(node(&["rust", "rust"]).await).await;
        let coordinator = coordinator(address);
        let next = node(&["rust", "rust"]).await;
        service.before_score.lock().unwrap().push_back(next);
        if streaming {
            use pipestream_search::pb::*;
            use tokio_stream::StreamExt;
            let mut stream = SearchService::query_stream(
                &coordinator,
                tonic::Request::new(QueryStreamRequest {
                    query: Some(public_query()),
                    timeout_ms: 10000,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_inner();
            let mut completion = None;
            while let Some(event) = stream.next().await {
                match event.unwrap().payload.unwrap() {
                    query_stream_response::Payload::Revision(revision) => {
                        assert_ne!(revision.phase, QueryStreamPhase::Final as i32)
                    }
                    query_stream_response::Payload::Completion(done) => completion = Some(done),
                }
            }
            let done = completion.unwrap();
            assert!(!done.completed);
            assert!(done.response.is_none());
            assert_eq!(done.error_code, tonic::Code::FailedPrecondition as u32);
            assert!(done.error_message.contains("query data changed"));
        } else {
            let error = SearchService::query(&coordinator, tonic::Request::new(public_query()))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(error.message().contains("query data changed"));
        }
        assert!(
            service.scoring_calls.load(Ordering::SeqCst) >= 2,
            "the lexical delegate must retry successfully before the final read check"
        );
        handle.abort();
        let _ = handle.await;
    }
}

#[tokio::test]
async fn cursor_rejects_a_new_lifetime_even_when_ids_and_scores_are_unchanged() {
    use pipestream_search::pb::search_service_server::SearchService;
    let (address, service, handle) = start(node(&["rust", "rust"]).await).await;
    let coordinator = coordinator(address);
    let first = SearchService::query(&coordinator, tonic::Request::new(public_query()))
        .await
        .unwrap()
        .into_inner();
    assert!(!first.next_cursor.is_empty());
    *service.current.lock().unwrap() = node(&["rust", "rust"]).await;
    let fresh = SearchService::query(&coordinator, tonic::Request::new(public_query()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.hits, fresh.hits);
    let mut resume = public_query();
    resume.cursor = first.next_cursor;
    let error = SearchService::query(&coordinator, tonic::Request::new(resume))
        .await
        .unwrap_err();
    assert!(error.message().contains("cursor data context changed"));
    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn query_pins_a_reachable_replica_for_selection_values_and_cursor_continuation() {
    use pipestream_search::pb::{search_service_server::SearchService, *};
    let (replica, _, handle) = start(node(&["rust", "rust"]).await).await;
    let unused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = format!("http://{}", unused.local_addr().unwrap());
    drop(unused);
    let coordinator = coordinator(unavailable)
        .with_replicas(vec![Some(replica)])
        .with_limits(pipestream_search::coordinator::FanoutLimits {
            shard_deadline: Some(std::time::Duration::from_secs(2)),
            hedge_delay: None,
        });
    let mut query = public_query();
    query.sort = vec![QuerySort {
        column: "boost".into(),
        descending: false,
    }];
    query.projections = vec![NamedProjection {
        name: "boost".into(),
        expression: "boost".into(),
    }];
    let first = SearchService::query(&coordinator, tonic::Request::new(query.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.hits[0].doc_id, 0);
    assert_eq!(first.hits[0].projected.len(), 1);
    query.cursor = first.next_cursor;
    let next = SearchService::query(&coordinator, tonic::Request::new(query))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next.hits[0].doc_id, 1);
    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn query_stream_deadline_includes_the_initial_version_probe() {
    use pipestream_search::pb::{search_service_server::SearchService, *};
    use tokio_stream::StreamExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        loop {
            sockets.push(listener.accept().await.unwrap().0);
        }
    });
    let coordinator = coordinator(address);
    let mut stream = SearchService::query_stream(
        &coordinator,
        tonic::Request::new(QueryStreamRequest {
            query: Some(public_query()),
            timeout_ms: 20,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let completion = tokio::time::timeout(std::time::Duration::from_secs(2), async move {
        while let Some(message) = stream.next().await {
            if let query_stream_response::Payload::Completion(done) =
                message.unwrap().payload.unwrap()
            {
                return done;
            }
        }
        panic!("missing completion");
    })
    .await
    .unwrap();
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_eq!(completion.error_code, tonic::Code::DeadlineExceeded as u32);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn query_cursor_versions_follow_child_replacement_through_nested_relays() {
    use pipestream_search::pb::search_service_server::SearchService;
    for levels in 1..=2 {
        let (mut address, service, handle) = start(node(&["rust", "rust"]).await).await;
        let mut relays = Vec::new();
        for _ in 0..levels {
            let (next, _, relay) = pipestream_search::harness::start_relay(vec![address]).await;
            address = next;
            relays.push(relay);
        }
        let coordinator = coordinator(address);
        let first = SearchService::query(&coordinator, tonic::Request::new(public_query()))
            .await
            .unwrap()
            .into_inner();
        let replacement = node(&["rust", "rust"]).await;
        *service.current.lock().unwrap() = replacement;
        let mut query = public_query();
        query.cursor = first.next_cursor;
        let error = SearchService::query(&coordinator, tonic::Request::new(query))
            .await
            .unwrap_err();
        assert!(
            error.message().contains("cursor data context changed"),
            "{error}"
        );
        for relay in relays {
            relay.abort();
            let _ = relay.await;
        }
        handle.abort();
        let _ = handle.await;
    }
}

#[tokio::test]
async fn aggregate_refuses_replacement_at_every_read_boundary() {
    use pipestream_search::pb::{
        search_service_server::SearchService, AggregateOp, AggregateRequest, Aggregation,
        PercentileSpec,
    };
    for (route, varying, skip) in [
        ("AggregateShard", true, 0),
        ("QuantileCounts", true, 0),
        ("QuantileCounts", true, 1),
        ("TermStats", false, 1),
    ] {
        let original = node_values(&["alpha", "beta", "gamma"], varying).await;
        let replacement = node_values(&["alpha", "beta", "gamma"], varying).await;
        let (address, service, handle) = start(original).await;
        service.before_reads.lock().unwrap().insert(
            route.into(),
            (0..skip).map(|_| None).chain([Some(replacement)]).collect(),
        );
        let error = SearchService::aggregate(
            &coordinator(address),
            tonic::Request::new(AggregateRequest {
                aggregations: vec![Aggregation {
                    name: "count".into(),
                    expression: "1".into(),
                    op: AggregateOp::Count as i32,
                    ..Default::default()
                }],
                percentiles: vec![PercentileSpec {
                    name: "pct".into(),
                    expression: "boost".into(),
                    percentiles: vec![50.],
                }],
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{route}: {error}"
        );
        assert!(
            service.before_reads.lock().unwrap()[route].is_empty(),
            "replacement must have happened at {route}"
        );
        handle.abort();
        let _ = handle.await;
    }
}

#[tokio::test]
async fn public_query_collapse_refuses_lineage_from_a_replacement_lifetime() {
    use pipestream_search::pb::{search_service_server::SearchService, CollapseSpec};
    for column in ["parent_id", "group_id"] {
        let (address, service, handle) = start(node(&["rust", "rust"]).await).await;
        let mut query = public_query();
        query.collapse = Some(CollapseSpec {
            column: column.into(),
            inner_hits: 2,
        });
        // The same request executes before replacement, proving this reaches lineage.
        SearchService::query(
            &coordinator(address.clone()),
            tonic::Request::new(query.clone()),
        )
        .await
        .unwrap();
        service.before_reads.lock().unwrap().insert(
            "ResolveParents".into(),
            [Some(node(&["rust", "rust"]).await)].into(),
        );
        let error = SearchService::query(&coordinator(address), tonic::Request::new(query))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(service.before_reads.lock().unwrap()["ResolveParents"].is_empty());
        handle.abort();
        let _ = handle.await;
    }
}
