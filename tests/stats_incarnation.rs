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
    scoring_calls: Arc<AtomicUsize>,
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
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            ..Default::default()
        },
    ));
    NodeLink::Local(node.clone())
        .add_documents(tokio_stream::iter(
            texts
                .iter()
                .map(|text| AddDocumentsRequest {
                    analysis: Some(body_spec()),
                    text: (*text).into(),
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
        scoring_calls: Arc::new(AtomicUsize::new(0)),
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
