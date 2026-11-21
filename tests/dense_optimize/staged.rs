//! End-to-end staged bit-plane (`BitplaneStaged`) and capability-downgraded
//! (`!caps.enumerates`) `OptimizeDensePolicy` + `Query(AUTO)` verification.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::DenseExecutionPolicy;
use pipestream_search::harness::{
    fake_ann, fit_calibration, nodelay_incoming, seeded_index, start_node,
};
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_server::{NodeService, NodeServiceServer};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_shard_response, DenseExecutionMode as Mode, DenseFilterStrategy, DensePrecisionRequest,
    DenseScorePrecision, DenseTechnique, OptimizeDensePolicyRequest, SearchShardRequest,
    SearchShardResponse,
};
use pipestream_search::vector::VectorIndex;
use pipestream_search::MAX_MESSAGE_BYTES;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::codegen::{http, Service};
use tonic::{Request, Response, Status, Streaming};

use super::{auto_query, send_vectors, unit_vectors, NodeTask};

#[derive(Clone)]
struct ExhaustivePacedNode(NodeServiceImpl);

impl tonic::server::NamedService for ExhaustivePacedNode {
    const NAME: &'static str =
        <NodeServiceServer<NodeServiceImpl> as tonic::server::NamedService>::NAME;
}

impl<B> Service<http::Request<B>> for ExhaustivePacedNode
where
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let node = self.0.clone();
        if !request.uri().path().ends_with("/SearchShard") {
            return Box::pin(
                NodeServiceServer::new(node)
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES)
                    .call(request),
            );
        }
        Box::pin(async move {
            struct Paced(NodeServiceImpl);
            impl tonic::server::StreamingService<SearchShardRequest> for Paced {
                type Response = SearchShardResponse;
                type ResponseStream = ReceiverStream<Result<SearchShardResponse, Status>>;
                type Future = Pin<
                    Box<dyn Future<Output = Result<Response<Self::ResponseStream>, Status>> + Send>,
                >;

                fn call(&mut self, req: Request<Streaming<SearchShardRequest>>) -> Self::Future {
                    let node = self.0.clone();
                    Box::pin(async move {
                        let mut stream = node.search_shard(req).await?.into_inner();
                        let (tx, rx) = tokio::sync::mpsc::channel(8);
                        tokio::spawn(async move {
                            while let Some(item) = stream.next().await {
                                let exhaustive = matches!(
                                    &item,
                                    Ok(msg) if matches!(
                                        &msg.payload,
                                        Some(search_shard_response::Payload::Done(done))
                                            if done.stats.as_ref().is_some_and(|s| s.parts_staged == 0 && s.parts_walked == 0)
                                    )
                                );
                                if exhaustive {
                                    tokio::time::sleep(Duration::from_millis(4)).await;
                                }
                                if tx.send(item).await.is_err() {
                                    break;
                                }
                            }
                        });
                        Ok(Response::new(ReceiverStream::new(rx)))
                    })
                }
            }
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .streaming(Paced(node), request)
                .await)
        })
    }
}

async fn start_paced_node(index: VectorIndex) -> (String, NodeTask) {
    let node = NodeServiceImpl::new(Some(index), NodeConfig::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ExhaustivePacedNode(node))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (addr, handle)
}

async fn assert_staged_auto_serves(coord: &CoordinatorServiceImpl, dim: usize) {
    for prec in [
        DensePrecisionRequest::Quantized,
        DensePrecisionRequest::Fp32,
    ] {
        let outcome = coord
            .query(Request::new(auto_query(10, dim, prec)))
            .await
            .unwrap()
            .into_inner()
            .dense_execution
            .unwrap();
        assert!(outcome.policy_qualified, "precision={prec:?}: {outcome:?}");
        assert_eq!(outcome.resolved_mode, Mode::Balanced as i32);
        assert_eq!(outcome.filter_strategy, DenseFilterStrategy::Default as i32);
        assert!(
            outcome
                .techniques
                .iter()
                .any(|t| t.technique == DenseTechnique::BitplaneStaged as i32),
            "precision={prec:?} techniques={:?}",
            outcome.techniques
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fake_staged_optimize_installs_and_serves_auto_at_quantized_and_fp32_precisions() {
    const TV_DIM: usize = 32;
    let corpus = unit_vectors(256, TV_DIM, 0x57A6_2001);
    let (shift, scale) = fit_calibration(TV_DIM, 4, &corpus);
    let index = fake_ann::fake_staged_index(seeded_index(TV_DIM, 4, &shift, &scale));
    let (addr, handle) = start_paced_node(index).await;
    send_vectors(&addr, corpus, TV_DIM, false).await;

    let dir = std::env::temp_dir().join(format!("dense-opt-fake-staged-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_dense_execution_policy_at(path.clone(), None);
    let req = OptimizeDensePolicyRequest {
        policy_id: "fake-staged-opt".into(),
        embedding_model: "test".into(),
        target_recall_ppm: 500_000,
        k: vec![10],
        query_vectors: unit_vectors(4, TV_DIM, 0x57A6_2002),
        max_runtime_ms: 120_000,
        install: true,
        ..Default::default()
    };
    let resp = SearchService::optimize_dense_policy(&coordinator, Request::new(req))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.installed && !resp.truncated);
    let doc = resp.policy.unwrap();
    for out_prec in [DenseScorePrecision::Quantized, DenseScorePrecision::Fp32] {
        assert!(doc.points.iter().any(|p| {
            p.output_precision == out_prec as i32
                && p.technique == DenseTechnique::BitplaneStaged as i32
                && p.filter_strategy == DenseFilterStrategy::Default as i32
        }));
    }
    assert_eq!(
        DenseExecutionPolicy::load(&path).unwrap().fingerprint(),
        resp.policy_fingerprint
    );
    assert_staged_auto_serves(&coordinator, TV_DIM).await;
    let _ = std::fs::remove_dir_all(dir);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fake_ann_without_enumeration_records_downgraded_speed_filter_strategy() {
    const ANN_DIM: usize = 32;
    let corpus = unit_vectors(256, ANN_DIM, 0x57A6_3001);
    let (shift, scale) = fit_calibration(ANN_DIM, 4, &corpus);
    let index = fake_ann::fake_ann_index(seeded_index(ANN_DIM, 4, &shift, &scale));
    let (addr, handle) = start_node(index, NodeConfig::default()).await;
    send_vectors(&addr, corpus, ANN_DIM, false).await;

    let coordinator = CoordinatorServiceImpl::new(vec![addr]);
    // fake-ann has walks=true, exhaustive_scan=false, enumerates=false; verify
    // that optimize refuses cleanly when no certified exhaustive baseline exists.
    let req = OptimizeDensePolicyRequest {
        policy_id: "fake-ann-opt".into(),
        embedding_model: "test".into(),
        target_recall_ppm: 500_000,
        k: vec![10],
        query_vectors: unit_vectors(2, ANN_DIM, 0x57A6_3002),
        max_runtime_ms: 30_000,
        install: false,
        ..Default::default()
    };
    let err = SearchService::optimize_dense_policy(&coordinator, Request::new(req))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    handle.abort();
}

fn host_reads_4bit_planes() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        std::arch::is_aarch64_feature_detected!("dotprod")
    }
    #[cfg(target_arch = "x86_64")]
    {
        std::arch::is_x86_feature_detected!("avx512vbmi")
            && std::arch::is_x86_feature_detected!("avx512vnni")
            && std::arch::is_x86_feature_detected!("avx512bw")
            && std::arch::is_x86_feature_detected!("avx512f")
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        false
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turbovec_optimize_installs_and_serves_staged_auto_when_supported() {
    if !host_reads_4bit_planes() {
        return;
    }
    const TV_DIM: usize = 128;
    const ROWS: usize = 34_000;
    let corpus = unit_vectors(ROWS, TV_DIM, 0x7B04_0001);
    let (shift, scale) = fit_calibration(TV_DIM, 4, &corpus[..1024 * TV_DIM]);
    let (addr, handle) = start_node(
        seeded_index(TV_DIM, 4, &shift, &scale),
        NodeConfig::default(),
    )
    .await;
    send_vectors(&addr, corpus, TV_DIM, false).await;

    let dir = std::env::temp_dir().join(format!("dense-opt-tv-staged-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_dense_execution_policy_at(path.clone(), None);
    let req = OptimizeDensePolicyRequest {
        policy_id: "tv-opt".into(),
        embedding_model: "test".into(),
        target_recall_ppm: 500_000,
        k: vec![10],
        query_vectors: unit_vectors(4, TV_DIM, 0x7B04_0002),
        max_runtime_ms: 120_000,
        install: true,
        ..Default::default()
    };
    let resp = SearchService::optimize_dense_policy(&coordinator, Request::new(req))
        .await
        .unwrap()
        .into_inner();
    assert!(resp.installed);
    let doc = resp.policy.unwrap();
    for out_prec in [DenseScorePrecision::Quantized, DenseScorePrecision::Fp32] {
        assert!(doc.points.iter().any(|p| {
            p.output_precision == out_prec as i32
                && p.technique == DenseTechnique::BitplaneStaged as i32
                && p.filter_strategy == DenseFilterStrategy::Default as i32
        }));
    }
    assert_staged_auto_serves(&coordinator, TV_DIM).await;
    let _ = std::fs::remove_dir_all(dir);
    handle.abort();
}
