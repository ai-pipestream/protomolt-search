//! AnalyzeStream client acceptance: order restoration against a mock that
//! DELIBERATELY answers out of order, per-document error isolation, and
//! the unary fallback (batch and ingest) against a sidecar that predates
//! the RPC.

mod common;

use std::pin::Pin;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use turbovec_search::analyzer::{analyze_batch, analyze_document, AnalyzeStream};
use turbovec_search::harness::mock_analysis::MockAnalysis;
use turbovec_search::harness::nodelay_incoming;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::analysis::analysis_service_server::{
    AnalysisService, AnalysisServiceServer,
};
use turbovec_search::pb::analysis::{
    AnalyzeRequest, AnalyzeResponse, AnalyzeStreamRequest, AnalyzeStreamResponse,
    GetCapabilitiesRequest, GetCapabilitiesResponse,
};
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{AddDocumentsRequest, AnalysisSpec};

use common::{mock::start_mock_analysis, start_empty_node};

const TEXTS: [&str; 7] = [
    "the appellate court affirmed the ruling",
    "judges were running proceedings swiftly",
    "vector search rust engines",
    "the motion to dismiss was denied",
    "search engines love rust",
    "a longer opinion with many repeated repeated repeated terms",
    "certiorari granted in part",
];

#[tokio::test]
async fn batch_restores_input_order_despite_reordered_responses() {
    let (addr, server) = start_mock_analysis().await;
    let docs: Vec<(&str, Option<&AnalysisSpec>)> = TEXTS.iter().map(|t| (*t, None)).collect();
    let batch = analyze_batch(&addr, &docs).await.unwrap();
    assert_eq!(batch.len(), TEXTS.len());
    for (i, text) in TEXTS.iter().enumerate() {
        let unary = analyze_document(&addr, text, None).await.unwrap();
        assert_eq!(
            batch[i], unary,
            "document {i} differs from its unary analysis"
        );
    }
    server.abort();
}

#[tokio::test]
async fn session_delivers_every_sequence_out_of_order() {
    let (addr, server) = start_mock_analysis().await;
    let mut session = AnalyzeStream::open(&addr, None).await.unwrap();
    let submit = session.submitter();
    for (i, text) in TEXTS.iter().enumerate() {
        submit.submit(i as u64, text).await.unwrap();
    }
    drop(submit);
    session.finish();
    let mut order = Vec::new();
    while let Some((sequence, result)) = session.next().await.unwrap() {
        result.unwrap();
        order.push(sequence);
    }
    let mut sorted = order.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, (0..TEXTS.len() as u64).collect::<Vec<_>>());
    // The mock swaps each adjacent pair of completions, so completion
    // order MUST differ from submission order: reordering above is not
    // vacuously covered.
    assert_ne!(order, sorted, "mock failed to deliver out of order");
    server.abort();
}

#[tokio::test]
async fn one_bad_document_fails_only_its_sequence() {
    let (addr, server) = start_mock_analysis().await;
    let mut session = AnalyzeStream::open(&addr, None).await.unwrap();
    let submit = session.submitter();
    submit.submit(0, "a good document").await.unwrap();
    submit.submit(1, "").await.unwrap();
    submit.submit(2, "another good document").await.unwrap();
    drop(submit);
    session.finish();
    let mut ok = Vec::new();
    let mut failed = Vec::new();
    while let Some((sequence, result)) = session.next().await.unwrap() {
        match result {
            Ok(_) => ok.push(sequence),
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument);
                failed.push(sequence);
            }
        }
    }
    ok.sort_unstable();
    assert_eq!(ok, vec![0, 2]);
    assert_eq!(failed, vec![1]);
    server.abort();
}

/// A sidecar that predates AnalyzeStream: same unary analysis, but the
/// stream RPC answers UNIMPLEMENTED, the way a live grpc-java server
/// without the method does.
struct NoStreamMock(MockAnalysis);

#[tonic::async_trait]
impl AnalysisService for NoStreamMock {
    async fn analyze(
        &self,
        request: Request<AnalyzeRequest>,
    ) -> Result<Response<AnalyzeResponse>, Status> {
        AnalysisService::analyze(&self.0, request).await
    }

    type AnalyzeStreamStream =
        Pin<Box<dyn Stream<Item = Result<AnalyzeStreamResponse, Status>> + Send>>;

    async fn analyze_stream(
        &self,
        _request: Request<Streaming<AnalyzeStreamRequest>>,
    ) -> Result<Response<Self::AnalyzeStreamStream>, Status> {
        Err(Status::unimplemented("this sidecar predates AnalyzeStream"))
    }

    async fn get_capabilities(
        &self,
        request: Request<GetCapabilitiesRequest>,
    ) -> Result<Response<GetCapabilitiesResponse>, Status> {
        AnalysisService::get_capabilities(&self.0, request).await
    }
}

async fn start_no_stream_mock() -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(AnalysisServiceServer::new(NoStreamMock(MockAnalysis)))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn batch_falls_back_to_unary_on_unimplemented() {
    let (addr, server) = start_no_stream_mock().await;
    let docs: Vec<(&str, Option<&AnalysisSpec>)> = TEXTS.iter().map(|t| (*t, None)).collect();
    let batch = analyze_batch(&addr, &docs).await.unwrap();
    assert_eq!(batch.len(), TEXTS.len());
    for (i, text) in TEXTS.iter().enumerate() {
        let unary = analyze_document(&addr, text, None).await.unwrap();
        assert_eq!(batch[i], unary, "fallback document {i} differs");
    }
    server.abort();
}

#[tokio::test]
async fn ingest_falls_back_to_unary_on_unimplemented() {
    let (analysis, server) = start_no_stream_mock().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in TEXTS {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
        })
        .await
        .unwrap();
    }
    drop(tx);
    let response = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.added, TEXTS.len() as u64);
    assert_eq!(response.total, TEXTS.len() as u64);
    node.abort();
    server.abort();
}
