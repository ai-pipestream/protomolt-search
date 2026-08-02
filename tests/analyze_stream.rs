//! AnalyzeStream client acceptance: order restoration against a mock that
//! DELIBERATELY answers out of order, per-document error isolation, and
//! the LOUD REFUSAL (batch and ingest) of a sidecar that predates the RPC.
//!
//! There used to be a quiet downgrade to per-document unary calls here.
//! It cost real debugging time: a stale sidecar took it silently, its
//! server GOAWAYed after ~70 streams, and a multi-hour bulk job died with
//! an opaque h2 error while the node logged nothing. Both paths now name
//! the version skew instead, and these tests pin that.

mod common;

use std::pin::Pin;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};
use turbovec_search::analyzer::{
    analyze_batch, analyze_batch_streams, analyze_document, AnalyzeStream,
};
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
async fn batch_refuses_a_sidecar_without_analyze_stream() {
    let (addr, server) = start_no_stream_mock().await;
    let docs: Vec<(&str, Option<&AnalysisSpec>)> = TEXTS.iter().map(|t| (*t, None)).collect();
    let status = analyze_batch(&addr, &docs)
        .await
        .expect_err("a sidecar without AnalyzeStream must be refused, not silently downgraded");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("AnalyzeStream"),
        "the refusal must name the missing RPC so the fix is obvious: {}",
        status.message()
    );
    server.abort();
}

#[tokio::test]
async fn ingest_refuses_a_sidecar_without_analyze_stream() {
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
    let status = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .expect_err("ingest against a pre-AnalyzeStream sidecar must fail loudly");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("AnalyzeStream"),
        "the refusal must name the missing RPC: {}",
        status.message()
    );
    node.abort();
    server.abort();
}

/// The stream count is a throughput knob and nothing else.
///
/// Analysis is a pure function of (text, spec), and results are keyed by
/// the caller's sequence, so splitting a batch over N streams changes
/// only who waits. Pinned because a knob that quietly perturbed term
/// identity would corrupt an index rather than merely slow one down, and
/// the corruption would surface as bad rankings months later.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_count_does_not_change_a_single_result() {
    let (addr, server) = start_mock_analysis().await;
    // More documents than streams, and (below) more streams than
    // documents: both sides of the clamp.
    let docs: Vec<(&str, Option<&AnalysisSpec>)> = TEXTS.iter().map(|t| (*t, None)).collect();
    let baseline = analyze_batch_streams(&addr, &docs, 1).await.unwrap();
    for streams in [2, 3, 4, 7, 16] {
        let split = analyze_batch_streams(&addr, &docs, streams).await.unwrap();
        assert_eq!(
            split, baseline,
            "{streams} streams changed the analysis of an unchanged batch"
        );
    }
    // 0 is clamped to 1 rather than analyzing nothing.
    assert_eq!(analyze_batch_streams(&addr, &docs, 0).await.unwrap(), baseline);
    server.abort();
}

/// Mixed specs must stay grouped, and the split must respect the groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_streams_keep_each_spec_with_its_own_documents() {
    let (addr, server) = start_mock_analysis().await;
    let stemmed = AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        normalizer_rungs: Vec::new(),
    };
    // Interleaved specs, so a naive split would cross a group boundary.
    let docs: Vec<(&str, Option<&AnalysisSpec>)> = TEXTS
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, if i % 2 == 0 { Some(&stemmed) } else { None }))
        .collect();
    let baseline = analyze_batch_streams(&addr, &docs, 1).await.unwrap();
    for streams in [2, 5] {
        assert_eq!(
            analyze_batch_streams(&addr, &docs, streams).await.unwrap(),
            baseline,
            "{streams} streams crossed a spec boundary"
        );
    }
    // And the two specs really do produce different terms, or the check
    // above would pass on an accident.
    let plain = analyze_document(&addr, TEXTS[0], None).await.unwrap();
    let stem = analyze_document(&addr, TEXTS[0], Some(&stemmed)).await.unwrap();
    assert_ne!(plain, stem, "the fixture's two specs must actually differ");
    server.abort();
}

/// An empty batch opens no streams and returns nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_batch_is_not_an_error() {
    let (addr, server) = start_mock_analysis().await;
    assert!(analyze_batch_streams(&addr, &[], 8).await.unwrap().is_empty());
    server.abort();
}
