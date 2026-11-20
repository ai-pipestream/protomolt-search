mod analyze;
mod priority;
mod rerank;
mod service;

pub(crate) use priority::Lane;
pub use priority::{LaneStats, PriorityLanes};

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::{Error as TransportError, Server};

use turbo_client::proto::result::analysis_service_server::AnalysisServiceServer;

use crate::MAX_MESSAGE_BYTES;

/// Observable lifecycle of deliberately delayed unary analysis calls.
/// This keeps cancellation/deadline tests deterministic without putting
/// sleeps on the ordinary mock used by the rest of the suite.
#[derive(Clone, Default)]
pub struct AnalysisDelayProbe {
    started: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    cancelled: Arc<AtomicU64>,
}

impl AnalysisDelayProbe {
    pub fn started(&self) -> u64 {
        self.started.load(Ordering::Acquire)
    }

    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    pub fn cancelled(&self) -> u64 {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct DelayedCall {
    probe: AnalysisDelayProbe,
    completed: bool,
}

impl DelayedCall {
    fn begin(probe: AnalysisDelayProbe) -> Self {
        probe.started.fetch_add(1, Ordering::AcqRel);
        Self {
            probe,
            completed: false,
        }
    }

    fn complete(mut self) {
        self.probe.completed.fetch_add(1, Ordering::AcqRel);
        self.completed = true;
    }
}

impl Drop for DelayedCall {
    fn drop(&mut self) {
        if !self.completed {
            self.probe.cancelled.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// Toy stemmer: strips a trailing "ing" (>5 chars, folding a resulting
/// double consonant, so "running" -> "run") or trailing 's' (>3 chars)
/// after lowercasing. Deterministic; enough to exercise the STEMS
/// identity path.
pub fn toy_stem(word: &str) -> String {
    let w = word.to_lowercase();
    if w.ends_with("ing") && w.len() > 5 {
        let stem = &w[..w.len() - 3];
        let bytes = stem.as_bytes();
        if bytes.len() >= 2 && bytes[bytes.len() - 1] == bytes[bytes.len() - 2] {
            return stem[..stem.len() - 1].to_string();
        }
        return stem.to_string();
    }
    if w.ends_with('s') && w.len() > 3 {
        return w[..w.len() - 1].to_string();
    }
    w
}

/// Deterministic mock implementation of `turbo.result.v1.AnalysisService`.
pub struct MockAnalysis {
    /// Whether this mock advertises NER/geo layers in `Capabilities`.
    pub ner: bool,
    /// Whether this mock populates sentence spans when requested.
    pub sentence_layer: bool,
    /// Whether this mock serves both folded and cased term identities in one pass.
    pub dual_identity: bool,
    /// Whether `AnalyzeStream` answers `UNIMPLEMENTED` (`start_mock_analysis_without_stream`).
    pub no_stream: bool,
    /// Total per-document analyses executed across unary and streaming paths.
    pub calls: Arc<AtomicU64>,
    /// Count of unary `AnalyzeDocumentProgressive` invocations.
    pub unary_calls: Arc<AtomicUsize>,
    /// Count of `AnalyzeStream` sessions opened.
    pub stream_opens: Arc<AtomicUsize>,
    unary_delay: Option<(Duration, AnalysisDelayProbe)>,
    /// Optional semaphore holding `AnalyzeStream` open after its last result.
    pub stream_end_gate: Option<Arc<tokio::sync::Semaphore>>,
    /// Optional accelerator slot every streamed document and every rerank
    /// call takes one unit of, in the lane its `x-turbo-priority` names.
    pub lanes: Option<Arc<PriorityLanes>>,
}

impl Default for MockAnalysis {
    fn default() -> Self {
        MockAnalysis {
            ner: true,
            sentence_layer: true,
            dual_identity: true,
            no_stream: false,
            calls: Arc::new(AtomicU64::new(0)),
            unary_calls: Arc::new(AtomicUsize::new(0)),
            stream_opens: Arc::new(AtomicUsize::new(0)),
            unary_delay: None,
            stream_end_gate: None,
            lanes: None,
        }
    }
}

/// Start the mock on 127.0.0.1:0; returns its `http://` address.
pub async fn start_mock_analysis() -> (String, JoinHandle<Result<(), TransportError>>) {
    start_mock(MockAnalysis::default()).await
}

/// Start a mock whose `AnalyzeStream` RPC answers `UNIMPLEMENTED`.
pub async fn start_mock_analysis_without_stream() -> (String, JoinHandle<Result<(), TransportError>>)
{
    start_mock(MockAnalysis {
        no_stream: true,
        ..MockAnalysis::default()
    })
    .await
}

/// Start a mock exposing separate unary and stream-open transport counters.
pub async fn start_mock_analysis_transport_counts() -> (
    String,
    JoinHandle<Result<(), TransportError>>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let unary = Arc::new(AtomicUsize::new(0));
    let streams = Arc::new(AtomicUsize::new(0));
    let (addr, handle) = start_mock(MockAnalysis {
        unary_calls: unary.clone(),
        stream_opens: streams.clone(),
        ..MockAnalysis::default()
    })
    .await;
    (addr, handle, unary, streams)
}

/// A mock whose `AnalyzeStream` calls hold their response stream
/// open after the last result until the returned gate grants a
/// permit (one per stream).
pub async fn start_mock_analysis_gated() -> (
    String,
    Arc<tokio::sync::Semaphore>,
    JoinHandle<Result<(), TransportError>>,
) {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (addr, handle) = start_mock(MockAnalysis {
        stream_end_gate: Some(Arc::clone(&gate)),
        ..MockAnalysis::default()
    })
    .await;
    (addr, gate, handle)
}

/// A normal mock whose unary `AnalyzeDocumentProgressive` method pauses for `delay`.
/// The returned probe distinguishes completed calls from futures dropped
/// because their client cancelled or exceeded a deadline.
pub async fn start_mock_analysis_delayed(
    delay: Duration,
) -> (
    String,
    JoinHandle<Result<(), TransportError>>,
    AnalysisDelayProbe,
) {
    let probe = AnalysisDelayProbe::default();
    let (address, handle) = start_mock(MockAnalysis {
        unary_delay: Some((delay, probe.clone())),
        ..Default::default()
    })
    .await;
    (address, handle, probe)
}

/// [`start_mock_analysis`] modeling an analysis server with NO NER/geo
/// layers advertised in `Capabilities`.
pub async fn start_mock_analysis_without_ner() -> (String, JoinHandle<Result<(), TransportError>>) {
    start_mock(MockAnalysis {
        ner: false,
        ..Default::default()
    })
    .await
}

/// A mock whose analysis and reranking share one accelerator slot: every
/// streamed document and every rerank call holds it for `unit`, and a
/// freed slot goes to interactive calls before bulk ones.
pub async fn start_mock_analysis_prioritized(
    unit: Duration,
) -> (
    String,
    JoinHandle<Result<(), TransportError>>,
    Arc<PriorityLanes>,
) {
    let lanes = PriorityLanes::new(unit);
    let (address, handle) = start_mock(MockAnalysis {
        lanes: Some(Arc::clone(&lanes)),
        ..Default::default()
    })
    .await;
    (address, handle, lanes)
}

/// A normal mock plus its call meter: every analysis it serves,
/// unary or streaming, one per document.
pub async fn start_mock_analysis_metered() -> (
    String,
    JoinHandle<Result<(), TransportError>>,
    Arc<AtomicU64>,
) {
    let calls = Arc::new(AtomicU64::new(0));
    let (address, handle) = start_mock(MockAnalysis {
        calls: calls.clone(),
        ..Default::default()
    })
    .await;
    (address, handle, calls)
}

/// A mock that never returns the sentence layer, even when asked:
/// the response a sentence field's ingest refuses rather than
/// storing an empty table for a document with terms.
pub async fn start_mock_analysis_without_sentences(
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_mock(MockAnalysis {
        sentence_layer: false,
        ..Default::default()
    })
    .await
}

/// A mock modelling an analysis server lacking dual term identity support.
pub async fn start_mock_analysis_without_dual_identity() -> (
    String,
    JoinHandle<Result<(), TransportError>>,
    Arc<AtomicU64>,
) {
    let calls = Arc::new(AtomicU64::new(0));
    let (address, handle) = start_mock(MockAnalysis {
        dual_identity: false,
        calls: calls.clone(),
        ..Default::default()
    })
    .await;
    (address, handle, calls)
}

async fn start_mock(mock: MockAnalysis) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(
                AnalysisServiceServer::new(mock)
                    .max_decoding_message_size(MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(crate::harness::nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}
