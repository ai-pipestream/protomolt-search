//! Shared harness: deterministic corpus generation, calibration fitting,
//! shard partitioning, and loopback server startup.
//!
//! Used by the integration tests and the `sweep` benchmark binary. Also
//! usable for real deployments: [`write_shards`] persists sharded,
//! uniformly configured vector images that the search binary loads
//! via `[[shards]]` entries in the cluster config.

use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Error as TransportError, Server};

use crate::coordinator::CoordinatorServiceImpl;
use crate::node::{NodeConfig, NodeServiceImpl};
use crate::pb::{ConfigureVectorBackendRequest, VectorBackendConfig as WireVectorBackendConfig};
use crate::vector::{
    embedded_turbovec_config, legacy_calibration_config, VectorIndex, EMBEDDED_TURBOVEC,
};
use crate::MAX_MESSAGE_BYTES;

/// Deterministic pseudo-random unit vectors (LCG + L2 normalize), same
/// generator style as the turbovec test suite.
pub fn unit_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut s = seed.wrapping_add(0x9E3779B97F4A7C15);
    let mut out = vec![0.0f32; n * dim];
    for row in out.chunks_mut(dim) {
        let mut norm = 0.0f64;
        for x in row.iter_mut() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = ((s >> 33) as f64 / (1u64 << 31) as f64) - 1.0;
            *x = v as f32;
            norm += v * v;
        }
        let inv = 1.0 / (norm.sqrt() + 1e-9);
        for x in row.iter_mut() {
            *x = (*x as f64 * inv) as f32;
        }
    }
    out
}

/// Fit a TQ+ calibration on a representative sample: calibrate a
/// throwaway index with it (upstream's explicit `calibrate`, whose fit
/// is deterministic in the sample) and read out the committed pair.
///
/// Sample QUALITY is the caller's responsibility, per upstream's
/// design: ~1024 uniformly random rows lands within half a point of a
/// full-corpus fit, while the same count taken as a sorted prefix is
/// catastrophically biased. There is no warm-up threshold anymore —
/// a tiny sample fits (deterministically), it just fits noisily.
pub fn fit_calibration(dim: usize, bit_width: usize, sample: &[f32]) -> (Vec<f32>, Vec<f32>) {
    let config = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, dim, bit_width, sample)
        .expect("calibration sample must be non-empty finite rows");
    let legacy = legacy_calibration_config(&config)
        .expect("fitted backend config must decode")
        .expect("embedded backend exposes the legacy calibration view");
    (legacy.shift, legacy.scale)
}

/// An empty index committed to an externally fitted calibration pair:
/// upstream's `from_parts` with zero rows. Every index built from the
/// same pair encodes a given vector byte-identically regardless of
/// build history, which is what makes per-shard scores mergeable into
/// an exact global top-k. (This replaces the fork's former
/// `new_with_calibration` patch; upstream #474 made the property
/// expressible with stock API.)
pub fn seeded_index(dim: usize, bit_width: usize, shift: &[f32], scale: &[f32]) -> VectorIndex {
    let config = embedded_turbovec_config(bit_width, shift, scale)
        .expect("a fitted calibration pair is valid backend state");
    VectorIndex::from_backend_config(dim, &config)
        .expect("a fitted calibration pair is valid backend state")
}

/// Wire request for configuring the shipped embedded provider. Most tests use
/// this generic RPC; dedicated compatibility tests cover SetCalibration.
pub fn embedded_backend_request(
    dim: usize,
    bits_per_dimension: usize,
    shift: &[f32],
    scale: &[f32],
) -> ConfigureVectorBackendRequest {
    let config = embedded_turbovec_config(bits_per_dimension, shift, scale)
        .expect("test calibration is valid provider state");
    ConfigureVectorBackendRequest {
        dim: dim as u32,
        config: Some(WireVectorBackendConfig {
            backend_kind: config.backend_kind,
            config_format: config.config_format,
            payload: config.payload,
        }),
    }
}

/// One shard's index plus its global id base (the corpus offset of its
/// first vector; partitions are contiguous ranges).
pub struct Shard {
    pub index: VectorIndex,
    pub slot_offset: u64,
}

/// Build `n_shards` indexes over contiguous, disjoint partitions of
/// `corpus`, all committed to the same calibration — the property that
/// makes their scores mutually comparable.
///
/// Cut points are free: with one explicit global pair, codes are a
/// pure function of (row, pair), so ANY partition of the corpus is
/// bitwise consistent with the monolithic build. (The block-aligned
/// cuts the per-block-calibration chain required are gone with it.)
pub fn build_shards(
    corpus: &[f32],
    dim: usize,
    bit_width: usize,
    n_shards: usize,
    shift: &[f32],
    scale: &[f32],
) -> Vec<Shard> {
    let n = corpus.len() / dim;
    let cut = |i: usize| -> usize {
        if i >= n_shards {
            n
        } else {
            i * n / n_shards
        }
    };
    (0..n_shards)
        .map(|i| {
            let start = cut(i);
            let end = cut(i + 1).max(start);
            let mut index = seeded_index(dim, bit_width, shift, scale);
            index
                .add(&corpus[start * dim..end * dim], dim)
                .expect("generated vectors are valid");
            index.prepare().expect("backend prepare succeeds");
            Shard {
                index,
                slot_offset: start as u64,
            }
        })
        .collect()
}

/// The single-index reference: one index over the whole corpus, same
/// calibration.
pub fn build_monolithic(
    corpus: &[f32],
    dim: usize,
    bit_width: usize,
    shift: &[f32],
    scale: &[f32],
) -> VectorIndex {
    let mut index = seeded_index(dim, bit_width, shift, scale);
    index.add(corpus, dim).expect("generated vectors are valid");
    index.prepare().expect("backend prepare succeeds");
    index
}

/// Persist provider images as `<dir>/shard-<i>.vector` and print the
/// matching `[[shards]]` config entries (listen ports starting at
/// `base_port`, offsets from the partition layout) so a static cluster
/// config can be assembled by hand.
pub fn write_shards(
    shards: &[Shard],
    dir: &Path,
    base_port: u16,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    std::fs::create_dir_all(dir)?;
    let mut paths = Vec::with_capacity(shards.len());
    for (i, shard) in shards.iter().enumerate() {
        let path = dir.join(format!("shard-{i}.vector"));
        shard.index.write(&path).map_err(std::io::Error::other)?;
        println!(
            "[[shards]]\nlisten = \"0.0.0.0:{}\"\nindex = \"{}\"\nslot_offset = {}\n",
            base_port + i as u16,
            path.display(),
            shard.slot_offset
        );
        paths.push(path);
    }
    Ok(paths)
}

/// Accept stream for a tonic server with TCP_NODELAY set on every socket.
///
/// Without this, small gRPC writes (a shard's FloorUpdates, small Done
/// messages) can stall ~40ms on the Nagle/delayed-ACK interaction, which
/// dominates query latency on loopback and hurts on real networks too.
pub fn nodelay_incoming(
    listener: TcpListener,
) -> impl tokio_stream::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    use tokio_stream::StreamExt;
    TcpListenerStream::new(listener).map(|accepted| {
        accepted.inspect(|stream| {
            let _ = stream.set_nodelay(true);
        })
    })
}

/// Start a node server over a prebuilt index on 127.0.0.1:0. Returns its
/// `http://` address and the server task (abort to stop).
pub async fn start_node(
    index: VectorIndex,
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(Some(index), config, None).await
}

/// Start a node server with NO index (the from-scratch state: awaiting
/// SetCalibration or AddVectors).
pub async fn start_empty_node(
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(None, config, None).await
}

/// Start an empty node with the product-owned phrase vocabulary attached.
/// Reopen a persisted shard through [`NodeServiceImpl::open`] — the
/// recovery path the serving binary and the embedded runtime take — and
/// serve it. `start_empty_node` constructs a fresh shard instead.
pub async fn start_opened_node(
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let node = NodeServiceImpl::open(config, None, false).expect("reopen the shard");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    node.spawn_floor_listener(addr);
    let diagnostics = node.diagnostics_server(MAX_MESSAGE_BYTES);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES))
            .add_service(diagnostics)
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

pub async fn start_empty_phrase_node(
    config: NodeConfig,
    phrases: std::sync::Arc<crate::phrases::PhraseIndex>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(None, config, Some(phrases)).await
}

async fn start_node_inner(
    index: Option<VectorIndex>,
    config: NodeConfig,
    phrases: Option<std::sync::Arc<crate::phrases::PhraseIndex>>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    serve_node(NodeServiceImpl::new(index, config).with_phrase_index(phrases)).await
}

/// Serve an already-built node service on 127.0.0.1:0. Returns its
/// `http://` address and the server task (abort to stop).
pub async fn serve_node(node: NodeServiceImpl) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    // The UDP floor lane shares the gRPC listener's host:port.
    node.spawn_floor_listener(addr);
    let diagnostics = node.diagnostics_server(MAX_MESSAGE_BYTES);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES))
            .add_service(diagnostics)
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

/// Start a relay coordinator (`docs/relay-coordinators.md`) over
/// `children` on 127.0.0.1:0, its parent-facing UDP lane on the same
/// port, and return the relay handle beside the address so a test can
/// read its token registry. The children are not checked here: a test
/// that wants the startup refusal calls `check_children` itself.
pub async fn start_relay(
    children: Vec<String>,
) -> (
    String,
    crate::relay::RelayService,
    JoinHandle<Result<(), TransportError>>,
) {
    start_relay_over(CoordinatorServiceImpl::new(children)).await
}

/// [`start_relay`] over a coordinator the caller built, for a hot
/// topology the test then republishes.
pub async fn start_relay_over(
    coordinator: CoordinatorServiceImpl,
) -> (
    String,
    crate::relay::RelayService,
    JoinHandle<Result<(), TransportError>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let relay = crate::relay::RelayService::new(std::sync::Arc::new(coordinator));
    relay.spawn_floor_listener(addr);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(relay.clone().into_server(MAX_MESSAGE_BYTES))
            .add_service(relay.diagnostics_server(MAX_MESSAGE_BYTES))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), relay, handle)
}

/// Start a coordinator server on 127.0.0.1:0.
pub async fn start_coordinator(
    node_addrs: Vec<String>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(CoordinatorServiceImpl::into_server(
                CoordinatorServiceImpl::new(node_addrs),
                MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

// ---------------------------------------------------------------------------
// Mock analysis sidecar (test/dev fallback)
// ---------------------------------------------------------------------------

/// A mock `turbo.result.v1.AnalysisService`: deterministic, model-free
/// analysis with the same contract as `turbo-kserve` — whitespace tokens
/// with original-text UTF-8 spans, term vectors in first-occurrence order,
/// and a toy stemmer for the `SOURCE_STEMS` path. Used by the test suite
/// and as a deterministic loopback server when a model bundle is not needed.
pub mod mock_analysis {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::Stream;
    use tonic::transport::{Error as TransportError, Server};
    use tonic::{Request, Response, Status, Streaming};

    use turbo_client::proto::result::analysis_service_server::{
        AnalysisService, AnalysisServiceServer,
    };
    use turbo_client::proto::result::{
        analyze_progressive_event, analyze_stream_request, analyze_stream_response,
        AnalyzePipelineRequest, AnalyzeProgressiveEvent, AnalyzeQueryRequest, AnalyzeQueryResponse,
        AnalyzeStreamRequest, AnalyzeStreamResponse, CapabilitiesRequest, CapabilitiesResponse,
        ChunkLevelsResult, DocumentGraphRequest, DocumentGraphResponse, DocumentWindowRequest,
        DocumentWindowResponse, ExtractionResult, FormatDocumentRequest, FormatDocumentResponse,
        InspectDocumentGraphRequest, ListOutputFormatsRequest, ListOutputFormatsResponse,
        LoadBundleRequest, LoadBundleResponse, MultiResVectorsResult, ProcessingReceipt,
        ProfileCapabilities, ProjectChunksRequest, ProjectChunksResponse,
        ProjectSearchDocumentRequest, RecordsResult, RerankAndHighlightRequest,
        RerankAndHighlightResponse, RerankScoresResult, SearchDocumentProjection,
        SparseTermsResult, TokenVectorsResult, TypedDocumentResult, UnloadBundleRequest,
        UnloadBundleResponse,
    };

    use crate::analyzer::wire::{
        analyze_failure_from_status, MockAnalyzeResponse, WireGeoLocation, WireNoiseSpan,
        WireRegionVote, WireSessionOptions, WireSpan, WireTermVector, WireTextArtifact, WireToken,
        NO_DUAL_CASED_PROFILE, WIRE_OFFSET_UTF8,
    };
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
            }
        }
    }

    const GAZETTEER: [(&str, &str, &str, f64, f64, f64); 3] = [
        ("paris", "Paris", "FR", 48.8566, 2.3522, 0.9),
        ("berlin", "Berlin", "DE", 52.52, 13.405, 0.9),
        ("springfield", "Springfield", "US", 39.7817, -89.6501, 0.4),
    ];

    fn newline_sentences(text: &str) -> Vec<WireSpan> {
        let mut out = Vec::new();
        let mut cursor = 0usize;
        for line in text.split_inclusive(['\n', '\r']) {
            let body = line.trim_end_matches(['\n', '\r']);
            let lead = body.len() - body.trim_start().len();
            let trimmed = body.trim();
            if !trimmed.is_empty() {
                out.push(WireSpan {
                    start: (cursor + lead) as i32,
                    end: (cursor + lead + trimmed.len()) as i32,
                });
            }
            cursor += line.len();
        }
        out
    }

    fn analyze_text(
        text: &str,
        session: &WireSessionOptions,
        ner: bool,
        sentence_layer: bool,
        dual_identity: bool,
        calls: &AtomicU64,
    ) -> Result<MockAnalyzeResponse, Status> {
        calls.fetch_add(1, Ordering::SeqCst);
        if text.is_empty() {
            return Err(Status::invalid_argument("empty text"));
        }
        let mut tokens = Vec::new();
        let mut offset = 0usize;
        for word in text.split_whitespace() {
            let start = text[offset..].find(word).map(|i| offset + i).unwrap();
            let end = start + word.len();
            tokens.push(WireToken {
                span: Some(WireSpan {
                    start: start as i32,
                    end: end as i32,
                }),
                text: word.to_string(),
            });
            offset = end;
        }

        let (mode, source, steps, stemmer) = match &session.spec {
            Some(s) => (
                s.term_vector_mode,
                s.term_vector_source,
                s.char_filters.as_slice(),
                s.stemmer,
            ),
            None => (0, 0, &[][..], 0),
        };
        let stemming_on = stemmer > 1;
        let stems: Vec<String> = if stemming_on {
            tokens.iter().map(|t| toy_stem(&t.text)).collect()
        } else {
            Vec::new()
        };

        let mut term_vectors: Vec<WireTermVector> = Vec::new();
        let mut cased_term_vectors: Vec<WireTermVector> = Vec::new();
        {
            const SOURCE_STEMS: i32 = 2;
            const MODE_SCORING_ONLY: i32 = 2;
            const FULL_CASE_FOLD: i32 = 6;
            const CASE_FOLD: i32 = 16;
            if source == SOURCE_STEMS && !stemming_on {
                return Err(Status::invalid_argument("SOURCE_STEMS requires a stemmer"));
            }
            if session.layers.dual_cased && source == SOURCE_STEMS {
                return Err(Status::invalid_argument(
                    "dual_cased requires a step-chain term vector source; SOURCE_STEMS has no \
                     folded form to contrast with",
                ));
            }
            let folds = steps.is_empty()
                || steps
                    .iter()
                    .any(|step| *step == FULL_CASE_FOLD || *step == CASE_FOLD);
            let push =
                |vectors: &mut Vec<WireTermVector>, identity: String, span: WireSpan| match vectors
                    .iter_mut()
                    .find(|t| t.term == identity)
                {
                    Some(entry) => {
                        entry.frequency += 1;
                        if mode != MODE_SCORING_ONLY {
                            entry.occurrences.push(span);
                        }
                    }
                    None => vectors.push(WireTermVector {
                        term: identity,
                        frequency: 1,
                        occurrences: if mode == MODE_SCORING_ONLY {
                            Vec::new()
                        } else {
                            vec![span]
                        },
                    }),
                };
            for (i, token) in tokens.iter().enumerate() {
                let identity = if source == SOURCE_STEMS {
                    stems[i].clone()
                } else if folds {
                    token.text.to_lowercase()
                } else {
                    token.text.clone()
                };
                let span = token.span.unwrap();
                push(&mut term_vectors, identity, span);
                if session.layers.dual_cased && dual_identity {
                    push(&mut cased_term_vectors, token.text.clone(), span);
                }
            }
        }

        let noise = if session.layers.quality {
            tokens
                .iter()
                .filter(|t| !t.text.is_empty() && t.text.bytes().all(|b| b == b'#'))
                .map(|t| WireNoiseSpan {
                    span: t.span,
                    severity: "gibberish".to_string(),
                    score: (t.text.len() as f64 / 10.0).min(1.0),
                })
                .collect()
        } else {
            Vec::new()
        };
        let artifacts = if session.layers.quality {
            text.char_indices()
                .filter(|&(_, c)| c == '\u{FFFD}')
                .map(|(i, c)| WireTextArtifact {
                    span: Some(WireSpan {
                        start: i as i32,
                        end: (i + c.len_utf8()) as i32,
                    }),
                    kind: "replacement".to_string(),
                })
                .collect()
        } else {
            Vec::new()
        };

        let locations: Vec<WireGeoLocation> = if session.layers.geography && ner {
            tokens
                .iter()
                .filter_map(|t| {
                    let lower = t.text.to_lowercase();
                    GAZETTEER.iter().find(|(key, ..)| *key == lower).map(
                        |&(_, name, country, lat, lon, confidence)| WireGeoLocation {
                            span: t.span,
                            name: name.to_string(),
                            country_code: country.to_string(),
                            latitude: lat,
                            longitude: lon,
                            confidence,
                        },
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        let regions: Vec<WireRegionVote> = {
            let mut votes: Vec<WireRegionVote> = Vec::new();
            for location in &locations {
                match votes
                    .iter_mut()
                    .find(|v| v.country_code == location.country_code)
                {
                    Some(vote) => vote.share += 1.0,
                    None => votes.push(WireRegionVote {
                        country_code: location.country_code.clone(),
                        share: 1.0,
                    }),
                }
            }
            let total = locations.len() as f64;
            for vote in &mut votes {
                vote.share /= total;
            }
            votes.sort_by(|a, b| {
                b.share
                    .total_cmp(&a.share)
                    .then_with(|| a.country_code.cmp(&b.country_code))
            });
            votes
        };

        Ok(MockAnalyzeResponse {
            offset_unit: WIRE_OFFSET_UTF8,
            sentences: if session.layers.sentences && sentence_layer {
                newline_sentences(text)
            } else {
                Vec::new()
            },
            tokens,
            term_vectors,
            cased_term_vectors,
            noise,
            artifacts,
            locations,
            regions,
            entities: Vec::new(),
        })
    }

    #[tonic::async_trait]
    impl AnalysisService for MockAnalysis {
        type AnalyzeStreamStream =
            Pin<Box<dyn Stream<Item = Result<AnalyzeStreamResponse, Status>> + Send>>;
        type AnalyzeDocumentProgressiveStream =
            Pin<Box<dyn Stream<Item = Result<AnalyzeProgressiveEvent, Status>> + Send>>;

        async fn capabilities(
            &self,
            _request: Request<CapabilitiesRequest>,
        ) -> Result<Response<CapabilitiesResponse>, Status> {
            let mut profiles = vec![ProfileCapabilities {
                name: "default-v1".to_string(),
                version: "1".to_string(),
                steps: Vec::new(),
                layers: if self.ner {
                    vec!["turbo:entities".to_string(), "turbo:geo".to_string()]
                } else {
                    Vec::new()
                },
                required_bundles: Vec::new(),
            }];
            if !self.dual_identity {
                profiles.push(ProfileCapabilities {
                    name: NO_DUAL_CASED_PROFILE.to_string(),
                    version: "1".to_string(),
                    ..Default::default()
                });
            }
            Ok(Response::new(CapabilitiesResponse {
                bundles: Vec::new(),
                limits: None,
                server: None,
                profiles,
            }))
        }

        async fn analyze_document_progressive(
            &self,
            request: Request<AnalyzePipelineRequest>,
        ) -> Result<Response<Self::AnalyzeDocumentProgressiveStream>, Status> {
            self.unary_calls.fetch_add(1, Ordering::Relaxed);
            if let Some((delay, probe)) = &self.unary_delay {
                let call = DelayedCall::begin(probe.clone());
                tokio::time::sleep(*delay).await;
                call.complete();
            }
            let req = request.into_inner();
            let session = WireSessionOptions::from_binding(req.binding.as_ref())?;
            let mock_resp = analyze_text(
                &req.text,
                &session,
                self.ner,
                self.sentence_layer,
                self.dual_identity,
                &self.calls,
            )?;
            let complete = AnalyzeProgressiveEvent {
                sequence: 1,
                event: Some(analyze_progressive_event::Event::Complete(
                    DocumentGraphResponse {
                        graph: mock_resp.encode_graph().into(),
                        source_text: req.text,
                        ..Default::default()
                    },
                )),
            };
            let receipt = AnalyzeProgressiveEvent {
                sequence: 2,
                event: Some(analyze_progressive_event::Event::Receipt(
                    ProcessingReceipt::default(),
                )),
            };
            Ok(Response::new(Box::pin(tokio_stream::iter([
                Ok(complete),
                Ok(receipt),
            ]))))
        }

        async fn analyze_stream(
            &self,
            request: Request<Streaming<AnalyzeStreamRequest>>,
        ) -> Result<Response<Self::AnalyzeStreamStream>, Status> {
            if self.no_stream {
                return Err(Status::unimplemented("this server predates AnalyzeStream"));
            }
            self.stream_opens.fetch_add(1, Ordering::Relaxed);
            let mut inbound = request.into_inner();
            let ner = self.ner;
            let sentence_layer = self.sentence_layer;
            let dual_identity = self.dual_identity;
            let calls = self.calls.clone();
            let gate = self.stream_end_gate.clone();
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<AnalyzeStreamResponse, Status>>(16);
            tokio::spawn(async move {
                let mut saw_header = false;
                let mut held: Option<AnalyzeStreamResponse> = None;
                loop {
                    let message = match inbound.message().await {
                        Ok(Some(message)) => message,
                        Ok(None) => break,
                        Err(_) => return,
                    };
                    match message.kind {
                        Some(analyze_stream_request::Kind::Header(_)) => {
                            if saw_header {
                                let _ = tx
                                    .send(Err(Status::invalid_argument(
                                        "header may only be the first message of the stream",
                                    )))
                                    .await;
                                return;
                            }
                            saw_header = true;
                        }
                        Some(analyze_stream_request::Kind::Document(doc)) => {
                            if !saw_header {
                                let _ = tx
                                    .send(Err(Status::invalid_argument(
                                        "the first message of the stream must carry a header",
                                    )))
                                    .await;
                                return;
                            }
                            let outcome = match std::str::from_utf8(&doc.text)
                                .map_err(|_| Status::invalid_argument("document text is not UTF-8"))
                                .and_then(|text| {
                                    let session =
                                        WireSessionOptions::from_binding(doc.binding.as_ref())?;
                                    let ok = analyze_text(
                                        text,
                                        &session,
                                        ner,
                                        sentence_layer,
                                        dual_identity,
                                        &calls,
                                    )?;
                                    Ok(DocumentGraphResponse {
                                        graph: ok.encode_graph().into(),
                                        source_text: text.to_string(),
                                        ..Default::default()
                                    })
                                }) {
                                Ok(graph) => analyze_stream_response::Outcome::DocumentGraph(graph),
                                Err(status) => analyze_stream_response::Outcome::Failure(
                                    analyze_failure_from_status(status),
                                ),
                            };
                            let response = AnalyzeStreamResponse {
                                id: doc.id,
                                outcome: Some(outcome),
                                binding: doc.binding,
                            };
                            match held.take() {
                                None => held = Some(response),
                                Some(previous) => {
                                    if tx.send(Ok(response)).await.is_err()
                                        || tx.send(Ok(previous)).await.is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                        None => {
                            let _ = tx
                                .send(Err(Status::invalid_argument(
                                    "message carries neither header nor document",
                                )))
                                .await;
                            return;
                        }
                    }
                }
                if let Some(last) = held {
                    let _ = tx.send(Ok(last)).await;
                }
                if let Some(gate) = gate {
                    let permit = gate.acquire().await;
                    drop(permit);
                }
            });
            Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
        }

        async fn load_bundle(
            &self,
            _request: Request<LoadBundleRequest>,
        ) -> Result<Response<LoadBundleResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not serve LoadBundle",
            ))
        }

        async fn unload_bundle(
            &self,
            _request: Request<UnloadBundleRequest>,
        ) -> Result<Response<UnloadBundleResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not serve UnloadBundle",
            ))
        }

        async fn round_trip_sparse_terms(
            &self,
            req: Request<SparseTermsResult>,
        ) -> Result<Response<SparseTermsResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_token_vectors(
            &self,
            req: Request<TokenVectorsResult>,
        ) -> Result<Response<TokenVectorsResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_multi_res_vectors(
            &self,
            req: Request<MultiResVectorsResult>,
        ) -> Result<Response<MultiResVectorsResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_rerank_scores(
            &self,
            req: Request<RerankScoresResult>,
        ) -> Result<Response<RerankScoresResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_extraction(
            &self,
            req: Request<ExtractionResult>,
        ) -> Result<Response<ExtractionResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_records(
            &self,
            req: Request<RecordsResult>,
        ) -> Result<Response<RecordsResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_chunk_levels(
            &self,
            req: Request<ChunkLevelsResult>,
        ) -> Result<Response<ChunkLevelsResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn round_trip_document_graph(
            &self,
            _req: Request<DocumentGraphRequest>,
        ) -> Result<Response<DocumentGraphResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not round-trip TGRF",
            ))
        }

        async fn inspect_document_graph(
            &self,
            _req: Request<InspectDocumentGraphRequest>,
        ) -> Result<Response<TypedDocumentResult>, Status> {
            Err(Status::unimplemented("MockAnalysis does not inspect TGRF"))
        }

        async fn round_trip_typed_document(
            &self,
            req: Request<TypedDocumentResult>,
        ) -> Result<Response<TypedDocumentResult>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn project_search_document(
            &self,
            _req: Request<ProjectSearchDocumentRequest>,
        ) -> Result<Response<SearchDocumentProjection>, Status> {
            Err(Status::unimplemented("MockAnalysis does not project TGRF"))
        }

        async fn round_trip_search_projection(
            &self,
            req: Request<SearchDocumentProjection>,
        ) -> Result<Response<SearchDocumentProjection>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn analyze_query(
            &self,
            _req: Request<AnalyzeQueryRequest>,
        ) -> Result<Response<AnalyzeQueryResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not serve AnalyzeQuery",
            ))
        }

        async fn round_trip_analyze_query(
            &self,
            req: Request<AnalyzeQueryResponse>,
        ) -> Result<Response<AnalyzeQueryResponse>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn rerank_and_highlight(
            &self,
            req: Request<RerankAndHighlightRequest>,
        ) -> Result<Response<RerankAndHighlightResponse>, Status> {
            use turbo_client::proto::result::{
                ModelRef, RerankedCandidateProto, SemanticHighlightSpanProto,
            };

            let inner = req.into_inner();
            let q_tokens: Vec<String> = inner
                .query
                .split(|c: char| !c.is_alphanumeric())
                .filter(|t| !t.is_empty())
                .map(|t| t.to_lowercase())
                .collect();

            let mut results: Vec<RerankedCandidateProto> =
                Vec::with_capacity(inner.candidates.len());
            for (idx, cand) in inner.candidates.iter().enumerate() {
                let spans: Vec<(u32, u32)> = if !cand.sentence_span_start.is_empty()
                    && cand.sentence_span_start.len() == cand.sentence_span_end.len()
                {
                    cand.sentence_span_start
                        .iter()
                        .copied()
                        .zip(cand.sentence_span_end.iter().copied())
                        .collect()
                } else if !cand.text.is_empty() {
                    vec![(0, cand.text.len() as u32)]
                } else {
                    Vec::new()
                };

                let mut scored_spans: Vec<(u32, u32, f64)> = Vec::with_capacity(spans.len());
                let mut best = 0.0_f64;
                for &(s, e) in &spans {
                    let Some(slice) = cand.text.get(s as usize..e as usize) else {
                        continue;
                    };
                    let words: Vec<String> = slice
                        .split(|c: char| !c.is_alphanumeric())
                        .filter(|t| !t.is_empty())
                        .map(|t| t.to_lowercase())
                        .collect();
                    let overlap = if q_tokens.is_empty() {
                        0.5
                    } else {
                        let matched = q_tokens
                            .iter()
                            .filter(|qt| {
                                words.iter().any(|w| w == *qt || w.starts_with(qt.as_str()))
                            })
                            .count();
                        matched as f64 / q_tokens.len() as f64
                    };
                    best = best.max(overlap);
                    scored_spans.push((s, e, overlap));
                }
                scored_spans.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));

                let raw_logit = (3.0 * best - 1.0) as f32;
                let prob = (1.0 / (1.0 + (-f64::from(raw_logit)).exp())).clamp(0.0, 1.0);
                let max_snips = inner.max_snippets_per_candidate as usize;
                let highlights: Vec<SemanticHighlightSpanProto> = scored_spans
                    .into_iter()
                    .take(max_snips)
                    .filter_map(|(s, e, sc)| {
                        cand.text.get(s as usize..e as usize).map(|surface| {
                            SemanticHighlightSpanProto {
                                span_start: s,
                                span_end: e,
                                parent_span_start: cand.parent_offset + u64::from(s),
                                parent_span_end: cand.parent_offset + u64::from(e),
                                score: sc,
                                surface: surface.to_string(),
                            }
                        })
                    })
                    .collect();

                results.push(RerankedCandidateProto {
                    input_index: idx as u32,
                    doc_id: cand.doc_id.clone(),
                    raw_logit,
                    calibrated_probability: prob,
                    conformal_p_value: None,
                    highlights,
                    qa_abstained: false,
                    qa_null_probability: None,
                });
            }
            results.sort_by(|a, b| {
                b.calibrated_probability
                    .total_cmp(&a.calibrated_probability)
                    .then_with(|| a.input_index.cmp(&b.input_index))
            });

            Ok(Response::new(RerankAndHighlightResponse {
                query: inner.query,
                rerank_model: Some(ModelRef {
                    id: inner.rerank_model,
                    revision: "mock-v1".into(),
                    sha256: String::new(),
                }),
                highlight_model: None,
                offsets: inner.offsets,
                results,
            }))
        }

        async fn round_trip_rerank_and_highlight(
            &self,
            req: Request<RerankAndHighlightResponse>,
        ) -> Result<Response<RerankAndHighlightResponse>, Status> {
            Ok(Response::new(req.into_inner()))
        }

        async fn inspect_document_window(
            &self,
            _req: Request<DocumentWindowRequest>,
        ) -> Result<Response<DocumentWindowResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not serve InspectDocumentWindow",
            ))
        }

        async fn project_chunks(
            &self,
            _req: Request<ProjectChunksRequest>,
        ) -> Result<Response<ProjectChunksResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not serve ProjectChunks",
            ))
        }

        async fn list_output_formats(
            &self,
            _req: Request<ListOutputFormatsRequest>,
        ) -> Result<Response<ListOutputFormatsResponse>, Status> {
            Ok(Response::new(ListOutputFormatsResponse::default()))
        }

        async fn format_document(
            &self,
            _req: Request<FormatDocumentRequest>,
        ) -> Result<Response<FormatDocumentResponse>, Status> {
            Err(Status::unimplemented(
                "MockAnalysis does not serve FormatDocument",
            ))
        }
    }

    /// Start the mock on 127.0.0.1:0; returns its `http://` address.
    pub async fn start_mock_analysis() -> (String, JoinHandle<Result<(), TransportError>>) {
        start_mock(MockAnalysis::default()).await
    }

    /// Start a mock whose `AnalyzeStream` RPC answers `UNIMPLEMENTED`.
    pub async fn start_mock_analysis_without_stream(
    ) -> (String, JoinHandle<Result<(), TransportError>>) {
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
    pub async fn start_mock_analysis_without_ner(
    ) -> (String, JoinHandle<Result<(), TransportError>>) {
        start_mock(MockAnalysis {
            ner: false,
            ..Default::default()
        })
        .await
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
}

/// Spawn a `turbo-kserve` analysis server binary on `port` and wait for its
/// listener. Returns the child (kill on drop) and its `http://` address.
pub fn start_sidecar(binary: &str, port: u16) -> Result<(Child, String), String> {
    start_sidecar_with_env(binary, port, &[])
}

/// Like [`start_sidecar`], with extra environment variables (for
/// example `TURBO_EMBED_MODEL` or bundle paths).
pub fn start_sidecar_with_env(
    binary: &str,
    port: u16,
    envs: &[(&str, &str)],
) -> Result<(Child, String), String> {
    if !Path::new(binary).exists() {
        return Err(format!("sidecar binary not found at {binary}"));
    }
    let mut command = Command::new(binary);
    command
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().map_err(|e| format!("spawn sidecar: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok((child, format!("http://127.0.0.1:{port}")));
        }
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Err(format!("sidecar exited early: {status}"));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err("sidecar never opened its port in 30s".to_string())
}

/// A vector provider that advertises a configured ANN contract while
/// scoring through a real exhaustive image underneath. Tests use it to
/// exercise every AUTO/ANN rule the product has without a second
/// backend: the results are exact, the contract says approximate, and
/// the coordinator must believe the contract.
pub mod fake_ann {
    use std::path::Path;

    use crate::vector::{
        QualityContract, VectorBackendConfig, VectorBackendDescriptor, VectorCapability,
        VectorError, VectorIndex, VectorProvider, VectorSearchOptions, VectorSearchResults,
        VectorStreamBatch, VectorStreamControl, VectorStreamSummary,
    };

    pub const BACKEND_KIND: &str = "fake-ann";

    pub struct FakeAnn {
        inner: VectorIndex,
    }

    /// Wrap an exhaustive image as a fake ANN provider.
    pub fn fake_ann_index(inner: VectorIndex) -> VectorIndex {
        VectorIndex::from_provider(FakeAnn { inner })
    }

    /// The scoring fingerprint the fake advertises for an inner image.
    pub fn fingerprint_of(inner: &VectorIndex) -> String {
        format!("{BACKEND_KIND}:{}", inner.descriptor().scoring_fingerprint)
    }

    impl VectorProvider for FakeAnn {
        fn descriptor(&self) -> VectorBackendDescriptor {
            let inner = self.inner.descriptor();
            VectorBackendDescriptor {
                backend_kind: BACKEND_KIND.into(),
                backend_version: "test".into(),
                dimension: inner.dimension,
                bits_per_dimension: inner.bits_per_dimension,
                metric: inner.metric,
                score_direction: inner.score_direction,
                scoring_fingerprint: fingerprint_of(&self.inner),
                quality_contract: QualityContract::ConfiguredAnn,
                capabilities: vec![VectorCapability::BatchQuery],
            }
        }

        fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
            let mut config = self.inner.backend_config()?;
            config.backend_kind = BACKEND_KIND.into();
            Ok(config)
        }

        fn len(&self) -> usize {
            self.inner.len()
        }

        fn dimension(&self) -> Option<usize> {
            self.inner.dim_opt()
        }

        fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
            self.inner.add(vectors, dimension)
        }

        fn prepare(&mut self) -> Result<(), VectorError> {
            self.inner.prepare()
        }

        fn write(&self, path: &Path) -> Result<(), VectorError> {
            self.inner.write(path)
        }

        fn row_transcripts(
            &self,
            rows: std::ops::Range<usize>,
            sink: &mut dyn FnMut(usize, &[u8]),
        ) -> Result<(), VectorError> {
            self.inner.row_transcripts(rows, sink)
        }

        fn representation_materialized(&self) -> Option<bool> {
            self.inner.representation_materialized()
        }

        fn search(
            &self,
            queries: &[f32],
            k: usize,
            options: VectorSearchOptions<'_>,
        ) -> Result<VectorSearchResults, VectorError> {
            self.inner.try_search(queries, k, options)
        }

        fn search_streaming_controlled(
            &self,
            queries: &[f32],
            options: VectorSearchOptions<'_>,
            sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            self.inner
                .try_search_streaming_controlled(queries, options, sink, control)
        }
    }
}
