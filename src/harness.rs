//! Shared harness: deterministic corpus generation, calibration fitting,
//! shard partitioning, and loopback server startup.
//!
//! Used by the integration tests and the `sweep` benchmark binary. Also
//! usable for real deployments: [`write_shards`] persists sharded,
//! uniformly-calibrated `.tv` files that the `turbovec-search` binary loads
//! via `[[shards]]` entries in the cluster config.

use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Error as TransportError, Server};
use turbovec::TurboQuantIndex;

use crate::coordinator::CoordinatorServiceImpl;
use crate::node::{NodeConfig, NodeServiceImpl};
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

/// Fit a TQ+ calibration on a representative sample: build a throwaway
/// index from the sample and read out its locked (shift, scale).
pub fn fit_calibration(dim: usize, bit_width: usize, sample: &[f32]) -> (Vec<f32>, Vec<f32>) {
    // Upstream turbovec fits no calibration below ~1000 vectors (TQ+
    // warm-up): quantile estimates on fewer samples are noise. Mirror
    // that here as an explicit identity calibration — the same coordinate
    // system upstream serves during warm-up — but say so, because on a
    // real corpus an identity fit means the sampling is broken.
    let n = sample.len() / dim;
    if n < 1000 {
        eprintln!(
            "fit_calibration: sample of {n} vectors is below the TQ+ warm-up \
             threshold (1000); using identity calibration. Fine for tiny test \
             corpora, wrong for real ones: widen the sample."
        );
        return (vec![0.0; dim], vec![1.0; dim]);
    }
    let mut fitting = TurboQuantIndex::new(dim, bit_width).unwrap();
    fitting.add(sample);
    let (shift, scale) = fitting.calibration().expect("first add fits calibration");
    (shift.to_vec(), scale.to_vec())
}

/// One shard's index plus its global id base (the corpus offset of its
/// first vector; partitions are contiguous ranges).
pub struct Shard {
    pub index: TurboQuantIndex,
    pub slot_offset: u64,
}

/// Build `n_shards` indexes over contiguous, disjoint partitions of
/// `corpus`, all seeded with the same calibration — the property that makes
/// their scores mutually comparable.
///
/// Cuts are aligned to the engine's calibration block
/// ([`turbovec::DEFAULT_BLOCK_SIZE`] rows): per-block calibration fits
/// each sealed block on exactly its own rows, so distributed ==
/// monolithic stays BITWISE only when every shard's sealed blocks hold
/// exactly the rows the monolithic build seals together. Corpora at or
/// under one block are unaffected (nothing seals; the seed governs
/// every row), so small-corpus tests keep their naive cuts.
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
        if i == 0 {
            return 0;
        }
        if i >= n_shards {
            return n;
        }
        let naive = i * n / n_shards;
        let block = turbovec::DEFAULT_BLOCK_SIZE;
        if n <= block {
            return naive;
        }
        ((naive + block / 2) / block * block).min(n)
    };
    (0..n_shards)
        .map(|i| {
            let start = cut(i);
            let end = cut(i + 1).max(start);
            let mut index =
                TurboQuantIndex::new_with_calibration(dim, bit_width, shift, scale).unwrap();
            index.add(&corpus[start * dim..end * dim]);
            index.prepare();
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
) -> TurboQuantIndex {
    let mut index = TurboQuantIndex::new_with_calibration(dim, bit_width, shift, scale).unwrap();
    index.add(corpus);
    index.prepare();
    index
}

/// Persist shards as `.tv` files (`<dir>/shard-<i>.tv`) and print the
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
        let path = dir.join(format!("shard-{i}.tv"));
        shard.index.write(&path)?;
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
    index: TurboQuantIndex,
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(Some(index), config).await
}

/// Start a node server with NO index (the from-scratch state: awaiting
/// SetCalibration or AddVectors).
pub async fn start_empty_node(
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(None, config).await
}

async fn start_node_inner(
    index: Option<TurboQuantIndex>,
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let node = NodeServiceImpl::new(index, config);
    // The UDP floor lane shares the gRPC listener's host:port.
    node.spawn_floor_listener(addr);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
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

/// A mock AnalysisService: deterministic, model-free analysis with the
/// same contract as the real sidecar — whitespace tokens with
/// original-text spans, term vectors in first-occurrence order, and a toy
/// stemmer for the SOURCE_STEMS path. Used by the test suite and as a
/// documented fallback when the real native sidecar is unavailable.
/// Corpora are ASCII, so byte offsets == char offsets == UTF-16 units.
pub mod mock_analysis {
    use std::pin::Pin;

    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::ReceiverStream;
    use tokio_stream::Stream;
    use tonic::transport::{Error as TransportError, Server};
    use tonic::{Request, Response, Status, Streaming};

    use crate::pb::analysis::analysis_service_server::{AnalysisService, AnalysisServiceServer};
    use crate::pb::analysis::{
        analyze_stream_request, analyze_stream_response, AnalysisOptions, AnalyzeRequest,
        AnalyzeResponse, AnalyzeStreamError, AnalyzeStreamRequest, AnalyzeStreamResponse,
        GetCapabilitiesRequest, GetCapabilitiesResponse, Span, TermVector, Token,
    };
    use crate::MAX_MESSAGE_BYTES;

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

    /// The mock service: canned-but-faithful analysis.
    #[derive(Default)]
    pub struct MockAnalysis;

    /// The analysis itself, shared verbatim by the unary and streaming
    /// paths — the same guarantee the real sidecar tests prove.
    fn analyze_text(text: &str, options: &AnalysisOptions) -> Result<AnalyzeResponse, Status> {
        if text.is_empty() {
            return Err(Status::invalid_argument("empty text"));
        }
        // Whitespace tokenize with original-text spans.
            let mut tokens = Vec::new();
            let mut offset = 0usize;
            for word in text.split_whitespace() {
                let start = text[offset..].find(word).map(|i| offset + i).unwrap();
                let end = start + word.len();
                tokens.push(Token {
                    span: Some(Span {
                        start: start as i32,
                        end: end as i32,
                    }),
                    text: word.to_string(),
                    pos: String::new(),
                });
                offset = end;
            }

            let stemming_on = options.stemmer > 1;
            let stems: Vec<String> = if stemming_on {
                tokens.iter().map(|t| toy_stem(&t.text)).collect()
            } else {
                Vec::new()
            };

            let mut term_vectors: Vec<TermVector> = Vec::new();
            if let Some(tv) = &options.term_vectors {
                if tv.enabled {
                    const SOURCE_STEMS: i32 = 2;
                    const MODE_SCORING_ONLY: i32 = 2;
                    if tv.source == SOURCE_STEMS && !stemming_on {
                        return Err(Status::invalid_argument("SOURCE_STEMS requires a stemmer"));
                    }
                    for (i, token) in tokens.iter().enumerate() {
                        let identity = if tv.source == SOURCE_STEMS {
                            stems[i].clone()
                        } else {
                            token.text.to_lowercase()
                        };
                        let span = token.span.unwrap();
                        match term_vectors.iter_mut().find(|t| t.term == identity) {
                            Some(entry) => {
                                entry.frequency += 1;
                                if tv.mode != MODE_SCORING_ONLY {
                                    entry.occurrences.push(span);
                                }
                            }
                            None => term_vectors.push(TermVector {
                                term: identity,
                                frequency: 1,
                                occurrences: if tv.mode == MODE_SCORING_ONLY {
                                    Vec::new()
                                } else {
                                    vec![span]
                                },
                            }),
                        }
                    }
                }
            }

            Ok(AnalyzeResponse {
                sentences: Vec::new(),
                tokens,
                stems,
                entities: Vec::new(),
                term_vectors,
                embeddings: Vec::new(),
                warnings: Vec::new(),
                lemmas: Vec::new(),
            })
    }

    #[tonic::async_trait]
    impl AnalysisService for MockAnalysis {
        async fn analyze(
            &self,
            request: Request<AnalyzeRequest>,
        ) -> Result<Response<AnalyzeResponse>, Status> {
            let req = request.into_inner();
            let options = req.options.unwrap_or_default();
            Ok(Response::new(analyze_text(&req.text, &options)?))
        }

        type AnalyzeStreamStream =
            Pin<Box<dyn Stream<Item = Result<AnalyzeStreamResponse, Status>> + Send>>;

        /// The streaming contract of the real sidecar: options must come
        /// first (once), per-document failures come back as error results
        /// on their sequence, and responses are delivered DELIBERATELY out
        /// of request order (each pair swapped) so client reorder logic is
        /// actually exercised by the tests.
        async fn analyze_stream(
            &self,
            request: Request<Streaming<AnalyzeStreamRequest>>,
        ) -> Result<Response<Self::AnalyzeStreamStream>, Status> {
            let mut inbound = request.into_inner();
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<AnalyzeStreamResponse, Status>>(16);
            tokio::spawn(async move {
                let mut options: Option<AnalysisOptions> = None;
                let mut held: Option<AnalyzeStreamResponse> = None;
                loop {
                    let message = match inbound.message().await {
                        Ok(Some(message)) => message,
                        Ok(None) => break,
                        Err(_) => return,
                    };
                    match message.msg {
                        Some(analyze_stream_request::Msg::Options(o)) => {
                            if options.is_some() {
                                let _ = tx
                                    .send(Err(Status::invalid_argument(
                                        "options may only be the first message of the stream",
                                    )))
                                    .await;
                                return;
                            }
                            options = Some(o);
                        }
                        Some(analyze_stream_request::Msg::Doc(doc)) => {
                            let Some(options) = options.as_ref() else {
                                let _ = tx
                                    .send(Err(Status::invalid_argument(
                                        "the first message of the stream must carry options",
                                    )))
                                    .await;
                                return;
                            };
                            let result = match analyze_text(&doc.text, options) {
                                Ok(ok) => analyze_stream_response::Result::Ok(ok),
                                Err(status) => analyze_stream_response::Result::Error(
                                    AnalyzeStreamError {
                                        code: status.code() as i32,
                                        message: status.message().to_string(),
                                    },
                                ),
                            };
                            let response = AnalyzeStreamResponse {
                                sequence: doc.sequence,
                                result: Some(result),
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
                                    "message carries neither options nor doc",
                                )))
                                .await;
                            return;
                        }
                    }
                }
                if let Some(last) = held {
                    let _ = tx.send(Ok(last)).await;
                }
            });
            Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
        }

        async fn get_capabilities(
            &self,
            _request: Request<GetCapabilitiesRequest>,
        ) -> Result<Response<GetCapabilitiesResponse>, Status> {
            Ok(Response::new(GetCapabilitiesResponse::default()))
        }
    }

    /// Start the mock on 127.0.0.1:0; returns its `http://` address.
    pub async fn start_mock_analysis() -> (String, JoinHandle<Result<(), TransportError>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(
            Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
                .add_service(
                    AnalysisServiceServer::new(MockAnalysis)
                        .max_decoding_message_size(MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(MAX_MESSAGE_BYTES),
                )
                .serve_with_incoming(crate::harness::nodelay_incoming(listener)),
        );
        (format!("http://{addr}"), handle)
    }
}

/// Spawn the native OpenNLP analysis sidecar on `port` and wait for its
/// listener. Returns the child (kill on drop) and its `http://` address.
/// Used by binaries that run against the sidecar; tests use the mock.
pub fn start_sidecar(binary: &str, port: u16) -> Result<(Child, String), String> {
    start_sidecar_with_env(binary, port, &[])
}

/// Like [`start_sidecar`], with extra environment variables (for
/// example `OPENNLP_EMBEDDINGS_DIR` to enable static embeddings).
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
