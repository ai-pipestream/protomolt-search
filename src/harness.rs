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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let node = NodeServiceImpl::new(index, config).with_phrase_index(phrases);
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
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

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
        GeoLocation, GetCapabilitiesRequest, GetCapabilitiesResponse, NoiseSpan, RegionVote, Span,
        TermVector, TextArtifact, Token,
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

    /// The mock service: canned-but-faithful analysis.
    pub struct MockAnalysis {
        /// Whether this mock models a sidecar with an NER model
        /// configured — the capability the geography preflight checks.
        /// Defaults to true; [`start_mock_analysis_without_ner`] models
        /// the bare sidecar.
        pub ner: bool,
        /// Whether this mock serves the sentence layer when asked
        /// (`sentence_detection`). Defaults to true;
        /// [`start_mock_analysis_without_sentences`] models a sidecar
        /// that ignores the request — the state a sentence field's
        /// ingest refuses by name (docs/highlighting.md).
        pub sentence_layer: bool,
        /// Analysis calls this mock served, unary and streaming alike,
        /// one per document: the meter that proves a query path added
        /// no analysis (docs/highlighting.md).
        pub calls: Arc<AtomicU64>,
        unary_delay: Option<(Duration, AnalysisDelayProbe)>,
    }

    impl Default for MockAnalysis {
        fn default() -> Self {
            MockAnalysis {
                ner: true,
                sentence_layer: true,
                calls: Arc::new(AtomicU64::new(0)),
                unary_delay: None,
            }
        }
    }

    /// The mock's toy gazetteer: a lowercased token matching an entry
    /// is a location mention. Deterministic, so tests compute expected
    /// column values. Springfield's low confidence models gazetteer
    /// ambiguity (the United States has dozens).
    const GAZETTEER: [(&str, &str, &str, f64, f64, f64); 3] = [
        ("paris", "Paris", "FR", 48.8566, 2.3522, 0.9),
        ("berlin", "Berlin", "DE", 52.52, 13.405, 0.9),
        ("springfield", "Springfield", "US", 39.7817, -89.6501, 0.4),
    ];

    /// The sidecar's model-free sentence detector: one span per line
    /// holding a non-whitespace character, trimmed, in the same
    /// coordinates as the tokens.
    fn newline_sentences(text: &str) -> Vec<Span> {
        let mut out = Vec::new();
        let mut cursor = 0usize;
        for line in text.split_inclusive(['\n', '\r']) {
            let body = line.trim_end_matches(['\n', '\r']);
            let lead = body.len() - body.trim_start().len();
            let trimmed = body.trim();
            if !trimmed.is_empty() {
                out.push(Span {
                    start: (cursor + lead) as i32,
                    end: (cursor + lead + trimmed.len()) as i32,
                });
            }
            cursor += line.len();
        }
        out
    }

    /// The analysis itself, shared verbatim by the unary and streaming
    /// paths — the same guarantee the real sidecar tests prove.
    fn analyze_text(
        text: &str,
        options: &AnalysisOptions,
        ner: bool,
        sentence_layer: bool,
        calls: &AtomicU64,
    ) -> Result<AnalyzeResponse, Status> {
        calls.fetch_add(1, Ordering::SeqCst);
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

        // Quality layers, emitted only when requested (the real
        // sidecar's contract: "empty unless noise was requested" would
        // otherwise be indistinguishable from "clean"). The rules are
        // deterministic so tests can compute expected column values:
        // a token made entirely of '#' is a noise finding scored
        // len/10 capped at 1.0, and every U+FFFD in the text is a
        // "replacement" artifact.
        let noise = if options.noise {
            tokens
                .iter()
                .filter(|t| !t.text.is_empty() && t.text.bytes().all(|b| b == b'#'))
                .map(|t| NoiseSpan {
                    span: t.span,
                    severity: "gibberish".to_string(),
                    score: (t.text.len() as f64 / 10.0).min(1.0),
                })
                .collect()
        } else {
            Vec::new()
        };
        let artifacts = if options.artifacts {
            text.char_indices()
                .filter(|&(_, c)| c == '\u{FFFD}')
                .map(|(i, c)| TextArtifact {
                    span: Some(Span {
                        start: i as i32,
                        end: (i + c.len_utf8()) as i32,
                    }),
                    r#type: "replacement".to_string(),
                })
                .collect()
        } else {
            Vec::new()
        };

        // The geocoding layer, emitted only when requested AND the mock
        // models an NER-configured sidecar (the real one returns empty
        // layers plus a warning without a model — the state the
        // engine's preflight exists to refuse). Locations in text
        // order; region votes are per-country evidence shares, ranked
        // by share descending, country code ascending on ties.
        let locations: Vec<GeoLocation> = if options.geo && ner {
            tokens
                .iter()
                .filter_map(|t| {
                    let lower = t.text.to_lowercase();
                    GAZETTEER.iter().find(|(key, ..)| *key == lower).map(
                        |&(_, name, country, lat, lon, confidence)| GeoLocation {
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
        let regions: Vec<RegionVote> = {
            let mut votes: Vec<RegionVote> = Vec::new();
            for location in &locations {
                match votes
                    .iter_mut()
                    .find(|v| v.country_code == location.country_code)
                {
                    Some(vote) => vote.share += 1.0,
                    None => votes.push(RegionVote {
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

        Ok(AnalyzeResponse {
            sentences: if options.sentence_detection && sentence_layer {
                newline_sentences(text)
            } else {
                Vec::new()
            },
            tokens,
            stems,
            entities: Vec::new(),
            term_vectors,
            embeddings: Vec::new(),
            warnings: Vec::new(),
            lemmas: Vec::new(),
            noise,
            artifacts,
            locations,
            regions,
            // The rest of the sidecar's tier-1 surface (glossary, pii,
            // coref, dependencies, relations, geo). The mock models term
            // identity and the quality layers only, so it returns none
            // of it rather than inventing plausible-looking annotations.
            ..Default::default()
        })
    }

    #[tonic::async_trait]
    impl AnalysisService for MockAnalysis {
        async fn analyze(
            &self,
            request: Request<AnalyzeRequest>,
        ) -> Result<Response<AnalyzeResponse>, Status> {
            if let Some((delay, probe)) = &self.unary_delay {
                let call = DelayedCall::begin(probe.clone());
                tokio::time::sleep(*delay).await;
                call.complete();
            }
            let req = request.into_inner();
            let options = req.options.unwrap_or_default();
            Ok(Response::new(analyze_text(
                &req.text,
                &options,
                self.ner,
                self.sentence_layer,
                &self.calls,
            )?))
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
            let ner = self.ner;
            let sentence_layer = self.sentence_layer;
            let calls = self.calls.clone();
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
                            let result =
                                match analyze_text(&doc.text, options, ner, sentence_layer, &calls)
                                {
                                    Ok(ok) => analyze_stream_response::Result::Ok(ok),
                                    Err(status) => {
                                        analyze_stream_response::Result::Error(AnalyzeStreamError {
                                            code: status.code() as i32,
                                            message: status.message().to_string(),
                                        })
                                    }
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
            Ok(Response::new(GetCapabilitiesResponse {
                ner_available: self.ner,
                ..Default::default()
            }))
        }
    }

    /// Start the mock on 127.0.0.1:0; returns its `http://` address.
    pub async fn start_mock_analysis() -> (String, JoinHandle<Result<(), TransportError>>) {
        start_mock(MockAnalysis::default()).await
    }

    /// A normal mock whose unary `Analyze` method pauses for `delay`.
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

    /// [`start_mock_analysis`] modeling a sidecar with NO NER model
    /// configured: `GetCapabilities.ner_available` is false, and the
    /// geocoding layer stays empty even when requested — the state the
    /// engine's geography preflight exists to refuse.
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
