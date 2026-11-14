//! End-to-end `#105` Stage-2 calibrated reranking, semantic highlighting,
//! security/priority/deadline propagation, and `TurboVocabularyService`
//! feedback loop integration tests:
//! 1. Stage-2 calibrated `RerankAndHighlight` (`DeterministicCalibrated` and
//!    `Remote` over HTTP/2 gRPC `AnalysisService`) reordering pure `DenseQuery`
//!    and hybrid `CompositeSearchStrategy` (`RrfScore`) shortlists into
//!    calibrated `[0.0, 1.0]` probabilities, populating `QueryHit.snippets` on
//!    pure dense hits with zero lexical matches, and composing
//!    `DocLineage.span_start` parent-document UTF-8 byte offsets.
//! 2. `KserveCallPolicy` bearer auth (`authorization: Bearer ...`), mTLS
//!    material (`MtlsClientMaterial`), priority lanes (`x-turbo-priority: bulk`
//!    vs `interactive`), `finish_deadline_us` -> `grpc-timeout` / expired
//!    `DeadlineExceeded` refusal, and runtime-scoped `RemoteChannelPool`
//!    eviction when a Tokio runtime drops.
//! 3. `VocabularyListener` -> `export_corpus_statistics_to_kserve`
//!    (`ImportCorpusStatistics` -> `TCIDF001` `VocabularyCorpusIdf`) and
//!    `train_voronoi_centroids_on_kserve` (`TrainVoronoiCentroids` ->
//!    `TCENT001` spherical k-means Voronoi routing centroids).

mod common;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::mock::start_mock_analysis;
use common::{fit_calibration, unit_vectors};
use pipestream_search::analyzer::{body_spec, TURBO_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::storage::VocabChannel;
use pipestream_search::pb::{
    search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, CompositeSearchStrategy, DenseQuery, DocLineage, HighlightSpec,
    LexicalQuery, QueryRequest, RrfScore, SearchQuery, SelectionOperator, SelectionQuery,
    SelectionScoreStrategy, SetCalibrationRequest,
};
use pipestream_search::placement::{train_voronoi_centroids_on_kserve, VoronoiTrainingSpec};
use pipestream_search::turbo_bridge::rerank::{
    rerank_and_highlight_shortlist, RerankBackend, RerankCandidate, RerankSpec,
};
use pipestream_search::turbo_bridge::{KserveCallPolicy, MtlsClientMaterial};
use pipestream_search::vocab::{export_corpus_statistics_to_kserve, VocabularyListener};
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status, Streaming};
use turbo_client::proto::vocabulary::turbo_vocabulary_service_server::{
    TurboVocabularyService, TurboVocabularyServiceServer,
};
use turbo_client::proto::vocabulary::{
    import_corpus_statistics_request, train_voronoi_centroids_request, BuildSuggestLexiconRequest,
    CorpusStatisticsArtifactDescriptor, DeleteVocabularyArtifactRequest,
    DeleteVocabularyArtifactResponse, DictionaryArtifactDescriptor, DownloadVocabularyRequest,
    ImportCorpusStatisticsRequest, ImportDictionaryRequest, LearnVocabularyRequest,
    ListCorpusStatisticsRequest, ListCorpusStatisticsResponse, ListDictionariesRequest,
    ListDictionariesResponse, ListDictionaryFormatsRequest, ListDictionaryFormatsResponse,
    ListSuggestLexiconsRequest, ListSuggestLexiconsResponse, ListVocabulariesRequest,
    ListVocabulariesResponse, ListVoronoiCentroidsRequest, ListVoronoiCentroidsResponse,
    LookupStemSurfacesRequest, LookupStemSurfacesResponse, SpellcheckQueryRequest,
    SpellcheckQueryResponse, SuggestLexiconArtifactDescriptor, SuggestPrefixRequest,
    SuggestPrefixResponse, TrainVoronoiCentroidsRequest, VocabularyArtifactChunk,
    VocabularyArtifactDescriptor, VoronoiCentroidArtifactDescriptor,
};
use turbo_client::{
    RemoteChannelPool, RemoteClient, RemoteEndpointConfig, RemotePriority, VocabularyCorpusIdf,
};

const DIM: usize = 32;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "turboembed_rerank_ops_{tag}_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage2_rerank_and_highlight_reorders_dense_and_composite_and_composes_lineage_offsets() {
    let (mock_addr, mock_handle) = start_mock_analysis().await;

    // Build 4 vectors where Doc 0 has the closest vector to our query (`corpus_vecs[0]`),
    // while Doc 1 has a blended vector (`0.82 * v0 + 0.18 * v1`, normalized) so Doc 1 enters
    // the Stage-1 top-4 shortlist behind Doc 0, then wins Stage-2 cross-encoder reranking!
    let mut corpus_vecs = unit_vectors(4, DIM, 0x1050_CAFE);
    let mut blended = vec![0.0_f32; DIM];
    let mut sq = 0.0_f32;
    for d in 0..DIM {
        let v = 0.82 * corpus_vecs[d] + 0.18 * corpus_vecs[DIM + d];
        blended[d] = v;
        sq += v * v;
    }
    let inv = 1.0 / sq.sqrt();
    for (dst, v) in corpus_vecs[DIM..2 * DIM].iter_mut().zip(blended) {
        *dst = v * inv;
    }
    let (shift, scale) = fit_calibration(DIM, 4, &corpus_vecs);

    // Doc 1 carries `DocLineage { span_start: 1000 }` and two sentences; sentence 2 has exact
    // semantic overlap with `"constitutional appellate jurisdiction maritime appeals"`.
    let docs = [
        ("Industrial boiler valve pressure inspection log.", 0u32),
        (
            "Introductory docket header. The appellate court affirmed constitutional jurisdiction over maritime appeals.",
            1000u32,
        ),
        ("Municipal street lighting maintenance schedule.", 0u32),
        ("Agricultural grain elevator storage inventory.", 0u32),
    ];

    let mut nodes = Vec::new();
    for shard in 0..2usize {
        let node = Arc::new(NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: (shard * 2) as u64,
                analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
                sentence_fields: vec!["body".into()],
                ..Default::default()
            },
        ));
        let mut link = NodeLink::local(Arc::clone(&node));
        link.set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap();

        let doc_reqs: Vec<AddDocumentsRequest> = (0..2usize)
            .map(|local| {
                let id = shard * 2 + local;
                let (text, span_start) = docs[id];
                AddDocumentsRequest {
                    text: text.to_string(),
                    analysis: Some(body_spec()),
                    lineage: Some(DocLineage {
                        parent_id: 77,
                        group_id: 1,
                        span_start,
                        span_end: span_start + text.len() as u32,
                    }),
                    ..Default::default()
                }
            })
            .collect();
        link.add_documents(tokio_stream::iter(doc_reqs))
            .await
            .unwrap();

        let start = shard * 2 * DIM;
        link.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            vectors: corpus_vecs[start..start + 2 * DIM].to_vec(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();

        nodes.push(node);
    }

    let base_coord = CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default());
    let q_vec = corpus_vecs[..DIM].to_vec();

    // Baseline WITHOUT Stage-2 rerank: Doc 0 wins pure dense search (exact vector match) and has empty snippets!
    let baseline = SearchService::query(
        &base_coord,
        Request::new(QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "vec".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        vector: q_vec.clone(),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(baseline.hits[0].doc_id, 0);
    assert!(baseline.hits[0].snippets.is_empty());

    // Test BOTH `DeterministicCalibrated` AND `Remote` (`turbo-kserve` HTTP/2 `RerankAndHighlight`)!
    let remote_client = RemoteClient::new(
        RemoteEndpointConfig::new(mock_addr).with_priority(RemotePriority::Interactive),
    );
    let backends = vec![
        (
            "deterministic",
            RerankBackend::DeterministicCalibrated {
                platt_slope: 2.2,
                platt_intercept: -0.4,
            },
        ),
        (
            "remote_kserve",
            RerankBackend::Remote {
                client: Box::new(remote_client),
                rerank_model: "bge-reranker-v2-m3".into(),
                embed_model: String::new(),
                qa_model: String::new(),
                temperature: 1.0,
            },
        ),
    ];

    for (label, backend) in backends {
        let mut spec = RerankSpec::new(backend);
        spec.shortlist_size = 4;
        spec.highlight_top_k = 4;
        spec.max_snippets_per_candidate = 2;
        spec.compose_parent_lineage_offsets = true;
        spec.query_text_override =
            Some("constitutional appellate jurisdiction maritime appeals".into());

        let rerank_coord = base_coord.clone().with_rerank_spec(spec);

        // 1. Pure `DenseQuery` (zero lexical postings matched!):
        //    - Doc 1 is promoted to Rank 0 above Doc 0!
        //    - Calibrated scores lie strictly in `[0.0, 1.0]`.
        //    - `QueryHit.snippets` is populated on pure dense hits with `DocLineage.span_start` (`1000`) composed!
        let dense_reranked = SearchService::query(
            &rerank_coord,
            Request::new(QueryRequest {
                k: 4,
                selection_k: 4,
                highlight: Some(HighlightSpec::default()),
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Search(SearchQuery {
                        id: "vec".into(),
                        query: Some(search_query::Query::Dense(DenseQuery {
                            vector: q_vec.clone(),
                            ..Default::default()
                        })),
                    })),
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(
            dense_reranked.executed.contains("turbo_rerank"),
            "{label}: executed={}",
            dense_reranked.executed
        );
        assert_eq!(
            dense_reranked.hits[0].doc_id, 1,
            "{label}: expected Doc 1 promoted above Doc 0"
        );
        for hit in &dense_reranked.hits {
            assert!(
                (0.0..=1.0).contains(&hit.score),
                "{label}: uncalibrated score {}",
                hit.score
            );
        }
        let top_snips = &dense_reranked.hits[0].snippets;
        assert!(
            !top_snips.is_empty(),
            "{label}: pure dense hit must carry semantic snippets"
        );
        let snip = &top_snips[0];
        assert!(
            snip.start >= 1000 && snip.end > snip.start,
            "{label}: expected parent lineage offset >= 1000, got {}..{}",
            snip.start,
            snip.end
        );
        let local_start = (snip.start - 1000) as usize;
        let local_end = (snip.end - 1000) as usize;
        assert_eq!(&docs[1].0[local_start..local_end], snip.text.as_str());
        assert!(snip.text.contains("jurisdiction") || snip.text.contains("appellate"));
        assert!(!snip.highlights.is_empty());
        assert!(snip.highlights[0].start >= snip.start && snip.highlights[0].end <= snip.end);

        // 2. Hybrid `CompositeSearchStrategy` (`RrfScore` + `SelectionOperator::Or`):
        let hybrid_reranked = SearchService::query(
            &rerank_coord,
            Request::new(QueryRequest {
                k: 4,
                selection_k: 4,
                highlight: Some(HighlightSpec::default()),
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                        operator: SelectionOperator::Or as i32,
                        clauses: vec![
                            SelectionQuery {
                                node: Some(selection_query::Node::Search(SearchQuery {
                                    id: "vec".into(),
                                    query: Some(search_query::Query::Dense(DenseQuery {
                                        vector: q_vec.clone(),
                                        ..Default::default()
                                    })),
                                })),
                            },
                            SelectionQuery {
                                node: Some(selection_query::Node::Search(SearchQuery {
                                    id: "lex".into(),
                                    query: Some(search_query::Query::Lexical(LexicalQuery {
                                        text: "boiler inspection".into(),
                                        analysis: Some(body_spec()),
                                        ..Default::default()
                                    })),
                                })),
                            },
                        ],
                        scoring: Some(SelectionScoreStrategy {
                            strategy: Some(selection_score_strategy::Strategy::Rrf(
                                RrfScore::default(),
                            )),
                        }),
                    })),
                }),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert!(hybrid_reranked.executed.contains("turbo_rerank"));
        assert_eq!(hybrid_reranked.hits[0].doc_id, 1);
        assert!(!hybrid_reranked.hits[0].snippets.is_empty());
    }

    mock_handle.abort();
}

#[derive(Default)]
struct RecordedCallMeta {
    priorities: Vec<String>,
    has_timeout: Vec<bool>,
    compiled_idf_bytes: Vec<u8>,
}

struct LoopbackVocabServer {
    expected_bearer: String,
    recorded: Arc<Mutex<RecordedCallMeta>>,
}

impl LoopbackVocabServer {
    fn verify_and_record<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let want = format!("Bearer {}", self.expected_bearer);
        if auth != want {
            return Err(Status::unauthenticated("missing or invalid bearer token"));
        }
        let prio = req
            .metadata()
            .get("x-turbo-priority")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let has_timeout = req.metadata().get("grpc-timeout").is_some();
        let mut guard = self.recorded.lock().unwrap();
        guard.priorities.push(prio);
        guard.has_timeout.push(has_timeout);
        Ok(())
    }
}

#[tonic::async_trait]
impl TurboVocabularyService for LoopbackVocabServer {
    type DownloadVocabularyStream = ReceiverStream<Result<VocabularyArtifactChunk, Status>>;

    async fn list_dictionary_formats(
        &self,
        _req: Request<ListDictionaryFormatsRequest>,
    ) -> Result<Response<ListDictionaryFormatsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn list_dictionaries(
        &self,
        _req: Request<ListDictionariesRequest>,
    ) -> Result<Response<ListDictionariesResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn list_vocabularies(
        &self,
        _req: Request<ListVocabulariesRequest>,
    ) -> Result<Response<ListVocabulariesResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn delete_vocabulary_artifact(
        &self,
        _req: Request<DeleteVocabularyArtifactRequest>,
    ) -> Result<Response<DeleteVocabularyArtifactResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn import_dictionary(
        &self,
        _req: Request<Streaming<ImportDictionaryRequest>>,
    ) -> Result<Response<DictionaryArtifactDescriptor>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn learn_vocabulary(
        &self,
        _req: Request<Streaming<LearnVocabularyRequest>>,
    ) -> Result<Response<VocabularyArtifactDescriptor>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn download_vocabulary(
        &self,
        _req: Request<DownloadVocabularyRequest>,
    ) -> Result<Response<Self::DownloadVocabularyStream>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn list_suggest_lexicons(
        &self,
        _req: Request<ListSuggestLexiconsRequest>,
    ) -> Result<Response<ListSuggestLexiconsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn build_suggest_lexicon(
        &self,
        _req: Request<Streaming<BuildSuggestLexiconRequest>>,
    ) -> Result<Response<SuggestLexiconArtifactDescriptor>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn suggest_prefix(
        &self,
        _req: Request<SuggestPrefixRequest>,
    ) -> Result<Response<SuggestPrefixResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn lookup_stem_surfaces(
        &self,
        _req: Request<LookupStemSurfacesRequest>,
    ) -> Result<Response<LookupStemSurfacesResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn spellcheck_query(
        &self,
        _req: Request<SpellcheckQueryRequest>,
    ) -> Result<Response<SpellcheckQueryResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn import_corpus_statistics(
        &self,
        req: Request<Streaming<ImportCorpusStatisticsRequest>>,
    ) -> Result<Response<CorpusStatisticsArtifactDescriptor>, Status> {
        self.verify_and_record(&req)?;
        let mut stream = req.into_inner();
        let mut language = String::new();
        let mut name = String::new();
        let mut doc_count = 0u64;
        let mut entries: Vec<(String, u64)> = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame?.frame {
                Some(import_corpus_statistics_request::Frame::Header(h)) => {
                    language = h.language;
                    name = h.name;
                    doc_count = h.doc_count;
                }
                Some(import_corpus_statistics_request::Frame::Batch(b)) => {
                    for e in b.entries {
                        entries.push((e.term, e.doc_freq));
                    }
                }
                None => {}
            }
        }
        let idf = VocabularyCorpusIdf::from_entries(doc_count, entries)
            .map_err(Status::invalid_argument)?;
        let bytes = idf.to_bytes();
        let sha256 = pipestream_search::sha256::hex_digest(&bytes);
        self.recorded.lock().unwrap().compiled_idf_bytes = bytes.clone();
        Ok(Response::new(CorpusStatisticsArtifactDescriptor {
            artifact_id: format!("tcidf-{}", &sha256[..12]),
            language,
            name,
            doc_count: idf.doc_count(),
            term_count: idf.len() as u64,
            byte_size: bytes.len() as u64,
            sha256,
        }))
    }

    async fn list_corpus_statistics(
        &self,
        _req: Request<ListCorpusStatisticsRequest>,
    ) -> Result<Response<ListCorpusStatisticsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }

    async fn train_voronoi_centroids(
        &self,
        req: Request<Streaming<TrainVoronoiCentroidsRequest>>,
    ) -> Result<Response<VoronoiCentroidArtifactDescriptor>, Status> {
        self.verify_and_record(&req)?;
        let mut stream = req.into_inner();
        let mut header = None;
        let mut samples: Vec<f32> = Vec::new();
        while let Some(frame) = stream.next().await {
            match frame?.frame {
                Some(train_voronoi_centroids_request::Frame::Header(h)) => {
                    header = Some(h);
                }
                Some(train_voronoi_centroids_request::Frame::Batch(b)) => {
                    samples.extend(b.values);
                }
                None => {}
            }
        }
        let h = header.ok_or_else(|| Status::invalid_argument("missing Voronoi header"))?;
        let dim = h.dim as usize;
        let k = h.num_centroids as usize;
        if dim == 0 || k == 0 || !samples.len().is_multiple_of(dim) {
            return Err(Status::invalid_argument("invalid Voronoi dimensions"));
        }
        let n_samples = samples.len() / dim;

        // Deterministic spherical k-means++ / Lloyd assignment producing `TCENT001` payload.
        let mut centroids: Vec<Vec<f32>> = (0..k)
            .map(|c| {
                let src = &samples[(c * (n_samples / k.max(1))) * dim..][..dim];
                src.to_vec()
            })
            .collect();
        let mut counts = vec![0u64; k];
        let mut cosine_sum = 0.0_f64;
        for _iter in 0..h.max_iters.clamp(1, 8) {
            counts.fill(0);
            cosine_sum = 0.0;
            let mut sums = vec![vec![0.0_f32; dim]; k];
            for row in samples.chunks_exact(dim) {
                let mut best_c = 0usize;
                let mut best_dot = f32::NEG_INFINITY;
                for (c, cent) in centroids.iter().enumerate() {
                    let dot: f32 = row.iter().zip(cent).map(|(a, b)| a * b).sum();
                    if dot > best_dot {
                        best_dot = dot;
                        best_c = c;
                    }
                }
                counts[best_c] += 1;
                cosine_sum += f64::from(best_dot);
                for (dst, &v) in sums[best_c].iter_mut().zip(row) {
                    *dst += v;
                }
            }
            for c in 0..k {
                let sq: f32 = sums[c].iter().map(|v| v * v).sum();
                if sq > 0.0 {
                    let inv = 1.0 / sq.sqrt();
                    for (dst, v) in centroids[c].iter_mut().zip(&sums[c]) {
                        *dst = v * inv;
                    }
                }
            }
        }
        let mut artifact = Vec::from(&b"TCENT001"[..]);
        artifact.extend_from_slice(&h.dim.to_le_bytes());
        artifact.extend_from_slice(&h.num_centroids.to_le_bytes());
        for cent in &centroids {
            for &v in cent {
                artifact.extend_from_slice(&v.to_le_bytes());
            }
        }
        let sha256 = pipestream_search::sha256::hex_digest(&artifact);
        Ok(Response::new(VoronoiCentroidArtifactDescriptor {
            artifact_id: format!("tcent-{}", &sha256[..12]),
            model_id: h.model_id,
            model_revision: h.model_revision,
            model_sha256: h.model_sha256,
            dim: h.dim,
            num_centroids: h.num_centroids,
            sample_count: n_samples as u64,
            mean_cosine: cosine_sum / (n_samples.max(1) as f64),
            byte_size: artifact.len() as u64,
            sha256,
            cluster_counts: counts,
        }))
    }

    async fn list_voronoi_centroids(
        &self,
        _req: Request<ListVoronoiCentroidsRequest>,
    ) -> Result<Response<ListVoronoiCentroidsResponse>, Status> {
        Err(Status::unimplemented("unused in feedback test"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kserve_security_priority_deadlines_pool_eviction_and_vocabulary_feedback_loop() {
    let dir = tempdir("vocab_feedback");
    let recorded = Arc::new(Mutex::new(RecordedCallMeta::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let recorded_srv = Arc::clone(&recorded);
    let srv_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(TurboVocabularyServiceServer::new(LoopbackVocabServer {
                expected_bearer: "secret-kserve-token".into(),
                recorded: recorded_srv,
            }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let future_deadline_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
        + 15_000_000;

    // 1. Verify `MtlsClientMaterial` + `KserveCallPolicy` construction and unauthenticated refusal!
    let tls_policy = KserveCallPolicy {
        bearer_token: Some("secret-kserve-token".into()),
        mtls: Some(MtlsClientMaterial {
            server_ca_pem: b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n".to_vec(),
            client_identity_pem: Some((b"cert".to_vec(), b"key".to_vec())),
            domain_name: Some("kserve.internal".into()),
        }),
    };
    let tls_ep = tls_policy.endpoint_config(
        "https://127.0.0.1:8443",
        RemotePriority::Interactive,
        Some(Duration::from_secs(3)),
    );
    assert_eq!(tls_ep.default_priority, RemotePriority::Interactive);
    assert_eq!(tls_ep.bearer_token.as_deref(), Some("secret-kserve-token"));

    let unauth_client = RemoteClient::new(RemoteEndpointConfig::new(&addr));
    let unauth_err = export_corpus_statistics_to_kserve(
        &unauth_client,
        "en",
        "col-unauth",
        10,
        &[("court".into(), 5)],
        future_deadline_us,
    )
    .await
    .unwrap_err();
    assert_eq!(unauth_err.code(), Code::Unauthenticated);

    // 2. Verify expired `finish_deadline_us` (`1` us since epoch) aborts BEFORE network dispatch
    //    with `Code::DeadlineExceeded` across corpus export, Voronoi training, and Stage-2 rerank!
    let auth_policy = KserveCallPolicy {
        bearer_token: Some("secret-kserve-token".into()),
        mtls: None,
    };
    let auth_client = RemoteClient::new(auth_policy.endpoint_config(
        &addr,
        RemotePriority::Interactive,
        Some(Duration::from_secs(5)),
    ));

    assert_eq!(
        export_corpus_statistics_to_kserve(
            &auth_client,
            "en",
            "expired",
            10,
            &[("court".into(), 5)],
            1,
        )
        .await
        .unwrap_err()
        .code(),
        Code::DeadlineExceeded
    );

    let voronoi_spec = VoronoiTrainingSpec {
        model_id: "bge-small-en-v1.5".into(),
        model_revision: "rev1".into(),
        model_sha256: "deadbeef".into(),
        dim: 16,
        num_centroids: 4,
        max_iters: 6,
        seed: 42,
        tag_set_name: "shard-voronoi".into(),
        temperature: 1.0,
    };
    let sample_vecs = unit_vectors(64, 16, 0x2346_BEEF);
    assert_eq!(
        train_voronoi_centroids_on_kserve(&auth_client, &voronoi_spec, &sample_vecs, 1)
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );

    let rerank_expired = rerank_and_highlight_shortlist(
        &RerankSpec::new(RerankBackend::DeterministicCalibrated {
            platt_slope: 1.5,
            platt_intercept: 0.0,
        }),
        "query",
        &[RerankCandidate {
            doc_id: 1,
            prior_score: 1.0,
            text: "sample passage".into(),
            sentence_spans: Vec::new(),
            lineage_span_start: 0,
        }],
        1,
    )
    .await
    .unwrap_err();
    assert_eq!(rerank_expired.code(), Code::DeadlineExceeded);

    // 3. Feed documents into `VocabularyListener`, export live heavy hitters via
    //    `export_corpus_statistics_to_kserve`, and verify `TCIDF001` round-trip + Bulk priority!
    let vocab_listener = VocabularyListener::create(&dir, 1_000, 32).unwrap();
    for i in 0..20 {
        if i == 0 {
            vocab_listener.feed(
                [("common_term", 1), ("rare_jurisdiction", 1)].into_iter(),
                ["common_term", "rare_jurisdiction"].into_iter(),
            );
        } else {
            vocab_listener.feed(
                [("common_term", 1)].into_iter(),
                ["common_term"].into_iter(),
            );
        }
    }
    let terms_stats = vocab_listener
        .live_stats()
        .into_iter()
        .find(|s| s.channel == VocabChannel::Terms as i32)
        .unwrap();
    assert_eq!(terms_stats.documents, 20);
    let term_dfs: Vec<(String, u64)> = terms_stats
        .heavy_hitters
        .iter()
        .map(|h| (h.term.clone(), h.count.min(terms_stats.documents)))
        .collect();

    let idf_desc = export_corpus_statistics_to_kserve(
        &auth_client,
        "en",
        "legal-corpus-v1",
        terms_stats.documents,
        &term_dfs,
        future_deadline_us,
    )
    .await
    .unwrap();
    assert_eq!(idf_desc.doc_count, 20);
    assert_eq!(idf_desc.term_count, 2);

    // Decode the `TCIDF001` bytes compiled by the server and verify smooth BM25 IDF monotonicity!
    let idf_bytes = recorded.lock().unwrap().compiled_idf_bytes.clone();
    assert_eq!(&idf_bytes[..8], b"TCIDF001");
    let decoded_idf = VocabularyCorpusIdf::from_bytes(&idf_bytes).unwrap();
    assert!(decoded_idf.idf("rare_jurisdiction") > decoded_idf.idf("common_term"));
    assert!(decoded_idf.idf("common_term") > 0.0);

    // 4. Stream sample embeddings via `train_voronoi_centroids_on_kserve` (`TCENT001`)!
    let voronoi_desc = train_voronoi_centroids_on_kserve(
        &auth_client,
        &voronoi_spec,
        &sample_vecs,
        future_deadline_us,
    )
    .await
    .unwrap();
    assert_eq!(voronoi_desc.dim, 16);
    assert_eq!(voronoi_desc.num_centroids, 4);
    assert_eq!(voronoi_desc.sample_count, 64);
    assert_eq!(voronoi_desc.cluster_counts.iter().sum::<u64>(), 64);
    assert!(voronoi_desc.mean_cosine > 0.15);

    // Verify both background feedback RPCs overrode the client's `Interactive` default to `Bulk`
    // and propagated `grpc-timeout`!
    {
        let meta = recorded.lock().unwrap();
        assert_eq!(
            meta.priorities,
            vec!["bulk".to_string(), "bulk".to_string()]
        );
        assert_eq!(meta.has_timeout, vec![true, true]);
    }

    // 5. Verify `RemoteChannelPool` runtime-scoped eviction: opening a channel on a dedicated
    //    Tokio runtime caches 1 entry, and dropping that runtime evicts it automatically!
    let shared_pool = RemoteChannelPool::new();
    assert_eq!(shared_pool.active_channel_count(), 0);
    let pool_clone = shared_pool.clone();
    let addr_clone = addr.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let _ch = pool_clone
                .channel(&RemoteEndpointConfig::new(addr_clone))
                .unwrap();
            assert_eq!(pool_clone.active_channel_count(), 1);
        });
        drop(rt);
    })
    .join()
    .unwrap();
    assert_eq!(shared_pool.active_channel_count(), 0);

    srv_handle.abort();
}
