//! Public `Query` adapter acceptance (`docs/query-api.md`,
//! `src/query.rs`): every supported shape executes bitwise-identically
//! to the ordinary route it maps onto, provenance names the signals,
//! and every unsupported construct is refused by name.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    aggregate_result, query_stream_response, search_query, selection_query,
    selection_score_strategy, AddDocumentsRequest, AddVectorsRequest, AggregateOp,
    AggregateRequest, Aggregation, Bm25SearchRequest, BooleanQuery, BoostQuery, BoostRescore,
    CascadeScore, CollapseSpec, CompositeScorer, CompositeSearchStrategy, DecomposedScore,
    DeleteDocumentsRequest, DenseExecutionMode, DenseQualityPolicy, DenseQuery, DenseScoreMode,
    DocLineage, FacetValue, FilterQuery, FlushRequest, FusionMode, HealthRequest, HybridLegOptions,
    HybridSearchRequest, IntegerValue, LexicalQuery, QueryRequest, QueryResponse, QuerySort,
    QueryStreamCompletion, QueryStreamPhase, QueryStreamRequest, QueryStreamResponse,
    QueryStreamRevision, RrfScore, ScoreOp, ScoreStage, SearchQuery, SearchRequest,
    SelectionOperator, SelectionQuery, SelectionScoreStrategy, SetCalibrationRequest,
    VectorQualityContract,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Request;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};
use prost::Message;

const DIM: usize = 64;
const SHARD_DOCS: usize = 4;
const N_DOCS: usize = 2 * SHARD_DOCS;
const COURTS: [&str; 3] = ["ca9", "ca2", "scotus"];

/// Corpus: doc i's text mentions "zebra" for even i, and every doc
/// carries `year = i` for filters. Vectors are the seeded unit corpus;
/// the test query vector is doc 0's own.
async fn start_cluster() -> (
    CoordinatorServiceImpl,
    Vec<f32>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    start_cluster_with_addrs().await.0
}

async fn start_cluster_with_addrs() -> (
    (
        CoordinatorServiceImpl,
        Vec<f32>,
        Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    ),
    Vec<String>,
) {
    start_cluster_with_identities(false).await
}

fn fixture_identity(id: u64) -> Option<pipestream_search::pb::DocumentIdentity> {
    (id != 0).then(|| pipestream_search::pb::DocumentIdentity {
        document_key: [vec![0, 255], id.to_le_bytes().to_vec()].concat(),
        version: u64::MAX - id,
        chunk_ordinal: id.is_multiple_of(2).then_some(0),
    })
}

async fn start_cluster_with_identities(
    identities: bool,
) -> (
    (
        CoordinatorServiceImpl,
        Vec<f32>,
        Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    ),
    Vec<String>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0xC0FE_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut handles = vec![mock];
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["year".into()],
            facet_fields: vec!["court".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            let text = if id.is_multiple_of(2) {
                format!("zebra document {id}")
            } else {
                format!("plain document {id}")
            };
            tx.send(AddDocumentsRequest {
                text,
                original_source: identities
                    .then(|| common::protobuf_source("fixture", &id.to_string())),
                identity: identities.then(|| fixture_identity(id as u64)).flatten(),
                source_chunk_ordinal: if identities {
                    fixture_identity(id as u64).and_then(|identity| identity.chunk_ordinal)
                } else {
                    None
                },
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: id as i64,
                }],
                // A facet column and lineage for the sort and collapse
                // tests: three courts round-robin, parents in pairs,
                // groups in fours.
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: COURTS[id % 3].into(),
                }],
                lineage: Some(DocLineage {
                    parent_id: (id / 2) as u64,
                    group_id: (id / 4) as u64,
                    span_start: 0,
                    span_end: 0,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * SHARD_DOCS;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs.clone()).with_bm25(Some(analysis), Default::default());
    ((coordinator, corpus[..DIM].to_vec(), handles), addrs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_results_preserve_identity_in_classic_and_streaming_scans() {
    let ((coordinator, vector, handles), _) = start_cluster_with_identities(true).await;
    let request = QueryRequest {
        k: N_DOCS as u32,
        selection: Some(dense_leaf("vec", &vector)),
        ..Default::default()
    };
    let mut expected = None;
    for stream in [false, true] {
        let coordinator = coordinator.clone().with_stream_search(stream);
        let response = query(&coordinator, request.clone()).await.unwrap();
        assert_eq!(response.hits.len(), N_DOCS);
        for hit in &response.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        }
        let signature: Vec<_> = response
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits(), hit.identity.clone()))
            .collect();
        if let Some(expected) = expected.as_ref() {
            assert_eq!(&signature, expected);
        }
        expected = Some(signature);
        let events = streamed_query(&coordinator, Some(request.clone()), 0).await;
        let (_, completion) = stream_parts(&events);
        let terminal = completion.response.as_ref().unwrap();
        assert_eq!(terminal.hits.len(), N_DOCS);
        for hit in &terminal.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        }
    }
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_results_preserve_imported_identity_through_public_and_streamed_queries() {
    let ((coordinator, _, handles), addrs) = start_cluster_with_identities(true).await;
    let request = QueryRequest {
        explain: true,
        k: N_DOCS as u32,
        selection: Some(lexical_leaf("lex", "document")),
        ..Default::default()
    };
    for stream in [false, true] {
        let coordinator = coordinator.clone().with_bm25_stream(stream);
        for fused in [false, true] {
            let lexical = coordinator
                .bm25_search(Request::new(Bm25SearchRequest {
                    explain: true,
                    text: "document".into(),
                    k: N_DOCS as u32,
                    fields: if fused {
                        vec![pipestream_search::pb::QueryField {
                            field: "body".into(),
                            ..Default::default()
                        }]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(lexical.hits.len(), N_DOCS);
            for hit in &lexical.hits {
                assert_eq!(hit.identity, fixture_identity(hit.doc_id));
                assert!(hit.explain.is_some());
            }
        }
        let response = query(&coordinator, request.clone()).await.unwrap();
        assert_eq!(response.hits.len(), N_DOCS);
        for hit in &response.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
            assert!(hit.explain.is_some());
        }
    }
    // Explain remains unary-only; the stream still carries terminal identity.
    let events = streamed_query(
        &coordinator,
        Some(QueryRequest {
            explain: false,
            ..request
        }),
        0,
    )
    .await;
    let (_, completion) = stream_parts(&events);
    let response = completion.response.as_ref().unwrap();
    assert_eq!(response.hits.len(), N_DOCS);
    for hit in &response.hits {
        assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        assert!(hit.explain.is_none());
    }
    for addr in addrs {
        let rescored = NodeServiceClient::connect(addr)
            .await
            .unwrap()
            .bm25_rescore(pipestream_search::pb::Bm25RescoreRequest {
                terms: vec!["document".into()],
                global_doc_count: N_DOCS as u64,
                global_total_doc_length: (N_DOCS * 3) as u64,
                global_doc_frequencies: vec![N_DOCS as u32],
                candidate_ids: (0..N_DOCS as u64).collect(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rescored.hits.len(), SHARD_DOCS);
        for hit in rescored.hits {
            assert_eq!(hit.identity, fixture_identity(hit.doc_id));
        }
    }
    for handle in handles {
        handle.abort();
    }
}

fn lexical_leaf(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                ..Default::default()
            })),
        })),
    }
}

fn dense_leaf(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                ..Default::default()
            })),
        })),
    }
}

fn fp32_dense_leaf(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                score_mode: DenseScoreMode::Fp32Rerank as i32,
                ..Default::default()
            })),
        })),
    }
}

fn cel_filter(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn composite(
    operator: SelectionOperator,
    clauses: Vec<SelectionQuery>,
    strategy: Option<selection_score_strategy::Strategy>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: operator as i32,
            clauses,
            scoring: strategy.map(|s| SelectionScoreStrategy { strategy: Some(s) }),
        })),
    }
}

fn boolean(
    must: Vec<SelectionQuery>,
    should: Vec<SelectionQuery>,
    must_not: Vec<SelectionQuery>,
    minimum_should_match: u32,
    aggregate: Option<AggregateRequest>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should,
            must_not,
            minimum_should_match,
            aggregate,
        })),
    }
}

async fn query(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, tonic::Status> {
    coordinator
        .query(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

async fn streamed_query(
    coordinator: &CoordinatorServiceImpl,
    query: Option<QueryRequest>,
    timeout_ms: u64,
) -> Vec<QueryStreamResponse> {
    let mut inbound = coordinator
        .query_stream(Request::new(QueryStreamRequest {
            collection: String::new(),
            query,
            timeout_ms,
        }))
        .await
        .expect("QueryStream opens")
        .into_inner();
    let mut events = Vec::new();
    while let Some(event) = inbound.next().await {
        let event = event.expect("well-formed stream");
        events.push(event);
    }
    events
}

fn stream_parts(
    events: &[QueryStreamResponse],
) -> (Vec<&QueryStreamRevision>, &QueryStreamCompletion) {
    let mut revisions = Vec::new();
    let mut completion = None;
    for event in events {
        match event.payload.as_ref().expect("stream event payload") {
            query_stream_response::Payload::Revision(revision) => revisions.push(revision),
            query_stream_response::Payload::Completion(done) => {
                assert!(
                    completion.replace(done).is_none(),
                    "one terminal certificate"
                );
            }
        }
    }
    let completion = completion.expect("terminal completion certificate");
    assert!(matches!(
        events.last().and_then(|event| event.payload.as_ref()),
        Some(query_stream_response::Payload::Completion(_))
    ));
    (revisions, completion)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_leaves_execute_their_ordinary_routes() {
    let (coordinator, qvec, _handles) = start_cluster().await;

    // Lexical leaf == Bm25Search, bitwise, with named provenance.
    let public = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(lexical_leaf("lex", "zebra")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "zebra".into(),
            k: 5,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.executed, "bm25_search");
    assert_eq!(public.hits.len(), direct.hits.len());
    for (p, d) in public.hits.iter().zip(&direct.hits) {
        assert_eq!(p.doc_id, d.doc_id);
        assert_eq!(p.score.to_bits(), d.score.to_bits());
        assert_eq!(p.signals.len(), 1);
        assert_eq!(p.signals[0].id, "lex");
        assert_eq!(p.matched, vec!["lex"]);
    }

    // Dense leaf == Search, bitwise.
    let public = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(dense_leaf("vec", &qvec)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .search(Request::new(SearchRequest {
            k: 5,
            vector: qvec.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.executed, "search");
    let got: Vec<(u64, u32)> = public
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect();
    let want: Vec<(u64, u32)> = direct
        .hits
        .iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect();
    assert_eq!(got, want);
    assert_eq!(public.hits[0].doc_id, 0, "the query IS doc 0's vector");
    let execution = public
        .dense_execution
        .expect("every dense result reports its execution contract");
    assert_eq!(
        DenseExecutionMode::try_from(execution.requested_mode).unwrap(),
        DenseExecutionMode::Unspecified
    );
    assert_eq!(
        DenseExecutionMode::try_from(execution.resolved_mode).unwrap(),
        DenseExecutionMode::Exact
    );
    assert_eq!(execution.provider_backend, "embedded-turbovec");
    assert_eq!(
        VectorQualityContract::try_from(execution.quality_contract).unwrap(),
        VectorQualityContract::ExhaustiveNativeScore
    );
    assert!(execution.exhaustive_completion);
    assert!(!execution.scoring_fingerprint.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_execution_modes_are_explicit_and_fail_closed() {
    let (coordinator, qvec, _handles) = start_cluster().await;

    let run = |mode: DenseExecutionMode| QueryRequest {
        k: 5,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: qvec.clone(),
                    execution_mode: mode as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };

    let exact = query(&coordinator, run(DenseExecutionMode::Exact))
        .await
        .unwrap();
    let auto = query(&coordinator, run(DenseExecutionMode::Auto))
        .await
        .unwrap();
    assert_eq!(
        exact
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        auto.hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );
    for (response, requested) in [
        (exact, DenseExecutionMode::Exact),
        (auto, DenseExecutionMode::Auto),
    ] {
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.requested_mode, requested as i32);
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Exact as i32);
        assert!(outcome.exhaustive_completion);
        assert!(!outcome.planner_reason.is_empty());
    }

    let ann = query(&coordinator, run(DenseExecutionMode::Ann))
        .await
        .unwrap_err();
    assert_eq!(ann.code(), tonic::Code::FailedPrecondition);
    assert!(ann.message().contains("ANN"), "{}", ann.message());
    assert!(ann.message().contains("exhaustive"), "{}", ann.message());

    let mut unknown = run(DenseExecutionMode::Exact);
    let Some(selection_query::Node::Search(leaf)) =
        unknown.selection.as_mut().unwrap().node.as_mut()
    else {
        unreachable!()
    };
    let Some(search_query::Query::Dense(dense)) = leaf.query.as_mut() else {
        unreachable!()
    };
    dense.execution_mode = i32::MAX;
    let unknown = query(&coordinator, unknown).await.unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
    assert!(unknown.message().contains("execution_mode"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fp32_mode_reranks_a_persisted_mmap_candidate_pool() {
    const ROWS: usize = 64;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("protomolt_query_exact_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.vector");
    let corpus = unit_vectors(ROWS, DIM, 0xE7AC_7001);
    let query_vector = unit_vectors(1, DIM, 0xE7AC_7002);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        ..Default::default()
    })
    .await;
    let mut node = NodeServiceClient::connect(addr.clone()).await.unwrap();
    node.set_calibration(SetCalibrationRequest {
        dim: DIM as u32,
        bit_width: 4,
        shift,
        scale,
    })
    .await
    .unwrap();
    node.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
        vectors: corpus.clone(),
        dim: DIM as u32,
    }]))
    .await
    .unwrap();
    node.flush(FlushRequest {}).await.unwrap();
    let health = node.health(HealthRequest {}).await.unwrap().into_inner();
    assert!(health.exact_vectors_available);
    assert!(health.exact_vectors_mmap);
    assert_eq!(health.exact_vector_rows, ROWS as u64);
    assert!(pipestream_search::node::exact_vector_sidecar_path(&index_path).exists());

    let profile_path = dir.join("dense-quality.toml");
    std::fs::write(
        &profile_path,
        format!(
            r#"format_version = 1
profile_id = "held-out-64"
embedding_model = "test-unit-vectors"
corpus_generation = 0
corpus_rows = {ROWS}
dimensions = {DIM}
provider_backend = "{}"
scoring_fingerprint = "{}"
measured_queries = 32

[[points]]
k = 10
target_recall_ppm = 1000000
candidates = {ROWS}
"#,
            health.vector_backend, health.scoring_fingerprint
        ),
    )
    .unwrap();
    let quality_profile =
        pipestream_search::quality::DenseQualityProfile::load(&profile_path).unwrap();
    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_topology_generation(0)
        .with_dense_quality_profile(quality_profile);
    let request = QueryRequest {
        request_id: "fp32-public".into(),
        k: 10,
        selection_k: ROWS as u32,
        selection: Some(fp32_dense_leaf("vec", &query_vector)),
        profile: true,
        ..Default::default()
    };
    let response = query(&coordinator, request.clone()).await.unwrap();
    assert_eq!(response.executed, "search:fp32_rerank");
    let physical = response.profile.as_ref().unwrap();
    assert!(physical.rerank_ms >= 0.0);
    assert_eq!(physical.rerank_rows, ROWS as u64);
    assert_eq!(physical.rerank_logical_bytes, (ROWS * DIM * 4) as u64);
    assert!(physical.rerank_pages > 0);
    assert!(physical.rerank_tasks > 0);

    let mut expected: Vec<(u64, f32)> = corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .map(|(id, row)| {
            let score: f32 = row.iter().zip(&query_vector).map(|(a, b)| a * b).sum();
            (id as u64, score)
        })
        .collect();
    expected.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    expected.truncate(10);
    let actual: Vec<(u64, u32)> = response
        .hits
        .iter()
        .map(|hit| {
            assert_eq!(hit.signals[0].score.to_bits(), hit.score.to_bits());
            (hit.doc_id, hit.score.to_bits())
        })
        .collect();
    let expected_bits: Vec<(u64, u32)> = expected
        .iter()
        .map(|(id, score)| (*id, score.to_bits()))
        .collect();
    assert_eq!(actual, expected_bits);

    let measured_request = QueryRequest {
        request_id: "fp32-measured".into(),
        k: 10,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: query_vector.clone(),
                    score_mode: DenseScoreMode::Fp32Rerank as i32,
                    quality: Some(DenseQualityPolicy {
                        target_recall_ppm: 1_000_000,
                        max_candidates: ROWS as u32,
                        required_profile_fingerprint: String::new(),
                    }),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };
    let measured = query(&coordinator, measured_request.clone()).await.unwrap();
    assert_eq!(
        measured
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        expected_bits
    );
    let outcome = measured.dense_quality.expect("measured policy outcome");
    assert_eq!(outcome.target_recall_ppm, 1_000_000);
    assert_eq!(outcome.selection_k, ROWS as u32);
    assert_eq!(outcome.profile_id, "held-out-64");
    assert_eq!(outcome.corpus_rows, ROWS as u64);

    let mut conflicting = measured_request.clone();
    conflicting.selection_k = ROWS as u32;
    let err = query(&coordinator, conflicting).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("selection_k"));

    let stale_path = dir.join("dense-quality-stale.toml");
    let stale = std::fs::read_to_string(&profile_path)
        .unwrap()
        .replace("corpus_rows = 64", "corpus_rows = 65");
    std::fs::write(&stale_path, stale).unwrap();
    let stale_coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_topology_generation(0)
        .with_dense_quality_profile(
            pipestream_search::quality::DenseQualityProfile::load(&stale_path).unwrap(),
        );
    let err = query(&stale_coordinator, measured_request.clone())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("row"), "{}", err.message());

    let byte_limited =
        CoordinatorServiceImpl::new(vec![addr]).with_max_rerank_bytes((ROWS * DIM * 4 - 1) as u64);
    let err = query(&byte_limited, request.clone()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err.message().contains("bytes"), "{}", err.message());

    let mut stream_request = request;
    stream_request.profile = false;
    let stream_expected = query(&coordinator, stream_request.clone()).await.unwrap();
    let events = streamed_query(&coordinator, Some(stream_request), 0).await;
    let (revisions, completion) = stream_parts(&events);
    assert!(completion.completed, "{completion:?}");
    assert_eq!(
        completion.response.as_ref().unwrap().encode_to_vec(),
        stream_expected.encode_to_vec()
    );
    assert!(revisions.iter().any(|revision| {
        QueryStreamPhase::try_from(revision.phase).unwrap() == QueryStreamPhase::Dense
    }));
    assert_eq!(
        QueryStreamPhase::try_from(revisions.last().unwrap().phase).unwrap(),
        QueryStreamPhase::Final
    );

    node.delete_documents(DeleteDocumentsRequest {
        expected_wal_generation: None,
        doc_ids: vec![63],
    })
    .await
    .unwrap();
    let err = query(&coordinator, measured_request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("compact and remeasure"),
        "{}",
        err.message()
    );

    handle.abort();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filters_ride_the_and_wrapper_and_conjoin() {
    let (coordinator, _qvec, _handles) = start_cluster().await;

    let public = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(composite(
                SelectionOperator::And,
                vec![
                    lexical_leaf("lex", "document"),
                    cel_filter("recent", "year >= 2"),
                    cel_filter("old", "year < 6"),
                ],
                None,
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "document".into(),
            k: 10,
            filter: "(year >= 2) && (year < 6)".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let got: Vec<u64> = public.hits.iter().map(|h| h.doc_id).collect();
    let want: Vec<u64> = direct.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(got, want);
    assert!(got.iter().all(|&id| (2..6).contains(&id)));
    for hit in &public.hits {
        assert_eq!(hit.matched, vec!["lex", "recent", "old"]);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composites_execute_their_fusion_modes() {
    let (coordinator, qvec, _handles) = start_cluster().await;

    let two_leaves = || vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")];
    for (strategy, operator, mode, executed) in [
        (
            selection_score_strategy::Strategy::Rrf(RrfScore::default()),
            SelectionOperator::Or,
            FusionMode::GlobalRank,
            "hybrid_search:global_rank",
        ),
        (
            selection_score_strategy::Strategy::Decomposed(DecomposedScore::default()),
            SelectionOperator::Or,
            FusionMode::Decomposed,
            "hybrid_search:decomposed",
        ),
        (
            selection_score_strategy::Strategy::Cascade(CascadeScore {
                gate_id: "vec".into(),
            }),
            SelectionOperator::Unspecified,
            FusionMode::Cascade,
            "hybrid_search:cascade",
        ),
    ] {
        let public = query(
            &coordinator,
            QueryRequest {
                k: 6,
                selection: Some(composite(operator, two_leaves(), Some(strategy))),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let direct = coordinator
            .hybrid_search(Request::new(HybridSearchRequest {
                text: "zebra".into(),
                vector: qvec.clone(),
                k: 6,
                legs: Some(HybridLegOptions {
                    fusion_mode: mode as i32,
                    leg_k: 6,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(public.executed, executed);
        if mode == FusionMode::Cascade {
            let got: Vec<(u64, u32)> = public
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect();
            let want: Vec<(u64, u32)> = direct
                .cascade_hits
                .iter()
                .map(|h| (h.doc_id, h.bm25_score.to_bits()))
                .collect();
            assert_eq!(got, want, "{executed}");
        } else {
            let got: Vec<(u64, u32)> = public
                .hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect();
            let want: Vec<(u64, u32)> = direct
                .hits
                .iter()
                .map(|h| (h.doc_id, h.fused_score.to_bits()))
                .collect();
            assert_eq!(got, want, "{executed}");
        }
        // Provenance: doc 0 is in both legs (its own vector tops the
        // dense leg; "zebra document 0" matches the lexical leg), so
        // its hit names both signals.
        let zero = public.hits.iter().find(|h| h.doc_id == 0).unwrap();
        let mut signal_ids: Vec<&str> = zero.signals.iter().map(|s| s.id.as_str()).collect();
        signal_ids.sort_unstable();
        assert_eq!(signal_ids, vec!["lex", "vec"], "{executed}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boost_rescoring_draws_from_selection_k_and_returns_k() {
    let (coordinator, qvec, _handles) = start_cluster().await;

    let public = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection_k: 8,
            selection: Some(composite(
                SelectionOperator::Or,
                vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
                Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
            )),
            boosts: vec![BoostQuery {
                query: Some(SearchQuery {
                    id: "boost".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "plain".into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let direct = coordinator
        .hybrid_search(Request::new(HybridSearchRequest {
            text: "zebra".into(),
            vector: qvec.clone(),
            k: 8,
            legs: Some(HybridLegOptions {
                fusion_mode: FusionMode::GlobalRank as i32,
                leg_k: 8,
                ..Default::default()
            }),
            boost: Some(BoostRescore {
                text: "plain".into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(public.hits.len(), 3, "trimmed to k");
    for (p, d) in public.hits.iter().zip(&direct.hits) {
        assert_eq!(p.doc_id, d.doc_id);
        assert_eq!(p.score.to_bits(), d.fused_score.to_bits());
    }
    // A boosted hit ("plain" matches odd docs) names the boost signal.
    let boosted = public
        .hits
        .iter()
        .find(|h| h.doc_id % 2 == 1)
        .expect("the boost promoted a plain doc into the top 3");
    assert!(boosted.signals.iter().any(|s| s.id == "boost"));
    assert!(boosted.matched.contains(&"boost".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_shapes_refuse_by_name() {
    let (coordinator, qvec, _handles) = start_cluster().await;

    let or_two = |strategy| {
        composite(
            SelectionOperator::Or,
            vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
            strategy,
        )
    };
    let cases: Vec<(QueryRequest, &str)> = vec![
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(SelectionOperator::And, Vec::new(), None)),
                ..Default::default()
            },
            "empty browse",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::And,
                    vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
                    None,
                )),
                ..Default::default()
            },
            "AND over 2 scoring structures",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::Or,
                    vec![
                        dense_leaf("vec", &qvec),
                        lexical_leaf("lex", "zebra"),
                        cel_filter("f", "year >= 2"),
                    ],
                    Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
                )),
                ..Default::default()
            },
            "AND wrapper AROUND",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(or_two(None)),
                ..Default::default()
            },
            "explicit strategy",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(or_two(Some(selection_score_strategy::Strategy::Cascade(
                    CascadeScore {
                        gate_id: "vec".into(),
                    },
                )))),
                ..Default::default()
            },
            "operator must be left unspecified",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::Unspecified,
                    vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
                    Some(selection_score_strategy::Strategy::Cascade(CascadeScore {
                        gate_id: "lex".into(),
                    })),
                )),
                ..Default::default()
            },
            "vector-gate",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                scorer: Some(CompositeScorer {
                    operation: 1,
                    dimensions: Vec::new(),
                }),
                ..Default::default()
            },
            "no dimensions",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(composite(
                    SelectionOperator::And,
                    vec![lexical_leaf("dup", "zebra"), cel_filter("dup", "year >= 0")],
                    None,
                )),
                ..Default::default()
            },
            "duplicate query id",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("", "zebra")),
                ..Default::default()
            },
            "non-empty id",
        ),
        (
            QueryRequest {
                k: 5,
                selection_k: 3,
                selection: Some(lexical_leaf("lex", "zebra")),
                ..Default::default()
            },
            "must not exceed selection_k",
        ),
        (
            QueryRequest {
                k: 3,
                selection_k: 8,
                selection: Some(lexical_leaf("lex", "zebra")),
                ..Default::default()
            },
            "silent no-op",
        ),
        (
            // Two boosts with no scorer: nothing defines how their
            // signals combine.
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                boosts: ["b1", "b2"]
                    .iter()
                    .map(|id| BoostQuery {
                        query: Some(SearchQuery {
                            id: (*id).into(),
                            query: Some(search_query::Query::Lexical(LexicalQuery {
                                text: "plain".into(),
                                ..Default::default()
                            })),
                        }),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            },
            "no composite scorer",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(boolean(
                    Vec::new(),
                    Vec::new(),
                    vec![lexical_leaf("excluded", "zebra")],
                    0,
                    None,
                )),
                boosts: vec![BoostQuery {
                    query: Some(SearchQuery {
                        id: "boost".into(),
                        query: Some(search_query::Query::Lexical(LexicalQuery {
                            text: "plain".into(),
                            ..Default::default()
                        })),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            },
            "positive scoring clause",
        ),
    ];
    for (req, needle) in cases {
        let err = query(&coordinator, req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_boolean_is_exact_across_hybrid_signals_filters_aggregation_and_pages() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let selection = || {
        boolean(
            vec![
                lexical_leaf("body", "document"),
                cel_filter("adult", "year >= 1"),
            ],
            vec![
                lexical_leaf("zebra", "zebra"),
                dense_leaf("vector", &qvec),
                boolean(
                    vec![cel_filter("early", "year < 5")],
                    Vec::new(),
                    vec![cel_filter("not_three", "year == 3")],
                    0,
                    None,
                ),
            ],
            vec![cel_filter("not_six", "year == 6")],
            2,
            Some(AggregateRequest {
                aggregations: vec![Aggregation {
                    name: "year_sum".into(),
                    expression: "year".into(),
                    op: AggregateOp::Sum as i32,
                    max_distinct: 0,
                }],
                ..Default::default()
            }),
        )
    };

    let full = query(
        &coordinator,
        QueryRequest {
            request_id: "boolean-full".into(),
            k: 8,
            selection: Some(selection()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.executed, "boolean:bitmap");
    let mut matched: Vec<u64> = full.hits.iter().map(|hit| hit.doc_id).collect();
    matched.sort_unstable();
    assert_eq!(matched, vec![1, 2, 4]);
    let two = full.hits.iter().find(|hit| hit.doc_id == 2).unwrap();
    assert!(two.signals.iter().any(|signal| signal.id == "body"));
    assert!(two.signals.iter().any(|signal| signal.id == "zebra"));
    assert!(two.signals.iter().any(|signal| signal.id == "vector"));
    assert!(two.matched.contains(&"adult".to_string()));
    assert!(two.matched.contains(&"early".to_string()));
    assert!(!two.matched.contains(&"not_six".to_string()));
    let aggregate = full.aggregate.as_ref().expect("root boolean aggregate");
    assert_eq!(aggregate.matched, 3);
    assert_eq!(aggregate.results[0].present, 3);
    assert_eq!(
        aggregate.results[0].value,
        Some(aggregate_result::Value::IntValue(7))
    );

    let first = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(selection()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(first.hits.len(), 2);
    assert!(!first.next_cursor.is_empty());
    assert_eq!(first.aggregate.as_ref().unwrap().matched, 3);
    let second = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(selection()),
            cursor: first.next_cursor.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(second.hits.len(), 1);
    assert!(second.next_cursor.is_empty());
    assert_eq!(second.aggregate.as_ref().unwrap().matched, 3);
    let stitched: Vec<(u64, u32)> = first
        .hits
        .iter()
        .chain(&second.hits)
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect();
    assert_eq!(
        stitched,
        full.hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );

    let boosted = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(selection()),
            boosts: vec![BoostQuery {
                query: Some(SearchQuery {
                    id: "plain_boost".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "plain".into(),
                        ..Default::default()
                    })),
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let one = boosted.hits.iter().find(|hit| hit.doc_id == 1).unwrap();
    assert!(one.signals.iter().any(|signal| signal.id == "plain_boost"));

    let events = streamed_query(
        &coordinator,
        Some(QueryRequest {
            k: 8,
            selection: Some(selection()),
            ..Default::default()
        }),
        0,
    )
    .await;
    let (_, completion) = stream_parts(&events);
    assert!(completion.completed, "{completion:?}");
    assert_eq!(
        completion
            .response
            .as_ref()
            .unwrap()
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        full.hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boolean_candidate_rescore_preserves_the_ordinary_score_stage_bits() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let lexical = || SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "body".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "document".into(),
                score_stages: vec![ScoreStage {
                    op: ScoreOp::AddLinear as i32,
                    column: "year".into(),
                    weight: 0.125,
                    ..Default::default()
                }],
                ..Default::default()
            })),
        })),
    };
    let ordinary = query(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(lexical()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let planned = query(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(boolean(vec![lexical()], Vec::new(), Vec::new(), 0, None)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(planned.executed, "boolean:bitmap");
    assert_eq!(planned.hits.len(), ordinary.hits.len());
    assert_eq!(
        planned
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>(),
        ordinary
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_boolean_match_set_has_an_empty_aggregate() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(boolean(
                vec![cel_filter("none", "year > 100")],
                Vec::new(),
                Vec::new(),
                0,
                Some(AggregateRequest {
                    aggregations: vec![Aggregation {
                        name: "count".into(),
                        expression: "year".into(),
                        op: AggregateOp::Count as i32,
                        max_distinct: 0,
                    }],
                    ..Default::default()
                }),
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(response.hits.is_empty());
    let aggregate = response.aggregate.unwrap();
    assert_eq!(aggregate.matched, 0);
    assert_eq!(aggregate.results[0].present, 0);
    assert_eq!(
        aggregate.results[0].value,
        Some(aggregate_result::Value::IntValue(0))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_stitch_into_the_full_ranking() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let leaf = || lexical_leaf("lex", "document");

    let full = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(leaf()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.hits.len(), 8, "every doc says \"document\"");

    let mut stitched = Vec::new();
    let mut cursor = String::new();
    let mut pages = 0;
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 3,
                selection: Some(leaf()),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(
            resp.hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits(), h.rank)),
        );
        pages += 1;
        if resp.next_cursor.is_empty() {
            assert!(resp.hits.len() < 3, "a full page always mints a cursor");
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(pages, 3, "8 hits in pages of 3");
    let want: Vec<(u64, u32, u32)> = full
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.rank))
        .collect();
    assert_eq!(stitched, want, "pages stitch bitwise, ranks absolute");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_pages_within_its_fixed_pool() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let selection = || {
        composite(
            SelectionOperator::Or,
            vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")],
            Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
        )
    };

    let full = query(
        &coordinator,
        QueryRequest {
            k: 6,
            selection_k: 6,
            selection: Some(selection()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut stitched = Vec::new();
    let mut cursor = String::new();
    for _ in 0..3 {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 2,
                selection_k: 6,
                selection: Some(selection()),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(resp.hits.len(), 2);
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.score.to_bits())));
        cursor = resp.next_cursor.clone();
        assert!(!cursor.is_empty());
    }
    let want: Vec<(u64, u32)> = full
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect();
    assert_eq!(stitched, want, "pages of the fixed pool stitch bitwise");

    // The pool is exhausted; deepening would change the ranking, so
    // the refusal names the knob.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection_k: 6,
            selection: Some(selection()),
            cursor,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("selection_k"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_changed_corpus_refuses_the_cursor() {
    let ((coordinator, _qvec, _handles), addrs) = start_cluster_with_addrs().await;
    let leaf = || lexical_leaf("lex", "document");

    let first = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(leaf()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!first.next_cursor.is_empty());

    // Ingest changes the bound physical read version. Refuse the cursor
    // before re-executing selection, even if its boundary could still match.
    let mut client = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(2);
    tx.send(AddDocumentsRequest {
        text: "a brand new document about zebras".into(),
        integers: vec![IntegerValue {
            field: "year".into(),
            value: 99,
        }],
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();

    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(leaf()),
            cursor: first.next_cursor,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("cursor data context changed"),
        "{}",
        err.message()
    );

    // A malformed token refuses as malformed, not as a corpus change.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(leaf()),
            cursor: "not-a-cursor".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("malformed cursor"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_only_browse_pages_in_id_order() {
    let (coordinator, _qvec, _handles) = start_cluster().await;

    // One bare filter is a browse; docs 2..=6 pass, ids ascending.
    let full = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 2 && year <= 6")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.executed, "browse");
    let ids: Vec<u64> = full.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![2, 3, 4, 5, 6], "deterministic id order");
    assert!(full.next_cursor.is_empty(), "short page: nothing follows");
    for hit in &full.hits {
        assert_eq!(hit.score, 0.0, "no relevance score exists on this route");
        assert!(hit.signals.is_empty(), "a filter is never a signal");
        assert_eq!(hit.matched, vec!["f"]);
    }

    // Filters under an AND wrapper conjoin, and pages stitch with
    // continuing ranks.
    let selection = || {
        composite(
            SelectionOperator::And,
            vec![
                cel_filter("low", "year >= 1"),
                cel_filter("high", "year <= 6"),
            ],
            None,
        )
    };
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 4,
                selection: Some(selection()),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.rank)));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)],
        "pages stitch; ranks continue across pages"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn browse_refuses_an_unknown_column_by_name() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(cel_filter("f", "yaer >= 2")),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("yaer"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_browse_orders_by_column_and_pages() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let selection = || cel_filter("f", "year >= 1");
    let by = |column: &str, descending: bool| QuerySort {
        column: column.into(),
        descending,
    };

    let full = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(selection()),
            sort: vec![by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let rows: Vec<(u64, f64)> = full.hits.iter().map(|h| (h.doc_id, h.sort_key)).collect();
    assert_eq!(
        rows,
        (1..8)
            .rev()
            .map(|i| (i as u64, i as f64))
            .collect::<Vec<_>>(),
        "newest first, and the sort key is reported"
    );
    assert_eq!(
        full.hits[0].sort_values,
        vec![pipestream_search::pb::SortValue {
            value: Some(pipestream_search::pb::sort_value::Value::Integer(7))
        }],
        "the typed value rides alongside"
    );

    // Ascending, paged: stitches into the ascending order.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 3,
                selection: Some(selection()),
                sort: vec![by("year", false)],
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.rank)));
        if resp.next_cursor.is_empty() {
            break;
        }
        assert!(resp.next_cursor.starts_with("pqc1:"), "sealed sorted token");
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        (1..8).map(|i| (i as u64, i as u32)).collect::<Vec<_>>(),
        "sorted pages stitch with continuing ranks"
    );

    // Refusals: unknown sort column by name; sort on a dense shape.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(selection()),
            sort: vec![by("yaer", false)],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("yaer"), "{}", err.message());
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(dense_leaf("d", &_qvec)),
            sort: vec![by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("no membership to order"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_key_sort_orders_text_then_number_and_pages() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let by = |column: &str, descending: bool| QuerySort {
        column: column.into(),
        descending,
    };
    // court ascending (ca2 < ca9 < scotus), then year descending.
    let expected: Vec<(u64, &str)> = vec![
        (7, "ca2"),
        (4, "ca2"),
        (1, "ca2"),
        (6, "ca9"),
        (3, "ca9"),
        (0, "ca9"),
        (5, "scotus"),
        (2, "scotus"),
    ];
    let full = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 0")),
            sort: vec![by("court", false), by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let got: Vec<(u64, String)> = full
        .hits
        .iter()
        .map(|h| {
            let text = match h.sort_values[0].value.as_ref().unwrap() {
                pipestream_search::pb::sort_value::Value::Text(t) => t.clone(),
                other => panic!("{other:?}"),
            };
            (h.doc_id, text)
        })
        .collect();
    assert_eq!(
        got,
        expected
            .iter()
            .map(|(id, c)| (*id, c.to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        full.hits[0].sort_key, 0.0,
        "a text first key has no numeric view"
    );

    // Text descending reverses the court order and keeps the year order.
    let reversed = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 0")),
            sort: vec![by("court", true), by("year", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = reversed.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![5, 2, 6, 3, 0, 7, 4, 1]);

    // Paged by 3 with a text boundary in the cursor: stitches exactly.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 3,
                selection: Some(cel_filter("f", "year >= 0")),
                sort: vec![by("court", false), by("year", true)],
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| h.doc_id));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        expected.iter().map(|(id, _)| *id).collect::<Vec<_>>()
    );

    // A lineage key sorts too: parent_id descending, then id.
    let parents = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(cel_filter("f", "year >= 0")),
            sort: vec![by("parent_id", true)],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = parents.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![6, 7, 4, 5, 2, 3, 0, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lexical_leaf_sorts_its_exact_membership_without_scores() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    // "zebra" matches the even ids; year descending orders them 6,4,2,0.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(lexical_leaf("lex", "zebra")),
            sort: vec![QuerySort {
                column: "year".into(),
                descending: true,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![6, 4, 2, 0]);
    assert!(
        resp.hits.iter().all(|h| h.score == 0.0),
        "no relevance is computed"
    );
    assert_eq!(resp.hits[0].matched, vec!["lex".to_string()]);
    assert_eq!(resp.executed, "browse_shard:lexical");

    // With a filter: the membership is the AND of the leaf and the filter.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 10,
            selection: Some(composite(
                SelectionOperator::And,
                vec![lexical_leaf("lex", "zebra"), cel_filter("f", "year >= 3")],
                None,
            )),
            sort: vec![QuerySort {
                column: "year".into(),
                descending: false,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![4, 6]);
    assert_eq!(
        resp.hits[0].matched,
        vec!["lex".to_string(), "f".to_string()]
    );

    // Paged by 1, stitching through the sorted lexical membership.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 1,
                selection: Some(lexical_leaf("lex", "zebra")),
                sort: vec![QuerySort {
                    column: "year".into(),
                    descending: true,
                }],
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.rank)));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(stitched, vec![(6, 1), (4, 2), (2, 3), (0, 4)]);

    // A relevance shape on the sorted leaf refuses by name.
    let mut phrase = lexical_leaf("lex", "zebra document");
    if let Some(selection_query::Node::Search(SearchQuery {
        query: Some(search_query::Query::Lexical(q)),
        ..
    })) = phrase.node.as_mut()
    {
        q.phrase = Some(pipestream_search::pb::PhraseMatch::default());
    }
    let err = query(
        &coordinator,
        QueryRequest {
            k: 5,
            selection: Some(phrase),
            sort: vec![QuerySort {
                column: "year".into(),
                descending: true,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("a phrase"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collapse_groups_the_pool_by_key_with_inner_hits_and_pages() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let collapse = |column: &str, inner_hits: u32| {
        Some(CollapseSpec {
            column: column.into(),
            inner_hits,
        })
    };
    // The uncollapsed dense order over the whole corpus is the oracle:
    // groups appear in the order of their best hit.
    let full = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(dense_leaf("d", &qvec)),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.hits.len(), 8);
    let mut expected_groups: Vec<(u64, Vec<u64>)> = Vec::new();
    for h in &full.hits {
        let parent = h.doc_id / 2;
        match expected_groups.iter_mut().find(|(p, _)| *p == parent) {
            Some((_, members)) => members.push(h.doc_id),
            None => expected_groups.push((parent, vec![h.doc_id])),
        }
    }
    assert_eq!(expected_groups.len(), 4);

    // k = 2 groups by parent over a single dense leaf: the pool deepens
    // from k until two parents show, and the representatives are the
    // groups' best hits in relevance order.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(dense_leaf("d", &qvec)),
            collapse: collapse("parent_id", 2),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.executed, "search+collapse");
    let reps: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(
        reps,
        expected_groups
            .iter()
            .take(2)
            .map(|(_, m)| m[0])
            .collect::<Vec<_>>()
    );
    assert_eq!(resp.groups.len(), 2);
    for (i, (group, (parent, _))) in resp.groups.iter().zip(&expected_groups).enumerate() {
        assert_eq!(
            group.key,
            Some(pipestream_search::pb::SortValue {
                value: Some(pipestream_search::pb::sort_value::Value::UnsignedInteger(
                    *parent
                ))
            })
        );
        assert!(group.hits.len() <= 2 && !group.hits.is_empty());
        assert_eq!(
            group.hits[0].doc_id, reps[i],
            "the representative leads its group"
        );
        assert_eq!(group.hits[0].rank, 1, "ranks count within the group");
        assert!(group.hits.iter().all(|h| h.doc_id / 2 == *parent));
    }

    // A pool deeper than the corpus comes back short, which is the proof
    // that every group is complete; a pool exactly the corpus's size
    // cannot tell the end from a cut and reports incomplete groups.
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 8,
            selection: Some(dense_leaf("d", &qvec)),
            collapse: collapse("parent_id", 3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        resp.groups.iter().all(|g| !g.complete),
        "a full pool proves nothing about what follows"
    );
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 16,
            selection: Some(dense_leaf("d", &qvec)),
            collapse: collapse("parent_id", 3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.hits.len(), 4);
    for (group, (parent, members)) in resp.groups.iter().zip(&expected_groups) {
        let listed: Vec<u64> = group.hits.iter().map(|h| h.doc_id).collect();
        assert_eq!(&listed, members, "parent {parent}");
        assert_eq!(group.pool_hits, 2);
        assert!(group.complete, "the pool reached the end of the corpus");
    }

    // Paging by one group stitches into the full group order.
    let mut stitched = Vec::new();
    let mut cursor = String::new();
    loop {
        let resp = query(
            &coordinator,
            QueryRequest {
                k: 1,
                selection: Some(dense_leaf("d", &qvec)),
                collapse: collapse("parent_id", 0),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        stitched.extend(resp.hits.iter().map(|h| (h.doc_id, h.rank)));
        assert!(resp.groups.iter().all(|g| g.hits.len() == 1));
        if resp.next_cursor.is_empty() {
            break;
        }
        cursor = resp.next_cursor;
    }
    assert_eq!(
        stitched,
        expected_groups
            .iter()
            .enumerate()
            .map(|(i, (_, m))| (m[0], (i + 1) as u32))
            .collect::<Vec<_>>()
    );

    // A facet key on a lexical leaf: "document" matches everything, so
    // the groups are the three courts in the order of their best hit.
    let lexical_full = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut courts_in_order: Vec<&str> = Vec::new();
    for h in &lexical_full.hits {
        let court = COURTS[h.doc_id as usize % 3];
        if !courts_in_order.contains(&court) {
            courts_in_order.push(court);
        }
    }
    let resp = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(lexical_leaf("lex", "document")),
            collapse: collapse("court", 1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let keys: Vec<String> = resp
        .groups
        .iter()
        .map(|g| match g.key.as_ref().unwrap().value.as_ref().unwrap() {
            pipestream_search::pb::sort_value::Value::Text(t) => t.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(keys, courts_in_order);
    assert_eq!(resp.executed, "bm25_search+collapse");

    // A composite is a fixed pool: selection_k = 8 holds every parent;
    // a filtered pool of four hits that all share one lineage group
    // cannot show two groups and refuses naming selection_k.
    let composite_of = |selection_k: u32, k: u32, column: &str, filter: Option<SelectionQuery>| {
        let fused = composite(
            SelectionOperator::Or,
            vec![dense_leaf("d", &qvec), lexical_leaf("lex", "document")],
            Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
        );
        QueryRequest {
            k,
            selection_k,
            selection: Some(match filter {
                Some(f) => composite(SelectionOperator::And, vec![fused, f], None),
                None => fused,
            }),
            collapse: collapse(column, 2),
            ..Default::default()
        }
    };
    let resp = query(&coordinator, composite_of(8, 4, "parent_id", None))
        .await
        .unwrap();
    assert_eq!(resp.hits.len(), 4);
    assert_eq!(resp.groups.len(), 4);
    assert_eq!(resp.executed, "hybrid_search:global_rank+collapse");
    let err = query(
        &coordinator,
        composite_of(4, 2, "group_id", Some(cel_filter("f", "year < 4"))),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "{}",
        err.message()
    );
    assert!(err.message().contains("selection_k"), "{}", err.message());

    // Refusals by name: a browse has no order to collapse by; collapse
    // and sort do not combine.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(cel_filter("f", "year >= 0")),
            collapse: collapse("court", 1),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("SCORED selection"),
        "{}",
        err.message()
    );
    let err = query(
        &coordinator,
        QueryRequest {
            k: 3,
            selection: Some(lexical_leaf("lex", "document")),
            collapse: collapse("court", 1),
            sort: vec![QuerySort {
                column: "year".into(),
                descending: true,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("do not combine"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_is_exact_certified_and_retry_stable() {
    let (coordinator, qvec, _handles) = start_cluster().await;

    for (name, request, provisional_phase) in [
        (
            "lexical",
            QueryRequest {
                request_id: "stream-lexical".into(),
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                ..Default::default()
            },
            QueryStreamPhase::Lexical,
        ),
        (
            "dense",
            QueryRequest {
                request_id: "stream-dense".into(),
                k: 5,
                selection: Some(dense_leaf("vec", &qvec)),
                ..Default::default()
            },
            QueryStreamPhase::Dense,
        ),
    ] {
        let unary = query(&coordinator, request.clone()).await.unwrap();
        let events = streamed_query(&coordinator, Some(request.clone()), 0).await;
        let (revisions, completion) = stream_parts(&events);
        assert!(
            completion.completed,
            "{name}: terminal failure: {completion:?}"
        );
        assert_eq!(completion.error_code, 0);
        assert!(completion.error_message.is_empty());
        assert_eq!(
            completion.response.as_ref().unwrap().encode_to_vec(),
            unary.encode_to_vec(),
            "{name}: streamed terminal response differs from unary Query"
        );
        assert_eq!(
            completion.final_revision,
            revisions.last().unwrap().revision
        );
        assert!(!completion.scoring_fingerprints.is_empty());
        assert!(completion
            .scoring_fingerprints
            .iter()
            .all(|fingerprint| !fingerprint.is_empty()));

        assert_eq!(
            QueryStreamPhase::try_from(revisions[0].phase).unwrap(),
            QueryStreamPhase::Accepted
        );
        assert!(
            revisions.iter().any(|revision| {
                QueryStreamPhase::try_from(revision.phase).unwrap() == provisional_phase
            }),
            "{name}: collector never published a provisional revision"
        );
        let final_revision = revisions.last().unwrap();
        assert_eq!(
            QueryStreamPhase::try_from(final_revision.phase).unwrap(),
            QueryStreamPhase::Final
        );
        let final_rows: Vec<(u64, u32, u32)> = final_revision
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits(), hit.rank))
            .collect();
        let expected_rows: Vec<(u64, u32, u32)> = unary
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits(), hit.rank))
            .collect();
        assert_eq!(final_rows, expected_rows, "{name}: FINAL order");

        let mut fingerprints = std::collections::HashSet::new();
        for (position, revision) in revisions.iter().enumerate() {
            assert_eq!(revision.revision, position as u64 + 1, "{name}: gap");
            assert!(
                fingerprints.insert(revision.content_fingerprint.as_str()),
                "{name}: duplicate replacement snapshot"
            );
            for (rank, hit) in revision.hits.iter().enumerate() {
                assert_eq!(hit.rank, rank as u32 + 1, "{name}: rank column");
            }
        }

        // Timing may conflate intermediate replacement snapshots, but the
        // authoritative content and score-space identities are retry-stable.
        let retry = streamed_query(&coordinator, Some(request), 0).await;
        let (retry_revisions, retry_completion) = stream_parts(&retry);
        assert!(retry_completion.completed);
        assert_eq!(
            retry_revisions.last().unwrap().content_fingerprint,
            final_revision.content_fingerprint,
            "{name}: FINAL content fingerprint changed on retry"
        );
        assert_eq!(
            retry_completion.scoring_fingerprints, completion.scoring_fingerprints,
            "{name}: score-space identities changed on retry"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_errors_are_terminal_not_partial_success() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let events = streamed_query(&coordinator, None, 0).await;
    let (revisions, completion) = stream_parts(&events);
    assert_eq!(revisions.len(), 1);
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_eq!(completion.final_revision, 1);
    assert_eq!(completion.error_code, tonic::Code::InvalidArgument as u32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_missing_shard_cannot_certify_completion() {
    let ((_, qvec, _handles), mut addrs) = start_cluster_with_addrs().await;
    addrs.push("http://127.0.0.1:1".into());
    let coordinator = CoordinatorServiceImpl::new(addrs);
    let events = streamed_query(
        &coordinator,
        Some(QueryRequest {
            request_id: "stream-missing-shard".into(),
            k: 5,
            selection: Some(dense_leaf("vec", &qvec)),
            ..Default::default()
        }),
        1_000,
    )
    .await;
    let (_, completion) = stream_parts(&events);
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_ne!(completion.error_code, 0);
    assert!(!completion.error_message.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_deadline_and_client_drop_cancel_analysis() {
    use common::mock::start_mock_analysis_delayed;
    use std::time::{Duration, Instant};

    let ((_, _qvec, _handles), addrs) = start_cluster_with_addrs().await;
    let (analysis, _slow_handle, probe) = start_mock_analysis_delayed(Duration::from_secs(5)).await;
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    let request = || QueryRequest {
        k: 5,
        selection: Some(lexical_leaf("lex", "zebra")),
        ..Default::default()
    };

    let events = streamed_query(&coordinator, Some(request()), 25).await;
    let (_, completion) = stream_parts(&events);
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_eq!(completion.error_code, tonic::Code::DeadlineExceeded as u32);

    let cancelled_after_deadline = Instant::now() + Duration::from_secs(2);
    while probe.cancelled() < 1 && Instant::now() < cancelled_after_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(probe.started(), 1, "deadline must start one analysis call");
    assert_eq!(
        probe.completed(),
        0,
        "deadline must not let analysis finish"
    );
    assert_eq!(probe.cancelled(), 1, "deadline must drop analysis");

    let mut stream = coordinator
        .query_stream(Request::new(QueryStreamRequest {
            collection: String::new(),
            query: Some(request()),
            timeout_ms: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let accepted = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        accepted.payload,
        Some(query_stream_response::Payload::Revision(_))
    ));
    let started = Instant::now() + Duration::from_secs(2);
    while probe.started() < 2 && Instant::now() < started {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(probe.started(), 2, "cancel test never reached analysis");
    drop(stream);

    let cancelled_after_drop = Instant::now() + Duration::from_secs(2);
    while probe.cancelled() < 2 && Instant::now() < cancelled_after_drop {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        probe.completed(),
        0,
        "dropped client must not finish analysis"
    );
    assert_eq!(probe.cancelled(), 2, "dropped client must cancel execution");
}
