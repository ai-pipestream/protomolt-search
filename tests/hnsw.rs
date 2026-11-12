//! The HNSW graph provider (`docs/hnsw.md`) served by real nodes behind a
//! coordinator: it is selected by `vector_backend`, it is never treated as
//! exact (EXACT, the legacy unspecified mode and AUTO without a policy
//! refuse naming it), explicit ANN finds the FP32 top-k through a graph each
//! node builds on seal/flush (while unsealed tails enumerate) and reports
//! exact FP32 scores, AUTO qualifies it only through a policy bound to its
//! scoring fingerprint, and the streaming scan reaches it with the request's
//! candidate depth.

mod common;

use std::path::PathBuf;

use common::{start_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::DenseExecutionPolicy;
use pipestream_search::hnsw::{HnswConfig, HNSW};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    filter_query, search_query, selection_query, stream_search_request, stream_search_response,
    AddDocumentsRequest, AddVectorsRequest, BooleanQuery, CompositeSearchStrategy,
    DenseExecutionMode, DenseFilterRegimes, DenseQuery, DenseScoreMode, FacetValue, FilterQuery,
    FlushRequest, GetVectorBackendRequest, QueryRequest, QueryResponse, SearchQuery,
    SelectionOperator, SelectionQuery, StartStreamSearch, StreamSearchRequest,
    VectorQualityContract,
};
use pipestream_search::vector::VectorIndex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 32;
const N_DOCS: usize = 2200;
const SHARDS: usize = 2;
const GENERATION: u64 = 3;

struct Fixture {
    coordinator: CoordinatorServiceImpl,
    addrs: Vec<String>,
    corpus: Vec<f32>,
    query_vector: Vec<f32>,
    fingerprint: String,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

fn graph_index() -> VectorIndex {
    VectorIndex::create(HNSW, DIM, 8).unwrap()
}

async fn start_fixture() -> Fixture {
    let corpus = unit_vectors(N_DOCS, DIM, 0x4A5E_0001);
    let per_shard = N_DOCS / SHARDS;
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..SHARDS {
        let (addr, handle) = start_node(
            graph_index(),
            NodeConfig {
                slot_offset: (shard * per_shard) as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(per_shard);
        for i in 0..per_shard {
            tx.send(AddDocumentsRequest {
                text: format!("document {} about the appeal", shard * per_shard + i),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * per_shard;
        // Two batches: a bulk build, then tail insertion into the live graph.
        let split = start + per_shard / 2;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..split * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        vtx.send(AddVectorsRequest {
            vectors: corpus[split * DIM..(start + per_shard) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        // Flush builds the HNSW graph over all appended rows; the node reports
        // the provider and its config.
        client.flush(FlushRequest {}).await.unwrap();
        let backend = client
            .get_vector_backend(GetVectorBackendRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(backend.num_vectors, per_shard as u64);
        assert_eq!(backend.descriptor.unwrap().backend_kind, HNSW);
        assert_eq!(backend.config.unwrap().backend_kind, HNSW);
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    Fixture {
        coordinator,
        addrs,
        corpus,
        query_vector: unit_vectors(1, DIM, 0x4A5E_0002),
        fingerprint: graph_index().descriptor().scoring_fingerprint,
        handles,
    }
}

fn dense_request(vector: &[f32], k: u32, mode: DenseExecutionMode) -> QueryRequest {
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    execution_mode: mode as i32,
                    score_mode: DenseScoreMode::Unspecified as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
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

/// The FP32 inner-product top `k` as `(doc id, score)`.
fn fp32_top(corpus: &[f32], query: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut all: Vec<(u64, f32)> = corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .map(|(id, row)| (id as u64, row.iter().zip(query).map(|(a, b)| a * b).sum()))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_ann_returns_the_fp32_top_k_and_reports_the_graph() {
    let fixture = start_fixture().await;
    let queries = unit_vectors(8, DIM, 0x4A5E_0003);
    let mut found = 0;
    let mut response = None;
    for query_vector in queries.as_chunks::<DIM>().0 {
        let answer = query(
            &fixture.coordinator,
            dense_request(query_vector, 5, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(answer.hits.len(), 5);
        let truth = fp32_top(&fixture.corpus, query_vector, 5);
        for hit in &answer.hits {
            let row = &fixture.corpus[hit.doc_id as usize * DIM..][..DIM];
            let exact: f32 = row.iter().zip(query_vector).map(|(a, b)| a * b).sum();
            assert!(
                (hit.score - exact).abs() < 1.0e-5,
                "{} vs {exact}",
                hit.score
            );
            found += truth.iter().any(|t| t.0 == hit.doc_id) as usize;
        }
        response = Some(answer);
    }
    assert!(found >= 36, "recall@5 {found}/40");
    let response = response.unwrap();
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.provider_backend, HNSW);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(
        outcome.quality_contract,
        VectorQualityContract::ConfiguredAnn as i32
    );
    assert!(!outcome.exhaustive_completion);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_graph_is_never_taken_for_exact() {
    let fixture = start_fixture().await;
    for mode in [
        DenseExecutionMode::Exact,
        DenseExecutionMode::Unspecified,
        DenseExecutionMode::Auto,
    ] {
        let error = query(
            &fixture.coordinator,
            dense_request(&fixture.query_vector, 5, mode),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{mode:?}");
        assert!(
            error.message().contains(HNSW),
            "{mode:?}: {}",
            error.message()
        );
    }
}

/// A policy document through the file, as a coordinator loads it.
fn load_policy(tag: &str, text: &str) -> DenseExecutionPolicy {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("hnsw_policy_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.toml");
    std::fs::write(&path, text).unwrap();
    let policy = DenseExecutionPolicy::load(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    policy
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_qualifies_the_graph_only_through_its_own_fingerprint() {
    let fixture = start_fixture().await;
    let policy_for = |fingerprint: &str, tag: &str| {
        let text = format!(
            "format_version = 1\npolicy_id = \"hnsw-v1\"\nembedding_model = \"test-embed\"\n\
             corpus_generation = {GENERATION}\ncorpus_rows = {N_DOCS}\ndimensions = {DIM}\n\
             provider_backend = \"{HNSW}\"\nscoring_fingerprint = \"{fingerprint}\"\n\
             measured_queries = 16\n\n[[points]]\nk = 5\n\
             filter_selectivity_ppm_min = 1000000\nfilter_selectivity_ppm_max = 1000000\n\
             candidates = 10\nmeasured_recall_ppm = 990000\n"
        );
        load_policy(tag, &text)
    };

    // A policy measured at another graph configuration does not qualify.
    let other = VectorIndex::from_backend_config(
        DIM,
        &HnswConfig {
            ef_construction: 300,
            ..Default::default()
        }
        .backend_config()
        .unwrap(),
    )
    .unwrap()
    .descriptor()
    .scoring_fingerprint;
    assert_ne!(other, fixture.fingerprint);
    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy_for(&other, "other"));
    let error = query(
        &coordinator,
        dense_request(&fixture.query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains("scoring_fingerprint"),
        "{}",
        error.message()
    );

    let coordinator = fixture
        .coordinator
        .clone()
        .with_dense_execution_policy(policy_for(&fixture.fingerprint, "own"));
    let response = query(
        &coordinator,
        dense_request(&fixture.query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .unwrap();
    assert_eq!(response.hits.len(), 5);
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(outcome.policy_id, "hnsw-v1");
    assert_eq!(outcome.candidate_depth, 10);
}

/// The streaming scan reaches each node's graph walk with the request's
/// depth, and a seeded floor only filters what the walk returns: seeding it
/// with the unfloored k-th best must keep the identical top-k rather than
/// stopping the walk at the entry point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seeded_floor_filters_the_walk_without_cutting_it_short() {
    let fixture = start_fixture().await;
    let queries = unit_vectors(4, DIM, 0x4A5E_0004);
    for (q, query_vector) in queries.as_chunks::<DIM>().0.iter().enumerate() {
        let unfloored = fixture
            .coordinator
            .fanout_stream_search(
                &format!("hnsw-base-{q}"),
                query_vector,
                5,
                None,
                &Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(unfloored.hits.len(), 5);
        let truth = fp32_top(&fixture.corpus, query_vector, 5);
        let found = unfloored
            .hits
            .iter()
            .filter(|h| truth.iter().any(|t| t.0 == h.vector_id))
            .count();
        assert!(found >= 4, "query {q}: streamed recall@5 {found}/5");
        let kth = unfloored.hits.last().unwrap().score;
        let floored = fixture
            .coordinator
            .fanout_stream_search(
                &format!("hnsw-floor-{q}"),
                query_vector,
                5,
                Some(kth),
                &Default::default(),
            )
            .await
            .unwrap();
        let ids = |hits: &[pipestream_search::pb::ScoredHit]| {
            hits.iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&floored.hits), ids(&unfloored.hits), "query {q}");
    }
}

/// One node's streaming scan, driven directly: the rows it emitted as
/// `(global id, exact score)` and the terminal summary.
async fn node_stream(
    addr: &str,
    vector: &[f32],
    candidate_depth: Option<u32>,
) -> Result<(Vec<(u64, f32)>, pipestream_search::pb::StreamSearchSummary), tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(2);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: format!("depth-{candidate_depth:?}"),
            vector: vector.to_vec(),
            candidate_depth,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await?
        .into_inner();
    let mut hits = Vec::new();
    loop {
        let message = inbound.message().await?.expect("terminal summary");
        match message.payload {
            Some(stream_search_response::Payload::Batch(batch)) => {
                for record in batch.hits.as_chunks::<12>().0 {
                    let id = u64::from_le_bytes(record[..8].try_into().unwrap());
                    let score = f32::from_le_bytes(record[8..].try_into().unwrap());
                    hits.push((id, score));
                }
            }
            Some(stream_search_response::Payload::Summary(summary)) => return Ok((hits, summary)),
            _ => {}
        }
    }
}

/// Contract 1 of #70: the candidate depth reaches the provider on a real
/// node's streaming path and sizes the walk — a deeper request scores and
/// emits more rows and a different set — a stream without a depth is refused
/// by the provider's name, every emitted score is the exact FP32 product,
/// and the dense execution outcome reports the depth each request executed
/// at.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidate_depth_sizes_the_walk_and_is_reported() {
    let fixture = start_fixture().await;
    let node = &fixture.addrs[0];
    let query_vector = &fixture.query_vector;

    let refused = node_stream(node, query_vector, None).await.unwrap_err();
    assert!(
        refused.message().contains(HNSW) && refused.message().contains("candidate depth"),
        "{}",
        refused.message()
    );

    let (shallow, shallow_summary) = node_stream(node, query_vector, Some(16)).await.unwrap();
    let (deep, deep_summary) = node_stream(node, query_vector, Some(400)).await.unwrap();
    assert!(shallow_summary.completed && deep_summary.completed);
    assert!(
        deep.len() > shallow.len() && deep_summary.blocks_scanned > shallow_summary.blocks_scanned,
        "depth 16 emitted {} rows over {} scored, depth 400 emitted {} over {}",
        shallow.len(),
        shallow_summary.blocks_scanned,
        deep.len(),
        deep_summary.blocks_scanned
    );
    let shallow_ids: std::collections::BTreeSet<u64> = shallow.iter().map(|h| h.0).collect();
    let deep_ids: std::collections::BTreeSet<u64> = deep.iter().map(|h| h.0).collect();
    assert_ne!(shallow_ids, deep_ids);
    for (id, score) in shallow.iter().chain(&deep) {
        let row = &fixture.corpus[*id as usize * DIM..][..DIM];
        let exact = pipestream_search::exact_vectors::dot(row, query_vector);
        assert_eq!(score.to_bits(), exact.to_bits(), "row {id}");
    }

    // The outcome reports the depth the shards were asked for: k for a plain
    // ANN selection, selection_k when the FP32 rerank names a deeper pool.
    for k in [5u32, 40] {
        let response = query(
            &fixture.coordinator,
            dense_request(query_vector, k, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(response.hits.len(), k as usize);
        assert_eq!(response.dense_execution.unwrap().candidate_depth, k);
    }
    let mut pooled = dense_request(query_vector, 5, DenseExecutionMode::Ann);
    pooled.selection_k = 64;
    if let Some(selection_query::Node::Search(search)) =
        pooled.selection.as_mut().and_then(|s| s.node.as_mut())
    {
        if let Some(search_query::Query::Dense(dense)) = search.query.as_mut() {
            dense.score_mode = DenseScoreMode::Fp32Rerank as i32;
        }
    }
    let response = query(&fixture.coordinator, pooled).await.unwrap();
    assert_eq!(response.hits.len(), 5);
    assert_eq!(response.dense_execution.unwrap().candidate_depth, 64);
}

const FILTERED_SHARD_DOCS: usize = 7800;

/// Global doc id -> court: every even id is "wide" (half of each shard, well
/// above both the enumerate threshold and visit budget at this beam), one odd
/// id in 300 is "narrow" (26 rows a shard), one odd id in 30 is "sparse" (260
/// rows a shard, bridge-heavy when walked), the rest are "rest".
fn court_of(id: usize) -> &'static str {
    if id.is_multiple_of(2) {
        "wide"
    } else if id % 300 == 1 {
        "narrow"
    } else if id % 30 == 3 {
        "sparse"
    } else {
        "rest"
    }
}

/// Two graph shards, each larger than one scan chunk, whose documents carry
/// a `court` facet.
async fn start_filtered_fixture() -> (Fixture, Vec<String>) {
    let total = FILTERED_SHARD_DOCS * SHARDS;
    let corpus = unit_vectors(total, DIM, 0x4A5E_0055);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..SHARDS {
        let start = shard * FILTERED_SHARD_DOCS;
        let (addr, handle) = start_node(
            graph_index(),
            NodeConfig {
                slot_offset: start as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                facet_fields: vec!["court".to_string()],
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        let feeder = tokio::spawn(async move {
            for id in start..start + FILTERED_SHARD_DOCS {
                tx.send(AddDocumentsRequest {
                    text: format!("opinion {id}"),
                    analysis: Some(body_spec()),
                    facets: vec![FacetValue {
                        field: "court".into(),
                        value: court_of(id).into(),
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            }
        });
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        feeder.await.unwrap();
        let (vtx, vrx) = mpsc::channel(1);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + FILTERED_SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        client.flush(FlushRequest {}).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let fixture = Fixture {
        coordinator: CoordinatorServiceImpl::new(addrs.clone()),
        addrs: addrs.clone(),
        corpus,
        query_vector: Vec::new(),
        fingerprint: String::new(),
        handles,
    };
    (fixture, addrs)
}

fn filtered_dense_request(
    vector: &[f32],
    k: u32,
    court: &str,
    mode: DenseExecutionMode,
) -> QueryRequest {
    let dense = dense_request(vector, k, mode).selection.unwrap();
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![
                    dense,
                    SelectionQuery {
                        node: Some(selection_query::Node::Filter(FilterQuery {
                            id: "court".into(),
                            predicate: Some(filter_query::Predicate::Cel(format!(
                                r#"court == "{court}""#
                            ))),
                        })),
                    },
                ],
                scoring: None,
            })),
        }),
        ..Default::default()
    }
}

/// #55 through the product: a filtered ANN query on shards larger than one
/// scan chunk answers from one search of each shard's whole graph, on the
/// unary route and the streaming one alike. A mask of half the rows is
/// walked filter-aware and holds its recall against the exact filtered top-k; a
/// mask of twenty rows a shard is enumerated, which is exact. Every hit
/// passes the filter and carries its exact FP32 score, and the response says
/// which regime each shard answered in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_ann_query_reports_its_regime_and_keeps_recall() {
    const K: usize = 20;
    let (fixture, addrs) = start_filtered_fixture().await;
    let queries = unit_vectors(8, DIM, 0x4A5E_0056);
    let truth = |query: &[f32], court: &str| -> Vec<u64> {
        let mut all: Vec<(u64, f32)> = fixture
            .corpus
            .as_chunks::<DIM>()
            .0
            .iter()
            .enumerate()
            .filter(|(id, _)| court_of(*id) == court)
            .map(|(id, row)| (id as u64, pipestream_search::exact_vectors::dot(row, query)))
            .collect();
        all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        all.truncate(K);
        all.into_iter().map(|hit| hit.0).collect()
    };
    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        for (court, regimes, floor) in [
            (
                "wide",
                DenseFilterRegimes {
                    filter_aware: SHARDS as u32,
                    ..Default::default()
                },
                // A beam of 20 over uniform random vectors: measured 0.88 to
                // 0.94 across builds; a walk that lost the mask or the
                // graph falls far below this.
                0.75,
            ),
            (
                "narrow",
                DenseFilterRegimes {
                    enumerate: SHARDS as u32,
                    ..Default::default()
                },
                1.0,
            ),
        ] {
            let mut found = 0;
            for query_vector in queries.as_chunks::<DIM>().0 {
                let response = query(
                    &coordinator,
                    filtered_dense_request(query_vector, K as u32, court, DenseExecutionMode::Ann),
                )
                .await
                .unwrap();
                assert_eq!(response.hits.len(), K, "streaming={streaming} {court}");
                let expected = truth(query_vector, court);
                for hit in &response.hits {
                    assert_eq!(court_of(hit.doc_id as usize), court);
                    let row = &fixture.corpus[hit.doc_id as usize * DIM..][..DIM];
                    let exact = pipestream_search::exact_vectors::dot(row, query_vector);
                    assert_eq!(hit.score.to_bits(), exact.to_bits(), "row {}", hit.doc_id);
                    found += expected.contains(&hit.doc_id) as usize;
                }
                let outcome = response.dense_execution.unwrap();
                assert_eq!(outcome.provider_backend, HNSW);
                assert_eq!(
                    outcome.filter_regimes,
                    Some(regimes),
                    "streaming={streaming} {court}"
                );
            }
            let recall = found as f64 / (queries.len() / DIM * K) as f64;
            assert!(
                recall >= floor,
                "streaming={streaming} {court}: recall@{K} {recall}"
            );
        }
    }
    // An unfiltered query has no regime to report.
    let response = query(
        &CoordinatorServiceImpl::new(addrs).with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        ),
        dense_request(
            queries.as_chunks::<DIM>().0[0].as_slice(),
            K as u32,
            DenseExecutionMode::Ann,
        ),
    )
    .await
    .unwrap();
    assert_eq!(response.dense_execution.unwrap().filter_regimes, None);
}

/// Recursive Boolean evaluation needs every member's dense score, and a
/// graph walk reaches only part of a mask. On graph shards the dense pass
/// runs in the enumerate regime instead, which scores every member exactly:
/// a Boolean dense leaf returns the exact FP32 order of its match set, alone
/// and under a filter clause, while ANN stays refused inside a Boolean as on
/// every provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boolean_dense_leaves_are_answered_exactly_by_enumerate() {
    const K: usize = 10;
    let (fixture, addrs) = start_filtered_fixture().await;
    let coordinator = CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    let queries = unit_vectors(4, DIM, 0x4A5E_0057);
    let boolean = |vector: &[f32], mode: DenseExecutionMode, court: Option<&str>| {
        let mut must = vec![dense_request(vector, K as u32, mode).selection.unwrap()];
        if let Some(court) = court {
            must.push(SelectionQuery {
                node: Some(selection_query::Node::Filter(FilterQuery {
                    id: "court".into(),
                    predicate: Some(filter_query::Predicate::Cel(format!(
                        r#"court == "{court}""#
                    ))),
                })),
            });
        }
        QueryRequest {
            k: K as u32,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must,
                    ..Default::default()
                })),
            }),
            ..Default::default()
        }
    };
    for query_vector in queries.as_chunks::<DIM>().0 {
        for mode in [DenseExecutionMode::Exact, DenseExecutionMode::Unspecified] {
            for court in [None, Some("wide"), Some("narrow")] {
                let mut expected: Vec<(u64, f32)> = fixture
                    .corpus
                    .as_chunks::<DIM>()
                    .0
                    .iter()
                    .enumerate()
                    .filter(|(id, _)| court.is_none_or(|court| court_of(*id) == court))
                    .map(|(id, row)| {
                        (
                            id as u64,
                            pipestream_search::exact_vectors::dot(row, query_vector),
                        )
                    })
                    .collect();
                expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                expected.truncate(K);
                let response = query(&coordinator, boolean(query_vector, mode, court))
                    .await
                    .unwrap();
                let got: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
                let want: Vec<u64> = expected.iter().map(|hit| hit.0).collect();
                assert_eq!(got, want, "{mode:?} {court:?}");
            }
        }
        let error = query(
            &coordinator,
            boolean(query_vector, DenseExecutionMode::Ann, None),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("ANN"), "{}", error.message());
    }
}

/// AUTO serves a filtered query under the enumerate threshold its policy
/// point was measured with, not the provider's own. The same half-corpus
/// mask is walked filter-aware under a threshold below its 3900 rows a shard
/// and enumerated, exactly, under one above; the outcome names the
/// threshold; and a filtered point that names none is refused, because the
/// regime that would serve it is not the one anything measured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_policy_point_serves_a_filter_under_its_measured_threshold() {
    const K: usize = 20;
    let (fixture, addrs) = start_filtered_fixture().await;
    let fingerprint = graph_index().descriptor().scoring_fingerprint;
    let rows = FILTERED_SHARD_DOCS * SHARDS;
    let policy = |tag: &str, threshold: Option<u32>| {
        let threshold =
            threshold.map_or_else(String::new, |rows| format!("enumerate_max_rows = {rows}\n"));
        load_policy(
            tag,
            &format!(
                "format_version = 1\npolicy_id = \"hnsw-filtered\"\n\
                 embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
                 corpus_rows = {rows}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
                 scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
                 [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 400000\n\
                 filter_selectivity_ppm_max = 600000\ncandidates = {K}\n\
                 measured_recall_ppm = 900000\n{threshold}"
            ),
        )
    };
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0058);
    let mut exact: Vec<(u64, f32)> = fixture
        .corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .filter(|(id, _)| court_of(*id) == "wide")
        .map(|(id, row)| {
            (
                id as u64,
                pipestream_search::exact_vectors::dot(row, &query_vector),
            )
        })
        .collect();
    exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    exact.truncate(K);
    let exact: Vec<u64> = exact.into_iter().map(|hit| hit.0).collect();
    let request =
        || filtered_dense_request(&query_vector, K as u32, "wide", DenseExecutionMode::Auto);
    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_topology_generation(GENERATION)
            .with_stream_search(streaming);

        let walked = coordinator
            .clone()
            .with_dense_execution_policy(policy("walked", Some(100)));
        let response = query(&walked, request()).await.unwrap();
        assert_eq!(response.hits.len(), K);
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
        assert_eq!(outcome.filter_selectivity_ppm, 500_000);
        assert_eq!(outcome.policy_point.unwrap().enumerate_max_rows, Some(100));
        assert!(
            outcome
                .planner_reason
                .contains("enumerate threshold 100 rows"),
            "{}",
            outcome.planner_reason
        );
        assert_eq!(
            outcome.filter_regimes,
            Some(DenseFilterRegimes {
                filter_aware: SHARDS as u32,
                ..Default::default()
            }),
            "streaming={streaming}"
        );

        let enumerated = coordinator
            .clone()
            .with_dense_execution_policy(policy("enumerated", Some(4000)));
        let response = query(&enumerated, request()).await.unwrap();
        let outcome = response.dense_execution.unwrap();
        assert_eq!(
            outcome.filter_regimes,
            Some(DenseFilterRegimes {
                enumerate: SHARDS as u32,
                ..Default::default()
            }),
            "streaming={streaming}"
        );
        let got: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
        assert_eq!(got, exact, "streaming={streaming}");

        let unmeasured = coordinator
            .clone()
            .with_dense_execution_policy(policy("unmeasured", None));
        let error = query(&unmeasured, request()).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("names no enumerate_max_rows"),
            "{}",
            error.message()
        );

        // A bridge-heavy sparse filter just above the policy's enumerate
        // threshold starts filter-aware, hits its visit budget mid-walk,
        // finishes by enumerating the remaining allowed rows without
        // duplicates, and reports both `enumerate` and `budget_handoffs`.
        let budget_policy = load_policy(
            "budget-handoff",
            &format!(
                "format_version = 1\npolicy_id = \"hnsw-budget\"\n\
                 embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
                 corpus_rows = {rows}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
                 scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
                 [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 20000\n\
                 filter_selectivity_ppm_max = 50000\ncandidates = {K}\n\
                 measured_recall_ppm = 900000\nenumerate_max_rows = 150\n"
            ),
        );
        let mut exact_sparse: Vec<(u64, f32)> = fixture
            .corpus
            .as_chunks::<DIM>()
            .0
            .iter()
            .enumerate()
            .filter(|(id, _)| court_of(*id) == "sparse")
            .map(|(id, row)| {
                (
                    id as u64,
                    pipestream_search::exact_vectors::dot(row, &query_vector),
                )
            })
            .collect();
        exact_sparse.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        exact_sparse.truncate(K);
        let exact_sparse: Vec<u64> = exact_sparse.into_iter().map(|hit| hit.0).collect();

        let budgeted = coordinator
            .clone()
            .with_dense_execution_policy(budget_policy);
        let response = query(
            &budgeted,
            filtered_dense_request(&query_vector, K as u32, "sparse", DenseExecutionMode::Auto),
        )
        .await
        .unwrap();
        let outcome = response.dense_execution.unwrap();
        assert_eq!(
            outcome.filter_regimes,
            Some(DenseFilterRegimes {
                enumerate: SHARDS as u32,
                budget_handoffs: SHARDS as u32,
                ..Default::default()
            }),
            "streaming={streaming}"
        );
        let got_sparse: Vec<u64> = response.hits.iter().map(|hit| hit.doc_id).collect();
        assert_eq!(got_sparse, exact_sparse, "streaming={streaming}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsealed_tail_enumerates_until_flushed() {
    let per_shard = 400usize;
    let total = per_shard * SHARDS;
    let corpus = unit_vectors(total, DIM, 0x4A5E_0097);
    let query_vector = unit_vectors(1, DIM, 0x4A5E_9700);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..SHARDS {
        let start = shard * per_shard;
        let (addr, handle) = start_node(
            graph_index(),
            NodeConfig {
                slot_offset: start as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(per_shard);
        for i in 0..per_shard {
            tx.send(AddDocumentsRequest {
                text: format!("unsealed doc {}", start + i),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        client
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                vectors: corpus[start * DIM..(start + per_shard) * DIM].to_vec(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap();
        addrs.push(addr);
        handles.push(handle);
    }

    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        let unsealed = query(
            &coordinator,
            dense_request(&query_vector, 5, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(
            unsealed.dense_execution.unwrap().tail_enumerated_rows,
            total as u64,
            "streaming={streaming}: unsealed shards must report tail_enumerated_rows"
        );
    }

    for addr in &addrs {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client.flush(FlushRequest {}).await.unwrap();
    }

    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_stream_search(streaming);
        let sealed = query(
            &coordinator,
            dense_request(&query_vector, 5, DenseExecutionMode::Ann),
        )
        .await
        .unwrap();
        assert_eq!(
            sealed.dense_execution.unwrap().tail_enumerated_rows,
            0,
            "streaming={streaming}: flushed shards (>= MIN_GRAPH_ROWS) must build graphs"
        );
    }

    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_policy_point_serves_patience_and_reports_stopped_walks() {
    const K: u32 = 10;
    const DEPTH: u32 = 256;
    let (_fixture, addrs) = start_filtered_fixture().await;
    let fingerprint = graph_index().descriptor().scoring_fingerprint;
    let rows = FILTERED_SHARD_DOCS * SHARDS;
    let policy = load_policy(
        "patience-auto",
        &format!(
            "format_version = 1\npolicy_id = \"hnsw-patience\"\n\
             embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
             corpus_rows = {rows}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
             scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
             [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 1000000\n\
             filter_selectivity_ppm_max = 1000000\ncandidates = {DEPTH}\n\
             measured_recall_ppm = 990000\npatience_saturation_ppm = 900000\n\
             patience_hops = 3\n"
        ),
    );
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0095);
    for streaming in [false, true] {
        let coordinator = CoordinatorServiceImpl::new(addrs.clone())
            .with_bm25(
                Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                Default::default(),
            )
            .with_topology_generation(GENERATION)
            .with_stream_search(streaming)
            .with_dense_execution_policy(policy.clone());
        let response = query(
            &coordinator,
            dense_request(&query_vector, K, DenseExecutionMode::Auto),
        )
        .await
        .unwrap();
        assert_eq!(response.hits.len(), K as usize, "streaming={streaming}");
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
        assert_eq!(outcome.candidate_depth, DEPTH);
        let point = outcome.policy_point.unwrap();
        assert_eq!(point.patience_saturation_ppm, Some(900_000));
        assert_eq!(point.patience_hops, Some(3));
        assert!(
            outcome
                .planner_reason
                .contains("patience 900000 ppm over 3 hops"),
            "streaming={streaming}: {}",
            outcome.planner_reason
        );
        assert!(
            outcome.patience_stopped > 0,
            "streaming={streaming}: wide ef={DEPTH} walk with (s=0.90, p=3) must stop early on >=1 shard"
        );
    }

    // Direct node validation: half-specified patience is InvalidArgument.
    let mut node_client = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let half_err = node_client
        .stream_search(tokio_stream::iter(vec![
            pipestream_search::pb::StreamSearchRequest {
                payload: Some(
                    pipestream_search::pb::stream_search_request::Payload::Start(
                        pipestream_search::pb::StartStreamSearch {
                            request_id: "half-patience".into(),
                            vector: query_vector.clone(),
                            candidate_depth: Some(64),
                            patience_saturation_ppm: Some(990_000),
                            patience_hops: None,
                            ..Default::default()
                        },
                    ),
                ),
            },
        ]))
        .await;
    // Stream error arrives either immediately or as first item on the stream.
    let status = match half_err {
        Err(status) => status,
        Ok(resp) => resp.into_inner().message().await.unwrap_err(),
    };
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status
            .message()
            .contains("patience_saturation_ppm and patience_hops must be set together"),
        "{}",
        status.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finish_stream_search_flushes_straggler_resolves_identities_and_protects_exhaustive() {
    use pipestream_search::coordinator::RequestFilters;
    use pipestream_search::harness::serve_node;
    use pipestream_search::node::NodeServiceImpl;
    use pipestream_search::pb::{
        DocumentIdentity, FinishStreamSearch, StopStreamSearch, StreamEndReason,
    };
    use pipestream_search::vector::EMBEDDED_TURBOVEC;
    use std::time::Duration;

    const PER_SHARD: usize = 1200;
    const K: u32 = 10;
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0100);
    let mut corpus = unit_vectors(PER_SHARD * 2, DIM, 0x4A5E_0101);
    // Plant the unique global #1 hit (exact match = score 1.0) at row 0 of
    // shard 1 (global ID = PER_SHARD), so shard 1 discovers it in its very
    // first hops and flushes it via early first-flush before settle fires.
    corpus[PER_SHARD * DIM..(PER_SHARD + 1) * DIM].copy_from_slice(&query_vector);
    let planted_id = PER_SHARD as u64;
    let planted_identity = DocumentIdentity {
        document_key: vec![0xAB; 32],
        version: 11,
        chunk_ordinal: None,
    };

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2usize {
        let mut node = NodeServiceImpl::new(
            Some(graph_index()),
            NodeConfig {
                slot_offset: (shard * PER_SHARD) as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        );
        if shard == 1 {
            // 12 ms per hop: at candidate_depth = 200 (~40+ hops), a full walk
            // on shard 1 takes ~500 ms unless FinishStreamSearch ends it early.
            node = node.with_stream_poll_delay(Duration::from_millis(12));
        }
        let (addr, handle) = serve_node(node).await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(PER_SHARD);
        for i in 0..PER_SHARD {
            let gid = shard * PER_SHARD + i;
            let text = format!("straggler doc {gid}");
            let identity = if gid as u64 == planted_id {
                Some(planted_identity.clone())
            } else {
                Some(DocumentIdentity {
                    document_key: format!("doc-{gid:06}").into_bytes(),
                    version: 1,
                    chunk_ordinal: None,
                })
            };
            tx.send(AddDocumentsRequest {
                text: text.clone(),
                analysis: Some(body_spec()),
                original_source: Some(common::protobuf_source(&text, "src")),
                identity,
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();

        let start = shard * PER_SHARD * DIM;
        let end = (shard + 1) * PER_SHARD * DIM;
        client
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                vectors: corpus[start..end].to_vec(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap();
        client.flush(FlushRequest {}).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }

    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION)
        .with_stream_search(true);

    // 1. Coordinator settle finish (`finish_settle_us = 45_000` = 45 ms):
    //    shard 0 finishes quickly (< 10 ms), shard 1 emits its early first
    //    flush at hop 2 (~24 ms) containing `planted_id` (resetting settle),
    //    and once top-k stops improving for 45 ms, `FinishStreamSearch` closes
    //    shard 1 cleanly with `IdentityReady` intact.
    let t0 = std::time::Instant::now();
    let streamed = coordinator
        .fanout_stream_search(
            "settle-test",
            &query_vector,
            200,
            None,
            &RequestFilters {
                finish_settle_us: Some(45_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(320),
        "graceful settle should close straggler well before full ~500ms walk, took {elapsed:?}"
    );
    assert_eq!(streamed.shards_finished_early, 1);
    assert!(streamed.summaries[0].completed);
    assert!(streamed.summaries[1].completed);
    assert_eq!(
        streamed.summaries[0].end_reason,
        StreamEndReason::WalkConverged as i32
    );
    assert_eq!(
        streamed.summaries[1].end_reason,
        StreamEndReason::FinishRequested as i32
    );
    assert_eq!(streamed.hits[0].vector_id, planted_id);
    assert_eq!(
        streamed.hits[0].identity.as_ref(),
        Some(&planted_identity),
        "IdentityReady + ResolveStreamIdentities must resolve straggler's flushed #1 hit"
    );

    // 2. Policy-driven `finish_settle_us` through `Query` under `AUTO`.
    let fingerprint = graph_index().descriptor().scoring_fingerprint;
    let policy = load_policy(
        "finish-settle-auto",
        &format!(
            "format_version = 1\npolicy_id = \"hnsw-finish\"\n\
             embedding_model = \"test-embed\"\ncorpus_generation = {GENERATION}\n\
             corpus_rows = {}\ndimensions = {DIM}\nprovider_backend = \"{HNSW}\"\n\
             scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
             [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 1000000\n\
             filter_selectivity_ppm_max = 1000000\ncandidates = 200\n\
             measured_recall_ppm = 990000\nfinish_settle_us = 45000\n",
            PER_SHARD * 2
        ),
    );
    let auto_coord = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION)
        .with_dense_execution_policy(policy);
    let qresp = query(
        &auto_coord,
        dense_request(&query_vector, K, DenseExecutionMode::Auto),
    )
    .await
    .unwrap();
    assert_eq!(qresp.hits[0].doc_id, planted_id);
    assert_eq!(qresp.hits[0].identity.as_ref(), Some(&planted_identity));
    let outcome = qresp.dense_execution.unwrap();
    assert_eq!(outcome.shards_finished_early, 1);
    assert_eq!(outcome.policy_point.unwrap().finish_settle_us, Some(45_000));
    assert!(outcome.planner_reason.contains("finish settle 45000 us"));

    // 3. Coordinator deadline finish (`finish_deadline_us = 35_000`) sets
    //    `StreamEndReason::Deadline`.
    let deadlined = coordinator
        .fanout_stream_search(
            "deadline-test",
            &query_vector,
            200,
            None,
            &RequestFilters {
                finish_deadline_us: Some(35_000),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(deadlined.shards_finished_early, 1);
    assert!(deadlined.summaries[1].completed);
    assert_eq!(
        deadlined.summaries[1].end_reason,
        StreamEndReason::Deadline as i32
    );

    // 4. `StopStreamSearch` still means abandon (`completed = false`,
    //    `StreamEndReason::Abandoned`).
    let mut straggler = NodeServiceClient::connect(addrs[1].clone()).await.unwrap();
    let (stx, srx) = mpsc::channel(4);
    stx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "abandon-check".into(),
            vector: query_vector.clone(),
            candidate_depth: Some(200),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut sresp = straggler
        .stream_search(ReceiverStream::new(srx))
        .await
        .unwrap()
        .into_inner();
    stx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let mut stop_summary = None;
    while let Some(frame) = sresp.message().await.unwrap() {
        if let Some(stream_search_response::Payload::Summary(s)) = frame.payload {
            stop_summary = Some(s);
        }
    }
    let stop_summary = stop_summary.expect("StopStreamSearch must yield terminal Summary");
    assert!(!stop_summary.completed);
    assert_eq!(stop_summary.end_reason, StreamEndReason::Abandoned as i32);

    // 5. Exhaustive `EmbeddedTurboVec` ignores `FinishStreamSearch` and scans
    //    every block to `StreamEndReason::Exhausted`.
    let tv_vectors = unit_vectors(256, DIM, 0x4A5E_0102);
    let tv_cfg = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, DIM, 4, &tv_vectors).unwrap();
    let mut tv_index = VectorIndex::from_backend_config(DIM, &tv_cfg).unwrap();
    tv_index.add(&tv_vectors, DIM).unwrap();
    tv_index.prepare().unwrap();
    let tv_node = NodeServiceImpl::new(
        Some(tv_index),
        NodeConfig {
            chunk_blocks: 1,
            ..Default::default()
        },
    )
    .with_stream_poll_delay(Duration::from_millis(5));
    let (tv_addr, tv_handle) = serve_node(tv_node).await;
    handles.push(tv_handle);

    let mut tv_client = NodeServiceClient::connect(tv_addr).await.unwrap();
    let (ftx, frx) = mpsc::channel(4);
    ftx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "exhaustive-ignores-finish".into(),
            vector: query_vector.clone(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut fresp = tv_client
        .stream_search(ReceiverStream::new(frx))
        .await
        .unwrap()
        .into_inner();
    ftx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Finish(FinishStreamSearch {
            deadline: false,
        })),
    })
    .await
    .unwrap();
    let mut tv_summary = None;
    while let Some(frame) = fresp.message().await.unwrap() {
        if let Some(stream_search_response::Payload::Summary(s)) = frame.payload {
            tv_summary = Some(s);
        }
    }
    let tv_summary = tv_summary.expect("exhaustive stream must emit Summary");
    assert!(tv_summary.completed);
    assert_eq!(tv_summary.emitted, 256);
    assert_eq!(tv_summary.end_reason, StreamEndReason::Exhausted as i32);

    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floor_prune_multi_shard_prunes_cold_shard_handles_underfilled_and_dedupes_stream() {
    use pipestream_search::coordinator::RequestFilters;
    use pipestream_search::harness::serve_node;
    use pipestream_search::hnsw::{BuildMode, HnswIndex};
    use pipestream_search::node::NodeServiceImpl;
    use pipestream_search::vector::{
        FilterRegimeCounts, VectorBackendConfig, VectorBackendDescriptor, VectorError,
        VectorProvider, VectorSearchOptions, VectorSearchResults, VectorStreamBatch,
        VectorStreamControl, VectorStreamEndReason, VectorStreamSummary,
    };
    use std::ops::Range;
    use std::path::Path;
    use std::time::Duration;

    const LOCAL_DIM: usize = 16;
    const K: u32 = 32;
    const LOCAL_GENERATION: u64 = 19;

    let mut q = vec![0.0_f32; LOCAL_DIM];
    q[0] = 1.0;

    let shared_cfg = HnswConfig {
        m: 12,
        ef_construction: 64,
        build_mode: BuildMode::FixedOrder,
        seed: 0x7200,
    };
    let backend_cfg = shared_cfg.backend_config().unwrap();

    let make_hnsw_shard = |vectors: &[f32], offset: u64, delay_ms: u64| {
        let mut index = VectorIndex::from_backend_config(LOCAL_DIM, &backend_cfg).unwrap();
        if !vectors.is_empty() {
            index.add(vectors, LOCAL_DIM).unwrap();
        }
        index.prepare().unwrap();
        let fingerprint = index.descriptor().scoring_fingerprint;
        let mut node = NodeServiceImpl::new(
            Some(index),
            NodeConfig {
                slot_offset: offset,
                ..Default::default()
            },
        );
        if delay_ms > 0 {
            node = node.with_stream_poll_delay(Duration::from_millis(delay_ms));
        }
        (node, fingerprint)
    };

    // Shard 0 (hot, 256 rows): scores in ~[2.5, 5.05].
    let mut hot = unit_vectors(256, LOCAL_DIM, 0x7201);
    for r in 0..256 {
        hot[r * LOCAL_DIM] = 2.5 + (r as f32) * 0.01;
    }
    // Shard 1 (cold, 512 rows, delayed 2 ms/hop so Shard 0's first-flush floor
    // arrives early in Shard 1's layer-0 walk): negative scores in [-5.06, -2.5].
    let mut cold = unit_vectors(512, LOCAL_DIM, 0x7202);
    for r in 0..512 {
        cold[r * LOCAL_DIM] = -2.5 - (r as f32) * 0.005;
    }
    // Shard 2 (empty participant, 0 rows).
    let (node_hot, fingerprint) = make_hnsw_shard(&hot, 0, 0);
    let (node_cold, _) = make_hnsw_shard(&cold, 1_000, 2);
    let (node_empty, _) = make_hnsw_shard(&[], 2_000, 0);

    let (addr_hot, h0) = serve_node(node_hot).await;
    let (addr_cold, h1) = serve_node(node_cold).await;
    let (addr_empty, h2) = serve_node(node_empty).await;
    let addrs = vec![addr_hot, addr_cold, addr_empty];

    let coordinator = CoordinatorServiceImpl::new(addrs.clone()).with_stream_search(true);

    // 1. Baseline (`floor_prune = None`) vs `gateK` (`g = 0`) vs `clamp` (`g = 500_000`):
    //    exact top-K matches, cold shard prunes sub-floor candidates, empty shard completes.
    let base = coordinator
        .fanout_stream_search("fp-off", &q, K, None, &RequestFilters::default())
        .await
        .unwrap();
    assert_eq!(base.hits.len(), K as usize);
    assert_eq!(base.shards_floor_pruned, 0);

    for greediness in [0u32, 500_000u32] {
        let pruned = coordinator
            .fanout_stream_search(
                "fp-on",
                &q,
                K,
                None,
                &RequestFilters {
                    floor_prune: Some(true),
                    floor_k: Some(K),
                    floor_greediness_ppm: Some(greediness),
                    floor_clamp_min: Some(8),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(pruned.hits.len(), K as usize, "g={greediness}");
        assert_eq!(
            pruned.hits.iter().map(|h| h.vector_id).collect::<Vec<_>>(),
            base.hits.iter().map(|h| h.vector_id).collect::<Vec<_>>(),
            "g={greediness}"
        );
        assert!(
            pruned.shards_floor_pruned >= 1,
            "expected cold shard to prune candidates at g={greediness}"
        );
        assert!(
            pruned.summaries[1].blocks_scanned < base.summaries[1].blocks_scanned,
            "cold shard hops should drop under floor pruning (g={greediness}): got {} vs {}",
            pruned.summaries[1].blocks_scanned,
            base.summaries[1].blocks_scanned
        );
        assert!(pruned.summaries[2].completed);
        assert_eq!(pruned.summaries[2].emitted, 0);
    }

    // 2. Underfilled `< k` corpus (`k = 2_000` > 768 total rows): no floor is
    //    published (`floors_sent == 0`) and every live row is returned.
    let underfilled = coordinator
        .fanout_stream_search(
            "fp-underfilled",
            &q,
            2_000,
            None,
            &RequestFilters {
                floor_prune: Some(true),
                floor_k: Some(2_000),
                floor_greediness_ppm: Some(500_000),
                floor_clamp_min: Some(8),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(underfilled.floors_sent, 0);
    assert_eq!(underfilled.shards_floor_pruned, 0);
    assert_eq!(underfilled.hits.len(), 256 + 512);

    // 3. Policy `AUTO` qualification on HNSW carries `floor_greediness_ppm` + `floor_clamp_min`
    //    and reports `shards_floor_pruned` on `DenseExecutionOutcome`.
    let policy = load_policy(
        "hnsw-floor-prune",
        &format!(
            "format_version = 1\npolicy_id = \"hnsw-floor\"\n\
             embedding_model = \"test-embed\"\ncorpus_generation = {LOCAL_GENERATION}\n\
             corpus_rows = {}\ndimensions = {LOCAL_DIM}\nprovider_backend = \"{HNSW}\"\n\
             scoring_fingerprint = \"{fingerprint}\"\nmeasured_queries = 16\n\n\
             [[points]]\nk = {K}\nfilter_selectivity_ppm_min = 1000000\n\
             filter_selectivity_ppm_max = 1000000\ncandidates = 96\n\
             measured_recall_ppm = 995000\nfloor_greediness_ppm = 500000\n\
             floor_clamp_min = 8\n",
            256 + 512
        ),
    );
    let auto_coord = CoordinatorServiceImpl::new(addrs)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_stream_search(true)
        .with_topology_generation(LOCAL_GENERATION)
        .with_dense_execution_policy(policy);
    let qresp = query(&auto_coord, dense_request(&q, K, DenseExecutionMode::Auto))
        .await
        .unwrap();
    assert_eq!(qresp.hits.len(), K as usize);
    let outcome = qresp.dense_execution.unwrap();
    assert!(outcome.shards_floor_pruned >= 1);
    let pt = outcome.policy_point.unwrap();
    assert_eq!(pt.floor_greediness_ppm, Some(500_000));
    assert_eq!(pt.floor_clamp_min, Some(8));
    assert!(outcome
        .planner_reason
        .contains("floor prune 500000 ppm (clamp 8)"));

    h0.abort();
    h1.abort();
    h2.abort();

    // 4. Duplicate `(shard, vector_id)` stream emissions cannot manufacture `k`
    //    heap entries or publish a bogus floor that drops another shard's hits.
    struct DupStreamProvider {
        inner: HnswIndex,
        spam_copies: usize,
        spam_score: f32,
    }

    impl VectorProvider for DupStreamProvider {
        fn descriptor(&self) -> VectorBackendDescriptor {
            self.inner.descriptor()
        }
        fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
            self.inner.backend_config()
        }
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn dimension(&self) -> Option<usize> {
            self.inner.dimension()
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
            self.inner.search(queries, k, options)
        }
        fn search_streaming_controlled(
            &self,
            _queries: &[f32],
            _options: VectorSearchOptions<'_>,
            sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            _control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            let scores = vec![self.spam_score; self.spam_copies];
            let slots = vec![0i64; self.spam_copies];
            let _ = sink(&VectorStreamBatch {
                query_index: 0,
                block_base: 0,
                scores: &scores,
                slots: &slots,
            });
            Ok(VectorStreamSummary {
                query_count: 1,
                emitted: self.spam_copies,
                units_scanned: 1,
                completed: true,
                end_reason: VectorStreamEndReason::Exhausted,
                regimes: FilterRegimeCounts::default(),
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                floor_pruned: 0,
            })
        }
        fn row_transcripts(
            &self,
            rows: Range<usize>,
            sink: &mut dyn FnMut(usize, &[u8]),
        ) -> Result<(), VectorError> {
            self.inner.row_transcripts(rows, sink)
        }
    }

    let mut dup_inner = HnswIndex::new(LOCAL_DIM, shared_cfg).unwrap();
    dup_inner
        .add(&unit_vectors(1, LOCAL_DIM, 0x7298), LOCAL_DIM)
        .unwrap();
    dup_inner.prepare().unwrap();
    let dup_node = NodeServiceImpl::new(
        Some(VectorIndex::from_provider(DupStreamProvider {
            inner: dup_inner,
            spam_copies: 8,
            spam_score: 100.0,
        })),
        NodeConfig {
            slot_offset: 42,
            ..Default::default()
        },
    );
    let (dup_addr, dup_handle) = serve_node(dup_node).await;
    let (slow_real, _) = make_hnsw_shard(&unit_vectors(256, LOCAL_DIM, 0x7299), 5_000, 5);
    let (slow_addr, slow_handle) = serve_node(slow_real).await;

    let dedup_coord =
        CoordinatorServiceImpl::new(vec![dup_addr, slow_addr]).with_stream_search(true);
    let dedup_res = dedup_coord
        .fanout_stream_search(
            "dedup-stream",
            &q,
            4,
            None,
            &RequestFilters {
                floor_prune: Some(true),
                floor_k: Some(4),
                floor_greediness_ppm: Some(1_000_000),
                floor_clamp_min: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Without deduplication, 8 copies of `(42, 100.0)` would fill the k=4 heap
    // with 4 copies of id 42 and publish a floor ~100.0, starving `slow_real`!
    assert_eq!(dedup_res.hits.len(), 4);
    assert_eq!(
        dedup_res.hits.iter().filter(|h| h.vector_id == 42).count(),
        1,
        "duplicate (shard, vector_id) emissions must be deduplicated in heap"
    );
    assert_eq!(
        dedup_res
            .hits
            .iter()
            .filter(|h| h.vector_id >= 5_000)
            .count(),
        3,
        "real lower-scoring hits from shard 1 must survive duplicate spam on shard 0"
    );

    dup_handle.abort();
    slow_handle.abort();
}
