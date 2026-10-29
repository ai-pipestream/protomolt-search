//! The LEANN-style graph provider (`docs/leann-graph.md`) served by real
//! nodes behind a coordinator: it is selected by `vector_backend`, it is
//! never treated as exact (EXACT, the legacy unspecified mode and AUTO
//! without a policy refuse naming it), explicit ANN finds the FP32 top-k
//! through a graph each node really built (every shard holds more rows than
//! the build threshold, and the fixture waits until each node reports every
//! row in its graph, so nothing is answered by the exact tail scan) and
//! reports exact FP32 scores, and AUTO qualifies it only through a policy
//! bound to its scoring fingerprint.

mod common;

use std::path::PathBuf;

use common::{start_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::DenseExecutionPolicy;
use pipestream_search::leann_graph::LEANN_GRAPH;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddVectorsRequest, DenseExecutionMode,
    DenseQuery, DenseScoreMode, GetVectorBackendRequest, QueryRequest, QueryResponse, SearchQuery,
    SelectionQuery, VectorQualityContract,
};
use pipestream_search::vector::VectorIndex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 32;
/// 1,100 rows per shard: past the 1,024-row threshold at which `add`
/// builds the graph, so every query traverses one.
const N_DOCS: usize = 2200;
const SHARDS: usize = 2;
const GENERATION: u64 = 3;

struct Fixture {
    coordinator: CoordinatorServiceImpl,
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
    VectorIndex::create(LEANN_GRAPH, DIM, 8).unwrap()
}

async fn start_fixture() -> Fixture {
    let corpus = unit_vectors(N_DOCS, DIM, 0x1EA4_0001);
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
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + per_shard) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        // The append started a background build; wait until the node
        // reports every row in the graph, so no query below is answered by
        // the exhaustive tail scan.
        let mut polls = 0;
        loop {
            let backend = client
                .get_vector_backend(GetVectorBackendRequest {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(backend.num_vectors, per_shard as u64);
            if backend.indexed_vectors == Some(per_shard as u64) {
                break;
            }
            polls += 1;
            assert!(polls < 600, "shard {shard} graph never built: {backend:?}");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    Fixture {
        coordinator,
        corpus,
        query_vector: unit_vectors(1, DIM, 0x1EA4_0002),
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
    let queries = unit_vectors(8, DIM, 0x1EA4_0003);
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
    assert_eq!(outcome.provider_backend, LEANN_GRAPH);
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
            error.message().contains(LEANN_GRAPH),
            "{mode:?}: {}",
            error.message()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_qualifies_the_graph_only_through_its_own_fingerprint() {
    let fixture = start_fixture().await;
    let policy_for = |fingerprint: &str, tag: &str| {
        let text = format!(
            "format_version = 1\npolicy_id = \"leann-v1\"\nembedding_model = \"test-embed\"\n\
             corpus_generation = {GENERATION}\ncorpus_rows = {N_DOCS}\ndimensions = {DIM}\n\
             provider_backend = \"{LEANN_GRAPH}\"\nscoring_fingerprint = \"{fingerprint}\"\n\
             measured_queries = 16\n\n[[points]]\nk = 5\n\
             filter_selectivity_ppm_min = 1000000\nfilter_selectivity_ppm_max = 1000000\n\
             candidates = 10\nmeasured_recall_ppm = 990000\n"
        );
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("leann_policy_{tag}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("policy.toml");
        std::fs::write(&path, text).unwrap();
        let policy = DenseExecutionPolicy::load(&path).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        policy
    };

    // A policy measured at another graph configuration does not qualify.
    let other = VectorIndex::from_backend_config(
        DIM,
        &pipestream_search::leann_graph::LeannGraphConfig {
            prune_ratio_ppm: 500_000,
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
    assert_eq!(outcome.policy_id, "leann-v1");
    assert_eq!(outcome.candidate_depth, 10);
}

/// A seeded floor reaches each node's graph walk through the streaming scan.
/// The floor only filters what the walk returns, so seeding it with the
/// unfloored k-th best (well above the entry hub's own score) must keep the
/// identical top-k rather than stopping the walk at the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seeded_floor_filters_the_walk_without_cutting_it_short() {
    let fixture = start_fixture().await;
    let queries = unit_vectors(4, DIM, 0x1EA4_0004);
    for (q, query_vector) in queries.as_chunks::<DIM>().0.iter().enumerate() {
        let unfloored = fixture
            .coordinator
            .fanout_stream_search(
                &format!("leann-base-{q}"),
                query_vector,
                5,
                None,
                &Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(unfloored.hits.len(), 5);
        let kth = unfloored.hits.last().unwrap().score;
        let floored = fixture
            .coordinator
            .fanout_stream_search(
                &format!("leann-floor-{q}"),
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
