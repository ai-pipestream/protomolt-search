//! Integration tests for version-2 dense execution policy qualification,
//! patience early-stopping across Unary and Streaming shapes, and fail-closed
//! `AUTO -> EXHAUSTIVE` fallback on corpus mutations and same-row-count swaps.

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use common::{start_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::{DenseExecutionPolicy, LiveIdentity};
use pipestream_search::hnsw::{HnswConfig, HNSW};
use pipestream_search::metrics::dense_policy_drift_total;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::install_snapshot_from_request::Source;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddVectorsRequest,
    CommitReplacementsRequest, DeleteDocumentsRequest, DenseCoverage, DenseExecutionMode,
    DenseExecutionPolicyDocument, DenseExecutionShape, DenseFilterStrategy, DensePolicyPoint,
    DenseQuery, DenseScorePrecision, DenseTechnique, DenseTermination, FlushRequest,
    InstallSnapshotFromRequest, QueryRequest, QueryResponse, Replacement, SearchQuery,
    SelectionQuery,
};
use pipestream_search::snapshot::{export_snapshot, install_snapshot_from};
use pipestream_search::vector::VectorIndex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 32;
const GENERATION: u64 = 3;
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "dense-policy-v2-{prefix}-{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ShardFleet {
    coordinator: CoordinatorServiceImpl,
    addrs: Vec<String>,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    _dirs: Vec<TempDir>,
}

impl Drop for ShardFleet {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

fn graph_index() -> VectorIndex {
    VectorIndex::create(HNSW, DIM, 8).unwrap()
}

async fn populate_hnsw_shard(addr: &str, base_doc: usize, corpus: &[f32]) {
    let rows = corpus.len() / DIM;
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(rows.max(1));
    for i in 0..rows {
        tx.send(AddDocumentsRequest {
            text: format!("document {} about appellate review", base_doc + i),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    let (vtx, vrx) = mpsc::channel(2);
    vtx.send(AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: corpus.to_vec(),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(vtx);
    client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
    client.flush(FlushRequest {}).await.unwrap();
}

async fn start_hnsw_fleet(shards: usize, per_shard: usize, seed: u64, persist: bool) -> ShardFleet {
    let corpus = unit_vectors(shards * per_shard, DIM, seed);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    let mut dirs = Vec::new();
    for shard in 0..shards {
        let mut cfg = NodeConfig {
            slot_offset: (shard * per_shard) as u64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            ..Default::default()
        };
        if persist {
            let dir = TempDir::new("shard");
            cfg.index_path = Some(dir.0.join("index"));
            dirs.push(dir);
        }
        let (addr, handle) = start_node(graph_index(), cfg).await;
        let slice = &corpus[shard * per_shard * DIM..(shard + 1) * per_shard * DIM];
        populate_hnsw_shard(&addr, shard * per_shard, slice).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    ShardFleet {
        coordinator,
        addrs,
        handles,
        _dirs: dirs,
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
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

async fn run_query(coordinator: &CoordinatorServiceImpl, req: QueryRequest) -> QueryResponse {
    coordinator
        .query(Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

fn graph_policy(
    live: &LiveIdentity,
    k: u32,
    depth: u32,
    patience: Option<(u32, u32)>,
) -> DenseExecutionPolicy {
    let point = |shape: DenseExecutionShape| DensePolicyPoint {
        k,
        filter_selectivity_ppm_min: 1_000_000,
        filter_selectivity_ppm_max: 1_000_000,
        candidates: depth,
        measured_recall_ppm: 990_000,
        patience_saturation_ppm: patience.map(|(ppm, _)| ppm),
        patience_hops: patience.map(|(_, hops)| hops),
        mode: DenseExecutionMode::Balanced as i32,
        technique: DenseTechnique::HnswWalk as i32,
        selection_precision: DenseScorePrecision::Fp32 as i32,
        output_precision: DenseScorePrecision::Fp32 as i32,
        filter_strategy: DenseFilterStrategy::Speed as i32,
        shape: shape as i32,
        ..Default::default()
    };
    DenseExecutionPolicy::from_document(DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "hnsw-v2".into(),
        embedding_model: "test-embed".into(),
        provider_backend: live.provider_backend.clone(),
        scoring_fingerprint: live.scoring_fingerprint.clone(),
        corpus_generation: live.corpus_generation,
        corpus_rows: live.corpus_rows,
        deleted_docs: live.deleted_docs,
        corpus_fingerprint: live.corpus_fingerprint.clone(),
        dimensions: live.dimensions,
        measured_queries: 16,
        target_recall_ppm: 950_000,
        points: vec![
            point(DenseExecutionShape::Unary),
            point(DenseExecutionShape::Streaming),
        ],
    })
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_qualifies_hnsw_and_falls_back_exhaustively_on_scoring_fingerprint_drift() {
    let fleet = start_hnsw_fleet(2, 1100, 0x4A5E_0001, false).await;
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0002);
    let live = fleet.coordinator.live_dense_identity().await.unwrap();
    let other_fp = VectorIndex::from_backend_config(
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
    let mut drifted_live = live.clone();
    drifted_live.scoring_fingerprint = other_fp;
    let drifted = fleet
        .coordinator
        .clone()
        .with_dense_execution_policy(graph_policy(&drifted_live, 5, 10, None));
    let before = dense_policy_drift_total();
    let fallback = run_query(
        &drifted,
        dense_request(&query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .dense_execution
    .unwrap();
    assert_eq!(
        fallback.resolved_mode,
        DenseExecutionMode::Exhaustive as i32
    );
    assert_eq!(fallback.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(!fallback.policy_qualified);
    assert!(fallback.fallback_reason.contains("scoring_fingerprint"));
    assert!(dense_policy_drift_total() > before);

    let bound = fleet
        .coordinator
        .clone()
        .with_dense_execution_policy(graph_policy(&live, 5, 10, None));
    let qualified = run_query(
        &bound,
        dense_request(&query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .dense_execution
    .unwrap();
    assert_eq!(qualified.resolved_mode, DenseExecutionMode::Balanced as i32);
    assert!(qualified.policy_qualified && qualified.preset.is_empty());
    assert_eq!(qualified.policy_id, "hnsw-v2");
    assert_eq!(qualified.candidate_depth, 10);
    assert_eq!(qualified.coverage, DenseCoverage::CandidateSet as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_point_serves_patience_on_unary_and_streaming_shapes() {
    const K: u32 = 10;
    const DEPTH: u32 = 256;
    let fleet = start_hnsw_fleet(2, 1200, 0x4A5E_0091, false).await;
    let live = fleet.coordinator.live_dense_identity().await.unwrap();
    let policy = graph_policy(&live, K, DEPTH, Some((900_000, 3)));
    let query_vector = unit_vectors(1, DIM, 0x4A5E_0095);
    for streaming in [false, true] {
        let coordinator = fleet
            .coordinator
            .clone()
            .with_stream_search(streaming)
            .with_dense_execution_policy(policy.clone());
        let response = run_query(
            &coordinator,
            dense_request(&query_vector, K, DenseExecutionMode::Auto),
        )
        .await;
        assert_eq!(response.hits.len(), K as usize, "streaming={streaming}");
        let outcome = response.dense_execution.unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Balanced as i32);
        assert_eq!(outcome.candidate_depth, DEPTH);
        assert_eq!(outcome.policy_point.unwrap().patience_hops, Some(3));
        assert!(outcome.patience_stopped > 0, "streaming={streaming}");
        assert_eq!(outcome.termination, DenseTermination::BudgetStopped as i32);
    }
}

async fn assert_auto_fails_closed(
    coordinator: &CoordinatorServiceImpl,
    query_vector: &[f32],
    expected_reason: &str,
) {
    let before = dense_policy_drift_total();
    let outcome = run_query(
        coordinator,
        dense_request(query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .dense_execution
    .unwrap();
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Exhaustive as i32);
    assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(!outcome.policy_qualified);
    assert!(
        outcome.fallback_reason.contains(expected_reason),
        "expected {expected_reason} in {:?}",
        outcome.fallback_reason
    );
    assert!(dense_policy_drift_total() > before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_row_count_corpus_swap_invalidates_policy_and_fails_auto_closed() {
    let target = start_hnsw_fleet(1, 300, 0x4A5E_1001, true).await;
    let donor = start_hnsw_fleet(1, 300, 0x4A5E_2002, true).await;
    let query_vector = unit_vectors(1, DIM, 0x4A5E_3003);
    let live = target.coordinator.live_dense_identity().await.unwrap();
    let coordinator = target
        .coordinator
        .clone()
        .with_dense_execution_policy(graph_policy(&live, 5, 16, None));
    let before_swap = run_query(
        &coordinator,
        dense_request(&query_vector, 5, DenseExecutionMode::Auto),
    )
    .await
    .dense_execution
    .unwrap();
    assert!(before_swap.policy_qualified);

    let repo = TempDir::new("donor-repo");
    export_snapshot(&donor.addrs[0], &repo.0).await.unwrap();
    install_snapshot_from(
        &target.addrs[0],
        InstallSnapshotFromRequest {
            source: Some(Source::Directory(repo.0.display().to_string())),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let post_swap = target.coordinator.live_dense_identity().await.unwrap();
    assert_eq!(post_swap.corpus_rows, live.corpus_rows);
    assert_eq!(post_swap.deleted_docs, 0);
    assert_ne!(post_swap.corpus_fingerprint, live.corpus_fingerprint);
    assert_auto_fails_closed(&coordinator, &query_vector, "corpus_fingerprint").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_replacement_and_append_fail_auto_closed_to_exhaustive() {
    let fleet = start_hnsw_fleet(1, 300, 0x4A5E_4001, true).await;
    let query_vector = unit_vectors(1, DIM, 0x4A5E_4002);
    let bind = || async {
        let live = fleet.coordinator.live_dense_identity().await.unwrap();
        fleet
            .coordinator
            .clone()
            .with_dense_execution_policy(graph_policy(&live, 5, 16, None))
    };
    let mut client = NodeServiceClient::connect(fleet.addrs[0].clone())
        .await
        .unwrap();

    let after_delete = bind().await;
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_auto_fails_closed(&after_delete, &query_vector, "corpus_fingerprint").await;

    let after_replace = bind().await;
    client
        .commit_replacements(CommitReplacementsRequest {
            replacements: vec![Replacement {
                old_doc_id: 1,
                new_doc_id: 2,
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_auto_fails_closed(&after_replace, &query_vector, "corpus_fingerprint").await;

    let after_append = bind().await;
    let (vtx, vrx) = mpsc::channel(1);
    vtx.send(AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: unit_vectors(1, DIM, 0x4A5E_4099),
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(vtx);
    client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
    assert_auto_fails_closed(&after_append, &query_vector, "corpus_fingerprint").await;
}

#[path = "dense_policy_v2/selectivity_bitmaps.rs"]
mod selectivity_bitmaps;
#[path = "dense_policy_v2/staged_drift.rs"]
mod staged_drift;
