//! End-to-end verification for `OptimizeDensePolicy` sweeps, atomic installs,
//! hardware fingerprints, relays, staged bit-plane routing, and runtime guards.

mod common;
#[path = "dense_optimize/guards.rs"]
mod guards;
#[path = "dense_optimize/staged.rs"]
mod staged;

use common::{start_node, unit_vectors};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::{
    fleet_fingerprint, local_host_profile, DenseExecutionPolicy, Qualification, RequestKey,
};
use pipestream_search::harness::start_relay;
use pipestream_search::hnsw::HNSW;
use pipestream_search::metrics::dense_policy_drift_total;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddVectorsRequest, DenseCoverage, DenseExecutionMode as Mode,
    DenseExecutionShape, DenseHostProfile, DensePrecisionRequest, DenseQuery, DenseScorePrecision,
    FlushRequest, GetVectorBackendRequest, OptimizeDensePolicyRequest, QueryRequest, SearchQuery,
    SelectionQuery,
};
use pipestream_search::relay::merge_vector_backend;
use pipestream_search::vector::*;
use tonic::{Code, Request};

pub(crate) const DIM: usize = 16;
const PER_SHARD: usize = 1_500;
pub(crate) type NodeTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

pub(crate) async fn send_vectors(addr: &str, vectors: Vec<f32>, dim: usize, flush: bool) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let batches: Vec<AddVectorsRequest> = vectors
        .chunks(10_000 * dim)
        .map(|chunk| AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: chunk.to_vec(),
            dim: dim as u32,
        })
        .collect();
    client
        .add_vectors(tokio_stream::iter(batches))
        .await
        .unwrap();
    if flush {
        client.flush(FlushRequest {}).await.unwrap();
    }
}

async fn graph_fleet() -> (Vec<String>, Vec<NodeTask>) {
    let corpus = unit_vectors(PER_SHARD * 2, DIM, 0x0971_0001);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let index = VectorIndex::create(HNSW, DIM, 8).unwrap();
        let config = NodeConfig {
            slot_offset: (shard * PER_SHARD) as u64,
            ..Default::default()
        };
        let (addr, handle) = start_node(index, config).await;
        let rows = corpus[shard * PER_SHARD * DIM..(shard + 1) * PER_SHARD * DIM].to_vec();
        send_vectors(&addr, rows, DIM, true).await;
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

pub(crate) fn optimize_request(install: bool) -> OptimizeDensePolicyRequest {
    OptimizeDensePolicyRequest {
        policy_id: "graph-optimized".into(),
        embedding_model: "test".into(),
        target_recall_ppm: 900_000,
        k: vec![10, 100, 10],
        query_vectors: unit_vectors(8, DIM, 0x0971_0002),
        max_runtime_ms: 120_000,
        install,
        ..Default::default()
    }
}

pub(crate) fn auto_query(k: u32, dim: usize, precision: DensePrecisionRequest) -> QueryRequest {
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: unit_vectors(1, dim, 0x0971_0003),
                    execution_mode: Mode::Auto as i32,
                    precision: precision as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn expected(policy: &DenseExecutionPolicy, k: u32, shape: DenseExecutionShape) -> Option<u32> {
    match policy.qualify(RequestKey {
        k,
        candidate_depth: 0,
        filter_selectivity_ppm: 1_000_000,
        output_precision: DenseScorePrecision::Fp32,
        shape,
    }) {
        Qualification::Qualified(hit) => Some(hit.point.candidates),
        Qualification::Unqualified(_) => None,
    }
}

async fn run_auto(
    coord: &CoordinatorServiceImpl,
    k: u32,
) -> pipestream_search::pb::DenseExecutionOutcome {
    let req = auto_query(k, DIM, DensePrecisionRequest::Auto);
    coord
        .query(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .dense_execution
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optimize_measures_installs_and_auto_routes_on_unary_and_streaming() {
    let (addrs, handles) = graph_fleet().await;
    let dir = std::env::temp_dir().join(format!("dense-optimize-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let unconfigured = CoordinatorServiceImpl::new(addrs.clone());
    let refused =
        SearchService::optimize_dense_policy(&unconfigured, Request::new(optimize_request(true)))
            .await
            .unwrap_err();
    assert_eq!(refused.code(), Code::FailedPrecondition);

    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_dense_execution_policy_at(path.clone(), None);
    let response =
        SearchService::optimize_dense_policy(&coordinator, Request::new(optimize_request(true)))
            .await
            .unwrap()
            .into_inner();
    assert!(response.installed && !response.truncated && response.cells_measured >= 2);
    let document = response.policy.unwrap();
    assert_eq!(
        document.hardware_fingerprint,
        fleet_fingerprint(&[local_host_profile()])
    );
    assert_eq!(document.node_count, 2);
    assert_eq!(document.hosts.len(), 2);
    assert!(document
        .points
        .iter()
        .all(|p| match Mode::try_from(p.mode) {
            Ok(Mode::Balanced) => {
                p.filter_strategy
                    == pipestream_search::pb::DenseFilterStrategy::EnumerateSmall as i32
            }
            Ok(Mode::Fast) => {
                p.filter_strategy == pipestream_search::pb::DenseFilterStrategy::Speed as i32
            }
            _ => false,
        }));
    let installed = DenseExecutionPolicy::load(&path).unwrap();
    assert_eq!(installed.fingerprint(), response.policy_fingerprint);
    for k in [10, 100] {
        let out = run_auto(&coordinator, k).await;
        match expected(&installed, k, DenseExecutionShape::Unary) {
            Some(depth) => assert!(out.policy_qualified && out.candidate_depth == depth),
            None => assert_eq!(out.resolved_mode, Mode::Exhaustive as i32),
        }
    }

    let streaming = CoordinatorServiceImpl::new(addrs).with_stream_search(true);
    let stream_doc =
        SearchService::optimize_dense_policy(&streaming, Request::new(optimize_request(false)))
            .await
            .unwrap()
            .into_inner()
            .policy
            .unwrap();
    assert!(stream_doc
        .points
        .iter()
        .all(|p| p.shape == DenseExecutionShape::Streaming as i32));
    let _ = std::fs::remove_dir_all(dir);
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_scale_out_preserves_policy_while_foreign_hardware_and_relays_are_checked() {
    let (addrs, handles) = graph_fleet().await;
    let (relay_addr, _relay, relay_handle) = start_relay(addrs.clone()).await;
    let relay_coord = CoordinatorServiceImpl::new(vec![relay_addr]);
    let mut doc =
        SearchService::optimize_dense_policy(&relay_coord, Request::new(optimize_request(false)))
            .await
            .unwrap()
            .into_inner()
            .policy
            .unwrap();
    assert_eq!(
        doc.hardware_fingerprint,
        fleet_fingerprint(&[local_host_profile()])
    );
    assert_eq!(doc.node_count, 1);
    assert_eq!(doc.hosts.len(), 1);
    for p in &mut doc.points {
        p.approximate_p50_us = 1;
        p.measured_recall_ppm = 1_000_000;
    }
    let two_shard_probe = CoordinatorServiceImpl::new(addrs.clone());
    let two_live = two_shard_probe.live_dense_identity().await.unwrap();
    doc.corpus_fingerprint = two_live.corpus_fingerprint;
    let scaled_policy = DenseExecutionPolicy::from_document(doc.clone()).unwrap();
    let two_shard = two_shard_probe.with_dense_execution_policy(scaled_policy);
    let scaled_out = run_auto(&two_shard, 10).await;
    assert!(
        scaled_out.policy_qualified,
        "1-node hardware_fingerprint stays valid on 2-node homogeneous fleet"
    );

    let foreign_host = DenseHostProfile {
        cpu_model: "foreign-cpu-model".into(),
        simd_features: vec!["sse2".into()],
        logical_cores: 64,
    };
    doc.node_count = 2;
    doc.hosts = vec![foreign_host.clone(), foreign_host];
    doc.hardware_fingerprint = fleet_fingerprint(&doc.hosts);
    let foreign = CoordinatorServiceImpl::new(addrs.clone())
        .with_dense_execution_policy(DenseExecutionPolicy::from_document(doc).unwrap());
    let before = dense_policy_drift_total();
    let out = run_auto(&foreign, 10).await;
    assert_eq!(out.resolved_mode, Mode::Exhaustive as i32);
    assert_eq!(out.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(out.fallback_reason.contains("hardware_fingerprint"));
    assert!(dense_policy_drift_total() > before);

    let mut c0 = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let r0 = c0
        .get_vector_backend(GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    let mut r1 = r0.clone();
    r1.host.as_mut().unwrap().logical_cores += 4;
    let mixed = merge_vector_backend(&["a".into(), "b".into()], &[r0, r1]).unwrap();
    assert!(mixed.host.is_none());

    relay_handle.abort();
    for h in handles {
        h.abort();
    }
}
