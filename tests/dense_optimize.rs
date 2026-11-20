//! End-to-end verification for `OptimizeDensePolicy` sweeps, atomic installs,
//! replica-invariant hardware fingerprints, relays, and runtime/mutation guards.

mod common;

use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{fit_calibration, start_node, unit_vectors};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::{
    fleet_fingerprint, local_host_profile, DenseExecutionPolicy, Qualification, RequestKey,
};
use pipestream_search::harness::{seeded_index, serve_node, start_relay};
use pipestream_search::hnsw::{HnswConfig, HnswIndex, HNSW};
use pipestream_search::metrics::dense_policy_drift_total;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddVectorsRequest, DenseCoverage, DenseExecutionMode as Mode,
    DenseExecutionShape, DensePrecisionRequest, DenseQuery, DenseScorePrecision, FlushRequest,
    GetVectorBackendRequest, OptimizeDensePolicyRequest, QueryRequest, SearchQuery, SelectionQuery,
};
use pipestream_search::relay::merge_vector_backend;
use pipestream_search::vector::*;
use tonic::{Code, Request};

const DIM: usize = 16;
const PER_SHARD: usize = 1_500;
type NodeTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

async fn send_vectors(addr: &str, vectors: Vec<f32>, dim: usize, flush: bool) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let batch = AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors,
        dim: dim as u32,
    };
    client
        .add_vectors(tokio_stream::iter(vec![batch]))
        .await
        .unwrap();
    if flush {
        client.flush(FlushRequest {}).await.unwrap();
    }
}

struct DelayedProvider {
    inner: HnswIndex,
    delay_ms: Arc<AtomicU64>,
}

impl DelayedProvider {
    fn pause(&self) {
        let ms = self.delay_ms.load(Ordering::Relaxed);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

impl VectorProvider for DelayedProvider {
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
    fn is_graph_linked(&self) -> bool {
        self.inner.is_graph_linked()
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
        self.pause();
        self.inner.search(queries, k, options)
    }
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        self.pause();
        self.inner
            .search_streaming_controlled(queries, options, sink, control)
    }
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        self.inner.row_transcripts(rows, sink)
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

fn optimize_request(install: bool) -> OptimizeDensePolicyRequest {
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

fn auto_query(k: u32, dim: usize, precision: DensePrecisionRequest) -> QueryRequest {
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
        "1-node hardware_fingerprint stays valid on 2-node scale-out"
    );

    doc.hosts.clear();
    doc.hardware_fingerprint = "another-fleet".into();
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

async fn start_delayed_shard(delay_ms: u64) -> (String, Arc<AtomicU64>, NodeTask) {
    let inner = HnswIndex::new(DIM, HnswConfig::default()).unwrap();
    let delay = Arc::new(AtomicU64::new(0));
    let index = VectorIndex::from_provider(DelayedProvider {
        inner,
        delay_ms: Arc::clone(&delay),
    });
    let (addr, handle) = serve_node(NodeServiceImpl::new(Some(index), NodeConfig::default())).await;
    send_vectors(&addr, unit_vectors(1_200, DIM, 0x0971_0042), DIM, true).await;
    delay.store(delay_ms, Ordering::Relaxed);
    (addr, delay, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_provider_respects_max_runtime_ms_single_flight_and_mid_sweep_mutation() {
    let (addr, delay, handle) = start_delayed_shard(220).await;
    let dir = std::env::temp_dir().join(format!("dense-opt-abort-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_dense_execution_policy_at(path.clone(), None);

    let mut bad_k = optimize_request(false);
    bad_k.k = vec![99_999];
    let err = SearchService::optimize_dense_policy(&coordinator, Request::new(bad_k))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let mut short = optimize_request(false);
    short.max_runtime_ms = 60;
    let t0 = Instant::now();
    let err = SearchService::optimize_dense_policy(&coordinator, Request::new(short))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert!(
        t0.elapsed() < Duration::from_millis(210),
        "in-flight search exceeded deadline: {:?}",
        t0.elapsed()
    );

    let c1 = coordinator.clone();
    let mut slow_req = optimize_request(true);
    slow_req.k = vec![10];
    slow_req.query_vectors = unit_vectors(1, DIM, 0x0971_0055);
    let slow_clone = slow_req.clone();
    let first = tokio::spawn(async move {
        SearchService::optimize_dense_policy(&c1, Request::new(slow_clone)).await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    let second = SearchService::optimize_dense_policy(&coordinator, Request::new(slow_req))
        .await
        .unwrap_err();
    assert_eq!(second.code(), Code::Aborted);

    send_vectors(&addr, unit_vectors(1, DIM, 0x0971_0077), DIM, true).await;
    delay.store(0, Ordering::Relaxed);
    let mutated_err = first.await.unwrap().unwrap_err();
    assert_eq!(mutated_err.code(), Code::Aborted);
    assert!(mutated_err
        .message()
        .contains("corpus mutated during sweep"));
    assert!(!path.exists(), "aborted sweep must never install policy");
    let _ = std::fs::remove_dir_all(dir);
    handle.abort();
}

fn host_reads_4bit_planes() -> bool {
    #[cfg(target_arch = "aarch64")]
    {
        std::arch::is_aarch64_feature_detected!("dotprod")
    }
    #[cfg(target_arch = "x86_64")]
    {
        std::arch::is_x86_feature_detected!("avx512vbmi")
            && std::arch::is_x86_feature_detected!("avx512vnni")
            && std::arch::is_x86_feature_detected!("avx512bw")
            && std::arch::is_x86_feature_detected!("avx512f")
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        false
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turbovec_optimize_emits_both_quantized_and_fp32_points_when_staged() {
    if !host_reads_4bit_planes() {
        return;
    }
    const TV_DIM: usize = 32;
    const ROWS: usize = 34_000;
    let corpus = unit_vectors(ROWS, TV_DIM, 0x7B04_0001);
    let (shift, scale) = fit_calibration(TV_DIM, 4, &corpus[..1024 * TV_DIM]);
    let (addr, handle) = start_node(
        seeded_index(TV_DIM, 4, &shift, &scale),
        NodeConfig::default(),
    )
    .await;
    send_vectors(&addr, corpus, TV_DIM, false).await;
    let coordinator = CoordinatorServiceImpl::new(vec![addr]);
    let req = OptimizeDensePolicyRequest {
        policy_id: "tv-opt".into(),
        embedding_model: "test".into(),
        target_recall_ppm: 500_000,
        k: vec![10],
        query_vectors: unit_vectors(4, TV_DIM, 0x7B04_0002),
        max_runtime_ms: 120_000,
        install: false,
        ..Default::default()
    };
    let doc = SearchService::optimize_dense_policy(&coordinator, Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .policy
        .unwrap();
    assert!(doc
        .points
        .iter()
        .any(|p| p.output_precision == DenseScorePrecision::Quantized as i32));
    assert!(doc
        .points
        .iter()
        .any(|p| p.output_precision == DenseScorePrecision::Fp32 as i32));
    handle.abort();
}
