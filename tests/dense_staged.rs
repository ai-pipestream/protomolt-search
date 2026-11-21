//! Staged bit-plane selection end to end: an owned embedded TurboVec image
//! large enough for its bit-plane cache advertises `staged_top_k` on a host
//! whose kernels read bit planes, and BALANCED then selects by staged top-k
//! without certifying coverage; a smaller image keeps the disclosed
//! exhaustive fallback, and the streaming route refuses staged selection.

mod common;

use common::{fit_calibration, start_node, unit_vectors};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::{DenseExecutionPolicy, LiveIdentity};
use pipestream_search::harness::{fake_ann, seeded_index};
use pipestream_search::metrics::dense_policy_drift_total;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddVectorsRequest, DenseCoverage, DenseExecutionMode as Mode,
    DenseExecutionPolicyDocument, DenseExecutionShape, DenseFilterStrategy, DensePolicyPoint,
    DensePrecisionRequest as PrecisionRequest, DenseQuery, DenseScorePrecision, DenseTechnique,
    QueryRequest, SearchQuery, SelectionQuery,
};
use tonic::Request;

const DIM: usize = 32;

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

async fn coordinator_shape(
    rows: usize,
    dim: usize,
    bits: usize,
    stream: bool,
) -> (CoordinatorServiceImpl, Vec<f32>) {
    let sample_rows = rows.min(1024);
    let corpus = unit_vectors(rows, dim, 0x57A6_0001);
    let (shift, scale) = fit_calibration(dim, bits, &corpus[..sample_rows * dim]);
    let mut index = seeded_index(dim, bits, &shift, &scale);
    index.add(&corpus, dim).unwrap();
    let (addr, _handle) = start_node(index, NodeConfig::default()).await;
    let coordinator = CoordinatorServiceImpl::new(vec![addr]).with_stream_search(stream);
    (coordinator, unit_vectors(1, dim, 0x57A6_0002))
}

async fn coordinator(rows: usize, stream: bool) -> (CoordinatorServiceImpl, Vec<f32>) {
    coordinator_shape(rows, DIM, 4, stream).await
}

fn balanced(vector: &[f32], k: u32) -> QueryRequest {
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    execution_mode: Mode::Balanced as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balanced_selects_by_staged_top_k_where_the_image_and_host_allow() {
    let (coordinator, vector) = coordinator(40_000, false).await;
    let response = coordinator
        .query(Request::new(balanced(&vector, 10)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.hits.len(), 10);
    let outcome = response.dense_execution.unwrap();
    if host_reads_4bit_planes() {
        assert_eq!(outcome.resolved_mode, Mode::Balanced as i32);
        assert_eq!(outcome.preset, "bitplane-balanced-v1");
        assert_eq!(outcome.coverage, DenseCoverage::CandidateSet as i32);
        assert_eq!(
            outcome.techniques[0].technique,
            DenseTechnique::BitplaneStaged as i32
        );
    } else {
        assert_eq!(outcome.resolved_mode, Mode::Exhaustive as i32);
        assert!(!outcome.fallback_reason.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_bit_and_unsupported_geometry_never_advertise_or_report_staged_selection() {
    for (dim, bits) in [(32, 3), (16, 4), (16, 2)] {
        let (coordinator, vector) = coordinator_shape(40_000, dim, bits, false).await;
        let outcome = coordinator
            .query(Request::new(balanced(&vector, 10)))
            .await
            .unwrap()
            .into_inner()
            .dense_execution
            .unwrap();
        assert_eq!(
            outcome.resolved_mode,
            Mode::Exhaustive as i32,
            "dim={dim} bits={bits} must fall back to exhaustive scan"
        );
        assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert_ne!(
            outcome.techniques[0].technique,
            DenseTechnique::BitplaneStaged as i32
        );
        assert!(!outcome.fallback_reason.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_images_and_streaming_coordinators_keep_the_disclosed_fallback() {
    for (rows, stream) in [(2_048, false), (40_000, true)] {
        let (coordinator, vector) = coordinator(rows, stream).await;
        let outcome = coordinator
            .query(Request::new(balanced(&vector, 10)))
            .await
            .unwrap()
            .into_inner()
            .dense_execution
            .unwrap();
        assert_eq!(
            outcome.resolved_mode,
            Mode::Exhaustive as i32,
            "rows={rows} stream={stream}"
        );
        assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert!(!outcome.fallback_reason.is_empty());
    }
}

fn auto_query(vector: &[f32], k: u32, precision: PrecisionRequest) -> QueryRequest {
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    execution_mode: Mode::Auto as i32,
                    precision: precision as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn staged_policy(
    live: &LiveIdentity,
    k: u32,
    q_depth: u32,
    fp32_depth: u32,
) -> DenseExecutionPolicy {
    let point = |output: DenseScorePrecision, candidates: u32| DensePolicyPoint {
        k,
        filter_selectivity_ppm_min: 1_000_000,
        filter_selectivity_ppm_max: 1_000_000,
        candidates,
        measured_recall_ppm: 995_000,
        mode: Mode::Balanced as i32,
        technique: DenseTechnique::BitplaneStaged as i32,
        selection_precision: DenseScorePrecision::Quantized as i32,
        output_precision: output as i32,
        filter_strategy: DenseFilterStrategy::Default as i32,
        shape: DenseExecutionShape::Unary as i32,
        ..Default::default()
    };
    DenseExecutionPolicy::from_document(DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "staged-v2".into(),
        embedding_model: "test-embed".into(),
        provider_backend: live.provider_backend.clone(),
        scoring_fingerprint: live.scoring_fingerprint.clone(),
        corpus_generation: live.corpus_generation,
        corpus_rows: live.corpus_rows,
        deleted_docs: live.deleted_docs,
        corpus_fingerprint: live.corpus_fingerprint.clone(),
        dimensions: live.dimensions,
        measured_queries: 32,
        target_recall_ppm: 950_000,
        points: vec![
            point(DenseScorePrecision::Quantized, q_depth),
            point(DenseScorePrecision::Fp32, fp32_depth),
        ],
    })
    .unwrap()
}

async fn ingest_vectors(addr: &str, corpus: &[f32]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let batches: Vec<AddVectorsRequest> = corpus
        .chunks(10_000 * DIM)
        .map(|chunk| AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: chunk.to_vec(),
            dim: DIM as u32,
        })
        .collect();
    client
        .add_vectors(tokio_stream::iter(batches))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_qualifies_staged_quantized_and_fp32_points_deterministically() {
    let corpus = unit_vectors(128, DIM, 0x57A6_1001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let index = fake_ann::fake_staged_index(seeded_index(DIM, 4, &shift, &scale));
    let (addr, _handle) = start_node(index, NodeConfig::default()).await;
    ingest_vectors(&addr, &corpus).await;

    let base = CoordinatorServiceImpl::new(vec![addr]).with_topology_generation(5);
    let live = base.live_dense_identity().await.unwrap();
    let coordinator = base.with_dense_execution_policy(staged_policy(&live, 10, 24, 32));
    let qvec = unit_vectors(1, DIM, 0x57A6_1002);

    let quantized = coordinator
        .query(Request::new(auto_query(&qvec, 10, PrecisionRequest::Auto)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(quantized.executed, "search");
    let q_out = quantized.dense_execution.unwrap();
    assert_eq!(q_out.resolved_mode, Mode::Balanced as i32);
    assert!(q_out.policy_qualified);
    assert_eq!(q_out.candidate_depth, 24);
    assert_eq!(q_out.filter_strategy, DenseFilterStrategy::Default as i32);
    assert_eq!(q_out.coverage, DenseCoverage::CandidateSet as i32);
    assert_eq!(
        q_out.techniques[0].technique,
        DenseTechnique::BitplaneStaged as i32
    );

    let fp32 = coordinator
        .query(Request::new(auto_query(&qvec, 10, PrecisionRequest::Fp32)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fp32.executed, "search:fp32_pool");
    let fp_out = fp32.dense_execution.unwrap();
    assert_eq!(fp_out.resolved_mode, Mode::Balanced as i32);
    assert!(fp_out.policy_qualified);
    assert_eq!(fp_out.candidate_depth, 32);
    assert_eq!(
        fp_out.selection_precision,
        DenseScorePrecision::Quantized as i32
    );
    assert_eq!(fp_out.output_precision, DenseScorePrecision::Fp32 as i32);
    assert_eq!(
        fp_out.techniques[0].technique,
        DenseTechnique::BitplaneStaged as i32
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_serves_staged_policy_over_real_embedded_turbovec_40k() {
    let corpus = unit_vectors(40_000, DIM, 0x57A6_2001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus[..1024 * DIM]);
    let index = seeded_index(DIM, 4, &shift, &scale);
    let (addr, _handle) = start_node(index, NodeConfig::default()).await;
    ingest_vectors(&addr, &corpus).await;

    let base = CoordinatorServiceImpl::new(vec![addr]).with_topology_generation(9);
    let live = base.live_dense_identity().await.unwrap();
    let coordinator = base.with_dense_execution_policy(staged_policy(&live, 10, 40, 64));
    let qvec = unit_vectors(1, DIM, 0x57A6_2002);

    for (prec, expected_depth) in [
        (PrecisionRequest::Quantized, 40),
        (PrecisionRequest::Fp32, 64),
    ] {
        let before = dense_policy_drift_total();
        let outcome = coordinator
            .query(Request::new(auto_query(&qvec, 10, prec)))
            .await
            .unwrap()
            .into_inner()
            .dense_execution
            .unwrap();
        if host_reads_4bit_planes() {
            assert_eq!(outcome.resolved_mode, Mode::Balanced as i32);
            assert!(outcome.policy_qualified);
            assert_eq!(outcome.candidate_depth, expected_depth);
            assert_eq!(
                outcome.techniques[0].technique,
                DenseTechnique::BitplaneStaged as i32
            );
        } else {
            assert_eq!(outcome.resolved_mode, Mode::Exhaustive as i32);
            assert!(!outcome.policy_qualified);
            assert!(outcome
                .fallback_reason
                .contains("staged bit-plane selection"));
            assert!(dense_policy_drift_total() > before);
        }
    }
}
