//! Staged bit-plane selection end to end: an owned embedded TurboVec image
//! large enough for its bit-plane cache advertises `staged_top_k` on a host
//! whose kernels read bit planes, and BALANCED then selects by staged top-k
//! without certifying coverage; a smaller image keeps the disclosed
//! exhaustive fallback, and the streaming route refuses staged selection.

mod common;

use common::{fit_calibration, start_node, unit_vectors};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::seeded_index;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, DenseCoverage, DenseExecutionMode as Mode, DenseQuery,
    DenseTechnique, QueryRequest, SearchQuery, SelectionQuery,
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
