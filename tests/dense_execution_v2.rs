//! End-to-end verification for dense execution v2 receipts, composite legs,
//! direct RPC raw-control barriers, and shard-local FP32 sidecar scanning.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::exact_vectors::dot;
use pipestream_search::harness::seeded_index;
use pipestream_search::hnsw::{HnswConfig, HnswIndex, HNSW};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, selection_score_strategy as strat, AddDocumentsRequest,
    AddVectorsRequest, CascadeScore, CompositeSearchStrategy, DecomposedScore, DenseCoverage,
    DenseExecutionMode as Mode, DenseFilterStrategy, DensePrecisionRequest as Precision,
    DenseScorePrecision, DenseTechnique, DenseTechniqueCount, DenseTermination, FacetValue,
    FlushRequest, HybridSearchRequest, LexicalQuery, QueryRequest, QueryResponse, RrfScore,
    SearchQuery, SearchRequest, SelectionOperator, SelectionQuery, SelectionScoreStrategy,
};
use tonic::Request;

const DIM: usize = 32;

struct DirGuard(PathBuf);

impl DirGuard {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "dense_exec_v2_{tag}_{}_{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn hnsw_segmented_config(path: PathBuf, slot_offset: u64) -> NodeConfig {
    NodeConfig {
        slot_offset,
        index_path: Some(path),
        vector_backend: HNSW.to_string(),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".into()],
        vector_mmap: true,
        ..Default::default()
    }
}

async fn append_rows(addr: &str, vectors: &[f32], start_id: usize, court: &str) {
    let count = vectors.len() / DIM;
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let docs: Vec<AddDocumentsRequest> = (0..count)
        .map(|i| AddDocumentsRequest {
            text: format!("doc {} appellate opinion jurisdiction", start_id + i),
            analysis: Some(body_spec()),
            facets: vec![FacetValue {
                field: "court".into(),
                value: court.into(),
            }],
            ..Default::default()
        })
        .collect();
    client
        .add_documents(tokio_stream::iter(docs))
        .await
        .unwrap();
    let batch = AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors: vectors.to_vec(),
        dim: DIM as u32,
    };
    client
        .add_vectors(tokio_stream::iter(vec![batch]))
        .await
        .unwrap();
}

async fn flush_node(addr: &str) {
    let mut c = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    c.flush(FlushRequest {}).await.unwrap();
}

fn dense_leaf(vector: &[f32], mode: Mode, precision: Precision) -> SelectionQuery {
    common::dense_selection("dense", vector.to_vec(), mode as i32, precision as i32)
}

fn tech_count(technique: DenseTechnique, parts: u32) -> DenseTechniqueCount {
    DenseTechniqueCount {
        technique: technique as i32,
        parts,
    }
}

fn composite_query(vector: &[f32], mode: Mode, strategy: strat::Strategy) -> QueryRequest {
    let operator = if matches!(strategy, strat::Strategy::Cascade(_)) {
        SelectionOperator::Unspecified
    } else {
        SelectionOperator::Or
    };
    let lq = LexicalQuery {
        text: "appellate opinion".into(),
        analysis: Some(body_spec()),
        ..Default::default()
    };
    let lexical = SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lexical".into(),
            query: Some(search_query::Query::Lexical(lq)),
        })),
    };
    let comp = CompositeSearchStrategy {
        operator: operator as i32,
        clauses: vec![dense_leaf(vector, mode, Precision::Auto), lexical],
        scoring: Some(SelectionScoreStrategy {
            strategy: Some(strategy),
        }),
    };
    QueryRequest {
        k: 5,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(comp)),
        }),
        ..Default::default()
    }
}

async fn run_query(coord: &CoordinatorServiceImpl, req: QueryRequest) -> QueryResponse {
    coord.query(Request::new(req)).await.unwrap().into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_only_and_mixed_segmented_hnsw_report_honest_part_counts() {
    let dir = DirGuard::new("hnsw_parts");
    let index = HnswIndex::new(DIM, HnswConfig::default())
        .unwrap()
        .into_index();
    let (addr, handle) = start_node(index, hnsw_segmented_config(dir.0.join("s0.idx"), 0)).await;
    append_rows(&addr, &unit_vectors(24, DIM, 0x7A11_0001), 0, "ca9").await;
    let qvec = unit_vectors(1, DIM, 0x7A11_0002);
    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(1);

    for streaming in [false, true] {
        let coord = coordinator.clone().with_stream_search(streaming);
        let req = QueryRequest {
            k: 5,
            selection: Some(dense_leaf(&qvec, Mode::Balanced, Precision::Auto)),
            ..Default::default()
        };
        let out = run_query(&coord, req).await.dense_execution.unwrap();
        assert_eq!(out.coverage, DenseCoverage::CandidateSet as i32);
        assert_eq!(out.termination, DenseTermination::Exhausted as i32);
        assert_eq!(out.tail_enumerated_rows, 24);
        assert_eq!(
            out.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 1)]
        );
    }

    assert_mixed_and_empty_hnsw_parts(&addr, &coordinator, &qvec).await;
    handle.abort();
}

async fn assert_mixed_and_empty_hnsw_parts(
    addr: &str,
    coordinator: &CoordinatorServiceImpl,
    qvec: &[f32],
) {
    append_rows(addr, &unit_vectors(280, DIM, 0x7A11_0003), 24, "ca9").await;
    flush_node(addr).await;
    append_rows(addr, &unit_vectors(16, DIM, 0x7A11_0004), 304, "scotus").await;

    for streaming in [false, true] {
        let coord = coordinator.clone().with_stream_search(streaming);
        let req = QueryRequest {
            k: 5,
            selection: Some(dense_leaf(qvec, Mode::Balanced, Precision::Auto)),
            ..Default::default()
        };
        let mixed = run_query(&coord, req).await.dense_execution.unwrap();
        assert_eq!(mixed.termination, DenseTermination::Converged as i32);
        assert_eq!(
            mixed.techniques,
            vec![
                tech_count(DenseTechnique::HnswWalk, 1),
                tech_count(DenseTechnique::Fp32Scan, 1),
            ]
        );
        assert_zero_hit_exhaustive_fabricates_no_parts(&coord, qvec).await;
    }
}

async fn assert_zero_hit_exhaustive_fabricates_no_parts(
    coordinator: &CoordinatorServiceImpl,
    qvec: &[f32],
) {
    let comp = CompositeSearchStrategy {
        operator: SelectionOperator::And as i32,
        clauses: vec![
            common::cel_selection("none", "court == 'missing'"),
            dense_leaf(qvec, Mode::Exhaustive, Precision::Auto),
        ],
        scoring: None,
    };
    let req = QueryRequest {
        k: 5,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(comp)),
        }),
        ..Default::default()
    };
    let zero = run_query(coordinator, req).await;
    assert!(zero.hits.is_empty());
    let out = zero.dense_execution.unwrap();
    assert_eq!(out.coverage, DenseCoverage::ExhaustiveCertified as i32);
    assert!(out.techniques.is_empty(), "{:?}", out.techniques);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_segment_composite_threads_real_shard_execution_evidence() {
    let dir = DirGuard::new("composite_evidence");
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let index = HnswIndex::new(DIM, HnswConfig::default())
            .unwrap()
            .into_index();
        let path = dir.0.join(format!("shard{shard}.idx"));
        let (addr, handle) =
            start_node(index, hnsw_segmented_config(path, (shard * 300) as u64)).await;
        append_rows(
            &addr,
            &unit_vectors(280, DIM, 0xC047_0010 + shard as u64),
            shard * 300,
            "ca9",
        )
        .await;
        flush_node(&addr).await;
        append_rows(
            &addr,
            &unit_vectors(20, DIM, 0xC047_0020 + shard as u64),
            shard * 300 + 280,
            "scotus",
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(3);
    let qvec = unit_vectors(1, DIM, 0xC047_0099);

    for s in [
        strat::Strategy::Rrf(RrfScore::default()),
        strat::Strategy::Decomposed(DecomposedScore::default()),
        strat::Strategy::Cascade(CascadeScore {
            gate_id: "dense".into(),
        }),
    ] {
        let bal_req = composite_query(&qvec, Mode::Balanced, s.clone());
        let bal = run_query(&coordinator, bal_req)
            .await
            .dense_execution
            .unwrap();
        assert_eq!(bal.tail_enumerated_rows, 40);
        let want_bal = vec![
            tech_count(DenseTechnique::HnswWalk, 2),
            tech_count(DenseTechnique::Fp32Scan, 2),
        ];
        assert_eq!(bal.techniques, want_bal);
        let exh_req = composite_query(&qvec, Mode::Exhaustive, s);
        let exh = run_query(&coordinator, exh_req)
            .await
            .dense_execution
            .unwrap();
        assert_eq!(exh.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert_eq!(
            exh.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 4)]
        );
    }
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhaustive_fp32_above_max_k_scans_shard_sidecars_and_direct_rpcs_guard_knobs() {
    let rows_per_shard = 128usize;
    let mut corpus = unit_vectors(rows_per_shard * 2, DIM, 0xF932_0001);
    let tie_row = corpus[0..DIM].to_vec();
    corpus[rows_per_shard * DIM..(rows_per_shard + 1) * DIM].copy_from_slice(&tie_row);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let cfg = NodeConfig {
            slot_offset: (shard * rows_per_shard) as u64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            facet_fields: vec!["court".into()],
            ..Default::default()
        };
        let (addr, handle) = start_node(seeded_index(DIM, 4, &shift, &scale), cfg).await;
        let slice = &corpus[shard * rows_per_shard * DIM..(shard + 1) * rows_per_shard * DIM];
        append_rows(&addr, slice, shard * rows_per_shard, "ca9").await;
        addrs.push(addr);
        handles.push(handle);
    }
    let bm25_addr = Some(NATIVE_ANALYSIS_BACKEND.to_string());
    let coordinator = CoordinatorServiceImpl::new(addrs)
        .with_bm25(bm25_addr, Default::default())
        .with_max_k(16)
        .with_topology_generation(5);

    assert_exhaustive_fp32_matches_oracle(&coordinator, &corpus, &tie_row).await;
    assert_direct_rpc_knob_barriers(&coordinator, &tie_row).await;
    for h in handles {
        h.abort();
    }
}

async fn assert_exhaustive_fp32_matches_oracle(
    coordinator: &CoordinatorServiceImpl,
    corpus: &[f32],
    qvec: &[f32],
) {
    let mut oracle: Vec<(u64, f32)> = corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .map(|(id, row)| (id as u64, dot(row, qvec)))
        .collect();
    oracle.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    oracle.truncate(10);

    for streaming in [false, true] {
        let coord = coordinator.clone().with_stream_search(streaming);
        let resp = run_query(
            &coord,
            QueryRequest {
                k: 10,
                profile: true,
                selection: Some(dense_leaf(qvec, Mode::Exhaustive, Precision::Fp32)),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(resp.executed, "search:fp32_scan");
        assert_eq!(resp.profile.unwrap().rerank_rows, 256);
        let got: Vec<(u64, u32)> = resp
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect();
        let want: Vec<(u64, u32)> = oracle.iter().map(|&(id, s)| (id, s.to_bits())).collect();
        assert_eq!(got, want, "streaming={streaming}");
        let out = resp.dense_execution.unwrap();
        assert_eq!(out.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert_eq!(out.output_precision, DenseScorePrecision::Fp32 as i32);
        assert_eq!(
            out.techniques,
            vec![tech_count(DenseTechnique::Fp32Scan, 2)]
        );
    }
}

async fn assert_direct_rpc_knob_barriers(coordinator: &CoordinatorServiceImpl, qvec: &[f32]) {
    let bad_search = SearchRequest {
        k: 5,
        vector: qvec.to_vec(),
        enumerate_all: true,
        filter_strategy: DenseFilterStrategy::Speed as i32,
        ..Default::default()
    };
    let err = coordinator
        .search(Request::new(bad_search))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let unsupported_enum = SearchRequest {
        k: 5,
        vector: qvec.to_vec(),
        enumerate_all: true,
        ..Default::default()
    };
    let err = coordinator
        .search(Request::new(unsupported_enum))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let bad_hybrid = HybridSearchRequest {
        k: 5,
        text: "appellate".into(),
        vector: qvec.to_vec(),
        enumerate_all: true,
        patience_hops: Some(4),
        patience_saturation_ppm: Some(990_000),
        ..Default::default()
    };
    let err = coordinator
        .hybrid_search(Request::new(bad_hybrid))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let unsupported_hybrid = HybridSearchRequest {
        k: 5,
        text: "appellate".into(),
        vector: qvec.to_vec(),
        filter_strategy: DenseFilterStrategy::Speed as i32,
        ..Default::default()
    };
    let err = coordinator
        .hybrid_search(Request::new(unsupported_hybrid))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}
