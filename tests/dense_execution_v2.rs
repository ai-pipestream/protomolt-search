//! End-to-end verification for dense execution v2 receipts, composite legs,
//! direct RPC raw-control barriers, shard-local FP32 sidecar scanning, relay
//! child part-count integrity, and uncertified clustered TurboVec coverage.

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
    AddVectorsRequest, BlendScore, CascadeScore, CompositeSearchStrategy, DecomposedScore,
    DenseCoverage, DenseExecutionMode as Mode, DenseFilterStrategy,
    DensePrecisionRequest as Precision, DenseScorePrecision, DenseTechnique, DenseTechniqueCount,
    DenseTermination, FacetValue, FlushRequest, HybridSearchRequest, LexicalQuery, QueryRequest,
    QueryResponse, RrfScore, SearchQuery, SearchRequest, SelectionOperator, SelectionQuery,
    SelectionScoreStrategy,
};
use tonic::Request;

#[path = "dense_execution_v2/fp32_and_barriers.rs"]
mod fp32_and_barriers;
#[path = "dense_execution_v2/parts_and_composites.rs"]
mod parts_and_composites;
#[path = "dense_execution_v2/relay_and_clustered.rs"]
mod relay_and_clustered;

pub(crate) const DIM: usize = 32;

pub(crate) struct DirGuard(pub(crate) PathBuf);

impl DirGuard {
    pub(crate) fn new(tag: &str) -> Self {
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

pub(crate) fn hnsw_segmented_config(path: PathBuf, slot_offset: u64) -> NodeConfig {
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

pub(crate) async fn append_rows(addr: &str, vectors: &[f32], start_id: usize, court: &str) {
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

pub(crate) async fn flush_node(addr: &str) {
    let mut c = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    c.flush(FlushRequest {}).await.unwrap();
}

pub(crate) fn dense_leaf(vector: &[f32], mode: Mode, precision: Precision) -> SelectionQuery {
    common::dense_selection("dense", vector.to_vec(), mode as i32, precision as i32)
}

pub(crate) fn tech_count(technique: DenseTechnique, parts: u32) -> DenseTechniqueCount {
    DenseTechniqueCount {
        technique: technique as i32,
        parts,
    }
}

pub(crate) fn composite_query(
    vector: &[f32],
    mode: Mode,
    strategy: strat::Strategy,
) -> QueryRequest {
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

pub(crate) async fn run_query(coord: &CoordinatorServiceImpl, req: QueryRequest) -> QueryResponse {
    coord.query(Request::new(req)).await.unwrap().into_inner()
}
