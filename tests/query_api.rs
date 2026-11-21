//! Public `Query` adapter acceptance (`src/query.rs`): every supported shape executes bitwise-identically
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
    DeleteDocumentsRequest, DenseCoverage, DenseExecutionMode, DenseExecutionOutcome,
    DensePrecisionRequest, DenseQualityPolicy, DenseQuery, DocLineage, FacetValue, FilterQuery,
    FlushRequest, FusionMode, HealthRequest, HybridLegOptions, HybridSearchRequest, IntegerValue,
    LexicalQuery, QueryRequest, QueryResponse, QuerySort, QueryStreamCompletion, QueryStreamPhase,
    QueryStreamRequest, QueryStreamResponse, QueryStreamRevision, RrfScore, ScoreOp, ScoreStage,
    SearchQuery, SearchRequest, SelectionOperator, SelectionQuery, SelectionScoreStrategy,
    VectorQualityContract,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Request;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};
use prost::Message;

#[path = "common/cluster.rs"]
mod cluster;
use cluster::{start_shard, ShardGuard, ShardSpec};

#[path = "query_api/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "query_api/boolean_selection.rs"]
mod boolean_selection;
#[path = "query_api/collapse.rs"]
mod collapse;
#[path = "query_api/composites_and_validation.rs"]
mod composites_and_validation;
#[path = "query_api/cursors_and_browse.rs"]
mod cursors_and_browse;
#[path = "query_api/document_identities.rs"]
mod document_identities;
#[path = "query_api/leaves_and_dense_modes.rs"]
mod leaves_and_dense_modes;
#[path = "query_api/multi_key_sorting.rs"]
mod multi_key_sorting;
#[path = "query_api/query_streaming.rs"]
mod query_streaming;
