//! The HNSW graph provider served by real nodes behind a
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

#[path = "hnsw/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "hnsw/ann_and_qualification.rs"]
mod ann_and_qualification;
#[path = "hnsw/filtered_and_policy.rs"]
mod filtered_and_policy;
#[path = "hnsw/multi_shard_floor_pruning.rs"]
mod multi_shard_floor_pruning;
#[path = "hnsw/stream_straggler_and_identities.rs"]
mod stream_straggler_and_identities;
#[path = "hnsw/tail_and_patience.rs"]
mod tail_and_patience;
