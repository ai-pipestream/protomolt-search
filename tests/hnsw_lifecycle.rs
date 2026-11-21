//! Lifecycle of the `hnsw` provider on real nodes : a
//! segmented shard ingests, seals its tail into an hnsw image, reopens it
//! through a memory map and answers unary and streaming dense queries from
//! it; deleted rows and replaced revisions never come back; a snapshot of an
//! hnsw image installs and survives a restart; compaction rebuilds the graph
//! over the live rows; and a publication interrupted between its write and
//! its publish rolls back at open with the old generation still serving.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{start_empty_node, start_opened_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::exact_vectors::dot;
use pipestream_search::hnsw::{HnswConfig, HNSW};
use pipestream_search::node::{segments_root, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    install_snapshot_from_request, search_query, search_shard_request, search_shard_response,
    selection_query, AddDocumentsRequest, AddVectorsRequest, CommitReplacementsRequest,
    CompactShardRequest, ConfigureVectorBackendRequest, DeleteDocumentsRequest, DenseExecutionMode,
    DenseQuery, ExportSnapshotRequest, FlushRequest, GetDocumentsRequest, HealthRequest,
    InstallSnapshotFromRequest, QueryRequest, Replacement, ScoredHit, SearchQuery,
    SearchShardRequest, SelectionQuery, StartShardSearch, VectorBackendConfig,
};
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog, VectorLoad};
use pipestream_search::vector::VectorProvider;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Code, Request};

#[path = "common/dir.rs"]
mod dir;
use dir::tempdir;

#[path = "common/ingest.rs"]
mod ingest;
use ingest::ingest;

#[path = "hnsw_lifecycle/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "hnsw_lifecycle/compaction_columns.rs"]
mod compaction_columns;
#[path = "hnsw_lifecycle/compaction_recovery.rs"]
mod compaction_recovery;
#[path = "hnsw_lifecycle/locality_reordering.rs"]
mod locality_reordering;
#[path = "hnsw_lifecycle/locality_support.rs"]
mod locality_support;
#[path = "hnsw_lifecycle/segments_and_snapshots.rs"]
mod segments_and_snapshots;
