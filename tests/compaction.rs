//! Online compaction : a shard keeps taking writes
//! while `CompactShard` rebuilds it dense, tails the log into the
//! rebuild, and cuts over under one brief write lock.
//!
//! Pinned here:
//!
//! - Writes are not frozen: a concurrent task appends, deletes, and
//!   replaces through the public RPCs for the whole compaction; its
//!   calls complete in a small fraction of the compaction's wall time,
//!   and the tail applied its records (`tail_records_applied > 0`).
//! - The write-lock hold is bounded (< 500 ms on this fixture) and
//!   reported; the work directory is gone afterwards; the old generation
//!   is gone; the rewritten WAL generation is full history.
//! - A quiescent second compaction reclaims every tombstone: `Health`
//!   shows `deleted_docs = 0` and live rows equal to the tracked set.
//! - Every read path (lexical, dense, hybrid, facets, sorted browse,
//!   fetch, parents) equals a shard freshly built from the same FINAL
//!   document set, compared by stable text with scores bitwise; a reopen
//!   from disk equals; the rewritten generation replays to the same
//!   shard; a cursor minted before the cutover refuses by name.
//! - Distributed equals monolithic with an untouched second shard.
//! - The refusal table, the snapshot-during-compaction abort, the dry
//!   run, and the rollback of an interrupted cutover.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AddDocumentsResponse, AddVectorsRequest,
    AddVectorsResponse, Bm25SearchRequest, BrowseShardRequest, BrowseSort,
    CommitReplacementsRequest, CompactShardRequest, CompactShardResponse, DeleteDocumentsRequest,
    DenseQuery, DocLineage, FacetValue, FlushRequest, GetDocumentsRequest, HealthRequest,
    HybridSearchRequest, IntegerValue, QueryRequest, Replacement, ResolveParentsRequest,
    SearchQuery, SearchRequest, SelectionQuery,
};
use pipestream_search::segments::{SegmentCatalog, SegmentMetadata, SegmentSetManifest};
use tonic::transport::Channel;
use tonic::Request;

use common::mock::start_mock_analysis;
use common::{fit_calibration, start_empty_node, start_opened_node, unit_vectors, BIT_WIDTH, DIM};

#[path = "common/dir.rs"]
mod dir;
use dir::tempdir;

use pipestream_search::compaction::{default_work_dir, marker_path};
use pipestream_search::node::segments_root;
use pipestream_search::postings::{Bm25Index, Bm25Reader};
use pipestream_search::reshard::{split_logs_with, SplitRequest};
use pipestream_search::segments::OpenedSegmentSet;

#[path = "compaction/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "compaction/observer.rs"]
mod observer;
pub(crate) use observer::*;
#[path = "compaction/block_sealing_and_summaries.rs"]
mod block_sealing_and_summaries;
#[path = "compaction/online_compaction.rs"]
mod online_compaction;
#[path = "compaction/partitioned_compaction.rs"]
mod partitioned_compaction;
#[path = "compaction/refusals_and_concurrency.rs"]
mod refusals_and_concurrency;
