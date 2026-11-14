//! Shard-owner side: serves [`NodeService`] over one vector provider.
//!
//! The shard is a small state machine behind a write lock:
//!
//! ```text
//! empty ──ConfigureVectorBackend──▶ configured empty index ──AddVectors──▶ live index
//!   │
//!   └──AddVectors(dim=..)──▶ provider-created index
//! ```
//!
//! Provider configuration locks for the generation's lifetime and can only be
//! set on an empty shard. Adds hold the write lock on the blocking pool; searches hold the read lock for the
//! duration of their chunked scan, so a search never observes a
//! half-applied batch.

mod aggregate;
mod boolean;
mod config;
mod ingest;
mod lexical;
pub(in crate::node) mod projection;
mod publication;
mod rpc;
mod scan;
mod service;
mod shard;
mod snapshot;
#[cfg(test)]
mod source_owner_tests;
mod storage;
#[cfg(test)]
mod tests;
mod vectors;

pub use self::boolean::BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS;
pub use self::config::{Layout, NodeConfig, MAX_RERANK_PARALLEL};
pub use self::projection::StagedDocumentCandidate;
pub use self::scan::{
    observe_scan, scan_batch_counters, scan_rate, ScanRate, ScanWindow, SCAN_WINDOW_MIN_SAMPLES,
    SCAN_WINDOW_MS, SCAN_WINDOW_SAMPLES,
};
pub use self::service::NodeServiceImpl;
pub use self::shard::Bm25Shard;
pub use self::snapshot::SNAPSHOT_STREAM_CHUNK;
pub use self::storage::{
    bm25_build_dir, bm25_sidecar_path, exact_vector_sidecar_path, generation_bm25, generation_dir,
    generation_exact_vectors, generation_live_docs, generation_vector, live_docs_sidecar_path,
    recover_generation, recover_segments_swap, refuse_retired_generation, segments_root,
};

pub(crate) use self::aggregate::{agg_op_name, agg_op_of};
pub(crate) use self::boolean::{evaluate_boolean_membership, EvaluatedBoolean};
pub(crate) use self::config::{check_attached_derived, stored_derived};
pub(crate) use self::ingest::{
    apply_materialize, cased_field_index, compile_materialize_spec, join_fields, materialize_sha,
    session_layers,
};
#[cfg(test)]
pub(crate) use self::publication::{
    set_rewrite_test_hooks, AFTER_MAINTENANCE_VERIFIED, FAIL_PROJECTION_DECISION,
    INTERRUPT_MAINTENANCE,
};
pub(crate) use self::shard::{
    f64_from_order_bits, f64_order_bits, i64_from_order_bits, i64_order_bits, parse_score_stages,
    physical_rows, read_shard, timestamp_to_epoch_micros, validate_geo_filter,
    validate_geo_filters, validate_range_facet_fields, write_shard, ParentMap, ScoreStageSpec,
    ShardState, STALE_STATS_EPOCH,
};
pub(crate) use self::storage::{
    generation_old_dir, generation_tmp_dir, heap_store, live_docs_storage_path, storage_paths,
    wal_append_or_degrade, wal_manifest,
};

pub(in crate::node) use self::aggregate::{rankable_bits, resolve_rankable};
#[cfg(test)]
pub(in crate::node) use self::boolean::filter_leaf_domains;
#[cfg(test)]
pub(in crate::node) use self::boolean::EvaluatedLeaf;
#[cfg(test)]
pub(in crate::node) use self::boolean::LeafSlot;
pub(in crate::node) use self::boolean::{
    check_enumerate_threshold, check_floor_prune_options, check_patience_options,
};
pub(in crate::node) use self::config::{
    check_placement_columns, declare_derived_columns, declare_placement_column, node_knobs,
    resolved_rerank_parallel, source_tables, validate_column_tables,
};
pub(in crate::node) use self::ingest::{
    geography_wanted, quality_wanted, CompiledMaterialize, IngestDoc, IngestSource, MappedSource,
};
pub(in crate::node) use self::lexical::{
    bm25_scoring_fingerprint, cut_snippets, explain_terms, highlight_plan, params_from, LegFilters,
};
#[cfg(all(test, feature = "net"))]
pub(in crate::node) use self::scan::apply_stream_datagram;
#[cfg(test)]
pub(in crate::node) use self::scan::run_scan_batch;
#[cfg(doc)]
pub(in crate::node) use self::scan::MAX_COALESCE;
pub(in crate::node) use self::scan::{raise_floor_cell, ScanJob, ScanOutcome, StreamSignals};
pub(in crate::node) use self::shard::{
    active_artifact_rows, filter_known_flags, int_min_max_as_f64, live_document_count,
    live_field_stats, projected_value, prune_segments, resolve_shard_filters, uint_min_max_as_f64,
    unknown_range_counts, validate_lat_lon, EmptyColumns, IngestGuard, ResolvedShardFilters,
    SegmentPrune, ShardNumericRead,
};
pub(in crate::node) use self::snapshot::stream_file;
pub(in crate::node) use self::storage::{
    check_wal_tables, export_staging_dir, internal_backend_config, open_wal, probe_path_exists,
    segments_old_dir, wire_backend_config, wire_backend_descriptor,
};

pub(in crate::node) use std::collections::HashMap;
pub(in crate::node) use std::io::Read;
pub(in crate::node) use std::path::{Path, PathBuf};
pub(in crate::node) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
pub(in crate::node) use std::sync::{Arc, RwLock};

pub(in crate::node) use tokio::sync::{mpsc, watch};
pub(in crate::node) use tokio_stream::wrappers::ReceiverStream;
pub(in crate::node) use tonic::{Request, Response, Status, Streaming};

pub(in crate::node) use crate::bm25::{self, Bm25Params};
pub(in crate::node) use crate::chunked::{
    chunked_topk, chunked_topk_collapsed, BatchQuery, ChunkHit, ScanStats, DEFAULT_CHUNK_BLOCKS,
};
pub(in crate::node) use crate::exact_vectors::ExactVectorStore;
pub(in crate::node) use crate::fusion::{self, Leg};
pub(in crate::node) use crate::live_docs::LiveDocs;
pub(in crate::node) use crate::metrics::Route;
pub(in crate::node) use crate::pb::node_service_server::{NodeService, NodeServiceServer};
pub(in crate::node) use crate::pb::wal::{
    wal_record, FlushMarker, LoggedAddDocuments, LoggedAddVectors, LoggedDeleteDocument,
    LoggedReplacement, SnapshotMarker,
};
pub(in crate::node) use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, search_shard_request,
    search_shard_response, snapshot_chunk, stream_search_request, stream_search_response,
    AddDocumentsRequest, AddDocumentsResponse, AddVectorsRequest, AddVectorsResponse,
    Bm25CandidateBatch, Bm25Hit, Bm25QueryRequest, Bm25QueryResponse, Bm25QueryStreamRequest,
    Bm25QueryStreamResponse, Bm25RescoreRequest, Bm25RescoreResponse, Bm25StreamCompletion,
    CommitReplacementsRequest, CommitReplacementsResponse, ConfigureVectorBackendRequest,
    ConfigureVectorBackendResponse, DeleteDocumentsRequest, DeleteDocumentsResponse,
    ExactVectorRescoreRequest, ExactVectorRescoreResponse, ExportSnapshotRequest,
    ExportSnapshotResponse, FloorUpdate, FlushRequest, FlushResponse, GetDocumentsRequest,
    GetDocumentsResponse, GetVectorBackendRequest, GetVectorBackendResponse, HealthRequest,
    HealthResponse, HybridLegHit, HybridShardRequest, HybridShardResponse, IngestMappedRequest,
    IngestMappedResponse, InstallSnapshotFromRequest, InstallSnapshotResponse,
    LexicalBitmapRequest, MembershipBitmapResponse, OffsetSpan, RawLegHit, ReadWalRequest,
    ReadWalResponse, Replacement, ResolveParentsRequest, ResolveParentsResponse, ResolvedParent,
    ScoredHit, SearchShardDone, SearchShardRequest, SearchShardResponse, ShardLegsRequest,
    ShardLegsResponse, ShardScanStats, SnapshotChunk, SnapshotManifest, StartShardSearch,
    StoredDocument, StreamSearchBatch, StreamSearchRequest, StreamSearchResponse,
    StreamSearchSummary, StreamSnapshotRequest, TermOccurrences, TermStatsRequest,
    TermStatsResponse, VectorBackendConfig as WireVectorBackendConfig,
    VectorBackendDescriptor as WireVectorBackendDescriptor, VectorBitmapRequest,
    VectorQualityContract, VectorRescoreRequest, VectorRescoreResponse, VectorScoreDirection,
};
pub(in crate::node) use crate::postings::{Bm25Index, Bm25Reader, Bm25Store, SpillBuilder};
pub(in crate::node) use crate::segmented::SegmentedShard;
pub(in crate::node) use crate::segmented_vectors::SegmentedProvider;
pub(in crate::node) use crate::snapshot_repository::{
    self as repo, RepositoryManifest, CATALOG_DIR, LAYOUT_SEGMENTS, LAYOUT_SINGLE_IMAGE,
};
pub(in crate::node) use crate::vector::{
    first_invalid_coordinate, QualityContract, ScoreDirection, VectorBackendConfig, VectorIndex,
    VectorSearchOptions, VectorStreamControl, EMBEDDED_TURBOVEC,
};
pub(in crate::node) use crate::wal::{self, WalWriter};
