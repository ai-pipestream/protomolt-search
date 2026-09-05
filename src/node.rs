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
//! set on an empty shard. The legacy `SetCalibration` RPC adapts to the same
//! rule for embedded TurboVec. Adds hold the
//! write lock on the blocking pool; searches hold the read lock for the
//! duration of their chunked scan, so a search never observes a
//! half-applied batch.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::bm25::{self, Bm25Params};
use crate::chunked::{
    chunked_topk, chunked_topk_batch, chunked_topk_collapsed, BatchQuery, ChunkHit, ScanStats,
    DEFAULT_CHUNK_BLOCKS,
};
use crate::exact_vectors::ExactVectorStore;
use crate::fusion::{self, Leg};
use crate::live_docs::LiveDocs;
use crate::metrics::Route;
use crate::pb::node_service_server::{NodeService, NodeServiceServer};
use crate::pb::wal::{
    wal_record, FlushMarker, LoggedAddDocuments, LoggedAddVectors, LoggedDeleteDocument,
    LoggedReplacement, SnapshotMarker,
};
use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, search_shard_request,
    search_shard_response, snapshot_chunk, stream_search_request, stream_search_response,
    AddDocumentsRequest, AddDocumentsResponse, AddVectorsRequest, AddVectorsResponse,
    Bm25CandidateBatch, Bm25Hit, Bm25QueryRequest, Bm25QueryResponse, Bm25QueryStreamRequest,
    Bm25QueryStreamResponse, Bm25RescoreRequest, Bm25RescoreResponse, Bm25StreamCompletion,
    CommitReplacementsRequest, CommitReplacementsResponse, ConfigureVectorBackendRequest,
    ConfigureVectorBackendResponse, DeleteDocumentsRequest, DeleteDocumentsResponse,
    ExactVectorRescoreRequest, ExactVectorRescoreResponse, ExportSnapshotRequest,
    ExportSnapshotResponse, FloorUpdate, FlushRequest, FlushResponse, GetCalibrationRequest,
    GetCalibrationResponse, GetDocumentsRequest, GetDocumentsResponse, GetVectorBackendRequest,
    GetVectorBackendResponse, HealthRequest, HealthResponse, HybridLegHit, HybridShardRequest,
    HybridShardResponse, IngestMappedRequest, IngestMappedResponse, InstallSnapshotFromRequest,
    InstallSnapshotResponse, LexicalBitmapRequest, MembershipBitmapResponse, OffsetSpan, RawLegHit,
    ReadWalRequest, ReadWalResponse, Replacement, ResolveParentsRequest, ResolveParentsResponse,
    ResolvedParent, ScoredHit, SearchShardDone, SearchShardRequest, SearchShardResponse,
    SetCalibrationRequest, SetCalibrationResponse, ShardLegsRequest, ShardLegsResponse,
    ShardScanStats, SnapshotChunk, SnapshotManifest, StartShardSearch, StoredDocument,
    StreamSearchBatch, StreamSearchRequest, StreamSearchResponse, StreamSearchSummary,
    StreamSnapshotRequest, TermOccurrences, TermStatsRequest, TermStatsResponse,
    VectorBackendConfig as WireVectorBackendConfig,
    VectorBackendDescriptor as WireVectorBackendDescriptor, VectorBitmapRequest,
    VectorQualityContract, VectorRescoreRequest, VectorRescoreResponse, VectorScoreDirection,
};
use crate::postings::{Bm25Index, Bm25Reader, Bm25Store, SpillBuilder};
use crate::segmented::SegmentedShard;
use crate::segmented_vectors::SegmentedProvider;
use crate::snapshot_repository::{
    self as repo, RepositoryManifest, CATALOG_DIR, LAYOUT_SEGMENTS, LAYOUT_SINGLE_IMAGE,
};
use crate::vector::{
    embedded_turbovec_config, first_invalid_coordinate, legacy_calibration_config, QualityContract,
    ScoreDirection, VectorBackendConfig, VectorIndex, VectorSearchOptions, VectorStreamControl,
    EMBEDDED_TURBOVEC,
};
use crate::wal::{self, WalWriter};

pub const MAX_RERANK_PARALLEL: usize = 64;
const STABLE_ROUTING_KEY_METADATA: &str = "x-protomolt-stable-key-bin";

fn replication_stable_key<T>(request: &Request<T>) -> Result<Option<Vec<u8>>, Status> {
    request
        .metadata()
        .get_bin(STABLE_ROUTING_KEY_METADATA)
        .map(|value| {
            value
                .to_bytes()
                .map(|bytes| bytes.to_vec())
                .map_err(|error| {
                    Status::invalid_argument(format!(
                        "invalid replication stable-key metadata: {error}"
                    ))
                })
        })
        .transpose()
}

fn resolved_rerank_parallel(configured: usize) -> usize {
    if configured == 0 {
        std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(4)
    } else {
        configured.min(MAX_RERANK_PARALLEL)
    }
}

/// Identity of one BM25 score space. The canonical request keeps only
/// fields that can change a document's score; delivery depth, floors,
/// filters, projections, aggregations, and the per-shard epoch claim are
/// excluded so every shard in one globally scored round reports the same
/// value.
fn bm25_scoring_fingerprint(req: &Bm25QueryRequest) -> String {
    let mut canonical = req.clone();
    canonical.k = 0;
    canonical.min_score = 0.0;
    canonical.expected_stats_epoch = 0;
    canonical.facet_fields.clear();
    canonical.map_facet_fields.clear();
    canonical.range_facet_fields.clear();
    canonical.geo_filters.clear();
    canonical.filter = None;
    canonical.stats_fields.clear();
    canonical.cardinality_fields.clear();
    canonical.projections.clear();
    crate::sha256::hex_digest(&prost::Message::encode_to_vec(&canonical))
}

/// How a persisted shard lays out its documents and vectors
/// (`docs/immutable-segments.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Layout {
    /// The segment catalog under `<index>.segments/` plus a heap tail
    /// sealed into a new segment on every flush: the default for a new
    /// persisted shard.
    #[default]
    Segments,
    /// One vector image, one `.bm25` file, rewritten on every flush.
    SingleImage,
}

/// How a node scans and whether it participates in floor sharing.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Vector backend used for new and loaded generations. File extensions
    /// never select a backend.
    pub vector_backend: String,
    /// Added to every local slot to produce the global vector id reported
    /// in [`SearchShardDone`]. Shards must have disjoint ranges.
    pub slot_offset: u64,
    /// Chunk size in SIMD blocks for the scan (see [`chunked_topk`]).
    pub chunk_blocks: usize,
    /// When false, the node still scans in chunks but ignores coordinator
    /// floor updates and does not publish its own floor — the
    /// "sharing disabled" baseline for A/B benchmarking.
    pub share_floors: bool,
    /// When false, BM25 scoring always takes the exhaustive path
    /// (`top_k`) even on v5 shards with a skip run — the "block-max
    /// disabled" baseline for A/B benchmarking. Results are identical
    /// either way; only the cost changes.
    pub block_max: bool,
    /// Minimum improvement over the last PUBLISHED floor before the next
    /// one goes on the wire. 0.0 publishes every raise (the historical
    /// behavior); a small positive delta trades a sliver of pruning
    /// reactivity for far fewer floor messages on real networks.
    pub floor_delta: f32,
    /// Publish opportunities to SKIP before the first floor goes out.
    ///
    /// The scanner offers a floor after every chunk in which its heap is
    /// full, so on a large shard that is one message per chunk for the
    /// whole scan. The earliest floors are also the weakest: they prune
    /// least, yet each one costs a coordinator broadcast to EVERY shard.
    /// Skipping the first few trades a little early pruning for a large
    /// cut in messages. 0 keeps the historical behavior.
    ///
    /// Measured on the 86.6M-chunk corpus: 42 chunks per shard, so 42
    /// publishes per shard per query, 336 across the fleet. Note that a
    /// CANDIDATE-count warmup does not bite at this granularity -- the
    /// first chunk alone collects ~6,000 candidates, so the heap is full
    /// almost immediately; chunks are the unit that actually gates.
    pub floor_warmup_chunks: u32,
    /// Minimum wall time between two published floors, in milliseconds.
    ///
    /// Independent of `floor_delta`, which gates on score movement: a
    /// scan can raise its floor meaningfully many times in quick
    /// succession and still not be worth that many broadcasts. 0 disables
    /// the debounce. Floors are monotone, so a suppressed one is never
    /// lost information -- the next publish carries a floor at least as
    /// high.
    pub floor_min_interval_ms: u64,
    /// Provider-specific bit width hint used when `AddVectors` constructs an
    /// index from scratch with no loaded or configured backend.
    pub bit_width: usize,
    /// Persistence target for `Flush` / save-on-shutdown. `None` makes the
    /// shard purely in-memory (flush is a no-op).
    pub index_path: Option<PathBuf>,
    /// Lexical analysis backend (`native` or `http://host:port`) for
    /// AddDocuments. `None` makes AddDocuments fail UNAVAILABLE.
    pub analysis_addr: Option<String>,
    /// The BM25 field table for NEW builders (`docs/multi-field.md`):
    /// "body" first, then the extra indexed fields. Shards loaded from
    /// existing `.bm25` files keep the table they were written with;
    /// documents naming fields outside the active table are refused.
    pub bm25_fields: Vec<String>,
    /// The facet field table for NEW builders (dictionary-encoded
    /// per-doc columns, `docs/plans/track-1-features.md` section 2).
    /// Same rules as `bm25_fields`: shards loaded from existing files
    /// keep the table they were written with; documents naming facet
    /// fields outside the active table are refused. Non-empty makes
    /// new builders persist as v7.
    pub facet_fields: Vec<String>,
    /// The numeric field table for NEW builders (f64 columns,
    /// `docs/score-functions.md`). Same rules as `facet_fields`.
    pub numeric_fields: Vec<String>,
    /// The map<string, string> column table for NEW builders
    /// (`docs/map-columns.md`). Same rules as `facet_fields`.
    pub map_facet_fields: Vec<String>,
    /// The map<string, f64> column table for NEW builders. Same rules.
    pub map_numeric_fields: Vec<String>,
    /// The i64 column table for NEW builders (`docs/range-facets.md`).
    /// Same rules as `facet_fields`. Timestamp ingest lands in THESE
    /// columns as epoch micros — it is sugar, not a kind.
    pub integer_fields: Vec<String>,
    /// The geo-point column table for NEW builders
    /// (`docs/geo-columns.md`). Same rules as `facet_fields`; the
    /// columns geo FILTERS and distance-decay stages read.
    pub geo_fields: Vec<String>,
    /// BM25 fields that keep token positions per occurrence
    /// (`docs/phrase-proximity.md`), each a name from `bm25_fields`. New
    /// builders declare them; a shard loaded from a file keeps the
    /// file's own declaration, and ingest refuses a positional field the
    /// active file never declared rather than storing a half-positional
    /// column.
    pub position_fields: Vec<String>,
    /// Fields whose sentence spans are stored per document for
    /// server-side snippets (`docs/highlighting.md`). Only the body has
    /// stored text to cut from, so the list is `["body"]` or empty.
    pub sentence_fields: Vec<String>,
    /// The collection this node serves (`docs/collections.md`); empty for
    /// a node outside any named collection. Reported in health, written
    /// on every logged document, checked on every bind, and matched
    /// against the WAL manifest at open.
    pub collection: String,
    /// The key that authenticates UDP floor and cancel datagrams
    /// (`docs/security.md`). With a key, only signed datagrams with a
    /// fresh sequence are applied; without one, unsigned datagrams are
    /// accepted on a loopback listener only.
    pub udp_hmac_key: Option<crate::security::UdpKey>,
    /// The layout a NEW persisted shard gets (`docs/immutable-segments.md`).
    /// An existing shard keeps the layout its files have; nothing
    /// converts on open.
    pub layout: Layout,
    /// Serve sealed segments' vector images from their files through
    /// memory maps (`docs/mmap-vectors.md`); off loads them into memory.
    /// The tail and single-image shards are owned either way.
    pub vector_mmap: bool,
    /// On a segmented shard, seal the tail into a segment once it holds
    /// this many documents, so a bulk ingest stays bounded in heap
    /// without waiting for a flush. 0 seals only on flush.
    pub seal_tail_docs: u32,
    /// Source fields whose adjacent-token pairs are derived into a
    /// bigram column named `<source>.bigrams`, which must itself be in
    /// `bm25_fields`. Derived at ingest from the source's positions, so
    /// clients never supply the column.
    pub bigram_fields: Vec<String>,
    /// Keep a write-ahead log at `<index path>.wal/` (see [`crate::wal`]).
    /// Requires `index_path`; the config layer defaults this on for
    /// persisted shards and off for demo shards.
    pub wal: bool,
    /// Number of WAL hash buckets (`bucket-NNN.wal` files per
    /// generation). Fixed at WAL creation; a resumed log keeps its own.
    pub wal_buckets: u32,
    /// Accumulate vocabulary statistics inline in the AddDocuments
    /// AnalyzeStream path, snapshotting per window to
    /// `<index path>.vocab/` (see [`crate::vocab`]). Requires
    /// `index_path`; defaults off — zero overhead when off.
    pub vocab: bool,
    /// Documents per vocabulary window before automatic rollover.
    pub vocab_window_docs: u64,
    /// Heavy-hitter list size per vocabulary channel.
    pub vocab_top_k: usize,
    /// Coalesce concurrent shard scans into batched kernel calls (up to
    /// [`MAX_COALESCE`] queries share each pass over the packed codes —
    /// the scan is bandwidth-bound, so batched queries ride the same
    /// memory traffic). `false` runs one scan per RPC — the A/B
    /// baseline; results are identical either way.
    pub coalesce: bool,
    /// Concurrent batched scans (blocking threads). 0 sizes from the
    /// machine: half the available cores, at least one.
    pub scan_parallel: usize,
    /// Bounded worker lanes for page-local FP32 candidate reranking. Zero
    /// sizes from the machine, capped at four to avoid competing with vector
    /// scans by default.
    pub rerank_parallel: usize,
}

impl NodeConfig {
    /// How sealed vector images are served under this configuration.
    pub fn vector_load(&self) -> crate::segments::VectorLoad {
        if self.vector_mmap {
            crate::segments::VectorLoad::Mapped
        } else {
            crate::segments::VectorLoad::Heap
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            vector_backend: EMBEDDED_TURBOVEC.to_string(),
            slot_offset: 0,
            chunk_blocks: DEFAULT_CHUNK_BLOCKS,
            share_floors: true,
            block_max: true,
            floor_delta: 0.0,
            floor_warmup_chunks: 0,
            floor_min_interval_ms: 0,
            bit_width: 4,
            index_path: None,
            analysis_addr: None,
            bm25_fields: vec!["body".to_string()],
            facet_fields: Vec::new(),
            numeric_fields: Vec::new(),
            map_facet_fields: Vec::new(),
            map_numeric_fields: Vec::new(),
            integer_fields: Vec::new(),
            geo_fields: Vec::new(),
            position_fields: Vec::new(),
            sentence_fields: Vec::new(),
            collection: String::new(),
            udp_hmac_key: None,
            layout: Layout::Segments,
            vector_mmap: true,
            seal_tail_docs: 500_000,
            bigram_fields: Vec::new(),
            wal: false,
            wal_buckets: 64,
            vocab: false,
            vocab_window_docs: crate::vocab::DEFAULT_WINDOW_DOCS,
            vocab_top_k: crate::vocab::HeavyHitters::DEFAULT_CAPACITY,
            coalesce: true,
            scan_parallel: 0,
            rerank_parallel: 0,
        }
    }
}

/// Raw leg hits as `(global_doc_id, raw_score)`, score-descending.
type RawLeg = Vec<(u64, f64)>;

/// The filter half of a leg request: the wire filters plus the geo
/// regions already validated at the RPC boundary. Bundled because both
/// leg routes carry the identical trio and `compute_legs` was already
/// at the argument limit.
struct LegFilters<'a> {
    geo: &'a [crate::pb::GeoFilter],
    regions: Vec<crate::geo::GeoRegion>,
    tree: Option<&'a crate::pb::FilterExpr>,
}

/// What one shard's leg computation produced: the two raw legs plus
/// the known-column handshakes, which travel with every filtered
/// response so the coordinator can refuse a name no shard resolves.
struct LegResults {
    vector: RawLeg,
    bm25: RawLeg,
    geo_columns_known: Vec<bool>,
    filter_columns_known: Vec<bool>,
}

/// The BM25 half's two storage shapes: the heap builder used during
/// ingest, and the disk-resident mmap reader used after Flush and on
/// startup. Once resident, a shard holds no postings or document texts
/// in heap — only the small per-doc tables.
pub enum Bm25Shard {
    /// Heap builder (small or append ingests; searchable mid-build).
    Building(Bm25Store),
    /// Disk-spilling bulk builder (fresh persisted shards): bounded heap,
    /// NOT searchable until flushed.
    Spilling(SpillBuilder),
    /// Disk-resident mmap reader over the v3 file.
    Resident(Bm25Reader),
    /// The segment catalog plus a heap tail (`docs/immutable-segments.md`):
    /// the default layout of a new persisted shard.
    Segmented(SegmentedShard),
}

impl Bm25Shard {
    /// The searchable read surface; `None` while bulk-building (a spill
    /// builder cannot answer term lookups without scanning every run).
    fn as_index(&self) -> Option<&dyn Bm25Index> {
        match self {
            Bm25Shard::Building(s) => Some(s),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(r),
            Bm25Shard::Segmented(g) => Some(g),
        }
    }

    pub(crate) fn next_doc_id(&self) -> u32 {
        match self {
            Bm25Shard::Building(s) => s.next_doc_id(),
            Bm25Shard::Spilling(s) => s.next_doc_id(),
            Bm25Shard::Resident(r) => r.next_doc_id(),
            Bm25Shard::Segmented(g) => g.next_doc_id(),
        }
    }

    fn doc_count(&self) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.doc_count(),
            Bm25Shard::Spilling(s) => s.doc_count(),
            Bm25Shard::Resident(r) => Bm25Index::doc_count(r),
            Bm25Shard::Segmented(g) => Bm25Index::doc_count(g),
        }
    }

    /// Fields in the active table (`docs/multi-field.md`).
    pub(crate) fn field_count(&self) -> usize {
        match self {
            Bm25Shard::Building(s) => s.field_count(),
            Bm25Shard::Spilling(s) => s.field_count(),
            Bm25Shard::Resident(r) => r.field_count(),
            Bm25Shard::Segmented(g) => g.field_count(),
        }
    }

    /// The name of field `f` in the active table.
    pub(crate) fn field_name(&self, f: usize) -> &str {
        match self {
            Bm25Shard::Building(s) => s.field_name(f),
            Bm25Shard::Spilling(s) => s.field_name(f),
            Bm25Shard::Resident(r) => r.field_name(f),
            Bm25Shard::Segmented(g) => g.field_name(f),
        }
    }

    /// Field `f`'s analyzer fingerprint in the active table (0 =
    /// unknown, which never enforces).
    /// The mapped-plan binding persisted with this shard, if any.
    pub(crate) fn binding(&self) -> Option<&crate::postings::StoredBinding> {
        match self {
            Bm25Shard::Building(s) => s.binding(),
            Bm25Shard::Spilling(s) => s.binding(),
            Bm25Shard::Resident(r) => r.binding(),
            Bm25Shard::Segmented(g) => g.binding(),
        }
    }

    pub(crate) fn analysis_fingerprint(&self, f: usize) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.analysis_fingerprint(f),
            Bm25Shard::Spilling(s) => s.analysis_fingerprint(f),
            Bm25Shard::Resident(r) => r.analysis_fingerprint(f),
            Bm25Shard::Segmented(g) => g.analysis_fingerprint(f),
        }
    }

    /// Whether field `f` keeps token positions (`docs/phrase-proximity.md`)
    /// in this shard's active storage — the declaration on a builder,
    /// the kind-7 entry on a file.
    fn field_has_positions(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_positions(f),
            Bm25Shard::Spilling(s) => s.field_has_positions(f),
            Bm25Shard::Resident(r) => r.field_has_positions(f),
            Bm25Shard::Segmented(g) => g.field_has_positions(f),
        }
    }

    /// Whether field `f` keeps sentence spans (`docs/highlighting.md`)
    /// in this shard's active storage — the declaration on a builder,
    /// the kind-8 entry on a file.
    fn field_has_sentences(&self, f: usize) -> bool {
        match self {
            Bm25Shard::Building(s) => s.field_has_sentences(f),
            Bm25Shard::Spilling(s) => s.field_has_sentences(f),
            Bm25Shard::Resident(r) => r.field_has_sentences(f),
            Bm25Shard::Segmented(g) => g.field_has_sentences(f),
        }
    }

    /// Record field `f`'s analyzer fingerprint, refusing a contradiction.
    /// A disk-resident shard is not being written to, so it has nothing
    /// to record.
    fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        match self {
            Bm25Shard::Building(s) => s.set_analysis_fingerprint(f, fingerprint),
            Bm25Shard::Spilling(s) => s.set_analysis_fingerprint(f, fingerprint),
            Bm25Shard::Resident(_) => Ok(()),
            Bm25Shard::Segmented(g) => g.set_analysis_fingerprint(f, fingerprint),
        }
    }

    /// The table index of the field named `name`, if present. `None`
    /// while bulk-building (no searchable surface to resolve against).
    fn field_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.field_index(name),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => r.field_index(name),
            Bm25Shard::Segmented(g) => g.field_index(name),
        }
    }

    /// The facet-table index of the facet field named `name`, if the
    /// active table has it.
    fn facet_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.facet_index(name),
            Bm25Shard::Spilling(s) => s.facet_index(name),
            Bm25Shard::Resident(r) => r.facet_index(name),
            Bm25Shard::Segmented(g) => g.facet_index(name),
        }
    }

    /// Number of distinct values facet field `fi` holds. Counting only
    /// runs against searchable shapes, so the Spilling arm is
    /// unreachable (a spilling shard refused the query already).
    fn facet_value_count(&self, fi: usize) -> usize {
        match self {
            Bm25Shard::Building(s) => s.facet_value_count(fi),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value_count(fi),
            Bm25Shard::Segmented(g) => g.facet_value_count(fi),
        }
    }

    /// The value of facet field `fi` at ordinal `ord`.
    fn facet_value(&self, fi: usize, ord: u32) -> &str {
        match self {
            Bm25Shard::Building(s) => s.facet_value(fi, ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value(fi, ord),
            Bm25Shard::Segmented(g) => g.facet_value(fi, ord),
        }
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`.
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.facet_ord(fi, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_ord(fi, doc_id),
            Bm25Shard::Segmented(g) => g.facet_ord(fi, doc_id),
        }
    }

    /// The ordinal of `value` in facet field `fi`'s dictionary, `None`
    /// when this shard never ingested it (`docs/cel-filters.md`).
    fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.facet_value_ord_of(fi, value),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value_ord_of(fi, value),
            Bm25Shard::Segmented(g) => g.facet_value_ord_of(fi, value),
        }
    }

    /// The numeric-table index of the numeric field named `name`, if
    /// the active table has it.
    fn numeric_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.numeric_index(name),
            Bm25Shard::Spilling(s) => s.numeric_index(name),
            Bm25Shard::Resident(r) => r.numeric_index(name),
            Bm25Shard::Segmented(g) => g.numeric_index(name),
        }
    }

    /// (min, max) of numeric field `ni` over present values. Scoring
    /// only runs against searchable shapes, so Spilling is unreachable.
    fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        match self {
            Bm25Shard::Building(s) => s.numeric_min_max(ni),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.numeric_min_max(ni),
            Bm25Shard::Segmented(g) => g.numeric_min_max(ni),
        }
    }

    /// `doc_id`'s value for numeric field `ni`.
    fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        match self {
            Bm25Shard::Building(s) => s.numeric_value(ni, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.numeric_value(ni, doc_id),
            Bm25Shard::Segmented(g) => g.numeric_value(ni, doc_id),
        }
    }

    /// The integer-table index of the i64 field named `name`, if the
    /// active table has it.
    fn integer_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.integer_index(name),
            Bm25Shard::Spilling(s) => s.integer_index(name),
            Bm25Shard::Resident(r) => r.integer_index(name),
            Bm25Shard::Segmented(g) => g.integer_index(name),
        }
    }

    /// (min, max) of integer field `ii` over present values; the empty
    /// range (i64::MAX, i64::MIN) when the column holds none. Scoring
    /// only runs against searchable shapes, so Spilling is unreachable.
    fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        match self {
            Bm25Shard::Building(s) => s.integer_min_max(ii),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.integer_min_max(ii),
            Bm25Shard::Segmented(g) => g.integer_min_max(ii),
        }
    }

    /// `doc_id`'s value for integer field `ii`.
    fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        match self {
            Bm25Shard::Building(s) => s.integer_value(ii, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.integer_value(ii, doc_id),
            Bm25Shard::Segmented(g) => g.integer_value(ii, doc_id),
        }
    }

    /// The geo-table index of the geo field named `name`, if the
    /// active table has it.
    fn geo_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.geo_index(name),
            Bm25Shard::Spilling(s) => s.geo_index(name),
            Bm25Shard::Resident(r) => r.geo_index(name),
            Bm25Shard::Segmented(g) => g.geo_index(name),
        }
    }

    /// `doc_id`'s (lat, lon) for geo field `gi`. Scoring only runs
    /// against searchable shapes, so Spilling is unreachable.
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        match self {
            Bm25Shard::Building(s) => s.geo_value(gi, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.geo_value(gi, doc_id),
            Bm25Shard::Segmented(g) => g.geo_value(gi, doc_id),
        }
    }

    /// Resolve validated geo filters against THIS shard's geo table
    /// (`docs/geo-columns.md`). A column the table lacks resolves to
    /// `None`, which fails every document — its documents genuinely
    /// hold no location, so "not inside the region" is the exact
    /// answer. The coordinator refuses a column NO shard knows.
    fn resolve_geo_filters(
        &self,
        filters: &[crate::pb::GeoFilter],
        regions: &[crate::geo::GeoRegion],
    ) -> crate::geo::GeoFilters {
        crate::geo::GeoFilters {
            filters: filters
                .iter()
                .zip(regions)
                .map(|(f, &region)| crate::geo::GeoFilter {
                    column: self.geo_index(&f.column),
                    region,
                })
                .collect(),
        }
    }

    /// Which requested geo filters' columns this shard's table has,
    /// parallel to `filters`. Computed regardless of `k`, like the
    /// facet and stage known flags: a typo must refuse even on a query
    /// that returns nothing.
    fn geo_columns_known(&self, filters: &[crate::pb::GeoFilter]) -> Vec<bool> {
        filters
            .iter()
            .map(|f| self.geo_index(&f.column).is_some())
            .collect()
    }

    /// Resolve a VALIDATED filter tree against this shard's tables
    /// (`docs/cel-filters.md`): names to table indices, string values
    /// to dictionary ordinals, number bounds into the resolved
    /// family's exact domain. A name (or map key) this shard lacks
    /// resolves to the absent case for every document — exact, the
    /// same argument as [`Self::resolve_geo_filters`] — and the
    /// coordinator refuses a leaf NO shard resolves.
    fn resolve_filter(
        &self,
        expr: &crate::pb::FilterExpr,
    ) -> Result<crate::filter::ResolvedFilter, Status> {
        use crate::filter::{MapKeyRef, ResolvedFilter, ResolvedLeaf};
        use crate::pb::filter_expr::Expr;
        let sorted_ords = |mut ords: Vec<u32>| {
            ords.sort_unstable();
            ords.dedup();
            ords
        };
        Ok(
            match expr.expr.as_ref().expect("filter validated before resolve") {
                Expr::And(list) => ResolvedFilter::And(
                    list.exprs
                        .iter()
                        .map(|e| self.resolve_filter(e))
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Or(list) => ResolvedFilter::Or(
                    list.exprs
                        .iter()
                        .map(|e| self.resolve_filter(e))
                        .collect::<Result<_, _>>()?,
                ),
                Expr::Not(child) => ResolvedFilter::Not(Box::new(self.resolve_filter(child)?)),
                Expr::StringRange(p) => ResolvedFilter::Leaf(self.resolve_string_predicate(
                    &p.column,
                    &p.key,
                    p.min.as_ref().map(|b| (b.value.as_str(), b.exclusive)),
                    p.max.as_ref().map(|b| (b.value.as_str(), b.exclusive)),
                    None,
                )?),
                Expr::StringPrefix(p) => ResolvedFilter::Leaf(self.resolve_string_predicate(
                    &p.column,
                    &p.key,
                    None,
                    None,
                    Some(p.prefix.as_str()),
                )?),
                Expr::Facet(p) => {
                    let column = self.facet_index(&p.column);
                    let ords = match column {
                        None => Vec::new(),
                        Some(fi) => sorted_ords(
                            p.values
                                .iter()
                                .filter_map(|v| self.facet_value_ord_of(fi, v))
                                .collect(),
                        ),
                    };
                    ResolvedFilter::Leaf(ResolvedLeaf::Facet { column, ords })
                }
                Expr::Number(p) => {
                    let lo = p.min.as_ref().and_then(crate::filter::edge_of);
                    let hi = p.max.as_ref().and_then(crate::filter::edge_of);
                    // i64 first, then f64 — the RangeFacetField resolution
                    // order, so the two number surfaces can never disagree
                    // about which table a name means.
                    let leaf = if let Some(ii) = self.integer_index(&p.column) {
                        let (lo, hi) = crate::filter::int_range(&lo, &hi);
                        ResolvedLeaf::IntRange { column: ii, lo, hi }
                    } else if let Some(ni) = self.numeric_index(&p.column) {
                        ResolvedLeaf::F64Range { column: ni, lo, hi }
                    } else {
                        ResolvedLeaf::NumberUnknown
                    };
                    ResolvedFilter::Leaf(leaf)
                }
                Expr::MapFacet(p) => {
                    let target = self
                        .map_facet_index(&p.column)
                        .and_then(|ci| self.map_facet_key_ord(ci, &p.key).map(|k| (ci, k)));
                    let ords = match target {
                        None => Vec::new(),
                        Some((ci, _)) => sorted_ords(
                            p.values
                                .iter()
                                .filter_map(|v| self.map_facet_value_ord_of(ci, v))
                                .collect(),
                        ),
                    };
                    ResolvedFilter::Leaf(ResolvedLeaf::MapFacet { target, ords })
                }
                Expr::MapNumber(p) => {
                    let target = self
                        .map_numeric_index(&p.column)
                        .and_then(|ci| self.map_numeric_key_ord(ci, &p.key).map(|k| (ci, k)));
                    ResolvedFilter::Leaf(ResolvedLeaf::MapNumber {
                        target,
                        lo: p.min.as_ref().and_then(crate::filter::edge_of),
                        hi: p.max.as_ref().and_then(crate::filter::edge_of),
                    })
                }
                Expr::MapHasKey(p) => {
                    // Map-facet first, then map-numeric: the one order,
                    // shared with `filter_columns_known` below.
                    let target = if let Some(ci) = self.map_facet_index(&p.column) {
                        MapKeyRef::Facet {
                            column: ci,
                            key_ord: self.map_facet_key_ord(ci, &p.key),
                        }
                    } else if let Some(ci) = self.map_numeric_index(&p.column) {
                        MapKeyRef::Numeric {
                            column: ci,
                            key_ord: self.map_numeric_key_ord(ci, &p.key),
                        }
                    } else {
                        MapKeyRef::Unknown
                    };
                    ResolvedFilter::Leaf(ResolvedLeaf::MapHasKey(target))
                }
                Expr::Has(p) => ResolvedFilter::Leaf(ResolvedLeaf::Has {
                    facet: self.facet_index(&p.column),
                    numeric: self.numeric_index(&p.column),
                    integer: self.integer_index(&p.column),
                    geo: self.geo_index(&p.column),
                }),
                Expr::Geo(g) => ResolvedFilter::Leaf(ResolvedLeaf::Geo {
                    column: self.geo_index(&g.column),
                    region: validate_geo_filter(g).expect("filter validated before resolve"),
                }),
            },
        )
    }

    /// Resolve a string range (`min`/`max`, each `(value, exclusive)`)
    /// or a `prefix` over a facet column (`key` empty) or a map-facet
    /// value (`docs/prefix-terms.md`), to an ORDINAL RANGE when the
    /// column's dictionary is in byte order — every file this writer
    /// produces, checked at open — and to plain ordinal membership on
    /// the heap builder, whose first-seen dictionary is in heap and
    /// scanned once per request. A disk-resident dictionary in the
    /// older first-seen order refuses by name: an ordinal range over
    /// it would be a lie, and a per-document string walk is the thing
    /// this predicate exists not to do.
    fn resolve_string_predicate(
        &self,
        column: &str,
        key: &str,
        min: Option<(&str, bool)>,
        max: Option<(&str, bool)>,
        prefix: Option<&str>,
    ) -> Result<crate::filter::ResolvedLeaf, Status> {
        use crate::filter::ResolvedLeaf;
        let admits = |value: &str| -> bool {
            let v = value.as_bytes();
            let above = min.is_none_or(|(b, exclusive)| {
                if exclusive {
                    v > b.as_bytes()
                } else {
                    v >= b.as_bytes()
                }
            });
            let below = max.is_none_or(|(b, exclusive)| {
                if exclusive {
                    v < b.as_bytes()
                } else {
                    v <= b.as_bytes()
                }
            });
            above && below && prefix.is_none_or(|p| v.starts_with(p.as_bytes()))
        };
        // The contiguous ordinal range of a byte-sorted dictionary.
        let ord_range = |dict: &[String]| -> (u32, u32) {
            let lo = dict.partition_point(|v| {
                let v = v.as_bytes();
                let under_min = min.is_some_and(|(b, exclusive)| {
                    if exclusive {
                        v <= b.as_bytes()
                    } else {
                        v < b.as_bytes()
                    }
                });
                under_min || prefix.is_some_and(|p| v < p.as_bytes())
            });
            let hi = dict.partition_point(|v| {
                let v = v.as_bytes();
                let in_prefix =
                    prefix.is_none_or(|p| v < p.as_bytes() || v.starts_with(p.as_bytes()));
                let under_max = max.is_none_or(|(b, exclusive)| {
                    if exclusive {
                        v < b.as_bytes()
                    } else {
                        v <= b.as_bytes()
                    }
                });
                in_prefix && under_max
            });
            (lo as u32, hi.max(lo) as u32)
        };
        let unordered = |what: String| {
            Status::failed_precondition(format!(
                "{what} was written with a first-seen (unordered) dictionary; string ordering \
                 and prefixes need a dictionary in byte order, which this version writes at \
                 flush — rebuild or reshard the generation"
            ))
        };
        if key.is_empty() {
            let Some(fi) = self.facet_index(column) else {
                return Ok(ResolvedLeaf::FacetOrdRange {
                    column: None,
                    lo: 0,
                    hi: 0,
                });
            };
            return match self {
                Bm25Shard::Building(s) => {
                    if s.facet_dictionary_sorted(fi) {
                        let (lo, hi) = ord_range(s.facet_dictionary(fi));
                        Ok(ResolvedLeaf::FacetOrdRange {
                            column: Some(fi),
                            lo,
                            hi,
                        })
                    } else {
                        let ords = s
                            .facet_dictionary(fi)
                            .iter()
                            .enumerate()
                            .filter(|(_, v)| admits(v))
                            .map(|(ord, _)| ord as u32)
                            .collect();
                        Ok(ResolvedLeaf::Facet {
                            column: Some(fi),
                            ords,
                        })
                    }
                }
                Bm25Shard::Segmented(g) => {
                    if g.facet_dictionary_sorted(fi) {
                        let (lo, hi) = ord_range(g.facet_dictionary(fi));
                        Ok(ResolvedLeaf::FacetOrdRange {
                            column: Some(fi),
                            lo,
                            hi,
                        })
                    } else {
                        let ords = g
                            .facet_dictionary(fi)
                            .iter()
                            .enumerate()
                            .filter(|(_, v)| admits(v))
                            .map(|(ord, _)| ord as u32)
                            .collect();
                        Ok(ResolvedLeaf::Facet {
                            column: Some(fi),
                            ords,
                        })
                    }
                }
                Bm25Shard::Resident(r) => {
                    if !r.facet_dictionary_sorted(fi) {
                        return Err(unordered(format!("facet column {column:?}")));
                    }
                    let (lo, hi) = ord_range(r.facet_dictionary(fi));
                    Ok(ResolvedLeaf::FacetOrdRange {
                        column: Some(fi),
                        lo,
                        hi,
                    })
                }
                Bm25Shard::Spilling(_) => Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                )),
            };
        }
        let Some((ci, key_ord)) = self
            .map_facet_index(column)
            .and_then(|ci| self.map_facet_key_ord(ci, key).map(|k| (ci, k)))
        else {
            return Ok(ResolvedLeaf::MapFacetOrdRange {
                target: None,
                lo: 0,
                hi: 0,
            });
        };
        match self {
            Bm25Shard::Building(s) => {
                if s.map_facet_values_sorted(ci) {
                    let (lo, hi) = ord_range(s.map_facet_values(ci));
                    Ok(ResolvedLeaf::MapFacetOrdRange {
                        target: Some((ci, key_ord)),
                        lo,
                        hi,
                    })
                } else {
                    let ords = s
                        .map_facet_values(ci)
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| admits(v))
                        .map(|(ord, _)| ord as u32)
                        .collect();
                    Ok(ResolvedLeaf::MapFacet {
                        target: Some((ci, key_ord)),
                        ords,
                    })
                }
            }
            Bm25Shard::Segmented(g) => {
                if g.map_facet_values_sorted(ci) {
                    let (lo, hi) = ord_range(g.map_facet_values(ci));
                    Ok(ResolvedLeaf::MapFacetOrdRange {
                        target: Some((ci, key_ord)),
                        lo,
                        hi,
                    })
                } else {
                    let ords = g
                        .map_facet_values(ci)
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| admits(v))
                        .map(|(ord, _)| ord as u32)
                        .collect();
                    Ok(ResolvedLeaf::MapFacet {
                        target: Some((ci, key_ord)),
                        ords,
                    })
                }
            }
            Bm25Shard::Resident(r) => {
                if !r.map_facet_values_sorted(ci) {
                    return Err(unordered(format!("map-facet column {column:?} (values)")));
                }
                let (lo, hi) = ord_range(r.map_facet_values(ci));
                Ok(ResolvedLeaf::MapFacetOrdRange {
                    target: Some((ci, key_ord)),
                    lo,
                    hi,
                })
            }
            Bm25Shard::Spilling(_) => Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            )),
        }
    }

    /// Whether this shard can resolve each leaf of `expr`, positionally
    /// over [`crate::filter::walk_leaves`] order — the wire contract of
    /// `Bm25QueryResponse.filter_columns_known`. Computed regardless of
    /// `k`, like every other known flag: a typo must refuse even on a
    /// query that returns nothing. What "resolve" means per leaf kind
    /// is pinned on the FilterExpr proto messages.
    fn filter_columns_known(&self, expr: &crate::pb::FilterExpr) -> Vec<bool> {
        let mut known = Vec::new();
        crate::filter::walk_leaves(expr, &mut |leaf| {
            use crate::filter::LeafRef;
            known.push(match leaf {
                LeafRef::Facet(p) => self.facet_index(&p.column).is_some(),
                LeafRef::Number(p) => {
                    self.integer_index(&p.column).is_some()
                        || self.numeric_index(&p.column).is_some()
                }
                LeafRef::MapFacet(p) => self
                    .map_facet_index(&p.column)
                    .and_then(|ci| self.map_facet_key_ord(ci, &p.key))
                    .is_some(),
                LeafRef::MapNumber(p) => self
                    .map_numeric_index(&p.column)
                    .and_then(|ci| self.map_numeric_key_ord(ci, &p.key))
                    .is_some(),
                LeafRef::MapHasKey(p) => {
                    self.map_facet_index(&p.column).is_some()
                        || self.map_numeric_index(&p.column).is_some()
                }
                LeafRef::Has(p) => {
                    self.facet_index(&p.column).is_some()
                        || self.numeric_index(&p.column).is_some()
                        || self.integer_index(&p.column).is_some()
                        || self.geo_index(&p.column).is_some()
                }
                LeafRef::Geo(g) => self.geo_index(&g.column).is_some(),
                LeafRef::StringRange(p) => self.string_target_known(&p.column, &p.key),
                LeafRef::StringPrefix(p) => self.string_target_known(&p.column, &p.key),
            });
        });
        known
    }

    /// The known rule of a string range / prefix: the facet column, or
    /// the map-facet column and its key (the FacetPredicate /
    /// MapFacetPredicate rules, since the predicate reads the same
    /// dictionaries).
    fn string_target_known(&self, column: &str, key: &str) -> bool {
        if key.is_empty() {
            self.facet_index(column).is_some()
        } else {
            self.map_facet_index(column)
                .and_then(|ci| self.map_facet_key_ord(ci, key))
                .is_some()
        }
    }

    /// The index of the map-facet column named `name`.
    fn map_facet_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_index(name),
            Bm25Shard::Spilling(s) => s.map_facet_index(name),
            Bm25Shard::Resident(r) => r.map_facet_index(name),
            Bm25Shard::Segmented(g) => g.map_facet_index(name),
        }
    }

    /// The key ordinal of `key` in map-facet column `ci`.
    fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_key_ord(ci, key),
            Bm25Shard::Segmented(g) => g.map_facet_key_ord(ci, key),
        }
    }

    /// Number of distinct values map-facet column `ci` holds.
    fn map_facet_value_count(&self, ci: usize) -> usize {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_count(ci),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_count(ci),
            Bm25Shard::Segmented(g) => g.map_facet_value_count(ci),
        }
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value(ci, ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value(ci, ord),
            Bm25Shard::Segmented(g) => g.map_facet_value(ci, ord),
        }
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`.
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_ord(ci, key_ord, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_ord(ci, key_ord, doc_id),
            Bm25Shard::Segmented(g) => g.map_facet_value_ord(ci, key_ord, doc_id),
        }
    }

    /// The ordinal of `value` in map-facet column `ci`'s value
    /// dictionary, `None` when this shard never ingested it.
    fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_ord_of(ci, value),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_ord_of(ci, value),
            Bm25Shard::Segmented(g) => g.map_facet_value_ord_of(ci, value),
        }
    }

    /// The index of the map-numeric column named `name`.
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_index(name),
            Bm25Shard::Spilling(s) => s.map_numeric_index(name),
            Bm25Shard::Resident(r) => r.map_numeric_index(name),
            Bm25Shard::Segmented(g) => g.map_numeric_index(name),
        }
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_key_ord(ci, key),
            Bm25Shard::Segmented(g) => g.map_numeric_key_ord(ci, key),
        }
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`.
    fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_key_min_max(ci, key_ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_key_min_max(ci, key_ord),
            Bm25Shard::Segmented(g) => g.map_numeric_key_min_max(ci, key_ord),
        }
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`.
    fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_value(ci, key_ord, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_value(ci, key_ord, doc_id),
            Bm25Shard::Segmented(g) => g.map_numeric_value(ci, key_ord, doc_id),
        }
    }

    /// Field `f` as its own searchable [`Bm25Index`]; `None` while
    /// bulk-building, exactly like [`Self::as_index`].
    fn field_view(&self, f: usize) -> Option<Box<dyn Bm25Index + '_>> {
        match self {
            Bm25Shard::Building(s) => Some(Box::new(s.field(f))),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(Box::new(r.field(f))),
            Bm25Shard::Segmented(g) => Some(Box::new(g.field(f))),
        }
    }

    /// Count-then-rank facet counting
    /// (`docs/plans/track-1-features.md` section 2): count the
    /// requested facet fields over this shard's match set — every
    /// document holding at least one scored term in any queried field
    /// AND passing every filter — independent of `k`, `min_score`, and
    /// block-max pruning, which bound what is SURFACED, never what
    /// matched. A filter, by contrast, bounds what MATCHES: a
    /// filtered-out document is not in the result set, and counting it
    /// would misstate the drill-down. Walks each term's doc run to
    /// exhaustion (fixed-stride on a v5-shaped reader, never
    /// occurrence bytes), dedups documents in a slot bitmap, masks the
    /// bitmap with THE SAME [`crate::filter::DocFilter::passes`] the
    /// scorers gate the heap with, and resolves one facet ordinal per
    /// surviving document. A facet field the shard's table lacks
    /// answers `known: false` and no counts — the coordinator turns
    /// all-unknown into a refusal.
    ///
    /// All three facet kinds — plain, map-keyed, and range
    /// (`docs/range-facets.md`) — share the ONE bitmap this builds:
    /// the traversal is the expensive half, and asking for two kinds
    /// must not pay for it twice. Range edges are validated by
    /// [`validate_range_facet_fields`] before this runs.
    #[allow(clippy::too_many_arguments)]
    fn count_facets(
        &self,
        views: &[(&dyn Bm25Index, &[String])],
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        stats_fields: &[String],
        cardinality_fields: &[String],
        filter: crate::bm25::FilterCtx,
    ) -> (
        Vec<crate::pb::FacetFieldCounts>,
        Vec<crate::pb::RangeFacetCounts>,
        Vec<crate::pb::ColumnStats>,
        Vec<crate::pb::FacetDistinct>,
    ) {
        let n_slots = self.next_doc_id() as usize;
        let mut bits = vec![0u64; n_slots.div_ceil(64)];
        for &(view, terms) in views {
            for term in terms {
                view.for_each_doc_tf(term, &mut |doc_id, _tf| {
                    bits[doc_id as usize / 64] |= 1u64 << (doc_id % 64);
                });
            }
        }
        // One filter evaluation per matched document, exactly like the
        // ordinal resolution below — never per posting.
        if let Some((doc_filter, cols)) = filter {
            for (wi, bits_word) in bits.iter_mut().enumerate() {
                let mut w = *bits_word;
                while w != 0 {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    if !doc_filter.passes(doc, cols) {
                        *bits_word &= !(1u64 << (doc % 64));
                    }
                    w &= w - 1;
                }
            }
        }
        // One counting pass over the matched docs per requested field:
        // resolve an ordinal, bump a slot. Plain facets read the ords
        // column; map facets binary-search the doc's pair list for the
        // requested key (docs/map-columns.md).
        let count_by = |resolve: &dyn Fn(u32) -> Option<u32>, n_values: usize| -> Vec<u64> {
            let mut counts = vec![0u64; n_values];
            for (wi, &word) in bits.iter().enumerate() {
                let mut w = word;
                while w != 0 {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    if let Some(ord) = resolve(doc) {
                        counts[ord as usize] += 1;
                    }
                    w &= w - 1;
                }
            }
            counts
        };
        // Dictionary (ordinal) order — deterministic per shard; the
        // coordinator sorts the merged counts.
        let to_wire = |counts: Vec<u64>, value_of: &dyn Fn(u32) -> String| {
            counts
                .iter()
                .enumerate()
                .filter(|&(_, &c)| c > 0)
                .map(|(ord, &c)| crate::pb::FacetCount {
                    value: value_of(ord as u32),
                    count: c,
                })
                .collect()
        };
        let mut out: Vec<crate::pb::FacetFieldCounts> = facet_fields
            .iter()
            .map(|name| {
                let Some(fi) = self.facet_index(name) else {
                    return crate::pb::FacetFieldCounts {
                        field: name.clone(),
                        known: false,
                        counts: Vec::new(),
                        key: String::new(),
                    };
                };
                let counts = count_by(&|doc| self.facet_ord(fi, doc), self.facet_value_count(fi));
                crate::pb::FacetFieldCounts {
                    field: name.clone(),
                    known: true,
                    counts: to_wire(counts, &|ord| self.facet_value(fi, ord).to_string()),
                    key: String::new(),
                }
            })
            .collect();
        for req in map_facet_fields {
            // Known = the column exists AND its key dictionary has the
            // key: a key no shard ever ingested would otherwise make a
            // typo'd drill-down read as zero results everywhere.
            let resolved = self
                .map_facet_index(&req.column)
                .and_then(|ci| self.map_facet_key_ord(ci, &req.key).map(|k| (ci, k)));
            let Some((ci, key_ord)) = resolved else {
                out.push(crate::pb::FacetFieldCounts {
                    field: req.column.clone(),
                    known: false,
                    counts: Vec::new(),
                    key: req.key.clone(),
                });
                continue;
            };
            let counts = count_by(
                &|doc| self.map_facet_value_ord(ci, key_ord, doc),
                self.map_facet_value_count(ci),
            );
            out.push(crate::pb::FacetFieldCounts {
                field: req.column.clone(),
                known: true,
                counts: to_wire(counts, &|ord| self.map_facet_value(ci, ord).to_string()),
                key: req.key.clone(),
            });
        }
        // Range facets: one value read per matched document, then a
        // binary search of the (validated) edge list. Buckets are
        // half-open [edges[i], edges[i+1]), so a value sitting exactly
        // on an interior edge lands in the upper bucket; a value below
        // the first edge or at/above the last is counted in NO bucket.
        // There are deliberately no implicit tail buckets — the caller
        // asked for these intervals and gets these intervals.
        let bucket_counts = |value_of: &dyn Fn(u32) -> Option<f64>, edges: &[f64]| -> Vec<u64> {
            let mut counts = vec![0u64; edges.len() - 1];
            for (wi, &word) in bits.iter().enumerate() {
                let mut w = word;
                while w != 0 {
                    let doc = (wi * 64) as u32 + w.trailing_zeros();
                    if let Some(v) = value_of(doc) {
                        let upper = edges.partition_point(|&e| e <= v);
                        if upper > 0 && upper < edges.len() {
                            counts[upper - 1] += 1;
                        }
                    }
                    w &= w - 1;
                }
            }
            counts
        };
        let ranges = range_facet_fields
            .iter()
            .map(|req| {
                let source = self.resolve_range_column(&req.column, &req.key);
                let Some(source) = source else {
                    return crate::pb::RangeFacetCounts {
                        column: req.column.clone(),
                        key: req.key.clone(),
                        known: false,
                        buckets: Vec::new(),
                    };
                };
                let counts = bucket_counts(&|doc| self.range_value(source, doc), &req.edges);
                crate::pb::RangeFacetCounts {
                    column: req.column.clone(),
                    key: req.key.clone(),
                    known: true,
                    buckets: counts
                        .into_iter()
                        .enumerate()
                        .map(|(i, count)| crate::pb::RangeBucket {
                            from: req.edges[i],
                            to: req.edges[i + 1],
                            count,
                        })
                        .collect(),
                }
            })
            .collect();
        // Column stats: one value read per matched document per field,
        // over the same bitmap. Absence contributes nothing — `count`
        // is the number of documents that HELD a value, which is what
        // makes mean = sum / count honest.
        let stats = stats_fields
            .iter()
            .map(|name| {
                let value_of: Box<dyn Fn(u32) -> Option<f64>> =
                    if let Some(ni) = self.numeric_index(name) {
                        Box::new(move |doc| self.numeric_value(ni, doc))
                    } else if let Some(ii) = self.integer_index(name) {
                        Box::new(move |doc| self.integer_value(ii, doc).map(|v| v as f64))
                    } else {
                        return crate::pb::ColumnStats {
                            field: name.clone(),
                            known: false,
                            ..Default::default()
                        };
                    };
                let mut out = crate::pb::ColumnStats {
                    field: name.clone(),
                    known: true,
                    min: f64::INFINITY,
                    max: f64::NEG_INFINITY,
                    ..Default::default()
                };
                for (wi, &word) in bits.iter().enumerate() {
                    let mut w = word;
                    while w != 0 {
                        let doc = (wi * 64) as u32 + w.trailing_zeros();
                        if let Some(v) = value_of(doc) {
                            out.count += 1;
                            out.sum += v;
                            out.min = out.min.min(v);
                            out.max = out.max.max(v);
                        }
                        w &= w - 1;
                    }
                }
                if out.count == 0 {
                    out.min = 0.0;
                    out.max = 0.0;
                }
                out
            })
            .collect();
        // Distinct facet values in the match set: an ordinal bitset
        // over this shard's dictionary, then the VALUES — ordinals are
        // shard-local, so strings are the only union-able currency the
        // coordinator can merge.
        let distinct = cardinality_fields
            .iter()
            .map(|name| {
                let Some(fi) = self.facet_index(name) else {
                    return crate::pb::FacetDistinct {
                        field: name.clone(),
                        known: false,
                        values: Vec::new(),
                    };
                };
                let n_values = self.facet_value_count(fi);
                let mut present = vec![0u64; n_values.div_ceil(64)];
                for (wi, &word) in bits.iter().enumerate() {
                    let mut w = word;
                    while w != 0 {
                        let doc = (wi * 64) as u32 + w.trailing_zeros();
                        if let Some(ord) = self.facet_ord(fi, doc) {
                            present[ord as usize / 64] |= 1u64 << (ord % 64);
                        }
                        w &= w - 1;
                    }
                }
                let values = (0..n_values as u32)
                    .filter(|&ord| present[ord as usize / 64] >> (ord % 64) & 1 == 1)
                    .map(|ord| self.facet_value(fi, ord).to_string())
                    .collect();
                crate::pb::FacetDistinct {
                    field: name.clone(),
                    known: true,
                    values,
                }
            })
            .collect();
        (out, ranges, stats, distinct)
    }

    /// Resolve a range facet's column against THIS shard's tables:
    /// with no key the f64 table then the i64 table (one name space
    /// across kinds, so at most one answers), with a key the
    /// map-numeric column and its key ordinal. `None` = this shard
    /// cannot resolve it, which answers `known: false`.
    fn resolve_range_column(&self, column: &str, key: &str) -> Option<RangeSource> {
        if key.is_empty() {
            if let Some(ni) = self.numeric_index(column) {
                return Some(RangeSource::Numeric(ni));
            }
            return self.integer_index(column).map(RangeSource::Integer);
        }
        let ci = self.map_numeric_index(column)?;
        let key_ord = self.map_numeric_key_ord(ci, key)?;
        Some(RangeSource::MapKey {
            column: ci,
            key_ord,
        })
    }

    /// A document's value for a resolved range source, on the bucket
    /// scale. i64 values convert with `as f64`: bucketing is a
    /// comparison against f64 edges, so the edges are the precision
    /// limit either way, and the cast is monotone so ordering (which is
    /// all a bucket test uses) is preserved.
    fn range_value(&self, source: RangeSource, doc_id: u32) -> Option<f64> {
        match source {
            RangeSource::Numeric(ni) => self.numeric_value(ni, doc_id),
            RangeSource::Integer(ii) => self.integer_value(ii, doc_id).map(|v| v as f64),
            RangeSource::MapKey { column, key_ord } => {
                self.map_numeric_value(column, key_ord, doc_id)
            }
        }
    }

    /// Resolve parsed score stages against THIS shard's numeric table
    /// (`docs/score-functions.md`): the column index when the table
    /// has it (`None` = every document absent = identity, which is
    /// exact) and the column's min/max bound metadata. Only called on
    /// searchable shapes.
    fn resolve_chain(
        &self,
        specs: &[(crate::scorefn::StageOp, String, String)],
    ) -> crate::scorefn::ScoreChain {
        use crate::scorefn::ColumnRef;
        crate::scorefn::ScoreChain {
            stages: specs
                .iter()
                .map(|(op, column, key)| {
                    // A geo stage reads a geo-point column and nothing
                    // else: its op carries the origin, and its bound is
                    // identity, so there is no min/max to lift.
                    if matches!(op, crate::scorefn::StageOp::MultGeoDecay { .. }) {
                        return crate::scorefn::Stage {
                            op: *op,
                            column: self.geo_index(column).map(ColumnRef::Geo),
                            min_max: (f64::NAN, f64::NAN),
                        };
                    }
                    let (column, min_max) = if key.is_empty() {
                        // f64 table first, then i64: one name space
                        // across kinds (config refuses collisions), so
                        // at most one of the two ever answers.
                        match self.numeric_index(column) {
                            Some(ni) => {
                                (Some(ColumnRef::Numeric(ni)), Some(self.numeric_min_max(ni)))
                            }
                            None => {
                                let ii = self.integer_index(column);
                                (
                                    ii.map(ColumnRef::Integer),
                                    ii.map(|ii| int_min_max_as_f64(self.integer_min_max(ii))),
                                )
                            }
                        }
                    } else {
                        // Map stage: both the column and the key must
                        // resolve; bounds lift from the KEY's min/max.
                        let hit = self
                            .map_numeric_index(column)
                            .and_then(|ci| self.map_numeric_key_ord(ci, key).map(|k| (ci, k)));
                        (
                            hit.map(|(ci, key_ord)| ColumnRef::MapKey {
                                column: ci,
                                key_ord,
                            }),
                            hit.map(|(ci, key_ord)| self.map_numeric_key_min_max(ci, key_ord)),
                        )
                    };
                    crate::scorefn::Stage {
                        op: *op,
                        column,
                        min_max: min_max.unwrap_or((f64::NAN, f64::NAN)),
                    }
                })
                .collect(),
        }
    }

    /// Open a `.bm25` path in the right shape: every reader-supported
    /// format (v3 through v8) maps disk-resident; only the pre-v3
    /// formats load into the heap builder (and are upgraded to the
    /// current format on the next flush). v5/v6 were missing from this
    /// list once, and v8 repeated the mistake in review, so a
    /// restarted node heap-loaded its whole postings file — at real
    /// shard sizes that is the exact failure the resident reader
    /// exists to prevent. When a format version is added, it goes
    /// HERE too.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let mut magic = [0u8; 8];
        std::fs::File::open(path)?.read_exact(&mut magic)?;
        if matches!(
            &magic,
            b"TVBM2503" | b"TVBM2504" | b"TVBM2505" | b"TVBM2506" | b"TVBM2507" | b"TVBM2508"
        ) {
            Ok(Bm25Shard::Resident(Bm25Reader::open(path)?))
        } else {
            Ok(Bm25Shard::Building(Bm25Store::load(path)?))
        }
    }
}

/// A range facet's resolved column on THIS shard. The three shapes a
/// bucketable value can live in; see [`Bm25Shard::resolve_range_column`].
#[derive(Debug, Clone, Copy)]
enum RangeSource {
    /// Index into the shard's f64 table.
    Numeric(usize),
    /// Index into the shard's i64 table.
    Integer(usize),
    /// A map-numeric column and a key ordinal in ITS key dictionary.
    MapKey {
        /// Index into the shard's map-numeric table.
        column: usize,
        /// Key ordinal within that column's key dictionary.
        key_ord: u32,
    },
}

/// Validate a request's range-facet edge lists (`docs/range-facets.md`).
/// Edges must be at least two finite values in strictly ascending
/// order: fewer than two describes no interval at all, a non-finite
/// edge makes the bucket test meaningless, and an unsorted list would
/// silently answer for intervals nobody asked for. Every refusal names
/// the column and the knob, like every other column refusal here.
/// Purely local — no shard state involved — so the coordinator ALSO
/// runs it before its zero-term/k=0 early return: a malformed edge
/// list must refuse even when there is no match set to count.
pub(crate) fn validate_range_facet_fields(
    fields: &[crate::pb::RangeFacetField],
) -> Result<(), Status> {
    for req in fields {
        if req.column.is_empty() {
            return Err(Status::invalid_argument(
                "range facet: a request names the column it buckets",
            ));
        }
        let named = if req.key.is_empty() {
            format!("{:?}", req.column)
        } else {
            format!("{:?}[{:?}]", req.column, req.key)
        };
        if req.edges.len() < 2 {
            return Err(Status::invalid_argument(format!(
                "range facet {named}: edges must hold at least 2 values (one bucket); \
                 got {}",
                req.edges.len()
            )));
        }
        for (i, e) in req.edges.iter().enumerate() {
            if !e.is_finite() {
                return Err(Status::invalid_argument(format!(
                    "range facet {named}: edge {i} is not finite"
                )));
            }
            if i > 0 && *e <= req.edges[i - 1] {
                return Err(Status::invalid_argument(format!(
                    "range facet {named}: edges must be strictly ascending (edge {i} is \
                     not above edge {})",
                    i - 1
                )));
            }
        }
    }
    Ok(())
}

/// Refuse a (lat, lon) pair that is not a finite degree pair on the
/// globe, naming `what`. Shared by ingest, geo filters, and geo score
/// stages: a coordinate that cannot exist is a producer bug wherever it
/// arrives, and the engine says so rather than clamping it onto the
/// nearest pole.
fn validate_lat_lon(what: &str, lat: f64, lon: f64) -> Result<(), Status> {
    if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
        return Err(Status::invalid_argument(format!(
            "{what}: latitude {lat} is not a finite degree in [-90, 90]"
        )));
    }
    if !lon.is_finite() || !(-180.0..=180.0).contains(&lon) {
        return Err(Status::invalid_argument(format!(
            "{what}: longitude {lon} is not a finite degree in [-180, 180]"
        )));
    }
    Ok(())
}

/// Validate a request's geo filters (`docs/geo-columns.md`) and resolve
/// each one's region. Purely local — no shard state involved — so the
/// coordinator ALSO runs it before its zero-term/k=0 early return, on
/// the [`validate_range_facet_fields`] pattern: a malformed filter must
/// refuse even when there is no match set to filter.
///
/// What it refuses, each by name and with the knob:
///
/// - an empty column name, or an unset `region` (a filter that filters
///   nothing is not a filter),
/// - coordinates that are not finite degrees on the globe,
/// - `min_lat > max_lat`,
/// - `min_lon > max_lon` — an ANTIMERIDIAN-CROSSING box. This
///   increment refuses it rather than reinterpreting the pair as a
///   wraparound: the two readings differ by the whole planet, and only
///   the caller knows which was meant. Wraparound is future work with
///   its own decision to make, not a default to guess.
/// - a radius that is not a finite, strictly positive number of meters,
///   or one whose metric is unspecified.
pub(crate) fn validate_geo_filters(
    filters: &[crate::pb::GeoFilter],
) -> Result<Vec<crate::geo::GeoRegion>, Status> {
    filters.iter().map(validate_geo_filter).collect()
}

/// [`validate_geo_filters`] for one filter — also the validation a geo
/// LEAF of a compiled filter tree gets (`docs/cel-filters.md`), so the
/// two ways of sending a region can never diverge on what a legal
/// region is.
pub(crate) fn validate_geo_filter(
    f: &crate::pb::GeoFilter,
) -> Result<crate::geo::GeoRegion, Status> {
    if f.column.is_empty() {
        return Err(Status::invalid_argument(
            "geo filter: a filter names the geo column it reads",
        ));
    }
    let named = format!("geo filter {:?}", f.column);
    match f.region.as_ref() {
        Some(crate::pb::geo_filter::Region::Bbox(b)) => {
            validate_lat_lon(&format!("{named} bbox min"), b.min_lat, b.min_lon)?;
            validate_lat_lon(&format!("{named} bbox max"), b.max_lat, b.max_lon)?;
            if b.min_lat > b.max_lat {
                return Err(Status::invalid_argument(format!(
                    "{named}: min_lat {} is above max_lat {}",
                    b.min_lat, b.max_lat
                )));
            }
            if b.min_lon > b.max_lon {
                return Err(Status::invalid_argument(format!(
                    "{named}: min_lon {} is east of max_lon {}, which would describe \
                             an antimeridian-crossing box. This increment REFUSES wraparound \
                             boxes rather than guessing: send two boxes, one each side of \
                             180 degrees, and union the results yourself.",
                    b.min_lon, b.max_lon
                )));
            }
            Ok(crate::geo::GeoRegion::Bbox {
                min_lat: b.min_lat,
                max_lat: b.max_lat,
                min_lon: b.min_lon,
                max_lon: b.max_lon,
            })
        }
        Some(crate::pb::geo_filter::Region::Radius(r)) => {
            validate_lat_lon(&format!("{named} radius origin"), r.lat, r.lon)?;
            if !r.meters.is_finite() || r.meters <= 0.0 {
                return Err(Status::invalid_argument(format!(
                    "{named}: radius meters {} must be finite and above zero",
                    r.meters
                )));
            }
            let metric = match crate::pb::GeoMetric::try_from(r.metric) {
                Ok(crate::pb::GeoMetric::Haversine) => crate::geo::GeoMetric::Haversine,
                Ok(crate::pb::GeoMetric::Manhattan) => crate::geo::GeoMetric::Manhattan,
                Ok(crate::pb::GeoMetric::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "{named}: unknown geo metric {}",
                        r.metric
                    )));
                }
            };
            Ok(crate::geo::GeoRegion::Radius {
                lat: r.lat,
                lon: r.lon,
                meters: r.meters,
                metric,
            })
        }
        None => Err(Status::invalid_argument(format!(
            "{named}: no region set; a filter must name a bbox or a radius"
        ))),
    }
}

/// Convert a `google.protobuf.Timestamp` to the epoch MICROSECONDS an
/// i64 column stores (`docs/range-facets.md`). Timestamps are pure
/// ingest sugar: the node does this once, the column holds plain i64,
/// and replay from the WAL redoes it from the same instant.
///
/// The remainder below a microsecond is not representable and is
/// dropped; `nanos` is non-negative in a valid Timestamp, so the drop
/// always floors toward negative infinity and the unit contract holds
/// on both sides of the epoch. Everything that could make the stored
/// value a lie instead refuses: `nanos` outside its declared range,
/// an overflowing conversion, and a result that would collide with the
/// column's absence sentinel.
pub(crate) fn timestamp_to_epoch_micros(
    field: &str,
    ts: &prost_types::Timestamp,
) -> Result<i64, Status> {
    if !(0..1_000_000_000).contains(&ts.nanos) {
        return Err(Status::invalid_argument(format!(
            "timestamp field {field:?}: nanos {} is outside [0, 1e9) — not a valid \
             google.protobuf.Timestamp",
            ts.nanos
        )));
    }
    let micros = ts
        .seconds
        .checked_mul(1_000_000)
        .and_then(|s| s.checked_add(i64::from(ts.nanos) / 1_000))
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "timestamp field {field:?}: {} seconds does not fit i64 epoch micros",
                ts.seconds
            ))
        })?;
    if micros == crate::postings::INTEGER_ABSENT {
        return Err(Status::invalid_argument(format!(
            "timestamp field {field:?}: this instant converts to i64::MIN epoch micros, \
             which is the column's absence sentinel"
        )));
    }
    Ok(micros)
}

/// Every requested range facet answered unknown with no buckets — what
/// a shard with no lexical half (and therefore no columns) reports.
fn unknown_range_counts(fields: &[crate::pb::RangeFacetField]) -> Vec<crate::pb::RangeFacetCounts> {
    fields
        .iter()
        .map(|req| crate::pb::RangeFacetCounts {
            column: req.column.clone(),
            key: req.key.clone(),
            known: false,
            buckets: Vec::new(),
        })
        .collect()
}

/// [`crate::scorefn::NumericRead`] over a searchable shard shape, for
/// score-chain evaluation during scoring.
struct ShardNumericRead<'a>(&'a Bm25Shard);

/// The value-expression resolution surface (`docs/cel-values.md`):
/// the same name-to-index lookups filters use, packaged as the trait
/// `values::resolve` consumes.
impl crate::values::ColumnLookup for Bm25Shard {
    fn numeric_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::numeric_index(self, name)
    }
    fn integer_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::integer_index(self, name)
    }
    fn facet_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::facet_index(self, name)
    }
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::map_numeric_index(self, name)
    }
    fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        Bm25Shard::map_numeric_key_ord(self, ci, key)
    }
    fn map_facet_index(&self, name: &str) -> Option<usize> {
        Bm25Shard::map_facet_index(self, name)
    }
    fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        Bm25Shard::map_facet_key_ord(self, ci, key)
    }
    fn facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        Bm25Shard::facet_value_ord_of(self, ci, value)
    }
    fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        Bm25Shard::map_facet_value_ord_of(self, ci, value)
    }
}

impl crate::scorefn::NumericRead for ShardNumericRead<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key_ord, doc_id)
    }
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.0.integer_value(ii, doc_id)
    }
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.0.geo_value(gi, doc_id)
    }
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc_id)
    }
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.0.map_facet_value_ord(ci, key_ord, doc_id)
    }
}

/// Resolve one request's filters against one shard: the predicate the
/// lexical heap gate uses, and the slot allowlist the vector scan uses
/// (`docs/vector-filters.md`). Both come from the SAME resolved
/// [`crate::filter::DocFilter`], which is the point — a fused result
/// cannot mix a filtered half with an unfiltered one when there is one
/// resolution and one truth.
///
/// The rules are the lexical leg's rules, unchanged: absence fails, a
/// name this shard lacks resolves to absent for every document, and the
/// coordinator refuses a name NO shard resolves. A shard with no
/// lexical half therefore has no columns and admits nothing — exact,
/// since its documents genuinely hold no value — while a shard still
/// bulk-building refuses, because "no columns yet" is a transient state
/// and answering it as an empty result would be a silent lie.
///
/// `None` allowlist means the request carried no filters, which every
/// scan path reads as "scan everything". That is deliberately not the
/// same as an all-true allowlist: an unfiltered scan batches with every
/// other unfiltered scan and takes a path bit-identical to the one it
/// took before filters existed.
/// Render one evaluated projection value onto the wire. Absence is the
/// unset oneof; string ordinals render against the dictionary the
/// shard owns.
fn projected_value(
    val: Option<crate::values::Val>,
    store: &Bm25Shard,
) -> crate::pb::ProjectedValue {
    use crate::pb::projected_value::Value as W;
    let value = match val {
        None => None,
        Some(crate::values::Val::Int(v)) => Some(W::IntValue(v)),
        Some(crate::values::Val::Double(v)) => Some(W::DoubleValue(v)),
        Some(crate::values::Val::FacetOrd { column, ord }) => {
            Some(W::StringValue(store.facet_value(column, ord).to_string()))
        }
        Some(crate::values::Val::MapFacetOrd { column, ord }) => Some(W::StringValue(
            store.map_facet_value(column, ord).to_string(),
        )),
        Some(crate::values::Val::Bool(v)) => Some(W::BoolValue(v)),
    };
    crate::pb::ProjectedValue { value }
}

/// Decode one wire aggregate op.
pub(crate) fn agg_op_of(op: i32) -> Result<crate::pb::AggregateOp, Status> {
    match crate::pb::AggregateOp::try_from(op) {
        Ok(crate::pb::AggregateOp::Unspecified) | Err(_) => Err(Status::invalid_argument(
            "aggregation with an unspecified operation",
        )),
        Ok(op) => Ok(op),
    }
}

/// One shard's exact accumulator for one aggregation, typed at
/// resolution. Every statistic for the type folds in the single pass
/// (they are a handful of flops each); the response carries them all
/// and the coordinator reads the ones the op needs.
enum AggAcc {
    /// The expression can never hold a value on this shard.
    Absent,
    Int {
        present: u64,
        sum: i128,
        min: i64,
        max: i64,
        /// CARDINALITY: the distinct values seen.
        distinct: Option<std::collections::HashSet<i64>>,
    },
    Double {
        present: u64,
        /// Neumaier-compensated running sum.
        sum: f64,
        compensation: f64,
        /// NaN-propagating extrema, the math.least/greatest rule.
        min: f64,
        max: f64,
        /// Welford.
        mean: f64,
        m2: f64,
        /// CARDINALITY: the distinct canonical bit patterns seen.
        distinct: Option<std::collections::HashSet<u64>>,
    },
    Str {
        present: u64,
        /// CARDINALITY: the distinct (map, column, ordinal) reads seen;
        /// rendered against the dictionary on the way out.
        distinct: Option<std::collections::HashSet<(bool, usize, u32)>>,
    },
    /// CARDINALITY over a boolean expression: which of the values
    /// occurred.
    Bool {
        present: u64,
        seen_true: bool,
        seen_false: bool,
    },
}

/// One bit pattern per double value for the distinct count: a single
/// NaN and a single zero, so `-0.0` and `0.0` are one value and every
/// NaN payload is one value. Everything else is its own bits.
fn canonical_double_bits(x: f64) -> u64 {
    if x.is_nan() {
        f64::NAN.to_bits()
    } else if x == 0.0 {
        0
    } else {
        x.to_bits()
    }
}

impl AggAcc {
    fn push(&mut self, v: crate::values::Val) {
        match (self, v) {
            (AggAcc::Absent, _) => unreachable!("absent expressions never evaluate"),
            (
                AggAcc::Int {
                    present,
                    sum,
                    min,
                    max,
                    distinct,
                },
                crate::values::Val::Int(x),
            ) => {
                if *present == 0 {
                    *min = x;
                    *max = x;
                } else {
                    *min = (*min).min(x);
                    *max = (*max).max(x);
                }
                *present += 1;
                *sum += i128::from(x);
                if let Some(set) = distinct {
                    set.insert(x);
                }
            }
            (
                AggAcc::Double {
                    present,
                    sum,
                    compensation,
                    min,
                    max,
                    mean,
                    m2,
                    distinct,
                },
                crate::values::Val::Double(x),
            ) => {
                if let Some(set) = distinct {
                    set.insert(canonical_double_bits(x));
                }
                if *present == 0 {
                    *min = x;
                    *max = x;
                } else if x.is_nan() || min.is_nan() {
                    *min = f64::NAN;
                    *max = f64::NAN;
                } else {
                    *min = (*min).min(x);
                    *max = (*max).max(x);
                }
                *present += 1;
                // Neumaier: the compensation catches whichever addend
                // the naive add truncated.
                let t = *sum + x;
                *compensation += if sum.abs() >= x.abs() {
                    (*sum - t) + x
                } else {
                    (x - t) + *sum
                };
                *sum = t;
                // Welford, in doc order.
                let n = *present as f64;
                let delta = x - *mean;
                *mean += delta / n;
                *m2 += delta * (x - *mean);
            }
            (AggAcc::Str { present, distinct }, crate::values::Val::FacetOrd { column, ord }) => {
                *present += 1;
                if let Some(set) = distinct {
                    set.insert((false, column, ord));
                }
            }
            (
                AggAcc::Str { present, distinct },
                crate::values::Val::MapFacetOrd { column, ord },
            ) => {
                *present += 1;
                if let Some(set) = distinct {
                    set.insert((true, column, ord));
                }
            }
            (
                AggAcc::Bool {
                    present,
                    seen_true,
                    seen_false,
                },
                crate::values::Val::Bool(b),
            ) => {
                *present += 1;
                if b {
                    *seen_true = true;
                } else {
                    *seen_false = true;
                }
            }
            _ => unreachable!("resolution typed the accumulator"),
        }
    }

    /// This shard's distinct-value count, when CARDINALITY asked for
    /// one.
    fn distinct_len(&self) -> Option<usize> {
        match self {
            AggAcc::Absent => None,
            AggAcc::Int { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Double { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Str { distinct, .. } => distinct.as_ref().map(|s| s.len()),
            AggAcc::Bool {
                seen_true,
                seen_false,
                ..
            } => Some(usize::from(*seen_true) + usize::from(*seen_false)),
        }
    }

    /// The wire partial. `store` renders string ordinals; only a
    /// document-less shard, whose accumulators are all absent, has
    /// none to offer.
    fn partial(&self, store: Option<&Bm25Shard>) -> crate::pb::AggregatePartial {
        use crate::pb::AggregateValueType as T;
        let mut p = crate::pb::AggregatePartial {
            vtype: T::Absent as i32,
            ..Default::default()
        };
        match self {
            AggAcc::Absent => {}
            AggAcc::Int {
                present,
                sum,
                min,
                max,
                distinct,
            } => {
                p.vtype = T::Int as i32;
                p.present = *present;
                p.int_sum_hi = (*sum >> 64) as i64;
                p.int_sum_lo = *sum as u64;
                p.int_min = *min;
                p.int_max = *max;
                if let Some(set) = distinct {
                    let mut values: Vec<i64> = set.iter().copied().collect();
                    values.sort_unstable();
                    p.distinct_ints = values;
                }
            }
            AggAcc::Double {
                present,
                sum,
                compensation,
                min,
                max,
                mean,
                m2,
                distinct,
            } => {
                p.vtype = T::Double as i32;
                p.present = *present;
                p.double_sum = *sum;
                p.double_compensation = *compensation;
                p.double_min = *min;
                p.double_max = *max;
                p.mean = *mean;
                p.m2 = *m2;
                if let Some(set) = distinct {
                    let mut values: Vec<u64> = set.iter().copied().collect();
                    values.sort_unstable();
                    p.distinct_double_bits = values;
                }
            }
            AggAcc::Str { present, distinct } => {
                p.vtype = T::String as i32;
                p.present = *present;
                if let (Some(set), Some(store)) = (distinct, store) {
                    let mut values: Vec<String> = set
                        .iter()
                        .map(|&(map, column, ord)| {
                            if map {
                                store.map_facet_value(column, ord).to_string()
                            } else {
                                store.facet_value(column, ord).to_string()
                            }
                        })
                        .collect();
                    values.sort_unstable();
                    values.dedup();
                    p.distinct_strings = values;
                }
            }
            AggAcc::Bool {
                present,
                seen_true,
                seen_false,
            } => {
                p.vtype = T::Bool as i32;
                p.present = *present;
                if *seen_false {
                    p.distinct_bools.push(false);
                }
                if *seen_true {
                    p.distinct_bools.push(true);
                }
            }
        }
        p
    }
}

/// A fresh accumulator for one resolved type; `cardinality` adds the
/// distinct set the op counts.
fn acc_of(vt: crate::values::ValueType, cardinality: bool) -> AggAcc {
    match vt {
        crate::values::ValueType::Unknown => AggAcc::Absent,
        crate::values::ValueType::Int => AggAcc::Int {
            present: 0,
            sum: 0,
            min: 0,
            max: 0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Double => AggAcc::Double {
            present: 0,
            sum: 0.0,
            compensation: 0.0,
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            m2: 0.0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Str => AggAcc::Str {
            present: 0,
            distinct: cardinality.then(Default::default),
        },
        crate::values::ValueType::Bool => {
            debug_assert!(
                cardinality,
                "check_agg_type admits booleans under CARDINALITY only"
            );
            AggAcc::Bool {
                present: 0,
                seen_true: false,
                seen_false: false,
            }
        }
    }
}

/// How one histogram buckets: a fixed width over doubles, or a
/// calendar unit over epoch-micros ints (docs/aggregations.md).
#[derive(Clone, Copy)]
enum Bucketing {
    Fixed(f64),
    Calendar {
        unit: crate::pb::CalendarInterval,
        utc_offset_minutes: i32,
    },
}

/// One shard's sparse histogram fold.
#[derive(Default)]
struct HistAcc {
    buckets: std::collections::HashMap<i64, u64>,
    present: u64,
    unbucketable: u64,
}

impl HistAcc {
    /// Fold one present value. `Err` = this shard alone exceeds the
    /// bucket cap.
    fn push(&mut self, x: f64, interval: f64, cap: usize, name: &str) -> Result<(), Status> {
        self.present += 1;
        let idx_f = (x / interval).floor();
        // Any integral float in [-2^63, 2^63) casts exactly; NaN,
        // infinities, and indexes outside i64 have no honest bucket.
        let lo = -(2f64.powi(63));
        let hi = 2f64.powi(63);
        if !idx_f.is_finite() || idx_f < lo || idx_f >= hi {
            self.unbucketable += 1;
            return Ok(());
        }
        self.count(idx_f as i64, cap, name)
    }

    /// Fold one present epoch-micros value into its calendar bucket,
    /// keyed by the bucket's start instant. `Err` = this shard alone
    /// exceeds the bucket cap.
    fn push_calendar(
        &mut self,
        micros: i64,
        unit: crate::pb::CalendarInterval,
        utc_offset_minutes: i32,
        cap: usize,
        name: &str,
    ) -> Result<(), Status> {
        self.present += 1;
        let Some(start) = crate::calendar::bucket_start(micros, unit, utc_offset_minutes) else {
            self.unbucketable += 1;
            return Ok(());
        };
        self.count(start, cap, name)
    }

    fn count(&mut self, key: i64, cap: usize, name: &str) -> Result<(), Status> {
        let n = self.buckets.len();
        let slot = self.buckets.entry(key).or_insert(0);
        *slot += 1;
        if *slot == 1 && n == cap {
            return Err(Status::failed_precondition(format!(
                "histogram {name:?} exceeds {cap} buckets on one shard; use a coarser \
                 interval or a tighter filter"
            )));
        }
        Ok(())
    }

    fn response(&self) -> crate::pb::ShardHistogram {
        let mut rows: Vec<(i64, u64)> = self.buckets.iter().map(|(k, v)| (*k, *v)).collect();
        rows.sort_unstable();
        crate::pb::ShardHistogram {
            bucket_index: rows.iter().map(|r| r.0).collect(),
            bucket_count: rows.iter().map(|r| r.1).collect(),
            present: self.present,
            unbucketable: self.unbucketable,
        }
    }
}

/// One shard's phase-1 percentile fold.
#[derive(Default)]
struct PctAcc {
    present: u64,
    unrankable: u64,
    min_bits: u64,
    max_bits: u64,
}

impl PctAcc {
    fn push(&mut self, bits: Option<u64>) {
        let Some(bits) = bits else {
            self.unrankable += 1;
            return;
        };
        if self.present == 0 {
            self.min_bits = bits;
            self.max_bits = bits;
        } else {
            self.min_bits = self.min_bits.min(bits);
            self.max_bits = self.max_bits.max(bits);
        }
        self.present += 1;
    }
}

/// Resolve one percentile expression: only int and double rank.
/// `Ok(None)` = the expression can never hold a value on this shard.
fn resolve_rankable(
    store: &Bm25Shard,
    expr: Option<&crate::pb::ValueExpr>,
    name: &str,
    what: &str,
) -> Result<Option<(crate::values::ResolvedValue, bool)>, Status> {
    let expr = expr.ok_or_else(|| {
        Status::invalid_argument(format!("{what} {name:?} without an expression"))
    })?;
    let (rv, vt) = crate::values::resolve(expr, store)
        .map_err(|e| Status::invalid_argument(format!("{what} {name:?}: {}", e.message())))?;
    match vt {
        crate::values::ValueType::Int => Ok(Some((rv, true))),
        crate::values::ValueType::Double => Ok(Some((rv, false))),
        crate::values::ValueType::Unknown => Ok(None),
        crate::values::ValueType::Str => Err(Status::invalid_argument(format!(
            "{what} {name:?} ranks numbers; a string does not order here"
        ))),
        crate::values::ValueType::Bool => Err(Status::invalid_argument(format!(
            "{what} {name:?} ranks numbers, not booleans"
        ))),
    }
}

/// Order bits of one evaluated rankable value; `None` = computed NaN
/// (present but unrankable).
fn rankable_bits(v: crate::values::Val, int_typed: bool) -> Option<u64> {
    match v {
        crate::values::Val::Int(x) => {
            debug_assert!(int_typed);
            let _ = int_typed;
            Some(i64_order_bits(x))
        }
        crate::values::Val::Double(x) => {
            if x.is_nan() {
                None
            } else {
                Some(f64_order_bits(x))
            }
        }
        _ => unreachable!("resolution typed the expression numeric"),
    }
}

/// The op-versus-type admission rule, shared wording with the
/// coordinator's literal-pinned precheck.
fn check_agg_type(
    name: &str,
    op: crate::pb::AggregateOp,
    vt: crate::values::ValueType,
) -> Result<(), Status> {
    use crate::pb::AggregateOp as O;
    use crate::values::ValueType as V;
    if op == O::Cardinality {
        // Distinct values exist for every type, booleans included.
        return Ok(());
    }
    match vt {
        V::Bool => Err(Status::invalid_argument(format!(
            "aggregation {name:?}: a boolean aggregates nowhere; filter on the \
             expression instead and read `matched`"
        ))),
        V::Str if op != O::Count => Err(Status::invalid_argument(format!(
            "aggregation {name:?}: a string expression aggregates only under COUNT"
        ))),
        V::Int if matches!(op, O::Mean | O::Variance | O::Stddev) => {
            Err(Status::invalid_argument(format!(
                "aggregation {name:?}: {} takes a double; convert explicitly with double()",
                agg_op_name(op)
            )))
        }
        _ => Ok(()),
    }
}

/// Lowercase op name for refusals.
pub(crate) fn agg_op_name(op: crate::pb::AggregateOp) -> &'static str {
    use crate::pb::AggregateOp as O;
    match op {
        O::Unspecified => "unspecified",
        O::Count => "count",
        O::Sum => "sum",
        O::Min => "min",
        O::Max => "max",
        O::Mean => "mean",
        O::Variance => "variance",
        O::Stddev => "stddev",
        O::Cardinality => "cardinality",
    }
}

/// The explain breakdown of one scored document (docs/explain.md):
/// each present (field, term) pair's inputs and contribution, computed
/// with the scorers' own `idf` and `tf_norm` in the scorers' own
/// operand order, and the pre-stage sum recomposed from them in
/// accumulation order. `names[fi]` is the field name reported for
/// `fields[fi]`; `phrase` is the phrase leg's view index and parallel
/// term weights when the phrase scorer ran.
fn explain_terms(
    fields: &[bm25::FieldQuery<'_>],
    names: &[&str],
    presence: &[bm25::TermPresence],
    phrase: Option<(usize, &[f64])>,
) -> crate::pb::Bm25Explain {
    let mut base = 0.0f64;
    let mut phrase_max = 0.0f64;
    let mut terms = Vec::with_capacity(presence.len());
    for hit in presence {
        let fq = &fields[hit.field];
        let avgdl = fq.stats.avgdl();
        let idf = bm25::idf(fq.stats.doc_count, fq.stats.dfs[hit.term]);
        let tf_norm = bm25::tf_norm(fq.params, hit.tf, hit.doc_len, avgdl);
        let mut contribution = fq.weight * idf * tf_norm;
        let mut phrase_group = false;
        let mut phrase_weight = 1.0;
        match phrase {
            Some((view, weights)) if view == hit.field => {
                phrase_group = true;
                phrase_weight = weights[hit.term];
                contribution *= phrase_weight;
                phrase_max = phrase_max.max(contribution);
            }
            _ => base += contribution,
        }
        terms.push(crate::pb::Bm25TermExplain {
            field: names[hit.field].to_string(),
            term: fq.terms[hit.term].clone(),
            tf: hit.tf,
            doc_length: hit.doc_len,
            avgdl,
            k1: fq.params.k1,
            b: fq.params.b,
            tf_norm,
            doc_count: fq.stats.doc_count,
            df: fq.stats.dfs[hit.term],
            idf,
            weight: fq.weight,
            contribution,
            phrase_group,
            phrase_weight,
        });
    }
    crate::pb::Bm25Explain {
        terms,
        bm25: base + phrase_max,
        stages: Vec::new(),
        phrase: phrase.is_some(),
        phrase_max,
    }
}

/// The score-stage rows of an explain breakdown: the chain replayed
/// stage by stage from the pre-stage sum with the same reads and the
/// same float expressions `ScoreChain::eval` uses.
fn explain_stages(
    explain: &mut crate::pb::Bm25Explain,
    chain: &crate::scorefn::ScoreChain,
    specs: &[crate::pb::ScoreStage],
    doc_id: u32,
    columns: &dyn crate::scorefn::NumericRead,
) {
    let mut score = explain.bm25;
    for (i, stage) in chain.stages.iter().enumerate() {
        let spec = &specs[i];
        let mut row = crate::pb::ScoreStageExplain {
            stage: i as u32,
            column: spec.column.clone(),
            key: spec.key.clone(),
            present: false,
            input: 0.0,
            contribution: 0.0,
            output: score,
        };
        if let (Some(input), Some(contribution)) = (
            stage.input(doc_id, columns),
            stage.contribution(doc_id, columns),
        ) {
            score = if stage.is_additive() {
                score + contribution
            } else {
                score * contribution
            };
            row.present = true;
            row.input = input;
            row.contribution = contribution;
            row.output = score;
        }
        explain.stages.push(row);
    }
}

fn resolve_shard_filters(
    bm25: Option<&Bm25Shard>,
    deleted: Option<Arc<Vec<u64>>>,
    n: usize,
    geo_filters: &[crate::pb::GeoFilter],
    geo_regions: &[crate::geo::GeoRegion],
    filter: Option<&crate::pb::FilterExpr>,
) -> Result<(Option<crate::filter::DocFilter<'static>>, Option<Vec<bool>>), Status> {
    if geo_filters.is_empty() && filter.is_none() && deleted.is_none() {
        return Ok((None, None));
    }
    let Some(store) = bm25 else {
        if geo_filters.is_empty() && filter.is_none() {
            let allow = (0..n)
                .map(|slot| {
                    !deleted.as_ref().is_some_and(|words| {
                        words
                            .get(slot / 64)
                            .is_some_and(|word| word & (1u64 << (slot % 64)) != 0)
                    })
                })
                .collect();
            return Ok((None, Some(allow)));
        }
        return Ok((None, Some(vec![false; n])));
    };
    if store.as_index().is_none() {
        return Err(Status::failed_precondition(
            "bm25 bulk build in progress; Flush before filtering the vector leg",
        ));
    }
    let doc_filter = crate::filter::DocFilter {
        deleted,
        geo: store.resolve_geo_filters(geo_filters, geo_regions),
        pred: filter.map(|f| store.resolve_filter(f)).transpose()?,
        phrase: Vec::new(),
    };
    let allow = {
        let cols = ShardNumericRead(store);
        (0..n as u32)
            .map(|slot| doc_filter.passes(slot, &cols))
            .collect()
    };
    Ok((Some(doc_filter), Some(allow)))
}

/// The two known-column handshakes for one request, in the shape every
/// response carries them: which requested geo columns this shard's
/// table has, and which leaves of the filter tree it can resolve
/// ([`crate::filter::walk_leaves`] order). Computed regardless of `k`
/// and regardless of whether the shard scores, so a typo refuses even
/// on a query that would legitimately return nothing.
fn filter_known_flags(
    bm25: Option<&Bm25Shard>,
    geo_filters: &[crate::pb::GeoFilter],
    filter: Option<&crate::pb::FilterExpr>,
) -> (Vec<bool>, Vec<bool>) {
    let geo = match bm25 {
        Some(store) => store.geo_columns_known(geo_filters),
        None => vec![false; geo_filters.len()],
    };
    let tree = match (bm25, filter) {
        (Some(store), Some(f)) => store.filter_columns_known(f),
        (None, Some(f)) => vec![false; crate::filter::leaf_count(f)],
        (_, None) => Vec::new(),
    };
    (geo, tree)
}

/// Remove generation tombstones from one postings view's corpus shares.
fn live_field_stats(
    index: &dyn Bm25Index,
    terms: &[String],
    live_docs: &LiveDocs,
    slots: u32,
) -> (u64, Vec<u32>) {
    if !live_docs.has_deletes() {
        return (
            index.total_doc_length(),
            terms.iter().map(|term| index.df(term)).collect(),
        );
    }
    let deleted_length: u64 = (0..slots)
        .filter(|slot| live_docs.is_deleted(*slot as usize))
        .map(|slot| u64::from(index.doc_length(slot)))
        .sum();
    let dfs = terms
        .iter()
        .map(|term| {
            let mut live_df = 0u32;
            index.for_each_doc_tf(term, &mut |doc, _tf| {
                if !live_docs.is_deleted(doc as usize) {
                    live_df += 1;
                }
            });
            live_df
        })
        .collect();
    (index.total_doc_length().saturating_sub(deleted_length), dfs)
}

fn live_document_count(store: &Bm25Shard, live_docs: &LiveDocs) -> u64 {
    if !live_docs.has_deletes() {
        return store.doc_count();
    }
    let views: Vec<_> = (0..store.field_count())
        .filter_map(|field| store.field_view(field))
        .collect();
    (0..store.next_doc_id())
        .filter(|slot| {
            !live_docs.is_deleted(*slot as usize)
                && views.iter().any(|view| view.doc_length(*slot) > 0)
        })
        .count() as u64
}

fn active_artifact_rows(guard: &ShardState) -> Vec<u64> {
    [
        guard.index.as_ref().map(|index| index.len() as u64),
        guard
            .bm25
            .as_ref()
            .map(|store| u64::from(store.next_doc_id())),
        guard.exact_vectors.as_ref().map(|store| store.len() as u64),
    ]
    .into_iter()
    .flatten()
    .filter(|rows| *rows != 0)
    .collect()
}

pub(crate) fn physical_rows(guard: &ShardState) -> u64 {
    active_artifact_rows(guard).into_iter().max().unwrap_or(0)
}

/// Order-preserving u64 key for an i64 value: offset binary, so the
/// unsigned comparison of the results matches the signed comparison of
/// the inputs (docs/query-api.md, sorted browse).
pub(crate) fn i64_order_bits(x: i64) -> u64 {
    (x as u64) ^ (1u64 << 63)
}

/// Inverse of [`i64_order_bits`].
pub(crate) fn i64_from_order_bits(bits: u64) -> i64 {
    (bits ^ (1u64 << 63)) as i64
}

/// Inverse of [`f64_order_bits`].
pub(crate) fn f64_from_order_bits(bits: u64) -> f64 {
    if bits >> 63 == 1 {
        f64::from_bits(bits & !(1u64 << 63))
    } else {
        f64::from_bits(!bits)
    }
}

/// Order-preserving u64 key for a (finite) f64 value: the sign-flip
/// trick — negatives flip every bit, positives set the sign bit — so
/// unsigned comparison matches numeric order. NaN never reaches this
/// (NaN is the f64 column's absence sentinel and absent values are
/// excluded before keying).
pub(crate) fn f64_order_bits(x: f64) -> u64 {
    let bits = x.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1u64 << 63)
    }
}

/// An i64 column's bound metadata on the score-chain's f64 scale. The
/// empty range (min > max, the i64 column's stand-in for NaN) becomes
/// the (NaN, NaN) the bound rules already read as "no metadata"; a real
/// range converts through the same monotone cast the values do, so the
/// bound still dominates every converted value.
fn int_min_max_as_f64((min, max): (i64, i64)) -> (f64, f64) {
    if min > max {
        (f64::NAN, f64::NAN)
    } else {
        (min as f64, max as f64)
    }
}

/// Parse and validate a wire score-stage list into resolved ops plus
/// their column names — the shard-independent half of chain building
/// (`docs/score-functions.md`). Refuses unknown ops, empty column
/// names, and parameters outside each op's admission rule: every
/// refusal here is a stage whose monotonicity or bound would not hold.
pub(crate) fn parse_score_stages(
    stages: &[crate::pb::ScoreStage],
) -> Result<Vec<(crate::scorefn::StageOp, String, String)>, Status> {
    use crate::scorefn::StageOp;
    stages
        .iter()
        .enumerate()
        .map(|(i, stage)| {
            if stage.column.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "score stage {i}: a stage names the numeric column it reads"
                )));
            }
            let op = match crate::pb::ScoreOp::try_from(stage.op) {
                Ok(crate::pb::ScoreOp::MultExpDecay) => {
                    if !(stage.scale.is_finite() && stage.scale > 0.0) || !stage.origin.is_finite()
                    {
                        return Err(Status::invalid_argument(format!(
                            "score stage {i}: MULT_EXP_DECAY needs finite origin and scale > 0"
                        )));
                    }
                    StageOp::MultExpDecay {
                        origin: stage.origin,
                        scale: stage.scale,
                    }
                }
                Ok(crate::pb::ScoreOp::MultLog) => {
                    if !stage.weight.is_finite() || stage.weight < 0.0 {
                        return Err(Status::invalid_argument(format!(
                            "score stage {i}: MULT_LOG needs a finite weight >= 0 (a negative \
                             weight could turn the factor negative, breaking monotonicity)"
                        )));
                    }
                    StageOp::MultLog {
                        weight: stage.weight,
                    }
                }
                Ok(crate::pb::ScoreOp::AddLinear) => {
                    if !stage.weight.is_finite() {
                        return Err(Status::invalid_argument(format!(
                            "score stage {i}: ADD_LINEAR needs a finite weight"
                        )));
                    }
                    StageOp::AddLinear {
                        weight: stage.weight,
                    }
                }
                Ok(
                    op @ (crate::pb::ScoreOp::MultGeoDecayHaversine
                    | crate::pb::ScoreOp::MultGeoDecayManhattan),
                ) => {
                    if !(stage.scale.is_finite() && stage.scale > 0.0) {
                        return Err(Status::invalid_argument(format!(
                            "score stage {i}: MULT_GEO_DECAY needs a finite scale > 0 (meters)"
                        )));
                    }
                    validate_lat_lon(
                        &format!("score stage {i}"),
                        stage.origin_lat,
                        stage.origin_lon,
                    )?;
                    if !stage.key.is_empty() {
                        return Err(Status::invalid_argument(format!(
                            "score stage {i}: MULT_GEO_DECAY reads a geo-point column, which \
                             has no keys; leave `key` empty"
                        )));
                    }
                    StageOp::MultGeoDecay {
                        metric: if op == crate::pb::ScoreOp::MultGeoDecayHaversine {
                            crate::geo::GeoMetric::Haversine
                        } else {
                            crate::geo::GeoMetric::Manhattan
                        },
                        origin_lat: stage.origin_lat,
                        origin_lon: stage.origin_lon,
                        scale: stage.scale,
                    }
                }
                Ok(crate::pb::ScoreOp::Unspecified) | Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "score stage {i}: unknown op {}",
                        stage.op
                    )));
                }
            };
            Ok((op, stage.column.clone(), stage.key.clone()))
        })
        .collect()
}

/// The shard's two indexes behind one lock: the turbovec vector index and
/// the BM25 postings store. Either may be absent (vector-only shards,
/// docs-only shards, from-scratch shards).
#[derive(Default)]
pub(crate) struct ShardState {
    pub(crate) index: Option<VectorIndex>,
    /// Original row-major FP32 vectors, aligned one-for-one with provider
    /// slots. Legacy generations may lack this sidecar; native provider
    /// search remains available, while FP32 rerank refuses by name.
    pub(crate) exact_vectors: Option<ExactVectorStore>,
    pub(crate) bm25: Option<Bm25Shard>,
    /// Generation tombstones shared by every lexical and vector read path.
    pub(crate) live_docs: LiveDocs,
    /// The active snapshot generation directory, when the shard's files
    /// came from (or were replaced by) an `InstallSnapshot` image.
    /// `Flush` and the AddDocuments reload path read/write THERE, never
    /// the legacy `<index path>` layout, so the two never split-brain.
    pub(crate) generation: Option<PathBuf>,
    /// The write-ahead log (`<index path>.wal/`), behind the same lock as
    /// the index it precedes. `None` when the shard runs without one.
    pub(crate) wal: Option<WalWriter>,
    /// Cached slot -> parent map for collapse scans (lineage parent_id
    /// per slot). Self-validating: rebuilt whenever its length disagrees
    /// with the index, cleared on snapshot install.
    pub(crate) parents: Option<std::sync::Arc<Vec<u64>>>,
    /// The shard's mapped-plan binding (`docs/descriptor-mappings.md`
    /// section 4a): RAM authority for the identity every mapped stream
    /// must match. Loaded from the store's kind-6 entry on attach,
    /// recorded to the WAL (markers) on first bind, written back into
    /// the store at flush. `None` = never bound.
    pub(crate) mapped_binding: Option<crate::postings::StoredBinding>,
    /// Advances on every mutation of `bm25` (ingest, flush, snapshot
    /// install, startup attach). `TermStats` reports it and the scoring
    /// RPCs enforce a caller's claim against it, which is what lets a
    /// coordinator cache term stats without ever scoring against a
    /// store the stats no longer describe. Starts at 1: 0 is the wire's
    /// "no claim". Over-bumping is safe (a cache refetches); a missed
    /// bump is the only unsound direction.
    pub(crate) stats_epoch: u64,
    /// A compaction cutover whose closing flush has not run yet
    /// (`docs/mutations.md`): the marker on disk says "roll back on
    /// restart" until the next flush writes the new generation's images
    /// and retires what the marker lists. `Flush` completes it.
    pub(crate) pending_compaction: Option<crate::compaction::PendingCommit>,
}

/// Message prefix of a stats-epoch refusal. The coordinator's retry
/// distinguishes this refusal from every other FAILED_PRECONDITION by
/// this prefix, so it and [`ShardState::check_stats_epoch`] must move
/// together.
pub(crate) const STALE_STATS_EPOCH: &str = "stale stats epoch";

impl ShardState {
    /// Enforce a scoring request's stats-epoch claim (see
    /// `Bm25QueryRequest.expected_stats_epoch`). Must be called under
    /// the same guard the scoring reads through — checking on one guard
    /// acquisition and scoring on another would leave a gap an ingest
    /// commit can slip into.
    fn check_stats_epoch(&self, expected: u64) -> Result<(), Status> {
        if expected != 0 && expected != self.stats_epoch {
            return Err(Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: the request's global stats were computed at epoch \
                 {expected} but this shard is at {}; its postings changed in between, so \
                 those stats no longer describe it",
                self.stats_epoch
            )));
        }
        Ok(())
    }
}

/// The persistence path of a shard's BM25 store: `<index path>.bm25`.
/// The segment catalog root of a persisted shard: `<index>.segments/`
/// (`docs/immutable-segments.md`).
pub fn segments_root(index_path: &std::path::Path) -> PathBuf {
    let mut name = index_path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".segments");
    index_path.with_file_name(name)
}

/// The heap store a node's configuration declares: the tail of a
/// segmented shard, or the whole store of an in-memory one.
pub(crate) fn heap_store(config: &NodeConfig) -> Result<Bm25Store, String> {
    let names: Vec<&str> = config.bm25_fields.iter().map(String::as_str).collect();
    for name in config.position_fields.iter().chain(&config.sentence_fields) {
        if !names.contains(&name.as_str()) {
            return Err(format!(
                "field {name:?} is not in this shard's BM25 field table {names:?}"
            ));
        }
    }
    let facets: Vec<&str> = config.facet_fields.iter().map(String::as_str).collect();
    let numerics: Vec<&str> = config.numeric_fields.iter().map(String::as_str).collect();
    let map_facets: Vec<&str> = config.map_facet_fields.iter().map(String::as_str).collect();
    let map_numerics: Vec<&str> = config
        .map_numeric_fields
        .iter()
        .map(String::as_str)
        .collect();
    let integers: Vec<&str> = config.integer_fields.iter().map(String::as_str).collect();
    let geos: Vec<&str> = config.geo_fields.iter().map(String::as_str).collect();
    let positions: Vec<&str> = config.position_fields.iter().map(String::as_str).collect();
    let sentences: Vec<&str> = config.sentence_fields.iter().map(String::as_str).collect();
    Ok(Bm25Store::with_fields(&names)
        .with_facets(&facets)
        .with_numerics(&numerics)
        .with_map_facets(&map_facets)
        .with_map_numerics(&map_numerics)
        .with_integers(&integers)
        .with_geos(&geos)
        .with_positions(&positions)
        .with_sentences(&sentences))
}

pub fn bm25_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".bm25");
    PathBuf::from(p)
}

/// The product-owned FP32 sidecar beside a legacy provider image.
pub fn exact_vector_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".exact");
    PathBuf::from(p)
}

/// The persisted live-row overlay beside a legacy provider image.
pub fn live_docs_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".live");
    PathBuf::from(p)
}

/// Where a bulk BM25 build spills while it runs: `<bm25 path>.build`.
///
/// A successful `Flush` removes it, so finding one beside a MISSING
/// `.bm25` is unambiguous evidence of an interrupted build — as opposed
/// to a shard that simply has no postings, which is what a vector-only
/// deployment legitimately looks like.
pub fn bm25_build_dir(bm25_path: &std::path::Path) -> PathBuf {
    let mut p = bm25_path.as_os_str().to_owned();
    p.push(".build");
    PathBuf::from(p)
}

/// Snapshot generation layout, next to the shard's configured index path.
/// New generations use `vector.index` and `documents.bm25`; readers also
/// recognize the former `index.tv` pair.
pub fn generation_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap");
    PathBuf::from(p)
}

/// The provider image inside a generation directory. Existing generations
/// written before the provider abstraction keep their legacy filename.
pub fn generation_vector(dir: &Path) -> PathBuf {
    let current = dir.join("vector.index");
    let legacy = dir.join("index.tv");
    if current.exists() || !legacy.exists() {
        current
    } else {
        legacy
    }
}
/// The product-owned FP32 sidecar inside a generation directory.
pub fn generation_exact_vectors(dir: &Path) -> PathBuf {
    dir.join("vectors.f32")
}
/// The BM25 sidecar path inside a generation directory.
pub fn generation_bm25(dir: &Path) -> PathBuf {
    let current = dir.join("documents.bm25");
    let legacy = dir.join("index.tv.bm25");
    if current.exists() || !legacy.exists() {
        current
    } else {
        legacy
    }
}

/// Product-owned live-row overlay inside a snapshot generation.
pub fn generation_live_docs(dir: &Path) -> PathBuf {
    dir.join("live-docs.bin")
}

pub(crate) fn live_docs_storage_path(index_path: &Path, generation: Option<&PathBuf>) -> PathBuf {
    generation.map_or_else(
        || live_docs_sidecar_path(index_path),
        |dir| generation_live_docs(dir),
    )
}

/// Receive staging (`<index path>.snap-tmp/`) and swap-out
/// (`<index path>.snap-old/`) directories for the generation swap.
pub(crate) fn generation_tmp_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-tmp");
    PathBuf::from(p)
}
pub(crate) fn generation_old_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-old");
    PathBuf::from(p)
}

/// Where a segment-layout install parks the previous catalog during
/// its swap (`<index path>.segments.snap-old`); see
/// [`recover_segments_swap`].
fn segments_old_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".segments.snap-old");
    PathBuf::from(p)
}

/// The private directory a `StreamSnapshot` exports into before
/// streaming (`<index path>.snap-export-<n>/`): one per stream, removed
/// when the stream ends.
fn export_staging_dir(index_path: &Path) -> PathBuf {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut p = index_path.as_os_str().to_owned();
    p.push(format!(
        ".snap-export-{}-{}",
        std::process::id(),
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    PathBuf::from(p)
}

/// Crash recovery for a segment-layout snapshot install, the catalog
/// twin of [`recover_generation`]: the swap renames the live catalog to
/// `.segments.snap-old` and the staged one into place. `snap-old`
/// present with no live catalog means the crash fell between the two
/// renames — the previous catalog is whole, rename it back; both present
/// means the new catalog is live — delete the old one. Stray export
/// staging directories are unreceived garbage and are removed.
pub fn recover_segments_swap(index_path: &Path) {
    let root = segments_root(index_path);
    let old = segments_old_dir(index_path);
    if old.exists() {
        if root.join("segments.json").exists() {
            let _ = std::fs::remove_dir_all(&old);
        } else {
            let _ = std::fs::remove_dir_all(&root);
            let _ = std::fs::rename(&old, &root);
        }
    }
    if let (Some(parent), Some(name)) = (index_path.parent(), index_path.file_name()) {
        let prefix = format!("{}.snap-export-", name.to_string_lossy());
        if let Ok(entries) = std::fs::read_dir(parent) {
            for entry in entries.flatten() {
                if entry.file_name().to_string_lossy().starts_with(&prefix) {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
    }
}

/// Where the shard's files live: the active snapshot generation when one
/// was installed, else the legacy `<index path>` (+`.bm25`) layout.
/// Returns `(provider index, exact vectors, bm25)` paths.
pub(crate) fn storage_paths(
    index_path: &Path,
    generation: Option<&PathBuf>,
) -> (PathBuf, PathBuf, PathBuf) {
    match generation {
        Some(dir) => (
            generation_vector(dir),
            generation_exact_vectors(dir),
            generation_bm25(dir),
        ),
        None => (
            index_path.to_path_buf(),
            exact_vector_sidecar_path(index_path),
            bm25_sidecar_path(index_path),
        ),
    }
}

/// Crash recovery for the generation swap, and the startup answer to
/// "does this shard have an installed snapshot?". Every interleave of the
/// two swap renames has a defined outcome:
///
/// - `snap-old` present, `snap` missing: crashed between the renames —
///   the previous generation is whole, rename it back.
/// - both present: crashed after the second rename — the new generation
///   is live, delete the old one.
/// - a stray `snap-tmp` is always deleted: only a COMPLETE staging dir is
///   ever renamed into place, so a leftover one is unreceived garbage.
///
/// Returns the active generation directory when it holds an index.
pub fn recover_generation(index_path: &Path) -> Option<PathBuf> {
    // A compaction cutover that never reached its closing flush rolls
    // back to the generation it replaced BEFORE the swap rules below
    // run, or they would retire the very files the rollback restores
    // (docs/mutations.md).
    crate::compaction::recover_interrupted(index_path);
    let snap = generation_dir(index_path);
    let old = generation_old_dir(index_path);
    let tmp = generation_tmp_dir(index_path);
    let _ = std::fs::remove_dir_all(&tmp);
    if old.exists() {
        if snap.exists() {
            let _ = std::fs::remove_dir_all(&old);
        } else {
            let _ = std::fs::rename(&old, &snap);
        }
    }
    generation_vector(&snap).exists().then_some(snap)
}

/// Build the manifest describing the shard's current provider state and
/// dimension. An empty shard completes it lazily through backend configuration
/// or its first batch, before any records make the manifest immutable.
///
/// `preexisting` is the (vectors, documents) the shard already holds
/// that this generation's log will NOT contain — the installed image on
/// a snapshot rotation, or the whole shard when logging is enabled on an
/// already-populated index. Nonzero preexisting state marks the log as
/// partial history, which the reshard tool refuses (a log-only replay
/// would silently drop that state).
/// The index's committed TQ+ pair, `None` when uncalibrated — the
/// shape the fork's former `calibration()` getter returned, read here
/// through upstream's explicit-calibration accessors (#474).
fn calibration_of(index: &VectorIndex) -> Option<(Vec<f32>, Vec<f32>)> {
    let config = index.backend_config().ok()?;
    let legacy = legacy_calibration_config(&config).ok()??;
    (!legacy.shift.is_empty()).then_some((legacy.shift, legacy.scale))
}

fn wire_backend_config(config: &VectorBackendConfig) -> WireVectorBackendConfig {
    WireVectorBackendConfig {
        backend_kind: config.backend_kind.clone(),
        config_format: config.config_format.clone(),
        payload: config.payload.clone(),
    }
}

fn internal_backend_config(config: WireVectorBackendConfig) -> Result<VectorBackendConfig, Status> {
    if config.backend_kind.trim().is_empty() {
        return Err(Status::invalid_argument("vector backend kind is required"));
    }
    if config.config_format.trim().is_empty() {
        return Err(Status::invalid_argument(
            "vector backend config format is required",
        ));
    }
    Ok(VectorBackendConfig {
        backend_kind: config.backend_kind,
        config_format: config.config_format,
        payload: config.payload,
    })
}

fn wire_backend_descriptor(index: &VectorIndex) -> WireVectorBackendDescriptor {
    let descriptor = index.descriptor();
    WireVectorBackendDescriptor {
        backend_kind: descriptor.backend_kind,
        backend_version: descriptor.backend_version,
        dim: descriptor.dimension.unwrap_or(0) as u32,
        bits_per_dimension: descriptor.bits_per_dimension.unwrap_or(0),
        metric: descriptor.metric,
        score_direction: match descriptor.score_direction {
            ScoreDirection::HigherIsBetter => VectorScoreDirection::HigherIsBetter as i32,
            ScoreDirection::LowerIsBetter => VectorScoreDirection::LowerIsBetter as i32,
        },
        scoring_fingerprint: descriptor.scoring_fingerprint,
        quality_contract: match descriptor.quality_contract {
            QualityContract::ExhaustiveQuantized => {
                VectorQualityContract::ExhaustiveNativeScore as i32
            }
            QualityContract::ConfiguredAnn => VectorQualityContract::ConfiguredAnn as i32,
            QualityContract::ProbabilisticBound => VectorQualityContract::ProbabilisticBound as i32,
        },
        capabilities: descriptor
            .capabilities
            .into_iter()
            .map(|capability| capability.as_str().to_string())
            .collect(),
    }
}

pub(crate) fn wal_manifest(
    index: Option<&VectorIndex>,
    config: &NodeConfig,
    generation: u64,
    preexisting: (u64, u64),
) -> wal::WalManifest {
    let backend_config = index.and_then(|index| index.backend_config().ok());
    let (dim, bit_width, shift, scale) = match index {
        Some(index) => {
            let (shift, scale) = calibration_of(index).unwrap_or_default();
            (
                index.dim_opt().unwrap_or(0) as u32,
                index.bits_per_dimension().unwrap_or(config.bit_width) as u32,
                shift,
                scale,
            )
        }
        None => (0, config.bit_width as u32, Vec::new(), Vec::new()),
    };
    wal::WalManifest {
        dim,
        vector_backend: backend_config
            .as_ref()
            .map_or_else(|| config.vector_backend.clone(), |c| c.backend_kind.clone()),
        vector_config_format: backend_config
            .as_ref()
            .map_or_else(String::new, |c| c.config_format.clone()),
        vector_config_payload: backend_config.map_or_else(Vec::new, |c| c.payload),
        bit_width,
        calibration_shift: shift,
        calibration_scale: scale,
        collection: config.collection.clone(),
        slot_offset: config.slot_offset,
        generation,
        bucket_bits: config.wal_buckets.trailing_zeros(),
        bucket_count: config.wal_buckets,
        preexisting_vectors: preexisting.0,
        preexisting_documents: preexisting.1,
        format_version: wal::FORMAT_VERSION,
    }
}

/// The persisted document tip of a shard: `next_doc_id` of the on-disk
/// BM25 sidecar (generation-aware), 0 when none exists. Opened read-only
/// and dropped — the serving copy is attached separately (`with_bm25`);
/// this exists so WAL reconciliation can know the applied tip without
/// depending on attachment order.
fn persisted_doc_tip(index_path: &Path) -> u64 {
    let generation = recover_generation(index_path);
    let (_, _, bm25_path) = storage_paths(index_path, generation.as_ref());
    if !bm25_path.exists() {
        return 0;
    }
    match Bm25Shard::open(&bm25_path) {
        Ok(store) => u64::from(store.next_doc_id()),
        // Panic, like the rest of the WAL open path: guessing a tip of 0
        // would truncate legitimate document records, and the binary
        // would refuse to serve this sidecar at attach time anyway.
        Err(e) => panic!(
            "wal reconciliation: cannot read {}: {e}",
            bm25_path.display()
        ),
    }
}

/// Open the shard's WAL at `<index path>.wal/`: resume the newest
/// generation after a restart (truncating any torn tails, continuing the
/// per-file sequences) or start generation 0. A resumed log keeps its own
/// bucket count — the configured `--wal-buckets` only applies at WAL
/// creation. Panics on IO failure, like the BM25 load path in the
/// binary — a shard that cannot log must not silently run unlogged.
///
/// Resume reconciles the log against the applied state first: records at
/// or above the applied tip (`slot_offset + max(vector tip, document
/// tip)`) are truncated, because appends are buffered and a crash can
/// leave the on-disk log ahead of the on-disk indexes — the reopening
/// shard would otherwise re-assign ids the log already holds. The
/// dropped records were never durable-acked (Flush is the durability
/// point).
///
/// A log CREATED over an already-populated shard records the shard's
/// current contents as `preexisting_*` in its manifest: it can serve and
/// recover, but it is not full history and cannot drive a reshard.
///
/// Concurrent first-open is idempotent (see [`wal::open_or_create`]):
/// twin nodes sharing the shard files can cold-start against a shard
/// whose WAL directory does not exist yet, and both end up with a
/// writer onto the same well-formed generation instead of one of them
/// panicking on the mid-create directory.
fn open_wal(index: Option<&VectorIndex>, config: &NodeConfig) -> Option<WalWriter> {
    if !config.wal {
        return None;
    }
    let index_path = config
        .index_path
        .as_ref()
        .expect("wal requires an index path");
    let vector_tip = index.map_or(0, |i| i.len() as u64);
    let doc_tip = persisted_doc_tip(index_path);
    let dir = wal::wal_dir(index_path);
    let cutoff = config.slot_offset + vector_tip.max(doc_tip);
    let fresh = wal_manifest(index, config, 0, (vector_tip, doc_tip));
    let mut writer = wal::open_or_create(&dir, cutoff, fresh)
        .unwrap_or_else(|e| panic!("open WAL at {}: {e}", dir.display()));
    // The log belongs to one collection (docs/collections.md). A manifest
    // from before collections is adopted by the node that opens it, and
    // written; one that names another collection is refused, because a
    // replay of it here would put that dataset's documents into this one.
    let held = writer.manifest().collection.clone();
    if held.is_empty() && !config.collection.is_empty() {
        writer.update_manifest(|m| m.collection = config.collection.clone());
    } else if held != config.collection {
        panic!(
            "WAL at {} belongs to collection {held:?}, but this node serves {:?}; a shard \
             belongs to only one collection",
            dir.display(),
            config.collection
        );
    }
    if writer.manifest().bucket_count != config.wal_buckets {
        eprintln!(
            "wal: --wal-buckets={} ignored; the existing log at {} has bucket_count={}",
            config.wal_buckets,
            writer.dir().display(),
            writer.manifest().bucket_count
        );
    }
    // A resumed generation whose calibration never locked (no records
    // yet) still accepts manifest completion.
    writer.update_manifest(|m| {
        let fresh = wal_manifest(index, config, m.generation, (0, 0));
        if m.dim == 0 {
            m.dim = fresh.dim;
            m.bit_width = fresh.bit_width;
            m.slot_offset = fresh.slot_offset;
        }
        if m.calibration_shift.is_empty() {
            m.calibration_shift = fresh.calibration_shift;
            m.calibration_scale = fresh.calibration_scale;
        }
        if m.vector_config_format.is_empty() && !fresh.vector_config_format.is_empty() {
            m.vector_backend = fresh.vector_backend;
            m.vector_config_format = fresh.vector_config_format;
            m.vector_config_payload = fresh.vector_config_payload;
        }
    });
    eprintln!("wal: logging to {}", writer.dir().display());
    Some(writer)
}

/// Send `path` onto a snapshot stream in [`SNAPSHOT_STREAM_CHUNK`]
/// pieces. False when the receiver hung up or the file could not be read
/// (the receiver then sees a truncated artifact and refuses it by hash).
async fn stream_file(tx: &mpsc::Sender<Result<SnapshotChunk, Status>>, path: &Path) -> bool {
    use tokio::io::AsyncReadExt;
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) => {
            let _ = tx
                .send(Err(Status::internal(format!(
                    "read {}: {e}",
                    path.display()
                ))))
                .await;
            return false;
        }
    };
    let mut buf = vec![0u8; SNAPSHOT_STREAM_CHUNK];
    loop {
        match file.read(&mut buf).await {
            Ok(0) => return true,
            Ok(n) => {
                let chunk = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Data(buf[..n].to_vec())),
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    return false;
                }
            }
            Err(e) => {
                let _ = tx
                    .send(Err(Status::internal(format!(
                        "read {}: {e}",
                        path.display()
                    ))))
                    .await;
                return false;
            }
        }
    }
}

/// Snapshot stream chunk: 1 MiB keeps messages far under
/// [`crate::MAX_MESSAGE_BYTES`] while amortizing per-message overhead.
pub const SNAPSHOT_STREAM_CHUNK: usize = 1024 * 1024;

/// Log `op`, or degrade the shard to unlogged if the append fails.
///
/// The mutation was already applied when this runs (apply-then-log, see
/// [`WalWriter::append`]), so failing the client's request would report a
/// write that in fact happened. Instead the shard keeps serving and the
/// log is retired loudly: the generation directory is renamed `.broken`
/// (so the reshard tool and a restarting node cannot mistake it for
/// history) and the writer is dropped. Per the resharding policy, a
/// shard without a WAL serves fine but can only be rebuilt, never
/// resharded.
pub(crate) fn wal_append_or_degrade(wal_slot: &mut Option<WalWriter>, op: wal_record::Op) {
    let Some(wal) = wal_slot.as_mut() else { return };
    if let Err(e) = wal.append(op) {
        let dir = wal.dir().to_path_buf();
        let broken = dir.with_extension("broken");
        eprintln!(
            "wal: append to {} failed ({e}); retiring the log as {} — this shard continues \
             UNLOGGED and can no longer be resharded from its log (rebuild required)",
            dir.display(),
            broken.display()
        );
        *wal_slot = None;
        if let Err(e) = std::fs::rename(&dir, &broken) {
            eprintln!("wal: could not rename the broken generation: {e}");
        }
    }
}

/// The shard-owner gRPC service. Cheap to clone (state is shared).
/// What a seal in flight writes out (`NodeServiceImpl::seal_tail`):
/// the frozen part's rows and shared artifacts, gathered under the
/// write lock, written with no lock held.
struct SealPlan {
    base: usize,
    rows: usize,
    documents: Arc<Bm25Store>,
    live: LiveDocs,
    image: Option<Arc<VectorIndex>>,
    backend_kind: String,
    catalog: crate::segments::SegmentCatalog,
    stage: PathBuf,
    segment_id: String,
    generation: u64,
}

#[derive(Clone)]
pub struct NodeServiceImpl {
    /// Locked shard state; see [`ShardState`].
    pub(crate) state: Arc<RwLock<ShardState>>,
    /// Single-writer gate for ingest streams. Two concurrent AddDocuments
    /// (or AddVectors) streams would interleave positional ids into one
    /// shard — every doc logged, none attributable — so the second stream
    /// is refused outright rather than merged.
    ingest_busy: Arc<std::sync::atomic::AtomicBool>,
    /// A fence on ingest (`docs/cluster-control.md`, "Shard split"): set
    /// by the node agent when the shard's rows are moving to its split
    /// children, so no append can land between the children's final
    /// catch-up and the topology cutover. The reason names the
    /// children; a fenced shard refuses every ingest stream by name and
    /// keeps serving queries.
    ingest_fence: Arc<std::sync::Mutex<Option<String>>>,
    pub(crate) config: NodeConfig,
    /// Shared scan queue for coalesced searches; the scheduler task is
    /// spawned on first use (shared across service clones).
    scan_jobs: Arc<std::sync::OnceLock<mpsc::Sender<ScanJob>>>,
    /// Lane budget shared by every exact-rerank request on this node.
    rerank_slots: Arc<tokio::sync::Semaphore>,
    /// UDP fast-lane registry: stream token -> that stream's monotone floor
    /// and advisory cancellation flag. Fed by [`Self::spawn_floor_listener`]
    /// and polled by the streaming scan before every chunk.
    stream_signals: Arc<std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>>,
    /// The shard's vocabulary listener (`<index path>.vocab/`), attached
    /// to every ingest AnalyzeStream. `None` when vocabulary accumulation
    /// is off (the default) or its directory failed to initialize.
    vocab: Option<Arc<crate::vocab::VocabularyListener>>,
    /// Compiled materialization spec for the current ingest stream
    /// (docs/cel-values.md): expressions compile once per spec CHANGE,
    /// never per document — the ingest analog of the query rule.
    materialize_cache: Arc<std::sync::Mutex<Option<CompiledMaterialize>>>,
    /// Optional product-owned phrase vocabulary shared by ingest and query.
    pub(crate) phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    /// Seals on this shard run one at a time; the frozen part of a seal
    /// in flight is the reason (`docs/immutable-segments.md`).
    pub(crate) seal_lock: Arc<std::sync::Mutex<()>>,
    /// One compaction at a time per shard (`docs/mutations.md`): a
    /// second `CompactShard` refuses by name while this is set.
    pub(crate) compacting: Arc<std::sync::atomic::AtomicBool>,
    /// Woken after every flush: the node agent reports the shard to the
    /// control plane on it (`docs/cluster-control.md`).
    flush_notify: Option<Arc<tokio::sync::Notify>>,
}

/// One compiled MaterializeSpec, cached against spec equality.
struct CompiledMaterialize {
    /// The spec these columns were compiled from, for the equality test.
    spec: crate::pb::MaterializeSpec,
    /// (column name, compiled expression, target kind), in spec order.
    columns: Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>,
}

/// Open the shard's vocabulary listener at `<index path>.vocab/`,
/// resuming its snapshot history. Unlike the WAL this is analytics, not
/// a ledger: a directory that cannot be created, probed, or scanned
/// degrades the shard to uncounted with a loud warning — ingest itself
/// is unaffected.
fn open_vocab(config: &NodeConfig) -> Option<Arc<crate::vocab::VocabularyListener>> {
    if !config.vocab {
        return None;
    }
    let index_path = config.index_path.as_ref()?;
    let dir = crate::vocab::vocab_dir(index_path);
    match crate::vocab::VocabularyListener::create(
        &dir,
        config.vocab_window_docs,
        config.vocab_top_k,
    ) {
        Ok(listener) => {
            eprintln!(
                "vocab: accumulating to {} (window {} docs, top-K {})",
                dir.display(),
                config.vocab_window_docs,
                config.vocab_top_k
            );
            Some(Arc::new(listener))
        }
        Err(e) => {
            eprintln!(
                "vocab: {} is not writable ({e}); vocabulary accumulation is DISABLED, \
                 ingest is unaffected",
                dir.display()
            );
            None
        }
    }
}

/// Raise a floor cell (f32 bits) to `floor` if that is higher. Monotone
/// under any interleaving of the gRPC and UDP lanes; NaN is ignored.
fn raise_floor_cell(cell: &std::sync::atomic::AtomicU32, floor: f32) {
    if floor.is_nan() {
        return;
    }
    let _ = cell.fetch_update(
        std::sync::atomic::Ordering::AcqRel,
        std::sync::atomic::Ordering::Acquire,
        |cur| (floor > f32::from_bits(cur)).then(|| floor.to_bits()),
    );
}

#[cfg_attr(not(feature = "net"), allow(dead_code))]
struct StreamSignals {
    floor: std::sync::atomic::AtomicU32,
    cancelled: std::sync::atomic::AtomicBool,
    /// The newest signed sequence applied to this stream; a datagram
    /// at or behind it is a replay and is ignored.
    last_seq: std::sync::atomic::AtomicU32,
}

#[cfg_attr(not(feature = "net"), allow(dead_code))]
impl StreamSignals {
    fn new(initial_floor: f32) -> Self {
        Self {
            floor: std::sync::atomic::AtomicU32::new(initial_floor.to_bits()),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            last_seq: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Accept `seq` when it is newer than the last applied one.
    fn accept_seq(&self, seq: u32) -> bool {
        let mut current = self.last_seq.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if seq <= current {
                return false;
            }
            match self.last_seq.compare_exchange_weak(
                current,
                seq,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(seen) => current = seen,
            }
        }
    }
}

#[cfg(feature = "net")]
/// Fold one typed UDP signal into its stream state. Anything malformed or
/// addressed to an unknown token is dropped. A UDP cancel is advisory only:
/// it makes the node return `completed = false`, and the authoritative gRPC
/// stream carries the matching `StopStreamSearch`.
fn apply_stream_datagram(
    signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>,
    key: Option<&crate::security::UdpKey>,
    datagram: &[u8],
) {
    // With a key configured only a signed datagram is read at all
    // (docs/security.md); the plain frame is for loopback lanes.
    let (signal, seq) = match key {
        Some(key) => match crate::stream_signal::decode_signed(key, datagram) {
            Some((signal, seq)) => (Some(signal), Some(seq)),
            None => return,
        },
        None => (crate::stream_signal::decode(datagram), None),
    };
    let token = match signal {
        Some(crate::stream_signal::StreamSignal::RaiseFloor { token, .. })
        | Some(crate::stream_signal::StreamSignal::Cancel { token }) => token,
        None => return,
    };
    let state = signals
        .lock()
        .expect("stream signal registry poisoned")
        .get(&token)
        .cloned();
    let Some(state) = state else {
        return;
    };
    if let Some(seq) = seq {
        if !state.accept_seq(seq) {
            return;
        }
    }
    match signal.expect("validated above") {
        crate::stream_signal::StreamSignal::RaiseFloor { floor, .. } => {
            raise_floor_cell(&state.floor, floor);
        }
        crate::stream_signal::StreamSignal::Cancel { .. } => {
            state
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Kernel batch width: turbovec's multi-query scan scores up to four
/// queries per pass over each block, so batches beyond four stop
/// amortizing memory traffic.
const MAX_COALESCE: usize = 4;

static SCAN_BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SCAN_BATCHED_JOBS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Process-wide coalescing telemetry: `(batches formed, jobs in them)`.
/// Jobs exceeding batches means multi-query batches actually formed —
/// the observable that coalescing engaged, used by tests and benchmarks.
pub fn scan_batch_counters() -> (u64, u64) {
    (
        SCAN_BATCHES.load(std::sync::atomic::Ordering::Relaxed),
        SCAN_BATCHED_JOBS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// One shard scan queued for a batched kernel pass.
/// One completed shard scan, with the handshake flags that let the
/// coordinator refuse a filter column no shard resolves.
struct ScanOutcome {
    hits: Vec<ChunkHit>,
    stats: ScanStats,
    geo_columns_known: Vec<bool>,
    filter_columns_known: Vec<bool>,
}

struct ScanJob {
    vector: Vec<f32>,
    k: usize,
    tie_complete: bool,
    /// The request's filters, resolved into an allowlist by the batch
    /// runner under the SAME read guard the scan holds, so the columns
    /// and the index a scan sees are one snapshot.
    geo_filters: Vec<crate::pb::GeoFilter>,
    geo_regions: Vec<crate::geo::GeoRegion>,
    filter: Option<crate::pb::FilterExpr>,
    /// Polled between chunks for the best coordinator-pushed floor
    /// (returns `None` when floor sharing is off or no floor arrived).
    external: Box<dyn FnMut() -> Option<f32> + Send>,
    /// Receives this query's k-th-best raises (the caller bakes in the
    /// share gate and delta filter).
    publish: Box<dyn FnMut(f32) -> bool + Send>,
    done: tokio::sync::oneshot::Sender<Result<ScanOutcome, Status>>,
}

/// Batch former: one scan slot at a time per permit, and every job that
/// queued while all slots were busy coalesces into the next drain. Under
/// light load batches are singletons and scans run as parallel as before;
/// under heavy load freed slots pick up to [`MAX_COALESCE`] waiting
/// queries and score them in one pass over the packed codes.
async fn scan_scheduler(
    state: Arc<std::sync::RwLock<ShardState>>,
    chunk_blocks: usize,
    parallel: usize,
    mut jobs: mpsc::Receiver<ScanJob>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(parallel.max(1)));
    loop {
        // A slot first, then the batch: the wait for a slot is exactly
        // when coalescable jobs accumulate.
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .expect("scan semaphore never closes");
        let Some(first) = jobs.recv().await else {
            break;
        };
        let mut batch = vec![first];
        while batch.len() < MAX_COALESCE {
            match jobs.try_recv() {
                Ok(job) => batch.push(job),
                Err(_) => break,
            }
        }
        SCAN_BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SCAN_BATCHED_JOBS.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let state = state.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = permit;
            run_scan_batch(&state, chunk_blocks, batch);
        });
    }
}

/// Run one batched scan under the shard read lock and deliver every job's
/// result. Blocking-pool context.
fn run_scan_batch(state: &std::sync::RwLock<ShardState>, chunk_blocks: usize, batch: Vec<ScanJob>) {
    let guard = state.read().expect("shard state lock poisoned");
    let index = match guard.index.as_ref() {
        Some(index) => index,
        None => {
            for job in batch {
                let _ = job.done.send(Err(Status::failed_precondition(
                    "shard has no index yet (set calibration or add vectors)",
                )));
            }
            return;
        }
    };
    // Re-validate dimensions against the CURRENT index: the shard may have
    // been swapped (InstallSnapshot) between the RPC's validation and this
    // batch winning a slot.
    let dim = index.dim_opt();
    let slots = index.len();
    let mut specs: Vec<(Vec<f32>, usize, bool)> = Vec::with_capacity(batch.len());
    let mut allows: Vec<Option<Vec<bool>>> = Vec::with_capacity(batch.len());
    let mut knowns: Vec<(Vec<bool>, Vec<bool>)> = Vec::with_capacity(batch.len());
    let mut externals: Vec<Box<dyn FnMut() -> Option<f32> + Send>> = Vec::new();
    let mut publishers: Vec<Box<dyn FnMut(f32) -> bool + Send>> = Vec::new();
    let mut dones = Vec::new();
    for job in batch {
        if Some(job.vector.len()) != dim {
            let _ = job.done.send(Err(Status::failed_precondition(format!(
                "query dim {} no longer matches the index",
                job.vector.len()
            ))));
            continue;
        }
        let resolved = resolve_shard_filters(
            guard.bm25.as_ref(),
            guard.live_docs.words(),
            slots,
            &job.geo_filters,
            &job.geo_regions,
            job.filter.as_ref(),
        );
        let allow = match resolved {
            Ok((_, allow)) => allow,
            Err(e) => {
                let _ = job.done.send(Err(e));
                continue;
            }
        };
        knowns.push(filter_known_flags(
            guard.bm25.as_ref(),
            &job.geo_filters,
            job.filter.as_ref(),
        ));
        allows.push(allow);
        specs.push((job.vector, job.k, job.tie_complete));
        externals.push(job.external);
        publishers.push(job.publish);
        dones.push(job.done);
    }
    if dones.is_empty() {
        return;
    }
    let queries: Vec<BatchQuery> = specs
        .iter()
        .zip(&allows)
        .map(|((vector, k, keep_ties), allow)| BatchQuery {
            vector,
            k: *k,
            keep_ties: *keep_ties,
            allow: allow.as_deref(),
        })
        .collect();
    let results = chunked_topk_batch(
        index,
        &queries,
        chunk_blocks,
        &mut |qi| (externals[qi])(),
        &mut |qi, floor| (publishers[qi])(floor),
    );
    for ((done, (hits, stats)), (geo_columns_known, filter_columns_known)) in
        dones.into_iter().zip(results).zip(knowns)
    {
        crate::metrics::record_scan(&stats);
        let _ = done.send(Ok(ScanOutcome {
            hits,
            stats,
            geo_columns_known,
            filter_columns_known,
        }));
    }
}

/// RAII release for the ingest gate.
struct IngestGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for IngestGuard {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

impl NodeServiceImpl {
    /// Open a local shard using the same persisted-generation rules as the
    /// network server. A missing vector/BM25 image starts an empty shard;
    /// an unfinished BM25 spill is refused unless the caller explicitly
    /// allows a vector-only recovery.
    pub fn open(
        config: NodeConfig,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
        allow_missing_bm25: bool,
    ) -> Result<Self, String> {
        let generation = config.index_path.as_ref().and_then(|path| {
            recover_segments_swap(path);
            recover_generation(path)
        });

        let mut index = match config.index_path.as_ref() {
            Some(index_path) => {
                let path = generation
                    .as_ref()
                    .map_or_else(|| index_path.clone(), |dir| generation_vector(dir));
                if path.exists() {
                    let mut loaded = VectorIndex::load(&config.vector_backend, &path)
                        .map_err(|error| format!("load {}: {error}", path.display()))?;
                    loaded
                        .prepare()
                        .map_err(|error| format!("prepare {}: {error}", path.display()))?;
                    Some(loaded)
                } else {
                    None
                }
            }
            None => None,
        };

        let mut exact_vectors = None;
        let mut bm25 = None;
        let mut live_docs = LiveDocs::default();
        if let Some(index_path) = config.index_path.as_ref() {
            let (_, exact_path, bm25_path) = storage_paths(index_path, generation.as_ref());
            if exact_path.exists() {
                exact_vectors = Some(
                    ExactVectorStore::open(&exact_path)
                        .map_err(|error| format!("load {}: {error}", exact_path.display()))?,
                );
            }
            let root = segments_root(index_path);
            let catalog_exists = root.join("segments.json").exists();
            if bm25_path.exists() {
                if catalog_exists && generation.is_none() {
                    return Err(format!(
                        "{} has both a single-image file and a segment catalog at {}; a shard has \
                         one layout, and nothing converts on open",
                        bm25_path.display(),
                        root.display()
                    ));
                }
                bm25 = Some(
                    Bm25Shard::open(&bm25_path)
                        .map_err(|error| format!("load {}: {error}", bm25_path.display()))?,
                );
            } else if generation.is_none()
                && (catalog_exists
                    || (config.layout == Layout::Segments
                        && !bm25_build_dir(&bm25_path).exists()
                        && index.is_none()))
            {
                // The segment layout: an existing catalog, or a fresh
                // persisted shard under the default. Never a conversion —
                // a single-image file above took the other branch.
                let tail = heap_store(&config)?;
                let shard = SegmentedShard::open_with(&root, tail, config.vector_load())
                    .map_err(|error| format!("segment catalog {}: {error}", root.display()))?;
                let set = shard.snapshot().clone();
                if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                    let backend = first
                        .backend_config()
                        .map_err(|error| format!("segment vector backend: {error}"))?;
                    let dim = first
                        .dim_opt()
                        .ok_or_else(|| "segment vector image has no dimension".to_string())?;
                    // The whole-shard FP32 sidecar is written at a flush;
                    // a node that stopped without one (a refused shutdown
                    // flush, a crash) has its sealed rows' FP32 files in
                    // the segments, so the sidecar is rebuilt from them
                    // here rather than refusing the next append by name.
                    let sealed_rows: usize = (0..set.len())
                        .filter(|i| set.vector(*i).is_some())
                        .map(|i| set.metadata(i).rows as usize)
                        .sum();
                    let held = exact_vectors.as_ref().map_or(0, ExactVectorStore::len);
                    if held != sealed_rows {
                        let parts: Vec<PathBuf> = (0..set.len())
                            .filter(|i| set.vector(*i).is_some())
                            .map(|i| {
                                root.join("segments")
                                    .join(&set.metadata(i).segment_id)
                                    .join(&set.metadata(i).exact_vectors.file)
                            })
                            .collect();
                        let part_refs: Vec<&Path> = parts.iter().map(PathBuf::as_path).collect();
                        eprintln!(
                            "exact-vector sidecar {} holds {held} rows against {sealed_rows} \
                             sealed; rebuilding it from {} segment files",
                            exact_path.display(),
                            parts.len()
                        );
                        exact_vectors = Some(
                            ExactVectorStore::write_concatenated(dim, &part_refs, &exact_path)
                                .map_err(|error| {
                                    format!("rebuild {}: {error}", exact_path.display())
                                })?,
                        );
                    }
                    let tail_image = VectorIndex::from_backend_config(dim, &backend)
                        .map_err(|error| format!("segment tail image: {error}"))?;
                    let provider = SegmentedProvider::open(set, tail_image)
                        .map_err(|error| format!("segment vectors: {error}"))?;
                    index = Some(VectorIndex::from_provider(provider));
                }
                bm25 = Some(Bm25Shard::Segmented(shard));
            } else {
                let build_dir = bm25_build_dir(&bm25_path);
                if build_dir.exists() && !allow_missing_bm25 {
                    return Err(format!(
                        "BM25 build directory {} exists but {} does not. The bulk build was \
                         interrupted; this shard would answer lexical queries with silence, \
                         which is indistinguishable from a corpus that genuinely lacks those \
                         terms. Re-run ingest for this shard, or pass --allow-missing-bm25 to \
                         serve it vector-only on purpose.",
                        build_dir.display(),
                        bm25_path.display()
                    ));
                }
            }
            let live_path = generation.as_ref().map_or_else(
                || live_docs_sidecar_path(index_path),
                |dir| generation_live_docs(dir),
            );
            if live_path.exists() {
                live_docs = LiveDocs::open(&live_path)
                    .map_err(|error| format!("load {}: {error}", live_path.display()))?;
            }
        }

        let service = Self::new(index, config)
            .with_bm25(bm25)
            .with_exact_vectors(exact_vectors)?
            .with_live_docs(live_docs)?
            .with_phrase_index(phrase_index)
            .with_generation(generation);
        Ok(service)
    }

    /// Wrap an optional preloaded index in a node service.
    pub fn new(index: Option<VectorIndex>, config: NodeConfig) -> Self {
        let wal = open_wal(index.as_ref(), &config);
        let vocab = open_vocab(&config);
        let rerank_parallel = resolved_rerank_parallel(config.rerank_parallel);
        Self {
            state: Arc::new(RwLock::new(ShardState {
                index,
                exact_vectors: None,
                bm25: None,
                live_docs: LiveDocs::default(),
                generation: None,
                wal,
                parents: None,
                mapped_binding: None,
                stats_epoch: 1,
                pending_compaction: None,
            })),
            ingest_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ingest_fence: Arc::new(std::sync::Mutex::new(None)),
            seal_lock: Arc::new(std::sync::Mutex::new(())),
            compacting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config,
            scan_jobs: Arc::new(std::sync::OnceLock::new()),
            rerank_slots: Arc::new(tokio::sync::Semaphore::new(rerank_parallel)),
            stream_signals: Arc::new(std::sync::Mutex::new(HashMap::new())),
            vocab,
            materialize_cache: Arc::new(std::sync::Mutex::new(None)),
            phrase_index: None,
            flush_notify: None,
        }
    }

    /// Wake `notify` after every flush (the node agent's report trigger).
    pub fn with_flush_notify(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.flush_notify = Some(notify);
        self
    }

    /// The shard's configuration.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Attach the process-wide phrase vocabulary. It is immutable and cheap
    /// to share across every shard in a `both` process.
    pub fn with_phrase_index(
        mut self,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    ) -> Self {
        self.phrase_index = phrase_index;
        self
    }

    /// The shard's vocabulary listener, when accumulation is enabled.
    pub fn vocab_listener(&self) -> Option<Arc<crate::vocab::VocabularyListener>> {
        self.vocab.clone()
    }

    /// Seal the live vocabulary window on graceful shutdown (the Rust
    /// counterpart of the Java listener's JVM shutdown hook). No-op when
    /// accumulation is off or the window is empty.
    pub fn snapshot_vocab_on_shutdown(&self) {
        if let Some(vocab) = &self.vocab {
            vocab.persist_on_shutdown();
        }
    }

    /// Bind the UDP signal lane on `addr` — the same host:port as the
    /// gRPC listener, UDP namespace — and fold typed floor/cancel datagrams
    /// into the matching stream state (see [`apply_stream_datagram`]). A
    /// failed bind only loses the fast lane: both signals still travel on
    /// every stream's authoritative gRPC leg.
    #[cfg(feature = "net")]
    pub fn spawn_floor_listener(&self, addr: std::net::SocketAddr) {
        let signals = Arc::clone(&self.stream_signals);
        let key = self.config.udp_hmac_key.clone();
        // Unsigned floors are accepted on loopback only: off loopback, a
        // forged floor could cut candidates, so the lane stays closed
        // without a key and the gRPC streams carry every signal.
        if key.is_none() && !crate::security::is_loopback(&addr) {
            eprintln!(
                "stream-signal UDP lane on {addr} stays closed: no --udp-hmac-key, and unsigned \
                 datagrams are accepted on loopback only; signals ride the gRPC streams"
            );
            return;
        }
        tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind(addr).await {
                Ok(socket) => socket,
                Err(e) => {
                    eprintln!(
                        "stream-signal UDP bind {addr}: {e}; signals ride the gRPC streams only"
                    );
                    return;
                }
            };
            let mut buf = [0u8; 64];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, _peer)) => apply_stream_datagram(&signals, key.as_ref(), &buf[..n]),
                    Err(_) => continue,
                }
            }
        });
    }

    /// The shared scan queue, spawning the scheduler on first use (RPC
    /// handlers guarantee a runtime here).
    fn scan_queue(&self) -> mpsc::Sender<ScanJob> {
        self.scan_jobs
            .get_or_init(|| {
                let (tx, rx) = mpsc::channel(4096);
                let parallel = if self.config.scan_parallel > 0 {
                    self.config.scan_parallel
                } else {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(2)
                        .div_ceil(2)
                };
                tokio::spawn(scan_scheduler(
                    self.state.clone(),
                    self.config.chunk_blocks,
                    parallel,
                    rx,
                ));
                tx
            })
            .clone()
    }

    /// The builder shape for a fresh ingest: persisted shards bulk-build
    /// through the disk spiller (bounded heap, not searchable until
    /// Flush); path-less demo shards build in heap.
    fn new_builder(&self, generation: Option<&PathBuf>) -> Result<Bm25Shard, Status> {
        let names: Vec<&str> = self.config.bm25_fields.iter().map(String::as_str).collect();
        let facets: Vec<&str> = self
            .config
            .facet_fields
            .iter()
            .map(String::as_str)
            .collect();
        let numerics: Vec<&str> = self
            .config
            .numeric_fields
            .iter()
            .map(String::as_str)
            .collect();
        let map_facets: Vec<&str> = self
            .config
            .map_facet_fields
            .iter()
            .map(String::as_str)
            .collect();
        let map_numerics: Vec<&str> = self
            .config
            .map_numeric_fields
            .iter()
            .map(String::as_str)
            .collect();
        let integers: Vec<&str> = self
            .config
            .integer_fields
            .iter()
            .map(String::as_str)
            .collect();
        let geos: Vec<&str> = self.config.geo_fields.iter().map(String::as_str).collect();
        let positions: Vec<&str> = self
            .config
            .position_fields
            .iter()
            .map(String::as_str)
            .collect();
        for name in &positions {
            if !names.contains(name) {
                return Err(Status::failed_precondition(format!(
                    "positional field {name:?} is not in this shard's BM25 field table {names:?}"
                )));
            }
        }
        let sentences: Vec<&str> = self
            .config
            .sentence_fields
            .iter()
            .map(String::as_str)
            .collect();
        for name in &sentences {
            if !names.contains(name) {
                return Err(Status::failed_precondition(format!(
                    "sentence field {name:?} is not in this shard's BM25 field table {names:?}"
                )));
            }
        }
        match self.config.index_path.as_ref() {
            // A new persisted shard under the segment layout: the tail is a
            // heap store, searchable at once, sealed into the catalog on
            // flush (docs/immutable-segments.md).
            Some(p) if self.config.layout == Layout::Segments && generation.is_none() => {
                let root = segments_root(p);
                let tail = heap_store(&self.config).map_err(Status::failed_precondition)?;
                SegmentedShard::open_with(&root, tail, self.config.vector_load())
                    .map(Bm25Shard::Segmented)
                    .map_err(|e| {
                        Status::internal(format!("segment catalog {}: {e}", root.display()))
                    })
            }
            Some(p) => {
                let dir = bm25_build_dir(&storage_paths(p, generation).2);
                SpillBuilder::create_with_fields(&dir, &names)
                    .map(|b| {
                        Bm25Shard::Spilling(
                            b.with_facet_fields(&facets)
                                .with_numeric_fields(&numerics)
                                .with_map_facet_fields(&map_facets)
                                .with_map_numeric_fields(&map_numerics)
                                .with_integer_fields(&integers)
                                .with_geo_fields(&geos)
                                .with_position_fields(&positions)
                                .with_sentence_fields(&sentences),
                        )
                    })
                    .map_err(|e| Status::internal(format!("spill dir {}: {e}", dir.display())))
            }
            None => Ok(Bm25Shard::Building(
                Bm25Store::with_fields(&names)
                    .with_facets(&facets)
                    .with_numerics(&numerics)
                    .with_map_facets(&map_facets)
                    .with_map_numerics(&map_numerics)
                    .with_integers(&integers)
                    .with_geos(&geos)
                    .with_positions(&positions)
                    .with_sentences(&sentences),
            )),
        }
    }

    /// Fence ingest on this shard: every later ingest stream is refused
    /// naming `reason`; queries are unaffected. Idempotent.
    pub fn fence_ingest(&self, reason: String) {
        *self.ingest_fence.lock().expect("ingest fence lock") = Some(reason);
    }

    /// The fence reason, when the shard is fenced.
    pub fn ingest_fence(&self) -> Option<String> {
        self.ingest_fence.lock().expect("ingest fence lock").clone()
    }

    /// Claim the single-writer ingest gate, or refuse the stream.
    fn claim_ingest(&self) -> Result<IngestGuard, Status> {
        use std::sync::atomic::Ordering;
        if let Some(reason) = self.ingest_fence() {
            return Err(Status::failed_precondition(format!(
                "ingest is fenced on this shard: {reason}"
            )));
        }
        if self
            .ingest_busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(Status::failed_precondition(
                "another ingest stream is active on this shard",
            ));
        }
        Ok(IngestGuard(self.ingest_busy.clone()))
    }

    /// Attach a preloaded BM25 shard (from `<index path>.bm25`). The
    /// store's persisted mapped-plan binding becomes the shard's: the
    /// file IS the durable record of what its columns were written
    /// under.
    pub fn with_bm25(self, store: Option<Bm25Shard>) -> Self {
        {
            let mut guard = self.state.write().expect("shard state lock poisoned");
            guard.mapped_binding = store.as_ref().and_then(|s| s.binding().cloned());
            guard.bm25 = store;
            guard.stats_epoch += 1;
        }
        self
    }

    /// Attach the product-owned FP32 sidecar loaded at startup. The sidecar
    /// must describe exactly the provider slots in this shard generation.
    pub fn with_exact_vectors(self, store: Option<ExactVectorStore>) -> Result<Self, String> {
        {
            let mut guard = self.state.write().expect("shard state lock poisoned");
            if let Some(exact) = store.as_ref() {
                if let Some(index) = guard.index.as_ref() {
                    if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                        return Err(format!(
                            "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                            exact.dim(),
                            exact.len(),
                            index.dim_opt(),
                            index.len()
                        ));
                    }
                } else if guard
                    .bm25
                    .as_ref()
                    .is_some_and(|bm25| exact.len() != bm25.next_doc_id() as usize)
                {
                    return Err(
                        "exact-vector sidecar row count does not match product document rows"
                            .to_string(),
                    );
                }
            }
            guard.exact_vectors = store;
        }
        Ok(self)
    }

    /// Attach the persisted generation overlay loaded at startup.
    pub fn with_live_docs(self, live_docs: LiveDocs) -> Result<Self, String> {
        {
            let mut guard = self.state.write().expect("shard state lock poisoned");
            let rows = physical_rows(&guard);
            if live_docs.persisted_rows() > rows {
                return Err(format!(
                    "live-doc overlay describes {} rows but the shard has only {rows}",
                    live_docs.persisted_rows()
                ));
            }
            guard.live_docs = live_docs;
        }
        Ok(self)
    }

    /// Mark the shard as serving from a snapshot generation directory
    /// (startup found one via [`recover_generation`]): Flush and the
    /// AddDocuments reload path then read/write inside it.
    pub fn with_generation(self, dir: Option<PathBuf>) -> Self {
        self.state
            .write()
            .expect("shard state lock poisoned")
            .generation = dir;
        self
    }

    /// Build the tonic server for this service with explicit message size
    /// limits (see [`crate::MAX_MESSAGE_BYTES`]). tonic's 4 MiB default
    /// decoding cap is comfortably above even k=10000 shard responses
    /// (~160 KiB), but the limit is set explicitly so it never silently
    /// depends on a library default. NOTE: the cap also bounds AddVectors
    /// batch messages; clients should keep batches well under it.
    /// A metrics gauge sampler over this shard's live state
    /// (`docs/metrics.md`): called at scrape time, reads under the
    /// state lock, and so can never go stale.
    pub fn metrics_provider(&self) -> crate::metrics::GaugeProvider {
        let state = Arc::clone(&self.state);
        let slot_offset = self.config.slot_offset;
        Box::new(move || {
            let guard = state.read().expect("shard state lock poisoned");
            crate::metrics::ShardGauges {
                slot_offset,
                vectors: guard.index.as_ref().map_or(0, |i| i.len() as u64),
                documents: guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
                stats_epoch: guard.stats_epoch,
            }
        })
    }

    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Validate an incoming `StartShardSearch` against the index shape.
    /// turbovec panics on wrong-dim or non-finite queries; the service
    /// turns both into `INVALID_ARGUMENT` before the scan starts.
    /// The slot -> parent map for collapse scans: lineage `parent_id`
    /// per slot, or a high-bit-tagged global id for slots without
    /// lineage (self-parents; the tag keeps them disjoint from real
    /// opinion ids). Cached on the shard and rebuilt whenever the index
    /// length disagrees (append-only ingest makes length the only
    /// staleness signal; snapshot installs clear the cache explicitly).
    fn parent_map(
        state: &Arc<std::sync::RwLock<ShardState>>,
        slot_offset: u64,
        n: usize,
    ) -> Arc<Vec<u64>> {
        const SELF_PARENT_TAG: u64 = 1 << 63;
        {
            let guard = state.read().expect("shard state lock poisoned");
            if let Some(p) = guard.parents.as_ref() {
                if p.len() == n {
                    return Arc::clone(p);
                }
            }
        }
        let built = {
            let guard = state.read().expect("shard state lock poisoned");
            let store = guard.bm25.as_ref().and_then(|b| b.as_index());
            let mut parents = Vec::with_capacity(n);
            for slot in 0..n {
                let parent = store
                    .and_then(|s| s.lineage(slot as u32))
                    .map(|l| l.parent_id)
                    .unwrap_or(SELF_PARENT_TAG | (slot_offset + slot as u64));
                parents.push(parent);
            }
            Arc::new(parents)
        };
        state.write().expect("shard state lock poisoned").parents = Some(Arc::clone(&built));
        built
    }

    fn validate_start(index: &VectorIndex, start: &StartShardSearch) -> Result<(), Status> {
        let dim = index
            .dim_opt()
            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
        if start.vector.len() != dim {
            return Err(Status::invalid_argument(format!(
                "query vector has dim {}, index expects {dim}",
                start.vector.len()
            )));
        }
        if let Some((_, coord, value)) = first_invalid_coordinate(&start.vector, dim) {
            return Err(Status::invalid_argument(format!(
                "query coordinate {coord} is invalid: {value}"
            )));
        }
        Ok(())
    }

    /// Persist the index to its configured path, if any. Shared by the
    /// `Flush` RPC and save-on-shutdown in the binary.
    /// A new provider image for `dim`: wrapped as the tail of a
    /// segmented provider when the shard is segmented, so every row the
    /// node adds joins the catalog's positional space.
    fn fresh_index(&self, bm25: Option<&Bm25Shard>, dim: usize) -> Result<VectorIndex, Status> {
        let created = VectorIndex::create(&self.config.vector_backend, dim, self.config.bit_width)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        Self::adopt_layout(bm25, created)
    }

    /// A fresh, empty exact-vector store for this shard: on disk beside
    /// the generation's sidecar path when the shard persists, so an
    /// ingest never holds its FP32 rows in heap (`docs/mmap-vectors.md`);
    /// in memory for an in-memory shard.
    fn fresh_exact_store(
        &self,
        generation: Option<&PathBuf>,
        dim: usize,
    ) -> Result<ExactVectorStore, Status> {
        match self.config.index_path.as_ref() {
            Some(index_path) => {
                let (_, exact_path, _) = storage_paths(index_path, generation);
                ExactVectorStore::spilling(&exact_path, Some(dim)).map_err(|e| {
                    Status::internal(format!(
                        "exact-vector builder {}: {e}",
                        exact_path.display()
                    ))
                })
            }
            None => Ok(ExactVectorStore::empty(Some(dim))),
        }
    }

    /// A new, empty vector index in the shard's layout: the tail image of
    /// a segmented provider over the catalog's sealed images when the
    /// documents are segmented, else the index as created. Every path
    /// that creates the shard's first index goes through here, so a
    /// calibrated-then-ingested segmented shard seals its vectors like
    /// an ingested-then-calibrated one.
    fn adopt_layout(bm25: Option<&Bm25Shard>, created: VectorIndex) -> Result<VectorIndex, Status> {
        match bm25 {
            Some(Bm25Shard::Segmented(g)) => {
                let provider = SegmentedProvider::open(g.snapshot().clone(), created)
                    .map_err(|e| Status::failed_precondition(format!("{e}")))?;
                Ok(VectorIndex::from_provider(provider))
            }
            _ => Ok(created),
        }
    }

    /// Seal a segmented shard's tail into a new catalog segment
    /// (`docs/immutable-segments.md`) in three steps. Under the shard's
    /// write lock the tail freezes into a read-only part and a fresh
    /// tail starts after it. With no lock held, the frozen part's
    /// documents, vectors, FP32 rows, and live bitmap are written,
    /// hashed, fsynced, and appended to the catalog with one manifest
    /// swap. Under the write lock again the shard adopts the published
    /// snapshot. Queries serve the frozen part throughout and ingest
    /// continues into the new tail. Seals on one shard run one at a
    /// time, and a seal that failed after freezing is finished by the
    /// next attempt. The WAL is untouched: sealing changes the on-disk
    /// layout, not the log's history. Returns whether a segment was
    /// written.
    fn seal_tail(&self) -> Result<bool, Status> {
        let _one_at_a_time = self.seal_lock.lock().expect("seal lock poisoned");
        let Some(plan) = self.freeze_tail()? else {
            return Ok(false);
        };
        let outcome = self.write_segment(&plan);
        let _ = std::fs::remove_dir_all(&plan.stage);
        let published = outcome?;
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_mut() else {
            return Err(Status::internal(
                "the segmented shard changed layout while a seal was in flight",
            ));
        };
        shard
            .republish(published.clone())
            .map_err(|e| Status::internal(format!("republish segments: {e}")))?;
        if let Some(provider) = guard.index.as_mut().and_then(VectorIndex::as_segmented_mut) {
            provider
                .republish(published)
                .map_err(|e| Status::internal(format!("republish vectors: {e}")))?;
        }
        guard.stats_epoch += 1;
        drop(guard);
        // The frozen tail just went out of scope: hand its freed pages
        // back to the kernel. glibc keeps freed small chunks in the
        // arena that allocated them, and a long ingest touches a
        // different arena per stream, so without this a node's resident
        // set grows by a tail's worth per seal (measured 4 GB per
        // million rows; MALLOC_ARENA_MAX=2 in the launch environment
        // bounds the spread, this returns the rest).
        #[cfg(all(feature = "net", target_os = "linux", target_env = "gnu"))]
        // SAFETY: malloc_trim takes an integer pad and touches only the
        // allocator's own free lists; it has no preconditions.
        unsafe {
            libc::malloc_trim(0);
        }
        Ok(true)
    }

    /// Step one of a seal, under the write lock: freeze the tail (or
    /// pick up the frozen part a failed seal left) and gather what the
    /// writer needs. `None` when there is nothing to seal.
    fn freeze_tail(&self) -> Result<Option<SealPlan>, Status> {
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let ShardState {
            bm25,
            index,
            live_docs,
            mapped_binding,
            ..
        } = &mut *guard;
        let Some(Bm25Shard::Segmented(shard)) = bm25.as_mut() else {
            return Ok(None);
        };
        let provider = index.as_mut().and_then(VectorIndex::as_segmented_mut);
        let (base, rows, documents) = match shard.frozen() {
            Some((base, rows, store)) => (base as usize, rows as usize, Arc::clone(store)),
            None => {
                let base = shard.tail_base() as usize;
                let docs = shard.tail().next_doc_id() as usize;
                let vectors = provider.as_deref().map_or(0, |p| p.tail().len());
                if docs == 0 && vectors == 0 {
                    return Ok(None);
                }
                if docs != 0 && vectors != 0 && docs != vectors {
                    return Err(Status::failed_precondition(format!(
                        "the tail has {docs} documents and {vectors} vectors; a segment's \
                         artifacts cover the same rows, so ingest through the mapped path, \
                         which keeps them aligned, or run this shard with --layout=single-image"
                    )));
                }
                let fresh = heap_store(&self.config).map_err(Status::failed_precondition)?;
                shard.tail_mut().set_binding(mapped_binding.clone());
                let rows = docs.max(vectors);
                let store = shard
                    .freeze_tail(fresh, rows as u32)
                    .map_err(Status::internal)?;
                (base, rows, store)
            }
        };
        let image = match provider {
            Some(provider) => match provider.frozen() {
                Some((_, frozen_rows, image)) => (frozen_rows > 0).then(|| Arc::clone(image)),
                None => {
                    let held = provider.tail().len();
                    let backend = provider
                        .tail()
                        .backend_config()
                        .map_err(|e| Status::internal(format!("tail backend: {e}")))?;
                    let dim = provider
                        .tail()
                        .dim_opt()
                        .ok_or_else(|| Status::internal("segmented tail image has no dimension"))?;
                    let fresh = VectorIndex::from_backend_config(dim, &backend)
                        .map_err(|e| Status::internal(format!("fresh tail image: {e}")))?;
                    let image = provider
                        .freeze_tail(fresh, rows)
                        .map_err(|e| Status::internal(format!("freeze vectors: {e}")))?;
                    (held > 0).then_some(image)
                }
            },
            None => None,
        };
        let backend_kind = image
            .as_ref()
            .map(|i| i.descriptor().backend_kind)
            .unwrap_or_default();
        // The segment's id is its generation, the published epoch plus
        // one: monotone across seals AND across a compaction cutover,
        // which renumbers the catalog and would otherwise let a fresh
        // `seg-<count>` collide with a replaced directory the closing
        // flush has not retired yet (docs/mutations.md).
        let generation = shard.snapshot().epoch() + 1;
        Ok(Some(SealPlan {
            base,
            rows,
            documents,
            live: live_docs.slice(base, rows),
            image,
            backend_kind,
            catalog: shard.catalog().clone(),
            stage: shard.stage_dir(&format!("{generation:08}")),
            segment_id: format!("seg-{generation:08}"),
            generation,
        }))
    }

    /// Step two of a seal, with no lock held: write the frozen part's
    /// artifacts into the stage directory and append them to the
    /// catalog, which hashes, fsyncs, and publishes them. The FP32 rows
    /// are copied out of the exact-vector sidecar under a read lock (a
    /// sequential copy); their file is written, hashed, and fsynced
    /// after that lock is released.
    fn write_segment(
        &self,
        plan: &SealPlan,
    ) -> Result<Arc<crate::segments::OpenedSegmentSet>, Status> {
        let _ = std::fs::remove_dir_all(&plan.stage);
        std::fs::create_dir_all(&plan.stage)
            .map_err(|e| Status::internal(format!("stage {}: {e}", plan.stage.display())))?;
        let io = |what: &str, e: std::io::Error| Status::internal(format!("seal {what}: {e}"));
        let bm25_path = plan.stage.join("documents.bm25");
        plan.documents
            .save(&bm25_path)
            .map_err(|e| io("documents", e))?;
        let live_path = plan.stage.join("live-docs.bin");
        plan.live
            .write(&live_path, plan.rows as u64)
            .map_err(|e| io("live docs", e))?;
        let (vector_path, exact_path) = match plan.image.as_deref() {
            Some(image) => {
                let vector_path = plan.stage.join("vector.index");
                image
                    .write(&vector_path)
                    .map_err(|e| Status::internal(format!("seal vectors: {e}")))?;
                let (dim, values) = {
                    let guard = self.state.read().expect("shard state lock poisoned");
                    let exact = guard.exact_vectors.as_ref().ok_or_else(|| {
                        Status::failed_precondition(
                            "sealing a segment with vectors needs the exact-vector sidecar for \
                             its FP32 rows; the node has none",
                        )
                    })?;
                    if exact.len() < plan.base + plan.rows {
                        return Err(Status::failed_precondition(format!(
                            "the exact-vector sidecar has {} rows, fewer than the {} the seal \
                             covers",
                            exact.len(),
                            plan.base + plan.rows
                        )));
                    }
                    let dim = exact.dim().ok_or_else(|| {
                        Status::failed_precondition("exact-vector sidecar has no dim")
                    })?;
                    (dim, exact.row_values(plan.base, plan.base + plan.rows))
                };
                let exact_path = plan.stage.join("vectors.f32");
                ExactVectorStore::from_values(dim, values)
                    .and_then(|store| store.write(&exact_path))
                    .map_err(|e| io("exact rows", e))?;
                (Some(vector_path), Some(exact_path))
            }
            None => (None, None),
        };
        let source = crate::segments::SegmentSource {
            segment_id: &plan.segment_id,
            generation: plan.generation,
            base_label: plan.base as u64,
            backend_kind: &plan.backend_kind,
            vector_path: vector_path.as_deref(),
            exact_vector_path: exact_path.as_deref(),
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
        };
        plan.catalog
            .append(source)
            .map_err(|e| Status::internal(format!("seal segment: {e}")))
    }

    /// Whether the tail reached `--seal-tail-docs` (documents or
    /// vectors; the mapped path grows them together).
    fn tail_full(&self) -> bool {
        if self.config.seal_tail_docs == 0 || self.config.index_path.is_none() {
            return false;
        }
        let bound = self.config.seal_tail_docs as usize;
        let guard = self.state.read().expect("shard state lock poisoned");
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        let docs = shard.tail().next_doc_id() as usize;
        let vectors = guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .map_or(0, |p| p.tail().len());
        // A segment's artifacts cover the same rows, so a tail that holds
        // both documents and vectors seals only at a moment when the two
        // agree: the legacy two-call append (AddDocuments, then
        // AddVectors for the same rows) is between its calls otherwise,
        // and the seal waits for the vectors rather than sealing a
        // document-only segment the vectors could never join.
        if docs == 0 {
            vectors >= bound
        } else if guard.index.is_some() {
            // A provider is configured, so the documents' vectors are
            // coming; a document-only segment would leave them nowhere
            // to go (their call is refused by name), so the seal waits.
            docs == vectors && docs >= bound
        } else {
            docs >= bound
        }
    }

    /// Seal the tail when it reached the configured size, so a long
    /// ingest stays bounded without a flush. Blocking; the ingest loops
    /// call it through [`Self::seal_if_due`].
    fn seal_if_due_blocking(&self) -> Result<bool, Status> {
        if self.tail_full() {
            self.seal_tail()
        } else {
            Ok(false)
        }
    }

    /// The async face of the due-check for the ingest loops: the seal
    /// itself runs on a blocking thread, with the shard lock free.
    async fn seal_if_due(&self) -> Result<bool, Status> {
        if !self.tail_full() {
            return Ok(false);
        }
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.seal_if_due_blocking())
            .await
            .map_err(|e| Status::internal(format!("seal task failed: {e}")))?
    }

    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        let Some(config_path) = self.config.index_path.clone() else {
            let guard = self.state.read().expect("shard state lock poisoned");
            return Ok(FlushResponse {
                path: String::new(),
                num_vectors: guard.index.as_ref().map_or(0, |i| i.len() as u64),
                num_documents: guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
                written: false,
            });
        };
        // Log before data: fsync the WAL BEFORE the index images are
        // written, so a crash between the two leaves the log a superset
        // of the on-disk indexes — never the reverse. An index image
        // whose records the log lost would silently drop those records
        // from every future replay (reshard, recovery).
        {
            let mut guard = self.state.write().expect("shard state lock poisoned");
            if let Some(wal) = guard.wal.as_mut() {
                wal.flush().map_err(|e| {
                    Status::internal(format!("wal fsync {}: {e}", wal.dir().display()))
                })?;
            }
        }
        // A segmented shard seals its tail with the lock free (see
        // `seal_tail`); the single-image writes below hold it.
        let sealed = self.seal_tail()?;
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let num_vectors = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let (vector_path, exact_path, bm25_path) =
            storage_paths(&config_path, guard.generation.as_ref());
        let live_docs_path = live_docs_storage_path(&config_path, guard.generation.as_ref());
        if let Some(exact) = guard.exact_vectors.as_ref() {
            if let Some(index) = guard.index.as_ref() {
                if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                    return Err(Status::failed_precondition(format!(
                        "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                        exact.dim(),
                        exact.len(),
                        index.dim_opt(),
                        index.len()
                    )));
                }
            } else if guard
                .bm25
                .as_ref()
                .is_some_and(|bm25| exact.len() != bm25.next_doc_id() as usize)
            {
                return Err(Status::failed_precondition(
                    "exact-vector sidecar row count does not match product document rows",
                ));
            }
        }
        if let Some(index) = guard.index.as_ref() {
            if index.as_segmented().is_none() {
                index.write(&vector_path).map_err(|e| {
                    Status::internal(format!("write {}: {e}", vector_path.display()))
                })?;
            }
        }
        if let Some(exact) = guard.exact_vectors.as_ref() {
            let mapped = exact
                .write(&exact_path)
                .map_err(|e| Status::internal(format!("write {}: {e}", exact_path.display())))?;
            guard.exact_vectors = Some(mapped);
        }
        let physical_rows = physical_rows(&guard);
        guard.live_docs = guard
            .live_docs
            .write(&live_docs_path, physical_rows)
            .map_err(|e| Status::internal(format!("write {}: {e}", live_docs_path.display())))?;
        // Save the builder as v3 and immediately reopen it disk-resident:
        // after Flush a shard holds no postings or texts in heap.
        // Already-resident shards have nothing to write.
        // The binding persists with the store bytes (the kind-6 table
        // entry), so the flushed file carries exactly what the shard is
        // bound to.
        let binding = guard.mapped_binding.clone();
        let built = match guard.bm25.as_mut() {
            Some(Bm25Shard::Building(store)) => {
                store.set_binding(binding);
                store
                    .save(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            Some(Bm25Shard::Spilling(builder)) => {
                builder.set_binding(binding);
                builder
                    .finish(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            _ => false,
        };
        if built {
            guard.bm25 = Some(
                Bm25Reader::open(&bm25_path)
                    .map(Bm25Shard::Resident)
                    .map_err(|e| {
                        Status::internal(format!("reopen {}: {e}", bm25_path.display()))
                    })?,
            );
            // The stats are the same numbers after a flush, but a
            // Spilling shard was refusing TermStats until now — the
            // bump is what tells a cache to come look again.
            guard.stats_epoch += 1;
        }
        let written = sealed
            || guard.index.is_some()
            || guard.exact_vectors.is_some()
            || guard.bm25.is_some();
        // A compaction cutover commits here (docs/mutations.md): its
        // images are on disk now, so the marker that would roll it back
        // on restart goes, and the generation it replaced with it.
        if let Some(pending) = guard.pending_compaction.take() {
            pending
                .complete()
                .map_err(|e| Status::internal(format!("complete compaction cutover: {e}")))?;
        }
        // Durability point reached: the log was fsynced above, then the
        // indexes hit disk. The marker records that a flush happened;
        // its own fsync failing degrades the log rather than un-flushing
        // the indexes (they are already durable and consistent).
        wal_append_or_degrade(&mut guard.wal, wal_record::Op::Flush(FlushMarker {}));
        if let Some(wal) = guard.wal.as_mut() {
            if let Err(e) = wal.flush() {
                eprintln!("wal: post-flush marker fsync failed: {e}");
            }
        }
        if let Some(notify) = &self.flush_notify {
            notify.notify_one();
        }
        Ok(FlushResponse {
            path: vector_path.display().to_string(),
            num_vectors,
            num_documents,
            written,
        })
    }

    /// Receive one snapshot image into the staging generation directory
    /// (`vector.index`, `vectors.f32`, `documents.bm25`, and `live-docs.bin`
    /// when declared).
    /// The byte counts in the manifest split the stream; every file is synced
    /// before the caller swaps anything. Returns
    /// with the staging dir complete or not at all — on error the caller
    /// removes it.
    async fn receive_image(
        inbound: &mut Streaming<SnapshotChunk>,
        manifest: &SnapshotManifest,
        tmp_dir: &Path,
    ) -> Result<(), Status> {
        use tokio::io::AsyncWriteExt;
        let io_err = |what: &Path, e: std::io::Error| {
            Status::internal(format!("snapshot receive {}: {e}", what.display()))
        };
        tokio::fs::create_dir_all(tmp_dir)
            .await
            .map_err(|e| io_err(tmp_dir, e))?;
        let tv_tmp = generation_vector(tmp_dir);
        let exact_tmp = generation_exact_vectors(tmp_dir);
        let bm25_tmp = generation_bm25(tmp_dir);
        let live_tmp = generation_live_docs(tmp_dir);
        let mut tv = tokio::fs::File::create(&tv_tmp)
            .await
            .map_err(|e| io_err(&tv_tmp, e))?;
        let mut exact = if manifest.exact_vector_bytes > 0 {
            Some(
                tokio::fs::File::create(&exact_tmp)
                    .await
                    .map_err(|e| io_err(&exact_tmp, e))?,
            )
        } else {
            None
        };
        let mut bm25 = if manifest.bm25_bytes > 0 {
            Some(
                tokio::fs::File::create(&bm25_tmp)
                    .await
                    .map_err(|e| io_err(&bm25_tmp, e))?,
            )
        } else {
            None
        };
        let mut live = if manifest.live_docs_bytes > 0 {
            Some(
                tokio::fs::File::create(&live_tmp)
                    .await
                    .map_err(|e| io_err(&live_tmp, e))?,
            )
        } else {
            None
        };
        let (mut tv_written, mut exact_written, mut bm25_written, mut live_written) =
            (0u64, 0u64, 0u64, 0u64);
        while let Some(chunk) = inbound.message().await? {
            let Some(snapshot_chunk::Payload::Data(mut data)) = chunk.payload else {
                return Err(Status::invalid_argument(
                    "SnapshotChunk after the manifest must carry data",
                ));
            };
            // Fill the provider image first, then the exact-vector sidecar,
            // BM25, and the live-row overlay. A chunk may straddle any
            // boundary.
            let tv_take = (manifest.vector_bytes - tv_written).min(data.len() as u64) as usize;
            if tv_take > 0 {
                tv.write_all(&data[..tv_take])
                    .await
                    .map_err(|e| io_err(&tv_tmp, e))?;
                tv_written += tv_take as u64;
                data.drain(..tv_take);
            }
            let exact_take =
                (manifest.exact_vector_bytes - exact_written).min(data.len() as u64) as usize;
            if exact_take > 0 {
                let Some(sidecar) = exact.as_mut() else {
                    return Err(Status::invalid_argument(
                        "snapshot carries exact-vector data absent from its manifest",
                    ));
                };
                sidecar
                    .write_all(&data[..exact_take])
                    .await
                    .map_err(|e| io_err(&exact_tmp, e))?;
                exact_written += exact_take as u64;
                data.drain(..exact_take);
            }
            let bm25_take = (manifest.bm25_bytes - bm25_written).min(data.len() as u64) as usize;
            if bm25_take > 0 {
                let Some(sidecar) = bm25.as_mut() else {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                };
                sidecar
                    .write_all(&data[..bm25_take])
                    .await
                    .map_err(|e| io_err(&bm25_tmp, e))?;
                bm25_written += bm25_take as u64;
                data.drain(..bm25_take);
            }
            if !data.is_empty() {
                let Some(sidecar) = live.as_mut() else {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                };
                if live_written + data.len() as u64 > manifest.live_docs_bytes {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                }
                sidecar
                    .write_all(&data)
                    .await
                    .map_err(|e| io_err(&live_tmp, e))?;
                live_written += data.len() as u64;
            }
        }
        if tv_written != manifest.vector_bytes
            || exact_written != manifest.exact_vector_bytes
            || bm25_written != manifest.bm25_bytes
            || live_written != manifest.live_docs_bytes
        {
            return Err(Status::invalid_argument(format!(
                "truncated snapshot: received {tv_written}+{exact_written}+{bm25_written}+{live_written} of \
                 declared {}+{}+{}+{} bytes",
                manifest.vector_bytes,
                manifest.exact_vector_bytes,
                manifest.bm25_bytes,
                manifest.live_docs_bytes
            )));
        }
        tv.sync_all().await.map_err(|e| io_err(&tv_tmp, e))?;
        if let Some(sidecar) = exact.as_mut() {
            sidecar
                .sync_all()
                .await
                .map_err(|e| io_err(&exact_tmp, e))?;
        }
        if let Some(sidecar) = bm25.as_mut() {
            sidecar.sync_all().await.map_err(|e| io_err(&bm25_tmp, e))?;
        }
        if let Some(sidecar) = live.as_mut() {
            sidecar.sync_all().await.map_err(|e| io_err(&live_tmp, e))?;
        }
        Ok(())
    }

    /// Validate a received snapshot image and atomically swap it in (the
    /// blocking half of `InstallSnapshot`). Everything that can fail —
    /// loading the index, opening all sidecars, the scoring-identity check —
    /// happens BEFORE the swap, so a rejected install leaves the live
    /// shard and the on-disk generation untouched.
    ///
    /// The swap itself is one directory rename: every generation artifact
    /// travels inside the staging dir, so the files can never tear. Replacing
    /// an existing generation renames it aside first; the
    /// crash window between the two renames is covered by
    /// [`recover_generation`] at startup.
    fn apply_snapshot(
        &self,
        tmp_dir: &Path,
        with_exact_vectors: bool,
        with_bm25: bool,
        with_live_docs: bool,
    ) -> Result<InstallSnapshotResponse, Status> {
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path")
            .clone();
        let tv_tmp = generation_vector(tmp_dir);
        let exact_tmp = generation_exact_vectors(tmp_dir);
        let bm25_tmp = generation_bm25(tmp_dir);
        let live_tmp = generation_live_docs(tmp_dir);

        let loaded = VectorIndex::load(&self.config.vector_backend, &tv_tmp).map_err(|e| {
            Status::invalid_argument(format!("snapshot is not a valid vector backend image: {e}"))
        })?;
        // A snapshot installs only from the same provider state: kind and
        // scoring fingerprint (calibration included), or the fleet would
        // score one shard in another space (docs/mmap-vectors.md).
        {
            let incoming = loaded.descriptor();
            let guard = self.state.read().expect("shard state lock poisoned");
            let serving_kind = guard
                .index
                .as_ref()
                .map(|index| index.descriptor().backend_kind)
                .or_else(|| {
                    guard
                        .wal
                        .as_ref()
                        .map(|wal| wal.manifest().vector_backend.clone())
                        .filter(|kind| !kind.is_empty())
                });
            if let Some(kind) = serving_kind {
                if kind != incoming.backend_kind {
                    return Err(Status::failed_precondition(format!(
                        "snapshot image is a {:?} image but this shard serves {kind:?}; a \
                         snapshot installs only from the same provider",
                        incoming.backend_kind
                    )));
                }
            }
            if let Some(serving) = guard.index.as_ref().map(|index| index.descriptor()) {
                if serving.scoring_fingerprint != incoming.scoring_fingerprint {
                    return Err(Status::failed_precondition(format!(
                        "snapshot image scores under {}/{} but this shard serves {}/{}; a \
                         snapshot installs only from the same provider state (calibration \
                         included), or the fleet would score in two spaces",
                        incoming.backend_kind,
                        incoming.scoring_fingerprint,
                        serving.backend_kind,
                        serving.scoring_fingerprint
                    )));
                }
            }
        }
        if with_exact_vectors {
            let exact = ExactVectorStore::open(&exact_tmp).map_err(|e| {
                Status::invalid_argument(format!(
                    "snapshot sidecar is not a valid exact-vector store: {e}"
                ))
            })?;
            exact.verify_payload().map_err(|e| {
                Status::invalid_argument(format!(
                    "snapshot exact-vector integrity check failed: {e}"
                ))
            })?;
            if exact.len() != loaded.len() || exact.dim() != loaded.dim_opt() {
                return Err(Status::invalid_argument(format!(
                    "snapshot exact-vector shape {:?}x{} does not match provider shape {:?}x{}",
                    exact.dim(),
                    exact.len(),
                    loaded.dim_opt(),
                    loaded.len()
                )));
            }
        }
        let incoming_doc_rows = if with_bm25 {
            // Open-check the sidecar (and drop it again) before the swap;
            // the live shard re-opens from the generation dir.
            let store = Bm25Shard::open(&bm25_tmp).map_err(|e| {
                Status::invalid_argument(format!("snapshot sidecar is not a valid BM25 store: {e}"))
            })?;
            u64::from(store.next_doc_id())
        } else {
            0
        };
        let incoming_live_docs = if with_live_docs {
            LiveDocs::open(&live_tmp).map_err(|e| {
                Status::invalid_argument(format!("snapshot live-row overlay is invalid: {e}"))
            })?
        } else {
            LiveDocs::default()
        };
        if incoming_live_docs.persisted_rows() > (loaded.len() as u64).max(incoming_doc_rows) {
            return Err(Status::invalid_argument(
                "snapshot live-row overlay exceeds every aligned artifact's row count",
            ));
        }

        let mut guard = self.state.write().expect("shard state lock poisoned");
        // Scoring comparability: a shard with a locked backend configuration
        // only accepts an image with the identical scoring fingerprint.
        if let Some(index) = guard.index.as_ref() {
            let current = index.descriptor();
            let incoming = loaded.descriptor();
            if current.backend_kind != incoming.backend_kind
                || current.scoring_fingerprint != incoming.scoring_fingerprint
            {
                return Err(Status::failed_precondition(
                    "snapshot vector backend or scoring fingerprint differs from the \
                     generation locked on this shard; mixed native scores are not mergeable",
                ));
            }
        }

        if guard.pending_compaction.is_some() {
            return Err(Status::failed_precondition(
                "a compaction cutover is pending its closing flush on this shard; call Flush \
                 before installing a snapshot",
            ));
        }
        let snap = Self::adopt_generation(&path, tmp_dir, true)?;

        guard.bm25 = if with_bm25 {
            Some(Bm25Shard::open(&generation_bm25(&snap)).map_err(|e| {
                Status::internal(format!(
                    "open installed {}: {e}",
                    generation_bm25(&snap).display()
                ))
            })?)
        } else {
            // Wholesale replacement: an image without a sidecar replaces
            // any existing postings store (its ids would describe a
            // different corpus). The old store's files left with the old
            // generation.
            None
        };
        guard.exact_vectors = if with_exact_vectors {
            Some(
                ExactVectorStore::open(&generation_exact_vectors(&snap)).map_err(|e| {
                    Status::internal(format!(
                        "open installed {}: {e}",
                        generation_exact_vectors(&snap).display()
                    ))
                })?,
            )
        } else {
            None
        };
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let num_vectors = loaded.len() as u64;
        // Wholesale replace: the image's binding (usually none) is now
        // the shard's. A stale binding describing replaced columns
        // would lie.
        guard.mapped_binding = guard.bm25.as_ref().and_then(|b| b.binding().cloned());
        guard.index = Some(loaded);
        guard.live_docs = incoming_live_docs;
        guard.parents = None;
        guard.generation = Some(snap.clone());
        guard.stats_epoch += 1;
        Self::rotate_wal_after_install(
            &mut guard,
            &self.config,
            &path,
            (num_vectors, num_documents),
        )?;
        Ok(InstallSnapshotResponse {
            path: generation_vector(&snap).display().to_string(),
            num_vectors,
            num_documents,
            manifest: None,
        })
    }

    /// The snapshot supersedes the log: fsync and retire the current
    /// generation, open gen-(g+1) with the installed image's provider
    /// state (same bucket geometry), and mark where it came from.
    /// Records before this point describe the OLD shard contents.
    fn rotate_wal_after_install(
        guard: &mut ShardState,
        config: &NodeConfig,
        path: &Path,
        counts: (u64, u64),
    ) -> Result<(), Status> {
        if guard.wal.is_none() {
            return Ok(());
        }
        let source_generation = guard.wal.as_ref().map_or(0, WalWriter::generation);
        // The installed image is state this fresh log does NOT
        // contain: record it as preexisting so the reshard tool
        // refuses a log-only replay that would drop the image.
        let mut manifest =
            wal_manifest(guard.index.as_ref(), config, source_generation + 1, counts);
        let previous = guard.wal.as_ref().expect("checked above").manifest();
        manifest.bucket_bits = previous.bucket_bits;
        manifest.bucket_count = previous.bucket_count;
        let wal_err = |e: std::io::Error| Status::internal(format!("wal rotate: {e}"));
        let wal = guard.wal.as_mut().expect("checked above");
        wal.flush().map_err(wal_err)?;
        *wal = WalWriter::create(&wal::wal_dir(path), manifest).map_err(wal_err)?;
        wal.append(wal_record::Op::Snapshot(SnapshotMarker {
            source_generation,
        }))
        .map_err(wal_err)?;
        wal.flush().map_err(wal_err)?;
        Ok(())
    }

    // -----------------------------------------------------------------
    // Snapshot repository (docs/snapshots.md)
    // -----------------------------------------------------------------

    /// The layout the shard's files form: the segment catalog, or one
    /// image plus sidecars (an installed generation is always the latter).
    fn served_layout(guard: &ShardState) -> &'static str {
        if guard.generation.is_none()
            && (matches!(guard.bm25, Some(Bm25Shard::Segmented(_)))
                || guard
                    .index
                    .as_ref()
                    .is_some_and(|index| index.as_segmented().is_some()))
        {
            LAYOUT_SEGMENTS
        } else {
            LAYOUT_SINGLE_IMAGE
        }
    }

    /// Per-field analysis fingerprints of the BM25 store, in field-table
    /// order (empty without a store).
    pub fn analysis_fingerprints(&self) -> Vec<u64> {
        let guard = self.state.read().expect("shard state lock poisoned");
        Self::analysis_fingerprints_of(&guard)
    }

    fn analysis_fingerprints_of(guard: &ShardState) -> Vec<u64> {
        guard.bm25.as_ref().map_or_else(Vec::new, |bm25| {
            (0..bm25.field_count())
                .map(|f| bm25.analysis_fingerprint(f))
                .collect()
        })
    }

    /// Sealed immutable segments of the shard: the catalog's parts for
    /// the segment layout, one for a disk-resident single image, zero
    /// for a heap builder or an empty shard.
    pub fn immutable_segments(&self) -> u32 {
        let guard = self.state.read().expect("shard state lock poisoned");
        match guard.bm25.as_ref() {
            Some(Bm25Shard::Segmented(shard)) => shard.sealed_parts() as u32,
            Some(Bm25Shard::Resident(_)) => 1,
            _ => 0,
        }
    }

    /// `ExportSnapshot` (`docs/snapshots.md`): flush, then copy the
    /// current generation into `directory` under the shard's read lock
    /// and write the manifest beside it. The read lock is what makes the
    /// hashes, the row counts, and the WAL cutoff describe one state:
    /// queries proceed, writes wait for the copy. A shard with a WAL is
    /// copied only when the log holds nothing since the flush (a write
    /// that slips in between flushes again, a bounded number of times);
    /// a shard without one is copied under the write lock, because
    /// nothing else can tell whether its files are current.
    pub fn export_snapshot_blocking(
        &self,
        directory: &Path,
    ) -> Result<ExportSnapshotResponse, Status> {
        let index_path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "shard has no persistence path (index_path); a snapshot export needs one",
            )
        })?;
        if directory.as_os_str().is_empty() {
            return Err(Status::invalid_argument("ExportSnapshot needs a directory"));
        }
        std::fs::create_dir_all(directory).map_err(|e| {
            Status::internal(format!("create repository {}: {e}", directory.display()))
        })?;
        let entries = std::fs::read_dir(directory)
            .map_err(|e| Status::internal(format!("read {}: {e}", directory.display())))?
            .count();
        if entries > 0 {
            return Err(Status::invalid_argument(format!(
                "repository directory {} is not empty ({entries} entries); a snapshot is \
                 exported into an empty directory only",
                directory.display()
            )));
        }
        self.flush_index()?;
        let started = std::time::Instant::now();
        let mut attempts = 0u32;
        let (manifest, bytes) = loop {
            attempts += 1;
            let guard = self.state.read().expect("shard state lock poisoned");
            match guard.wal.as_ref() {
                Some(wal) if wal.is_dirty() => {
                    drop(guard);
                    if attempts >= 8 {
                        return Err(Status::aborted(format!(
                            "shard kept writing through {attempts} flushes; a snapshot copies \
                             a flushed generation, retry when ingest pauses"
                        )));
                    }
                    self.flush_index()?;
                    continue;
                }
                Some(_) => break self.export_locked(&guard, &index_path, directory)?,
                None => {
                    drop(guard);
                    let guard = self.state.write().expect("shard state lock poisoned");
                    break self.export_locked(&guard, &index_path, directory)?;
                }
            }
        };
        let copy_millis = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let (manifest_path, manifest_sha256) = repo::write_manifest(directory, &manifest)
            .map_err(|e| Status::internal(format!("write manifest: {e}")))?;
        crate::postings::fsync_parent(&manifest_path)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", directory.display())))?;
        Ok(ExportSnapshotResponse {
            manifest: Some(manifest.to_pb()),
            manifest_path: manifest_path.display().to_string(),
            manifest_sha256,
            copy_millis,
            bytes,
        })
    }

    /// The copy itself, under whichever lock the caller holds.
    fn export_locked(
        &self,
        guard: &ShardState,
        index_path: &Path,
        directory: &Path,
    ) -> Result<(RepositoryManifest, u64), Status> {
        let layout = Self::served_layout(guard);
        let mut sources: Vec<(String, PathBuf)> = Vec::new();
        if layout == LAYOUT_SEGMENTS {
            let root = segments_root(index_path);
            for (name, path) in repo::walk_files(&root)
                .map_err(|e| Status::internal(format!("walk {}: {e}", root.display())))?
            {
                sources.push((format!("{CATALOG_DIR}/{name}"), path));
            }
            for (name, path) in [
                ("vector.index", index_path.to_path_buf()),
                ("vectors.f32", exact_vector_sidecar_path(index_path)),
                ("live-docs.bin", live_docs_sidecar_path(index_path)),
            ] {
                if path.is_file() {
                    sources.push((name.to_string(), path));
                }
            }
        } else {
            let (vector, exact, bm25) = storage_paths(index_path, guard.generation.as_ref());
            let live = live_docs_storage_path(index_path, guard.generation.as_ref());
            for (name, path) in [
                ("vector.index", vector),
                ("vectors.f32", exact),
                ("documents.bm25", bm25),
                ("live-docs.bin", live),
            ] {
                if path.is_file() {
                    sources.push((name.to_string(), path));
                }
            }
        }
        let mut artifacts = Vec::with_capacity(sources.len());
        let mut bytes = 0u64;
        for (name, source) in &sources {
            let artifact = repo::copy_and_hash(source, &directory.join(name), name)
                .map_err(|e| Status::internal(format!("copy {}: {e}", source.display())))?;
            bytes += artifact.bytes;
            artifacts.push(artifact);
        }
        let mut synced = std::collections::BTreeSet::new();
        for artifact in &artifacts {
            let path = directory.join(&artifact.file);
            if let Some(parent) = path.parent() {
                if synced.insert(parent.to_path_buf()) {
                    crate::postings::fsync_parent(&path).map_err(|e| {
                        Status::internal(format!("fsync {}: {e}", parent.display()))
                    })?;
                }
            }
        }
        let (backend_kind, scoring_fingerprint, dim) = match guard.index.as_ref() {
            Some(index) => {
                let descriptor = index.descriptor();
                (
                    descriptor.backend_kind,
                    descriptor.scoring_fingerprint,
                    index.dim_opt().unwrap_or(0) as u32,
                )
            }
            None => (self.config.vector_backend.clone(), String::new(), 0),
        };
        let physical = physical_rows(guard);
        let deleted = guard.live_docs.deleted_count().min(physical);
        let (wal_generation, wal_high_watermark, wal_clocked) =
            guard.wal.as_ref().map_or((0, 0, false), |wal| {
                (
                    wal.generation(),
                    wal.high_watermark(),
                    !wal.has_legacy_clock_records(),
                )
            });
        Ok((
            RepositoryManifest {
                format_version: repo::FORMAT_VERSION,
                layout: layout.to_string(),
                backend_kind,
                scoring_fingerprint,
                dim,
                slot_offset: self.config.slot_offset,
                collection: self.config.collection.clone(),
                vector_rows: guard.index.as_ref().map_or(0, |i| i.len() as u64),
                document_rows: guard
                    .bm25
                    .as_ref()
                    .map_or(0, |b| u64::from(b.next_doc_id())),
                live_rows: physical - deleted,
                analysis_fingerprints: Self::analysis_fingerprints_of(guard),
                wal_generation,
                wal_high_watermark,
                wal_clocked,
                artifacts,
            },
            bytes,
        ))
    }

    /// Stage a repository directory into the staging generation dir:
    /// the manifest and every artifact it names, copied as they are.
    /// Verification happens in [`Self::install_staged_repository`].
    fn stage_from_directory(
        source: &Path,
        tmp_dir: &Path,
    ) -> Result<(RepositoryManifest, String), Status> {
        let (manifest, sha) = repo::read_manifest(source).map_err(|e| {
            Status::invalid_argument(format!("repository {}: {e}", source.display()))
        })?;
        let io_err = |what: &Path, e: std::io::Error| {
            Status::internal(format!("stage {}: {e}", what.display()))
        };
        std::fs::create_dir_all(tmp_dir).map_err(|e| io_err(tmp_dir, e))?;
        std::fs::copy(
            source.join(repo::MANIFEST_FILE),
            tmp_dir.join(repo::MANIFEST_FILE),
        )
        .map_err(|e| io_err(&source.join(repo::MANIFEST_FILE), e))?;
        for artifact in &manifest.artifacts {
            let from = source.join(&artifact.file);
            let to = tmp_dir.join(&artifact.file);
            if let Some(parent) = to.parent() {
                std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
            }
            std::fs::copy(&from, &to).map_err(|e| {
                Status::invalid_argument(format!(
                    "artifact {:?} is unreadable at {}: {e}",
                    artifact.file,
                    from.display()
                ))
            })?;
        }
        Ok((manifest, sha))
    }

    /// Verify a staged repository against its manifest and install it
    /// through the layout's install path (the single-image path is the
    /// same [`Self::apply_snapshot`] the client stream takes). Refuses,
    /// by name and before touching the live shard: a size or digest
    /// mismatch, another shard's slot offset or collection, a layout
    /// this shard cannot adopt.
    pub fn install_staged_repository(
        &self,
        tmp_dir: &Path,
        manifest: &RepositoryManifest,
    ) -> Result<InstallSnapshotResponse, Status> {
        repo::verify_artifacts(tmp_dir, manifest).map_err(Status::invalid_argument)?;
        if manifest.slot_offset != self.config.slot_offset {
            return Err(Status::failed_precondition(format!(
                "snapshot was exported from a shard at slot offset {}, this shard serves {}; \
                 a repository image installs only on the shard it describes",
                manifest.slot_offset, self.config.slot_offset
            )));
        }
        if manifest.collection != self.config.collection {
            return Err(Status::failed_precondition(format!(
                "snapshot belongs to collection {:?}, this shard serves {:?}",
                manifest.collection, self.config.collection
            )));
        }
        if manifest.layout == LAYOUT_SEGMENTS {
            return self.apply_segment_snapshot(tmp_dir, manifest);
        }
        if manifest.artifact("vector.index").is_none() {
            return Err(Status::failed_precondition(
                "snapshot has no provider image (vector.index); a single-image install needs one",
            ));
        }
        {
            let guard = self.state.read().expect("shard state lock poisoned");
            if Self::served_layout(&guard) == LAYOUT_SEGMENTS && physical_rows(&guard) > 0 {
                return Err(Status::failed_precondition(format!(
                    "this shard serves a segment catalog with {} rows; a single-image snapshot \
                     installs only on a single-image shard or an empty one (layouts do not mix)",
                    physical_rows(&guard)
                )));
            }
        }
        let mut response = self.apply_snapshot(
            tmp_dir,
            manifest.artifact("vectors.f32").is_some(),
            manifest.artifact("documents.bm25").is_some(),
            manifest.artifact("live-docs.bin").is_some(),
        )?;
        response.manifest = Some(manifest.to_pb());
        Ok(response)
    }

    /// Install a staged segment-catalog snapshot: validate the catalog
    /// and the shard-level sidecars, then swap the catalog directory into
    /// place with one rename (the previous catalog goes aside first, the
    /// crash window is covered by [`recover_segments_swap`]) and reopen
    /// the segmented shard over it. The shard-level exact-vector and
    /// live-row sidecars move after the catalog; a crash between the two
    /// leaves a catalog whose sidecars disagree with it, which the next
    /// open refuses by name rather than serving.
    fn apply_segment_snapshot(
        &self,
        tmp_dir: &Path,
        manifest: &RepositoryManifest,
    ) -> Result<InstallSnapshotResponse, Status> {
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path")
            .clone();
        if self.config.layout != Layout::Segments {
            return Err(Status::failed_precondition(
                "this shard is configured --layout=single-image; a segment-catalog snapshot \
                 installs only on a segment-layout shard",
            ));
        }
        {
            let guard = self.state.read().expect("shard state lock poisoned");
            if guard.generation.is_some() {
                return Err(Status::failed_precondition(
                    "this shard serves an installed single-image generation; a segment-catalog \
                     snapshot installs only on a segment-layout shard (layouts do not mix)",
                ));
            }
        }
        let catalog_tmp = tmp_dir.join(CATALOG_DIR);
        if !catalog_tmp.join("segments.json").exists() {
            return Err(Status::invalid_argument(
                "segment-layout snapshot has no catalog/segments.json",
            ));
        }
        let staged = crate::segments::OpenedSegmentSet::open_with(
            &catalog_tmp,
            self.config.vector_load(),
        )
        .map_err(|e| Status::invalid_argument(format!("snapshot catalog is invalid: {e}")))?;
        let incoming = (0..staged.len())
            .find_map(|i| staged.vector(i))
            .map(|vector| vector.descriptor());
        let vector_tmp = tmp_dir.join("vector.index");
        let exact_tmp = tmp_dir.join("vectors.f32");
        let live_tmp = tmp_dir.join("live-docs.bin");
        let plain_image = if vector_tmp.exists() {
            let mut loaded =
                VectorIndex::load(&self.config.vector_backend, &vector_tmp).map_err(|e| {
                    Status::invalid_argument(format!(
                        "snapshot is not a valid vector backend image: {e}"
                    ))
                })?;
            loaded
                .prepare()
                .map_err(|e| Status::invalid_argument(format!("snapshot image: {e}")))?;
            Some(loaded)
        } else {
            None
        };
        let incoming = incoming.or_else(|| plain_image.as_ref().map(|i| i.descriptor()));
        {
            let guard = self.state.read().expect("shard state lock poisoned");
            let serving_kind = guard
                .index
                .as_ref()
                .map(|index| index.descriptor().backend_kind)
                .or_else(|| {
                    guard
                        .wal
                        .as_ref()
                        .map(|wal| wal.manifest().vector_backend.clone())
                        .filter(|kind| !kind.is_empty())
                });
            if let (Some(kind), Some(incoming)) = (serving_kind, incoming.as_ref()) {
                if kind != incoming.backend_kind {
                    return Err(Status::failed_precondition(format!(
                        "snapshot image is a {:?} image but this shard serves {kind:?}; a \
                         snapshot installs only from the same provider",
                        incoming.backend_kind
                    )));
                }
            }
            if let (Some(serving), Some(incoming)) = (
                guard.index.as_ref().map(|index| index.descriptor()),
                incoming.as_ref(),
            ) {
                if serving.scoring_fingerprint != incoming.scoring_fingerprint {
                    return Err(Status::failed_precondition(format!(
                        "snapshot image scores under {}/{} but this shard serves {}/{}; a \
                         snapshot installs only from the same provider state (calibration \
                         included), or the fleet would score in two spaces",
                        incoming.backend_kind,
                        incoming.scoring_fingerprint,
                        serving.backend_kind,
                        serving.scoring_fingerprint
                    )));
                }
            }
        }
        if exact_tmp.exists() {
            let exact = ExactVectorStore::open(&exact_tmp).map_err(|e| {
                Status::invalid_argument(format!(
                    "snapshot sidecar is not a valid exact-vector store: {e}"
                ))
            })?;
            exact.verify_payload().map_err(|e| {
                Status::invalid_argument(format!(
                    "snapshot exact-vector integrity check failed: {e}"
                ))
            })?;
        }
        let incoming_live = if live_tmp.exists() {
            LiveDocs::open(&live_tmp).map_err(|e| {
                Status::invalid_argument(format!("snapshot live-row overlay is invalid: {e}"))
            })?
        } else {
            LiveDocs::default()
        };
        let staged_rows: u64 = staged
            .manifest()
            .segments
            .iter()
            .map(|segment| segment.rows)
            .sum::<u64>()
            .max(plain_image.as_ref().map_or(0, |i| i.len() as u64));
        if incoming_live.persisted_rows() > staged_rows {
            return Err(Status::invalid_argument(
                "snapshot live-row overlay exceeds every aligned artifact's row count",
            ));
        }
        drop(staged);
        drop(plain_image);

        let mut guard = self.state.write().expect("shard state lock poisoned");
        if let (Some(index), Some(incoming)) = (guard.index.as_ref(), incoming.as_ref()) {
            let current = index.descriptor();
            if current.backend_kind != incoming.backend_kind
                || current.scoring_fingerprint != incoming.scoring_fingerprint
            {
                return Err(Status::failed_precondition(
                    "snapshot vector backend or scoring fingerprint differs from the \
                     generation locked on this shard; mixed native scores are not mergeable",
                ));
            }
        }
        let root = segments_root(&path);
        let old = segments_old_dir(&path);
        crate::postings::fsync_parent(&catalog_tmp.join("segments.json"))
            .map_err(|e| Status::internal(format!("fsync staging {}: {e}", tmp_dir.display())))?;
        let _ = std::fs::remove_dir_all(&old);
        if root.exists() {
            std::fs::rename(&root, &old)
                .map_err(|e| Status::internal(format!("retire {}: {e}", old.display())))?;
        }
        if let Err(e) = std::fs::rename(&catalog_tmp, &root) {
            if old.exists() && !root.exists() {
                let _ = std::fs::rename(&old, &root);
            }
            return Err(Status::internal(format!("install {}: {e}", root.display())));
        }
        for (tmp, dst) in [
            (vector_tmp, path.clone()),
            (exact_tmp, exact_vector_sidecar_path(&path)),
            (live_tmp, live_docs_sidecar_path(&path)),
        ] {
            if tmp.exists() {
                std::fs::rename(&tmp, &dst)
                    .map_err(|e| Status::internal(format!("install {}: {e}", dst.display())))?;
            } else if dst.exists() {
                std::fs::remove_file(&dst)
                    .map_err(|e| Status::internal(format!("retire {}: {e}", dst.display())))?;
            }
        }
        let _ = std::fs::remove_dir_all(&old);
        let _ = std::fs::remove_dir_all(tmp_dir);
        crate::postings::fsync_parent(&root)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", root.display())))?;

        // Reopen over the installed files, the way `open` does at start.
        let tail = heap_store(&self.config).map_err(Status::failed_precondition)?;
        let shard = SegmentedShard::open_with(&root, tail, self.config.vector_load())
            .map_err(|e| Status::internal(format!("open installed catalog: {e}")))?;
        let set = shard.snapshot().clone();
        let index = if path.exists() {
            let mut loaded = VectorIndex::load(&self.config.vector_backend, &path)
                .map_err(|e| Status::internal(format!("open installed {}: {e}", path.display())))?;
            loaded
                .prepare()
                .map_err(|e| Status::internal(format!("prepare {}: {e}", path.display())))?;
            Some(loaded)
        } else if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
            let backend = first
                .backend_config()
                .map_err(|e| Status::internal(format!("segment vector backend: {e}")))?;
            let dim = first
                .dim_opt()
                .ok_or_else(|| Status::internal("segment vector image has no dimension"))?;
            let tail_image = VectorIndex::from_backend_config(dim, &backend)
                .map_err(|e| Status::internal(format!("segment tail image: {e}")))?;
            let provider = SegmentedProvider::open(set, tail_image)
                .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
            Some(VectorIndex::from_provider(provider))
        } else {
            None
        };
        let exact_path = exact_vector_sidecar_path(&path);
        let exact_vectors = if exact_path.exists() {
            Some(ExactVectorStore::open(&exact_path).map_err(|e| {
                Status::internal(format!("open installed {}: {e}", exact_path.display()))
            })?)
        } else {
            None
        };
        let live_path = live_docs_sidecar_path(&path);
        let live_docs = if live_path.exists() {
            LiveDocs::open(&live_path).map_err(|e| {
                Status::internal(format!("open installed {}: {e}", live_path.display()))
            })?
        } else {
            LiveDocs::default()
        };
        let num_documents = shard.doc_count();
        let num_vectors = index.as_ref().map_or(0, |i| i.len() as u64);
        guard.mapped_binding = shard.binding().cloned();
        guard.bm25 = Some(Bm25Shard::Segmented(shard));
        guard.index = index;
        guard.exact_vectors = exact_vectors;
        guard.live_docs = live_docs;
        guard.parents = None;
        guard.generation = None;
        guard.stats_epoch += 1;
        Self::rotate_wal_after_install(
            &mut guard,
            &self.config,
            &path,
            (num_vectors, num_documents),
        )?;
        Ok(InstallSnapshotResponse {
            path: root.display().to_string(),
            num_vectors,
            num_documents,
            manifest: Some(manifest.to_pb()),
        })
    }

    /// The atomic generation swap shared by snapshot install and
    /// compaction cutover (`docs/mutations.md`): the previous generation
    /// aside (if any), the staged directory into place, both inside ONE
    /// directory rename each. The staged dir's own entries are fsynced
    /// first (the files' sync_all covered bytes and inodes, not the names
    /// pointing at them), and the parent is fsynced after so the swap
    /// itself survives a crash; the window between the two renames is
    /// covered by [`recover_generation`]. With `retire_old` the previous
    /// generation is removed at once (a snapshot install); a compaction
    /// keeps it until its closing flush and removes it then. Returns the
    /// active generation directory.
    pub(crate) fn adopt_generation(
        index_path: &Path,
        staged: &Path,
        retire_old: bool,
    ) -> Result<PathBuf, Status> {
        let snap = generation_dir(index_path);
        let old = generation_old_dir(index_path);
        crate::postings::fsync_parent(&generation_vector(staged))
            .map_err(|e| Status::internal(format!("fsync staging {}: {e}", staged.display())))?;
        if snap.exists() {
            std::fs::rename(&snap, &old)
                .map_err(|e| Status::internal(format!("retire {}: {e}", old.display())))?;
        }
        if let Err(e) = std::fs::rename(staged, &snap) {
            // Best-effort rollback so startup recovery sees a clean state.
            if old.exists() && !snap.exists() {
                let _ = std::fs::rename(&old, &snap);
            }
            return Err(Status::internal(format!("install {}: {e}", snap.display())));
        }
        if retire_old {
            let _ = std::fs::remove_dir_all(&old);
        }
        crate::postings::fsync_parent(&snap)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", snap.display())))?;
        Ok(snap)
    }

    /// Apply one backend-owned configuration to an empty shard.
    fn apply_backend_config(&self, req: ConfigureVectorBackendRequest) -> Result<bool, Status> {
        let dim = req.dim as usize;
        if dim == 0 {
            return Err(Status::invalid_argument(
                "vector dimension must be positive",
            ));
        }
        let config = internal_backend_config(
            req.config
                .ok_or_else(|| Status::invalid_argument("vector backend config is required"))?,
        )?;
        let build = || {
            VectorIndex::from_backend_config(dim, &config)
                .map_err(|e| Status::invalid_argument(format!("invalid backend config: {e}")))
        };
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let result = match guard.index.as_ref() {
            Some(index) if !index.is_empty() => Err(Status::failed_precondition(format!(
                "shard holds {} vectors; vector backend configuration is locked for the generation",
                index.len()
            ))),
            Some(index) => {
                let existing = index
                    .backend_config()
                    .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
                if index.dim_opt() == Some(dim) && existing == config {
                    return Ok(true);
                }
                Err(Status::already_exists(
                    "a different vector backend configuration is already locked on this shard",
                ))
            }
            None => {
                let built = build()?;
                let adopted = Self::adopt_layout(guard.bm25.as_ref(), built)?;
                guard.index = Some(adopted);
                Ok(false)
            }
        };
        if result.is_ok() {
            if guard.exact_vectors.is_none()
                && guard.index.as_ref().is_some_and(VectorIndex::is_empty)
            {
                let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
                guard.exact_vectors = Some(store);
            }
            if let Some(wal) = guard.wal.as_mut() {
                wal.update_manifest(|manifest| {
                    manifest.dim = dim as u32;
                    manifest.set_backend_config(config.clone());
                });
            }
        }
        result
    }

    /// Apply one legacy calibration request through the generic provider
    /// configuration path.
    fn apply_calibration(&self, req: &SetCalibrationRequest) -> Result<bool, Status> {
        let dim = req.dim as usize;
        if req.shift.len() != dim || req.scale.len() != dim {
            return Err(Status::invalid_argument(format!(
                "invalid calibration: shift/scale hold {}/{} values for dim {dim}",
                req.shift.len(),
                req.scale.len()
            )));
        }
        let config = embedded_turbovec_config(req.bit_width as usize, &req.shift, &req.scale)
            .map_err(|e| Status::invalid_argument(format!("invalid calibration: {e}")))?;
        self.apply_backend_config(ConfigureVectorBackendRequest {
            dim: req.dim,
            config: Some(wire_backend_config(&config)),
        })
    }

    /// Apply one ingested batch under the write lock. Returns
    /// `(added, global id of the batch's first vector)`.
    fn apply_batch(
        &self,
        batch: AddVectorsRequest,
        stable_routing_key: Option<Vec<u8>>,
    ) -> Result<(u64, u64), Status> {
        let mut guard = self.state.write().expect("shard state lock poisoned");
        self.apply_batch_locked(&mut guard, batch, stable_routing_key)
    }

    /// [`Self::apply_batch`] against a state the caller holds: the live
    /// shard under its write lock, or the shadow of a compaction that
    /// tails the same records into its own generation
    /// (`docs/mutations.md`). One apply function, so the shadow cannot
    /// drift from what ingest does.
    pub(crate) fn apply_batch_locked(
        &self,
        guard: &mut ShardState,
        batch: AddVectorsRequest,
        stable_routing_key: Option<Vec<u8>>,
    ) -> Result<(u64, u64), Status> {
        if batch.vectors.is_empty() {
            return Ok((0, 0));
        }
        let known_dim = guard.index.as_ref().and_then(|i| i.dim_opt());
        let dim = if batch.dim != 0 {
            let d = batch.dim as usize;
            if let Some(known) = known_dim {
                if known != d {
                    return Err(Status::invalid_argument(format!(
                        "batch dim {d} does not match shard dim {known}"
                    )));
                }
            }
            d
        } else {
            known_dim.ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no index or calibration yet; set calibration first or pass dim",
                )
            })?
        };
        if !batch.vectors.len().is_multiple_of(dim) {
            return Err(Status::invalid_argument(format!(
                "batch of {} floats is not a multiple of dim {dim}",
                batch.vectors.len()
            )));
        }
        if stable_routing_key.is_some() && batch.vectors.len() / dim != 1 {
            return Err(Status::invalid_argument(
                "a replication stable-key metadata value may carry exactly one vector",
            ));
        }
        if let Some((vi, ci, v)) = first_invalid_coordinate(&batch.vectors, dim) {
            return Err(Status::invalid_argument(format!(
                "invalid input value at vector {vi}, coord {ci}: {v}"
            )));
        }
        let (first_id, index_bit_width, index_len) = {
            let index = match guard.index.as_mut() {
                Some(index) => index,
                None => {
                    // From-scratch: create the configured provider. Explicit
                    // provisioning is the multi-shard path; this remains the
                    // single-shard convenience path.
                    let created = self.fresh_index(guard.bm25.as_ref(), dim)?;
                    guard.index = Some(created);
                    guard.index.as_mut().expect("just constructed")
                }
            };
            (
                self.config.slot_offset + index.len() as u64,
                index.bits_per_dimension().unwrap_or(self.config.bit_width),
                index.len(),
            )
        };
        if guard.exact_vectors.is_none() {
            if index_len != 0 {
                return Err(Status::failed_precondition(format!(
                    "the shard has {index_len} provider vectors but no exact-vector sidecar; \
                     rebuild or backfill the generation before appending"
                )));
            }
            let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
            guard.exact_vectors = Some(store);
        }
        let exact = guard.exact_vectors.as_ref().expect("ensured above");
        if exact.len() != index_len || exact.dim() != Some(dim) {
            // On the segment layout the provider's length counts every
            // catalog row, sealed document-only rows included: vectors
            // arriving after such a seal have no segment to join.
            let tail_len = guard
                .index
                .as_ref()
                .and_then(VectorIndex::as_segmented)
                .map_or(index_len, |p| p.tail().len());
            let sealed_without_vectors = index_len
                .saturating_sub(tail_len)
                .saturating_sub(exact.len());
            if sealed_without_vectors > 0 {
                return Err(Status::failed_precondition(format!(
                    "{sealed_without_vectors} rows are sealed in segments without vectors; \
                     vectors seal with their documents, so send AddVectors for each \
                     AddDocuments batch before the next (or ingest through IngestMapped), \
                     or run this shard with --layout=single-image"
                )));
            }
            return Err(Status::failed_precondition(format!(
                "exact-vector sidecar shape {:?}x{} does not match provider shape {dim}x{index_len}",
                exact.dim(),
                exact.len()
            )));
        }
        // Apply first, log after, under this one lock. A failed apply
        // must never reach the log: its assigned ids would be reused by
        // the next batch and the duplicate would poison every replay.
        // Durability is unaffected — both sides are volatile until
        // Flush, which fsyncs the log BEFORE the index images.
        guard
            .index
            .as_mut()
            .expect("constructed or present above")
            .add(&batch.vectors, dim)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        guard
            .exact_vectors
            .as_mut()
            .expect("validated above")
            .append(&batch.vectors, dim)
            .map_err(|e| {
                Status::internal(format!(
                    "exact-vector append failed after provider commit: {e}; refuse further \
                     ingest and rebuild this generation"
                ))
            })?;
        let committed_config = guard
            .index
            .as_ref()
            .expect("constructed or present above")
            .backend_config()
            .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
        // One record PER VECTOR: contiguous ids hash to different
        // buckets, and a bucket file must never hold vectors that belong
        // to another bucket. Buffered (no fsync per batch); Flush and
        // generation rotation fsync.
        if let Some(wal) = guard.wal.as_mut() {
            wal.update_manifest(|m| {
                if m.dim == 0 {
                    m.dim = dim as u32;
                }
                m.bit_width = index_bit_width as u32;
                m.set_backend_config(committed_config.clone());
            });
        }
        for (i, vector) in batch.vectors.chunks_exact(dim).enumerate() {
            wal_append_or_degrade(
                &mut guard.wal,
                wal_record::Op::AddVectors(LoggedAddVectors {
                    first_id: first_id + i as u64,
                    batch: Some(AddVectorsRequest {
                        vectors: vector.to_vec(),
                        dim: dim as u32,
                    }),
                    stable_routing_keys: stable_routing_key.clone().into_iter().collect(),
                }),
            );
        }
        Ok(((batch.vectors.len() / dim) as u64, first_id))
    }

    /// Compute both raw legs for a hybrid query: `(vector_leg, bm25_leg)`
    /// as `(global_doc_id, raw_score)` lists, score-descending.
    ///
    /// Vector leg: the chunked scan (local floor seeding only — the
    /// cross-shard floor-sharing protocol lives on SearchShard's bidi
    /// stream and is not part of the unary hybrid path). A shard with no
    /// vector index, or an empty query vector, contributes an empty leg
    /// rather than failing the whole hybrid query. BM25 leg: scored with
    /// the coordinator-supplied GLOBAL stats.
    /// The fused multi-field Bm25Query route (`docs/multi-field.md`):
    /// legs resolve to field views by name, score through
    /// [`bm25::top_k_fused_pruned`] (exhaustive when impacts are
    /// missing or `--block-max=false`; results identical), and the
    /// floor applies to the FUSED score. A leg naming a field this
    /// shard lacks is skipped: its documents hold no postings there, so
    /// every fused score is unchanged — the graceful path for a
    /// heterogeneous fleet. That is safe only because the coordinator
    /// refuses a field NO shard knows (see `fanout_bm25_fused` and
    /// `FieldStats.known`); skipping alone would turn a misspelled field
    /// into a silently different ranking.
    fn bm25_query_fused(
        &self,
        req: &Bm25QueryRequest,
        phrase: Option<(usize, &[f32])>,
        live: Option<bm25::LiveFloorHook>,
        mut candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        if !req.projections.is_empty() {
            return Err(Status::invalid_argument(
                "projection: projections are not certified on the fused route yet; \
                 use the flat Bm25Search route",
            ));
        }

        for leg in &req.fields {
            if leg.terms.len() != leg.global_doc_frequencies.len() {
                return Err(Status::invalid_argument(format!(
                    "leg {:?}: terms and global_doc_frequencies must have the same length",
                    leg.field
                )));
            }
            if leg.weight < 0.0 || leg.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "leg {:?}: weight must be >= 0",
                    leg.field
                )));
            }
        }
        validate_range_facet_fields(&req.range_facet_fields)?;
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let guard = self.state.read().expect("shard state lock poisoned");
        guard.check_stats_epoch(req.expected_stats_epoch)?;
        // Filled inside the scoring arm (the facet walk reuses the
        // resolved field views); a shard with no lexical half answers
        // every requested facet field as unknown.
        let mut facets: Vec<crate::pb::FacetFieldCounts> = Vec::new();
        let mut range_facets: Vec<crate::pb::RangeFacetCounts> = Vec::new();
        // Computed regardless of k and of whether the shard scores, so
        // a typo'd column refuses on the fused route too.
        let geo_columns_known = match guard.bm25.as_ref() {
            Some(store) => store.geo_columns_known(&req.geo_filters),
            None => vec![false; req.geo_filters.len()],
        };
        let filter_columns_known = match (guard.bm25.as_ref(), req.filter.as_ref()) {
            (Some(store), Some(f)) => store.filter_columns_known(f),
            (None, Some(f)) => vec![false; crate::filter::leaf_count(f)],
            (_, None) => Vec::new(),
        };
        let hits: Vec<Bm25Hit> = match guard.bm25.as_ref() {
            // Facet counting enters the arm even at k == 0 (the flat
            // path counts regardless of k; the scorers return no hits
            // for k == 0 on their own).
            Some(store)
                if req.k > 0
                    || !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty()
                    || !req.range_facet_fields.is_empty() =>
            {
                if store.as_index().is_none() {
                    return Err(Status::failed_precondition(
                        "bm25 bulk build in progress; Flush first",
                    ));
                }
                let mut views: Vec<Box<dyn Bm25Index + '_>> = Vec::new();
                let mut leg_of_view: Vec<usize> = Vec::new();
                for (li, leg) in req.fields.iter().enumerate() {
                    if let Some(fi) = store.field_index(&leg.field) {
                        // Term identity is a contract, and the field
                        // name does not carry it. A column built folded
                        // and queried cased matches on name, scores
                        // different terms, and returns a ranking that
                        // looks perfectly reasonable. Refuse instead.
                        let held = store.analysis_fingerprint(fi);
                        if held != 0
                            && leg.analysis_fingerprint != 0
                            && held != leg.analysis_fingerprint
                        {
                            return Err(Status::failed_precondition(format!(
                                "field {:?} was built with analyzer fingerprint {held:#x} but the \
                                 query's terms were analyzed under {:#x}; the two score different \
                                 term identities",
                                leg.field, leg.analysis_fingerprint
                            )));
                        }
                        views.push(store.field_view(fi).expect("searchable, checked above"));
                        leg_of_view.push(li);
                    }
                }
                // The request's filters, resolved ONCE against this
                // shard's tables and shared by facet counting and the
                // scorers below — one resolution, one truth
                // (docs/geo-columns.md, docs/cel-filters.md). With no
                // filters the ctx is None and every path below is
                // bit-identical to its unfiltered form.
                let numeric_read = ShardNumericRead(store);
                // Phrase gates (docs/phrase-proximity.md): a leg's
                // ordered window, checked at the same heap gate as every
                // other filter. The field must be on this shard and must
                // carry positions; a shard cannot approximate the window
                // from spans, so it refuses by name instead of scoring
                // the terms unconstrained.
                let mut phrase_gates: Vec<crate::filter::PhraseGate<'_>> = Vec::new();
                for (li, leg) in req.fields.iter().enumerate() {
                    let Some(phrase) = leg.phrase.as_ref() else {
                        continue;
                    };
                    let Some(vi) = leg_of_view.iter().position(|&candidate| candidate == li) else {
                        return Err(Status::failed_precondition(format!(
                            "phrase field {:?} is absent from this shard; a phrase cannot be \
                             served on part of the fleet",
                            leg.field
                        )));
                    };
                    if !views[vi].has_positions() {
                        return Err(Status::failed_precondition(format!(
                            "field {:?} has no token positions on this shard; a phrase or slop \
                             query needs --position-fields={} and a rebuilt generation",
                            leg.field, leg.field
                        )));
                    }
                    if phrase.sequence.len() < 2
                        || phrase
                            .sequence
                            .iter()
                            .any(|&i| i as usize >= leg.terms.len())
                    {
                        return Err(Status::invalid_argument(format!(
                            "field {:?}: phrase sequence must name at least two of the leg's {} \
                             terms",
                            leg.field,
                            leg.terms.len()
                        )));
                    }
                    phrase_gates.push(crate::filter::PhraseGate {
                        index: views[vi].as_ref(),
                        terms: &leg.terms,
                        sequence: phrase.sequence.iter().map(|&i| i as usize).collect(),
                        slop: phrase.slop,
                    });
                }
                let doc_filter = crate::filter::DocFilter {
                    deleted: guard.live_docs.words(),
                    geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                    pred: req
                        .filter
                        .as_ref()
                        .map(|f| store.resolve_filter(f))
                        .transpose()?,
                    phrase: phrase_gates,
                };
                let filter_ctx: bm25::FilterCtx = if req.geo_filters.is_empty()
                    && doc_filter.pred.is_none()
                    && doc_filter.deleted.is_none()
                    && doc_filter.phrase.is_empty()
                {
                    None
                } else {
                    Some((&doc_filter, &numeric_read))
                };
                if !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty()
                    || !req.range_facet_fields.is_empty()
                {
                    let pairs: Vec<(&dyn Bm25Index, &[String])> = views
                        .iter()
                        .zip(&leg_of_view)
                        .map(|(view, &li)| (view.as_ref(), req.fields[li].terms.as_slice()))
                        .collect();
                    // The fused route refuses stats/cardinality
                    // upstream, like score stages.
                    (facets, range_facets, _, _) = store.count_facets(
                        &pairs,
                        &req.facet_fields,
                        &req.map_facet_fields,
                        &req.range_facet_fields,
                        &[],
                        &[],
                        filter_ctx,
                    );
                }
                // Leg list order is the pinned accumulation order; the
                // coordinator sends the same order to every shard, so
                // distributed fused scores are bit-identical to the
                // monolith's.
                let queries: Vec<bm25::FieldQuery> = views
                    .iter()
                    .zip(&leg_of_view)
                    .map(|(view, &li)| {
                        let leg = &req.fields[li];
                        Ok(bm25::FieldQuery {
                            index: view.as_ref(),
                            terms: &leg.terms,
                            stats: bm25::CorpusStats {
                                doc_count: req.global_doc_count,
                                total_doc_length: leg.global_total_doc_length,
                                dfs: leg.global_doc_frequencies.clone(),
                            },
                            params: params_from(leg.k1, leg.b)?,
                            weight: if leg.weight == 0.0 {
                                1.0
                            } else {
                                f64::from(leg.weight)
                            },
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                let floor = if req.min_score == 0.0 {
                    f64::NEG_INFINITY
                } else {
                    f64::from(req.min_score)
                };
                let prunable = self.config.block_max
                    && queries.iter().all(|fq| {
                        fq.terms
                            .iter()
                            .enumerate()
                            // Local absence is not a missing impact
                            // surface: see top_k_fused_pruned_stats.
                            // Global df alone would forfeit pruning on
                            // every shard lacking a rare term.
                            .all(|(ti, t)| {
                                fq.stats.dfs[ti] == 0
                                    || fq.index.df(t) == 0
                                    || fq.index.has_impacts(t)
                            })
                    });
                // Filters ride the fused route exactly as range facets
                // do: the coordinator forwards them verbatim and the
                // node applies them at the one place a filter belongs,
                // before heap insertion (`filter_ctx`, resolved above).
                let phrase_view = match phrase {
                    Some((leg, weights)) => Some((
                        leg_of_view
                        .iter()
                        .position(|&candidate| candidate == leg)
                        .ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "phrase field {:?} is absent from this shard; rebuild the complete generation",
                                req.fields[leg].field
                            ))
                        })?,
                        weights,
                    )),
                    None => None,
                };
                let docs = if let Some((phrase_view, weights)) = phrase_view {
                    let docs = bm25::filter_fused_to_floor(
                        bm25::top_k_phrase_exhaustive_filtered(
                            &queries,
                            phrase_view,
                            &weights
                                .iter()
                                .map(|&value| f64::from(value))
                                .collect::<Vec<_>>(),
                            req.k as usize,
                            filter_ctx,
                        ),
                        floor,
                    );
                    if let Some(sink) = candidate.as_mut() {
                        for doc in &docs {
                            sink(doc.doc_id, doc.score as f32);
                        }
                    }
                    docs
                } else if prunable {
                    let mut prune = bm25::PruneStats::default();
                    bm25::top_k_fused_pruned_filtered_stats_streaming(
                        &queries,
                        req.k as usize,
                        floor,
                        filter_ctx,
                        &mut prune,
                        live,
                        candidate,
                    )
                } else {
                    let docs = bm25::filter_fused_to_floor(
                        bm25::top_k_fused_exhaustive_filtered(&queries, req.k as usize, filter_ctx),
                        floor,
                    );
                    if let Some(sink) = candidate.as_mut() {
                        for doc in &docs {
                            sink(doc.doc_id, doc.score as f32);
                        }
                    }
                    docs
                };
                let highlight = highlight_plan(store, req.highlight.as_ref())?;
                let body = store.field_name(0);
                let text_index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // The explain breakdown, only for the hits that survived
                // the top-k (docs/explain.md).
                let presence = req.explain.then(|| {
                    let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
                    bm25::breakdown(&queries, &ids)
                });
                let leg_names: Vec<&str> = leg_of_view
                    .iter()
                    .map(|&li| req.fields[li].field.as_str())
                    .collect();
                let phrase_weights: Option<(usize, Vec<f64>)> =
                    phrase_view.map(|(view, weights)| {
                        (
                            view,
                            weights.iter().map(|&value| f64::from(value)).collect(),
                        )
                    });
                docs.into_iter()
                    .enumerate()
                    .map(|(hit_index, doc)| -> Result<Bm25Hit, Status> {
                        let explain = presence.as_ref().map(|presence| {
                            explain_terms(
                                &queries,
                                &leg_names,
                                &presence[hit_index],
                                phrase_weights
                                    .as_ref()
                                    .map(|(view, weights)| (*view, weights.as_slice())),
                            )
                        });
                        let snippets = match highlight.as_ref() {
                            Some(plan) => {
                                // Body occurrences only (the stored text
                                // is the body's); a term is distinct per
                                // leg, so the key carries the leg.
                                let occurrences: Vec<(usize, (u32, u32))> = doc
                                    .term_offsets
                                    .iter()
                                    .filter(|(fi, _, _)| req.fields[leg_of_view[*fi]].field == body)
                                    .flat_map(|(fi, ti, offsets)| {
                                        let key = (leg_of_view[*fi] << 20) | *ti;
                                        offsets.iter().map(move |&span| (key, span))
                                    })
                                    .collect();
                                cut_snippets(plan, text_index, body, doc.doc_id, &occurrences)?
                            }
                            None => Vec::new(),
                        };
                        Ok(Bm25Hit {
                            explain,
                            snippets,
                            projected: Vec::new(),
                            doc_id: self.config.slot_offset + u64::from(doc.doc_id),
                            score: doc.score as f32,
                            terms: doc
                                .term_offsets
                                .into_iter()
                                .map(|(fi, ti, offsets)| {
                                    let leg = &req.fields[leg_of_view[fi]];
                                    TermOccurrences {
                                        term: leg.terms[ti].clone(),
                                        field: leg.field.clone(),
                                        offsets: offsets
                                            .into_iter()
                                            .map(|(start, end)| OffsetSpan { start, end })
                                            .collect(),
                                    }
                                })
                                .collect(),
                        })
                    })
                    .collect::<Result<Vec<_>, Status>>()?
            }
            _ => {
                facets = req
                    .facet_fields
                    .iter()
                    .map(|name| (name.clone(), String::new()))
                    .chain(
                        req.map_facet_fields
                            .iter()
                            .map(|m| (m.column.clone(), m.key.clone())),
                    )
                    .map(|(field, key)| crate::pb::FacetFieldCounts {
                        field,
                        known: false,
                        counts: Vec::new(),
                        key,
                    })
                    .collect();
                range_facets = unknown_range_counts(&req.range_facet_fields);
                Vec::new()
            }
        };
        // Same seed rule as the flat path: one f32 ULP below the k-th
        // fused score when the heap filled, 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits.last()
                .map(|h| bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Bm25QueryResponse {
            projection_leaves_known: Vec::new(),
            hits,
            kth_best,
            facets,
            // The fused route refuses score stages upstream, and
            // stats/cardinality with them.
            stage_columns_known: Vec::new(),
            stats: Vec::new(),
            distinct: Vec::new(),
            range_facets,
            geo_columns_known,
            filter_columns_known,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_legs(
        &self,
        vector: &[f32],
        terms: &[String],
        global_doc_count: u64,
        global_total_doc_length: u64,
        global_doc_frequencies: &[u32],
        params: Bm25Params,
        k: usize,
        expected_stats_epoch: u64,
        filters: &LegFilters<'_>,
    ) -> Result<LegResults, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        guard.check_stats_epoch(expected_stats_epoch)?;
        // One resolution for both legs (docs/vector-filters.md): the
        // allowlist the vector kernel scans under and the predicate the
        // lexical heap gate applies are the same DocFilter, so the two
        // halves of a fused list cannot disagree about what matched.
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), filters.geo, filters.tree);
        let slots = guard.index.as_ref().map_or(0, |index| index.len());
        let (doc_filter, allow) = resolve_shard_filters(
            guard.bm25.as_ref(),
            guard.live_docs.words(),
            slots,
            filters.geo,
            &filters.regions,
            filters.tree,
        )?;

        let mut vector_leg: Vec<(u64, f64)> = Vec::new();
        if k > 0 && !vector.is_empty() {
            if let Some(index) = guard.index.as_ref() {
                let dim = index.dim_opt().unwrap_or(0);
                if vector.len() != dim {
                    return Err(Status::invalid_argument(format!(
                        "hybrid vector has dim {}, index expects {dim}",
                        vector.len()
                    )));
                }
                if let Some((_, coord, value)) = first_invalid_coordinate(vector, dim) {
                    return Err(Status::invalid_argument(format!(
                        "hybrid vector coordinate {coord} is invalid: {value}"
                    )));
                }
                let (hits, _) = chunked_topk(
                    index,
                    vector,
                    k,
                    self.config.chunk_blocks,
                    &mut || None,
                    &mut |_| false,
                    false,
                    allow.as_deref(),
                );
                vector_leg = hits
                    .into_iter()
                    .map(|h| {
                        (
                            self.config.slot_offset + u64::from(h.slot),
                            f64::from(h.score),
                        )
                    })
                    .collect();
            }
        }

        let mut bm25_leg: Vec<(u64, f64)> = Vec::new();
        if k > 0 && !terms.is_empty() {
            if let Some(store) = guard.bm25.as_ref() {
                let stats = bm25::CorpusStats {
                    doc_count: global_doc_count,
                    total_doc_length: global_total_doc_length,
                    dfs: global_doc_frequencies.to_vec(),
                };
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // Block-max path when every scored term has impacts
                // (v5 shards) and the node flag allows it; heap store,
                // v3/v4, and --block-max=false keep top_k. The results
                // are bit-identical either way.
                let prunable = self.config.block_max
                    && terms
                        .iter()
                        .enumerate()
                        // Local absence is not a missing impact surface:
                        // see top_k_pruned. Global df alone would forfeit
                        // pruning on every shard lacking a rare term.
                        .all(|(ti, t)| {
                            stats.dfs[ti] == 0 || index.df(t) == 0 || index.has_impacts(t)
                        });
                let numeric_read = ShardNumericRead(store);
                let filter_ctx: bm25::FilterCtx = doc_filter
                    .as_ref()
                    .map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
                let docs = if prunable {
                    let mut prune = bm25::PruneStats::default();
                    bm25::top_k_pruned_chained_filtered_stats(
                        index,
                        terms,
                        &stats,
                        params,
                        k,
                        f64::NEG_INFINITY,
                        None,
                        filter_ctx,
                        &mut prune,
                    )
                } else {
                    bm25::top_k_chained_filtered(index, terms, &stats, params, k, None, filter_ctx)
                };
                bm25_leg = docs
                    .into_iter()
                    .map(|d| (self.config.slot_offset + u64::from(d.doc_id), d.score))
                    .collect();
            }
        }

        Ok(LegResults {
            vector: vector_leg,
            bm25: bm25_leg,
            geo_columns_known,
            filter_columns_known,
        })
    }

    /// Level one of the two-level hybrid fusion: run both legs locally
    /// and RRF-fuse them (see `SearchService.HybridSearch`).
    fn run_hybrid(&self, req: HybridShardRequest) -> Result<HybridShardResponse, Status> {
        let k = req.k as usize;
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let vector_weight = weight_or_default(req.vector_weight, "vector_weight")?;
        let bm25_weight = weight_or_default(req.bm25_weight, "bm25_weight")?;
        let rrf_k = if req.rrf_k == 0.0 {
            fusion::DEFAULT_RRF_K
        } else {
            f64::from(req.rrf_k)
        };
        if rrf_k.is_nan() || rrf_k <= 0.0 {
            return Err(Status::invalid_argument("rrf_k must be positive"));
        }

        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let legs = self.compute_legs(
            &req.vector,
            &req.terms,
            req.global_doc_count,
            req.global_total_doc_length,
            &req.global_doc_frequencies,
            params_from(req.k1, req.b)?,
            k,
            req.expected_stats_epoch,
            &LegFilters {
                geo: &req.geo_filters,
                regions: geo_regions,
                tree: req.filter.as_ref(),
            },
        )?;
        let geo_columns_known = legs.geo_columns_known;
        let filter_columns_known = legs.filter_columns_known;

        let fused = fusion::rrf_fuse(
            &[
                Leg {
                    hits: legs.vector,
                    weight: vector_weight,
                },
                Leg {
                    hits: legs.bm25,
                    weight: bm25_weight,
                },
            ],
            rrf_k,
            k,
        );
        Ok(HybridShardResponse {
            hits: fused
                .into_iter()
                .map(|h| HybridLegHit {
                    doc_id: h.doc_id,
                    fused_score: h.fused_score as f32,
                    vector_rank: h.leg_ranks[0],
                    vector_score: h.leg_scores[0].unwrap_or(0.0) as f32,
                    bm25_rank: h.leg_ranks[1],
                    bm25_score: h.leg_scores[1].unwrap_or(0.0) as f32,
                })
                .collect(),
            geo_columns_known,
            filter_columns_known,
        })
    }
}

/// Request-carried BM25 params: 0 selects the default (proto3 "absent").
///
/// Values are RANGE CHECKED, not just defaulted. `b` outside [0, 1]
/// breaks the monotonicity precondition the block-max bounds rest on
/// (see `postings::SkipRun`), so a bound can fall below a real score and
/// the pruned scorer silently drops hits the exhaustive one keeps. A NaN
/// is worse: BM25's `partial_cmp(..).unwrap_or(Equal)` degrades to
/// insertion order, which makes the ranking depend on shard layout. Both
/// arrive straight off the wire, so both are refused here rather than
/// discovered as an unreproducible ranking difference.
fn params_from(k1: f32, b: f32) -> Result<Bm25Params, Status> {
    if !k1.is_finite() || k1 < 0.0 {
        return Err(Status::invalid_argument(format!(
            "bm25 k1 must be finite and >= 0, got {k1}"
        )));
    }
    if !b.is_finite() || !(0.0..=1.0).contains(&b) {
        return Err(Status::invalid_argument(format!(
            "bm25 b must be finite and within [0, 1], got {b}"
        )));
    }
    Ok(Bm25Params {
        k1: if k1 == 0.0 {
            bm25::DEFAULT_K1
        } else {
            f64::from(k1)
        },
        b: if b == 0.0 {
            bm25::DEFAULT_B
        } else {
            f64::from(b)
        },
    })
}

/// Request weights default to 1.0 (0 means "unset" in the proto);
/// negatives are rejected.
fn weight_or_default(value: f32, name: &str) -> Result<f64, Status> {
    if value == 0.0 {
        return Ok(1.0);
    }
    if value < 0.0 || value.is_nan() {
        return Err(Status::invalid_argument(format!("{name} must be >= 0")));
    }
    Ok(f64::from(value))
}

/// Documents held for ordered apply; bounds this side's memory the way
/// ANALYZE_PIPELINE bounded the unary path.
const MAX_PENDING: usize = 32;

/// One event from the extra-field analysis streams.
enum FieldEvent {
    /// A field finished: the submission sequence it was tagged with, and
    /// either its analysis or that ONE field's own failure.
    Result(u64, Result<crate::postings::AnalyzedField, Status>),
    /// The stream itself failed, so every field riding it is lost. Kept
    /// distinct from a per-field error because a dead stream cannot be
    /// attributed to a sequence, and silently dropping it would hang the
    /// apply wavefront on a field that is never coming.
    StreamFailed(Status),
}

/// Persistent per-spec [`crate::analyzer::AnalyzeStream`] sessions for
/// documents' extra fields.
///
/// Extra fields used to ride concurrent UNARY `Analyze` calls, one h2
/// stream per field per document. The asymmetry with the body (which has
/// always streamed) was deliberate: field texts were captions and titles,
/// small next to the body, so a dedicated stream bought nothing. That
/// stops being true with multi-field body columns. A rebuild of 86.6M
/// chunks carrying `case_name` plus two A/B body columns is ~260M unary
/// calls against the sidecar, which the rebuild README names as the
/// ingest throughput ceiling, not shard parallelism. The sidecar's
/// listener has died under rapid-fire unary traffic before, which is why
/// `analyzer::shared_channel` exists at all.
///
/// Fields on ONE document deliberately carry DIFFERENT specs (that is
/// what a body column IS), so this holds one session per distinct spec,
/// opened on first use and reused for the rest of the call.
struct FieldStreams {
    addr: String,
    /// Submission handles, one per distinct spec in first-seen order.
    /// Holding the SUBMITTER here rather than the session is what lets
    /// [`finish`](Self::finish) half-close every stream by dropping them;
    /// each session itself is owned by its driver task.
    sessions: Vec<(
        Option<crate::pb::AnalysisSpec>,
        crate::analyzer::AnalyzeSubmit,
    )>,
    events: tokio::sync::mpsc::Receiver<FieldEvent>,
    /// Cloned into each driver as it is spawned. Taking it in `finish` is
    /// what makes `recv` observe `None` once every driver has exited.
    emit: Option<tokio::sync::mpsc::Sender<FieldEvent>>,
    /// Monotonic across every session: the sequence is this side's
    /// routing key, and results are matched by it alone.
    next_sequence: u64,
    /// Submitted minus delivered. `recv` must not be selected on when
    /// this is zero, or it parks on a channel nothing will feed.
    outstanding: usize,
}

impl FieldStreams {
    fn new(addr: &str) -> Self {
        // Deep enough that drivers rarely block on a full channel while
        // the apply wavefront is busy; every item is one analyzed field.
        let (emit, events) = tokio::sync::mpsc::channel(MAX_PENDING * 4);
        Self {
            addr: addr.to_string(),
            sessions: Vec::new(),
            events,
            emit: Some(emit),
            next_sequence: 0,
            outstanding: 0,
        }
    }

    /// Queue one field's text on the session for its spec, opening that
    /// session if this is the spec's first field. Returns the sequence
    /// the result will carry.
    async fn submit(
        &mut self,
        spec: Option<&crate::pb::AnalysisSpec>,
        text: &str,
    ) -> Result<u64, Status> {
        let index = match self.sessions.iter().position(|(s, _)| s.as_ref() == spec) {
            Some(index) => index,
            None => {
                let mut session = crate::analyzer::AnalyzeStream::open(&self.addr, spec)
                    .await
                    .map_err(|status| {
                        if status.code() == tonic::Code::Unimplemented {
                            Status::failed_precondition(format!(
                                "analysis sidecar at {} does not implement AnalyzeStream; \
                                 it predates the RPC and must be rebuilt (./gradlew installDist \
                                 in grpc-opennlp-analysis). Refusing to analyze fields on the \
                                 removed unary path.",
                                self.addr
                            ))
                        } else {
                            status
                        }
                    })?;
                let submit = session.submitter();
                // Hand the driver a session that holds NO submitter of
                // its own, so the clone kept here is the only one alive
                // and dropping it in `finish` really does half-close the
                // stream. `open` leaves a submitter inside the session;
                // leaving it there means the sidecar never sees the
                // half-close, and a held response never flushes.
                session.finish();
                let emit = self
                    .emit
                    .clone()
                    .ok_or_else(|| Status::internal("field streams already finished"))?;
                tokio::spawn(drive_field_stream(session, emit));
                self.sessions.push((spec.cloned(), submit));
                self.sessions.len() - 1
            }
        };
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.sessions[index].1.submit(sequence, text).await?;
        self.outstanding += 1;
        Ok(sequence)
    }

    /// True when a field result is still owed, and therefore when `recv`
    /// is safe to select on.
    fn pending(&self) -> bool {
        self.outstanding > 0
    }

    /// The next field event. `None` once every driver has exited, which
    /// only happens after [`finish`](Self::finish) drops the last sender.
    async fn recv(&mut self) -> Option<FieldEvent> {
        let event = self.events.recv().await;
        if matches!(event, Some(FieldEvent::Result(..))) {
            self.outstanding -= 1;
        }
        event
    }

    /// Half-close every stream so the sidecar drains what is in flight.
    /// Idempotent, and required before awaiting the final results: a
    /// sidecar may hold a response until more work arrives (the test mock
    /// deliberately does), so the last field of a call only lands once
    /// its stream is closing.
    fn finish(&mut self) {
        self.sessions.clear();
        self.emit = None;
    }
}

/// Pump one field session's results into the shared event channel.
///
/// Runs as its own task so the main ingest loop never has to poll N
/// streams by hand, and so a session whose results the sidecar is holding
/// cannot stall submission on a different session.
async fn drive_field_stream(
    mut session: crate::analyzer::AnalyzeStream,
    emit: tokio::sync::mpsc::Sender<FieldEvent>,
) {
    loop {
        let event = match session.next().await {
            Ok(Some((sequence, result))) => FieldEvent::Result(
                sequence,
                result.map(crate::postings::AnalyzedDoc::into_body),
            ),
            Ok(None) => return,
            Err(status) => FieldEvent::StreamFailed(status),
        };
        let failed = matches!(event, FieldEvent::StreamFailed(_));
        // A closed receiver means the ingest call is already unwinding.
        if emit.send(event).await.is_err() || failed {
            return;
        }
    }
}

/// One document held between submission and apply: the request itself,
/// plus its extra fields filling in as their results arrive.
struct PendingDoc {
    doc: AddDocumentsRequest,
    /// The document's dense vector, mapped ingest only: it applies in
    /// LOCKSTEP with the document, landing at the same id. `None` on
    /// the ordinary AddDocuments path.
    vector: Option<Vec<f32>>,
    stable_routing_key: Option<Vec<u8>>,
    /// `(field table index, analysis)`, in submission order. `None` until
    /// that field's result lands.
    extras: Vec<(usize, Option<crate::postings::AnalyzedField>)>,
    /// Extras still unfilled. The document is ready to apply when this is
    /// zero AND its body result has arrived.
    outstanding: usize,
}

/// One document entering the shared ingest pipeline: the ordinary
/// request plus, on the mapped path, its vector.
struct IngestDoc {
    req: AddDocumentsRequest,
    vector: Option<Vec<f32>>,
    /// Opaque product identity supplied only by routed mapped ingest.
    stable_routing_key: Option<Vec<u8>>,
}

/// Where the ingest pipeline's documents come from: the ordinary
/// AddDocuments stream verbatim, or the mapped stream decoding each
/// serialized protobuf document against the bound plan. One pipeline,
/// two front doors — the mapped path reuses the analysis session, the
/// apply wavefront, the column validation, and the WAL records
/// unchanged.
enum IngestSource<'a> {
    Plain {
        stream: &'a mut Streaming<AddDocumentsRequest>,
        stable_routing_key: Option<Vec<u8>>,
        consumed: bool,
    },
    /// Boxed: the extractor's trie dwarfs the plain variant.
    Mapped(Box<MappedSource<'a>>),
}

struct MappedSource<'a> {
    stream: &'a mut Streaming<crate::pb::IngestMappedRequest>,
    extractor: crate::mapping::Extractor,
    /// Session properties from the bind, attached to every decoded
    /// document.
    analysis: Option<crate::pb::AnalysisSpec>,
    materialize: Option<crate::pb::MaterializeSpec>,
    /// Source documents decoded so far; extraction errors name the
    /// failing position.
    position: u64,
    /// Source documents consumed (== position, kept for the response's
    /// accounting: a chunked document yields as many rows as it has
    /// chunks, including zero).
    parents: u64,
    /// Rows decoded but not yet handed to the pipeline: a chunked
    /// document explodes into one row per chunk.
    rows: std::collections::VecDeque<IngestDoc>,
}

impl IngestSource<'_> {
    async fn next(&mut self) -> Result<Option<IngestDoc>, Status> {
        match self {
            IngestSource::Plain {
                stream,
                stable_routing_key,
                consumed,
            } => {
                let request = stream.message().await?;
                if request.is_some() && *consumed && stable_routing_key.is_some() {
                    return Err(Status::invalid_argument(
                        "a replication stable-key metadata value may carry exactly one document",
                    ));
                }
                *consumed |= request.is_some();
                Ok(request.map(|req| IngestDoc {
                    req,
                    vector: None,
                    stable_routing_key: stable_routing_key.clone(),
                }))
            }
            IngestSource::Mapped(source) => source.next().await,
        }
    }
}

impl MappedSource<'_> {
    async fn next(&mut self) -> Result<Option<IngestDoc>, Status> {
        use crate::pb::ingest_mapped_request::Payload;
        loop {
            if let Some(row) = self.rows.pop_front() {
                return Ok(Some(row));
            }
            match self.stream.message().await? {
                None => return Ok(None),
                Some(message) => match message.payload {
                    // A zero-chunk document decodes to zero rows; loop
                    // for the next message rather than ending the
                    // stream.
                    Some(Payload::Document(bytes)) => self.decode(&bytes, None)?,
                    Some(Payload::RoutedDocument(document)) => {
                        if document.stable_key.is_empty() {
                            return Err(Status::invalid_argument(
                                "routed mapped document has an empty stable key",
                            ));
                        }
                        self.decode(&document.document, Some(document.stable_key))?;
                    }
                    Some(Payload::Bind(_)) => {
                        return Err(Status::invalid_argument(
                            "bind repeats mid-stream; a mapped stream binds exactly once, \
                             first",
                        ))
                    }
                    None => {
                        return Err(Status::invalid_argument(
                            "empty IngestMappedRequest payload",
                        ))
                    }
                },
            }
        }
    }

    fn decode(&mut self, bytes: &[u8], stable_routing_key: Option<Vec<u8>>) -> Result<(), Status> {
        let position = self.position;
        self.position += 1;
        let rows = self.extractor.extract(bytes).map_err(|status| {
            Status::new(
                status.code(),
                format!("document {position}: {}", status.message()),
            )
        })?;
        self.parents += 1;
        for extracted in rows {
            let mut req = extracted.request;
            req.analysis = self.analysis.clone();
            req.materialize = self.materialize.clone();
            self.rows.push_back(IngestDoc {
                req,
                vector: Some(extracted.vector),
                stable_routing_key: stable_routing_key.clone(),
            });
        }
        Ok(())
    }
}

/// Assemble one document's positional [`crate::postings::AnalyzedDoc`]:
/// body at field 0, extras at their table indexes, gaps empty. Body-only
/// documents pass through untouched (the exact pre-multi-field shape).
///
/// Nothing is awaited here. The apply wavefront only reaches a document
/// once every one of its fields has already landed, which is what keeps
/// a sidecar that holds a response (the test mock deliberately does, and
/// the streaming contract permits it) from stalling the whole ingest on
/// one field.
pub(crate) fn join_fields(
    mut body: crate::postings::AnalyzedDoc,
    extras: Vec<(usize, Option<crate::postings::AnalyzedField>)>,
    cased: Option<usize>,
) -> Result<crate::postings::AnalyzedDoc, Status> {
    // The body's cased identity (docs/dual-cased.md) came out of the
    // same pass; it lands at the field the request named.
    let cased_identity = body.cased.take();
    match (cased, &cased_identity) {
        (Some(_), None) => {
            return Err(Status::internal(
                "the document names a cased field but its analysis carried no cased identity",
            ))
        }
        (None, Some(_)) => {
            return Err(Status::internal(
                "the analysis carried a cased identity no field asked for",
            ))
        }
        _ => {}
    }
    if extras.is_empty() && cased.is_none() {
        return Ok(body);
    }
    let n = extras
        .iter()
        .map(|&(fi, _)| fi + 1)
        .chain(cased.map(|ci| ci + 1))
        .max()
        .unwrap_or(1);
    let mut fields = vec![crate::postings::AnalyzedField::default(); n];
    let quality = body.quality;
    let geography = body.geography.clone();
    let entities = body.entities.clone();
    fields[0] = body.into_body();
    for (fi, analyzed) in extras {
        fields[fi] = analyzed.ok_or_else(|| {
            Status::internal(format!(
                "field {fi} applied before its analysis arrived; the apply \
                 wavefront must not advance past an unfilled field"
            ))
        })?;
    }
    if let (Some(ci), Some(identity)) = (cased, cased_identity) {
        fields[ci] = identity;
    }
    Ok(crate::postings::AnalyzedDoc {
        fields,
        cased: None,
        quality,
        geography,
        entities,
    })
}

/// The field index `cased_field` names, validated (`docs/dual-cased.md`):
/// a declared BM25 field other than the body, not derived from the phrase
/// glossary or a bigram source, with an explicit step-chain body spec to
/// twin. `None` when the request names none.
pub(crate) fn cased_field_index(
    config: &NodeConfig,
    phrase_index: Option<&crate::phrases::PhraseIndex>,
    doc: &AddDocumentsRequest,
) -> Result<Option<usize>, Status> {
    if doc.cased_field.is_empty() {
        return Ok(None);
    }
    let name = doc.cased_field.as_str();
    if name == "body" {
        return Err(Status::invalid_argument(
            "cased_field must name a field other than \"body\": the body is the folded identity",
        ));
    }
    if phrase_index.is_some_and(|phrases| name == phrases.phrase_field()) {
        return Err(Status::invalid_argument(format!(
            "cased_field {name:?} is the configured phrase glossary field; it is derived, not \
             cased"
        )));
    }
    if let Some(source) = config
        .bigram_fields
        .iter()
        .find(|source| crate::proximity::bigram_field_name(source) == name)
    {
        return Err(Status::invalid_argument(format!(
            "cased_field {name:?} is the bigram column derived from {source:?}; it is derived, \
             not cased"
        )));
    }
    let Some(fi) = config.bm25_fields.iter().position(|n| n == name) else {
        return Err(Status::invalid_argument(format!(
            "unknown cased_field {name:?}; this shard indexes {:?}",
            config.bm25_fields
        )));
    };
    crate::analyzer::validate_dual_cased_spec(doc.analysis.as_ref())?;
    Ok(Some(fi))
}

/// Whether an ingest asked for any quality column at all. An empty
/// spec (every column name blank) asks for nothing, and must not make
/// the node request layers the caller will not store.
fn quality_wanted(spec: Option<&crate::pb::QualitySpec>) -> bool {
    spec.is_some_and(|q| {
        !q.noise_column.is_empty()
            || !q.noise_chars_column.is_empty()
            || !q.artifact_column.is_empty()
    })
}

/// Whether an ingest asked for any geography column at all. Same
/// blank-spec rule as [`quality_wanted`]: a spec with every column
/// name empty asks for nothing and must not make the session request
/// (and pay for) the geocoding layer.
fn geography_wanted(spec: Option<&crate::pb::GeographySpec>) -> bool {
    spec.is_some_and(|g| {
        !g.point_column.is_empty()
            || !g.country_column.is_empty()
            || !g.confidence_column.is_empty()
    })
}

/// Canonical content hash of a materialize spec — the piece of the
/// mapped-plan binding covering derived columns, because changing a
/// materialization expression changes what an index means
/// (`docs/cel-values.md`). Empty for no spec (and for an empty one,
/// which asks for nothing).
pub(crate) fn materialize_sha(spec: Option<&crate::pb::MaterializeSpec>) -> String {
    let Some(spec) = spec else {
        return String::new();
    };
    if spec.columns.is_empty() {
        return String::new();
    }
    let mut hasher = crate::sha256::Sha256::new();
    for column in &spec.columns {
        for part in [column.name.as_str(), column.expression.as_str()] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part.as_bytes());
        }
        hasher.update(&column.kind.to_be_bytes());
    }
    crate::sha256::to_hex(&hasher.finalize())
}

/// Validate a request's `HighlightSpec` against this shard's storage
/// (`docs/highlighting.md`): every named field must be the one with
/// stored text (the body), and sentence mode needs the body's kind-8
/// table. Both refuse by name before any hit is scored, so a shard with
/// no matching document refuses exactly like one with a thousand.
fn highlight_plan(
    store: &Bm25Shard,
    spec: Option<&crate::pb::HighlightSpec>,
) -> Result<Option<crate::highlight::Plan>, Status> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let plan = crate::highlight::Plan::from_spec(spec)?;
    let body = store.field_name(0);
    for field in &plan.fields {
        if field != body {
            return Err(Status::invalid_argument(format!(
                "highlight field {field:?}: snippets are cut from stored text, and only the \
                 body's text ({body:?}) is stored on this shard"
            )));
        }
    }
    if plan.mode == crate::highlight::Mode::Sentence && !store.field_has_sentences(0) {
        return Err(Status::failed_precondition(format!(
            "field {body:?} on this shard stores no sentence spans, so sentence-mode snippets \
             cannot be cut; ingest with --sentence-fields={body}, or ask for \
             HIGHLIGHT_MODE_WINDOW, which cuts at whitespace without them"
        )));
    }
    Ok(Some(plan))
}

/// Cut one hit's snippets from the shard's stored text and sentence
/// table, around the occurrence spans the scorer already collected
/// (`(distinct term key, span)` pairs). No analyzer runs here.
fn cut_snippets(
    plan: &crate::highlight::Plan,
    index: &dyn Bm25Index,
    field: &str,
    local: u32,
    occurrences: &[(usize, (u32, u32))],
) -> Result<Vec<crate::pb::Snippet>, Status> {
    if occurrences.is_empty() {
        return Ok(Vec::new());
    }
    let text = index.text(local).ok_or_else(|| {
        Status::internal(format!(
            "highlight: hit {local} has no stored text to cut snippets from"
        ))
    })?;
    let sentences = match plan.mode {
        crate::highlight::Mode::Sentence => index.doc_sentences(local),
        crate::highlight::Mode::Window => None,
    };
    let cut = crate::highlight::snippets(
        &text,
        sentences.as_deref(),
        occurrences,
        plan.mode,
        plan.max_snippets,
        plan.max_chars,
    )?;
    Ok(cut
        .into_iter()
        .map(|s| crate::pb::Snippet {
            field: field.to_string(),
            text: s.text,
            start: s.start,
            end: s.end,
            highlights: s
                .highlights
                .into_iter()
                .map(|(start, end)| OffsetSpan { start, end })
                .collect(),
            cut: crate::highlight::Plan::wire_cut(s.cut),
            sentence_index: s.sentence_index.map(|i| i as u32),
        })
        .collect())
}

/// The optional sidecar layers a document's specs ask its analysis
/// session for — the session-identity companion to the reopen
/// condition (a change to either spec reopens the session).
pub(crate) fn session_layers(
    doc: &AddDocumentsRequest,
    phrase_index: Option<&crate::phrases::PhraseIndex>,
    sentence_fields: &[String],
) -> crate::analyzer::SessionLayers {
    crate::analyzer::SessionLayers {
        quality: quality_wanted(doc.quality.as_ref()),
        geography: geography_wanted(doc.geography.as_ref()),
        entities: phrase_index.is_some_and(crate::phrases::PhraseIndex::include_ner),
        // A node that stores sentence spans asks every session for the
        // layer (docs/highlighting.md): the node's configuration, not
        // the document, because the document's own record is filled
        // from that configuration only after analysis is requested.
        sentences: !sentence_fields.is_empty(),
        dual_cased: !doc.cased_field.is_empty(),
    }
}

/// Fold a document's derived quality scalars into its own `numerics` /
/// `integers` lists and clear the spec (`docs/quality-columns.md`).
///
/// A spec that names columns but whose analysis produced no measurement
/// is a contract break between the session's options and its responses,
/// not a document that happens to be clean: a clean document measures
/// `noise = 0`. It refuses rather than silently writing zeros.
fn materialize_quality(
    mut doc: AddDocumentsRequest,
    analyzed: &crate::postings::AnalyzedDoc,
) -> Result<AddDocumentsRequest, Status> {
    let Some(spec) = doc.quality.take() else {
        return Ok(doc);
    };
    if !quality_wanted(Some(&spec)) {
        return Ok(doc);
    }
    let Some(quality) = analyzed.quality else {
        return Err(Status::internal(
            "quality columns were requested but the analysis session returned no quality layers; \
             the sidecar's options and its responses disagree",
        ));
    };
    if !spec.noise_column.is_empty() {
        doc.numerics.push(crate::pb::NumericValue {
            field: spec.noise_column,
            value: quality.noise,
        });
    }
    if !spec.noise_chars_column.is_empty() {
        doc.integers.push(crate::pb::IntegerValue {
            field: spec.noise_chars_column,
            value: quality.noise_chars,
        });
    }
    if !spec.artifact_column.is_empty() {
        doc.integers.push(crate::pb::IntegerValue {
            field: spec.artifact_column,
            value: quality.artifacts,
        });
    }
    Ok(doc)
}

/// Fold a document's geography reduction into its own `geo_points` /
/// `facets` / `numerics` lists and clear the spec
/// (`docs/geography-columns.md`) — [`materialize_quality`]'s shape
/// over the geocoding layer, with one deliberate difference: absence
/// is a legitimate measurement here. A document that mentions no
/// resolvable place writes NO point and NO confidence (there is no
/// neutral coordinate; (0,0) is a real place), and no top region vote
/// writes no country. Filters then treat it by the documented absence
/// rules instead of finding it "at" a fabricated location.
fn materialize_geography(
    mut doc: AddDocumentsRequest,
    analyzed: &crate::postings::AnalyzedDoc,
) -> Result<AddDocumentsRequest, Status> {
    let Some(spec) = doc.geography.take() else {
        return Ok(doc);
    };
    if !geography_wanted(Some(&spec)) {
        return Ok(doc);
    }
    let Some(geography) = analyzed.geography.as_ref() else {
        return Err(Status::internal(
            "geography columns were requested but the analysis session returned no \
             geocoding layer; the sidecar's options and its responses disagree",
        ));
    };
    if let Some((lat, lon)) = geography.point {
        if !spec.point_column.is_empty() {
            doc.geo_points.push(crate::pb::GeoPointValue {
                field: spec.point_column,
                lat,
                lon,
            });
        }
        if !spec.confidence_column.is_empty() {
            doc.numerics.push(crate::pb::NumericValue {
                field: spec.confidence_column,
                value: geography.confidence,
            });
        }
    }
    if !geography.country.is_empty() && !spec.country_column.is_empty() {
        doc.facets.push(crate::pb::FacetValue {
            field: spec.country_column,
            value: geography.country.clone(),
        });
    }
    Ok(doc)
}

/// Bulk-ingest internals: the two analysis transports and the shared
/// per-document apply step.
impl NodeServiceImpl {
    /// Validate a document's extra fields against the shard's configured
    /// field table and queue their analyses on the per-spec field
    /// streams. Validation failures surface BEFORE any store or WAL
    /// effect, and before anything is submitted.
    ///
    /// Every field's sequence is recorded in `route` so its result can be
    /// steered back to `(document sequence, slot in that document's
    /// extras)`; the sequence is the only key the wire carries.
    async fn submit_field_analyses(
        &self,
        doc: &AddDocumentsRequest,
        sequence: u64,
        streams: &mut FieldStreams,
        route: &mut std::collections::HashMap<u64, (u64, usize)>,
    ) -> Result<Vec<(usize, Option<crate::postings::AnalyzedField>)>, Status> {
        // A cased field is validated here, before the body is submitted,
        // so a misnamed one is refused without analyzing anything.
        cased_field_index(&self.config, self.phrase_index.as_deref(), doc)?;
        if doc.fields.is_empty() {
            return Ok(Vec::new());
        }
        // Validate the whole document first: a partially submitted
        // document would leave orphan sequences in flight that nothing
        // ever routes.
        let mut seen: Vec<&str> = Vec::new();
        let mut accepted: Vec<usize> = Vec::with_capacity(doc.fields.len());
        for field in &doc.fields {
            if field.field == "body" {
                return Err(Status::invalid_argument(
                    "\"body\" is the top-level text; DocumentField names extra fields only",
                ));
            }
            if self
                .phrase_index
                .as_ref()
                .is_some_and(|phrases| field.field == phrases.phrase_field())
            {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is derived from the configured phrase glossary; clients must not supply it",
                    field.field
                )));
            }
            if let Some(source) = self
                .config
                .bigram_fields
                .iter()
                .find(|source| crate::proximity::bigram_field_name(source) == field.field)
            {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is the bigram column derived from {source:?}; clients must not supply it",
                    field.field
                )));
            }
            if seen.contains(&field.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in one document",
                    field.field
                )));
            }
            if !doc.cased_field.is_empty() && field.field == doc.cased_field {
                return Err(Status::invalid_argument(format!(
                    "field {:?} is the body's cased identity (cased_field), produced from the \
                     body's analysis; do not supply it as a DocumentField",
                    field.field
                )));
            }
            seen.push(&field.field);
            let Some(fi) = self
                .config
                .bm25_fields
                .iter()
                .position(|n| *n == field.field)
            else {
                return Err(Status::invalid_argument(format!(
                    "unknown field {:?}; this shard indexes {:?}",
                    field.field, self.config.bm25_fields
                )));
            };
            if field.text.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "field {:?} has empty text; omit absent fields instead",
                    field.field
                )));
            }
            accepted.push(fi);
        }
        let mut extras = Vec::with_capacity(accepted.len());
        for (slot, (field, fi)) in doc.fields.iter().zip(accepted).enumerate() {
            let tag = streams.submit(field.analysis.as_ref(), &field.text).await?;
            route.insert(tag, (sequence, slot));
            extras.push((fi, None));
        }
        Ok(extras)
    }

    /// Compile (or fetch the cached compilation of) one ingest
    /// materialization spec. Validation is whole-spec: empty or
    /// duplicate names, an unset kind, or an expression that does not
    /// compile all refuse before any document is touched.
    fn compiled_materialize(
        &self,
        spec: &crate::pb::MaterializeSpec,
    ) -> Result<Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>, Status> {
        let mut cache = self
            .materialize_cache
            .lock()
            .expect("materialize cache lock poisoned");
        if let Some(compiled) = cache.as_ref() {
            if compiled.spec == *spec {
                return Ok(compiled.columns.clone());
            }
        }
        let mut names = std::collections::HashSet::new();
        let mut columns = Vec::with_capacity(spec.columns.len());
        for column in &spec.columns {
            if column.name.is_empty() {
                return Err(Status::invalid_argument(
                    "materialize: a derived column needs a non-empty name",
                ));
            }
            if !names.insert(column.name.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "materialize: duplicate derived column {:?}",
                    column.name
                )));
            }
            let kind = match crate::pb::MaterializeKind::try_from(column.kind) {
                Ok(crate::pb::MaterializeKind::F64) => crate::pb::MaterializeKind::F64,
                Ok(crate::pb::MaterializeKind::I64) => crate::pb::MaterializeKind::I64,
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {:?} declares no target kind; kinds are \
                         explicit (MATERIALIZE_KIND_F64 or MATERIALIZE_KIND_I64), \
                         never inferred from data",
                        column.name
                    )))
                }
            };
            let expr = crate::cel::compile_value(&column.expression).map_err(|e| {
                Status::invalid_argument(format!(
                    "materialize: column {:?}: {}",
                    column.name,
                    e.message()
                ))
            })?;
            columns.push((column.name.clone(), expr, kind));
        }
        *cache = Some(CompiledMaterialize {
            spec: spec.clone(),
            columns: columns.clone(),
        });
        Ok(columns)
    }

    /// Materialize derived columns (docs/cel-values.md): evaluate each
    /// declared expression against THIS document's own values and push
    /// the results into the ordinary `numerics` / `integers` lists, so
    /// name resolution, the duplicate refusal, the apply, and the WAL
    /// record all take the path they already take. Runs AFTER the
    /// quality and geography layers so their derived columns are
    /// readable inputs. Clearing the spec makes replay exact: the
    /// logged request carries the values, so replay never evaluates
    /// twice. An absent result stores nothing (the Kleene rule); a type
    /// conflict the document exposes refuses loudly.
    fn materialize_columns(
        &self,
        mut doc: AddDocumentsRequest,
    ) -> Result<AddDocumentsRequest, Status> {
        let Some(spec) = doc.materialize.take() else {
            return Ok(doc);
        };
        if spec.columns.is_empty() {
            return Ok(doc);
        }
        let compiled = self.compiled_materialize(&spec)?;
        let mut env = crate::values::IngestEnv::default();
        for nv in &doc.numerics {
            env.numerics.insert(nv.field.clone(), nv.value);
        }
        for iv in &doc.integers {
            env.integers.insert(iv.field.clone(), iv.value);
        }
        for entry in &doc.map_numerics {
            env.map_numerics
                .insert((entry.field.clone(), entry.key.clone()), entry.value);
        }
        for (name, expr, kind) in &compiled {
            let value = crate::values::eval_ingest(expr, &env).map_err(|e| {
                Status::invalid_argument(format!("materialize: column {name:?}: {}", e.message()))
            })?;
            match value {
                None => {}
                Some(crate::values::IngestVal::Bool(_)) => {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {name:?} evaluated a boolean; a stored \
                         column holds numbers — wrap the expression in a ternary \
                         (`cond ? 1 : 0`)"
                    )));
                }
                Some(crate::values::IngestVal::Double(v)) => {
                    if *kind != crate::pb::MaterializeKind::F64 {
                        return Err(Status::invalid_argument(format!(
                            "materialize: column {name:?} declares I64 but its \
                             expression evaluated double on this document; stock CEL \
                             does not coerce — align the kind or the expression"
                        )));
                    }
                    doc.numerics.push(crate::pb::NumericValue {
                        field: name.clone(),
                        value: v,
                    });
                }
                Some(crate::values::IngestVal::Int(v)) => {
                    if *kind != crate::pb::MaterializeKind::I64 {
                        return Err(Status::invalid_argument(format!(
                            "materialize: column {name:?} declares F64 but its \
                             expression evaluated int on this document; write \
                             double(...) to land it in the f64 family"
                        )));
                    }
                    // i64::MIN is the i64 column's absence sentinel, so
                    // the one unrepresentable computed value stores as
                    // ABSENT — the same edge the checked arithmetic
                    // already maps to absence.
                    if v == i64::MIN {
                        continue;
                    }
                    doc.integers.push(crate::pb::IntegerValue {
                        field: name.clone(),
                        value: v,
                    });
                }
            }
        }
        Ok(doc)
    }

    /// Derive or validate phrase postings, materialize entity map entries on
    /// the first pass, and install the dedicated analyzed field. The returned
    /// request is the durable WAL form.
    fn materialize_phrases(
        &self,
        mut doc: AddDocumentsRequest,
        mut analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        let Some(index) = &self.phrase_index else {
            if !doc.phrases.is_empty()
                || doc.phrase_fingerprint != 0
                || !doc.phrase_field.is_empty()
            {
                return Err(Status::failed_precondition(
                    "document carries derived phrase data but this node has no phrase glossary configured",
                ));
            }
            return Ok((doc, analyzed));
        };
        let expected = index.fingerprint();
        let fresh = doc.phrase_fingerprint == 0;
        if fresh {
            if !doc.phrases.is_empty() {
                return Err(Status::invalid_argument(
                    "client-supplied phrase postings require their non-zero vocabulary fingerprint",
                ));
            }
            doc.phrases = index.postings(&doc.text);
            doc.phrase_fingerprint = expected;
            doc.phrase_field = index.phrase_field().to_string();
        } else if doc.phrase_fingerprint != expected {
            return Err(Status::failed_precondition(format!(
                "document phrase fingerprint {:016x} differs from configured vocabulary {:016x}; rebuild or replay with the matching glossary",
                doc.phrase_fingerprint, expected
            )));
        } else if doc.phrase_field != index.phrase_field() {
            return Err(Status::failed_precondition(format!(
                "document phrase field {:?} differs from configured field {:?}",
                doc.phrase_field,
                index.phrase_field()
            )));
        }
        for posting in &doc.phrases {
            if posting.term != protomolt_analyzer::phrase_posting_term(&posting.concept_id)
                || posting.field != index.phrase_field()
                || posting.concept_id.is_empty()
                || posting.token_count == 0
                || posting.offsets.is_empty()
                || posting.offsets.iter().any(|span| span.start >= span.end)
            {
                return Err(Status::invalid_argument(format!(
                    "invalid durable phrase posting for concept {:?}",
                    posting.concept_id
                )));
            }
        }
        // A non-zero fingerprint marks a replay-ready document. Its entity
        // entries were already derived before the WAL append, so never run a
        // possibly changed NER model over it again.
        if fresh {
            let mut derived = index.glossary_entities(&doc.phrases);
            if let Some(field) = index.entity_map_field() {
                derived.extend(crate::phrases::ner_entities(field, &analyzed.entities));
            }
            for entry in derived {
                match doc
                    .map_facets
                    .iter()
                    .find(|held| held.field == entry.field && held.key == entry.key)
                {
                    Some(held) if held.value == entry.value => {}
                    Some(_) => {
                        return Err(Status::invalid_argument(format!(
                            "derived entity map key {:?} conflicts with a client value",
                            entry.key
                        )))
                    }
                    None => doc.map_facets.push(entry),
                }
            }
        }
        let field_index = self
            .config
            .bm25_fields
            .iter()
            .position(|field| field == index.phrase_field())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "configured phrase field {:?} is absent from this node's BM25 field table",
                    index.phrase_field()
                ))
            })?;
        if analyzed.fields.len() <= field_index {
            analyzed
                .fields
                .resize_with(field_index + 1, Default::default);
        }
        if analyzed.fields[field_index] != crate::postings::AnalyzedField::default() {
            return Err(Status::invalid_argument(format!(
                "derived phrase field {:?} collides with supplied analyzed data",
                index.phrase_field()
            )));
        }
        analyzed.fields[field_index] = crate::phrases::analyzed_field(&doc.phrases);
        Ok((doc, analyzed))
    }

    /// Derive the proximity payloads (`docs/phrase-proximity.md`) and pin
    /// the document's durable proximity record: which fields keep token
    /// positions, and which bigram columns derive from which sources.
    ///
    /// Fresh ingest (an empty record) fills both from this node's
    /// configuration; a record that already names them — a replayed WAL
    /// entry, a resharded child, a client that copied one — must agree
    /// with the configuration exactly, or the document refuses by name:
    /// a column that is positional on some documents and not on others
    /// is a phrase index that lies about half the corpus.
    ///
    /// A positional field needs the analysis to have carried token
    /// positions (the native tokenizer always does; a sidecar response
    /// without a token layer cannot), and needs FULL term vectors — a
    /// scoring-only field has no occurrences to place, so a phrase over
    /// it could never match and would silently return nothing. Bigram
    /// columns derive from the source's positions by the same rule.
    /// Admit a write only for this node's collection (`docs/collections.md`):
    /// an empty name means "this node's", any other name refuses.
    fn admit_collection(&self, requested: &str) -> Result<(), Status> {
        if requested.is_empty() || requested == self.config.collection {
            return Ok(());
        }
        Err(if self.config.collection.is_empty() {
            Status::invalid_argument(format!(
                "collection {requested:?} named, but this node serves no named collection"
            ))
        } else {
            Status::invalid_argument(format!(
                "this node serves collection {:?}, not {requested:?}",
                self.config.collection
            ))
        })
    }

    fn materialize_proximity(
        &self,
        mut doc: AddDocumentsRequest,
        mut analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        use crate::proximity::{bigram_field_name, derive_bigrams};
        // A shard belongs to only one collection (docs/collections.md):
        // a document naming another refuses, and an unnamed one is written
        // with this node's before it is logged, so a replay carries it.
        self.admit_collection(&doc.collection)?;
        doc.collection = self.config.collection.clone();
        let configured_bigrams: Vec<crate::pb::BigramField> = self
            .config
            .bigram_fields
            .iter()
            .map(|source| crate::pb::BigramField {
                source: source.clone(),
                field: bigram_field_name(source),
            })
            .collect();
        let fresh = doc.position_fields.is_empty()
            && doc.bigram_fields.is_empty()
            && doc.sentence_fields.is_empty();
        if fresh {
            doc.position_fields = self.config.position_fields.clone();
            doc.bigram_fields = configured_bigrams.clone();
            doc.sentence_fields = self.config.sentence_fields.clone();
        } else {
            let mut held: Vec<&str> = doc.sentence_fields.iter().map(String::as_str).collect();
            let mut want: Vec<&str> = self
                .config
                .sentence_fields
                .iter()
                .map(String::as_str)
                .collect();
            held.sort_unstable();
            want.sort_unstable();
            if held != want {
                return Err(Status::failed_precondition(format!(
                    "document records sentence spans on {held:?} but this node keeps them on \
                     {want:?}; rebuild or replay with the matching --sentence-fields"
                )));
            }
            let mut held: Vec<&str> = doc.position_fields.iter().map(String::as_str).collect();
            let mut want: Vec<&str> = self
                .config
                .position_fields
                .iter()
                .map(String::as_str)
                .collect();
            held.sort_unstable();
            want.sort_unstable();
            if held != want {
                return Err(Status::failed_precondition(format!(
                    "document records token positions on {held:?} but this node keeps them on \
                     {want:?}; rebuild or replay with the matching --position-fields"
                )));
            }
            let mut held: Vec<(&str, &str)> = doc
                .bigram_fields
                .iter()
                .map(|b| (b.source.as_str(), b.field.as_str()))
                .collect();
            let mut want: Vec<(&str, &str)> = configured_bigrams
                .iter()
                .map(|b| (b.source.as_str(), b.field.as_str()))
                .collect();
            held.sort_unstable();
            want.sort_unstable();
            if held != want {
                return Err(Status::failed_precondition(format!(
                    "document records bigram columns {held:?} but this node derives {want:?}; \
                     rebuild or replay with the matching --bigram-fields"
                )));
            }
        }
        if doc.position_fields.is_empty()
            && doc.bigram_fields.is_empty()
            && doc.sentence_fields.is_empty()
        {
            return Ok((doc, analyzed));
        }
        let table = &self.config.bm25_fields;
        let field_index = |name: &str| -> Result<usize, Status> {
            table.iter().position(|n| n == name).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "proximity field {name:?} is absent from this node's BM25 field table {table:?}"
                ))
            })
        };
        // The spec a field was analyzed under: the body's, or the extra
        // field's own. Positions need FULL vectors.
        let scoring_only = |fi: usize| -> bool {
            let spec = if fi == 0 {
                doc.analysis.as_ref()
            } else {
                doc.fields
                    .iter()
                    .find(|f| f.field == table[fi])
                    .and_then(|f| f.analysis.as_ref())
            };
            spec.is_some_and(|s| {
                s.term_vector_mode == crate::analyzer::TERM_VECTOR_MODE_SCORING_ONLY
            })
        };
        let require_positions = |analyzed: &crate::postings::AnalyzedDoc,
                                 fi: usize,
                                 role: &str|
         -> Result<(), Status> {
            let Some(field) = analyzed.fields.get(fi) else {
                // The document does not carry this field at all: there
                // is nothing to position and nothing to derive.
                return Ok(());
            };
            if field.terms.is_empty() {
                return Ok(());
            }
            if scoring_only(fi) {
                return Err(Status::invalid_argument(format!(
                    "{role} {:?} requires FULL term vectors: a SCORING_ONLY analysis has no \
                     occurrences to place, so no phrase could ever match it",
                    table[fi]
                )));
            }
            if field.positions.is_none() {
                return Err(Status::failed_precondition(format!(
                    "{role} {:?} needs token positions, but the analysis of this document carried \
                     no token layer to derive them from; positions are never guessed from spans",
                    table[fi]
                )));
            }
            field.check_positions().map_err(|error| {
                Status::invalid_argument(format!(
                    "{role} {:?}: malformed token positions: {error}",
                    table[fi]
                ))
            })
        };
        for name in &doc.position_fields {
            let fi = field_index(name)?;
            require_positions(&analyzed, fi, "positional field")?;
        }
        // A sentence field needs the sentence layer on every document,
        // shaped so the query path can trust it: sorted, non-overlapping,
        // and covering every occurrence (docs/highlighting.md). A
        // document with terms but no sentence covering them is a broken
        // analysis contract, refused here, not a snippet-less hit later.
        for name in &doc.sentence_fields {
            let fi = field_index(name)?;
            let Some(field) = analyzed.fields.get(fi) else {
                continue;
            };
            if field.sentences.is_none() {
                return Err(Status::failed_precondition(format!(
                    "sentence field {name:?}: the analysis carried no sentence layer, so the \
                     document cannot be indexed with sentence spans; the analysis backend must \
                     return its sentence layer (the sidecar's sentence_detection)"
                )));
            }
            field.check_sentences().map_err(|error| {
                Status::failed_precondition(format!(
                    "sentence field {name:?}: malformed sentence spans: {error}"
                ))
            })?;
        }
        for bigram in &doc.bigram_fields {
            let source = field_index(&bigram.source)?;
            let derived = field_index(&bigram.field)?;
            if bigram.field != bigram_field_name(&bigram.source) {
                return Err(Status::invalid_argument(format!(
                    "bigram column {:?} must be named {:?}",
                    bigram.field,
                    bigram_field_name(&bigram.source)
                )));
            }
            require_positions(&analyzed, source, "bigram source field")?;
            let Some(source_field) = analyzed.fields.get(source) else {
                continue;
            };
            if source_field.terms.is_empty() {
                continue;
            }
            let column = derive_bigrams(source_field).map_err(|error| {
                Status::invalid_argument(format!(
                    "bigram column {:?} from {:?}: {error}",
                    bigram.field, bigram.source
                ))
            })?;
            if analyzed.fields.len() <= derived {
                analyzed.fields.resize_with(derived + 1, Default::default);
            }
            if analyzed.fields[derived] != crate::postings::AnalyzedField::default() {
                return Err(Status::invalid_argument(format!(
                    "bigram column {:?} collides with supplied analyzed data",
                    bigram.field
                )));
            }
            analyzed.fields[derived] = column;
        }
        Ok((doc, analyzed))
    }

    /// The pre-lock half of [`Self::apply_analyzed_document`]: derive or
    /// validate the document's proximity, phrase, quality, geography, and
    /// materialized columns. On a document that already went through it
    /// (a WAL record) every step validates instead of deriving, so the
    /// compaction shadow replays records through the same function.
    pub(crate) fn materialize_document(
        &self,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
    ) -> Result<(AddDocumentsRequest, crate::postings::AnalyzedDoc), Status> {
        let (doc, analyzed) = self.materialize_proximity(doc, analyzed)?;
        let (doc, analyzed) = self.materialize_phrases(doc, analyzed)?;
        let doc = materialize_quality(doc, &analyzed)?;
        let doc = materialize_geography(doc, &analyzed)?;
        let doc = self.materialize_columns(doc)?;
        Ok((doc, analyzed))
    }

    /// Apply one analyzed document: id assignment, store insert, WAL
    /// append. Must be called in arrival order — both transports
    /// guarantee it.
    fn apply_analyzed_document(
        &self,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        vector: Option<Vec<f32>>,
        stable_routing_key: Option<Vec<u8>>,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        // Quality columns are materialized BEFORE anything else looks at
        // the request (docs/quality-columns.md): the derived values join
        // the ordinary `numerics` / `integers` lists, so name resolution,
        // the duplicate-column refusal, the apply, and the WAL record all
        // take the one path they already took. Clearing the spec is what
        // makes replay exact — the logged request carries the values, so
        // replay never calls the sidecar and never derives twice.
        let (doc, analyzed) = self.materialize_document(doc, analyzed)?;
        let mut guard = self.state.write().expect("shard state lock poisoned");
        self.apply_document_locked(
            &mut guard,
            doc,
            analyzed,
            vector,
            stable_routing_key,
            added,
            first_id,
        )
    }

    /// The durable-form document of [`Self::materialize_document`],
    /// applied against a state the caller holds (see
    /// [`Self::apply_batch_locked`]): id assignment, store insert, WAL
    /// append, and the mapped vector in lockstep.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_document_locked(
        &self,
        guard: &mut ShardState,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        vector: Option<Vec<f32>>,
        stable_routing_key: Option<Vec<u8>>,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        // A disk-resident shard that receives more documents is first
        // reloaded into the heap builder (the append path is
        // bulk-load: build in memory, flush back to v3).
        if matches!(guard.bm25, Some(Bm25Shard::Resident(_))) {
            let bm25_path = self
                .config
                .index_path
                .as_ref()
                .map(|p| storage_paths(p, guard.generation.as_ref()).2)
                .ok_or_else(|| {
                    Status::failed_precondition("resident shard has no index path to reload from")
                })?;
            let store = Bm25Store::load(&bm25_path)
                .map_err(|e| Status::internal(format!("reload {}: {e}", bm25_path.display())))?;
            guard.bm25 = Some(Bm25Shard::Building(store));
        }
        // Shared positional id space with the vector side: the next id
        // is past both indexes' tips.
        let vector_tip = guard.index.as_ref().map_or(0, |i| i.len() as u32);
        // Mapped ingest carries the document's vector, applied in
        // LOCKSTEP below. Validate it BEFORE anything mutates, the same
        // rule every column list follows: dimension against the index,
        // coordinates finite, and — after doc_id is known — the tips in
        // agreement, so a document that fails never half-enters either
        // leg.
        let vector_dim = match vector.as_deref() {
            Some(v) => {
                let dim = v.len();
                if let Some(known) = guard.index.as_ref().and_then(|i| i.dim_opt()) {
                    if known != dim {
                        return Err(Status::invalid_argument(format!(
                            "mapped vector has {dim} floats but the shard's index is dim {known}"
                        )));
                    }
                }
                if let Some((_, ci, value)) = first_invalid_coordinate(v, dim) {
                    return Err(Status::invalid_argument(format!(
                        "mapped vector coordinate {ci} is {value}; vectors must be finite"
                    )));
                }
                if guard.index.is_none() {
                    // From-scratch: same single-shard convenience the
                    // AddVectors path allows.
                    let created = self.fresh_index(guard.bm25.as_ref(), dim)?;
                    guard.index = Some(created);
                }
                if guard.exact_vectors.is_none() {
                    if vector_tip != 0 {
                        return Err(Status::failed_precondition(format!(
                            "the shard has {vector_tip} provider vectors but no exact-vector \
                             sidecar; rebuild or backfill the generation before mapped ingest"
                        )));
                    }
                    let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
                    guard.exact_vectors = Some(store);
                }
                let exact = guard.exact_vectors.as_ref().expect("ensured above");
                if exact.len() != vector_tip as usize || exact.dim() != Some(dim) {
                    return Err(Status::failed_precondition(format!(
                        "exact-vector sidecar shape {:?}x{} does not match provider shape \
                         {dim}x{vector_tip}",
                        exact.dim(),
                        exact.len()
                    )));
                }
                Some(dim)
            }
            None => None,
        };
        if guard.bm25.is_none() {
            let builder = self.new_builder(guard.generation.as_ref())?;
            guard.bm25 = Some(builder);
            // A vector index created before the first document (a
            // calibration, or vectors ingested first) becomes the
            // segmented provider's tail now that the catalog exists, so
            // its rows seal with the documents (docs/immutable-segments.md).
            let snapshot = match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(g)) => Some(g.snapshot().clone()),
                _ => None,
            };
            if let Some(set) = snapshot {
                if guard
                    .index
                    .as_ref()
                    .is_some_and(|index| index.as_segmented().is_none())
                {
                    let plain = guard.index.take().expect("checked above");
                    let provider = SegmentedProvider::adopt(set, plain)
                        .map_err(|e| Status::failed_precondition(format!("{e}")))?;
                    guard.index = Some(VectorIndex::from_provider(provider));
                }
            }
        }
        let doc_id = vector_tip.max(
            guard
                .bm25
                .as_ref()
                .expect("builder just ensured")
                .next_doc_id(),
        );
        // The lockstep rule: a mapped document's vector lands at the
        // SAME id, which is only true when the vector leg's tip is the
        // id being assigned. A shard whose document leg ran ahead (per-
        // leg ingest history) cannot take mapped documents — the vector
        // would land below its document and silently corrupt every
        // hybrid result, so it refuses by name instead.
        if vector.is_some() && u64::from(doc_id) != u64::from(vector_tip) {
            return Err(Status::failed_precondition(format!(
                "the shard's document leg is ahead of its vector leg ({} documents, \
                 {vector_tip} vectors); mapped ingest appends both legs in lockstep — \
                 rebuild the shard or backfill vectors with AddVectors first",
                doc_id
            )));
        }
        // Multi-field documents were positioned against the CONFIGURED
        // table; the ACTIVE table (possibly loaded from a file) must be
        // at least as wide and agree on names, or the document would
        // index under the wrong field — refuse as a Status instead of
        // tripping the store's positional assert. Body-only documents
        // skip this entirely (any table serves them).
        if analyzed.fields.len() > 1 {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            if analyzed.fields.len() > shard.field_count() {
                return Err(Status::failed_precondition(format!(
                    "document carries {} fields but the shard's table has {}; the shard \
                     predates the configured field table — rebuild or reshard it",
                    analyzed.fields.len(),
                    shard.field_count()
                )));
            }
            for fi in 1..analyzed.fields.len() {
                let want = self.config.bm25_fields.get(fi).map(String::as_str);
                if want != Some(shard.field_name(fi)) {
                    return Err(Status::failed_precondition(format!(
                        "shard field {fi} is {:?} but the configured table names {:?}; \
                         field tables must agree",
                        shard.field_name(fi),
                        want.unwrap_or("<missing>")
                    )));
                }
            }
        }
        // Positions agree between the record and the ACTIVE storage
        // (docs/phrase-proximity.md). A file the node loaded declares
        // its own positional fields; a document that keeps positions on
        // a field the file never declared would build a column that is
        // positional for new documents only, and a document without
        // positions into a field that keeps them would leave a hole the
        // store cannot represent. Both refuse here, before anything
        // mutates, naming the field and the fix.
        {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            for name in &doc.position_fields {
                let Some(fi) = self.config.bm25_fields.iter().position(|n| n == name) else {
                    continue; // refused upstream
                };
                if fi < shard.field_count() && !shard.field_has_positions(fi) {
                    return Err(Status::failed_precondition(format!(
                        "shard field {name:?} predates token positions (its storage declares \
                         none); rebuild or reshard the generation before positional ingest"
                    )));
                }
            }
            for name in &doc.sentence_fields {
                let Some(fi) = self.config.bm25_fields.iter().position(|n| n == name) else {
                    continue; // refused upstream
                };
                if fi < shard.field_count() && !shard.field_has_sentences(fi) {
                    return Err(Status::failed_precondition(format!(
                        "shard field {name:?} predates sentence spans (its storage declares \
                         none); rebuild or reshard the generation before sentence ingest"
                    )));
                }
            }
            for (fi, field) in analyzed.fields.iter().enumerate() {
                if fi < shard.field_count()
                    && shard.field_has_positions(fi)
                    && !field.terms.is_empty()
                    && field.positions.is_none()
                {
                    return Err(Status::failed_precondition(format!(
                        "shard field {:?} keeps token positions but this document's analysis \
                         carried none; positions are never guessed from spans",
                        shard.field_name(fi)
                    )));
                }
            }
        }
        // Record which analyzer produced each column, and refuse a
        // document that contradicts one already recorded. Field NAME
        // agreement (just above) does not catch this: two documents can
        // agree on the name `body_norm` and disagree on what a term IS,
        // and nothing downstream would notice. Half a column folded and
        // half not scores both halves against one idf.
        {
            let shard = guard.bm25.as_mut().expect("builder just ensured");
            let body = crate::analyzer::analysis_fingerprint(doc.analysis.as_ref());
            shard
                .set_analysis_fingerprint(0, body)
                .map_err(Status::failed_precondition)?;
            // The cased field is fingerprinted as the twin of the body's
            // spec: what the same pass computed for it.
            if let Some(ci) = cased_field_index(&self.config, self.phrase_index.as_deref(), &doc)? {
                let spec = doc
                    .analysis
                    .as_ref()
                    .expect("cased_field_index requires an explicit body spec");
                let twin = crate::analyzer::cased_twin_spec(spec);
                shard
                    .set_analysis_fingerprint(
                        ci,
                        crate::analyzer::analysis_fingerprint(Some(&twin)),
                    )
                    .map_err(Status::failed_precondition)?;
            }
            for field in &doc.fields {
                let Some(fi) = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|n| *n == field.field)
                else {
                    continue; // already refused upstream
                };
                let fingerprint = crate::analyzer::analysis_fingerprint(field.analysis.as_ref());
                shard
                    .set_analysis_fingerprint(fi, fingerprint)
                    .map_err(Status::failed_precondition)?;
            }
            if let Some(phrases) = &self.phrase_index {
                let fi = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|field| field == phrases.phrase_field())
                    .expect("phrase field validated at configuration");
                shard
                    .set_analysis_fingerprint(fi, phrases.fingerprint())
                    .map_err(Status::failed_precondition)?;
            }
            // A bigram column's identity is its source's analyzer plus
            // the derivation: two columns built from differently
            // analyzed sources hold different pairs under one name.
            for bigram in &doc.bigram_fields {
                let (Some(source), Some(derived)) = (
                    self.config
                        .bm25_fields
                        .iter()
                        .position(|n| *n == bigram.source),
                    self.config
                        .bm25_fields
                        .iter()
                        .position(|n| *n == bigram.field),
                ) else {
                    continue; // refused upstream
                };
                let source_fingerprint = shard.analysis_fingerprint(source);
                if source_fingerprint != 0 {
                    shard
                        .set_analysis_fingerprint(
                            derived,
                            crate::proximity::bigram_fingerprint(source_fingerprint),
                        )
                        .map_err(Status::failed_precondition)?;
                }
            }
        }
        // Facet values: refuse unknown fields, repeats, and empty
        // values BEFORE anything mutates — a document that fails
        // validation must never half-enter the store or reach the log.
        let facet_slots: Vec<(usize, String)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.facets.len());
            for fv in &doc.facets {
                if seen.contains(&fv.field.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "facet field {:?} repeats in one document",
                        fv.field
                    )));
                }
                seen.push(&fv.field);
                let Some(fi) = shard.facet_index(&fv.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown facet field {:?}; this shard's facet table \
                         (--facet-fields) does not have it",
                        fv.field
                    )));
                };
                if fv.value.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "facet field {:?} has an empty value; omit absent facets instead",
                        fv.field
                    )));
                }
                slots.push((fi, fv.value.clone()));
            }
            slots
        };
        // Numeric values: same shape as facets — unknown fields,
        // repeats, and non-finite values refused before anything
        // mutates (NaN is the absence sentinel, infinities break the
        // score-function bound algebra).
        let numeric_slots: Vec<(usize, f64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.numerics.len());
            for nv in &doc.numerics {
                if seen.contains(&nv.field.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "numeric field {:?} repeats in one document",
                        nv.field
                    )));
                }
                seen.push(&nv.field);
                let Some(ni) = shard.numeric_index(&nv.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown numeric field {:?}; this shard's numeric table \
                         (--numeric-fields) does not have it",
                        nv.field
                    )));
                };
                if !nv.value.is_finite() {
                    return Err(Status::invalid_argument(format!(
                        "numeric field {:?} has a non-finite value; omit absent values instead",
                        nv.field
                    )));
                }
                slots.push((ni, nv.value));
            }
            slots
        };
        // Map entries (docs/map-columns.md): unknown columns, empty
        // keys, repeated (column, key) pairs, empty string values, and
        // non-finite numeric values all refuse before anything mutates.
        let map_facet_slots: Vec<(usize, &str, &str)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<(&str, &str)> = Vec::new();
            let mut slots = Vec::with_capacity(doc.map_facets.len());
            for e in &doc.map_facets {
                if e.key.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?}: empty keys are refused (almost always a producer bug)",
                        e.field
                    )));
                }
                if seen.contains(&(e.field.as_str(), e.key.as_str())) {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} repeats in one document (a map holds one \
                         value per key)",
                        e.field, e.key
                    )));
                }
                seen.push((&e.field, &e.key));
                let Some(ci) = shard.map_facet_index(&e.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown map column {:?}; this shard's map-facet table \
                         (--map-facet-fields) does not have it",
                        e.field
                    )));
                };
                if e.value.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} has an empty value; omit absent entries",
                        e.field, e.key
                    )));
                }
                slots.push((ci, e.key.as_str(), e.value.as_str()));
            }
            slots
        };
        let map_numeric_slots: Vec<(usize, &str, f64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<(&str, &str)> = Vec::new();
            let mut slots = Vec::with_capacity(doc.map_numerics.len());
            for e in &doc.map_numerics {
                if e.key.is_empty() {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?}: empty keys are refused (almost always a producer bug)",
                        e.field
                    )));
                }
                if seen.contains(&(e.field.as_str(), e.key.as_str())) {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} repeats in one document (a map holds one \
                         value per key)",
                        e.field, e.key
                    )));
                }
                seen.push((&e.field, &e.key));
                let Some(ci) = shard.map_numeric_index(&e.field) else {
                    return Err(Status::invalid_argument(format!(
                        "unknown map column {:?}; this shard's map-numeric table \
                         (--map-numeric-fields) does not have it",
                        e.field
                    )));
                };
                if !e.value.is_finite() {
                    return Err(Status::invalid_argument(format!(
                        "map column {:?} key {:?} has a non-finite value; omit absent entries",
                        e.field, e.key
                    )));
                }
                slots.push((ci, e.key.as_str(), e.value));
            }
            slots
        };
        // Integer values, and timestamps as sugar over them: both land
        // in the SAME i64 table, so "repeats in one document" spans the
        // two lists. Same refuse-before-mutating shape as the others,
        // plus the sentinel rule — i64::MIN means absent in the column,
        // so a document may not hold it.
        let integer_slots: Vec<(usize, i64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.integers.len() + doc.timestamps.len());
            let resolve = |field: &str, seen: &[&str]| -> Result<usize, Status> {
                if seen.contains(&field) {
                    return Err(Status::invalid_argument(format!(
                        "integer field {field:?} repeats in one document (integers and \
                         timestamps name the same columns)"
                    )));
                }
                shard.integer_index(field).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "unknown integer field {field:?}; this shard's integer table \
                         (--integer-fields) does not have it"
                    ))
                })
            };
            for iv in &doc.integers {
                let ii = resolve(&iv.field, &seen)?;
                seen.push(&iv.field);
                if iv.value == crate::postings::INTEGER_ABSENT {
                    return Err(Status::invalid_argument(format!(
                        "integer field {:?}: i64::MIN is the column's absence sentinel and \
                         cannot be a value; omit absent values instead",
                        iv.field
                    )));
                }
                slots.push((ii, iv.value));
            }
            for tv in &doc.timestamps {
                let ii = resolve(&tv.field, &seen)?;
                seen.push(&tv.field);
                let Some(ts) = tv.value.as_ref() else {
                    return Err(Status::invalid_argument(format!(
                        "timestamp field {:?} carries no instant; omit the entry instead",
                        tv.field
                    )));
                };
                slots.push((ii, timestamp_to_epoch_micros(&tv.field, ts)?));
            }
            slots
        };
        // Geo points (docs/geo-columns.md). Same refuse-before-mutating
        // shape as the others: every entry resolves and validates before
        // a single value is written, so a document that names one bad
        // coordinate leaves no half-applied point behind.
        let geo_slots: Vec<(usize, f64, f64)> = {
            let shard = guard.bm25.as_ref().expect("builder just ensured");
            let mut seen: Vec<&str> = Vec::new();
            let mut slots = Vec::with_capacity(doc.geo_points.len());
            for gp in &doc.geo_points {
                if seen.contains(&gp.field.as_str()) {
                    return Err(Status::invalid_argument(format!(
                        "geo field {:?} repeats in one document (a document holds one point \
                         per column)",
                        gp.field
                    )));
                }
                seen.push(&gp.field);
                let gi = shard.geo_index(&gp.field).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "unknown geo field {:?}; this shard's geo table (--geo-fields) does \
                         not have it",
                        gp.field
                    ))
                })?;
                validate_lat_lon(&format!("geo field {:?}", gp.field), gp.lat, gp.lon)?;
                slots.push((gi, gp.lat, gp.lon));
            }
            slots
        };
        let global_id = self.config.slot_offset + u64::from(doc_id);
        if *added == 0 {
            *first_id = global_id;
        }
        // Apply first, log after, as for vectors: a document that
        // fails to enter the store must never reach the log, or its
        // id would be reassigned and poison the replay.
        let lineage = doc.lineage.map(|l| crate::postings::DocLineage {
            parent_id: l.parent_id,
            group_id: l.group_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        match guard.bm25.as_mut().expect("builder just ensured") {
            Bm25Shard::Segmented(g) => {
                let local = doc_id - g.tail_base();
                let store = g.tail_mut();
                store.add_document_with_lineage(local, doc.text.clone(), analyzed, lineage);
                for (fi, value) in &facet_slots {
                    store.set_facet(*fi, local, value);
                }
                for &(ni, value) in &numeric_slots {
                    store.set_numeric(ni, local, value);
                }
                for &(ci, key, value) in &map_facet_slots {
                    store.set_map_facet(ci, local, key, value);
                }
                for &(ci, key, value) in &map_numeric_slots {
                    store.set_map_numeric(ci, local, key, value);
                }
                for &(ii, value) in &integer_slots {
                    store.set_integer(ii, local, value);
                }
                for &(gi, lat, lon) in &geo_slots {
                    store.set_geo(gi, local, lat, lon);
                }
                g.sync_tail();
            }
            Bm25Shard::Building(store) => {
                store.add_document_with_lineage(doc_id, doc.text.clone(), analyzed, lineage);
                for (fi, value) in &facet_slots {
                    store.set_facet(*fi, doc_id, value);
                }
                for &(ni, value) in &numeric_slots {
                    store.set_numeric(ni, doc_id, value);
                }
                for &(ci, key, value) in &map_facet_slots {
                    store.set_map_facet(ci, doc_id, key, value);
                }
                for &(ci, key, value) in &map_numeric_slots {
                    store.set_map_numeric(ci, doc_id, key, value);
                }
                for &(ii, value) in &integer_slots {
                    store.set_integer(ii, doc_id, value);
                }
                for &(gi, lat, lon) in &geo_slots {
                    store.set_geo(gi, doc_id, lat, lon);
                }
            }
            Bm25Shard::Spilling(builder) => {
                builder
                    .add_document_with_lineage(doc_id, doc.text.clone(), analyzed, lineage)
                    .map_err(|e| Status::internal(format!("spill write: {e}")))?;
                for (fi, value) in &facet_slots {
                    builder.set_facet(*fi, doc_id, value);
                }
                for &(ni, value) in &numeric_slots {
                    builder.set_numeric(ni, doc_id, value);
                }
                for &(ci, key, value) in &map_facet_slots {
                    builder.set_map_facet(ci, doc_id, key, value);
                }
                for &(ci, key, value) in &map_numeric_slots {
                    builder.set_map_numeric(ci, doc_id, key, value);
                }
                for &(ii, value) in &integer_slots {
                    builder.set_integer(ii, doc_id, value);
                }
                for &(gi, lat, lon) in &geo_slots {
                    builder.set_geo(gi, doc_id, lat, lon);
                }
            }
            Bm25Shard::Resident(_) => {
                return Err(Status::internal("shard builder unavailable"));
            }
        }
        wal_append_or_degrade(
            &mut guard.wal,
            wal_record::Op::AddDocuments(LoggedAddDocuments {
                first_id: global_id,
                documents: vec![doc],
                stable_routing_keys: stable_routing_key.clone().into_iter().collect(),
            }),
        );
        // The mapped document's vector, at the same id, under the same
        // lock, with the same WAL record AddVectors writes — replay
        // rebuilds both legs from their own records. Failure here is
        // ruled out by the validation above; if it happens anyway the
        // legs have diverged by one and the next mapped document
        // refuses on the lockstep check, loudly.
        if let Some(v) = vector {
            let dim = vector_dim.expect("validated alongside the vector");
            let index = guard.index.as_mut().expect("ensured during validation");
            index.add(&v, dim).map_err(|e| {
                Status::internal(format!(
                    "vector apply failed after validation: {e}; the shard's legs may have \
                     diverged — the next mapped document will refuse if so"
                ))
            })?;
            guard
                .exact_vectors
                .as_mut()
                .expect("validated alongside provider index")
                .append(&v, dim)
                .map_err(|e| {
                    Status::internal(format!(
                        "exact-vector append failed after provider commit: {e}; refuse further \
                         ingest and rebuild this generation"
                    ))
                })?;
            let index = guard
                .index
                .as_ref()
                .expect("provider index remains present");
            let bit_width = index.bits_per_dimension().unwrap_or(self.config.bit_width);
            let committed_config = index.backend_config().map_err(|e| {
                Status::internal(format!(
                    "read vector backend config after mapped ingest: {e}"
                ))
            })?;
            if let Some(wal) = guard.wal.as_mut() {
                wal.update_manifest(|m| {
                    if m.dim == 0 {
                        m.dim = dim as u32;
                    }
                    m.bit_width = bit_width as u32;
                    m.set_backend_config(committed_config.clone());
                });
            }
            wal_append_or_degrade(
                &mut guard.wal,
                wal_record::Op::AddVectors(LoggedAddVectors {
                    first_id: global_id,
                    batch: Some(AddVectorsRequest {
                        vectors: v,
                        dim: dim as u32,
                    }),
                    stable_routing_keys: stable_routing_key.into_iter().collect(),
                }),
            );
        }
        guard.stats_epoch += 1;
        *added += 1;
        Ok(())
    }

    /// Validate one mapped bind against this shard: derive the plan
    /// (derivation is deterministic, so both sides compute it
    /// independently), hold it to the client's expected fingerprint,
    /// and refuse — up front, naming every gap at once — landing
    /// columns this shard does not declare. Nothing streams until the
    /// bind stands.
    fn bind_mapped(
        &self,
        bind: &crate::pb::MappedBind,
    ) -> Result<crate::mapping::Extractor, Status> {
        if bind.expected_fingerprint.is_empty() {
            return Err(Status::invalid_argument(
                "expected_fingerprint is required: dry-run the plan with PlanIndex first, \
                 review it, and bind the fingerprint you saw",
            ));
        }
        let extractor = crate::mapping::Extractor::new(
            &bind.descriptor_set,
            &bind.message_type,
            &bind.body_path,
        )?;
        let plan = extractor.plan();
        if plan.fingerprint != bind.expected_fingerprint {
            return Err(Status::failed_precondition(format!(
                "plan fingerprint mismatch: this node derives {} but the bind expects {}; \
                 the descriptor set or the derivation rules changed since the plan was \
                 reviewed — re-run PlanIndex and review the difference",
                plan.fingerprint, bind.expected_fingerprint
            )));
        }
        let mut missing: Vec<String> = Vec::new();
        for field in &plan.fields {
            use crate::pb::ColumnFamily;
            let name = &field.name;
            let (table, flag) = if field.family == ColumnFamily::TextField as i32 {
                if field.path == extractor.body_path() {
                    // The body is the top-level text; it needs no
                    // declared column.
                    continue;
                }
                if name == "body" {
                    return Err(Status::invalid_argument(format!(
                        "mapped text field {} lands as \"body\" but is not the bound body; \
                         rename it with a hint, or bind it as body_path",
                        field.path
                    )));
                }
                (&self.config.bm25_fields, "--bm25-fields")
            } else if field.family == ColumnFamily::Facet as i32 {
                (&self.config.facet_fields, "--facet-fields")
            } else if field.family == ColumnFamily::I64 as i32 {
                (&self.config.integer_fields, "--integer-fields")
            } else if field.family == ColumnFamily::F64 as i32 {
                (&self.config.numeric_fields, "--numeric-fields")
            } else {
                // VECTOR is the dense leg; NONE lands nowhere, visibly.
                continue;
            };
            if !table.iter().any(|declared| declared == name) {
                missing.push(format!("{name:?} ({flag})"));
            }
        }
        if !missing.is_empty() {
            return Err(Status::failed_precondition(format!(
                "the plan lands columns this shard does not declare: {}; declare them and \
                 restart the node, or revise the plan",
                missing.join(", ")
            )));
        }
        // The durable shard-level binding: the FIRST bind pins the
        // shard to this plan identity (recorded to the WAL now, to the
        // store's kind-6 entry at flush), and every later bind must
        // match it exactly. An index only ever pairs with the plan it
        // was written under; changing the mapping is a rebuild, never a
        // rebind.
        let incoming = crate::postings::StoredBinding {
            plan_fingerprint: plan.fingerprint.clone(),
            body_path: extractor.body_path().to_string(),
            materialize_sha: materialize_sha(bind.materialize.as_ref()),
        };
        let mut guard = self.state.write().expect("shard state lock poisoned");
        match &guard.mapped_binding {
            Some(bound) if *bound != incoming => {
                let mut differs = Vec::new();
                if bound.plan_fingerprint != incoming.plan_fingerprint {
                    differs.push(format!(
                        "the plan (bound {}, offered {})",
                        bound.plan_fingerprint, incoming.plan_fingerprint
                    ));
                }
                if bound.body_path != incoming.body_path {
                    differs.push(format!(
                        "the body (bound {:?}, offered {:?})",
                        bound.body_path, incoming.body_path
                    ));
                }
                if bound.materialize_sha != incoming.materialize_sha {
                    differs.push("the materialize spec".to_string());
                }
                Err(Status::failed_precondition(format!(
                    "this shard is durably bound to another mapping; {} differ{}. An index \
                     only ever pairs with the plan it was written under — rebuild or \
                     reshard to change the mapping",
                    differs.join(" and "),
                    if differs.len() == 1 { "s" } else { "" }
                )))
            }
            Some(_) => Ok(extractor),
            None => {
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::Bind(crate::pb::wal::LoggedBinding {
                        plan_fingerprint: incoming.plan_fingerprint.clone(),
                        body_path: incoming.body_path.clone(),
                        materialize_sha: incoming.materialize_sha.clone(),
                    }),
                );
                guard.mapped_binding = Some(incoming);
                Ok(extractor)
            }
        }
    }

    /// Bulk ingest over one AnalyzeStream: submissions run ahead of the
    /// apply point as far as the sidecar grants credit, results return
    /// in completion order, and the apply wavefront advances over
    /// consecutive sequences so application stays in arrival order.
    async fn ingest_streamed(
        &self,
        mut session: crate::analyzer::AnalyzeStream,
        first: IngestDoc,
        source: &mut IngestSource<'_>,
        addr: &str,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        fn store_result(
            results: &mut std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
            item: Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>,
        ) -> Result<(), Status> {
            match item {
                Some((sequence, Ok(analyzed))) => {
                    results.insert(sequence, analyzed);
                    Ok(())
                }
                // One document failing fails the ingest call, exactly as
                // a failed unary analysis did.
                Some((_, Err(status))) => Err(status),
                None => Err(Status::internal(
                    "analysis stream completed with documents in flight",
                )),
            }
        }
        /// Steer one field result into its document's slot. The route
        /// table is the only thing that knows which document a sequence
        /// belonged to, so an unknown sequence is a wire-level bug, not a
        /// recoverable condition.
        fn store_field(
            pending: &mut std::collections::BTreeMap<u64, PendingDoc>,
            route: &mut std::collections::HashMap<u64, (u64, usize)>,
            event: Option<FieldEvent>,
        ) -> Result<(), Status> {
            match event {
                Some(FieldEvent::Result(tag, result)) => {
                    let (sequence, slot) = route.remove(&tag).ok_or_else(|| {
                        Status::internal(format!("field result {tag} matches no submitted field"))
                    })?;
                    // One field failing fails the ingest call, exactly as
                    // a failed unary field analysis did.
                    let analyzed = result?;
                    let doc = pending.get_mut(&sequence).ok_or_else(|| {
                        Status::internal(format!(
                            "field result for document {sequence}, which is no longer pending"
                        ))
                    })?;
                    if doc.extras[slot].1.replace(analyzed).is_none() {
                        doc.outstanding -= 1;
                    }
                    Ok(())
                }
                Some(FieldEvent::StreamFailed(status)) => Err(status),
                None => Err(Status::internal(
                    "field analysis streams ended with fields in flight",
                )),
            }
        }
        // One Step lives at a time, on the stack of this loop; boxing
        // the request to shrink the enum would cost an allocation per
        // ingested document for no held memory.
        #[allow(clippy::large_enum_variant)]
        enum Step {
            Doc(IngestDoc),
            InboundClosed,
            Result(Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>),
            Field(Option<FieldEvent>),
        }
        let IngestDoc {
            req: first,
            vector: first_vector,
            stable_routing_key: first_stable_routing_key,
        } = first;
        let mut spec = first.analysis.clone();
        let mut quality = first.quality.clone();
        let mut geography = first.geography.clone();
        let mut cased_field = first.cased_field.clone();
        let mut submit = Some(session.submitter());
        // The body session covers the BODY only; extra fields ride their
        // own per-spec sessions, and a document applies once its body and
        // every one of its fields have landed.
        let mut fields = FieldStreams::new(addr);
        let mut route: std::collections::HashMap<u64, (u64, usize)> =
            std::collections::HashMap::new();
        let mut pending: std::collections::BTreeMap<u64, PendingDoc> =
            std::collections::BTreeMap::new();
        let mut results: std::collections::HashMap<u64, crate::postings::AnalyzedDoc> =
            std::collections::HashMap::new();
        let first_extras = self
            .submit_field_analyses(&first, 0, &mut fields, &mut route)
            .await?;
        submit
            .as_ref()
            .expect("submitter set above")
            .submit(0, &first.text)
            .await?;
        pending.insert(
            0,
            PendingDoc {
                doc: first,
                vector: first_vector,
                stable_routing_key: first_stable_routing_key,
                outstanding: first_extras.len(),
                extras: first_extras,
            },
        );
        let mut next_seq = 1u64;
        let mut next_apply = 0u64;
        let mut inbound_open = true;
        loop {
            self.advance_apply(&mut pending, &mut results, &mut next_apply, added, first_id)?;
            // A full tail seals here, between documents, so one long
            // stream never grows a segment past --seal-tail-docs.
            if self.seal_if_due().await? {
                continue;
            }
            if pending.is_empty() && !inbound_open {
                break;
            }
            // Guards, so a stream that owes nothing is never polled. The
            // body session owes exactly the pending documents whose body
            // result has not landed; once it is finished and drained it
            // yields `None` forever, which `store_result` would rightly
            // read as truncation.
            let want_body = pending.len() > results.len();
            let want_field = fields.pending();
            // At least one arm is always live: if a document is still
            // pending after `advance_apply`, the one at the wavefront is
            // owed either its body or a field.
            let step = tokio::select! {
                message = source.next(),
                    if inbound_open && pending.len() < MAX_PENDING => match message? {
                        Some(doc) => Step::Doc(doc),
                        None => Step::InboundClosed,
                    },
                result = session.next(), if want_body => Step::Result(result?),
                event = fields.recv(), if want_field => Step::Field(event),
            };
            match step {
                Step::Doc(doc) => {
                    let IngestDoc {
                        req: doc,
                        vector: doc_vector,
                        stable_routing_key,
                    } = doc;
                    // Extra-field analyses are queued on arrival
                    // (validated now, so a bad field fails before the
                    // body enters the session).
                    let extras = self
                        .submit_field_analyses(&doc, next_seq, &mut fields, &mut route)
                        .await?;
                    // The quality layers are requested in the session's
                    // options message, so a change to them reopens the
                    // session for the same reason a spec change does.
                    if doc.analysis != spec
                        || doc.quality != quality
                        || doc.geography != geography
                        || doc.cased_field != cased_field
                    {
                        // A mid-stream BODY spec change (rare): collect
                        // what the current session still owes so nothing
                        // is lost when it is replaced, then open a new
                        // one. Dropping the submitter clone is what lets
                        // the old session half-close and drain.
                        //
                        // Only the BODY session is being replaced. Field
                        // sessions are per-spec and outlive this, and
                        // draining them here would DEADLOCK: a sidecar
                        // may hold a result until more work arrives on
                        // that stream (the test mock deliberately does),
                        // and no more field work is coming until the new
                        // body session is open.
                        drop(submit.take());
                        session.finish();
                        while pending.len() > results.len() {
                            store_result(&mut results, session.next().await?)?;
                        }
                        self.advance_apply(
                            &mut pending,
                            &mut results,
                            &mut next_apply,
                            added,
                            first_id,
                        )?;
                        session = crate::analyzer::AnalyzeStream::open_with_vocab(
                            addr,
                            doc.analysis.as_ref(),
                            self.vocab.clone(),
                            session_layers(
                                &doc,
                                self.phrase_index.as_deref(),
                                &self.config.sentence_fields,
                            ),
                        )
                        .await?;
                        spec = doc.analysis.clone();
                        quality = doc.quality.clone();
                        geography = doc.geography.clone();
                        cased_field = doc.cased_field.clone();
                        submit = Some(session.submitter());
                    }
                    submit
                        .as_ref()
                        .expect("stream open while inbound open")
                        .submit(next_seq, &doc.text)
                        .await?;
                    pending.insert(
                        next_seq,
                        PendingDoc {
                            doc,
                            vector: doc_vector,
                            stable_routing_key,
                            outstanding: extras.len(),
                            extras,
                        },
                    );
                    next_seq += 1;
                }
                Step::InboundClosed => {
                    inbound_open = false;
                    submit = None;
                    session.finish();
                    // Half-close the field streams too. A sidecar may
                    // hold a result until more work arrives, so the last
                    // field of the call only lands once its stream is
                    // closing.
                    fields.finish();
                }
                Step::Result(item) => store_result(&mut results, item)?,
                Step::Field(event) => store_field(&mut pending, &mut route, event)?,
            }
        }
        Ok(())
    }

    /// Advance the apply wavefront over every consecutive sequence whose
    /// body AND every extra field have landed, keeping application in
    /// arrival order.
    fn advance_apply(
        &self,
        pending: &mut std::collections::BTreeMap<u64, PendingDoc>,
        results: &mut std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
        next_apply: &mut u64,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        loop {
            let ready = results.contains_key(next_apply)
                && pending.get(next_apply).is_some_and(|p| p.outstanding == 0);
            if !ready || self.tail_full() {
                return Ok(());
            }
            let analyzed = results.remove(next_apply).expect("readiness just checked");
            let held = pending.remove(next_apply).expect("readiness just checked");
            let cased = cased_field_index(&self.config, self.phrase_index.as_deref(), &held.doc)?;
            let analyzed = join_fields(analyzed, held.extras, cased)?;
            self.apply_analyzed_document(
                held.doc,
                analyzed,
                held.vector,
                held.stable_routing_key,
                added,
                first_id,
            )?;
            *next_apply += 1;
        }
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    type SearchShardStream =
        crate::metrics::Timed<ReceiverStream<Result<SearchShardResponse, Status>>>;
    type StreamSearchStream =
        crate::metrics::Timed<ReceiverStream<Result<StreamSearchResponse, Status>>>;
    type Bm25QueryStreamStream =
        crate::metrics::Timed<ReceiverStream<Result<Bm25QueryStreamResponse, Status>>>;
    type ReadWalStream = crate::metrics::Timed<ReceiverStream<Result<ReadWalResponse, Status>>>;
    type StreamSnapshotStream =
        crate::metrics::Timed<ReceiverStream<Result<SnapshotChunk, Status>>>;

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        crate::metrics::timed_stream(Route::SearchShard, request, |request| async move {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let state = self.state.clone();
            let config = self.config.clone();
            let scan_queue = config.coalesce.then(|| self.scan_queue());

            tokio::spawn(async move {
                // Protocol: the first message must be Start.
                let start = match inbound.message().await {
                    Ok(Some(SearchShardRequest {
                        payload: Some(search_shard_request::Payload::Start(start)),
                    })) => start,
                    Ok(_) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first SearchShardRequest must be StartShardSearch",
                            )))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                // Filter validation is shape-only and shard-independent, so
                // it happens once here rather than inside the scan: a
                // malformed tree must refuse before any work, exactly as on
                // the lexical routes.
                let geo_regions = match validate_geo_filters(&start.geo_filters) {
                    Ok(regions) => regions,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if let Some(f) = start.filter.as_ref() {
                    if let Err(e) = crate::filter::validate_filter(f) {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }

                // Floor updates arrive on the same stream; a pump task folds
                // them into a watch cell the blocking scan polls between chunks.
                // Updates are monotone maxes, so only raises are stored.
                let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
                tokio::spawn(async move {
                    loop {
                        match inbound.message().await {
                            Ok(Some(SearchShardRequest {
                                payload: Some(search_shard_request::Payload::FloorUpdate(u)),
                            })) => {
                                floor_tx.send_if_modified(|cur| {
                                    if !u.floor.is_nan() && u.floor > *cur {
                                        *cur = u.floor;
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                            // Duplicate Start or empty payload: ignore.
                            Ok(Some(_)) => {}
                            // Client closed (end of updates or cancellation) or
                            // the stream broke: stop pumping; the scan finishes
                            // on its own either way.
                            Ok(None) | Err(_) => break,
                        }
                    }
                });

                let share = config.share_floors;
                let floor_delta = config.floor_delta;
                let chunk_blocks = config.chunk_blocks;
                let slot_offset = config.slot_offset;
                let scan_tx = tx.clone();
                // Publish only raises that clear the delta gate, and never
                // block the scan on a full channel: intermediate floors are
                // disposable (they are monotone, so the next chunk's publish
                // supersedes any dropped one). The terminal Done is sent
                // with `.await` below and cannot be dropped.
                let warmup = config.floor_warmup_chunks;
                let min_interval = (config.floor_min_interval_ms > 0)
                    .then(|| std::time::Duration::from_millis(config.floor_min_interval_ms));
                let mut last_published = f32::NEG_INFINITY;
                let mut offers = 0u32;
                let mut last_at: Option<std::time::Instant> = None;
                // Returns whether the floor actually went on the wire, so
                // the scan can report offers and publishes apart. Reporting
                // only offers is how the warmup and debounce knobs came to
                // look like no-ops.
                let publish_floor = move |floor: f32| -> bool {
                    if !share {
                        return false;
                    }
                    // Skip the opening chunks: their floors are the weakest
                    // and cost a broadcast to every shard apiece.
                    offers += 1;
                    if offers <= warmup {
                        return false;
                    }
                    if floor <= last_published + floor_delta {
                        return false;
                    }
                    // Debounce. Suppressing a floor loses nothing: they are
                    // monotone, so the next one published is at least as
                    // high as the one dropped.
                    if let (Some(interval), Some(at)) = (min_interval, last_at) {
                        if at.elapsed() < interval {
                            return false;
                        }
                    }
                    last_published = floor;
                    last_at = Some(std::time::Instant::now());
                    let _ = scan_tx.try_send(Ok(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::FloorUpdate(FloorUpdate {
                            floor,
                        })),
                    }));
                    true
                };
                let external_floor = move || {
                    if share {
                        let f = *floor_rx.borrow();
                        (f != f32::NEG_INFINITY).then_some(f)
                    } else {
                        None
                    }
                };

                // Collapse-by-parent scans run their own solo path: the
                // collection semantics (one entry per parent, parent floors,
                // saturation escalation) do not batch with plain scans.
                if start.collapse_parents {
                    if start.tie_complete {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "collapse_parents and tie_complete are mutually exclusive",
                            )))
                            .await;
                        return;
                    }
                    let mut external_floor = external_floor;
                    let mut publish_floor = publish_floor;
                    let geo_regions = geo_regions.clone();
                    let scan = tokio::task::spawn_blocking(move || {
                        let n = {
                            let guard = state.read().expect("shard state lock poisoned");
                            let index = guard.index.as_ref().ok_or_else(|| {
                                Status::failed_precondition(
                                    "shard has no index yet (set calibration or add vectors)",
                                )
                            })?;
                            Self::validate_start(index, &start)?;
                            index.len()
                        };
                        // parent_map takes its own locks (read to build, write
                        // to cache), so the validation guard is dropped first.
                        let parents = Self::parent_map(&state, slot_offset, n);
                        let guard = state.read().expect("shard state lock poisoned");
                        let index = guard.index.as_ref().ok_or_else(|| {
                            Status::failed_precondition("shard index disappeared mid-setup")
                        })?;
                        if index.len() != parents.len() {
                            return Err(Status::aborted(
                                "shard grew between setup and scan; retry",
                            ));
                        }
                        // Filters remove chunks, and a parent's score is the
                        // max over its SURVIVING chunks, so collapse under a
                        // filter is the collapse of the filtered corpus —
                        // still every floor a valid lower bound, still no new
                        // pruning math.
                        let (_, allow) = resolve_shard_filters(
                            guard.bm25.as_ref(),
                            guard.live_docs.words(),
                            index.len(),
                            &start.geo_filters,
                            &geo_regions,
                            start.filter.as_ref(),
                        )?;
                        let known = filter_known_flags(
                            guard.bm25.as_ref(),
                            &start.geo_filters,
                            start.filter.as_ref(),
                        );
                        let (hits, stats) = chunked_topk_collapsed(
                            index,
                            &start.vector,
                            start.k as usize,
                            chunk_blocks,
                            &parents,
                            &mut external_floor,
                            &mut publish_floor,
                            allow.as_deref(),
                        );
                        Ok((hits, stats, known))
                    });
                    let outcome = match scan.await {
                        Ok(result) => result,
                        Err(e) => Err(Status::internal(format!("collapse scan task failed: {e}"))),
                    };
                    match outcome {
                        Ok((hits, stats, (geo_columns_known, filter_columns_known))) => {
                            let done = SearchShardDone {
                                hits: hits
                                    .into_iter()
                                    .map(|h| ScoredHit {
                                        vector_id: slot_offset + u64::from(h.slot),
                                        score: h.score,
                                        parent_id: h.parent,
                                    })
                                    .collect(),
                                stats: Some(ShardScanStats {
                                    chunk_calls: stats.chunk_calls,
                                    candidates_collected: stats.candidates_collected,
                                    floors_published: stats.floors_published,
                                    floor_updates_applied: stats.floor_updates_applied,
                                    floors_offered: stats.floors_offered,
                                    segments_total: 0,
                                    segments_skipped: 0,
                                }),
                                geo_columns_known,
                                filter_columns_known,
                            };
                            let _ = tx
                                .send(Ok(SearchShardResponse {
                                    payload: Some(search_shard_response::Payload::Done(done)),
                                }))
                                .await;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                        }
                    }
                    return;
                }

                let outcome: Result<ScanOutcome, Status> = match scan_queue {
                    Some(jobs) => {
                        // Coalesced path: validate against the current index
                        // cheaply, then queue for a batched kernel pass. The
                        // batch runner holds the read lock for the scan, the
                        // same consistency the solo path gets.
                        let validated = {
                            let guard = state.read().expect("shard state lock poisoned");
                            match guard.index.as_ref() {
                                Some(index) => Self::validate_start(index, &start),
                                None => Err(Status::failed_precondition(
                                    "shard has no index yet (set calibration or add vectors)",
                                )),
                            }
                        };
                        match validated {
                            Ok(()) => {
                                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                                let job = ScanJob {
                                    vector: start.vector.clone(),
                                    k: start.k as usize,
                                    tie_complete: start.tie_complete,
                                    geo_filters: start.geo_filters.clone(),
                                    geo_regions: geo_regions.clone(),
                                    filter: start.filter.clone(),
                                    external: Box::new(external_floor),
                                    publish: Box::new(publish_floor),
                                    done: done_tx,
                                };
                                if jobs.send(job).await.is_err() {
                                    Err(Status::internal("scan scheduler unavailable"))
                                } else {
                                    match done_rx.await {
                                        Ok(result) => result,
                                        Err(_) => Err(Status::internal(
                                            "scan batch dropped before finishing",
                                        )),
                                    }
                                }
                            }
                            Err(e) => Err(e),
                        }
                    }
                    None => {
                        // Solo path (the coalescing A/B baseline): one
                        // blocking scan per RPC, exactly the historical
                        // behavior.
                        let mut external_floor = external_floor;
                        let mut publish_floor = publish_floor;
                        let scan = tokio::task::spawn_blocking(move || {
                            // The read guard is held for the whole chunked
                            // scan: adds (write lock) never interleave with a
                            // scan, so a search sees one consistent index
                            // snapshot.
                            let guard = state.read().expect("shard state lock poisoned");
                            let index = guard.index.as_ref().ok_or_else(|| {
                                Status::failed_precondition(
                                    "shard has no index yet (set calibration or add vectors)",
                                )
                            })?;
                            Self::validate_start(index, &start)?;
                            let (_, allow) = resolve_shard_filters(
                                guard.bm25.as_ref(),
                                guard.live_docs.words(),
                                index.len(),
                                &start.geo_filters,
                                &geo_regions,
                                start.filter.as_ref(),
                            )?;
                            let (geo_columns_known, filter_columns_known) = filter_known_flags(
                                guard.bm25.as_ref(),
                                &start.geo_filters,
                                start.filter.as_ref(),
                            );
                            let (hits, stats) = chunked_topk(
                                index,
                                &start.vector,
                                start.k as usize,
                                chunk_blocks,
                                &mut external_floor,
                                &mut publish_floor,
                                start.tie_complete,
                                allow.as_deref(),
                            );
                            crate::metrics::record_scan(&stats);
                            Ok(ScanOutcome {
                                hits,
                                stats,
                                geo_columns_known,
                                filter_columns_known,
                            })
                        });
                        match scan.await {
                            Ok(result) => result,
                            Err(e) => Err(Status::internal(format!("scan task failed: {e}"))),
                        }
                    }
                };

                match outcome {
                    Ok(ScanOutcome {
                        hits,
                        stats,
                        geo_columns_known,
                        filter_columns_known,
                    }) => {
                        let done = SearchShardDone {
                            hits: hits
                                .into_iter()
                                .map(|h| ScoredHit {
                                    vector_id: slot_offset + u64::from(h.slot),
                                    score: h.score,
                                    parent_id: 0,
                                })
                                .collect(),
                            stats: Some(ShardScanStats {
                                chunk_calls: stats.chunk_calls,
                                candidates_collected: stats.candidates_collected,
                                floors_published: stats.floors_published,
                                floor_updates_applied: stats.floor_updates_applied,
                                floors_offered: stats.floors_offered,
                                segments_total: 0,
                                segments_skipped: 0,
                            }),
                            geo_columns_known,
                            filter_columns_known,
                        };
                        let _ = tx
                            .send(Ok(SearchShardResponse {
                                payload: Some(search_shard_response::Payload::Done(done)),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    /// Filter-only browse: admitted ids above the floor, ascending, at
    /// most k, or ordered by a column under `sort`. Match-all over the DOCUMENT space (bm25 postings), gated
    /// by the same resolved filter every scored route admits through;
    /// no scoring, no heap — the order is the id order.
    async fn browse_shard(
        &self,
        request: Request<crate::pb::BrowseShardRequest>,
    ) -> Result<Response<crate::pb::BrowseShardResponse>, Status> {
        crate::metrics::timed(Route::BrowseShard, request, |request| async move {
            let req = request.into_inner();
            if req.k == 0 {
                return Err(Status::invalid_argument("browse requires k > 0"));
            }
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = self.state.read().expect("shard state lock poisoned");
            let (geo_columns_known, filter_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
            let slot_offset = self.config.slot_offset;
            let Some(store) = guard.bm25.as_ref() else {
                // A document-less shard admits nothing; its all-false known
                // flags feed the coordinator's typo rule like everywhere.
                return Ok(Response::new(crate::pb::BrowseShardResponse {
                    doc_ids: Vec::new(),
                    geo_columns_known,
                    filter_columns_known,
                    sort_rows: Vec::new(),
                    sort_columns_known: vec![false; req.sort.len()],
                }));
            };
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush before browsing",
                ));
            }
            let n = u64::from(store.next_doc_id());
            // The lexical membership predicate (the terms of one lexical
            // leaf, OR): the same bitmap ResolveLexicalBitmap answers,
            // built here so the walk reads it in place.
            let lexical: Option<Vec<u8>> = if req.lexical_terms.is_empty() {
                None
            } else {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                let count = n as usize;
                let mut bits = vec![0u8; count.div_ceil(8)];
                for term in &req.lexical_terms {
                    index.for_each_doc_tf(term, &mut |doc_id, _tf| {
                        let position = doc_id as usize;
                        if position < count {
                            bits[position / 8] |= 1 << (position % 8);
                        }
                    });
                }
                Some(bits)
            };
            let in_lexical = |local: u32| -> bool {
                match &lexical {
                    None => true,
                    Some(bits) => {
                        let p = local as usize;
                        bits.get(p / 8).is_some_and(|b| b & (1 << (p % 8)) != 0)
                    }
                }
            };
            // The exclusive floor in local id space. The first page carries
            // no floor at all (proto3 cannot distinguish after = 0 from
            // unset, so the request says which it is).
            let start = if req.first_page || req.after < slot_offset {
                0
            } else {
                (req.after - slot_offset + 1).min(n)
            };
            let doc_filter = crate::filter::DocFilter {
                deleted: guard.live_docs.words(),
                geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                pred: req
                    .filter
                    .as_ref()
                    .map(|f| store.resolve_filter(f))
                    .transpose()?,
                phrase: Vec::new(),
            };
            let cols = ShardNumericRead(store);
            if !req.sort.is_empty() {
                // Column-ordered browse: walk the FULL admitted set with a
                // k-bounded heap. Exhaustive by construction, so the
                // exactness certificate is trivial; per-shard exact top-k
                // by key means the coordinator's merged union contains the
                // global top-k (local rank <= global rank).
                use crate::sortkeys::{cmp_candidate, Key, KeyRef, Value};
                enum Column {
                    Numeric(usize),
                    Integer(usize),
                    Facet(usize),
                    Parent,
                    Group,
                }
                let mut columns = Vec::with_capacity(req.sort.len());
                let mut known = Vec::with_capacity(req.sort.len());
                for sort in &req.sort {
                    let column = if let Some(ni) = store.numeric_index(&sort.column) {
                        Some(Column::Numeric(ni))
                    } else if let Some(ii) = store.integer_index(&sort.column) {
                        Some(Column::Integer(ii))
                    } else if let Some(fi) = store.facet_index(&sort.column) {
                        Some(Column::Facet(fi))
                    } else if sort.column == "parent_id" {
                        Some(Column::Parent)
                    } else if sort.column == "group_id" {
                        Some(Column::Group)
                    } else {
                        None
                    };
                    known.push(column.is_some());
                    columns.push(column);
                }
                if columns.iter().any(Option::is_none) {
                    // Unknown here; the coordinator's typo rule refuses only
                    // when NO shard knows a column. A shard that lacks any
                    // key holds no value for it on any document, so it
                    // contributes no rows.
                    return Ok(Response::new(crate::pb::BrowseShardResponse {
                        doc_ids: Vec::new(),
                        geo_columns_known,
                        filter_columns_known,
                        sort_rows: Vec::new(),
                        sort_columns_known: known,
                    }));
                }
                let columns: Vec<Column> = columns.into_iter().flatten().collect();
                let descending: Vec<bool> = req.sort.iter().map(|s| s.descending).collect();
                let index = store.as_index();
                // A candidate's keys, borrowed where the column lets them
                // be, and its reported values; None when any key is absent.
                let keys_of = |doc: u32| -> Option<(Vec<KeyRef<'_>>, Vec<Value>)> {
                    let mut keys = Vec::with_capacity(columns.len());
                    let mut values = Vec::with_capacity(columns.len());
                    for (column, desc) in columns.iter().zip(&descending) {
                        let adjust = |bits: u64| if *desc { !bits } else { bits };
                        match column {
                            Column::Numeric(ni) => {
                                let v = store.numeric_value(*ni, doc)?;
                                keys.push(KeyRef::Bits(adjust(f64_order_bits(v))));
                                values.push(Value::Number(v));
                            }
                            Column::Integer(ii) => {
                                let v = store.integer_value(*ii, doc)?;
                                keys.push(KeyRef::Bits(adjust(i64_order_bits(v))));
                                values.push(Value::Integer(v));
                            }
                            Column::Facet(fi) => {
                                let ord = store.facet_ord(*fi, doc)?;
                                let text = store.facet_value(*fi, ord);
                                keys.push(KeyRef::Text(text));
                                values.push(Value::Text(text.to_string()));
                            }
                            Column::Parent | Column::Group => {
                                let lineage = index.and_then(|index| index.lineage(doc))?;
                                let v = if matches!(column, Column::Parent) {
                                    lineage.parent_id
                                } else {
                                    lineage.group_id
                                };
                                keys.push(KeyRef::Bits(adjust(v)));
                                values.push(Value::Integer(v as i64));
                            }
                        }
                    }
                    Some((keys, values))
                };
                let boundary: Option<Vec<Key>> = if req.first_page {
                    None
                } else {
                    let keys: Option<Vec<Key>> = req.after_keys.iter().map(Key::from_pb).collect();
                    let keys = keys.ok_or_else(|| {
                        Status::invalid_argument("sorted browse boundary carries an empty key")
                    })?;
                    if keys.len() != req.sort.len() {
                        return Err(Status::invalid_argument(format!(
                            "sorted browse boundary has {} keys for {} sort columns",
                            keys.len(),
                            req.sort.len()
                        )));
                    }
                    Some(keys)
                };
                // The k best rows so far, worst last (a small k: the heap's
                // work is the comparison against the worst kept row, and
                // an insertion is a shift of at most k entries).
                struct Row {
                    keys: Vec<Key>,
                    values: Vec<Value>,
                    id: u64,
                }
                let k = req.k as usize;
                let mut kept: Vec<Row> = Vec::with_capacity(k + 1);
                for local in 0..n {
                    let doc = local as u32;
                    if !in_lexical(doc) || !doc_filter.passes(doc, &cols) {
                        continue;
                    }
                    // A document without a value has no honest position in
                    // a column order: excluded, same stance as the filters.
                    let Some((keys, values)) = keys_of(doc) else {
                        continue;
                    };
                    let id = slot_offset + local;
                    if let Some(b) = &boundary {
                        if cmp_candidate(&keys, id, b, req.after, &descending)
                            != std::cmp::Ordering::Greater
                        {
                            continue;
                        }
                    }
                    if kept.len() == k {
                        let worst = &kept[k - 1];
                        if cmp_candidate(&keys, id, &worst.keys, worst.id, &descending)
                            != std::cmp::Ordering::Less
                        {
                            continue;
                        }
                    }
                    let row = Row {
                        keys: keys.into_iter().map(KeyRef::to_owned).collect(),
                        values,
                        id,
                    };
                    let at = kept.partition_point(|r| {
                        crate::sortkeys::cmp_rows(&r.keys, r.id, &row.keys, row.id, &descending)
                            == std::cmp::Ordering::Less
                    });
                    kept.insert(at, row);
                    if kept.len() > k {
                        kept.pop();
                    }
                }
                let mut doc_ids = Vec::with_capacity(kept.len());
                let mut sort_rows = Vec::with_capacity(kept.len());
                for row in kept {
                    doc_ids.push(row.id);
                    sort_rows.push(crate::pb::SortKeyRow {
                        keys: row.keys.iter().map(Key::to_pb).collect(),
                        values: row.values.iter().map(Value::to_pb).collect(),
                    });
                }
                return Ok(Response::new(crate::pb::BrowseShardResponse {
                    doc_ids,
                    geo_columns_known,
                    filter_columns_known,
                    sort_rows,
                    sort_columns_known: known,
                }));
            }
            let mut doc_ids = Vec::new();
            for local in start..n {
                if in_lexical(local as u32) && doc_filter.passes(local as u32, &cols) {
                    doc_ids.push(slot_offset + local);
                    if doc_ids.len() == req.k as usize {
                        break;
                    }
                }
            }
            Ok(Response::new(crate::pb::BrowseShardResponse {
                doc_ids,
                geo_columns_known,
                filter_columns_known,
                sort_rows: Vec::new(),
                sort_columns_known: Vec::new(),
            }))
        })
        .await
    }

    async fn resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        crate::metrics::timed(Route::ResolveFilterBitmap, request, |request| async move {
            let req = request.into_inner();
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = self.state.read().expect("shard state lock poisoned");
            let (geo_columns_known, filter_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
            let label_count = guard
                .bm25
                .as_ref()
                .map_or(0, |store| store.next_doc_id() as usize);
            let (_, allow) = resolve_shard_filters(
                guard.bm25.as_ref(),
                guard.live_docs.words(),
                label_count,
                &req.geo_filters,
                &geo_regions,
                req.filter.as_ref(),
            )?;
            let allow = allow.unwrap_or_else(|| vec![true; label_count]);
            let mut bits = vec![0u8; label_count.div_ceil(8)];
            for (position, admitted) in allow.into_iter().enumerate() {
                if admitted {
                    bits[position / 8] |= 1 << (position % 8);
                }
            }
            Ok(Response::new(crate::pb::FilterBitmapResponse {
                base_label: self.config.slot_offset,
                label_count: label_count as u64,
                bits,
                geo_columns_known,
                filter_columns_known,
            }))
        })
        .await
    }

    async fn resolve_lexical_bitmap(
        &self,
        request: Request<LexicalBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        let req = request.into_inner();
        let guard = self.state.read().expect("shard state lock poisoned");
        let Some(shard) = guard.bm25.as_ref() else {
            return Ok(Response::new(MembershipBitmapResponse {
                base_label: self.config.slot_offset,
                label_count: 0,
                bits: Vec::new(),
                stats_epoch: guard.stats_epoch,
            }));
        };
        let index = shard.as_index().ok_or_else(|| {
            Status::failed_precondition("bm25 bulk build in progress; Flush first")
        })?;
        let label_count = usize::try_from(shard.next_doc_id())
            .map_err(|_| Status::resource_exhausted("lexical row count does not fit usize"))?;
        let mut bits = vec![0u8; label_count.div_ceil(8)];
        for term in req.terms {
            index.for_each_doc_tf(&term, &mut |doc_id, _tf| {
                let position = doc_id as usize;
                if position < label_count && !guard.live_docs.is_deleted(position) {
                    bits[position / 8] |= 1 << (position % 8);
                }
            });
        }
        Ok(Response::new(MembershipBitmapResponse {
            base_label: self.config.slot_offset,
            label_count: label_count as u64,
            bits,
            stats_epoch: guard.stats_epoch,
        }))
    }

    async fn resolve_vector_bitmap(
        &self,
        _request: Request<VectorBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let label_count = guard.index.as_ref().map_or(0, VectorIndex::len);
        let mut bits = vec![0xffu8; label_count.div_ceil(8)];
        if let Some(last) = bits.last_mut() {
            let used = label_count % 8;
            if used != 0 {
                *last &= (1u8 << used) - 1;
            }
        }
        for position in 0..label_count {
            if guard.live_docs.is_deleted(position) {
                bits[position / 8] &= !(1 << (position % 8));
            }
        }
        Ok(Response::new(MembershipBitmapResponse {
            base_label: self.config.slot_offset,
            label_count: label_count as u64,
            bits,
            stats_epoch: 0,
        }))
    }

    async fn aggregate_shard(
        &self,
        request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        crate::metrics::timed(Route::AggregateShard, request, |request| async move {
            let req = request.into_inner();
            if req.aggregations.is_empty()
                && req.histograms.is_empty()
                && req.percentiles.is_empty()
            {
                return Err(Status::invalid_argument(
                    "aggregate requires at least one aggregation, histogram, or percentile",
                ));
            }
            let grouping = !req.group_by.is_empty();
            let group_cap = req.max_groups as usize;
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = self.state.read().expect("shard state lock poisoned");
            let (geo_columns_known, filter_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
            // Expression column leaves: aggregations first, then
            // histograms, request order then depth-first — the projection
            // typo contract.
            let mut leaves = Vec::new();
            for expr in req
                .aggregations
                .iter()
                .filter_map(|a| a.expr.as_ref())
                .chain(req.histograms.iter().filter_map(|h| h.expr.as_ref()))
                .chain(req.percentiles.iter().filter_map(|p| p.expr.as_ref()))
            {
                crate::values::column_leaves(expr, &mut leaves);
            }
            let Some(store) = guard.bm25.as_ref() else {
                // A document-less shard holds no values and no columns; its
                // all-absent partials and all-false flags feed the merge
                // and the typo rule like everywhere.
                return Ok(Response::new(crate::pb::AggregateShardResponse {
                    partials: req
                        .aggregations
                        .iter()
                        .map(|_| AggAcc::Absent.partial(None))
                        .collect(),
                    matched: 0,
                    geo_columns_known,
                    filter_columns_known,
                    expr_leaves_known: vec![false; leaves.len()],
                    groups: Vec::new(),
                    ungrouped: 0,
                    group_column_known: false,
                    histograms: req
                        .histograms
                        .iter()
                        .map(|_| crate::pb::ShardHistogram::default())
                        .collect(),
                    percentile_partials: req
                        .percentiles
                        .iter()
                        .map(|_| crate::pb::PercentilePartial {
                            vtype: crate::pb::AggregateValueType::Absent as i32,
                            ..Default::default()
                        })
                        .collect(),
                }));
            };
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush before aggregating",
                ));
            }
            let expr_leaves_known: Vec<bool> = leaves
                .iter()
                .map(|l| crate::values::leaf_known(l, store))
                .collect();
            let group_facet = grouping.then(|| store.facet_index(&req.group_by)).flatten();
            let group_column_known = group_facet.is_some();
            // Resolve every expression and admit its op against the
            // resolved type BEFORE touching any document: a type conflict
            // refuses the request, it never mis-aggregates.
            let mut exprs: Vec<(crate::values::ResolvedValue, crate::values::ValueType, bool)> =
                Vec::with_capacity(req.aggregations.len());
            let mut totals: Vec<AggAcc> = Vec::with_capacity(req.aggregations.len());
            for agg in &req.aggregations {
                let op = agg_op_of(agg.op)?;
                let cardinality = op == crate::pb::AggregateOp::Cardinality;
                if cardinality && agg.max_distinct == 0 {
                    return Err(Status::internal(format!(
                        "aggregation {:?} arrived without a distinct cap",
                        agg.name
                    )));
                }
                let expr = agg.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "aggregation {:?} without an expression",
                        agg.name
                    ))
                })?;
                let (rv, vt) = crate::values::resolve(expr, store).map_err(|e| {
                    Status::invalid_argument(format!("aggregation {:?}: {}", agg.name, e.message()))
                })?;
                check_agg_type(&agg.name, op, vt)?;
                totals.push(acc_of(vt, cardinality));
                exprs.push((rv, vt, cardinality));
            }
            let mut hists: Vec<(
                Option<crate::values::ResolvedValue>,
                Bucketing,
                usize,
                String,
                HistAcc,
            )> = Vec::with_capacity(req.histograms.len());
            for h in &req.histograms {
                let bucketing = if h.calendar != 0 {
                    let unit = crate::calendar::interval_of(h.calendar).ok_or_else(|| {
                        Status::internal(format!(
                            "histogram {:?} arrived with an unvalidated calendar unit",
                            h.name
                        ))
                    })?;
                    Bucketing::Calendar {
                        unit,
                        utc_offset_minutes: h.utc_offset_minutes,
                    }
                } else {
                    if !(h.interval > 0.0 && h.interval.is_finite()) {
                        return Err(Status::internal(format!(
                            "histogram {:?} arrived with an unvalidated interval",
                            h.name
                        )));
                    }
                    Bucketing::Fixed(h.interval)
                };
                let expr = h.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "histogram {:?} without an expression",
                        h.name
                    ))
                })?;
                let (rv, vt) = crate::values::resolve(expr, store).map_err(|e| {
                    Status::invalid_argument(format!("histogram {:?}: {}", h.name, e.message()))
                })?;
                let type_name = |vt: crate::values::ValueType| match vt {
                    crate::values::ValueType::Str => "string",
                    crate::values::ValueType::Bool => "boolean",
                    crate::values::ValueType::Int => "int",
                    crate::values::ValueType::Double => "double",
                    crate::values::ValueType::Unknown => "unknown",
                };
                let rv = match (bucketing, vt) {
                    (_, crate::values::ValueType::Unknown) => None,
                    (Bucketing::Fixed(_), crate::values::ValueType::Double) => Some(rv),
                    (Bucketing::Fixed(_), crate::values::ValueType::Int) => {
                        return Err(Status::invalid_argument(format!(
                            "histogram {:?} takes a double expression; convert explicitly \
                         with double()",
                            h.name
                        )));
                    }
                    (Bucketing::Fixed(_), other) => {
                        return Err(Status::invalid_argument(format!(
                            "histogram {:?} takes a double expression, not a {}",
                            h.name,
                            type_name(other)
                        )));
                    }
                    (Bucketing::Calendar { .. }, crate::values::ValueType::Int) => Some(rv),
                    (Bucketing::Calendar { .. }, other) => {
                        return Err(Status::invalid_argument(format!(
                            "histogram {:?} buckets by calendar over an int expression in \
                             epoch micros (a timestamp column), not a {}",
                            h.name,
                            type_name(other)
                        )));
                    }
                };
                hists.push((
                    rv,
                    bucketing,
                    h.max_buckets as usize,
                    h.name.clone(),
                    HistAcc::default(),
                ));
            }
            let mut pcts: Vec<(Option<(crate::values::ResolvedValue, bool)>, PctAcc)> =
                Vec::with_capacity(req.percentiles.len());
            for spec in &req.percentiles {
                let resolved =
                    resolve_rankable(store, spec.expr.as_ref(), &spec.name, "percentile")?;
                pcts.push((resolved, PctAcc::default()));
            }
            let doc_filter = crate::filter::DocFilter {
                deleted: guard.live_docs.words(),
                geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                pred: req
                    .filter
                    .as_ref()
                    .map(|f| store.resolve_filter(f))
                    .transpose()?,
                phrase: Vec::new(),
            };
            let cols = ShardNumericRead(store);
            let n = u64::from(store.next_doc_id());
            let id_allowlist = req.restrict_doc_ids.then(|| {
                req.doc_ids
                    .iter()
                    .filter_map(|id| id.checked_sub(self.config.slot_offset))
                    .filter_map(|local| u32::try_from(local).ok())
                    .collect::<std::collections::HashSet<_>>()
            });
            let mut matched = 0u64;
            let mut ungrouped = 0u64;
            // Group accumulators by facet ordinal; each holds (matched,
            // one accumulator per aggregation).
            let mut groups: std::collections::HashMap<u32, (u64, Vec<AggAcc>)> =
                std::collections::HashMap::new();
            // One pass in doc order: the fold orders themselves are part
            // of the determinism contract (Neumaier and Welford both fold
            // exactly this sequence on every run).
            for local in 0..n {
                let doc = local as u32;
                if id_allowlist
                    .as_ref()
                    .is_some_and(|allowlist| !allowlist.contains(&doc))
                {
                    continue;
                }
                if !doc_filter.passes(doc, &cols) {
                    continue;
                }
                matched += 1;
                let group = if grouping {
                    match group_facet.and_then(|fi| store.facet_ord(fi, doc)) {
                        Some(ord) => {
                            let n_groups = groups.len();
                            let entry = groups.entry(ord).or_insert_with(|| {
                                (
                                    0,
                                    exprs
                                        .iter()
                                        .map(|(_, vt, cardinality)| acc_of(*vt, *cardinality))
                                        .collect(),
                                )
                            });
                            if entry.0 == 0 && n_groups == group_cap {
                                return Err(Status::failed_precondition(format!(
                                    "group_by {:?} exceeds {group_cap} distinct values on \
                                 one shard; tighten the filter or raise max_groups",
                                    req.group_by
                                )));
                            }
                            entry.0 += 1;
                            Some(entry)
                        }
                        None => {
                            ungrouped += 1;
                            None
                        }
                    }
                } else {
                    None
                };
                let mut group_accs = group.map(|g| &mut g.1);
                for (i, (rv, _, _)) in exprs.iter().enumerate() {
                    // Absent-typed totals imply an absent-typed group
                    // accumulator: the type is the expression's, not the
                    // group's.
                    if matches!(totals[i], AggAcc::Absent) {
                        continue;
                    }
                    if let Some(v) = crate::values::eval(rv, doc, &cols) {
                        totals[i].push(v);
                        if let Some(accs) = group_accs.as_deref_mut() {
                            accs[i].push(v);
                        }
                    }
                }
                for (rv, bucketing, cap, name, acc) in hists.iter_mut() {
                    let Some(rv) = rv else { continue };
                    match (*bucketing, crate::values::eval(rv, doc, &cols)) {
                        (Bucketing::Fixed(interval), Some(crate::values::Val::Double(x))) => {
                            acc.push(x, interval, *cap, name)?;
                        }
                        (
                            Bucketing::Calendar {
                                unit,
                                utc_offset_minutes,
                            },
                            Some(crate::values::Val::Int(micros)),
                        ) => {
                            acc.push_calendar(micros, unit, utc_offset_minutes, *cap, name)?;
                        }
                        _ => {}
                    }
                }
                for (resolved, acc) in pcts.iter_mut() {
                    let Some((rv, int_typed)) = resolved else {
                        continue;
                    };
                    if let Some(v) = crate::values::eval(rv, doc, &cols) {
                        acc.push(rankable_bits(v, *int_typed));
                    }
                }
            }
            for (agg, acc) in req.aggregations.iter().zip(&totals) {
                if let Some(n) = acc.distinct_len() {
                    if n > agg.max_distinct as usize {
                        return Err(Status::failed_precondition(format!(
                            "aggregation {:?}: more than {} distinct values on one shard; \
                             raise max_distinct or tighten the filter",
                            agg.name, agg.max_distinct
                        )));
                    }
                }
            }
            let mut group_rows: Vec<(u32, u64, Vec<AggAcc>)> = groups
                .into_iter()
                .map(|(ord, (m, accs))| (ord, m, accs))
                .collect();
            group_rows.sort_unstable_by_key(|r| r.0);
            let groups = group_facet
                .map(|fi| {
                    group_rows
                        .iter()
                        .map(|(ord, m, accs)| crate::pb::AggregateShardGroup {
                            value: store.facet_value(fi, *ord).to_string(),
                            matched: *m,
                            partials: accs.iter().map(|acc| acc.partial(Some(store))).collect(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(Response::new(crate::pb::AggregateShardResponse {
                partials: totals.iter().map(|acc| acc.partial(Some(store))).collect(),
                matched,
                geo_columns_known,
                filter_columns_known,
                expr_leaves_known,
                groups,
                ungrouped,
                group_column_known,
                histograms: hists
                    .iter()
                    .map(|(_, _, _, _, acc)| acc.response())
                    .collect(),
                percentile_partials: pcts
                    .iter()
                    .map(|(resolved, acc)| {
                        use crate::pb::AggregateValueType as T;
                        crate::pb::PercentilePartial {
                            vtype: match resolved {
                                None => T::Absent as i32,
                                Some((_, true)) => T::Int as i32,
                                Some((_, false)) => T::Double as i32,
                            },
                            present: acc.present,
                            unrankable: acc.unrankable,
                            min_bits: acc.min_bits,
                            max_bits: acc.max_bits,
                        }
                    })
                    .collect(),
            }))
        })
        .await
    }

    async fn quantile_counts(
        &self,
        request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        crate::metrics::timed(Route::QuantileCounts, request, |request| async move {
            let req = request.into_inner();
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = self.state.read().expect("shard state lock poisoned");
            let Some(store) = guard.bm25.as_ref() else {
                return Ok(Response::new(crate::pb::QuantileCountsResponse {
                    counts: vec![0; req.targets.len()],
                }));
            };
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush before aggregating",
                ));
            }
            let mut exprs = Vec::with_capacity(req.exprs.len());
            for spec in &req.exprs {
                exprs.push(resolve_rankable(
                    store,
                    spec.expr.as_ref(),
                    &spec.name,
                    "percentile",
                )?);
            }
            for t in &req.targets {
                if t.expr_index as usize >= exprs.len() {
                    return Err(Status::internal(
                        "quantile target refers past the expression list",
                    ));
                }
            }
            let doc_filter = crate::filter::DocFilter {
                deleted: guard.live_docs.words(),
                geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                pred: req
                    .filter
                    .as_ref()
                    .map(|f| store.resolve_filter(f))
                    .transpose()?,
                phrase: Vec::new(),
            };
            let cols = ShardNumericRead(store);
            let n = u64::from(store.next_doc_id());
            let id_allowlist = req.restrict_doc_ids.then(|| {
                req.doc_ids
                    .iter()
                    .filter_map(|id| id.checked_sub(self.config.slot_offset))
                    .filter_map(|local| u32::try_from(local).ok())
                    .collect::<std::collections::HashSet<_>>()
            });
            let mut counts = vec![0u64; req.targets.len()];
            let mut bits_of = vec![None; exprs.len()];
            // One admitted-set pass per round: each expression evaluates
            // once per document, every target reads the cached bits.
            for local in 0..n {
                let doc = local as u32;
                if id_allowlist
                    .as_ref()
                    .is_some_and(|allowlist| !allowlist.contains(&doc))
                {
                    continue;
                }
                if !doc_filter.passes(doc, &cols) {
                    continue;
                }
                for (slot, resolved) in bits_of.iter_mut().zip(&exprs) {
                    *slot = match resolved {
                        Some((rv, int_typed)) => crate::values::eval(rv, doc, &cols)
                            .and_then(|v| rankable_bits(v, *int_typed)),
                        None => None,
                    };
                }
                for (count, t) in counts.iter_mut().zip(&req.targets) {
                    if let Some(bits) = bits_of[t.expr_index as usize] {
                        if bits <= t.threshold_bits {
                            *count += 1;
                        }
                    }
                }
            }
            Ok(Response::new(crate::pb::QuantileCountsResponse { counts }))
        })
        .await
    }

    async fn read_wal(
        &self,
        request: Request<ReadWalRequest>,
    ) -> Result<Response<Self::ReadWalStream>, Status> {
        crate::metrics::timed_stream(Route::ReadWal, request, |request| async move {
            let req = request.into_inner();
            let (generation, high_watermark, records, prefix_health) = loop {
                let needs_flush = self
                    .state
                    .read()
                    .expect("shard state lock poisoned")
                    .wal
                    .as_ref()
                    .is_some_and(WalWriter::is_dirty);
                if needs_flush {
                    let service = self.clone();
                    tokio::task::spawn_blocking(move || service.flush_index())
                        .await
                        .map_err(|error| {
                            Status::internal(format!("WAL flush task failed: {error}"))
                        })??;
                }
                let snapshot = {
                    let guard = self.state.read().expect("shard state lock poisoned");
                    let wal = guard.wal.as_ref().ok_or_else(|| {
                        Status::failed_precondition(
                            "this shard has no WAL; live catch-up is unavailable",
                        )
                    })?;
                    if req.generation != wal.generation() {
                        return Err(Status::failed_precondition(format!(
                            "WAL generation mismatch: requested {}, live {}",
                            req.generation,
                            wal.generation()
                        )));
                    }
                    if req.after_clock > wal.high_watermark() {
                        return Err(Status::invalid_argument(format!(
                            "after_clock {} is beyond WAL high watermark {}",
                            req.after_clock,
                            wal.high_watermark()
                        )));
                    }
                    let records =
                        wal::read_clocked_records(wal.dir(), req.after_clock).map_err(|error| {
                            Status::failed_precondition(format!("read WAL: {error}"))
                        })?;
                    let num_vectors = guard.index.as_ref().map_or(0, |index| index.len() as u64);
                    let document_slots = guard
                        .bm25
                        .as_ref()
                        .map_or(0, |shard| u64::from(shard.next_doc_id()));
                    let physical_docs = physical_rows(&guard);
                    let deleted_docs = guard.live_docs.deleted_count().min(physical_docs);
                    let scoring_fingerprint = guard
                        .index
                        .as_ref()
                        .map_or_else(String::new, |index| index.descriptor().scoring_fingerprint);
                    (
                        wal.generation(),
                        wal.high_watermark(),
                        records,
                        (
                            num_vectors,
                            document_slots,
                            physical_docs - deleted_docs,
                            deleted_docs,
                            scoring_fingerprint,
                        ),
                    )
                };
                // A writer can append after the dirty check and before the read
                // lock. Its buffered bytes then trail the in-memory watermark.
                // Never advertise that watermark: retry, which sees dirty=true
                // and makes the whole index/WAL prefix durable first.
                if snapshot.1 == req.after_clock
                    || snapshot
                        .2
                        .last()
                        .is_some_and(|record| record.clock == snapshot.1)
                {
                    break snapshot;
                }
                tokio::task::yield_now().await;
            };
            let (tx, rx) = mpsc::channel(64);
            let slot_offset = self.config.slot_offset;
            tokio::spawn(async move {
                for record in records {
                    if tx
                        .send(Ok(ReadWalResponse {
                            generation,
                            high_watermark,
                            record: prost::Message::encode_to_vec(&record),
                            completed: false,
                            ..Default::default()
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = tx
                    .send(Ok(ReadWalResponse {
                        generation,
                        high_watermark,
                        record: Vec::new(),
                        completed: true,
                        num_vectors: prefix_health.0,
                        document_slots: prefix_health.1,
                        live_docs: prefix_health.2,
                        deleted_docs: prefix_health.3,
                        scoring_fingerprint: prefix_health.4,
                        slot_offset,
                    }))
                    .await;
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn apply_wal_binding(
        &self,
        request: Request<crate::pb::ApplyWalBindingRequest>,
    ) -> Result<Response<crate::pb::ApplyWalBindingResponse>, Status> {
        let req = request.into_inner();
        self.admit_collection(&req.collection)?;
        if req.plan_fingerprint.is_empty() || req.body_path.is_empty() {
            return Err(Status::invalid_argument(
                "WAL binding requires plan_fingerprint and body_path",
            ));
        }
        let incoming = crate::postings::StoredBinding {
            plan_fingerprint: req.plan_fingerprint,
            body_path: req.body_path,
            materialize_sha: req.materialize_sha,
        };
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let already_bound = Self::apply_binding_locked(&mut guard, incoming)?;
        Ok(Response::new(crate::pb::ApplyWalBindingResponse {
            already_bound,
        }))
    }

    async fn compact_shard(
        &self,
        request: Request<crate::pb::CompactShardRequest>,
    ) -> Result<Response<crate::pb::CompactShardResponse>, Status> {
        crate::metrics::timed(Route::CompactShard, request, |request| async move {
            let req = request.into_inner();
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.compact_shard(&req))
                .await
                .map_err(|e| Status::internal(format!("compaction task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let (num_vectors, dim, bit_width, vector_backend, scoring_fingerprint, quality_contract) =
            match guard.index.as_ref() {
                Some(index) => {
                    let descriptor = index.descriptor();
                    (
                        index.len() as u64,
                        index.dim_opt().unwrap_or(0) as u32,
                        index.bits_per_dimension().unwrap_or(self.config.bit_width) as u32,
                        descriptor.backend_kind,
                        descriptor.scoring_fingerprint,
                        format!("{:?}", descriptor.quality_contract).to_ascii_lowercase(),
                    )
                }
                None => (
                    0,
                    0,
                    self.config.bit_width as u32,
                    self.config.vector_backend.clone(),
                    String::new(),
                    String::new(),
                ),
            };
        let (bm25_docs, bm25_building) = match guard.bm25.as_ref() {
            Some(shard) => (shard.doc_count(), matches!(shard, Bm25Shard::Spilling(_))),
            None => (0, false),
        };
        let document_slots = guard
            .bm25
            .as_ref()
            .map_or(0, |shard| u64::from(shard.next_doc_id()));
        let exact_vector_rows = guard.exact_vectors.as_ref().map_or(0, |s| s.len() as u64);
        let exact_vectors_available = guard.exact_vectors.as_ref().is_some_and(|exact| {
            if let Some(index) = guard.index.as_ref() {
                exact.len() == index.len() && exact.dim() == index.dim_opt()
            } else if let Some(store) = guard.bm25.as_ref() {
                exact.len() == store.next_doc_id() as usize
            } else {
                true
            }
        });
        let exact_vectors_mmap = guard
            .exact_vectors
            .as_ref()
            .is_some_and(ExactVectorStore::is_mapped);
        let physical_docs = physical_rows(&guard);
        let deleted_docs = guard.live_docs.deleted_count().min(physical_docs);
        let (wal_generation, wal_high_watermark, wal_clocked) =
            guard.wal.as_ref().map_or((0, 0, false), |wal| {
                (
                    wal.generation(),
                    wal.high_watermark(),
                    !wal.has_legacy_clock_records(),
                )
            });
        Ok(Response::new(HealthResponse {
            collection: self.config.collection.clone(),
            num_vectors,
            dim,
            bits_per_dimension: bit_width,
            slot_offset: self.config.slot_offset,
            bm25_docs,
            bm25_building,
            ingest_active: self.ingest_busy.load(std::sync::atomic::Ordering::Acquire),
            vector_backend,
            scoring_fingerprint,
            quality_contract,
            exact_vectors_available,
            exact_vector_rows,
            exact_vectors_mmap,
            live_docs: physical_docs - deleted_docs,
            deleted_docs,
            live_revision: guard.live_docs.revision(),
            wal_generation,
            wal_high_watermark,
            wal_clocked,
            document_slots,
        }))
    }

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSearch, request, |request| async move {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let state = self.state.clone();
            let slot_offset = self.config.slot_offset;
            let stream_signals = Arc::clone(&self.stream_signals);

            tokio::spawn(async move {
                // Protocol: the first message must be Start.
                let start = match inbound.message().await {
                    Ok(Some(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::Start(start)),
                    })) => start,
                    Ok(_) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first StreamSearchRequest must be StartStreamSearch",
                            )))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if start.initial_floor.is_some_and(f32::is_nan) {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "initial_floor must not be NaN",
                        )))
                        .await;
                    return;
                }
                // Shape-only filter validation before any scan work, the
                // same order the lexical routes use.
                let geo_regions = match validate_geo_filters(&start.geo_filters) {
                    Ok(regions) => regions,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if let Some(f) = start.filter.as_ref() {
                    if let Err(e) = crate::filter::validate_filter(f) {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }

                // Floor raises and cancellation fold into one stream state that
                // the blocking scan polls before every chunk. The gRPC request
                // stream is authoritative; UDP is only a fast lossy duplicate.
                let signals = Arc::new(StreamSignals::new(
                    start.initial_floor.unwrap_or(f32::NEG_INFINITY),
                ));
                let udp_token = (start.floor_token != 0).then_some(start.floor_token);
                if let Some(token) = udp_token {
                    stream_signals
                        .lock()
                        .expect("stream signal registry poisoned")
                        .insert(token, Arc::clone(&signals));
                }
                let pump_signals = Arc::clone(&signals);
                tokio::spawn(async move {
                    loop {
                        match inbound.message().await {
                            Ok(Some(StreamSearchRequest {
                                payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                            })) => raise_floor_cell(&pump_signals.floor, u.floor),
                            Ok(Some(StreamSearchRequest {
                                payload: Some(stream_search_request::Payload::Stop(_)),
                            })) => {
                                pump_signals
                                    .cancelled
                                    .store(true, std::sync::atomic::Ordering::Release);
                                break;
                            }
                            // Duplicate Start or empty payload: ignore.
                            Ok(Some(_)) => {}
                            // Client closed or the stream broke: no more
                            // raises can arrive; the scan finishes (or hits
                            // the dead response channel) on its own.
                            Ok(None) | Err(_) => break,
                        }
                    }
                });

                let scan_tx = tx.clone();
                let scan_signals = Arc::clone(&signals);
                let scan =
                    tokio::task::spawn_blocking(move || -> Result<StreamSearchSummary, Status> {
                        // Document mode: resolve each emitted slot's parent.
                        // parent_map takes its own locks (read to build,
                        // write to cache), so it runs before the scan's read
                        // guard is taken, exactly as in the bidi collapse
                        // path.
                        let parents = if start.collapse_parents {
                            let n = {
                                let guard = state.read().expect("shard state lock poisoned");
                                guard.index.as_ref().map_or(0, |index| index.len())
                            };
                            Some(Self::parent_map(&state, slot_offset, n))
                        } else {
                            None
                        };
                        let guard = state.read().expect("shard state lock poisoned");
                        let index = guard.index.as_ref().ok_or_else(|| {
                            Status::failed_precondition(
                                "shard has no index yet (set calibration or add vectors)",
                            )
                        })?;
                        let dim = index
                            .dim_opt()
                            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
                        if start.vector.len() != dim {
                            return Err(Status::invalid_argument(format!(
                                "query vector has dim {}, index expects {dim}",
                                start.vector.len()
                            )));
                        }
                        if let Some((_, coord, value)) =
                            first_invalid_coordinate(&start.vector, dim)
                        {
                            return Err(Status::invalid_argument(format!(
                                "query coordinate {coord} is invalid: {value}"
                            )));
                        }
                        if let Some(p) = parents.as_ref() {
                            if p.len() != index.len() {
                                return Err(Status::aborted(
                                    "shard grew between setup and scan; retry",
                                ));
                            }
                        }
                        let scoring_fingerprint = index.descriptor().scoring_fingerprint;
                        if scoring_fingerprint.is_empty() {
                            return Err(Status::failed_precondition(
                                "vector backend has no scoring fingerprint",
                            ));
                        }

                        // The request's filters as a slot allowlist, resolved
                        // under this scan's read guard so columns and index
                        // are one snapshot. The streaming engine emits every
                        // live slot at or above the floor; with an allowlist
                        // "live" means "survived the filters", so the
                        // completion certificate covers the filtered corpus
                        // and means exactly what it meant before.
                        let (_, allow) = resolve_shard_filters(
                            guard.bm25.as_ref(),
                            guard.live_docs.words(),
                            index.len(),
                            &start.geo_filters,
                            &geo_regions,
                            start.filter.as_ref(),
                        )?;
                        let (geo_columns_known, filter_columns_known) = filter_known_flags(
                            guard.bm25.as_ref(),
                            &start.geo_filters,
                            start.filter.as_ref(),
                        );

                        let mut options = VectorSearchOptions::new();
                        if let Some(a) = allow.as_deref() {
                            options = options.with_mask(a);
                        }
                        let mut floor_now = f32::NEG_INFINITY;
                        if let Some(f) = start.initial_floor {
                            options = options.with_initial_threshold(f);
                            floor_now = f;
                        }
                        let mut raises = 0u64;
                        let stride = if parents.is_some() { 20 } else { 12 };
                        let summary = index
                            .try_search_streaming_controlled(
                                &start.vector,
                                options,
                                |batch| {
                                    // Pack the batch as fixed-stride LE records
                                    // (u64 global id, f32 score, and in document
                                    // mode the slot's u64 parent), fused into the
                                    // slot-to-global-id rebase — one pass, no
                                    // per-hit messages. Real emissions only
                                    // carry live slots; a negative would be an
                                    // engine contract break, dropped rather
                                    // than wrapped into a bogus global id.
                                    let mut hits: Vec<u8> =
                                        Vec::with_capacity(stride * batch.slots.len());
                                    for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                                        if slot < 0 {
                                            continue;
                                        }
                                        hits.extend_from_slice(
                                            &(slot_offset + slot as u64).to_le_bytes(),
                                        );
                                        hits.extend_from_slice(&score.to_le_bytes());
                                        if let Some(p) = parents.as_deref() {
                                            hits.extend_from_slice(&p[slot as usize].to_le_bytes());
                                        }
                                    }
                                    let sent = scan_tx.blocking_send(Ok(StreamSearchResponse {
                                        payload: Some(stream_search_response::Payload::Batch(
                                            StreamSearchBatch { hits },
                                        )),
                                    }));
                                    if sent.is_err() {
                                        VectorStreamControl::Stop
                                    } else {
                                        VectorStreamControl::Continue
                                    }
                                },
                                || {
                                    if scan_signals
                                        .cancelled
                                        .load(std::sync::atomic::Ordering::Acquire)
                                    {
                                        return VectorStreamControl::Stop;
                                    }
                                    let f = f32::from_bits(
                                        scan_signals
                                            .floor
                                            .load(std::sync::atomic::Ordering::Acquire),
                                    );
                                    if f > floor_now {
                                        floor_now = f;
                                        raises += 1;
                                        VectorStreamControl::RaiseFloor(f)
                                    } else {
                                        VectorStreamControl::Continue
                                    }
                                },
                            )
                            .map_err(|e| Status::invalid_argument(e.to_string()))?;
                        Ok(StreamSearchSummary {
                            completed: summary.completed
                                && !scan_signals
                                    .cancelled
                                    .load(std::sync::atomic::Ordering::Acquire),
                            emitted: summary.emitted as u64,
                            blocks_scanned: summary.units_scanned as u64,
                            floor_raises_applied: raises,
                            geo_columns_known,
                            filter_columns_known,
                            scoring_fingerprint,
                        })
                    });
                let outcome = scan.await;
                if let Some(token) = udp_token {
                    stream_signals
                        .lock()
                        .expect("stream signal registry poisoned")
                        .remove(&token);
                }
                match outcome {
                    Ok(Ok(summary)) => {
                        let _ = tx
                            .send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::Summary(summary)),
                            }))
                            .await;
                    }
                    Ok(Err(status)) => {
                        let _ = tx.send(Err(status)).await;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("stream scan panicked: {e}"))))
                            .await;
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn get_vector_backend(
        &self,
        _request: Request<GetVectorBackendRequest>,
    ) -> Result<Response<GetVectorBackendResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let Some(index) = guard.index.as_ref() else {
            return Ok(Response::new(GetVectorBackendResponse {
                descriptor: None,
                config: None,
                num_vectors: 0,
            }));
        };
        let config = index
            .backend_config()
            .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
        Ok(Response::new(GetVectorBackendResponse {
            descriptor: Some(wire_backend_descriptor(index)),
            config: Some(wire_backend_config(&config)),
            num_vectors: index.len() as u64,
        }))
    }

    async fn configure_vector_backend(
        &self,
        request: Request<ConfigureVectorBackendRequest>,
    ) -> Result<Response<ConfigureVectorBackendResponse>, Status> {
        let already_configured = self.apply_backend_config(request.into_inner())?;
        Ok(Response::new(ConfigureVectorBackendResponse {
            already_configured,
        }))
    }

    async fn get_calibration(
        &self,
        _request: Request<GetCalibrationRequest>,
    ) -> Result<Response<GetCalibrationResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let (dim, bit_width, num_vectors, shift, scale) = match guard.index.as_ref() {
            Some(index) => {
                let (shift, scale) = calibration_of(index).unwrap_or_default();
                (
                    index.dim_opt().unwrap_or(0) as u32,
                    index.bits_per_dimension().unwrap_or(self.config.bit_width) as u32,
                    index.len() as u64,
                    shift,
                    scale,
                )
            }
            None => (0, 0, 0, Vec::new(), Vec::new()),
        };
        Ok(Response::new(GetCalibrationResponse {
            dim,
            bit_width,
            num_vectors,
            shift,
            scale,
        }))
    }

    async fn set_calibration(
        &self,
        request: Request<SetCalibrationRequest>,
    ) -> Result<Response<SetCalibrationResponse>, Status> {
        let already_seeded = self.apply_calibration(&request.into_inner())?;
        Ok(Response::new(SetCalibrationResponse { already_seeded }))
    }

    async fn add_vectors(
        &self,
        request: Request<Streaming<AddVectorsRequest>>,
    ) -> Result<Response<AddVectorsResponse>, Status> {
        crate::metrics::timed(Route::AddVectors, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let stable_routing_key = replication_stable_key(&request)?;
            let mut inbound = request.into_inner();
            let mut added = 0u64;
            let mut first_id = 0u64;
            let mut batches = 0usize;
            while let Some(batch) = inbound.message().await? {
                batches += 1;
                if stable_routing_key.is_some() && batches > 1 {
                    return Err(Status::invalid_argument(
                    "a replication stable-key metadata value may carry exactly one vector batch",
                ));
                }
                let service = self.clone();
                let key = stable_routing_key.clone();
                let (batch_added, batch_first_id) =
                    tokio::task::spawn_blocking(move || service.apply_batch(batch, key))
                        .await
                        .map_err(|e| Status::internal(format!("add task failed: {e}")))??;
                if added == 0 && batch_added > 0 {
                    first_id = batch_first_id;
                }
                added += batch_added;
                self.seal_if_due().await?;
            }
            let (total, wal_generation) = {
                let guard = self.state.read().expect("shard state lock poisoned");
                (
                    guard.index.as_ref().map_or(0, |i| i.len() as u64),
                    guard.wal.as_ref().map_or(0, WalWriter::generation),
                )
            };
            crate::metrics::add_ingested(0, added);
            Ok(Response::new(AddVectorsResponse {
                added,
                total,
                first_id,
                wal_generation,
            }))
        })
        .await
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.flush_index())
            .await
            .map_err(|e| Status::internal(format!("flush task failed: {e}")))?
            .map(Response::new)
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "shard has no persistence path (index_path); a snapshot install IS persistence",
            )
        })?;
        let tmp_dir = generation_tmp_dir(&path);

        let mut inbound = request.into_inner();
        // Protocol: the first message must be the manifest.
        let manifest = match inbound.message().await? {
            Some(SnapshotChunk {
                payload: Some(snapshot_chunk::Payload::Manifest(m)),
            }) if m.vector_bytes > 0 => m,
            _ => {
                return Err(Status::invalid_argument(
                    "first SnapshotChunk must be a SnapshotManifest with vector_bytes > 0",
                ))
            }
        };

        if let Err(e) = Self::receive_image(&mut inbound, &manifest, &tmp_dir).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            return Err(e);
        }

        let service = self.clone();
        let cleanup = tmp_dir.clone();
        let with_exact_vectors = manifest.exact_vector_bytes > 0;
        let with_bm25 = manifest.bm25_bytes > 0;
        let with_live_docs = manifest.live_docs_bytes > 0;
        let result = tokio::task::spawn_blocking(move || {
            service.apply_snapshot(&tmp_dir, with_exact_vectors, with_bm25, with_live_docs)
        })
        .await
        .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
        if result.is_err() {
            // Rejected AFTER receive (bad image, calibration mismatch):
            // leave no staging dir behind either.
            let _ = tokio::fs::remove_dir_all(&cleanup).await;
        }
        result.map(Response::new)
    }

    async fn export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<ExportSnapshotResponse>, Status> {
        crate::metrics::timed(Route::ExportSnapshot, request, |request| async move {
            let directory = PathBuf::from(request.into_inner().directory);
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.export_snapshot_blocking(&directory))
                .await
                .map_err(|e| Status::internal(format!("export task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    async fn stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSnapshot, request, |_request| async move {
            let path = self.config.index_path.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no persistence path (index_path); a snapshot export needs one",
                )
            })?;
            let staging = export_staging_dir(&path);
            let service = self.clone();
            let dir = staging.clone();
            let exported =
                tokio::task::spawn_blocking(move || service.export_snapshot_blocking(&dir))
                    .await
                    .map_err(|e| Status::internal(format!("export task failed: {e}")))?;
            let exported = match exported {
                Ok(exported) => exported,
                Err(e) => {
                    let _ = tokio::fs::remove_dir_all(&staging).await;
                    return Err(e);
                }
            };
            let manifest = exported
                .manifest
                .ok_or_else(|| Status::internal("export produced no manifest"))?;
            let artifacts: Vec<PathBuf> = manifest
                .artifacts
                .iter()
                .map(|artifact| staging.join(&artifact.file))
                .collect();
            let (tx, rx) = mpsc::channel::<Result<SnapshotChunk, Status>>(2);
            tokio::spawn(async move {
                let first = SnapshotChunk {
                    payload: Some(snapshot_chunk::Payload::Repository(manifest)),
                };
                if tx.send(Ok(first)).await.is_ok() {
                    for artifact in artifacts {
                        if !stream_file(&tx, &artifact).await {
                            break;
                        }
                    }
                }
                let _ = tokio::fs::remove_dir_all(&staging).await;
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn install_snapshot_from(
        &self,
        request: Request<InstallSnapshotFromRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        crate::metrics::timed(Route::InstallSnapshotFrom, request, |request| async move {
            use crate::pb::install_snapshot_from_request::Source;
            let path = self.config.index_path.clone().ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no persistence path (index_path); a snapshot install IS persistence",
                )
            })?;
            let req = request.into_inner();
            let source = req.source.ok_or_else(|| {
                Status::invalid_argument(
                    "InstallSnapshotFrom needs a source: directory, url, or peer_addr",
                )
            })?;
            let tmp_dir = generation_tmp_dir(&path);
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            let staged = match source {
            Source::Directory(directory) => {
                let staging = tmp_dir.clone();
                tokio::task::spawn_blocking(move || {
                    Self::stage_from_directory(Path::new(&directory), &staging)
                })
                .await
                .map_err(|e| Status::internal(format!("stage task failed: {e}")))?
            }
            #[cfg(feature = "net")]
            Source::Url(url) => {
                crate::snapshot::stage_from_url(&url, &req.bearer_token, &tmp_dir).await
            }
            #[cfg(feature = "net")]
            Source::PeerAddr(peer) => crate::snapshot::stage_from_peer(&peer, &tmp_dir).await,
            #[cfg(not(feature = "net"))]
            Source::Url(_) | Source::PeerAddr(_) => Err(Status::failed_precondition(
                "this build has no network stack (feature `net` is off); install from a directory",
            )),
        };
            let (manifest, sha) = match staged {
                Ok(staged) => staged,
                Err(e) => {
                    let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
                    return Err(e);
                }
            };
            if let Err(e) = repo::check_expected_sha(&req.expected_manifest_sha256, &sha) {
                let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
                return Err(Status::invalid_argument(e));
            }
            let service = self.clone();
            let cleanup = tmp_dir.clone();
            let result = tokio::task::spawn_blocking(move || {
                service.install_staged_repository(&tmp_dir, &manifest)
            })
            .await
            .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
            if result.is_err() {
                let _ = tokio::fs::remove_dir_all(&cleanup).await;
            }
            result.map(Response::new)
        })
        .await
    }

    async fn add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        crate::metrics::timed(Route::AddDocuments, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let addr = self.config.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable("no analysis backend configured for this shard (analysis_addr)")
            })?;
            let stable_routing_key = replication_stable_key(&request)?;
            let mut inbound = request.into_inner();
            let mut source = IngestSource::Plain {
                stream: &mut inbound,
                stable_routing_key,
                consumed: false,
            };
            let mut added = 0u64;
            let mut first_id = 0u64;
            // Analysis dominates bulk ingest. One analysis stream covers the
            // whole call, using either native bounded channels or sidecar flow
            // control. Documents are applied strictly in arrival order, so ids
            // and WAL order stay deterministic.
            //
            // A sidecar without AnalyzeStream is REFUSED rather than served on
            // the old per-document unary path. That fallback existed and cost
            // real debugging time: a stale sidecar silently took it, then its
            // gRPC server GOAWAYed the connection after ~70 streams, and the
            // bulk driver died seconds into a multi-hour job with an opaque
            // "h2 protocol error" while this node logged nothing and stayed
            // healthy. Degrading quietly turned a one-line version mismatch
            // into an h2 forensics exercise; failing here names it instead.
            if let Some(first) = source.next().await? {
                match crate::analyzer::AnalyzeStream::open_with_vocab(
                    &addr,
                    first.req.analysis.as_ref(),
                    self.vocab.clone(),
                    session_layers(
                        &first.req,
                        self.phrase_index.as_deref(),
                        &self.config.sentence_fields,
                    ),
                )
                .await
                {
                    Ok(session) => {
                        self.ingest_streamed(
                            session,
                            first,
                            &mut source,
                            &addr,
                            &mut added,
                            &mut first_id,
                        )
                        .await?;
                    }
                    Err(status) if status.code() == tonic::Code::Unimplemented => {
                        return Err(Status::failed_precondition(format!(
                            "analysis sidecar at {addr} does not implement AnalyzeStream; \
                         it predates the RPC and must be rebuilt (./gradlew installDist \
                         in grpc-opennlp-analysis). Refusing to ingest on the removed \
                         unary path."
                        )));
                    }
                    Err(status) => return Err(status),
                }
            }
            let (total, wal_generation) = {
                let guard = self.state.read().expect("shard state lock poisoned");
                (
                    guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
                    guard.wal.as_ref().map_or(0, WalWriter::generation),
                )
            };
            crate::metrics::add_ingested(added, 0);
            self.seal_if_due().await?;
            Ok(Response::new(AddDocumentsResponse {
                added,
                total,
                first_id,
                wal_generation,
            }))
        })
        .await
    }

    async fn delete_documents(
        &self,
        request: Request<DeleteDocumentsRequest>,
    ) -> Result<Response<DeleteDocumentsResponse>, Status> {
        crate::metrics::timed(Route::DeleteDocuments, request, |request| async move {
            let req = request.into_inner();
            let mut guard = self.state.write().expect("shard state lock poisoned");
            self.delete_documents_locked(&mut guard, &req.doc_ids, req.expected_wal_generation)
                .map(Response::new)
        })
        .await
    }

    async fn commit_replacements(
        &self,
        request: Request<CommitReplacementsRequest>,
    ) -> Result<Response<CommitReplacementsResponse>, Status> {
        crate::metrics::timed(Route::CommitReplacements, request, |request| async move {
            let req = request.into_inner();
            let mut guard = self.state.write().expect("shard state lock poisoned");
            self.commit_replacements_locked(
                &mut guard,
                &req.replacements,
                req.expected_wal_generation,
            )
            .map(Response::new)
        })
        .await
    }

    async fn ingest_mapped(
        &self,
        request: Request<Streaming<IngestMappedRequest>>,
    ) -> Result<Response<IngestMappedResponse>, Status> {
        crate::metrics::timed(Route::IngestMapped, request, |request| async move {
            let _ingest = self.claim_ingest()?;
            let addr = self.config.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable("no analysis backend configured for this shard (analysis_addr)")
            })?;
            let mut inbound = request.into_inner();
            // Protocol: the first message must be the bind, and the bind
            // must stand — fingerprint agreement, declared columns, a body
            // — before a single document streams.
            let bind = match inbound.message().await? {
                Some(IngestMappedRequest {
                    payload: Some(crate::pb::ingest_mapped_request::Payload::Bind(bind)),
                }) => bind,
                _ => {
                    return Err(Status::invalid_argument(
                        "first IngestMappedRequest must be a MappedBind",
                    ))
                }
            };
            self.admit_collection(&bind.collection)?;
            let extractor = self.bind_mapped(&bind)?;
            let fingerprint = extractor.plan().fingerprint.clone();
            let mut added = 0u64;
            let mut first_id = 0u64;
            let mut source = IngestSource::Mapped(Box::new(MappedSource {
                stream: &mut inbound,
                extractor,
                analysis: bind.analysis.clone(),
                materialize: bind.materialize.clone(),
                position: 0,
                parents: 0,
                rows: std::collections::VecDeque::new(),
            }));
            if let Some(first) = source.next().await? {
                match crate::analyzer::AnalyzeStream::open_with_vocab(
                    &addr,
                    first.req.analysis.as_ref(),
                    self.vocab.clone(),
                    session_layers(
                        &first.req,
                        self.phrase_index.as_deref(),
                        &self.config.sentence_fields,
                    ),
                )
                .await
                {
                    Ok(session) => {
                        self.ingest_streamed(
                            session,
                            first,
                            &mut source,
                            &addr,
                            &mut added,
                            &mut first_id,
                        )
                        .await?;
                    }
                    Err(status) if status.code() == tonic::Code::Unimplemented => {
                        return Err(Status::failed_precondition(format!(
                            "analysis sidecar at {addr} does not implement AnalyzeStream; \
                         it predates the RPC and must be rebuilt (./gradlew installDist \
                         in grpc-opennlp-analysis). Refusing to ingest on the removed \
                         unary path."
                        )));
                    }
                    Err(status) => return Err(status),
                }
            }
            let (total, wal_generation) = {
                let guard = self.state.read().expect("shard state lock poisoned");
                (
                    guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
                    guard.wal.as_ref().map_or(0, WalWriter::generation),
                )
            };
            // Each mapped row carries exactly one vector.
            crate::metrics::add_ingested(added, added);
            let parents = match &source {
                IngestSource::Mapped(mapped) => mapped.parents,
                IngestSource::Plain { .. } => unreachable!("this handler built a mapped source"),
            };
            Ok(Response::new(IngestMappedResponse {
                added,
                total,
                first_id,
                fingerprint,
                parents,
                wal_generation,
            }))
        })
        .await
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            let req = request.into_inner();
            let guard = self.state.read().expect("shard state lock poisoned");
            let (doc_count, total_doc_length, doc_frequencies, field_stats) = match guard
                .bm25
                .as_ref()
            {
                Some(store) => {
                    let index = store.as_index().ok_or_else(|| {
                        Status::failed_precondition("bm25 bulk build in progress; Flush first")
                    })?;
                    // Per-field shares: a shard without a named field
                    // answers zeros — that IS its share of the globals.
                    let field_stats = req
                        .fields
                        .iter()
                        .map(|ft| match store.field_index(&ft.field) {
                            Some(fi) => {
                                let view = store
                                    .field_view(fi)
                                    .expect("as_index above proves the shard is searchable");
                                let (total_doc_length, doc_frequencies) = live_field_stats(
                                    view.as_ref(),
                                    &ft.terms,
                                    &guard.live_docs,
                                    store.next_doc_id(),
                                );
                                crate::pb::FieldStats {
                                    sentences: store.field_has_sentences(fi),
                                    total_doc_length,
                                    doc_frequencies,
                                    known: true,
                                    positions: store.field_has_positions(fi),
                                }
                            }
                            None => crate::pb::FieldStats {
                                sentences: false,
                                total_doc_length: 0,
                                doc_frequencies: vec![0; ft.terms.len()],
                                known: false,
                                positions: false,
                            },
                        })
                        .collect();
                    let (total_doc_length, doc_frequencies) =
                        live_field_stats(index, &req.terms, &guard.live_docs, store.next_doc_id());
                    (
                        live_document_count(store, &guard.live_docs),
                        total_doc_length,
                        doc_frequencies,
                        field_stats,
                    )
                }
                None => (
                    0,
                    0,
                    req.terms.iter().map(|_| 0).collect(),
                    // No postings at all: this shard knows no field, which is
                    // a different statement from "the field does not exist".
                    req.fields
                        .iter()
                        .map(|ft| crate::pb::FieldStats {
                            sentences: false,
                            total_doc_length: 0,
                            doc_frequencies: vec![0; ft.terms.len()],
                            known: false,
                            positions: false,
                        })
                        .collect(),
                ),
            };
            Ok(Response::new(TermStatsResponse {
                doc_count,
                total_doc_length,
                doc_frequencies,
                field_stats,
                stats_epoch: guard.stats_epoch,
            }))
        })
        .await
    }

    async fn expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let req = request.into_inner();
            if req.prefix.is_empty() {
                return Err(Status::invalid_argument("a term prefix must be non-empty"));
            }
            let guard = self.state.read().expect("shard state lock poisoned");
            let Some(store) = guard.bm25.as_ref() else {
                return Ok(Response::new(crate::pb::ExpandTermPrefixResponse {
                    terms: Vec::new(),
                    count: 0,
                    known: false,
                }));
            };
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                ));
            }
            let Some(fi) = store.field_index(&req.field) else {
                return Ok(Response::new(crate::pb::ExpandTermPrefixResponse {
                    terms: Vec::new(),
                    count: 0,
                    known: false,
                }));
            };
            let view = store
                .field_view(fi)
                .expect("as_index above proves the shard is searchable");
            let (terms, count) = match view.expand_prefix(&req.prefix, req.cap as usize) {
                Ok(terms) => {
                    let count = terms.len() as u64;
                    (terms, count)
                }
                Err(count) => (Vec::new(), count as u64),
            };
            Ok(Response::new(crate::pb::ExpandTermPrefixResponse {
                terms,
                count,
                known: true,
            }))
        })
        .await
    }

    /// Autocomplete scan over one field's dictionary (`docs/suggest.md`):
    /// the prefix scan of [`Self::expand_term_prefix`] with each term's
    /// posting df read from the directory (heap: the posting list's
    /// length; file: the directory entry; segmented: the sum over parts
    /// and tail). Past `max_scan` the shard reports the count and no
    /// entries. `tombstoned_rows` tells the coordinator the df still
    /// counts deleted rows until compaction.
    async fn suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let req = request.into_inner();
            if req.prefix.is_empty() {
                return Err(Status::invalid_argument("a term prefix must be non-empty"));
            }
            if req.max_scan == 0 {
                return Err(Status::invalid_argument(
                    "max_scan must be positive: it bounds the dictionary scan",
                ));
            }
            let guard = self.state.read().expect("shard state lock poisoned");
            let tombstoned_rows = guard.live_docs.deleted_count();
            let unknown = || crate::pb::SuggestTermsResponse {
                entries: Vec::new(),
                count: 0,
                known: false,
                tombstoned_rows,
            };
            let Some(store) = guard.bm25.as_ref() else {
                return Ok(Response::new(unknown()));
            };
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                ));
            }
            let Some(fi) = store.field_index(&req.field) else {
                return Ok(Response::new(unknown()));
            };
            let view = store
                .field_view(fi)
                .expect("as_index above proves the shard is searchable");
            let max_scan = usize::try_from(req.max_scan).map_err(|_| {
                Status::invalid_argument(format!("max_scan {} is out of range", req.max_scan))
            })?;
            let (entries, count) = match view.suggest_prefix(&req.prefix, max_scan) {
                Ok(entries) => {
                    let count = entries.len() as u64;
                    let entries = entries
                        .into_iter()
                        .map(|(term, df)| crate::pb::SuggestTermEntry {
                            term,
                            df: u64::from(df),
                        })
                        .collect();
                    (entries, count)
                }
                Err(count) => (Vec::new(), count as u64),
            };
            Ok(Response::new(crate::pb::SuggestTermsResponse {
                entries,
                count,
                known: true,
                tombstoned_rows,
            }))
        })
        .await
    }

    async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_query(req))
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    async fn bm25_phrase_query(
        &self,
        request: Request<crate::pb::Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_phrase_query(req))
                .await
                .map_err(|error| {
                    Status::internal(format!("bm25 phrase query task failed: {error}"))
                })?
                .map(Response::new)
        })
        .await
    }

    async fn bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<Self::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            let service = self.clone();

            tokio::spawn(async move {
                // Protocol: the first message must be Start.
                let req = match inbound.message().await {
                    Ok(Some(Bm25QueryStreamRequest {
                        payload: Some(bm25_query_stream_request::Payload::Start(req)),
                    })) => req,
                    Ok(_) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                            )))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

                // Floor updates arrive on the same stream; a pump task folds
                // them into a watch cell the blocking scan polls each loop
                // iteration. Updates are monotone maxes, so only raises are
                // stored — exactly the SearchShard pump.
                let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
                let cancelled = Arc::new(AtomicBool::new(false));
                let pump_cancelled = Arc::clone(&cancelled);
                tokio::spawn(async move {
                    loop {
                        match inbound.message().await {
                            Ok(Some(Bm25QueryStreamRequest {
                                payload: Some(bm25_query_stream_request::Payload::FloorUpdate(u)),
                            })) => {
                                floor_tx.send_if_modified(|cur| {
                                    if !u.floor.is_nan() && u.floor > *cur {
                                        *cur = u.floor;
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                            Ok(Some(Bm25QueryStreamRequest {
                                payload: Some(bm25_query_stream_request::Payload::Stop(_)),
                            })) => {
                                pump_cancelled.store(true, AtomicOrdering::Release);
                                break;
                            }
                            // Duplicate Start or empty payload: ignore.
                            Ok(Some(_)) => {}
                            // Client closed or the stream broke: stop pumping;
                            // the scan finishes on its own either way.
                            Ok(None) | Err(_) => break,
                        }
                    }
                });

                // The scorer-side hook: publish strict raises of the running
                // k-th best (gated by the node's floor knobs, never blocking
                // the scan — dropped raises are superseded by the next one),
                // and hand back the highest coordinator floor seen.
                let share = service.config.share_floors;
                let floor_delta = service.config.floor_delta;
                let min_interval = (service.config.floor_min_interval_ms > 0).then(|| {
                    std::time::Duration::from_millis(service.config.floor_min_interval_ms)
                });
                let scan_tx = tx.clone();
                let hook_cancelled = Arc::clone(&cancelled);
                let mut last_published = f32::NEG_INFINITY;
                let mut last_at: Option<std::time::Instant> = None;
                let mut hook = move |seed: Option<f32>| -> Option<f32> {
                    if hook_cancelled.load(AtomicOrdering::Acquire) || scan_tx.is_closed() {
                        hook_cancelled.store(true, AtomicOrdering::Release);
                        return Some(f32::INFINITY);
                    }
                    if !share {
                        return None;
                    }
                    if let Some(seed) = seed {
                        let debounced = matches!(
                            (min_interval, last_at),
                            (Some(interval), Some(at)) if at.elapsed() < interval
                        );
                        if seed > last_published + floor_delta && !debounced {
                            last_published = seed;
                            last_at = Some(std::time::Instant::now());
                            let _ = scan_tx.try_send(Ok(Bm25QueryStreamResponse {
                                payload: Some(bm25_query_stream_response::Payload::FloorUpdate(
                                    FloorUpdate { floor: seed },
                                )),
                            }));
                        }
                    }
                    let f = *floor_rx.borrow();
                    (f != f32::NEG_INFINITY).then_some(f)
                };

                let scoring_fingerprint = bm25_scoring_fingerprint(&req);
                let candidate_tx = tx.clone();
                let scan_cancelled = Arc::clone(&cancelled);
                let outcome = tokio::task::spawn_blocking(move || {
                    // Stay below tonic's default 4 MiB even if a caller lowers
                    // the process-wide cap. 64 KiB amortizes framing without
                    // delaying the first useful candidates.
                    const BATCH_BYTES: usize = (64 * 1024 / 12) * 12;
                    let mut pending = Vec::with_capacity(BATCH_BYTES);
                    let mut emitted = 0u64;
                    let response = {
                        let mut emit = |doc_id: u32, score: f32| {
                            pending.extend_from_slice(
                                &(service.config.slot_offset + u64::from(doc_id)).to_le_bytes(),
                            );
                            pending.extend_from_slice(&score.to_le_bytes());
                            if pending.len() >= BATCH_BYTES {
                                let records = (pending.len() / 12) as u64;
                                let batch = std::mem::replace(
                                    &mut pending,
                                    Vec::with_capacity(BATCH_BYTES),
                                );
                                if candidate_tx
                                    .blocking_send(Ok(Bm25QueryStreamResponse {
                                        payload: Some(
                                            bm25_query_stream_response::Payload::CandidateBatch(
                                                Bm25CandidateBatch { candidates: batch },
                                            ),
                                        ),
                                    }))
                                    .is_ok()
                                {
                                    emitted += records;
                                } else {
                                    scan_cancelled.store(true, AtomicOrdering::Release);
                                }
                            }
                        };
                        service.run_bm25_query_live(req, Some(&mut hook), Some(&mut emit))
                    };
                    if response.is_ok() && !pending.is_empty() {
                        let records = (pending.len() / 12) as u64;
                        if candidate_tx
                            .blocking_send(Ok(Bm25QueryStreamResponse {
                                payload: Some(bm25_query_stream_response::Payload::CandidateBatch(
                                    Bm25CandidateBatch {
                                        candidates: pending,
                                    },
                                )),
                            }))
                            .is_ok()
                        {
                            emitted += records;
                        } else {
                            scan_cancelled.store(true, AtomicOrdering::Release);
                        }
                    }
                    let completed = !scan_cancelled.load(AtomicOrdering::Acquire);
                    (response, completed, emitted)
                })
                .await
                .unwrap_or_else(|e| {
                    (
                        Err(Status::internal(format!("bm25 stream task failed: {e}"))),
                        false,
                        0,
                    )
                });
                let _ = match outcome {
                    (Ok(response), completed, candidates_emitted) => {
                        tx.send(Ok(Bm25QueryStreamResponse {
                            payload: Some(bm25_query_stream_response::Payload::Completion(
                                Bm25StreamCompletion {
                                    completed,
                                    response: completed.then_some(response),
                                    scoring_fingerprint,
                                    candidates_emitted,
                                },
                            )),
                        }))
                        .await
                    }
                    (Err(e), _, _) => tx.send(Err(e)).await,
                };
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_rescore(req))
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    async fn fetch_values(
        &self,
        request: Request<crate::pb::FetchValuesRequest>,
    ) -> Result<Response<crate::pb::FetchValuesResponse>, Status> {
        crate::metrics::timed(Route::FetchValues, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let resp = tokio::task::spawn_blocking(
                move || -> Result<crate::pb::FetchValuesResponse, Status> {
                    // Stage parameters validate everywhere they arrive; a
                    // malformed stage is a request error, not a shard gap.
                    let specs = parse_score_stages(&req.stages)?;
                    let guard = state.read().expect("shard state lock poisoned");
                    let projection_leaves = {
                        let mut leaves = Vec::new();
                        for p in &req.projections {
                            if let Some(expr) = p.expr.as_ref() {
                                crate::values::column_leaves(expr, &mut leaves);
                            }
                        }
                        leaves
                    };
                    // No column tables at all: this shard holds none of the
                    // candidates' values and resolves no column.
                    let Some(store) = guard.bm25.as_ref() else {
                        return Ok(crate::pb::FetchValuesResponse {
                            rows: Vec::new(),
                            stage_columns_known: vec![false; req.stages.len()],
                            projection_leaves_known: vec![false; projection_leaves.len()],
                        });
                    };
                    let projection_leaves_known: Vec<bool> = projection_leaves
                        .iter()
                        .map(|leaf| crate::values::leaf_known(leaf, store))
                        .collect();
                    // Projections resolve against this shard's tables once
                    // per request; type conflicts refuse here, by name —
                    // the same rule as the lexical route.
                    let resolved: Vec<crate::values::ResolvedValue> = req
                        .projections
                        .iter()
                        .map(|p| {
                            let expr = p.expr.as_ref().ok_or_else(|| {
                                Status::invalid_argument("projection: empty compiled expression")
                            })?;
                            crate::values::resolve(expr, store).map(|(rv, _)| rv)
                        })
                        .collect::<Result<_, Status>>()?;
                    let chain = store.resolve_chain(&specs);
                    let stage_columns_known =
                        chain.stages.iter().map(|s| s.column.is_some()).collect();
                    let numeric_read = ShardNumericRead(store);
                    let n = u64::from(store.next_doc_id());
                    let mut ids: Vec<u64> = req
                        .candidate_ids
                        .iter()
                        .copied()
                        .filter(|&id| {
                            id >= offset
                                && id - offset < n
                                && !guard.live_docs.is_deleted((id - offset) as usize)
                        })
                        .collect();
                    ids.sort_unstable();
                    ids.dedup();
                    let rows = ids
                        .into_iter()
                        .map(|id| {
                            let local = (id - offset) as u32;
                            crate::pb::FetchedRow {
                                doc_id: id,
                                values: resolved
                                    .iter()
                                    .map(|rv| {
                                        projected_value(
                                            crate::values::eval(rv, local, &numeric_read),
                                            store,
                                        )
                                    })
                                    .collect(),
                                stage_values: chain
                                    .stages
                                    .iter()
                                    .map(|s| crate::pb::ProjectedValue {
                                        value: s
                                            .contribution(local, &numeric_read)
                                            .map(crate::pb::projected_value::Value::DoubleValue),
                                    })
                                    .collect(),
                            }
                        })
                        .collect();
                    Ok(crate::pb::FetchValuesResponse {
                        rows,
                        stage_columns_known,
                        projection_leaves_known,
                    })
                },
            )
            .await
            .map_err(|e| Status::internal(format!("fetch values task failed: {e}")))??;
            Ok(Response::new(resp))
        })
        .await
    }

    async fn vector_rescore(
        &self,
        request: Request<VectorRescoreRequest>,
    ) -> Result<Response<VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let hits = tokio::task::spawn_blocking(move || -> Result<Vec<RawLegHit>, Status> {
                let guard = state.read().expect("shard state lock poisoned");
                // A shard with no index holds none of the candidates.
                let Some(index) = guard.index.as_ref() else {
                    return Ok(Vec::new());
                };
                let Some(dim) = index.dim_opt() else {
                    return Ok(Vec::new());
                };
                if req.vector.len() != dim {
                    return Err(Status::invalid_argument(format!(
                        "query vector has dim {}, index expects {dim}",
                        req.vector.len()
                    )));
                }
                if let Some((_, coord, value)) = first_invalid_coordinate(&req.vector, dim) {
                    return Err(Status::invalid_argument(format!(
                        "query coordinate {coord} is invalid: {value}"
                    )));
                }
                // Route global ids into this shard's live slots; the mask
                // names slots and is sized to the slot count (slots are
                // dense on the mainline engine: no capacity/len split). The
                // kernel short-circuits fully-masked SIMD blocks, so a tiny
                // allowlist costs a mask walk, not a scan.
                let n = index.len();
                let mut mask = vec![false; index.len()];
                let mut allowed = 0usize;
                for &id in &req.candidate_ids {
                    if id >= offset && id - offset < n as u64 {
                        let slot = (id - offset) as usize;
                        if !guard.live_docs.is_deleted(slot) && !mask[slot] {
                            mask[slot] = true;
                            allowed += 1;
                        }
                    }
                }
                if allowed == 0 {
                    return Ok(Vec::new());
                }
                let results = index
                    .try_search_with_mask(&req.vector, allowed, Some(&mask))
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                let hits = results
                    .indices_for_query(0)
                    .iter()
                    .zip(results.scores_for_query(0))
                    .filter(|&(&slot, _)| slot >= 0)
                    .map(|(&slot, &score)| RawLegHit {
                        doc_id: offset + slot as u64,
                        score,
                    })
                    .collect();
                Ok(hits)
            })
            .await
            .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            Ok(Response::new(VectorRescoreResponse { hits }))
        })
        .await
    }

    async fn exact_vector_rescore(
        &self,
        request: Request<ExactVectorRescoreRequest>,
    ) -> Result<Response<ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
        let req = request.into_inner();
        let offset = self.config.slot_offset;
        let state = self.state.clone();
        let parallel = resolved_rerank_parallel(self.config.rerank_parallel);
        let lanes = crate::exact_vectors::rerank_task_count(req.candidate_ids.len(), parallel);
        let permits = self
            .rerank_slots
            .clone()
            .acquire_many_owned(lanes as u32)
            .await
            .map_err(|_| Status::unavailable("exact rerank worker budget closed"))?;
        let result = tokio::task::spawn_blocking(move || -> Result<ExactVectorRescoreResponse, Status> {
            let _permits = permits;
            let guard = state.read().expect("shard state lock poisoned");
            // Product shards may own exact rows for a clustered vector
            // collection without also serving a local provider image. The
            // product slot range is therefore the maximum aligned artifact,
            // not `index.len()` alone.
            let n = guard
                .index
                .as_ref()
                .map_or(0, VectorIndex::len)
                .max(guard.exact_vectors.as_ref().map_or(0, ExactVectorStore::len))
                .max(
                    guard
                        .bm25
                        .as_ref()
                        .map_or(0, |store| store.next_doc_id() as usize),
                );
            let mut slots = Vec::new();
            let mut seen = vec![false; n];
            for id in req.candidate_ids {
                if id >= offset && id - offset < n as u64 {
                    let slot = (id - offset) as usize;
                    if !guard.live_docs.is_deleted(slot) && !seen[slot] {
                        seen[slot] = true;
                        slots.push(slot);
                    }
                }
            }
            if slots.is_empty() {
                return Ok(ExactVectorRescoreResponse::default());
            }
            let exact = guard.exact_vectors.as_ref().ok_or_else(|| {
                Status::failed_precondition(format!(
                    "FP32 rerank requested for shard slots {offset}..{} but this generation \
                     has no exact-vector sidecar; rebuild or backfill it",
                    offset + n as u64
                ))
            })?;
            if let Some(index) = guard.index.as_ref() {
                if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                    return Err(Status::failed_precondition(format!(
                        "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                        exact.dim(),
                        exact.len(),
                        index.dim_opt(),
                        index.len()
                    )));
                }
            }
            let dim = exact.dim().ok_or_else(|| {
                Status::failed_precondition("exact-vector sidecar has no dimension")
            })?;
            let predicted = (slots.len() as u64)
                .checked_mul(dim as u64)
                .and_then(|bytes| bytes.checked_mul(4))
                .ok_or_else(|| Status::resource_exhausted("FP32 rerank byte count overflow"))?;
            if req.max_logical_bytes != 0 && predicted > req.max_logical_bytes {
                return Err(Status::resource_exhausted(format!(
                    "FP32 rerank needs {predicted} logical row bytes on this shard, above max_logical_bytes={}",
                    req.max_logical_bytes
                )));
            }
            let scored = exact
                .score_slots_profiled(&req.vector, &slots, parallel)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            Ok(ExactVectorRescoreResponse {
                hits: scored
                    .rows
                    .into_iter()
                    .map(|(slot, score)| RawLegHit {
                            doc_id: offset + slot as u64,
                            score,
                        })
                    .collect(),
                logical_bytes: scored.logical_bytes,
                pages_touched: scored.pages_touched,
                tasks: scored.tasks,
            })
        })
        .await
        .map_err(|e| Status::internal(format!("exact vector rescore task failed: {e}")))??;
        Ok(Response::new(result))
        })
        .await
    }

    async fn get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
        crate::metrics::timed(Route::GetDocuments, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let guard = self.state.read().expect("shard state lock poisoned");
            let mut documents = Vec::new();
            if let Some(store) = guard.bm25.as_ref() {
                let store = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                for id in req.doc_ids {
                    if id < offset {
                        continue;
                    }
                    let local = (id - offset) as u32;
                    if guard.live_docs.is_deleted(local as usize) {
                        continue;
                    }
                    if let Some(text) = store.text(local) {
                        documents.push(StoredDocument {
                            doc_id: id,
                            text,
                            lineage: store.lineage(local).map(|l| crate::pb::DocLineage {
                                parent_id: l.parent_id,
                                group_id: l.group_id,
                                span_start: l.span_start,
                                span_end: l.span_end,
                            }),
                        });
                    }
                }
            }
            Ok(Response::new(GetDocumentsResponse { documents }))
        })
        .await
    }

    async fn resolve_parents(
        &self,
        request: Request<ResolveParentsRequest>,
    ) -> Result<Response<ResolveParentsResponse>, Status> {
        crate::metrics::timed(Route::ResolveParents, request, |request| async move {
            const SELF_PARENT_TAG: u64 = 1 << 63;
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let guard = self.state.read().expect("shard state lock poisoned");
            let rows = guard
                .index
                .as_ref()
                .map_or(0, |index| index.len() as u64)
                .max(
                    guard
                        .bm25
                        .as_ref()
                        .map_or(0, |store| u64::from(store.next_doc_id())),
                );
            let store = guard.bm25.as_ref().and_then(|store| store.as_index());
            let mut parents = Vec::new();
            for doc_id in req.doc_ids {
                let Some(local) = doc_id.checked_sub(offset) else {
                    continue;
                };
                if local >= rows {
                    continue;
                }
                if guard.live_docs.is_deleted(local as usize) {
                    continue;
                }
                let lineage = u32::try_from(local)
                    .ok()
                    .and_then(|local| store.and_then(|store| store.lineage(local)));
                let (parent_id, group_id) =
                    lineage.map_or((SELF_PARENT_TAG | doc_id, 0), |l| (l.parent_id, l.group_id));
                parents.push(ResolvedParent {
                    doc_id,
                    parent_id,
                    group_id,
                });
            }
            Ok(Response::new(ResolveParentsResponse { parents }))
        })
        .await
    }

    async fn hybrid_shard(
        &self,
        request: Request<HybridShardRequest>,
    ) -> Result<Response<HybridShardResponse>, Status> {
        crate::metrics::timed(Route::HybridShard, request, |request| async move {
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.run_hybrid(request.into_inner()))
                .await
                .map_err(|e| Status::internal(format!("hybrid task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let req = request.into_inner();
            if req.terms.len() != req.global_doc_frequencies.len() {
                return Err(Status::invalid_argument(
                    "terms and global_doc_frequencies must have the same length",
                ));
            }
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                let legs = service.compute_legs(
                    &req.vector,
                    &req.terms,
                    req.global_doc_count,
                    req.global_total_doc_length,
                    &req.global_doc_frequencies,
                    params_from(req.k1, req.b)?,
                    req.k as usize,
                    req.expected_stats_epoch,
                    &LegFilters {
                        geo: &req.geo_filters,
                        regions: geo_regions,
                        tree: req.filter.as_ref(),
                    },
                )?;
                Ok(ShardLegsResponse {
                    vector_hits: legs
                        .vector
                        .into_iter()
                        .map(|(doc_id, score)| RawLegHit {
                            doc_id,
                            score: score as f32,
                        })
                        .collect(),
                    bm25_hits: legs
                        .bm25
                        .into_iter()
                        .map(|(doc_id, score)| RawLegHit {
                            doc_id,
                            score: score as f32,
                        })
                        .collect(),
                    geo_columns_known: legs.geo_columns_known,
                    filter_columns_known: legs.filter_columns_known,
                })
            })
            .await
            .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))?
            .map(Response::new)
        })
        .await
    }
}

// The synchronous BM25 scan bodies. The `NodeService` handlers wrap
// these in `spawn_blocking`: a postings walk is CPU-bound for as long
// as the highest-df term takes, and it must not occupy an async runtime
// worker -- the same discipline every vector scan path follows.
/// The mutation handlers' bodies against a state the caller holds (see
/// [`NodeServiceImpl::apply_batch_locked`]).
impl NodeServiceImpl {
    /// Refuse an id-addressed mutation whose ids were issued under
    /// another WAL generation: a compaction or snapshot install
    /// renumbered the rows since, so the ids name different documents
    /// now (docs/mutations.md). `None` claims nothing.
    fn check_wal_generation(guard: &ShardState, expected: Option<u64>) -> Result<u64, Status> {
        let held = guard.wal.as_ref().map_or(0, WalWriter::generation);
        if expected.is_some_and(|expected| expected != held) {
            return Err(Status::failed_precondition(format!(
                "stale WAL generation: the request's ids were issued under WAL generation \
                 {} but this shard is at {held}; a compaction or snapshot install renumbered \
                 its rows since, so those ids no longer name the same documents — resolve them \
                 again",
                expected.expect("checked above")
            )));
        }
        Ok(held)
    }

    pub(crate) fn delete_documents_locked(
        &self,
        guard: &mut ShardState,
        doc_ids: &[u64],
        expected_wal_generation: Option<u64>,
    ) -> Result<DeleteDocumentsResponse, Status> {
        let wal_generation = Self::check_wal_generation(guard, expected_wal_generation)?;
        let offset = self.config.slot_offset;
        let rows = physical_rows(guard);
        let mut slots = Vec::with_capacity(doc_ids.len());
        for id in doc_ids {
            let local = id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument(format!("document id {id} is below shard offset {offset}"))
            })?;
            if local >= rows {
                return Err(Status::invalid_argument(format!(
                    "document id {id} is outside this shard's {rows} physical rows"
                )));
            }
            slots.push((*id, local as usize));
        }
        let mut deleted = 0u64;
        let mut already_deleted = 0u64;
        for (id, slot) in slots {
            if guard.live_docs.delete(slot) {
                deleted += 1;
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::DeleteDocument(LoggedDeleteDocument { doc_id: id }),
                );
            } else {
                already_deleted += 1;
            }
        }
        if deleted > 0 {
            guard.stats_epoch = guard.stats_epoch.saturating_add(1);
            guard.parents = None;
        }
        Ok(DeleteDocumentsResponse {
            deleted,
            already_deleted,
            live_revision: guard.live_docs.revision(),
            wal_generation,
        })
    }

    pub(crate) fn commit_replacements_locked(
        &self,
        guard: &mut ShardState,
        replacements: &[Replacement],
        expected_wal_generation: Option<u64>,
    ) -> Result<CommitReplacementsResponse, Status> {
        let wal_generation = Self::check_wal_generation(guard, expected_wal_generation)?;
        let offset = self.config.slot_offset;
        let artifact_rows = active_artifact_rows(guard);
        let rows = artifact_rows.iter().copied().max().unwrap_or(0);
        let mut seen = std::collections::HashSet::new();
        let mut pairs = Vec::with_capacity(replacements.len());
        for replacement in replacements {
            if replacement.old_doc_id == replacement.new_doc_id {
                return Err(Status::invalid_argument(
                    "replacement old and new ids must differ",
                ));
            }
            if !seen.insert(replacement.old_doc_id) || !seen.insert(replacement.new_doc_id) {
                return Err(Status::invalid_argument(
                    "a replacement batch may mention each id only once",
                ));
            }
            let old = replacement.old_doc_id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument("replacement old id is below this shard's offset")
            })?;
            let new = replacement.new_doc_id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument("replacement new id is below this shard's offset")
            })?;
            if old >= rows || new >= rows {
                return Err(Status::invalid_argument(
                    "replacement ids must name existing rows on this shard",
                ));
            }
            if artifact_rows
                .iter()
                .any(|artifact_rows| old >= *artifact_rows || new >= *artifact_rows)
            {
                return Err(Status::failed_precondition(
                    "replacement ids must exist in every active provider, exact-vector, and document artifact",
                ));
            }
            if guard.live_docs.is_deleted(new as usize) {
                return Err(Status::failed_precondition(format!(
                    "replacement row {} is already deleted",
                    replacement.new_doc_id
                )));
            }
            pairs.push((*replacement, old as usize));
        }
        let mut committed = 0u64;
        let mut already_committed = 0u64;
        for (replacement, old) in pairs {
            if guard.live_docs.delete(old) {
                committed += 1;
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::Replacement(LoggedReplacement {
                        old_doc_id: replacement.old_doc_id,
                        new_doc_id: replacement.new_doc_id,
                    }),
                );
            } else {
                already_committed += 1;
            }
        }
        if committed > 0 {
            guard.stats_epoch = guard.stats_epoch.saturating_add(1);
            guard.parents = None;
        }
        Ok(CommitReplacementsResponse {
            committed,
            already_committed,
            live_revision: guard.live_docs.revision(),
            wal_generation,
        })
    }

    /// Establish or verify the mapped-plan binding on a state the caller
    /// holds: `Ok(true)` when it was already bound to exactly `incoming`,
    /// `Ok(false)` when this call bound it (logging the Bind record); a
    /// different binding, or a populated unbound shard, refuse by name.
    pub(crate) fn apply_binding_locked(
        guard: &mut ShardState,
        incoming: crate::postings::StoredBinding,
    ) -> Result<bool, Status> {
        match guard.mapped_binding.as_ref() {
            Some(bound) if *bound != incoming => Err(Status::failed_precondition(format!(
                "replica is bound to plan {} body {:?}, source WAL requires plan {} body {:?}",
                bound.plan_fingerprint,
                bound.body_path,
                incoming.plan_fingerprint,
                incoming.body_path
            ))),
            Some(_) => Ok(true),
            None => {
                if physical_rows(guard) != 0 {
                    return Err(Status::failed_precondition(
                        "cannot apply a WAL mapping binding to a populated unbound shard; install the matching base snapshot",
                    ));
                }
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::Bind(crate::pb::wal::LoggedBinding {
                        plan_fingerprint: incoming.plan_fingerprint.clone(),
                        body_path: incoming.body_path.clone(),
                        materialize_sha: incoming.materialize_sha.clone(),
                    }),
                );
                guard.mapped_binding = Some(incoming);
                Ok(false)
            }
        }
    }
}

impl NodeServiceImpl {
    fn run_bm25_query(&self, req: Bm25QueryRequest) -> Result<Bm25QueryResponse, Status> {
        self.run_bm25_query_live(req, None, None)
    }

    fn run_bm25_phrase_query(
        &self,
        request: crate::pb::Bm25PhraseQueryRequest,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = request
            .query
            .ok_or_else(|| Status::invalid_argument("phrase query is missing its BM25 query"))?;
        let phrase_leg = request.phrase_leg as usize;
        let Some(leg) = req.fields.get(phrase_leg) else {
            return Err(Status::invalid_argument(format!(
                "phrase_leg {} is outside {} BM25 fields",
                request.phrase_leg,
                req.fields.len()
            )));
        };
        if request.phrase_term_weights.len() != leg.terms.len() {
            return Err(Status::invalid_argument(
                "phrase_term_weights must be parallel to the selected phrase leg's terms",
            ));
        }
        if request
            .phrase_term_weights
            .iter()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err(Status::invalid_argument(
                "phrase term weights must be finite and non-negative",
            ));
        }
        let phrases = self.phrase_index.as_ref().ok_or_else(|| {
            Status::failed_precondition("this node has no phrase glossary configured")
        })?;
        if leg.field != phrases.phrase_field() {
            return Err(Status::invalid_argument(format!(
                "phrase leg names {:?}, but this node's phrase field is {:?}",
                leg.field,
                phrases.phrase_field()
            )));
        }
        if leg.analysis_fingerprint != phrases.fingerprint() {
            return Err(Status::failed_precondition(format!(
                "phrase query fingerprint {:016x} differs from configured vocabulary {:016x}",
                leg.analysis_fingerprint,
                phrases.fingerprint()
            )));
        }
        self.bm25_query_fused(
            &req,
            Some((phrase_leg, &request.phrase_term_weights)),
            None,
            None,
        )
    }

    /// [`Self::run_bm25_query`] with a mid-scan floor exchange for the
    /// streaming route. Only the flat pruned scorer consumes the hook;
    /// the fused multi-field route and the exhaustive fallbacks ignore
    /// it (no skip surface, so a mid-scan floor saves nothing there).
    fn run_bm25_query_live(
        &self,
        req: Bm25QueryRequest,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        // Fused multi-field legs replace the flat single-field query
        // (docs/multi-field.md).
        if !req.fields.is_empty() {
            if !req.score_stages.is_empty() {
                return Err(Status::invalid_argument(
                    "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
                ));
            }
            return self.bm25_query_fused(&req, None, live, candidate);
        }
        let stage_specs = parse_score_stages(&req.score_stages)?;
        validate_range_facet_fields(&req.range_facet_fields)?;
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let params = params_from(req.k1, req.b)?;
        let stats = bm25::CorpusStats {
            doc_count: req.global_doc_count,
            total_doc_length: req.global_total_doc_length,
            dfs: req.global_doc_frequencies.clone(),
        };
        if req.terms.len() != stats.dfs.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let guard = self.state.read().expect("shard state lock poisoned");
        guard.check_stats_epoch(req.expected_stats_epoch)?;
        // The request's filters, resolved ONCE against this shard's
        // tables and shared by facet counting and the scorers below —
        // one resolution, one truth (docs/geo-columns.md,
        // docs/cel-filters.md). `None` when the request has none, and
        // every path below is then bit-identical to its unfiltered
        // form.
        let doc_filter: Option<crate::filter::DocFilter<'_>> = match guard.bm25.as_ref() {
            Some(store)
                if !req.geo_filters.is_empty()
                    || req.filter.is_some()
                    || req.phrase.is_some()
                    || guard.live_docs.has_deletes() =>
            {
                // The flat route's phrase gate over the body
                // (docs/phrase-proximity.md); same contract as the fused
                // route's per-leg gate.
                let mut phrase = Vec::new();
                if let Some(constraint) = req.phrase.as_ref() {
                    let index = store.as_index().ok_or_else(|| {
                        Status::failed_precondition("bm25 bulk build in progress; Flush first")
                    })?;
                    if !index.has_positions() {
                        return Err(Status::failed_precondition(
                            "field \"body\" has no token positions on this shard; a phrase or \
                             slop query needs --position-fields=body and a rebuilt generation",
                        ));
                    }
                    if constraint.sequence.len() < 2
                        || constraint
                            .sequence
                            .iter()
                            .any(|&i| i as usize >= req.terms.len())
                    {
                        return Err(Status::invalid_argument(format!(
                            "phrase sequence must name at least two of the query's {} terms",
                            req.terms.len()
                        )));
                    }
                    phrase.push(crate::filter::PhraseGate {
                        index,
                        terms: &req.terms,
                        sequence: constraint.sequence.iter().map(|&i| i as usize).collect(),
                        slop: constraint.slop,
                    });
                }
                Some(crate::filter::DocFilter {
                    deleted: guard.live_docs.words(),
                    geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                    pred: req
                        .filter
                        .as_ref()
                        .map(|f| store.resolve_filter(f))
                        .transpose()?,
                    phrase,
                })
            }
            _ => None,
        };
        // Count-then-rank facets over the match set — term matches
        // that survive the filters — before any k/floor narrowing
        // (see count_facets). A shard with no lexical half has no
        // facet table: every requested field is legitimately unknown
        // here.
        let (facets, range_facets, column_stats, distinct) = match guard.bm25.as_ref() {
            Some(store)
                if !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty()
                    || !req.range_facet_fields.is_empty()
                    || !req.stats_fields.is_empty()
                    || !req.cardinality_fields.is_empty() =>
            {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                let numeric_read = ShardNumericRead(store);
                let filter_ctx: bm25::FilterCtx = doc_filter
                    .as_ref()
                    .map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
                store.count_facets(
                    &[(index, &req.terms)],
                    &req.facet_fields,
                    &req.map_facet_fields,
                    &req.range_facet_fields,
                    &req.stats_fields,
                    &req.cardinality_fields,
                    filter_ctx,
                )
            }
            _ => (
                req.facet_fields
                    .iter()
                    .map(|name| (name.clone(), String::new()))
                    .chain(
                        req.map_facet_fields
                            .iter()
                            .map(|m| (m.column.clone(), m.key.clone())),
                    )
                    .map(|(field, key)| crate::pb::FacetFieldCounts {
                        field,
                        known: false,
                        counts: Vec::new(),
                        key,
                    })
                    .collect(),
                unknown_range_counts(&req.range_facet_fields),
                req.stats_fields
                    .iter()
                    .map(|name| crate::pb::ColumnStats {
                        field: name.clone(),
                        known: false,
                        ..Default::default()
                    })
                    .collect(),
                req.cardinality_fields
                    .iter()
                    .map(|name| crate::pb::FacetDistinct {
                        field: name.clone(),
                        known: false,
                        values: Vec::new(),
                    })
                    .collect(),
            ),
        };
        // Which stage columns this shard's numeric table has —
        // computed regardless of k, like the facet known flags: a
        // shard lacking a column answers identity (exact), and the
        // coordinator refuses a column NO shard knows.
        let stage_columns_known: Vec<bool> = match guard.bm25.as_ref() {
            Some(store) => stage_specs
                .iter()
                .map(|(op, column, key)| {
                    // A geo stage resolves against the GEO table and
                    // nowhere else; asking the numeric tables about it
                    // would report a real column unknown and turn the
                    // coordinator's typo rule into a false refusal.
                    if matches!(op, crate::scorefn::StageOp::MultGeoDecay { .. }) {
                        return store.geo_index(column).is_some();
                    }
                    if key.is_empty() {
                        store.numeric_index(column).is_some()
                            || store.integer_index(column).is_some()
                    } else {
                        store
                            .map_numeric_index(column)
                            .and_then(|ci| store.map_numeric_key_ord(ci, key))
                            .is_some()
                    }
                })
                .collect(),
            None => vec![false; stage_specs.len()],
        };
        // Which geo columns this shard has, computed regardless of k
        // for the same reason: a shard without the column contributes
        // no hits through the filter (exact — its documents hold no
        // locations), but a column NO shard knows must refuse.
        let geo_columns_known = match guard.bm25.as_ref() {
            Some(store) => store.geo_columns_known(&req.geo_filters),
            None => vec![false; req.geo_filters.len()],
        };
        // And the filter tree's per-leaf flags, same contract.
        let filter_columns_known = match (guard.bm25.as_ref(), req.filter.as_ref()) {
            (Some(store), Some(f)) => store.filter_columns_known(f),
            (None, Some(f)) => vec![false; crate::filter::leaf_count(f)],
            (_, None) => Vec::new(),
        };
        // Projection column-read leaves, same contract again: a leaf
        // this shard lacks reads absent (exact), a leaf NO shard knows
        // is a typo the coordinator refuses (docs/cel-values.md).
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in &req.projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let projection_leaves_known: Vec<bool> = match guard.bm25.as_ref() {
            Some(store) => projection_leaves
                .iter()
                .map(|leaf| crate::values::leaf_known(leaf, store))
                .collect(),
            None => vec![false; projection_leaves.len()],
        };
        let hits = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 => {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // 0/absent means no supplied score floor (scores are positive).
                let floor = if req.min_score == 0.0 {
                    f64::NEG_INFINITY
                } else {
                    f64::from(req.min_score)
                };
                // The score-function chain, resolved against this
                // shard's numeric table (docs/score-functions.md).
                // With no stages the ctx is None and every scorer below
                // is bit-identical to its unchained form.
                let chain = store.resolve_chain(&stage_specs);
                // Projections resolve against this shard's tables once
                // per request; type conflicts refuse here, by name.
                let resolved_projections: Vec<crate::values::ResolvedValue> = req
                    .projections
                    .iter()
                    .map(|p| {
                        let expr = p.expr.as_ref().ok_or_else(|| {
                            Status::invalid_argument("projection: empty compiled expression")
                        })?;
                        crate::values::resolve(expr, store).map(|(rv, _)| rv)
                    })
                    .collect::<Result<_, Status>>()?;
                let numeric_read = ShardNumericRead(store);
                let chain_ctx: bm25::ChainCtx = if stage_specs.is_empty() {
                    None
                } else {
                    Some((&chain, &numeric_read))
                };
                // The filters resolved above, paired with this arm's
                // read surface (docs/geo-columns.md,
                // docs/cel-filters.md).
                let filter_ctx: bm25::FilterCtx = doc_filter
                    .as_ref()
                    .map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
                // Block-max path when every scored term has impacts (v5
                // shards) and the node flag allows it; the heap store,
                // v3/v4 files, and --block-max=false keep top_k with the
                // floor applied as a filter — same contract.
                let prunable = self.config.block_max
                    && req
                        .terms
                        .iter()
                        .enumerate()
                        // Local absence is not a missing impact surface:
                        // see top_k_pruned. Global df alone would forfeit
                        // pruning on every shard lacking a rare term.
                        .all(|(ti, t)| {
                            stats.dfs[ti] == 0 || index.df(t) == 0 || index.has_impacts(t)
                        });
                let docs = if prunable {
                    let mut prune = bm25::PruneStats::default();
                    bm25::top_k_pruned_chained_filtered_stats_streaming(
                        index,
                        &req.terms,
                        &stats,
                        params,
                        req.k as usize,
                        floor,
                        chain_ctx,
                        filter_ctx,
                        &mut prune,
                        live,
                        candidate,
                    )
                } else {
                    let docs = bm25::filter_to_floor(
                        bm25::top_k_chained_filtered(
                            index,
                            &req.terms,
                            &stats,
                            params,
                            req.k as usize,
                            chain_ctx,
                            filter_ctx,
                        ),
                        floor,
                    );
                    if let Some(sink) = candidate {
                        for doc in &docs {
                            sink(doc.doc_id, doc.score as f32);
                        }
                    }
                    docs
                };
                let highlight = highlight_plan(store, req.highlight.as_ref())?;
                let body = store.field_name(0);
                // The explain breakdown, only for the hits that survived
                // the top-k (docs/explain.md): the flat route is one
                // field at weight 1 in the fused scorer's terms.
                let flat_query = [bm25::FieldQuery {
                    index,
                    terms: &req.terms,
                    stats: stats.clone(),
                    params,
                    weight: 1.0,
                }];
                let presence = req.explain.then(|| {
                    let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
                    bm25::breakdown(&flat_query, &ids)
                });
                docs.into_iter()
                    .enumerate()
                    .map(|(hit_index, doc)| -> Result<Bm25Hit, Status> {
                        let explain = presence.as_ref().map(|presence| {
                            let mut explain =
                                explain_terms(&flat_query, &[body], &presence[hit_index], None);
                            explain_stages(
                                &mut explain,
                                &chain,
                                &req.score_stages,
                                doc.doc_id,
                                &numeric_read,
                            );
                            explain
                        });
                        let snippets = match highlight.as_ref() {
                            Some(plan) => {
                                let occurrences: Vec<(usize, (u32, u32))> = doc
                                    .term_offsets
                                    .iter()
                                    .flat_map(|(ti, offsets)| {
                                        offsets.iter().map(move |&span| (*ti, span))
                                    })
                                    .collect();
                                cut_snippets(plan, index, body, doc.doc_id, &occurrences)?
                            }
                            None => Vec::new(),
                        };
                        Ok(Bm25Hit {
                            explain,
                            snippets,
                            projected: resolved_projections
                                .iter()
                                .map(|rv| {
                                    projected_value(
                                        crate::values::eval(rv, doc.doc_id, &numeric_read),
                                        store,
                                    )
                                })
                                .collect(),
                            doc_id: self.config.slot_offset + u64::from(doc.doc_id),
                            score: doc.score as f32,
                            terms: doc
                                .term_offsets
                                .into_iter()
                                .map(|(ti, offsets)| TermOccurrences {
                                    term: req.terms[ti].clone(),
                                    offsets: offsets
                                        .into_iter()
                                        .map(|(start, end)| OffsetSpan { start, end })
                                        .collect(),
                                    field: String::new(),
                                })
                                .collect(),
                        })
                    })
                    .collect::<Result<Vec<_>, Status>>()?
            }
            _ => Vec::new(),
        };
        // The shard's k-th best: one f32 ULP below the last hit's score
        // when the heap filled (so a later f32 seed never exceeds the
        // true k-th best — ties at the floor survive), 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits.last()
                .map(|h| bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Bm25QueryResponse {
            projection_leaves_known,
            hits,
            kth_best,
            facets,
            stage_columns_known,
            stats: column_stats,
            distinct,
            range_facets,
            geo_columns_known,
            filter_columns_known,
        })
    }

    fn run_bm25_rescore(&self, req: Bm25RescoreRequest) -> Result<Bm25RescoreResponse, Status> {
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let stage_specs = parse_score_stages(&req.score_stages)?;
        let params = Bm25Params {
            k1: if req.k1 == 0.0 {
                bm25::DEFAULT_K1
            } else {
                f64::from(req.k1)
            },
            b: if req.b == 0.0 {
                bm25::DEFAULT_B
            } else {
                f64::from(req.b)
            },
        };
        let stats = bm25::CorpusStats {
            doc_count: req.global_doc_count,
            total_doc_length: req.global_total_doc_length,
            dfs: req.global_doc_frequencies.clone(),
        };
        let offset = self.config.slot_offset;
        let guard = self.state.read().expect("shard state lock poisoned");
        guard.check_stats_epoch(req.expected_stats_epoch)?;
        let (hits, stage_columns_known) = match guard.bm25.as_ref() {
            Some(store) => {
                // Route global ids to this shard's local range.
                let local: Vec<u32> = req
                    .candidate_ids
                    .iter()
                    .filter(|&&id| id >= offset && (id - offset) <= u64::from(u32::MAX))
                    .filter(|&&id| !guard.live_docs.is_deleted((id - offset) as usize))
                    .map(|id| (id - offset) as u32)
                    .collect();
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                let chain = store.resolve_chain(&stage_specs);
                let stage_columns_known = chain
                    .stages
                    .iter()
                    .map(|stage| stage.column.is_some())
                    .collect();
                let numeric_read = ShardNumericRead(store);
                let hits = bm25::score_candidates(index, &req.terms, &stats, params, &local)
                    .into_iter()
                    .map(|doc| Bm25Hit {
                        explain: None,
                        snippets: Vec::new(),
                        projected: Vec::new(),
                        doc_id: offset + u64::from(doc.doc_id),
                        score: chain.eval(doc.score, doc.doc_id, &numeric_read) as f32,
                        terms: doc
                            .term_offsets
                            .into_iter()
                            .map(|(ti, offsets)| TermOccurrences {
                                term: req.terms[ti].clone(),
                                offsets: offsets
                                    .into_iter()
                                    .map(|(start, end)| OffsetSpan { start, end })
                                    .collect(),
                                field: String::new(),
                            })
                            .collect(),
                    })
                    .collect::<Vec<_>>();
                (hits, stage_columns_known)
            }
            None => (Vec::new(), vec![false; stage_specs.len()]),
        };
        Ok(Bm25RescoreResponse {
            hits,
            stage_columns_known,
        })
    }
}

#[cfg(test)]
mod floor_lane_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn state_of(
        signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>,
        token: u64,
    ) -> Arc<StreamSignals> {
        Arc::clone(&signals.lock().unwrap()[&token])
    }

    fn floor_of(signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>, token: u64) -> f32 {
        f32::from_bits(state_of(signals, token).floor.load(Ordering::Acquire))
    }

    /// Typed UDP raises remain monotone, cancellation is sticky, and garbage
    /// cannot mutate either signal or panic the listener.
    #[test]
    fn stream_datagrams_fold_typed_signals_and_ignore_garbage() {
        let signals: std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>> =
            std::sync::Mutex::new(HashMap::new());
        signals
            .lock()
            .unwrap()
            .insert(7, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));

        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.25));
        assert_eq!(floor_of(&signals, 7), 0.25);
        // Lower and equal floors are ignored.
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.10));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.25));
        assert_eq!(floor_of(&signals, 7), 0.25);
        // Duplicated and reordered raises: max wins regardless.
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.75));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.50));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.75));
        assert_eq!(floor_of(&signals, 7), 0.75);

        let state = state_of(&signals, 7);
        assert!(!state.cancelled.load(Ordering::Acquire));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_cancel(7));
        assert!(state.cancelled.load(Ordering::Acquire));
        assert_eq!(
            floor_of(&signals, 7),
            0.75,
            "cancel is not a score sentinel"
        );

        // Unknown tokens, malformed frames, and empty datagrams are dropped.
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(8, 9.0));
        apply_stream_datagram(
            &signals,
            None,
            &crate::stream_signal::encode_floor(7, 9.0)[..crate::stream_signal::FRAME_LEN - 1],
        );
        apply_stream_datagram(&signals, None, &[0u8; crate::stream_signal::FRAME_LEN + 1]);
        apply_stream_datagram(&signals, None, &[]);
        assert_eq!(floor_of(&signals, 7), 0.75);
        assert!(state.cancelled.load(Ordering::Acquire));
        assert_eq!(signals.lock().unwrap().len(), 1);
    }

    /// With a key, only a signed datagram with a fresh sequence moves the
    /// floor (docs/security.md): a plain frame, a foreign key, a damaged
    /// tag, a replay, and a stale sequence all leave it where it was, so
    /// a forger cannot cut candidates. The gRPC twin still governs.
    #[test]
    fn signed_floor_lane_ignores_forgeries_and_replays() {
        use crate::security::UdpKey;
        use crate::stream_signal::{encode_cancel, encode_floor, sign};
        let key = UdpKey::from_bytes(&[1u8; 32]).unwrap();
        let other = UdpKey::from_bytes(&[2u8; 32]).unwrap();
        let signals: std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>> =
            std::sync::Mutex::new(HashMap::new());
        signals
            .lock()
            .unwrap()
            .insert(7, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));
        let state = state_of(&signals, 7);

        // An authentic raise applies.
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 0.25)));
        assert_eq!(floor_of(&signals, 7), 0.25);
        // A plain frame is not read when a key is configured.
        apply_stream_datagram(&signals, Some(&key), &encode_floor(7, 9.0));
        // A foreign key does not verify.
        apply_stream_datagram(
            &signals,
            Some(&key),
            &sign(&other, 2, &encode_floor(7, 9.0)),
        );
        // A damaged tag does not verify.
        let mut damaged = sign(&key, 2, &encode_floor(7, 9.0));
        let last = damaged.len() - 1;
        damaged[last] ^= 1;
        apply_stream_datagram(&signals, Some(&key), &damaged);
        // A replay of the authentic datagram, and a stale sequence with a
        // higher floor, are both behind the newest applied sequence.
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 0.25)));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 9.0)));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 0, &encode_floor(7, 9.0)));
        assert_eq!(floor_of(&signals, 7), 0.25, "no forgery moved the floor");
        assert!(!state.cancelled.load(Ordering::Acquire));
        // A forged cancel is ignored the same way; an authentic one lands.
        apply_stream_datagram(&signals, Some(&key), &encode_cancel(7));
        apply_stream_datagram(&signals, Some(&key), &sign(&other, 3, &encode_cancel(7)));
        assert!(!state.cancelled.load(Ordering::Acquire));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 2, &encode_floor(7, 0.5)));
        assert_eq!(floor_of(&signals, 7), 0.5);
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 3, &encode_cancel(7)));
        assert!(state.cancelled.load(Ordering::Acquire));
        // Sequences are per stream: a fresh token starts at zero.
        signals
            .lock()
            .unwrap()
            .insert(8, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(8, 0.1)));
        assert_eq!(floor_of(&signals, 8), 0.1);
    }
}
