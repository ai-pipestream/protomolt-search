//! Shard node configuration and column table validation.

use crate::node::*;

mod columns;
mod knobs;

pub(crate) use self::columns::{check_attached_derived, stored_derived};
pub(in crate::node) use self::columns::{
    check_placement_columns, declare_derived_columns, declare_placement_column, source_tables,
    validate_column_tables,
};
pub(in crate::node) use self::knobs::node_knobs;

pub const MAX_RERANK_PARALLEL: usize = 64;

/// The layout diagnostics report for a shard with no index path: its
/// heap store never reaches disk.
pub const LAYOUT_MEMORY: &str = "memory";

pub(in crate::node) fn resolved_rerank_parallel(configured: usize) -> usize {
    if configured == 0 {
        std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(4)
    } else {
        configured.min(MAX_RERANK_PARALLEL)
    }
}

/// How a node scans and whether it participates in floor sharing.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Admission cap for `ScoreSplade.k` (--max-k). Other node RPCs keep
    /// their existing limits; coordinator max_k remains a live knob.
    pub max_k: u32,
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
    /// Skip sealed segments a request's filter cannot match using per-segment
    /// column summaries (`false` disables pruning without changing results).
    pub segment_pruning: bool,
    /// Minimum improvement over the last PUBLISHED floor before the next
    /// one goes on the wire. 0.0 publishes every raise (the historical
    /// behavior); a small positive delta trades a sliver of pruning
    /// reactivity for far fewer floor messages on real networks.
    pub floor_delta: f32,
    /// Chunk-level floor publish opportunities to skip once the local heap fills
    /// before broadcasting the first floor (`0` publishes immediately).
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
    /// BM25 field table for new builders (`"body"` first, followed by extra
    /// indexed fields); loaded shards retain their persisted table.
    pub bm25_fields: Vec<String>,
    /// Dictionary-encoded facet column table for new builders; loaded shards
    /// retain their persisted table and unknown fields are refused at ingest.
    pub facet_fields: Vec<String>,
    /// `f64` numeric column table for new builders.
    pub numeric_fields: Vec<String>,
    /// `map<string, string>` column table for new builders.
    pub map_facet_fields: Vec<String>,
    /// The map<string, f64> column table for NEW builders. Same rules.
    pub map_numeric_fields: Vec<String>,
    pub map_integer_fields: Vec<String>,
    pub map_unsigned_integer_fields: Vec<String>,
    /// `i64` column table for new builders (timestamps also land here as epoch micros).
    pub integer_fields: Vec<String>,
    /// Full-domain unsigned numeric columns, distinct from signed columns.
    pub unsigned_integer_fields: Vec<String>,
    /// `i64` column holding each row's placement code; joins `integer_fields`
    /// and is populated from the document or `placement_leaf`.
    pub placement_column: Option<String>,
    /// The placement code this shard serves. Fills the placement column
    /// on a document that lacks it and rejects one that carries another
    /// code. Needs `placement_column`.
    pub placement_leaf: Option<i64>,
    /// Placement tree for `placement_leaf` (`--placement-tree`); verifies
    /// direct-ingest rows against leaf path predicates.
    pub placement_tree: Option<Arc<crate::placement::PinnedLeaf>>,
    /// Derived-column declaration (`--derived-columns`): CEL value columns
    /// evaluated at ingest, appended after source columns of matching kinds, and
    /// fingerprint-verified across store, WAL, and segments.
    pub derived: Option<Arc<crate::derived::Declaration>>,
    /// Geo-point column table for new builders, read by geo filters and decay stages.
    pub geo_fields: Vec<String>,
    /// BM25 fields (subset of `bm25_fields`) that store per-occurrence token
    /// positions; loaded shards enforce their persisted positional declaration.
    pub position_fields: Vec<String>,
    /// Fields storing per-document sentence spans for server-side snippets
    /// (`["body"]` or empty).
    pub sentence_fields: Vec<String>,
    /// Collection served by this node (empty when unnamed), checked on health,
    /// mapped binds, ingested documents, and WAL open.
    pub collection: String,
    /// HMAC key authenticating UDP floor and cancel datagrams; when `None`,
    /// unsigned datagrams are accepted on loopback listeners only.
    pub udp_hmac_key: Option<crate::security::UdpKey>,
    /// Memory-map sealed segment vector images when `true`, or load into heap
    /// when `false` (the tail remains heap-owned).
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
    /// `MAX_COALESCE` queries share each pass over the packed codes —
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
    /// Optional index-generation cryptographic handshake verified at shard
    /// startup/bind and during ingest.
    pub turbo_handshake: Option<turbo_client::IndexGenerationHandshake>,
    /// Optional Matryoshka prefix tiers (e.g. `[128, 256]`) sliced from full
    /// embeddings on ingest via [`crate::turbo_bridge::MultiResVectorOutput`].
    pub matryoshka_prefix_dims: Vec<u32>,
    /// Optional active Matryoshka prefix tier indexed into the primary shard
    /// vector index while full/higher tiers remain retained on the shard.
    pub matryoshka_active_dim: Option<u32>,
    /// Optional `turbo-kserve` gRPC endpoint receiving live corpus vocabulary
    /// feedback snapshots on `Flush`.
    pub vocab_kserve_endpoint: Option<String>,
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
            max_k: crate::coordinator::DEFAULT_MAX_K,
            vector_backend: EMBEDDED_TURBOVEC.to_string(),
            slot_offset: 0,
            chunk_blocks: DEFAULT_CHUNK_BLOCKS,
            share_floors: true,
            block_max: true,
            segment_pruning: true,
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
            map_integer_fields: Vec::new(),
            map_unsigned_integer_fields: Vec::new(),
            integer_fields: Vec::new(),
            unsigned_integer_fields: Vec::new(),
            placement_column: None,
            placement_leaf: None,
            placement_tree: None,
            derived: None,
            geo_fields: Vec::new(),
            position_fields: Vec::new(),
            sentence_fields: Vec::new(),
            collection: String::new(),
            udp_hmac_key: None,
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
            turbo_handshake: None,
            matryoshka_prefix_dims: Vec::new(),
            matryoshka_active_dim: None,
            vocab_kserve_endpoint: None,
        }
    }
}
