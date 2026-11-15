//! Shard node configuration, layout selection, and column table validation.

use crate::node::*;

pub const MAX_RERANK_PARALLEL: usize = 64;

/// Ensure `config.placement_column` is registered in `integer_fields` so the
/// column table and catalog agree across all constructors.
pub(in crate::node) fn declare_placement_column(config: &mut NodeConfig) {
    if let Some(column) = config.placement_column.as_deref() {
        if !config.integer_fields.iter().any(|name| name == column) {
            config.integer_fields.push(column.to_string());
        }
    }
}

/// The index's SOURCE column tables: the configured lists without the
/// derived names, which is what a derived expression or a request's
/// own materialize spec may read (outputs are not inputs).
pub(in crate::node) fn source_tables(config: &NodeConfig) -> SourceTables {
    let without = |names: &[String]| -> Vec<String> {
        names
            .iter()
            .filter(|name| {
                config
                    .derived
                    .as_ref()
                    .is_none_or(|declaration| !declaration.is_derived(name))
            })
            .cloned()
            .collect()
    };
    SourceTables {
        numerics: without(&config.numeric_fields),
        integers: without(&config.integer_fields),
        unsigned_integers: without(&config.unsigned_integer_fields),
        map_numerics: config.map_numeric_fields.clone(),
        map_integers: config.map_integer_fields.clone(),
        map_unsigned_integers: config.map_unsigned_integer_fields.clone(),
        facets: config.facet_fields.clone(),
    }
}

/// Owned form of [`crate::values::NumericTypes`].
pub(in crate::node) struct SourceTables {
    pub(in crate::node) numerics: Vec<String>,
    pub(in crate::node) integers: Vec<String>,
    pub(in crate::node) unsigned_integers: Vec<String>,
    pub(in crate::node) map_numerics: Vec<String>,
    pub(in crate::node) map_integers: Vec<String>,
    pub(in crate::node) map_unsigned_integers: Vec<String>,
    pub(in crate::node) facets: Vec<String>,
}

impl SourceTables {
    pub(in crate::node) fn types(&self) -> crate::values::NumericTypes<'_> {
        crate::values::NumericTypes {
            numerics: &self.numerics,
            integers: &self.integers,
            unsigned_integers: &self.unsigned_integers,
            map_numerics: &self.map_numerics,
            map_integers: &self.map_integers,
            map_unsigned_integers: &self.map_unsigned_integers,
            facets: &self.facets,
        }
    }
}

/// Validate `config.derived` against source column tables (verifying source-only
/// inputs, matching target kinds, and non-colliding names) and append derived
/// columns in declaration order. Idempotent across constructors.
pub(in crate::node) fn declare_derived_columns(config: &mut NodeConfig) -> Result<(), String> {
    let Some(declaration) = config.derived.clone() else {
        return Ok(());
    };
    // A derived name already in a table is either this function's own
    // earlier append (the derived names of the kind, in order, at the
    // table's end) or a collision with a source column.
    for (kind, table) in [
        (crate::pb::MaterializeKind::F64, &config.numeric_fields),
        (crate::pb::MaterializeKind::I64, &config.integer_fields),
        (
            crate::pb::MaterializeKind::U64,
            &config.unsigned_integer_fields,
        ),
    ] {
        let names = declaration.names_of(kind);
        let appended = table.ends_with(&names);
        if !appended {
            if let Some(name) = names.iter().find(|name| table.contains(name)) {
                return Err(format!(
                    "derived columns: derived column {name:?} collides with a source column \
                     of the same name"
                ));
            }
        }
    }
    let sources = source_tables(config);
    declaration
        .check_tables(&sources.types())
        .map_err(|e| format!("derived columns: {e}"))?;
    for (kind, table) in [
        (crate::pb::MaterializeKind::F64, &mut config.numeric_fields),
        (crate::pb::MaterializeKind::I64, &mut config.integer_fields),
        (
            crate::pb::MaterializeKind::U64,
            &mut config.unsigned_integer_fields,
        ),
    ] {
        for name in declaration.names_of(kind) {
            if !table.contains(&name) {
                table.push(name);
            }
        }
    }
    Ok(())
}

/// A placement tree's predicates read columns this shard declares,
/// source or derived; a predicate naming any other column would send
/// rows by a value no row carries, so it is refused at startup.
pub(in crate::node) fn check_placement_columns(config: &NodeConfig) -> Result<(), String> {
    let Some(pinned) = config.placement_tree.as_ref() else {
        return Ok(());
    };
    for column in pinned.placement().predicate_columns() {
        let declared = config.facet_fields.contains(&column)
            || config.numeric_fields.contains(&column)
            || config.map_facet_fields.contains(&column)
            || config.map_numeric_fields.contains(&column)
            || config.integer_fields.contains(&column)
            || config.unsigned_integer_fields.contains(&column)
            || config.geo_fields.contains(&column);
        if !declared {
            return Err(format!(
                "placement tree: a predicate reads column {column:?}, which this shard \
                 declares in no table, source or derived"
            ));
        }
    }
    Ok(())
}

/// The store form of the configured declaration.
pub(crate) fn stored_derived(config: &NodeConfig) -> Option<crate::postings::StoredDerived> {
    config
        .derived
        .as_deref()
        .map(crate::postings::StoredDerived::of)
}

/// A store or catalog attached to this shard was written under this
/// shard's declaration, or under none while still empty; anything else
/// is an index compatibility event, refused by name.
pub(crate) fn check_attached_derived(
    config: &NodeConfig,
    held: Option<&crate::postings::StoredDerived>,
    rows: u64,
    what: &str,
) -> Result<(), String> {
    match (held, config.derived.as_deref()) {
        (None, None) => Ok(()),
        (Some(held), Some(wanted)) if held.fingerprint == wanted.fingerprint() => Ok(()),
        (Some(held), Some(wanted)) => Err(format!(
            "{what} was written under derived-column declaration {:?}, this node declares \
             {:?}; a changed declaration is a rebuild through the reshard tool",
            held.fingerprint,
            wanted.fingerprint()
        )),
        (Some(held), None) => Err(format!(
            "{what} was written under derived-column declaration {:?}, which this node does \
             not declare (--derived-columns)",
            held.fingerprint
        )),
        (None, Some(_)) if rows == 0 => Ok(()),
        (None, Some(wanted)) => Err(format!(
            "{what} holds {rows} rows written without derived columns, this node declares \
             {:?}; backfill the rows through the reshard tool (--derive)",
            wanted.fingerprint()
        )),
    }
}

// Rust and embedded callers bypass the CLI parser. Enforce the same column
// namespace before they create or open storage, and before a fresh ingest.
pub(in crate::node) fn validate_column_tables(config: &NodeConfig) -> Result<(), String> {
    let mut names = std::collections::HashSet::new();
    for name in config
        .facet_fields
        .iter()
        .chain(&config.numeric_fields)
        .chain(&config.map_facet_fields)
        .chain(&config.map_numeric_fields)
        .chain(&config.map_integer_fields)
        .chain(&config.map_unsigned_integer_fields)
        .chain(&config.integer_fields)
        .chain(&config.unsigned_integer_fields)
        .chain(&config.geo_fields)
    {
        if !names.insert(name) {
            return Err(format!(
                "column {name:?} is declared more than once; each name has one column kind"
            ));
        }
    }
    Ok(())
}

/// Build runtime knobs from `config`: live atomics plus startup fixed entries.
pub(in crate::node) fn node_knobs(config: &NodeConfig) -> crate::diagnostics::Knobs {
    use crate::diagnostics::{Knobs, NodeKnobValues};
    let process = format!("node:{}", config.slot_offset);
    let mut fixed = core_fixed_knobs(config);
    fixed.extend(shard_and_placement_fixed_knobs(config));
    Knobs::node(
        process,
        NodeKnobValues {
            share_floors: config.share_floors,
            segment_pruning: config.segment_pruning,
            floor_delta: config.floor_delta,
            floor_warmup_chunks: config.floor_warmup_chunks,
            floor_min_interval_ms: config.floor_min_interval_ms,
        },
        fixed,
    )
}

fn core_fixed_knobs(config: &NodeConfig) -> Vec<crate::diagnostics::FixedKnob> {
    use crate::diagnostics::FixedKnob;
    use crate::pb::KnobKind;
    vec![
        FixedKnob {
            name: "score_splade_max_k",
            kind: KnobKind::Int,
            value: config.max_k.to_string(),
            description: "Largest ScoreSplade k admitted by this node (--max-k).",
        },
        FixedKnob {
            name: "chunk_blocks",
            kind: KnobKind::Int,
            value: config.chunk_blocks.to_string(),
            description: "SIMD blocks per scan chunk (--chunk-blocks).",
        },
        FixedKnob {
            name: "block_max",
            kind: KnobKind::Bool,
            value: config.block_max.to_string(),
            description: "Block-max pruning on the lexical leg (--block-max).",
        },
        FixedKnob {
            name: "coalesce",
            kind: KnobKind::Bool,
            value: config.coalesce.to_string(),
            description: "Coalesce concurrent scans into one pass (--coalesce).",
        },
        FixedKnob {
            name: "scan_parallel",
            kind: KnobKind::Int,
            value: config.scan_parallel.to_string(),
            description: "Scan slots (--scan-parallel).",
        },
        FixedKnob {
            name: "rerank_parallel",
            kind: KnobKind::Int,
            value: config.rerank_parallel.to_string(),
            description: "Exact rerank slots (--rerank-parallel).",
        },
        FixedKnob {
            name: "layout",
            kind: KnobKind::String,
            value: match config.layout {
                Layout::Segments => "segments".to_string(),
                Layout::SingleImage => "single-image".to_string(),
            },
            description: "On-disk layout of a new persisted shard (--layout).",
        },
        FixedKnob {
            name: "vector_mmap",
            kind: KnobKind::Bool,
            value: config.vector_mmap.to_string(),
            description: "Serve sealed vector images through a memory map (--vector-mmap).",
        },
        FixedKnob {
            name: "seal_tail_docs",
            kind: KnobKind::Int,
            value: config.seal_tail_docs.to_string(),
            description: "Rows that seal the tail into a segment (--seal-tail-docs).",
        },
        FixedKnob {
            name: "bit_width",
            kind: KnobKind::Int,
            value: config.bit_width.to_string(),
            description: "Quantized bits per dimension (--bit-width).",
        },
    ]
}

fn shard_and_placement_fixed_knobs(config: &NodeConfig) -> Vec<crate::diagnostics::FixedKnob> {
    use crate::diagnostics::FixedKnob;
    use crate::pb::KnobKind;
    vec![
        FixedKnob {
            name: "slot_offset",
            kind: KnobKind::Int,
            value: config.slot_offset.to_string(),
            description: "First global id of this shard.",
        },
        FixedKnob {
            name: "collection",
            kind: KnobKind::String,
            value: config.collection.clone(),
            description: "The collection this shard belongs to.",
        },
        FixedKnob {
            name: "vector_backend",
            kind: KnobKind::String,
            value: config.vector_backend.clone(),
            description: "The vector backend the shard was built with.",
        },
        FixedKnob {
            name: "placement_column",
            kind: KnobKind::String,
            value: config.placement_column.clone().unwrap_or_default(),
            description: "The i64 column that holds each row's placement code \
                          (--placement-column).",
        },
        FixedKnob {
            name: "placement_leaf",
            kind: KnobKind::String,
            value: config
                .placement_leaf
                .map(|code| code.to_string())
                .unwrap_or_default(),
            description: "The placement code this shard serves (--placement-leaf).",
        },
        FixedKnob {
            name: "placement_tree",
            kind: KnobKind::String,
            value: config
                .placement_tree
                .as_ref()
                .map(|pinned| pinned.leaf().name.clone())
                .unwrap_or_default(),
            description: "The pinned leaf's name under the tree this shard checks direct \
                          rows against (--placement-tree); empty without a tree.",
        },
        FixedKnob {
            name: "derived_columns",
            kind: KnobKind::String,
            value: config
                .derived
                .as_ref()
                .map(|declaration| {
                    crate::derived::declaration_toml(declaration.spec()).unwrap_or_default()
                })
                .unwrap_or_default(),
            description: "The index's derived-column declaration as a [[derived]] table \
                          (--derived-columns); empty without one.",
        },
        FixedKnob {
            name: "derived_fingerprint",
            kind: KnobKind::String,
            value: config
                .derived
                .as_ref()
                .map(|declaration| declaration.fingerprint().to_string())
                .unwrap_or_default(),
            description: "The declaration's fingerprint, which the store, the log and every \
                          segment of this shard carry.",
        },
    ]
}

pub(in crate::node) fn resolved_rerank_parallel(configured: usize) -> usize {
    if configured == 0 {
        std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(4)
    } else {
        configured.min(MAX_RERANK_PARALLEL)
    }
}

/// Persisted on-disk shard layout for documents and vectors.
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
    /// On-disk layout for newly created persisted shards; existing shards keep
    /// the layout of their persisted files.
    pub layout: Layout,
    /// Memory-map sealed segment vector images when `true`, or load into heap
    /// when `false` (tail and single-image shards remain heap-owned).
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
            turbo_handshake: None,
            matryoshka_prefix_dims: Vec::new(),
            matryoshka_active_dim: None,
            vocab_kserve_endpoint: None,
        }
    }
}
