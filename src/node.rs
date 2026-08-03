//! Shard-owner side: serves [`NodeService`] over one turbovec index.
//!
//! The shard is a small state machine behind a write lock:
//!
//! ```text
//! empty (no index) ──SetCalibration──▶ seeded empty index ──AddVectors──▶ live index
//!       │                                    │
//!       └──AddVectors(dim=..)──▶ unseeded index (calibration fitted from first batch)
//! ```
//!
//! Calibration locks for the index's lifetime (turbovec's own rule):
//! `SetCalibration` is only ever accepted on an empty shard. Adds hold the
//! write lock on the blocking pool; searches hold the read lock for the
//! duration of their chunked scan, so a search never observes a
//! half-applied batch.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use turbovec::TurboQuantIndex;

use crate::bm25::{self, Bm25Params};
use crate::chunked::{
    chunked_topk, chunked_topk_batch, chunked_topk_collapsed, BatchQuery, ChunkHit, ScanStats,
    DEFAULT_CHUNK_BLOCKS,
};
use crate::fusion::{self, Leg};
use crate::pb::node_service_server::{NodeService, NodeServiceServer};
use crate::pb::wal::{
    wal_record, FlushMarker, LoggedAddDocuments, LoggedAddVectors, SnapshotMarker,
};
use crate::pb::{
    search_shard_request, search_shard_response, snapshot_chunk, stream_search_request,
    stream_search_response, AddDocumentsRequest, AddDocumentsResponse, AddVectorsRequest,
    AddVectorsResponse, Bm25Hit, Bm25QueryRequest, Bm25QueryResponse, Bm25RescoreRequest,
    Bm25RescoreResponse, FloorUpdate, FlushRequest, FlushResponse, GetCalibrationRequest,
    GetCalibrationResponse, GetDocumentsRequest, GetDocumentsResponse, HealthRequest,
    HealthResponse, HybridLegHit, HybridShardRequest, HybridShardResponse, InstallSnapshotResponse,
    OffsetSpan, RawLegHit, ScoredHit, SearchShardDone, SearchShardRequest, SearchShardResponse,
    SetCalibrationRequest, SetCalibrationResponse, ShardLegsRequest, ShardLegsResponse,
    ShardScanStats, SnapshotChunk, SnapshotManifest, StartShardSearch, StoredDocument,
    StreamSearchBatch, StreamSearchRequest, StreamSearchResponse, StreamSearchSummary,
    TermOccurrences, TermStatsRequest, TermStatsResponse, VectorRescoreRequest,
    VectorRescoreResponse,
};
use crate::postings::{Bm25Index, Bm25Reader, Bm25Store, SpillBuilder};
use crate::wal::{self, WalWriter};

/// How a node scans and whether it participates in floor sharing.
#[derive(Debug, Clone)]
pub struct NodeConfig {
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
    /// Bit width used when `AddVectors` constructs an index from scratch
    /// (no loaded index, no seeded calibration).
    pub bit_width: usize,
    /// Persistence target for `Flush` / save-on-shutdown. `None` makes the
    /// shard purely in-memory (flush is a no-op).
    pub index_path: Option<PathBuf>,
    /// Analysis sidecar address (`http://host:port`) for AddDocuments.
    /// `None` makes AddDocuments fail UNAVAILABLE.
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
    /// Keep a write-ahead log at `<index path>.wal/` (see [`crate::wal`]).
    /// Requires `index_path`; the config layer defaults this on for
    /// persisted shards and off for demo shards.
    pub wal: bool,
    /// Number of WAL hash buckets (`bucket-NNN.wal` files per
    /// generation). Fixed at WAL creation; a resumed log keeps its own.
    pub wal_buckets: u32,
    /// Coalesce concurrent shard scans into batched kernel calls (up to
    /// [`MAX_COALESCE`] queries share each pass over the packed codes —
    /// the scan is bandwidth-bound, so batched queries ride the same
    /// memory traffic). `false` runs one scan per RPC — the A/B
    /// baseline; results are identical either way.
    pub coalesce: bool,
    /// Concurrent batched scans (blocking threads). 0 sizes from the
    /// machine: half the available cores, at least one.
    pub scan_parallel: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
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
            wal: false,
            wal_buckets: 64,
            coalesce: true,
            scan_parallel: 0,
        }
    }
}

/// Raw leg hits as `(global_doc_id, raw_score)`, score-descending.
type RawLeg = Vec<(u64, f64)>;

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
}

impl Bm25Shard {
    /// The searchable read surface; `None` while bulk-building (a spill
    /// builder cannot answer term lookups without scanning every run).
    fn as_index(&self) -> Option<&dyn Bm25Index> {
        match self {
            Bm25Shard::Building(s) => Some(s),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(r),
        }
    }

    fn next_doc_id(&self) -> u32 {
        match self {
            Bm25Shard::Building(s) => s.next_doc_id(),
            Bm25Shard::Spilling(s) => s.next_doc_id(),
            Bm25Shard::Resident(r) => r.next_doc_id(),
        }
    }

    fn doc_count(&self) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.doc_count(),
            Bm25Shard::Spilling(s) => s.doc_count(),
            Bm25Shard::Resident(r) => Bm25Index::doc_count(r),
        }
    }

    /// Fields in the active table (`docs/multi-field.md`).
    fn field_count(&self) -> usize {
        match self {
            Bm25Shard::Building(s) => s.field_count(),
            Bm25Shard::Spilling(s) => s.field_count(),
            Bm25Shard::Resident(r) => r.field_count(),
        }
    }

    /// The name of field `f` in the active table.
    fn field_name(&self, f: usize) -> &str {
        match self {
            Bm25Shard::Building(s) => s.field_name(f),
            Bm25Shard::Spilling(s) => s.field_name(f),
            Bm25Shard::Resident(r) => r.field_name(f),
        }
    }

    /// Field `f`'s analyzer fingerprint in the active table (0 =
    /// unknown, which never enforces).
    fn analysis_fingerprint(&self, f: usize) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.analysis_fingerprint(f),
            Bm25Shard::Spilling(s) => s.analysis_fingerprint(f),
            Bm25Shard::Resident(r) => r.analysis_fingerprint(f),
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
        }
    }

    /// The table index of the field named `name`, if present. `None`
    /// while bulk-building (no searchable surface to resolve against).
    fn field_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.field_index(name),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => r.field_index(name),
        }
    }

    /// The facet-table index of the facet field named `name`, if the
    /// active table has it.
    fn facet_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.facet_index(name),
            Bm25Shard::Spilling(s) => s.facet_index(name),
            Bm25Shard::Resident(r) => r.facet_index(name),
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
        }
    }

    /// The value of facet field `fi` at ordinal `ord`.
    fn facet_value(&self, fi: usize, ord: u32) -> &str {
        match self {
            Bm25Shard::Building(s) => s.facet_value(fi, ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_value(fi, ord),
        }
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`.
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.facet_ord(fi, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.facet_ord(fi, doc_id),
        }
    }

    /// The numeric-table index of the numeric field named `name`, if
    /// the active table has it.
    fn numeric_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.numeric_index(name),
            Bm25Shard::Spilling(s) => s.numeric_index(name),
            Bm25Shard::Resident(r) => r.numeric_index(name),
        }
    }

    /// (min, max) of numeric field `ni` over present values. Scoring
    /// only runs against searchable shapes, so Spilling is unreachable.
    fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        match self {
            Bm25Shard::Building(s) => s.numeric_min_max(ni),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.numeric_min_max(ni),
        }
    }

    /// `doc_id`'s value for numeric field `ni`.
    fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        match self {
            Bm25Shard::Building(s) => s.numeric_value(ni, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.numeric_value(ni, doc_id),
        }
    }

    /// The index of the map-facet column named `name`.
    fn map_facet_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_index(name),
            Bm25Shard::Spilling(s) => s.map_facet_index(name),
            Bm25Shard::Resident(r) => r.map_facet_index(name),
        }
    }

    /// The key ordinal of `key` in map-facet column `ci`.
    fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_key_ord(ci, key),
        }
    }

    /// Number of distinct values map-facet column `ci` holds.
    fn map_facet_value_count(&self, ci: usize) -> usize {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_count(ci),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_count(ci),
        }
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value(ci, ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value(ci, ord),
        }
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`.
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_facet_value_ord(ci, key_ord, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_facet_value_ord(ci, key_ord, doc_id),
        }
    }

    /// The index of the map-numeric column named `name`.
    fn map_numeric_index(&self, name: &str) -> Option<usize> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_index(name),
            Bm25Shard::Spilling(s) => s.map_numeric_index(name),
            Bm25Shard::Resident(r) => r.map_numeric_index(name),
        }
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_key_ord(ci, key),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_key_ord(ci, key),
        }
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`.
    fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_key_min_max(ci, key_ord),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_key_min_max(ci, key_ord),
        }
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`.
    fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        match self {
            Bm25Shard::Building(s) => s.map_numeric_value(ci, key_ord, doc_id),
            Bm25Shard::Spilling(_) => unreachable!("spilling shards are not searchable"),
            Bm25Shard::Resident(r) => r.map_numeric_value(ci, key_ord, doc_id),
        }
    }

    /// Field `f` as its own searchable [`Bm25Index`]; `None` while
    /// bulk-building, exactly like [`Self::as_index`].
    fn field_view(&self, f: usize) -> Option<Box<dyn Bm25Index + '_>> {
        match self {
            Bm25Shard::Building(s) => Some(Box::new(s.field(f))),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(Box::new(r.field(f))),
        }
    }

    /// Count-then-rank facet counting
    /// (`docs/plans/track-1-features.md` section 2): count the
    /// requested facet fields over this shard's FULL match set — every
    /// document holding at least one scored term in any queried field
    /// — independent of `k`, `min_score`, and block-max pruning, which
    /// bound what is SURFACED, never what matched. Walks each term's
    /// doc run to exhaustion (fixed-stride on a v5-shaped reader,
    /// never occurrence bytes), dedups documents in a slot bitmap, and
    /// resolves one facet ordinal per matched document. A facet field
    /// the shard's table lacks answers `known: false` and no counts —
    /// the coordinator turns all-unknown into a refusal.
    fn count_facets(
        &self,
        views: &[(&dyn Bm25Index, &[String])],
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
    ) -> Vec<crate::pb::FacetFieldCounts> {
        let n_slots = self.next_doc_id() as usize;
        let mut bits = vec![0u64; n_slots.div_ceil(64)];
        for &(view, terms) in views {
            for term in terms {
                view.for_each_doc_tf(term, &mut |doc_id, _tf| {
                    bits[doc_id as usize / 64] |= 1u64 << (doc_id % 64);
                });
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
        out
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
                    let (column, min_max) = if key.is_empty() {
                        let ni = self.numeric_index(column);
                        (
                            ni.map(ColumnRef::Numeric),
                            ni.map(|ni| self.numeric_min_max(ni)),
                        )
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
    /// format (v3 through v6) maps disk-resident; only the pre-v3
    /// formats load into the heap builder (and are upgraded to the
    /// current format on the next flush). v5/v6 were missing from this
    /// list, so a restarted node heap-loaded its whole postings file —
    /// at real shard sizes that is the exact failure the resident
    /// reader exists to prevent.
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let mut magic = [0u8; 8];
        std::fs::File::open(path)?.read_exact(&mut magic)?;
        if matches!(
            &magic,
            b"TVBM2503" | b"TVBM2504" | b"TVBM2505" | b"TVBM2506" | b"TVBM2507"
        ) {
            Ok(Bm25Shard::Resident(Bm25Reader::open(path)?))
        } else {
            Ok(Bm25Shard::Building(Bm25Store::load(path)?))
        }
    }
}

/// [`crate::scorefn::NumericRead`] over a searchable shard shape, for
/// score-chain evaluation during scoring.
struct ShardNumericRead<'a>(&'a Bm25Shard);

impl crate::scorefn::NumericRead for ShardNumericRead<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key_ord, doc_id)
    }
}

/// Parse and validate a wire score-stage list into resolved ops plus
/// their column names — the shard-independent half of chain building
/// (`docs/score-functions.md`). Refuses unknown ops, empty column
/// names, and parameters outside each op's admission rule: every
/// refusal here is a stage whose monotonicity or bound would not hold.
fn parse_score_stages(
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
struct ShardState {
    index: Option<TurboQuantIndex>,
    bm25: Option<Bm25Shard>,
    /// The active snapshot generation directory, when the shard's files
    /// came from (or were replaced by) an `InstallSnapshot` image.
    /// `Flush` and the AddDocuments reload path read/write THERE, never
    /// the legacy `<index path>` layout, so the two never split-brain.
    generation: Option<PathBuf>,
    /// The write-ahead log (`<index path>.wal/`), behind the same lock as
    /// the index it precedes. `None` when the shard runs without one.
    wal: Option<WalWriter>,
    /// Cached slot -> parent map for collapse scans (lineage opinion_id
    /// per slot). Self-validating: rebuilt whenever its length disagrees
    /// with the index, cleared on snapshot install.
    parents: Option<std::sync::Arc<Vec<u64>>>,
    /// Advances on every mutation of `bm25` (ingest, flush, snapshot
    /// install, startup attach). `TermStats` reports it and the scoring
    /// RPCs enforce a caller's claim against it, which is what lets a
    /// coordinator cache term stats without ever scoring against a
    /// store the stats no longer describe. Starts at 1: 0 is the wire's
    /// "no claim". Over-bumping is safe (a cache refetches); a missed
    /// bump is the only unsound direction.
    stats_epoch: u64,
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
pub fn bm25_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".bm25");
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

/// Snapshot generation layout, next to the shard's configured index path:
/// `<index path>.snap/` is the active generation holding the installed
/// image as `index.tv` + `index.tv.bm25`. Because BOTH files live inside
/// one directory, installing them is a single directory rename — which is
/// atomic, so the pair can never tear.
pub fn generation_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap");
    PathBuf::from(p)
}

/// The image paths inside a generation directory.
pub fn generation_tv(dir: &Path) -> PathBuf {
    dir.join("index.tv")
}
/// The BM25 sidecar path inside a generation directory.
pub fn generation_bm25(dir: &Path) -> PathBuf {
    dir.join("index.tv.bm25")
}

/// Receive staging (`<index path>.snap-tmp/`) and swap-out
/// (`<index path>.snap-old/`) directories for the generation swap.
fn generation_tmp_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-tmp");
    PathBuf::from(p)
}
fn generation_old_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-old");
    PathBuf::from(p)
}

/// Where the shard's files live: the active snapshot generation when one
/// was installed, else the legacy `<index path>` (+`.bm25`) layout.
/// Returns `(index, bm25)` paths.
fn storage_paths(index_path: &Path, generation: Option<&PathBuf>) -> (PathBuf, PathBuf) {
    match generation {
        Some(dir) => (generation_tv(dir), generation_bm25(dir)),
        None => (index_path.to_path_buf(), bm25_sidecar_path(index_path)),
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
    generation_tv(&snap).exists().then_some(snap)
}

/// The manifest describing a shard's current shape: calibration and dim
/// from the loaded index when it has them (a seeded or fitted shard),
/// zeros otherwise — an empty shard completes the manifest lazily, via
/// `SetCalibration` or its first batch, until calibration locks.
///
/// `preexisting` is the (vectors, documents) the shard already holds
/// that this generation's log will NOT contain — the installed image on
/// a snapshot rotation, or the whole shard when logging is enabled on an
/// already-populated index. Nonzero preexisting state marks the log as
/// partial history, which the reshard tool refuses (a log-only replay
/// would silently drop that state).
fn wal_manifest(
    index: Option<&TurboQuantIndex>,
    config: &NodeConfig,
    generation: u64,
    preexisting: (u64, u64),
) -> wal::WalManifest {
    let (dim, bit_width, shift, scale) = match index {
        Some(index) => {
            let (shift, scale) = index.calibration().unwrap_or((&[], &[]));
            (
                index.dim_opt().unwrap_or(0) as u32,
                index.bit_width() as u32,
                shift.to_vec(),
                scale.to_vec(),
            )
        }
        None => (0, config.bit_width as u32, Vec::new(), Vec::new()),
    };
    wal::WalManifest {
        dim,
        bit_width,
        calibration_shift: shift,
        calibration_scale: scale,
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
    let (_, bm25_path) = storage_paths(index_path, generation.as_ref());
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
fn open_wal(index: Option<&TurboQuantIndex>, config: &NodeConfig) -> Option<WalWriter> {
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
    let result = match wal::latest_gen(&dir) {
        Ok(Some((_, gen))) => wal::read_manifest(&gen).and_then(|m| {
            let cutoff = config.slot_offset + vector_tip.max(doc_tip);
            let dropped = wal::truncate_records_at_or_above(&gen, cutoff)?;
            if dropped > 0 {
                eprintln!(
                    "wal: truncated {dropped} record(s) at or above applied tip {cutoff} in {} \
                     (buffered appends that outlived a crash; never durable-acked)",
                    gen.display()
                );
            }
            WalWriter::resume(&gen, m)
        }),
        Ok(None) => {
            if vector_tip > 0 || doc_tip > 0 {
                eprintln!(
                    "wal: shard already holds {vector_tip} vectors / {doc_tip} documents; the new \
                     log records them as preexisting — this shard can serve but cannot be \
                     resharded from this log (rebuild via InstallSnapshot for full history)"
                );
            }
            WalWriter::create(&dir, wal_manifest(index, config, 0, (vector_tip, doc_tip)))
        }
        Err(e) => Err(e),
    };
    let mut writer = result.unwrap_or_else(|e| panic!("open WAL at {}: {e}", dir.display()));
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
    });
    eprintln!("wal: logging to {}", writer.dir().display());
    Some(writer)
}

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
fn wal_append_or_degrade(wal_slot: &mut Option<WalWriter>, op: wal_record::Op) {
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
#[derive(Clone)]
pub struct NodeServiceImpl {
    /// Locked shard state; see [`ShardState`].
    state: Arc<RwLock<ShardState>>,
    /// Single-writer gate for ingest streams. Two concurrent AddDocuments
    /// (or AddVectors) streams would interleave positional ids into one
    /// shard — every doc logged, none attributable — so the second stream
    /// is refused outright rather than merged.
    ingest_busy: Arc<std::sync::atomic::AtomicBool>,
    config: NodeConfig,
    /// Shared scan queue for coalesced searches; the scheduler task is
    /// spawned on first use (shared across service clones).
    scan_jobs: Arc<std::sync::OnceLock<mpsc::Sender<ScanJob>>>,
    /// UDP floor lane registry: stream token -> that stream's floor
    /// cell (f32 bits, monotone max). Fed by [`Self::spawn_floor_listener`],
    /// read by the streaming scan between blocks.
    floor_cells: Arc<std::sync::Mutex<HashMap<u64, Arc<std::sync::atomic::AtomicU32>>>>,
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

/// Fold one UDP floor datagram into its stream's cell. 12 bytes,
/// little-endian: u64 stream token, f32 floor. Anything else — short or
/// long datagrams, unknown tokens, NaN or non-raising floors — is
/// dropped: this lane is an unreliable fast copy of a monotone hint,
/// and the gRPC stream remains the reliable one.
fn apply_floor_datagram(
    cells: &std::sync::Mutex<HashMap<u64, Arc<std::sync::atomic::AtomicU32>>>,
    datagram: &[u8],
) {
    if datagram.len() != 12 {
        return;
    }
    let token = u64::from_le_bytes(datagram[..8].try_into().expect("8 bytes"));
    let floor = f32::from_le_bytes(datagram[8..12].try_into().expect("4 bytes"));
    let cell = cells
        .lock()
        .expect("floor registry poisoned")
        .get(&token)
        .cloned();
    if let Some(cell) = cell {
        raise_floor_cell(&cell, floor);
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
struct ScanJob {
    vector: Vec<f32>,
    k: usize,
    tie_complete: bool,
    /// Polled between chunks for the best coordinator-pushed floor
    /// (returns `None` when floor sharing is off or no floor arrived).
    external: Box<dyn FnMut() -> Option<f32> + Send>,
    /// Receives this query's k-th-best raises (the caller bakes in the
    /// share gate and delta filter).
    publish: Box<dyn FnMut(f32) -> bool + Send>,
    done: tokio::sync::oneshot::Sender<Result<(Vec<ChunkHit>, ScanStats), Status>>,
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
    let mut specs: Vec<(Vec<f32>, usize, bool)> = Vec::with_capacity(batch.len());
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
        .map(|(vector, k, keep_ties)| BatchQuery {
            vector,
            k: *k,
            keep_ties: *keep_ties,
        })
        .collect();
    let results = chunked_topk_batch(
        index,
        &queries,
        chunk_blocks,
        &mut |qi| (externals[qi])(),
        &mut |qi, floor| (publishers[qi])(floor),
    );
    for (done, result) in dones.into_iter().zip(results) {
        let _ = done.send(Ok(result));
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
    /// Wrap an optional preloaded index in a node service.
    pub fn new(index: Option<TurboQuantIndex>, config: NodeConfig) -> Self {
        let wal = open_wal(index.as_ref(), &config);
        Self {
            state: Arc::new(RwLock::new(ShardState {
                index,
                bm25: None,
                generation: None,
                wal,
                parents: None,
                stats_epoch: 1,
            })),
            ingest_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config,
            scan_jobs: Arc::new(std::sync::OnceLock::new()),
            floor_cells: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Bind the UDP floor lane on `addr` — the same host:port as the
    /// gRPC listener, UDP namespace — and fold incoming datagrams into
    /// the matching stream's floor cell (see [`apply_floor_datagram`]).
    /// A failed bind only loses the fast lane: floors still travel on
    /// every stream's reliable gRPC leg.
    pub fn spawn_floor_listener(&self, addr: std::net::SocketAddr) {
        let cells = Arc::clone(&self.floor_cells);
        tokio::spawn(async move {
            let socket = match tokio::net::UdpSocket::bind(addr).await {
                Ok(socket) => socket,
                Err(e) => {
                    eprintln!("floor UDP bind {addr}: {e}; floors ride the gRPC streams only");
                    return;
                }
            };
            let mut buf = [0u8; 64];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, _peer)) => apply_floor_datagram(&cells, &buf[..n]),
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
        let facets: Vec<&str> = self.config.facet_fields.iter().map(String::as_str).collect();
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
        match self.config.index_path.as_ref() {
            Some(p) => {
                let dir = bm25_build_dir(&storage_paths(p, generation).1);
                SpillBuilder::create_with_fields(&dir, &names)
                    .map(|b| {
                        Bm25Shard::Spilling(
                            b.with_facet_fields(&facets)
                                .with_numeric_fields(&numerics)
                                .with_map_facet_fields(&map_facets)
                                .with_map_numeric_fields(&map_numerics),
                        )
                    })
                    .map_err(|e| Status::internal(format!("spill dir {}: {e}", dir.display())))
            }
            None => Ok(Bm25Shard::Building(
                Bm25Store::with_fields(&names)
                    .with_facets(&facets)
                    .with_numerics(&numerics)
                    .with_map_facets(&map_facets)
                    .with_map_numerics(&map_numerics),
            )),
        }
    }

    /// Claim the single-writer ingest gate, or refuse the stream.
    fn claim_ingest(&self) -> Result<IngestGuard, Status> {
        use std::sync::atomic::Ordering;
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

    /// Attach a preloaded BM25 shard (from `<index path>.bm25`).
    pub fn with_bm25(self, store: Option<Bm25Shard>) -> Self {
        {
            let mut guard = self.state.write().expect("shard state lock poisoned");
            guard.bm25 = store;
            guard.stats_epoch += 1;
        }
        self
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
    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Validate an incoming `StartShardSearch` against the index shape.
    /// turbovec panics on wrong-dim or non-finite queries; the service
    /// turns both into `INVALID_ARGUMENT` before the scan starts.
    /// The slot -> parent map for collapse scans: lineage `opinion_id`
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
                    .map(|l| l.opinion_id)
                    .unwrap_or(SELF_PARENT_TAG | (slot_offset + slot as u64));
                parents.push(parent);
            }
            Arc::new(parents)
        };
        state.write().expect("shard state lock poisoned").parents = Some(Arc::clone(&built));
        built
    }

    fn validate_start(index: &TurboQuantIndex, start: &StartShardSearch) -> Result<(), Status> {
        let dim = index
            .dim_opt()
            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
        if start.vector.len() != dim {
            return Err(Status::invalid_argument(format!(
                "query vector has dim {}, index expects {dim}",
                start.vector.len()
            )));
        }
        if let Some((_, coord, value)) = turbovec::first_invalid_coord(&start.vector, dim) {
            return Err(Status::invalid_argument(format!(
                "query coordinate {coord} is invalid: {value}"
            )));
        }
        Ok(())
    }

    /// Persist the index to its configured path, if any. Shared by the
    /// `Flush` RPC and save-on-shutdown in the binary.
    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let num_vectors = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let Some(config_path) = self.config.index_path.clone() else {
            return Ok(FlushResponse {
                path: String::new(),
                num_vectors,
                num_documents,
                written: false,
            });
        };
        // Log before data: fsync the WAL BEFORE the index images are
        // written, so a crash between the two leaves the log a superset
        // of the on-disk indexes — never the reverse. An index image
        // whose records the log lost would silently drop those records
        // from every future replay (reshard, recovery).
        if let Some(wal) = guard.wal.as_mut() {
            wal.flush()
                .map_err(|e| Status::internal(format!("wal fsync {}: {e}", wal.dir().display())))?;
        }
        // Flush into the active snapshot generation when one was
        // installed, else the legacy layout — never split the two.
        let (tv_path, bm25_path) = storage_paths(&config_path, guard.generation.as_ref());
        if let Some(index) = guard.index.as_ref() {
            index
                .write(&tv_path)
                .map_err(|e| Status::internal(format!("write {}: {e}", tv_path.display())))?;
        }
        // Save the builder as v3 and immediately reopen it disk-resident:
        // after Flush a shard holds no postings or texts in heap.
        // Already-resident shards have nothing to write.
        let built = match guard.bm25.as_mut() {
            Some(Bm25Shard::Building(store)) => {
                store
                    .save(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            Some(Bm25Shard::Spilling(builder)) => {
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
        let written = guard.index.is_some() || guard.bm25.is_some();
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
        Ok(FlushResponse {
            path: tv_path.display().to_string(),
            num_vectors,
            num_documents,
            written,
        })
    }

    /// Receive one snapshot image into the staging generation directory
    /// (`index.tv`, plus `index.tv.bm25` when declared). The first
    /// `manifest.tv_bytes` of data land in the index, the rest in the
    /// sidecar; both are synced before the caller swaps anything. Returns
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
        let tv_tmp = generation_tv(tmp_dir);
        let bm25_tmp = generation_bm25(tmp_dir);
        let mut tv = tokio::fs::File::create(&tv_tmp)
            .await
            .map_err(|e| io_err(&tv_tmp, e))?;
        let mut bm25 = if manifest.bm25_bytes > 0 {
            Some(
                tokio::fs::File::create(&bm25_tmp)
                    .await
                    .map_err(|e| io_err(&bm25_tmp, e))?,
            )
        } else {
            None
        };
        let (mut tv_written, mut bm25_written) = (0u64, 0u64);
        while let Some(chunk) = inbound.message().await? {
            let Some(snapshot_chunk::Payload::Data(mut data)) = chunk.payload else {
                return Err(Status::invalid_argument(
                    "SnapshotChunk after the manifest must carry data",
                ));
            };
            // Fill the .tv first; overflow spills into the .bm25.
            let tv_take = (manifest.tv_bytes - tv_written).min(data.len() as u64) as usize;
            if tv_take > 0 {
                tv.write_all(&data[..tv_take])
                    .await
                    .map_err(|e| io_err(&tv_tmp, e))?;
                tv_written += tv_take as u64;
                data.drain(..tv_take);
            }
            if !data.is_empty() {
                let Some(sidecar) = bm25.as_mut() else {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                };
                if bm25_written + data.len() as u64 > manifest.bm25_bytes {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                }
                sidecar
                    .write_all(&data)
                    .await
                    .map_err(|e| io_err(&bm25_tmp, e))?;
                bm25_written += data.len() as u64;
            }
        }
        if tv_written != manifest.tv_bytes || bm25_written != manifest.bm25_bytes {
            return Err(Status::invalid_argument(format!(
                "truncated snapshot: received {tv_written}+{} of declared {}+{} bytes",
                bm25_written, manifest.tv_bytes, manifest.bm25_bytes
            )));
        }
        tv.sync_all().await.map_err(|e| io_err(&tv_tmp, e))?;
        if let Some(sidecar) = bm25.as_mut() {
            sidecar.sync_all().await.map_err(|e| io_err(&bm25_tmp, e))?;
        }
        Ok(())
    }

    /// Validate a received snapshot image and atomically swap it in (the
    /// blocking half of `InstallSnapshot`). Everything that can fail —
    /// loading the index, opening the sidecar, the calibration check —
    /// happens BEFORE the swap, so a rejected install leaves the live
    /// shard and the on-disk generation untouched.
    ///
    /// The swap itself is one directory rename: the whole `.tv` + `.bm25`
    /// pair travels inside the staging dir, so the two files can never
    /// tear. Replacing an existing generation renames it aside first; the
    /// crash window between the two renames is covered by
    /// [`recover_generation`] at startup.
    fn apply_snapshot(
        &self,
        tmp_dir: &Path,
        with_bm25: bool,
    ) -> Result<InstallSnapshotResponse, Status> {
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path")
            .clone();
        let snap = generation_dir(&path);
        let old = generation_old_dir(&path);
        let tv_tmp = generation_tv(tmp_dir);
        let bm25_tmp = generation_bm25(tmp_dir);

        let loaded = TurboQuantIndex::load(&tv_tmp).map_err(|e| {
            Status::invalid_argument(format!("snapshot is not a valid turbovec index: {e}"))
        })?;
        if with_bm25 {
            // Open-check the sidecar (and drop it again) before the swap;
            // the live shard re-opens from the generation dir.
            drop(Bm25Shard::open(&bm25_tmp).map_err(|e| {
                Status::invalid_argument(format!("snapshot sidecar is not a valid BM25 store: {e}"))
            })?);
        }

        let mut guard = self.state.write().expect("shard state lock poisoned");
        // Calibration comparability: a shard with a locked calibration
        // (seeded or fitted) only accepts an identically calibrated image.
        if let Some(index) = guard.index.as_ref() {
            if let Some((shift, scale)) = index.calibration() {
                let matches = loaded
                    .calibration()
                    .is_some_and(|(s, c)| s == shift && c == scale);
                if !matches {
                    return Err(Status::failed_precondition(
                        "snapshot calibration differs from the calibration locked on this \
                         shard; mixed calibrations make scores incomparable across shards",
                    ));
                }
            }
        }

        // The atomic swap: previous generation aside (if any), staging
        // dir into place. Both files move inside ONE directory rename.
        if snap.exists() {
            std::fs::rename(&snap, &old)
                .map_err(|e| Status::internal(format!("retire {}: {e}", old.display())))?;
        }
        if let Err(e) = std::fs::rename(tmp_dir, &snap) {
            // Best-effort rollback so startup recovery sees a clean state.
            if old.exists() && !snap.exists() {
                let _ = std::fs::rename(&old, &snap);
            }
            return Err(Status::internal(format!("install {}: {e}", snap.display())));
        }
        let _ = std::fs::remove_dir_all(&old);

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
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let num_vectors = loaded.len() as u64;
        guard.index = Some(loaded);
        guard.generation = Some(snap.clone());
        guard.stats_epoch += 1;
        // The snapshot supersedes the log: fsync and retire the current
        // generation, open gen-(g+1) with the installed image's
        // calibration (same bucket geometry), and mark where it came
        // from. Records before this point describe the OLD shard
        // contents.
        if guard.wal.is_some() {
            let source_generation = guard.wal.as_ref().map_or(0, WalWriter::generation);
            // The installed image is state this fresh log does NOT
            // contain: record it as preexisting so the reshard tool
            // refuses a log-only replay that would drop the image.
            let mut manifest = wal_manifest(
                guard.index.as_ref(),
                &self.config,
                source_generation + 1,
                (num_vectors, num_documents),
            );
            let previous = guard.wal.as_ref().expect("checked above").manifest();
            manifest.bucket_bits = previous.bucket_bits;
            manifest.bucket_count = previous.bucket_count;
            let wal_err = |e: std::io::Error| Status::internal(format!("wal rotate: {e}"));
            let wal = guard.wal.as_mut().expect("checked above");
            wal.flush().map_err(wal_err)?;
            *wal = WalWriter::create(&wal::wal_dir(&path), manifest).map_err(wal_err)?;
            wal.append(wal_record::Op::Snapshot(SnapshotMarker {
                source_generation,
            }))
            .map_err(wal_err)?;
            wal.flush().map_err(wal_err)?;
        }
        Ok(InstallSnapshotResponse {
            path: generation_tv(&snap).display().to_string(),
            num_vectors,
            num_documents,
        })
    }

    /// Apply one `SetCalibration`: lock the calibration on an empty shard.
    fn apply_calibration(&self, req: &SetCalibrationRequest) -> Result<bool, Status> {
        let dim = req.dim as usize;
        let bit_width = req.bit_width as usize;
        let build = || {
            TurboQuantIndex::new_with_calibration(dim, bit_width, &req.shift, &req.scale)
                .map_err(|e| Status::invalid_argument(format!("invalid calibration: {e}")))
        };
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let result = match guard.index.as_ref() {
            Some(index) if !index.is_empty() => Err(Status::failed_precondition(format!(
                "shard holds {} vectors; calibration is locked for the index lifetime",
                index.len()
            ))),
            Some(index) => {
                let same = index.dim_opt() == Some(dim)
                    && index.bit_width() == bit_width
                    && index.calibration().is_some_and(|(s, c)| {
                        s == req.shift.as_slice() && c == req.scale.as_slice()
                    });
                if same {
                    return Ok(true); // idempotent retry
                }
                if index.calibration().is_some() {
                    return Err(Status::already_exists(
                        "a different calibration is already locked on this shard",
                    ));
                }
                // Empty, unseeded index: replace with the seeded one.
                guard.index = Some(build()?);
                Ok(false)
            }
            None => {
                guard.index = Some(build()?);
                Ok(false)
            }
        };
        // Complete the pending WAL manifest with the locked calibration
        // (no-op once calibration is on disk).
        if result.is_ok() {
            if let Some(wal) = guard.wal.as_mut() {
                wal.update_manifest(|m| {
                    m.dim = dim as u32;
                    m.bit_width = bit_width as u32;
                    m.calibration_shift = req.shift.clone();
                    m.calibration_scale = req.scale.clone();
                });
            }
        }
        result
    }

    /// Apply one ingested batch under the write lock. Returns
    /// `(added, global id of the batch's first vector)`.
    fn apply_batch(&self, batch: AddVectorsRequest) -> Result<(u64, u64), Status> {
        if batch.vectors.is_empty() {
            return Ok((0, 0));
        }
        let mut guard = self.state.write().expect("shard state lock poisoned");
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
        if let Some((vi, ci, v)) = turbovec::first_invalid_coord(&batch.vectors, dim) {
            return Err(Status::invalid_argument(format!(
                "invalid input value at vector {vi}, coord {ci}: {v}"
            )));
        }
        let (first_id, index_bit_width) = {
            let index = match guard.index.as_mut() {
                Some(index) => index,
                None => {
                    // From-scratch, unseeded: turbovec fits calibration from
                    // this first batch. Seeded deployment is the SetCalibration
                    // path; this exists for single-shard convenience.
                    guard.index = Some(
                        TurboQuantIndex::new(dim, self.config.bit_width)
                            .map_err(|e| Status::invalid_argument(format!("{e}")))?,
                    );
                    guard.index.as_mut().expect("just constructed")
                }
            };
            (
                self.config.slot_offset + index.len() as u64,
                index.bit_width(),
            )
        };
        // Apply first, log after, under this one lock. A failed apply
        // must never reach the log: its assigned ids would be reused by
        // the next batch and the duplicate would poison every replay.
        // Durability is unaffected — both sides are volatile until
        // Flush, which fsyncs the log BEFORE the index images.
        guard
            .index
            .as_mut()
            .expect("constructed or present above")
            .add_2d(&batch.vectors, dim)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        // One record PER VECTOR: contiguous ids hash to different
        // buckets, and a bucket file must never hold vectors that belong
        // to another bucket. Buffered (no fsync per batch); Flush and
        // generation rotation fsync.
        if let Some(wal) = guard.wal.as_mut() {
            wal.update_manifest(|m| {
                if m.dim == 0 {
                    m.dim = dim as u32;
                    m.bit_width = index_bit_width as u32;
                }
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
    fn bm25_query_fused(&self, req: &Bm25QueryRequest) -> Result<Bm25QueryResponse, Status> {
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
        let guard = self.state.read().expect("shard state lock poisoned");
        guard.check_stats_epoch(req.expected_stats_epoch)?;
        // Filled inside the scoring arm (the facet walk reuses the
        // resolved field views); a shard with no lexical half answers
        // every requested facet field as unknown.
        let mut facets: Vec<crate::pb::FacetFieldCounts> = Vec::new();
        let hits: Vec<Bm25Hit> = match guard.bm25.as_ref() {
            // Facet counting enters the arm even at k == 0 (the flat
            // path counts regardless of k; the scorers return no hits
            // for k == 0 on their own).
            Some(store)
                if req.k > 0
                    || !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty() =>
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
                if !req.facet_fields.is_empty() || !req.map_facet_fields.is_empty() {
                    let pairs: Vec<(&dyn Bm25Index, &[String])> = views
                        .iter()
                        .zip(&leg_of_view)
                        .map(|(view, &li)| (view.as_ref(), req.fields[li].terms.as_slice()))
                        .collect();
                    facets =
                        store.count_facets(&pairs, &req.facet_fields, &req.map_facet_fields);
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
                let docs = if prunable {
                    bm25::top_k_fused_pruned(&queries, req.k as usize, floor)
                } else {
                    bm25::filter_fused_to_floor(
                        bm25::top_k_fused_exhaustive(&queries, req.k as usize),
                        floor,
                    )
                };
                docs.into_iter()
                    .map(|doc| Bm25Hit {
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
                    .collect()
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
            hits,
            kth_best,
            facets,
            // The fused route refuses score stages upstream.
            stage_columns_known: Vec::new(),
        })
    }

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
    ) -> Result<(RawLeg, RawLeg), Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        guard.check_stats_epoch(expected_stats_epoch)?;

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
                if let Some((_, coord, value)) = turbovec::first_invalid_coord(vector, dim) {
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
                let docs = if prunable {
                    bm25::top_k_pruned(index, terms, &stats, params, k, f64::NEG_INFINITY)
                } else {
                    bm25::top_k(index, terms, &stats, params, k)
                };
                bm25_leg = docs
                    .into_iter()
                    .map(|d| (self.config.slot_offset + u64::from(d.doc_id), d.score))
                    .collect();
            }
        }

        Ok((vector_leg, bm25_leg))
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

        let (vector_leg, bm25_leg) = self.compute_legs(
            &req.vector,
            &req.terms,
            req.global_doc_count,
            req.global_total_doc_length,
            &req.global_doc_frequencies,
            params_from(req.k1, req.b)?,
            k,
            req.expected_stats_epoch,
        )?;

        let fused = fusion::rrf_fuse(
            &[
                Leg {
                    hits: vector_leg,
                    weight: vector_weight,
                },
                Leg {
                    hits: bm25_leg,
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
    /// `(field table index, analysis)`, in submission order. `None` until
    /// that field's result lands.
    extras: Vec<(usize, Option<crate::postings::AnalyzedField>)>,
    /// Extras still unfilled. The document is ready to apply when this is
    /// zero AND its body result has arrived.
    outstanding: usize,
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
fn join_fields(
    body: crate::postings::AnalyzedDoc,
    extras: Vec<(usize, Option<crate::postings::AnalyzedField>)>,
) -> Result<crate::postings::AnalyzedDoc, Status> {
    if extras.is_empty() {
        return Ok(body);
    }
    let n = extras.iter().map(|&(fi, _)| fi + 1).max().unwrap_or(1);
    let mut fields = vec![crate::postings::AnalyzedField::default(); n];
    fields[0] = body.into_body();
    for (fi, analyzed) in extras {
        fields[fi] = analyzed.ok_or_else(|| {
            Status::internal(format!(
                "field {fi} applied before its analysis arrived; the apply \
                 wavefront must not advance past an unfilled field"
            ))
        })?;
    }
    Ok(crate::postings::AnalyzedDoc { fields })
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
            if seen.contains(&field.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in one document",
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

    /// Apply one analyzed document: id assignment, store insert, WAL
    /// append. Must be called in arrival order — both transports
    /// guarantee it.
    fn apply_analyzed_document(
        &self,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        let mut guard = self.state.write().expect("shard state lock poisoned");
        // A disk-resident shard that receives more documents is first
        // reloaded into the heap builder (the append path is
        // bulk-load: build in memory, flush back to v3).
        if matches!(guard.bm25, Some(Bm25Shard::Resident(_))) {
            let bm25_path = self
                .config
                .index_path
                .as_ref()
                .map(|p| storage_paths(p, guard.generation.as_ref()).1)
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
        if guard.bm25.is_none() {
            let builder = self.new_builder(guard.generation.as_ref())?;
            guard.bm25 = Some(builder);
        }
        let doc_id = vector_tip.max(
            guard
                .bm25
                .as_ref()
                .expect("builder just ensured")
                .next_doc_id(),
        );
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
        let global_id = self.config.slot_offset + u64::from(doc_id);
        if *added == 0 {
            *first_id = global_id;
        }
        // Apply first, log after, as for vectors: a document that
        // fails to enter the store must never reach the log, or its
        // id would be reassigned and poison the replay.
        let lineage = doc.lineage.map(|l| crate::postings::DocLineage {
            opinion_id: l.opinion_id,
            cluster_id: l.cluster_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        match guard.bm25.as_mut().expect("builder just ensured") {
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
            }),
        );
        guard.stats_epoch += 1;
        *added += 1;
        Ok(())
    }

    /// Bulk ingest over one AnalyzeStream: submissions run ahead of the
    /// apply point as far as the sidecar grants credit, results return
    /// in completion order, and the apply wavefront advances over
    /// consecutive sequences so application stays in arrival order.
    async fn ingest_streamed(
        &self,
        mut session: crate::analyzer::AnalyzeStream,
        first: AddDocumentsRequest,
        inbound: &mut Streaming<AddDocumentsRequest>,
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
        enum Step {
            Doc(AddDocumentsRequest),
            InboundClosed,
            Result(Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>),
            Field(Option<FieldEvent>),
        }
        let mut spec = first.analysis.clone();
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
                outstanding: first_extras.len(),
                extras: first_extras,
            },
        );
        let mut next_seq = 1u64;
        let mut next_apply = 0u64;
        let mut inbound_open = true;
        loop {
            self.advance_apply(&mut pending, &mut results, &mut next_apply, added, first_id)?;
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
                message = inbound.message(),
                    if inbound_open && pending.len() < MAX_PENDING => match message? {
                        Some(doc) => Step::Doc(doc),
                        None => Step::InboundClosed,
                    },
                result = session.next(), if want_body => Step::Result(result?),
                event = fields.recv(), if want_field => Step::Field(event),
            };
            match step {
                Step::Doc(doc) => {
                    // Extra-field analyses are queued on arrival
                    // (validated now, so a bad field fails before the
                    // body enters the session).
                    let extras = self
                        .submit_field_analyses(&doc, next_seq, &mut fields, &mut route)
                        .await?;
                    if doc.analysis != spec {
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
                        session = crate::analyzer::AnalyzeStream::open(addr, doc.analysis.as_ref())
                            .await?;
                        spec = doc.analysis.clone();
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
            if !ready {
                return Ok(());
            }
            let analyzed = results.remove(next_apply).expect("readiness just checked");
            let held = pending.remove(next_apply).expect("readiness just checked");
            let analyzed = join_fields(analyzed, held.extras)?;
            self.apply_analyzed_document(held.doc, analyzed, added, first_id)?;
            *next_apply += 1;
        }
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    type SearchShardStream = ReceiverStream<Result<SearchShardResponse, Status>>;
    type StreamSearchStream = ReceiverStream<Result<StreamSearchResponse, Status>>;

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
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
                        return Err(Status::aborted("shard grew between setup and scan; retry"));
                    }
                    Ok(chunked_topk_collapsed(
                        index,
                        &start.vector,
                        start.k as usize,
                        chunk_blocks,
                        &parents,
                        &mut external_floor,
                        &mut publish_floor,
                    ))
                });
                let outcome = match scan.await {
                    Ok(result) => result,
                    Err(e) => Err(Status::internal(format!("collapse scan task failed: {e}"))),
                };
                match outcome {
                    Ok((hits, stats)) => {
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
                            }),
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

            let outcome: Result<(Vec<ChunkHit>, ScanStats), Status> = match scan_queue {
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
                                external: Box::new(external_floor),
                                publish: Box::new(publish_floor),
                                done: done_tx,
                            };
                            if jobs.send(job).await.is_err() {
                                Err(Status::internal("scan scheduler unavailable"))
                            } else {
                                match done_rx.await {
                                    Ok(result) => result,
                                    Err(_) => {
                                        Err(Status::internal("scan batch dropped before finishing"))
                                    }
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
                        Ok(chunked_topk(
                            index,
                            &start.vector,
                            start.k as usize,
                            chunk_blocks,
                            &mut external_floor,
                            &mut publish_floor,
                            start.tie_complete,
                        ))
                    });
                    match scan.await {
                        Ok(result) => result,
                        Err(e) => Err(Status::internal(format!("scan task failed: {e}"))),
                    }
                }
            };

            match outcome {
                Ok((hits, stats)) => {
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
                        }),
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
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let (num_vectors, dim, bit_width) = match guard.index.as_ref() {
            Some(index) => (
                index.len() as u64,
                index.dim_opt().unwrap_or(0) as u32,
                index.bit_width() as u32,
            ),
            None => (0, 0, self.config.bit_width as u32),
        };
        let (bm25_docs, bm25_building) = match guard.bm25.as_ref() {
            Some(shard) => (shard.doc_count(), matches!(shard, Bm25Shard::Spilling(_))),
            None => (0, false),
        };
        Ok(Response::new(HealthResponse {
            num_vectors,
            dim,
            bit_width,
            slot_offset: self.config.slot_offset,
            bm25_docs,
            bm25_building,
            ingest_active: self.ingest_busy.load(std::sync::atomic::Ordering::Acquire),
        }))
    }

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
        let state = self.state.clone();
        let slot_offset = self.config.slot_offset;
        let floor_cells = Arc::clone(&self.floor_cells);

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

            // Floor raises fold into one shared cell the blocking scan
            // polls after each emitted block. Two lanes feed it: the
            // stream's own FloorUpdate messages (reliable) and, when
            // the Start carried a token, the node's UDP floor listener
            // (fast, lossy, same monotone fold).
            let floor_cell = Arc::new(std::sync::atomic::AtomicU32::new(
                f32::NEG_INFINITY.to_bits(),
            ));
            let udp_token = (start.floor_token != 0).then_some(start.floor_token);
            if let Some(token) = udp_token {
                floor_cells
                    .lock()
                    .expect("floor registry poisoned")
                    .insert(token, Arc::clone(&floor_cell));
            }
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let stop_pump = Arc::clone(&stop);
            let pump_cell = Arc::clone(&floor_cell);
            tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                        })) => raise_floor_cell(&pump_cell, u.floor),
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::Stop(_)),
                        })) => {
                            stop_pump.store(true, std::sync::atomic::Ordering::Release);
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
            let scan_cell = Arc::clone(&floor_cell);
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
                        turbovec::first_invalid_coord(&start.vector, dim)
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

                    let mut options = turbovec::SearchOptions::new();
                    let mut floor_now = f32::NEG_INFINITY;
                    if let Some(f) = start.initial_floor {
                        options = options.with_initial_threshold(f);
                        floor_now = f;
                    }
                    let mut raises = 0u64;
                    let stride = if parents.is_some() { 20 } else { 12 };
                    let summary = index
                        .try_search_streaming(&start.vector, options, |batch| {
                            // Pack the batch as fixed-stride LE records
                            // (u64 global id, f32 score, and in document
                            // mode the slot's u64 parent), fused into the
                            // slot-to-global-id rebase — one pass, no
                            // per-hit messages. Real emissions only
                            // carry live slots; a negative would be an
                            // engine contract break, dropped rather
                            // than wrapped into a bogus global id.
                            let mut hits: Vec<u8> = Vec::with_capacity(stride * batch.slots.len());
                            for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                                if slot < 0 {
                                    continue;
                                }
                                hits.extend_from_slice(&(slot_offset + slot as u64).to_le_bytes());
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
                            // A dead response channel means the client is
                            // gone: stop scanning, nobody is listening.
                            if sent.is_err() || stop.load(std::sync::atomic::Ordering::Acquire) {
                                return turbovec::StreamControl::Stop;
                            }
                            let f = f32::from_bits(
                                scan_cell.load(std::sync::atomic::Ordering::Acquire),
                            );
                            if f > floor_now {
                                floor_now = f;
                                raises += 1;
                                turbovec::StreamControl::RaiseFloor(f)
                            } else {
                                turbovec::StreamControl::Continue
                            }
                        })
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    Ok(StreamSearchSummary {
                        completed: summary.completed,
                        emitted: summary.emitted as u64,
                        blocks_scanned: summary.blocks_scanned as u64,
                        floor_raises_applied: raises,
                    })
                });
            let outcome = scan.await;
            if let Some(token) = udp_token {
                floor_cells
                    .lock()
                    .expect("floor registry poisoned")
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
    }

    async fn get_calibration(
        &self,
        _request: Request<GetCalibrationRequest>,
    ) -> Result<Response<GetCalibrationResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let (dim, bit_width, num_vectors, shift, scale) = match guard.index.as_ref() {
            Some(index) => {
                let (shift, scale) = index
                    .calibration()
                    .map(|(s, c)| (s.to_vec(), c.to_vec()))
                    .unwrap_or_default();
                (
                    index.dim_opt().unwrap_or(0) as u32,
                    index.bit_width() as u32,
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
        let _ingest = self.claim_ingest()?;
        let mut inbound = request.into_inner();
        let mut added = 0u64;
        let mut first_id = 0u64;
        while let Some(batch) = inbound.message().await? {
            let service = self.clone();
            let (batch_added, batch_first_id) =
                tokio::task::spawn_blocking(move || service.apply_batch(batch))
                    .await
                    .map_err(|e| Status::internal(format!("add task failed: {e}")))??;
            if added == 0 && batch_added > 0 {
                first_id = batch_first_id;
            }
            added += batch_added;
        }
        let total = self
            .state
            .read()
            .expect("shard state lock poisoned")
            .index
            .as_ref()
            .map_or(0, |i| i.len() as u64);
        Ok(Response::new(AddVectorsResponse {
            added,
            total,
            first_id,
        }))
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
            }) if m.tv_bytes > 0 => m,
            _ => {
                return Err(Status::invalid_argument(
                    "first SnapshotChunk must be a SnapshotManifest with tv_bytes > 0",
                ))
            }
        };

        if let Err(e) = Self::receive_image(&mut inbound, &manifest, &tmp_dir).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            return Err(e);
        }

        let service = self.clone();
        let cleanup = tmp_dir.clone();
        let with_bm25 = manifest.bm25_bytes > 0;
        let result =
            tokio::task::spawn_blocking(move || service.apply_snapshot(&tmp_dir, with_bm25))
                .await
                .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
        if result.is_err() {
            // Rejected AFTER receive (bad image, calibration mismatch):
            // leave no staging dir behind either.
            let _ = tokio::fs::remove_dir_all(&cleanup).await;
        }
        result.map(Response::new)
    }

    async fn add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        let _ingest = self.claim_ingest()?;
        let addr = self.config.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured for this shard (analysis_addr)")
        })?;
        let mut inbound = request.into_inner();
        let mut added = 0u64;
        let mut first_id = 0u64;
        // Analysis dominates bulk ingest, and the only supported transport
        // is one AnalyzeStream for the whole call, paced by the sidecar's
        // own flow control. Documents are applied strictly in arrival
        // order, so ids and WAL order stay deterministic.
        //
        // A sidecar without AnalyzeStream is REFUSED rather than served on
        // the old per-document unary path. That fallback existed and cost
        // real debugging time: a stale sidecar silently took it, then its
        // gRPC server GOAWAYed the connection after ~70 streams, and the
        // bulk driver died seconds into a multi-hour job with an opaque
        // "h2 protocol error" while this node logged nothing and stayed
        // healthy. Degrading quietly turned a one-line version mismatch
        // into an h2 forensics exercise; failing here names it instead.
        if let Some(first) = inbound.message().await? {
            match crate::analyzer::AnalyzeStream::open(&addr, first.analysis.as_ref()).await {
                Ok(session) => {
                    self.ingest_streamed(
                        session,
                        first,
                        &mut inbound,
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
        let total = self
            .state
            .read()
            .expect("shard state lock poisoned")
            .bm25
            .as_ref()
            .map_or(0, |b| b.doc_count());
        Ok(Response::new(AddDocumentsResponse {
            added,
            total,
            first_id,
        }))
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        let req = request.into_inner();
        let guard = self.state.read().expect("shard state lock poisoned");
        let (doc_count, total_doc_length, doc_frequencies, field_stats) = match guard.bm25.as_ref()
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
                            crate::pb::FieldStats {
                                total_doc_length: view.total_doc_length(),
                                doc_frequencies: ft.terms.iter().map(|t| view.df(t)).collect(),
                                known: true,
                            }
                        }
                        None => crate::pb::FieldStats {
                            total_doc_length: 0,
                            doc_frequencies: vec![0; ft.terms.len()],
                            known: false,
                        },
                    })
                    .collect();
                (
                    store.doc_count(),
                    index.total_doc_length(),
                    req.terms.iter().map(|t| index.df(t)).collect(),
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
                        total_doc_length: 0,
                        doc_frequencies: vec![0; ft.terms.len()],
                        known: false,
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
    }

    async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        let req = request.into_inner();
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
            return self.bm25_query_fused(&req).map(Response::new);
        }
        let stage_specs = parse_score_stages(&req.score_stages)?;
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
        // Count-then-rank facets over the full match set, before any
        // k/floor narrowing (see count_facets). A shard with no
        // lexical half has no facet table: every requested field is
        // legitimately unknown here.
        let facets = match guard.bm25.as_ref() {
            Some(store) if !req.facet_fields.is_empty() || !req.map_facet_fields.is_empty() => {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                store.count_facets(
                    &[(index, &req.terms)],
                    &req.facet_fields,
                    &req.map_facet_fields,
                )
            }
            _ => req
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
                .collect(),
        };
        // Which stage columns this shard's numeric table has —
        // computed regardless of k, like the facet known flags: a
        // shard lacking a column answers identity (exact), and the
        // coordinator refuses a column NO shard knows.
        let stage_columns_known: Vec<bool> = match guard.bm25.as_ref() {
            Some(store) => stage_specs
                .iter()
                .map(|(_, column, key)| {
                    if key.is_empty() {
                        store.numeric_index(column).is_some()
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
        let hits = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 => {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // 0/absent means unseeded (scores are always positive).
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
                let numeric_read = ShardNumericRead(store);
                let chain_ctx: bm25::ChainCtx = if stage_specs.is_empty() {
                    None
                } else {
                    Some((&chain, &numeric_read))
                };
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
                    bm25::top_k_pruned_chained(
                        index,
                        &req.terms,
                        &stats,
                        params,
                        req.k as usize,
                        floor,
                        chain_ctx,
                    )
                } else {
                    bm25::filter_to_floor(
                        bm25::top_k_chained(
                            index,
                            &req.terms,
                            &stats,
                            params,
                            req.k as usize,
                            chain_ctx,
                        ),
                        floor,
                    )
                };
                docs.into_iter()
                    .map(|doc| Bm25Hit {
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
                    .collect()
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
        Ok(Response::new(Bm25QueryResponse {
            hits,
            kth_best,
            facets,
            stage_columns_known,
        }))
    }

    async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        let req = request.into_inner();
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
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
        let hits = match guard.bm25.as_ref() {
            Some(store) => {
                // Route global ids to this shard's local range.
                let local: Vec<u32> = req
                    .candidate_ids
                    .iter()
                    .filter(|&&id| id >= offset && (id - offset) <= u64::from(u32::MAX))
                    .map(|id| (id - offset) as u32)
                    .collect();
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                bm25::score_candidates(index, &req.terms, &stats, params, &local)
                    .into_iter()
                    .map(|doc| Bm25Hit {
                        doc_id: offset + u64::from(doc.doc_id),
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
                    .collect()
            }
            None => Vec::new(),
        };
        Ok(Response::new(Bm25RescoreResponse { hits }))
    }

    async fn vector_rescore(
        &self,
        request: Request<VectorRescoreRequest>,
    ) -> Result<Response<VectorRescoreResponse>, Status> {
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
            if let Some((_, coord, value)) = turbovec::first_invalid_coord(&req.vector, dim) {
                return Err(Status::invalid_argument(format!(
                    "query coordinate {coord} is invalid: {value}"
                )));
            }
            // Route global ids into this shard's live slots; the mask
            // names slots, so it is sized to slot_capacity (== len on
            // these append-only shards, but the mask contract is
            // capacity). The kernel short-circuits fully-masked SIMD
            // blocks, so a tiny allowlist costs a mask walk, not a scan.
            let n = index.len();
            let mut mask = vec![false; index.slot_capacity()];
            let mut allowed = 0usize;
            for &id in &req.candidate_ids {
                if id >= offset && id - offset < n as u64 {
                    let slot = (id - offset) as usize;
                    if !mask[slot] {
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
    }

    async fn get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
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
                if let Some(text) = store.text(local) {
                    documents.push(StoredDocument {
                        doc_id: id,
                        text,
                        lineage: store.lineage(local).map(|l| crate::pb::DocLineage {
                            opinion_id: l.opinion_id,
                            cluster_id: l.cluster_id,
                            span_start: l.span_start,
                            span_end: l.span_end,
                        }),
                    });
                }
            }
        }
        Ok(Response::new(GetDocumentsResponse { documents }))
    }

    async fn hybrid_shard(
        &self,
        request: Request<HybridShardRequest>,
    ) -> Result<Response<HybridShardResponse>, Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.run_hybrid(request.into_inner()))
            .await
            .map_err(|e| Status::internal(format!("hybrid task failed: {e}")))?
            .map(Response::new)
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        let req = request.into_inner();
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let (vector_hits, bm25_hits) = service.compute_legs(
                &req.vector,
                &req.terms,
                req.global_doc_count,
                req.global_total_doc_length,
                &req.global_doc_frequencies,
                params_from(req.k1, req.b)?,
                req.k as usize,
                req.expected_stats_epoch,
            )?;
            Ok(ShardLegsResponse {
                vector_hits: vector_hits
                    .into_iter()
                    .map(|(doc_id, score)| RawLegHit {
                        doc_id,
                        score: score as f32,
                    })
                    .collect(),
                bm25_hits: bm25_hits
                    .into_iter()
                    .map(|(doc_id, score)| RawLegHit {
                        doc_id,
                        score: score as f32,
                    })
                    .collect(),
            })
        })
        .await
        .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))?
        .map(Response::new)
    }
}

#[cfg(test)]
mod floor_lane_tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn cell_of(cells: &std::sync::Mutex<HashMap<u64, Arc<AtomicU32>>>, token: u64) -> f32 {
        f32::from_bits(cells.lock().unwrap()[&token].load(Ordering::Acquire))
    }

    fn datagram(token: u64, floor: f32) -> Vec<u8> {
        let mut d = Vec::with_capacity(12);
        d.extend_from_slice(&token.to_le_bytes());
        d.extend_from_slice(&floor.to_le_bytes());
        d
    }

    /// The UDP fold: raises apply, non-raises and garbage never do, and
    /// no input shape can panic the listener.
    #[test]
    fn floor_datagrams_fold_monotonically_and_ignore_garbage() {
        let cells: std::sync::Mutex<HashMap<u64, Arc<AtomicU32>>> =
            std::sync::Mutex::new(HashMap::new());
        cells
            .lock()
            .unwrap()
            .insert(7, Arc::new(AtomicU32::new(f32::NEG_INFINITY.to_bits())));

        apply_floor_datagram(&cells, &datagram(7, 0.25));
        assert_eq!(cell_of(&cells, 7), 0.25);
        // Lower, equal, and NaN floors are ignored.
        apply_floor_datagram(&cells, &datagram(7, 0.10));
        apply_floor_datagram(&cells, &datagram(7, 0.25));
        apply_floor_datagram(&cells, &datagram(7, f32::NAN));
        assert_eq!(cell_of(&cells, 7), 0.25);
        // Duplicated and reordered raises: max wins regardless.
        apply_floor_datagram(&cells, &datagram(7, 0.75));
        apply_floor_datagram(&cells, &datagram(7, 0.50));
        apply_floor_datagram(&cells, &datagram(7, 0.75));
        assert_eq!(cell_of(&cells, 7), 0.75);
        // Unknown tokens, short, long, and empty datagrams: dropped.
        apply_floor_datagram(&cells, &datagram(8, 9.0));
        apply_floor_datagram(&cells, &datagram(7, 9.0)[..11].to_vec());
        apply_floor_datagram(&cells, &[0u8; 13]);
        apply_floor_datagram(&cells, &[]);
        assert_eq!(cell_of(&cells, 7), 0.75);
        assert_eq!(cells.lock().unwrap().len(), 1);
    }
}
