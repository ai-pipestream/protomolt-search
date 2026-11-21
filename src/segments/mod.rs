//! Immutable aligned search segments and atomic segment-set publication.
//!
//! A segment owns one provider image, exact FP32 rows, BM25/columns/lineage,
//! and the matching tombstone overlay over one stable-id range. New segments
//! are copied and fully hashed in a staging directory, opened and validated,
//! then added to a generation-stamped set manifest. Queries snapshot one
//! `Arc<OpenedSegmentSet>` and therefore never observe half a publish or
//! compaction. BM25 statistics are summed across the snapshot before any
//! segment scores, and vector/BM25 hits merge under one global total order.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::bm25::{self, Bm25Params, CorpusStats};
use crate::exact_vectors::ExactVectorStore;
use crate::live_docs::LiveDocs;
use crate::postings::{Bm25Index, Bm25Reader, StoredBinding};
use crate::vector::{QualityContract, ScoreDirection, VectorIndex, VectorSearchOptions};
use prost::Message;

mod artifact;
mod catalog;
mod catalog_fence;
mod compact;
pub(crate) mod generation;
mod maintenance;
mod manifest;
mod open;
mod opened;
pub(crate) mod rewrite_proof;
mod row_update;
mod search;
mod stage;
mod summary;
#[cfg(test)]
mod tests;
mod validate;

#[cfg(test)]
pub(crate) use artifact::FAIL_SET_SYNC;
use artifact::{copy_artifact, digest_file, verify_artifact, write_json_atomic};
pub(crate) use catalog_fence::CatalogCutover;
pub use generation::SegmentGenerationDeclaration;
pub use manifest::{
    IntColumnSummary, NumericColumnSummary, PartitionRange, SegmentArtifact, SegmentBinding,
    SegmentMetadata, SegmentSetManifest, SegmentSourceOwner, SegmentSummary, UintColumnSummary,
};
pub use rewrite_proof::{
    proof_batch_rows_for_budget, BATCH_ROW_BYTES, CERTIFICATE_FORMAT, DEFAULT_PROOF_BATCH_ROWS,
    MAX_BATCH_ROWS as MAX_PROOF_BATCH_ROWS,
};
pub use row_update::SegmentRowRetirement;
pub(crate) use row_update::SourceGeneration;
use stage::{cleanup_staged, stage_segment, verify_retry_source};
pub use stage::{remove_segment_dirs, stage_segments, write_manifest_file};
pub(crate) use summary::summarize_columns;
use validate::{probe_set_format, validate_manifest, validate_relative_file, validate_segment_id};

/// The one segment-set format this build writes or reads. Format 6 is
/// the first whose segment records carry the colocation proof; there is
/// no ladder to older formats, which are refused by version number.
const SET_FORMAT: u32 = 6;
const SET_FILE: &str = "segments.json";
const SEGMENT_META_FILE: &str = "segment.json";

pub struct SegmentSource<'a> {
    pub segment_id: &'a str,
    pub generation: u64,
    pub base_label: u64,
    pub backend_kind: &'a str,
    /// `None` seals a documents-only segment: no vector rows, and the
    /// exact rows must be absent too.
    pub vector_path: Option<&'a Path>,
    pub exact_vector_path: Option<&'a Path>,
    pub multi_res_path: Option<&'a Path>,
    pub bm25_path: &'a Path,
    pub live_docs_path: &'a Path,
    /// The integer column a partitioned compaction ordered this segment
    /// by: the sealed summary then records the segment's value range as
    /// its partition.
    /// `None` for a tail seal and for the bucket layout.
    pub partition_column: Option<&'a str>,
    /// Whether every lineage parent with a row in this segment holds all
    /// of its live rows on this shard. Reshard and routed ingest prove it;
    /// anything that cannot passes `false`.
    pub parents_colocated: bool,
}

/// The scoring contracts a sealed segment's provider may carry: the
/// exhaustive quantized scan, or a configured approximate traversal,
/// whose results the planner admits only under ANN or a measured AUTO
/// policy. A probabilistic bound has no provider yet.
pub fn admitted_quality_contract(contract: QualityContract) -> bool {
    matches!(
        contract,
        QualityContract::ExhaustiveQuantized | QualityContract::ConfiguredAnn
    )
}

/// The artifact record of an artifact a segment does not have.
pub fn absent_artifact() -> SegmentArtifact {
    SegmentArtifact {
        file: String::new(),
        bytes: 0,
        sha256: String::new(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentHit {
    pub doc_id: u64,
    pub score: f32,
    pub segment_id: String,
}

#[derive(Debug)]
pub struct WalCompactionResult {
    pub snapshot: Arc<OpenedSegmentSet>,
    pub output: crate::reshard::SegmentedReshardOutput,
}

/// Parameters for [`SegmentCatalog::compact_wal_generations`].
pub struct WalCompactionRequest<'a> {
    pub input_ids: &'a [String],
    pub generations: &'a [PathBuf],
    pub work_dir: &'a Path,
    pub backend_kind: &'a str,
    pub slot_base: u64,
    pub slot_capacity: u64,
    pub bm25_fields: Option<&'a [String]>,
    pub partition: Option<crate::reshard::PartitionSpec<'a>>,
}

struct OpenedSegment {
    metadata: SegmentMetadata,
    /// `None` for a documents-only segment (an empty `vector` artifact).
    vector: Option<Arc<VectorIndex>>,
    exact: Option<Arc<ExactVectorStore>>,
    retained: Arc<crate::multi_res::MultiResVectors>,
    bm25: Arc<Bm25Reader>,
    live_docs: LiveDocs,
}

pub struct OpenedSegmentSet {
    root: PathBuf,
    binding: Option<StoredBinding>,
    declaration: Option<crate::pb::storage::IndexGenerationDeclaration>,
    manifest: SegmentSetManifest,
    /// Shared with the set this one was published from: a segment that
    /// is already open and verified stays open across a publish.
    segments: Vec<Arc<OpenedSegment>>,
}

impl std::fmt::Debug for OpenedSegmentSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenedSegmentSet")
            .field("root", &self.root)
            .field("epoch", &self.epoch())
            .field("segments", &self.manifest.segments)
            .finish()
    }
}

/// How a sealed segment's vector image is served: mapped from its file
/// (the default), or loaded into memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VectorLoad {
    #[default]
    Mapped,
    Heap,
}

#[derive(Clone)]
pub struct SegmentCatalog {
    root: PathBuf,
    current: Arc<RwLock<Arc<OpenedSegmentSet>>>,
    update: Arc<Mutex<()>>,
    pending_publication: Arc<Mutex<Option<SegmentSetManifest>>>,
    expected_manifest: Arc<Mutex<Option<SegmentSetManifest>>>,
    /// How every snapshot this catalog publishes serves vector images.
    load: VectorLoad,
}
