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

mod row_update;
pub use row_update::SegmentRowRetirement;

const BASE_SET_FORMAT: u32 = 1;
const SET_FORMAT: u32 = 2;
const SET_FILE: &str = "segments.json";
const SEGMENT_META_FILE: &str = "segment.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentArtifact {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
}

/// One column's range over a sealed segment: the least and greatest
/// stored value and how many rows carry one. A segment whose range
/// cannot intersect a request's predicate holds no row that passes it,
/// so a planner may leave the segment unopened without changing the
/// answer (docs/immutable-segments.md "Segment summaries").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntColumnSummary {
    pub name: String,
    pub min: i64,
    pub max: i64,
    pub present: u64,
}

/// [`IntColumnSummary`] for an unsigned column, including values above `i64::MAX`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UintColumnSummary {
    pub name: String,
    pub min: u64,
    pub max: u64,
    pub present: u64,
}

/// [`IntColumnSummary`] for a double column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NumericColumnSummary {
    pub name: String,
    pub min: f64,
    pub max: f64,
    pub present: u64,
}

/// The value range a partitioned compaction gave this segment
/// (docs/immutable-segments.md "Partitioned layout"): every row that
/// carries `column` has a value in `lo..=hi`, and the segments of one
/// set cover disjoint ascending ranges. Absent on a bucket-layout
/// segment and on the unkeyed segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRange {
    pub column: String,
    pub lo: i64,
    pub hi: i64,
}

/// Per-segment column ranges, written at seal time from the sealed
/// BM25 image. Columns with no stored value in the segment are listed
/// with `present == 0` and a `min > max` placeholder range: the full
/// integer range inverted, and for doubles `f64::MAX` over `f64::MIN`
/// (JSON has no infinities, so those are the placeholders that read
/// back).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SegmentSummary {
    #[serde(default)]
    pub int_columns: Vec<IntColumnSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uint_columns: Vec<UintColumnSummary>,
    #[serde(default)]
    pub numeric_columns: Vec<NumericColumnSummary>,
    #[serde(default)]
    pub partition: Option<PartitionRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentMetadata {
    pub segment_id: String,
    pub generation: u64,
    pub base_label: u64,
    pub rows: u64,
    pub live_rows: u64,
    pub backend_kind: String,
    pub scoring_fingerprint: String,
    pub analysis_fingerprints: Vec<u64>,
    pub document_count: u64,
    pub total_document_length: u64,
    pub vector: SegmentArtifact,
    pub exact_vectors: SegmentArtifact,
    pub bm25: SegmentArtifact,
    pub live_docs: SegmentArtifact,
    /// Absent on segments sealed before summaries existed; such a
    /// segment is never pruned.
    #[serde(default)]
    pub summary: Option<SegmentSummary>,
}

impl SegmentMetadata {
    pub fn end_label_exclusive(&self) -> Result<u64, String> {
        self.base_label
            .checked_add(self.rows)
            .ok_or_else(|| format!("segment {:?} label range overflows", self.segment_id))
    }
}

/// Canonical protobuf binding and its checksum, published with the segment set.
/// The payload uses the complete LoggedBinding declaration shared with the WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentBinding {
    pub protobuf: Vec<u8>,
    pub sha256: String,
}

impl SegmentBinding {
    pub fn encode(binding: &StoredBinding) -> Result<Self, String> {
        let protobuf = crate::pb::wal::LoggedBinding {
            plan_fingerprint: binding.plan_fingerprint.clone(),
            body_path: binding.body_path.clone(),
            materialize_sha: binding.materialize_sha.clone(),
            analysis_sha: binding.analysis_sha.clone(),
            analysis_contract: binding.analysis_contract.clone(),
            vector_binding: binding.vector_binding.clone(),
            index_contract: binding.index_contract.clone(),
        }
        .encode_to_vec();
        let value = Self {
            sha256: crate::sha256::hex_digest(&protobuf),
            protobuf,
        };
        value.decode()?;
        Ok(value)
    }

    pub fn decode(&self) -> Result<StoredBinding, String> {
        if self.sha256 != crate::sha256::hex_digest(&self.protobuf) {
            return Err("segment binding checksum mismatch".into());
        }
        let value = crate::pb::wal::LoggedBinding::decode(self.protobuf.as_slice())
            .map_err(|e| format!("invalid segment binding protobuf: {e}"))?;
        if value.encode_to_vec() != self.protobuf
            || value.plan_fingerprint.is_empty()
            || value.body_path.is_empty()
        {
            return Err("segment binding must be canonical with a plan and body".into());
        }
        crate::mapped_analysis::decode_contract(
            &value.analysis_sha,
            &value.analysis_contract,
            &value.body_path,
        )?;
        crate::mapped_vector::decode(&value.vector_binding, &value.plan_fingerprint)
            .map_err(|e| e.to_string())?;
        crate::index_contract::validate_binding(
            &value.index_contract,
            &value.plan_fingerprint,
            &value.vector_binding,
        )
        .map_err(|e| e.to_string())?;
        Ok(StoredBinding {
            plan_fingerprint: value.plan_fingerprint,
            body_path: value.body_path,
            materialize_sha: value.materialize_sha,
            analysis_sha: value.analysis_sha,
            analysis_contract: value.analysis_contract,
            vector_binding: value.vector_binding,
            index_contract: value.index_contract,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentSetManifest {
    pub format: u32,
    /// Generation-level metadata remains present even when no segment has rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<SegmentBinding>,
    pub epoch: u64,
    pub segments: Vec<SegmentMetadata>,
    /// The integer column a partitioned compaction ordered this set by
    /// (docs/immutable-segments.md "Partitioned layout"); absent for the
    /// bucket layout. A later seal appends an unordered tail segment
    /// and leaves the key in place: the ordered segments keep their
    /// ranges, and the next partitioned compaction folds the tail in.
    #[serde(default)]
    pub partition_key: Option<String>,
}

impl Default for SegmentSetManifest {
    fn default() -> Self {
        Self {
            format: BASE_SET_FORMAT,
            binding: None,
            epoch: 0,
            segments: Vec::new(),
            partition_key: None,
        }
    }
}

impl SegmentSetManifest {
    pub fn with_binding(mut self, binding: Option<&StoredBinding>) -> Result<Self, String> {
        self.binding = binding.map(SegmentBinding::encode).transpose()?;
        self.format = if self.binding.is_some() {
            SET_FORMAT
        } else {
            BASE_SET_FORMAT
        };
        Ok(self)
    }
}

pub struct SegmentSource<'a> {
    pub segment_id: &'a str,
    pub generation: u64,
    pub base_label: u64,
    pub backend_kind: &'a str,
    /// `None` seals a documents-only segment: no vector rows, and the
    /// exact rows must be absent too.
    pub vector_path: Option<&'a Path>,
    pub exact_vector_path: Option<&'a Path>,
    pub bm25_path: &'a Path,
    pub live_docs_path: &'a Path,
    /// The integer column a partitioned compaction ordered this segment
    /// by (docs/immutable-segments.md "Partitioned layout"): the sealed
    /// summary then records the segment's value range as its partition.
    /// `None` for a tail seal and for the bucket layout.
    pub partition_column: Option<&'a str>,
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

struct OpenedSegment {
    metadata: SegmentMetadata,
    /// `None` for a documents-only segment (an empty `vector` artifact).
    vector: Option<Arc<VectorIndex>>,
    exact: Option<Arc<ExactVectorStore>>,
    bm25: Arc<Bm25Reader>,
    live_docs: LiveDocs,
}

pub struct OpenedSegmentSet {
    root: PathBuf,
    binding: Option<StoredBinding>,
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

struct NoColumns;

impl crate::scorefn::NumericRead for NoColumns {
    fn uint_value(&self, _ii: usize, _doc_id: u32) -> Option<u64> {
        None
    }
    fn value(&self, _ni: usize, _doc_id: u32) -> Option<f64> {
        None
    }
    fn map_value(&self, _column: usize, _key_ord: u32, _doc_id: u32) -> Option<f64> {
        None
    }
    fn map_int_value(&self, _: usize, _: u32, _: u32) -> Option<i64> {
        None
    }
    fn map_uint_value(&self, _: usize, _: u32, _: u32) -> Option<u64> {
        None
    }
    fn int_value(&self, _ii: usize, _doc_id: u32) -> Option<i64> {
        None
    }
    fn geo_value(&self, _gi: usize, _doc_id: u32) -> Option<(f64, f64)> {
        None
    }
    fn facet_ord(&self, _fi: usize, _doc_id: u32) -> Option<u32> {
        None
    }
    fn map_facet_value_ord(&self, _ci: usize, _key_ord: u32, _doc_id: u32) -> Option<u32> {
        None
    }
}

/// How a sealed segment's vector image is served: mapped from its file
/// (the default; `docs/mmap-vectors.md`), or loaded into memory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum VectorLoad {
    #[default]
    Mapped,
    Heap,
}

impl OpenedSegmentSet {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, String> {
        Self::open_with(root, VectorLoad::default())
    }

    pub fn open_with(root: impl Into<PathBuf>, load: VectorLoad) -> Result<Self, String> {
        let root = root.into();
        let path = root.join(SET_FILE);
        let manifest = if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|error| format!("read segment set {}: {error}", path.display()))?;
            serde_json::from_slice(&bytes)
                .map_err(|error| format!("parse segment set {}: {error}", path.display()))?
        } else {
            SegmentSetManifest::default()
        };
        Self::open_manifest(root, manifest, load)
    }

    fn open_manifest(
        root: PathBuf,
        manifest: SegmentSetManifest,
        load: VectorLoad,
    ) -> Result<Self, String> {
        Self::open_manifest_reusing(root, manifest, load, None)
    }

    /// Open `manifest`, taking every segment whose metadata is unchanged
    /// from `reuse` as it is (open, verified, its images prepared). A bitmap-only
    /// change shares those images and verifies the new bitmap. Other changes
    /// open and verify the new images. A publish adds one segment
    /// to a set of hundreds; hashing every artifact of every segment on
    /// each publish made a long ingest quadratic in its seal count
    /// (measured 2026-09-04: a Pi's rate fell from 1.2k to 340 rows/s
    /// over 56 seals). A node open passes no `reuse` and verifies all.
    fn open_manifest_reusing(
        root: PathBuf,
        manifest: SegmentSetManifest,
        load: VectorLoad,
        reuse: Option<&OpenedSegmentSet>,
    ) -> Result<Self, String> {
        validate_manifest(&manifest)?;
        let mut segments = Vec::with_capacity(manifest.segments.len());
        let mut scoring_fingerprint: Option<&str> = None;
        let mut analysis_fingerprints: Option<&[u64]> = None;
        for metadata in &manifest.segments {
            if let Some(held) = reuse.and_then(|set| {
                set.segments
                    .iter()
                    .find(|segment| same_segment_images(&segment.metadata, metadata))
            }) {
                if held.vector.is_some() {
                    match scoring_fingerprint {
                        Some(known) if known != metadata.scoring_fingerprint => {
                            return Err("segment set mixes vector scoring fingerprints".into())
                        }
                        None => scoring_fingerprint = Some(&metadata.scoring_fingerprint),
                        _ => {}
                    }
                }
                match analysis_fingerprints {
                    Some(known) if known != metadata.analysis_fingerprints => {
                        return Err("segment set mixes analysis fingerprints".into())
                    }
                    None => analysis_fingerprints = Some(&metadata.analysis_fingerprints),
                    _ => {}
                }
                if held.metadata == *metadata {
                    segments.push(Arc::clone(held));
                } else {
                    let directory = root.join("segments").join(&metadata.segment_id);
                    verify_artifact(&directory, &metadata.live_docs)?;
                    let live_docs = LiveDocs::open(&directory.join(&metadata.live_docs.file))
                        .map_err(|error| format!("open retired segment bitmap: {error}"))?;
                    if live_docs.persisted_rows() != metadata.rows
                        || metadata.rows.checked_sub(live_docs.deleted_count())
                            != Some(metadata.live_rows)
                    {
                        return Err(
                            "retired segment bitmap differs from manifest row counts".into()
                        );
                    }
                    segments.push(Arc::new(OpenedSegment {
                        metadata: metadata.clone(),
                        vector: held.vector.clone(),
                        exact: held.exact.clone(),
                        bm25: Arc::clone(&held.bm25),
                        live_docs,
                    }));
                }
                continue;
            }
            let directory = root.join("segments").join(&metadata.segment_id);
            let has_vectors = !metadata.vector.file.is_empty();
            if has_vectors {
                verify_artifact(&directory, &metadata.vector)?;
                verify_artifact(&directory, &metadata.exact_vectors)?;
            } else if !metadata.exact_vectors.file.is_empty() {
                return Err(format!(
                    "segment {:?} has exact rows but no vector artifact",
                    metadata.segment_id
                ));
            }
            verify_artifact(&directory, &metadata.bm25)?;
            verify_artifact(&directory, &metadata.live_docs)?;
            let vector = if has_vectors {
                let image = directory.join(&metadata.vector.file);
                let mut vector = match load {
                    VectorLoad::Mapped => VectorIndex::load_mapped(&metadata.backend_kind, &image),
                    VectorLoad::Heap => VectorIndex::load(&metadata.backend_kind, &image),
                }
                .map_err(|error| {
                    format!("open vector segment {:?}: {error}", metadata.segment_id)
                })?;
                vector.prepare().map_err(|error| {
                    format!("prepare vector segment {:?}: {error}", metadata.segment_id)
                })?;
                Some(vector)
            } else {
                None
            };
            let exact = if has_vectors {
                let exact = ExactVectorStore::open(&directory.join(&metadata.exact_vectors.file))
                    .map_err(|error| {
                    format!("open exact segment {:?}: {error}", metadata.segment_id)
                })?;
                if exact.dim() != vector.as_ref().and_then(VectorIndex::dim_opt) {
                    return Err(format!(
                        "segment {:?} exact-vector dimension differs from its provider",
                        metadata.segment_id
                    ));
                }
                Some(Arc::new(exact))
            } else {
                None
            };
            let exact_rows = exact.as_ref().map_or(0, |store| store.len());
            let bm25 = Bm25Reader::open(&directory.join(&metadata.bm25.file))
                .map_err(|error| format!("open BM25 segment {:?}: {error}", metadata.segment_id))?;
            let live_docs =
                LiveDocs::open(&directory.join(&metadata.live_docs.file)).map_err(|error| {
                    format!("open live-doc segment {:?}: {error}", metadata.segment_id)
                })?;
            let rows = usize::try_from(metadata.rows)
                .map_err(|_| format!("segment {:?} rows do not fit usize", metadata.segment_id))?;
            // Row alignment (docs/immutable-segments.md): every artifact
            // a segment has covers the same rows; a documents-only
            // segment has no vector rows at all.
            let vector_rows = vector.as_ref().map_or(0, VectorIndex::len);
            let bm25_rows = bm25.next_doc_id() as usize;
            let aligned = |n: usize| n == 0 || n == rows;
            if rows == 0
                || !aligned(vector_rows)
                || (has_vectors && vector_rows != rows)
                || !aligned(exact_rows)
                || exact_rows != vector_rows
                || !aligned(bm25_rows)
                || (vector_rows == 0 && bm25_rows == 0)
                || live_docs.persisted_rows() != metadata.rows
            {
                return Err(format!(
                    "segment {:?} is not aligned: manifest rows {}, vector {}, exact {}, BM25 {}, live-doc {}",
                    metadata.segment_id,
                    metadata.rows,
                    vector_rows,
                    exact_rows,
                    bm25_rows,
                    live_docs.persisted_rows()
                ));
            }
            let live_rows = metadata
                .rows
                .checked_sub(live_docs.deleted_count())
                .ok_or_else(|| {
                    format!(
                        "segment {:?} deletes exceed its physical rows",
                        metadata.segment_id
                    )
                })?;
            if metadata.live_rows != live_rows
                || metadata.document_count != Bm25Index::doc_count(&bm25)
                || metadata.total_document_length != Bm25Index::total_doc_length(&bm25)
            {
                return Err(format!(
                    "segment {:?} manifest statistics differ from its artifacts",
                    metadata.segment_id
                ));
            }
            if let Some(vector) = &vector {
                let descriptor = vector.descriptor();
                if descriptor.scoring_fingerprint != metadata.scoring_fingerprint
                    || descriptor.score_direction != ScoreDirection::HigherIsBetter
                    || descriptor.quality_contract != QualityContract::ExhaustiveQuantized
                {
                    return Err(format!(
                        "segment {:?} vector scoring contract differs from its aligned manifest or is not exhaustive",
                        metadata.segment_id
                    ));
                }
                match scoring_fingerprint {
                    Some(held) if held != metadata.scoring_fingerprint => {
                        return Err("segment set mixes vector scoring fingerprints".into())
                    }
                    None => scoring_fingerprint = Some(&metadata.scoring_fingerprint),
                    _ => {}
                }
            }
            match analysis_fingerprints {
                Some(held) if held != metadata.analysis_fingerprints => {
                    return Err("segment set mixes analysis fingerprints".into())
                }
                None => analysis_fingerprints = Some(&metadata.analysis_fingerprints),
                _ => {}
            }
            segments.push(Arc::new(OpenedSegment {
                metadata: metadata.clone(),
                vector: vector.map(Arc::new),
                exact,
                bm25: Arc::new(bm25),
                live_docs,
            }));
        }
        let declared = manifest
            .binding
            .as_ref()
            .map(SegmentBinding::decode)
            .transpose()?;
        let mut held = declared.clone().map(Some);
        for segment in &segments {
            let next = segment.bm25.binding().cloned();
            match &held {
                Some(expected) if expected != &next => return Err(
                    "segment set mixes mapped bindings or disagrees with its generation binding"
                        .into(),
                ),
                None => held = Some(next),
                _ => {}
            }
        }
        Ok(Self {
            root,
            binding: declared.or_else(|| held.flatten()),
            manifest,
            segments,
        })
    }

    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    /// Whether segment `i` is the same opened segment as `other`'s
    /// segment `j` (shared across a publish), for tests.
    pub fn shares_segment(&self, i: usize, other: &OpenedSegmentSet, j: usize) -> bool {
        Arc::ptr_eq(&self.segments[i], &other.segments[j])
    }

    /// The manifest this set was opened from. Its `epoch` is the epoch at
    /// open; [`Self::epoch`] is the published one.
    pub fn manifest(&self) -> &SegmentSetManifest {
        &self.manifest
    }

    /// The set's published epoch.
    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    /// The manifest as it is published: `manifest()` with the current
    /// epoch.
    pub fn published_manifest(&self) -> SegmentSetManifest {
        self.manifest.clone()
    }

    /// The catalog root this set was opened from.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Number of segments in the set, in ascending base order.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn metadata(&self, i: usize) -> &SegmentMetadata {
        &self.segments[i].metadata
    }

    pub fn bm25(&self, i: usize) -> &Bm25Reader {
        &self.segments[i].bm25
    }

    /// Segment `i`'s vector image, `None` for a documents-only segment.
    pub fn vector(&self, i: usize) -> Option<&VectorIndex> {
        self.segments[i].vector.as_deref()
    }

    /// The immutable FP32 image opened and verified with this segment snapshot.
    /// Documents-only segments have no exact rows. Bitmap-only publications
    /// share this image while retaining their own tombstone overlays.
    pub fn exact_vectors(&self, i: usize) -> Option<&Arc<ExactVectorStore>> {
        self.segments[i].exact.as_ref()
    }

    /// Add this snapshot's committed tombstones to a generation-wide serving
    /// overlay. Preserve newer overlay deletes and every previously held view.
    /// Work visits bitmap words and set bits, not each physical document.
    pub fn merge_tombstones(&self, live: &mut LiveDocs) -> Result<(), String> {
        for segment in &self.segments {
            usize::try_from(segment.metadata.end_label_exclusive()?)
                .map_err(|_| "segment tombstone row range does not fit usize")?;
        }
        for segment in &self.segments {
            let Some(words) = segment.live_docs.words() else {
                continue;
            };
            let base = segment.metadata.base_label as usize;
            for (ordinal, &word) in words.iter().enumerate() {
                let mut remaining = word;
                while remaining != 0 {
                    let bit = remaining.trailing_zeros() as usize;
                    live.delete(base + ordinal * 64 + bit);
                    remaining &= remaining - 1;
                }
            }
        }
        Ok(())
    }

    pub fn live_docs(&self, i: usize) -> &LiveDocs {
        &self.segments[i].live_docs
    }

    pub fn global_bm25_stats(&self, terms: &[String]) -> Result<CorpusStats, String> {
        let mut stats = CorpusStats {
            doc_count: 0,
            total_doc_length: 0,
            dfs: vec![0; terms.len()],
        };
        for segment in &self.segments {
            let (document_count, total_document_length, dfs) =
                live_stats_share(segment.bm25.as_ref(), &segment.live_docs, terms)?;
            stats.doc_count = stats
                .doc_count
                .checked_add(document_count)
                .ok_or("global BM25 document count overflow")?;
            stats.total_doc_length = stats
                .total_doc_length
                .checked_add(total_document_length)
                .ok_or("global BM25 length overflow")?;
            for (df, share) in stats.dfs.iter_mut().zip(dfs) {
                *df = df
                    .checked_add(share)
                    .ok_or("global BM25 document frequency overflow")?;
            }
        }
        Ok(stats)
    }

    pub fn search_bm25(
        &self,
        terms: &[String],
        params: Bm25Params,
        k: usize,
    ) -> Result<Vec<SegmentHit>, String> {
        if k == 0 || terms.is_empty() {
            return Ok(Vec::new());
        }
        let stats = self.global_bm25_stats(terms)?;
        let columns = NoColumns;
        let mut all = Vec::new();
        for segment in &self.segments {
            let filter = crate::filter::DocFilter {
                deleted: segment.live_docs.words(),
                ..Default::default()
            };
            let mut pruning = bm25::PruneStats::default();
            let hits = bm25::top_k_pruned_chained_filtered_stats(
                segment.bm25.as_ref(),
                terms,
                &stats,
                params,
                k,
                f64::NEG_INFINITY,
                None,
                Some((&filter, &columns)),
                &mut pruning,
            );
            for hit in hits {
                all.push(SegmentHit {
                    doc_id: segment
                        .metadata
                        .base_label
                        .checked_add(u64::from(hit.doc_id))
                        .ok_or("segment BM25 id overflow")?,
                    score: hit.score as f32,
                    segment_id: segment.metadata.segment_id.clone(),
                });
            }
        }
        sort_hits(&mut all, k);
        Ok(all)
    }

    pub fn search_vector(&self, vector: &[f32], k: usize) -> Result<Vec<SegmentHit>, String> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut all = Vec::new();
        for segment in &self.segments {
            let Some(image) = segment.vector.as_ref() else {
                continue;
            };
            let rows = image.len();
            if rows == 0 {
                continue;
            }
            let allow: Vec<bool> = (0..rows)
                .map(|row| !segment.live_docs.is_deleted(row))
                .collect();
            let result = image
                .try_search(
                    vector,
                    k.min(rows),
                    VectorSearchOptions::new().with_allowlist(&allow),
                )
                .map_err(|error| {
                    format!(
                        "search vector segment {:?}: {error}",
                        segment.metadata.segment_id
                    )
                })?;
            if result.query_count != 1 {
                return Err(format!(
                    "vector segment {:?} returned {} result groups for one query",
                    segment.metadata.segment_id, result.query_count
                ));
            }
            for (&slot, &score) in result
                .slots_for_query(0)
                .iter()
                .zip(result.scores_for_query(0))
            {
                if slot < 0 || !score.is_finite() {
                    continue;
                }
                let slot = u64::try_from(slot).map_err(|_| "negative vector slot")?;
                all.push(SegmentHit {
                    doc_id: segment
                        .metadata
                        .base_label
                        .checked_add(slot)
                        .ok_or("segment vector id overflow")?,
                    score,
                    segment_id: segment.metadata.segment_id.clone(),
                });
            }
        }
        sort_hits(&mut all, k);
        Ok(all)
    }
}

fn live_stats_share(
    index: &dyn Bm25Index,
    live_docs: &LiveDocs,
    terms: &[String],
) -> Result<(u64, u64, Vec<u32>), String> {
    if !live_docs.has_deletes() {
        return Ok((
            index.doc_count(),
            index.total_doc_length(),
            terms.iter().map(|term| index.df(term)).collect(),
        ));
    }
    let rows = u32::try_from(live_docs.persisted_rows())
        .map_err(|_| "segment BM25 row count exceeds u32")?;
    let mut deleted_documents = 0u64;
    let mut deleted_length = 0u64;
    for slot in 0..rows {
        if live_docs.is_deleted(slot as usize) && index.text(slot).is_some() {
            deleted_documents = deleted_documents
                .checked_add(1)
                .ok_or("live BM25 document count overflow")?;
            deleted_length = deleted_length
                .checked_add(u64::from(index.doc_length(slot)))
                .ok_or("live BM25 length overflow")?;
        }
    }
    let mut dfs = Vec::with_capacity(terms.len());
    for term in terms {
        let mut live_df = 0u64;
        index.for_each_doc_tf(term, &mut |doc, _| {
            if !live_docs.is_deleted(doc as usize) {
                live_df += 1;
            }
        });
        dfs.push(u32::try_from(live_df).map_err(|_| "live BM25 document frequency exceeds u32")?);
    }
    Ok((
        index
            .doc_count()
            .checked_sub(deleted_documents)
            .ok_or("deleted BM25 document count exceeds total")?,
        index
            .total_doc_length()
            .checked_sub(deleted_length)
            .ok_or("deleted BM25 length exceeds total")?,
        dfs,
    ))
}

fn sort_hits(hits: &mut Vec<SegmentHit>, k: usize) {
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    hits.truncate(k);
}

#[derive(Clone)]
pub struct SegmentCatalog {
    root: PathBuf,
    current: Arc<RwLock<Arc<OpenedSegmentSet>>>,
    update: Arc<Mutex<()>>,
    pending_publication: Arc<Mutex<Option<SegmentSetManifest>>>,
    /// How every snapshot this catalog publishes serves vector images.
    load: VectorLoad,
}

impl SegmentCatalog {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, String> {
        Self::open_with(root, VectorLoad::default())
    }

    pub fn open_with(root: impl Into<PathBuf>, load: VectorLoad) -> Result<Self, String> {
        let root = root.into();
        std::fs::create_dir_all(root.join("segments"))
            .map_err(|error| format!("mkdir segment root {}: {error}", root.display()))?;
        let current = OpenedSegmentSet::open_with(root.clone(), load)?;
        Ok(Self {
            root,
            current: Arc::new(RwLock::new(Arc::new(current))),
            update: Arc::new(Mutex::new(())),
            pending_publication: Arc::new(Mutex::new(None)),
            load,
        })
    }

    pub fn snapshot(&self) -> Arc<OpenedSegmentSet> {
        Arc::clone(&self.current.read().expect("segment catalog lock poisoned"))
    }

    /// A catalog over `root` whose current set is `manifest` — segments
    /// already staged under `root/segments/` by [`stage_segments`] — with
    /// NOTHING written to `segments.json`: the shadow of an online
    /// compaction (`docs/mutations.md`), which serves and tails the log
    /// in memory until [`Self::commit_current`] publishes it at cutover.
    /// The live catalog over the same root is untouched until then.
    pub fn open_staged(
        root: impl Into<PathBuf>,
        manifest: SegmentSetManifest,
        load: VectorLoad,
    ) -> Result<Self, String> {
        let root = root.into();
        let current = OpenedSegmentSet::open_manifest(root.clone(), manifest, load)?;
        Ok(Self {
            root,
            current: Arc::new(RwLock::new(Arc::new(current))),
            update: Arc::new(Mutex::new(())),
            pending_publication: Arc::new(Mutex::new(None)),
            load,
        })
    }

    /// Publish the current set as it stands, at `epoch`, by writing
    /// `segments.json` atomically: the manifest swap of a compaction
    /// cutover. `epoch` must exceed the set's own, so the on-disk epoch
    /// sequence stays monotone across the swap.
    pub fn commit_current(&self, epoch: u64) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        let current = self.snapshot();
        if epoch <= current.epoch() {
            return Err(format!(
                "compaction commit epoch {epoch} is not newer than the staged set's {}",
                current.epoch()
            ));
        }
        let mut manifest = current.published_manifest();
        manifest.epoch = epoch;
        self.publish(manifest)
    }

    /// The catalog root's set manifest as written on disk, `None` when the
    /// root has none yet.
    pub fn read_manifest(root: &Path) -> Result<Option<SegmentSetManifest>, String> {
        let path = root.join(SET_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("read segment set {}: {error}", path.display()))?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("parse segment set {}: {error}", path.display()))
    }

    /// The set manifest file inside a catalog root.
    pub fn manifest_path(root: &Path) -> PathBuf {
        root.join(SET_FILE)
    }

    /// The directory of segment `id` under a catalog root.
    pub fn segment_dir(root: &Path, id: &str) -> PathBuf {
        root.join("segments").join(id)
    }

    /// Record the integer column the catalog's sealed segments are cut
    /// by (`docs/replay-from-segments.md`, "Cutting the spill"): the
    /// same atomic manifest publication a compaction uses, so a node
    /// opening the catalog reports the partition key a partitioned
    /// compaction would have left. `None` returns the catalog to the
    /// bucket layout's declaration.
    pub fn publish_partition_key(&self, key: Option<String>) -> Result<(), String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        let current = self.snapshot();
        let mut manifest = current
            .published_manifest()
            .with_binding(current.binding())?;
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        manifest.partition_key = key;
        self.publish(manifest).map(|_| ())
    }

    /// Pin metadata by the same atomic manifest publication used for rows.
    /// No independently mutable binding file can outlive or lag its generation.
    pub fn publish_binding(
        &self,
        binding: &StoredBinding,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        let current = self.snapshot();
        match current.binding() {
            Some(held) if held != binding => {
                return Err("segment generation is bound to another mapping".into())
            }
            None if !current.is_empty() => {
                return Err("cannot bind a populated unbound segment set".into())
            }
            _ => {}
        }
        if current.manifest().binding.is_some() {
            return Ok(current);
        }
        let mut manifest = current.published_manifest().with_binding(Some(binding))?;
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        self.publish(manifest)
    }

    pub fn append(&self, source: SegmentSource<'_>) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        validate_segment_id(source.segment_id)?;
        if Self::segment_dir(&self.root, source.segment_id).exists() {
            let manifest = Self::read_manifest(&self.root)?.ok_or_else(|| {
                "existing staged segment is not published; recovery is required".to_string()
            })?;
            let held = manifest
                .segments
                .iter()
                .find(|segment| segment.segment_id == source.segment_id)
                .ok_or_else(|| {
                    "existing staged segment is not published; recovery is required".to_string()
                })?;
            verify_retry_source(&self.root, &source, held)?;
            // Reaffirm durability even if the first attempt failed after rename.
            // Opening reuses unchanged segments and validates any newly adopted ones.
            return self.publish(manifest);
        }
        let metadata = stage_segment(&self.root, source)?;
        let current = self.snapshot();
        let mut manifest = current.published_manifest();
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        manifest.segments.push(metadata.clone());
        manifest.segments.sort_by_key(|segment| segment.base_label);
        let published = self.publish(manifest);
        if published.is_err() {
            cleanup_staged(&self.root, std::slice::from_ref(&metadata));
        }
        published
    }

    /// Atomically replace compacted input segments with one output.
    pub fn replace_for_compaction(
        &self,
        input_ids: &[String],
        source: SegmentSource<'_>,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        self.replace_many_for_compaction(input_ids, vec![source], None)
    }

    /// Atomically replace compacted inputs with dense immutable outputs.
    /// No output is needed when the inputs contain no live rows. Stable product
    /// identity remains in lineage, so outputs may use
    /// fresh non-overlapping positional ranges. Every output belongs to one
    /// newer generation and their combined row count must equal the inputs'
    /// live row count.
    pub fn replace_many_for_compaction(
        &self,
        input_ids: &[String],
        sources: Vec<SegmentSource<'_>>,
        partition_key: Option<String>,
    ) -> Result<Arc<OpenedSegmentSet>, String> {
        let _guard = self
            .update
            .lock()
            .map_err(|_| "segment update lock poisoned".to_string())?;
        if input_ids.is_empty() {
            return Err("compaction needs at least one input segment".into());
        }
        let current = self.snapshot();
        let wanted: BTreeSet<&str> = input_ids.iter().map(String::as_str).collect();
        let inputs: Vec<&SegmentMetadata> = current
            .manifest
            .segments
            .iter()
            .filter(|segment| wanted.contains(segment.segment_id.as_str()))
            .collect();
        if inputs.len() != wanted.len() {
            return Err("compaction names an unknown or duplicate input segment".into());
        }
        let expected_rows = inputs.iter().try_fold(0u64, |total, segment| {
            total
                .checked_add(segment.live_rows)
                .ok_or("compaction live-row count overflow")
        })?;
        let newest_generation = inputs
            .iter()
            .map(|segment| segment.generation)
            .max()
            .unwrap();
        let output_generation = sources.first().map(|source| source.generation);
        if let Some(generation) = output_generation {
            if generation <= newest_generation {
                return Err(format!(
                    "compaction output generation {generation} is not newer than input generation {newest_generation}"
                ));
            }
        }
        if sources
            .iter()
            .any(|source| Some(source.generation) != output_generation)
        {
            return Err("compaction outputs do not share one generation".into());
        }
        let mut outputs = Vec::with_capacity(sources.len());
        for source in sources {
            match stage_segment(&self.root, source) {
                Ok(metadata) => outputs.push(metadata),
                Err(error) => {
                    cleanup_staged(&self.root, &outputs);
                    return Err(error);
                }
            }
        }
        let output_rows = outputs.iter().try_fold(0u64, |total, metadata| {
            if metadata.live_rows != metadata.rows {
                return Err(format!(
                    "compaction output {:?} is not dense: {}/{} live/physical rows",
                    metadata.segment_id, metadata.live_rows, metadata.rows
                ));
            }
            total
                .checked_add(metadata.rows)
                .ok_or_else(|| "compaction output row count overflow".to_string())
        });
        let output_rows = match output_rows {
            Ok(rows) => rows,
            Err(error) => {
                cleanup_staged(&self.root, &outputs);
                return Err(error);
            }
        };
        if output_rows != expected_rows {
            cleanup_staged(&self.root, &outputs);
            return Err(format!(
                "compaction outputs have {output_rows} rows, expected {expected_rows} dense live rows"
            ));
        }
        let mut manifest = current
            .published_manifest()
            .with_binding(current.binding())?;
        manifest.epoch = manifest
            .epoch
            .checked_add(1)
            .ok_or("segment catalog epoch overflow")?;
        manifest
            .segments
            .retain(|segment| !wanted.contains(segment.segment_id.as_str()));
        manifest.segments.extend(outputs.iter().cloned());
        manifest.segments.sort_by_key(|segment| segment.base_label);
        manifest.partition_key = partition_key;
        let published = self.publish(manifest);
        if published.is_err() {
            cleanup_staged(&self.root, &outputs);
        }
        published
    }

    /// Rebuild full-history WAL generations into bounded immutable segments
    /// and publish them as one atomic compaction. This blocking method is
    /// intended for a background worker. The work directory must be empty and
    /// remains available for audit or retry after publication.
    #[allow(clippy::too_many_arguments)]
    pub fn compact_wal_generations(
        &self,
        input_ids: &[String],
        generations: &[PathBuf],
        work_dir: &Path,
        backend_kind: &str,
        slot_base: u64,
        slot_capacity: u64,
        bm25_fields: Option<&[String]>,
        partition: Option<crate::reshard::PartitionSpec<'_>>,
        analyze: &mut crate::reshard::Analyzer,
    ) -> Result<WalCompactionResult, String> {
        if work_dir.exists()
            && std::fs::read_dir(work_dir)
                .map_err(|error| {
                    format!("read compaction work dir {}: {error}", work_dir.display())
                })?
                .next()
                .is_some()
        {
            return Err(format!(
                "compaction work directory {} is not empty",
                work_dir.display()
            ));
        }
        let output = match partition {
            None => crate::reshard::split_logs_segmented(
                generations,
                1,
                work_dir,
                slot_base,
                slot_capacity,
                false,
                bm25_fields,
                analyze,
            )?,
            Some(spec) => {
                let [gen] = generations else {
                    return Err(format!(
                        "a partitioned compaction takes one WAL generation, not {}",
                        generations.len()
                    ));
                };
                let mut sink = |_row: crate::reshard::CompactedRow<'_>| Ok(());
                // The outputs keep the declaration the log's manifest
                // records (docs/derived-columns.md).
                let derived = crate::reshard::manifest_derived(
                    &crate::wal::read_manifest(gen)
                        .map_err(|e| format!("read manifest of {}: {e}", gen.display()))?,
                )?;
                let build = crate::reshard::compact_log_partitioned(
                    gen,
                    u64::MAX,
                    work_dir,
                    spec,
                    bm25_fields,
                    None,
                    None,
                    derived.as_ref(),
                    analyze,
                    &mut sink,
                )?;
                let images: Vec<crate::reshard::ChildImage> = build
                    .images
                    .into_iter()
                    .map(|mut image| {
                        image.slot_offset += slot_base;
                        image
                    })
                    .collect();
                crate::reshard::SegmentedReshardOutput::from_images(build.generation, images)
            }
        };
        if output.segments.is_empty() {
            return Err("compaction produced no live segments".into());
        }
        let ids = output
            .segments
            .iter()
            .map(|segment| {
                format!(
                    "g{}-s{}-p{}",
                    output.generation, segment.logical_shard, segment.segment_ordinal
                )
            })
            .collect::<Vec<_>>();
        let mut live_paths = Vec::with_capacity(output.segments.len());
        for segment in &output.segments {
            if segment.image.num_vectors != segment.image.num_documents {
                return Err(format!(
                    "compaction segment {} has {} vectors but {} documents; aligned publication requires one document per vector",
                    segment.segment_ordinal,
                    segment.image.num_vectors,
                    segment.image.num_documents
                ));
            }
            if segment.image.bm25_path.is_none() {
                return Err(format!(
                    "compaction segment {} has no BM25 image",
                    segment.segment_ordinal
                ));
            }
            let path = crate::node::live_docs_sidecar_path(&segment.image.vector_path);
            LiveDocs::default()
                .write(&path, segment.image.num_vectors)
                .map_err(|error| {
                    format!("write compacted live docs {}: {error}", path.display())
                })?;
            live_paths.push(path);
        }
        let sources = output
            .segments
            .iter()
            .zip(ids.iter())
            .zip(live_paths.iter())
            .map(|((segment, id), live_docs_path)| SegmentSource {
                segment_id: id,
                generation: output.generation,
                base_label: segment.image.slot_offset,
                backend_kind,
                vector_path: Some(&segment.image.vector_path),
                exact_vector_path: Some(&segment.image.exact_vector_path),
                bm25_path: segment.image.bm25_path.as_deref().expect("validated above"),
                live_docs_path,
                partition_column: partition.map(|spec| spec.column),
            })
            .collect();
        let snapshot = self.replace_many_for_compaction(
            input_ids,
            sources,
            partition.map(|spec| spec.column.to_string()),
        )?;
        Ok(WalCompactionResult { snapshot, output })
    }

    fn publish(&self, manifest: SegmentSetManifest) -> Result<Arc<OpenedSegmentSet>, String> {
        let mut pending = self
            .pending_publication
            .lock()
            .map_err(|_| "segment publication lock poisoned".to_string())?;
        if pending.as_ref().is_some_and(|held| held != &manifest) {
            return Err(
                "segment publication is uncertain; retry the same manifest or reopen for recovery"
                    .into(),
            );
        }
        let current = self.snapshot();
        let opened = Arc::new(OpenedSegmentSet::open_manifest_reusing(
            self.root.clone(),
            manifest.clone(),
            self.load,
            Some(&current),
        )?);
        if let Err(error) = write_json_atomic(&self.root.join(SET_FILE), &manifest) {
            *pending = Some(manifest);
            return Err(error);
        }
        *self
            .current
            .write()
            .map_err(|_| "segment catalog lock poisoned".to_string())? = Arc::clone(&opened);
        *pending = None;
        Ok(opened)
    }
}

/// Stage `sources` under `root/segments/` — copied, hashed, opened,
/// verified, fsynced, and renamed into their final directories — WITHOUT
/// publishing them: they join no manifest until a caller commits one.
/// A compaction stages its dense outputs this way while the live set
/// keeps serving (`docs/mutations.md`). On any failure every directory
/// this call created is removed. A source whose directory already exists
/// refuses by name rather than adopting it.
pub fn stage_segments(
    root: &Path,
    sources: Vec<SegmentSource<'_>>,
) -> Result<Vec<SegmentMetadata>, String> {
    std::fs::create_dir_all(root.join("segments"))
        .map_err(|error| format!("mkdir segment root {}: {error}", root.display()))?;
    let mut staged = Vec::with_capacity(sources.len());
    for source in sources {
        match stage_segment(root, source) {
            Ok(metadata) => staged.push(metadata),
            Err(error) => {
                cleanup_staged(root, &staged);
                return Err(error);
            }
        }
    }
    Ok(staged)
}

/// Write a set manifest to `path` atomically: the copy a compaction
/// cutover keeps for rollback beside the live one (`docs/mutations.md`).
pub fn write_manifest_file(path: &Path, manifest: &SegmentSetManifest) -> Result<(), String> {
    write_json_atomic(path, manifest)
}

/// Remove the directories of `segments` under `root`: the staged outputs
/// of a compaction that did not commit, or the inputs one replaced.
pub fn remove_segment_dirs(root: &Path, segments: &[SegmentMetadata]) {
    cleanup_staged(root, segments)
}

fn same_segment_images(left: &SegmentMetadata, right: &SegmentMetadata) -> bool {
    if left.segment_id != right.segment_id {
        return false;
    }
    let mut normalized = left.clone();
    normalized.live_docs = right.live_docs.clone();
    normalized.live_rows = right.live_rows;
    normalized == *right
}

fn cleanup_staged(root: &Path, segments: &[SegmentMetadata]) {
    // A rename can succeed before directory fsync fails. Never delete files
    // named by that manifest, and retain them if the manifest cannot be read.
    let manifest = match SegmentCatalog::read_manifest(root) {
        Ok(value) => value,
        Err(_) => return,
    };
    for segment in segments {
        if manifest.as_ref().is_some_and(|m| {
            m.segments
                .iter()
                .any(|s| s.segment_id == segment.segment_id)
        }) {
            continue;
        }
        let _ = std::fs::remove_dir_all(root.join("segments").join(&segment.segment_id));
    }
}

fn verify_retry_source(
    root: &Path,
    source: &SegmentSource<'_>,
    held: &SegmentMetadata,
) -> Result<(), String> {
    let backend = if source.vector_path.is_some() {
        source.backend_kind
    } else {
        ""
    };
    if source.generation != held.generation
        || source.base_label != held.base_label
        || backend != held.backend_kind
    {
        return Err("segment retry differs from the published generation, range or backend".into());
    }
    let expected_partition = source.partition_column.and_then(|column| {
        held.summary
            .as_ref()?
            .int_columns
            .iter()
            .find(|summary| summary.name == column && summary.present > 0)
            .map(|summary| PartitionRange {
                column: column.into(),
                lo: summary.min,
                hi: summary.max,
            })
    });
    if held.summary.as_ref().and_then(|s| s.partition.clone()) != expected_partition {
        return Err("segment retry differs from the published partition contract".into());
    }
    let directory = SegmentCatalog::segment_dir(root, &held.segment_id);
    for (source, artifact) in [
        (source.vector_path, &held.vector),
        (source.exact_vector_path, &held.exact_vectors),
        (Some(source.bm25_path), &held.bm25),
        (Some(source.live_docs_path), &held.live_docs),
    ] {
        match source {
            Some(path) if !artifact.file.is_empty() => {
                let (bytes, sha256) = digest_file(path)?;
                if bytes != artifact.bytes || sha256 != artifact.sha256 {
                    return Err("segment retry content differs from its published artifact".into());
                }
                verify_artifact(&directory, artifact)?;
            }
            None if artifact.file.is_empty()
                && artifact.bytes == 0
                && artifact.sha256.is_empty() => {}
            _ => return Err("segment retry differs from the published artifact shape".into()),
        }
    }
    Ok(())
}

fn validate_manifest(manifest: &SegmentSetManifest) -> Result<(), String> {
    if !(BASE_SET_FORMAT..=SET_FORMAT).contains(&manifest.format)
        || (manifest.format == BASE_SET_FORMAT && manifest.binding.is_some())
        || (manifest.format == SET_FORMAT && manifest.binding.is_none())
    {
        return Err(format!(
            "segment set format {} or binding declaration is unsupported; expected format \
             {BASE_SET_FORMAT} without a binding or {SET_FORMAT} with a binding",
            manifest.format
        ));
    }
    if let Some(binding) = &manifest.binding {
        binding.decode()?;
    }
    let mut ids = BTreeSet::new();
    let mut previous_end = None;
    for segment in &manifest.segments {
        if segment.segment_id.is_empty()
            || !segment
                .segment_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(format!("invalid segment id {:?}", segment.segment_id));
        }
        if !ids.insert(&segment.segment_id) {
            return Err(format!("duplicate segment id {:?}", segment.segment_id));
        }
        if segment.rows == 0 || segment.live_rows > segment.rows {
            return Err(format!(
                "segment {:?} has invalid row counts",
                segment.segment_id
            ));
        }
        if let Some(end) = previous_end {
            if segment.base_label < end {
                return Err(format!(
                    "segment {:?} overlaps the preceding stable-id range",
                    segment.segment_id
                ));
            }
        }
        previous_end = Some(segment.end_label_exclusive()?);
        // A documents-only segment records no vector and no exact rows
        // (both absent, or both present).
        let has_vectors = !segment.vector.file.is_empty();
        if has_vectors != !segment.exact_vectors.file.is_empty() {
            return Err(format!(
                "segment {:?} records a vector image without exact rows, or the reverse",
                segment.segment_id
            ));
        }
        let mut artifacts: Vec<&SegmentArtifact> = vec![&segment.bm25, &segment.live_docs];
        if has_vectors {
            artifacts.push(&segment.vector);
            artifacts.push(&segment.exact_vectors);
        }
        for artifact in artifacts {
            validate_relative_file(&artifact.file)?;
            if artifact.sha256.len() != 64
                || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(format!("artifact {:?} has invalid SHA-256", artifact.file));
            }
        }
    }
    Ok(())
}

fn validate_relative_file(file: &str) -> Result<(), String> {
    let path = Path::new(file);
    let mut components = path.components();
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(format!(
            "segment artifact path {file:?} is not one relative file"
        ));
    }
    Ok(())
}

fn digest_file(path: &Path) -> Result<(u64, String), String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("open artifact {}: {error}", path.display()))?;
    let mut hash = crate::sha256::Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("read artifact {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or("artifact byte count overflow")?;
        hash.update(&buffer[..read]);
    }
    Ok((bytes, crate::sha256::to_hex(&hash.finalize())))
}

fn copy_artifact(source: &Path, directory: &Path, name: &str) -> Result<SegmentArtifact, String> {
    let target = directory.join(name);
    std::fs::copy(source, &target).map_err(|error| {
        format!(
            "copy segment artifact {} -> {}: {error}",
            source.display(),
            target.display()
        )
    })?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&target)
        .map_err(|error| format!("open copied artifact {}: {error}", target.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync copied artifact {}: {error}", target.display()))?;
    let (bytes, sha256) = digest_file(&target)?;
    Ok(SegmentArtifact {
        file: name.to_string(),
        bytes,
        sha256,
    })
}

fn validate_segment_id(segment_id: &str) -> Result<(), String> {
    if segment_id.is_empty()
        || !segment_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!("invalid segment id {segment_id:?}"));
    }
    Ok(())
}

fn stage_segment(root: &Path, source: SegmentSource<'_>) -> Result<SegmentMetadata, String> {
    validate_segment_id(source.segment_id)?;
    let final_dir = root.join("segments").join(source.segment_id);
    if final_dir.exists() {
        return Err(format!("segment {:?} already exists", source.segment_id));
    }
    let temp_dir = root.join("segments").join(format!(
        ".tmp-{}-{}-{}",
        source.segment_id,
        std::process::id(),
        source.generation
    ));
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)
            .map_err(|error| format!("remove stale stage {}: {error}", temp_dir.display()))?;
    }
    std::fs::create_dir_all(&temp_dir)
        .map_err(|error| format!("mkdir stage {}: {error}", temp_dir.display()))?;
    let result = (|| {
        // A documents-only segment (docs/immutable-segments.md) has no
        // vector rows; the exact rows go with the vector rows.
        let (vector_artifact, exact_artifact) =
            match (source.vector_path, source.exact_vector_path) {
                (Some(vector_path), Some(exact_path)) => (
                    copy_artifact(vector_path, &temp_dir, "vector.index")?,
                    copy_artifact(exact_path, &temp_dir, "vectors.f32")?,
                ),
                (None, None) => (absent_artifact(), absent_artifact()),
                _ => return Err(
                    "a segment carries its vector image and its exact rows together, or neither"
                        .to_string(),
                ),
            };
        let bm25_artifact = copy_artifact(source.bm25_path, &temp_dir, "documents.bm25")?;
        let live_artifact = copy_artifact(source.live_docs_path, &temp_dir, "live-docs.bin")?;

        let vector = if source.vector_path.is_some() {
            let mut vector = VectorIndex::load(source.backend_kind, &temp_dir.join("vector.index"))
                .map_err(|error| format!("open staged vector: {error}"))?;
            vector
                .prepare()
                .map_err(|error| format!("prepare staged vector: {error}"))?;
            Some(vector)
        } else {
            None
        };
        let exact_rows = if source.exact_vector_path.is_some() {
            let exact = ExactVectorStore::open(&temp_dir.join("vectors.f32"))
                .map_err(|error| format!("open staged exact vectors: {error}"))?;
            exact
                .verify_payload()
                .map_err(|error| format!("verify staged exact vectors: {error}"))?;
            if exact.dim() != vector.as_ref().and_then(VectorIndex::dim_opt) {
                return Err(
                    "staged segment exact-vector dimension differs from its provider".into(),
                );
            }
            exact.len()
        } else {
            0
        };
        let bm25 = Bm25Reader::open(&temp_dir.join("documents.bm25"))
            .map_err(|error| format!("open staged BM25: {error}"))?;
        bm25.verify_integrity()
            .map_err(|error| format!("verify staged BM25: {error}"))?;
        let live_docs = LiveDocs::open(&temp_dir.join("live-docs.bin"))
            .map_err(|error| format!("open staged live docs: {error}"))?;
        let vector_rows = vector.as_ref().map_or(0, VectorIndex::len);
        let bm25_rows = bm25.next_doc_id() as usize;
        let rows = vector_rows.max(bm25_rows);
        let aligned = |n: usize| n == 0 || n == rows;
        if rows == 0
            || !aligned(vector_rows)
            || (vector.is_some() && vector_rows != rows)
            || exact_rows != vector_rows
            || !aligned(bm25_rows)
            || live_docs.persisted_rows() != rows as u64
        {
            return Err(format!(
                "staged segment is not aligned: vector {vector_rows}, exact {exact_rows}, BM25 \
                 {bm25_rows}, live-doc {}",
                live_docs.persisted_rows()
            ));
        }
        let (backend_kind, scoring_fingerprint) = match &vector {
            Some(vector) => {
                let descriptor = vector.descriptor();
                if descriptor.score_direction != ScoreDirection::HigherIsBetter
                    || descriptor.quality_contract != QualityContract::ExhaustiveQuantized
                {
                    return Err(
                        "aligned segments currently require an exhaustive higher-is-better provider"
                            .into(),
                    );
                }
                (
                    source.backend_kind.to_string(),
                    descriptor.scoring_fingerprint,
                )
            }
            None => (String::new(), String::new()),
        };
        let metadata = SegmentMetadata {
            segment_id: source.segment_id.to_string(),
            generation: source.generation,
            base_label: source.base_label,
            rows: rows as u64,
            live_rows: rows as u64 - live_docs.deleted_count(),
            backend_kind,
            scoring_fingerprint,
            analysis_fingerprints: (0..bm25.field_count())
                .map(|field| bm25.analysis_fingerprint(field))
                .collect(),
            document_count: Bm25Index::doc_count(&bm25),
            total_document_length: Bm25Index::total_doc_length(&bm25),
            vector: vector_artifact,
            exact_vectors: exact_artifact,
            bm25: bm25_artifact,
            live_docs: live_artifact,
            summary: Some({
                let mut summary = summarize_columns(&bm25, rows as u32);
                summary.partition = source.partition_column.and_then(|column| {
                    summary
                        .int_columns
                        .iter()
                        .find(|c| c.name == column && c.present > 0)
                        .map(|c| PartitionRange {
                            column: column.to_string(),
                            lo: c.min,
                            hi: c.max,
                        })
                });
                summary
            }),
        };
        write_json_atomic(&temp_dir.join(SEGMENT_META_FILE), &metadata)?;
        std::fs::File::open(&temp_dir)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("sync stage {}: {error}", temp_dir.display()))?;
        std::fs::rename(&temp_dir, &final_dir).map_err(|error| {
            format!(
                "publish segment stage {} -> {}: {error}",
                temp_dir.display(),
                final_dir.display()
            )
        })?;
        std::fs::File::open(root.join("segments"))
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("sync segment directory: {error}"))?;
        Ok(metadata)
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
    result
}

fn verify_artifact(directory: &Path, artifact: &SegmentArtifact) -> Result<(), String> {
    validate_relative_file(&artifact.file)?;
    let path = directory.join(&artifact.file);
    let (bytes, sha256) = digest_file(&path)?;
    if bytes != artifact.bytes || sha256 != artifact.sha256 {
        return Err(format!(
            "segment artifact {} integrity mismatch: manifest {}/{}, actual {}/{}",
            path.display(),
            artifact.bytes,
            artifact.sha256,
            bytes,
            sha256
        ));
    }
    Ok(())
}

#[cfg(test)]
thread_local! { static FAIL_SET_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("mkdir {}: {error}", parent.display()))?;
    let mut temp = path.as_os_str().to_owned();
    temp.push(format!(".tmp-{}", std::process::id()));
    let temp = PathBuf::from(temp);
    let bytes =
        serde_json::to_vec_pretty(value).map_err(|error| format!("encode JSON: {error}"))?;
    {
        let mut file = std::fs::File::create(&temp)
            .map_err(|error| format!("create {}: {error}", temp.display()))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("write {}: {error}", temp.display()))?;
    }
    std::fs::rename(&temp, path).map_err(|error| format!("replace {}: {error}", path.display()))?;
    #[cfg(test)]
    if path.file_name().is_some_and(|name| name == SET_FILE)
        && FAIL_SET_SYNC.with(|fail| fail.replace(false))
    {
        return Err("injected directory sync failure after manifest rename".into());
    }
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("sync {}: {error}", parent.display()))
}

/// The column ranges of a sealed BM25 image, over every stored row
/// (deleted rows included: a range over a superset is still sound for
/// pruning, and the live bitmap keeps changing after the seal).
pub(crate) fn summarize_columns(bm25: &Bm25Reader, rows: u32) -> SegmentSummary {
    let int_columns = (0..bm25.integer_count())
        .map(|ii| {
            let mut present = 0u64;
            let mut min = i64::MAX;
            let mut max = i64::MIN;
            for doc in 0..rows {
                if let Some(value) = bm25.integer_value(ii, doc) {
                    present += 1;
                    min = min.min(value);
                    max = max.max(value);
                }
            }
            IntColumnSummary {
                name: bm25.integer_name(ii).to_string(),
                min,
                max,
                present,
            }
        })
        .collect();
    let uint_columns = (0..bm25.unsigned_integer_count())
        .map(|ii| {
            let mut present = 0u64;
            let mut min = u64::MAX;
            let mut max = 0;
            for doc in 0..rows {
                if let Some(value) = bm25.unsigned_integer_value(ii, doc) {
                    present += 1;
                    min = min.min(value);
                    max = max.max(value);
                }
            }
            UintColumnSummary {
                name: bm25.unsigned_integer_name(ii).to_string(),
                min,
                max,
                present,
            }
        })
        .collect();
    let numeric_columns = (0..bm25.numeric_count())
        .map(|ni| {
            let mut present = 0u64;
            let mut min = f64::MAX;
            let mut max = f64::MIN;
            for doc in 0..rows {
                if let Some(value) = bm25.numeric_value(ni, doc) {
                    present += 1;
                    min = min.min(value);
                    max = max.max(value);
                }
            }
            if present == 0 {
                min = f64::MAX;
                max = f64::MIN;
            }
            NumericColumnSummary {
                name: bm25.numeric_name(ni).to_string(),
                min,
                max,
                present,
            }
        })
        .collect();
    SegmentSummary {
        int_columns,
        uint_columns,
        numeric_columns,
        partition: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postings::{AnalyzedDoc, Bm25Store};
    use crate::vector::{VectorIndex, EMBEDDED_TURBOVEC};

    struct Fixture {
        root: PathBuf,
        vector: PathBuf,
        exact: PathBuf,
        bm25: PathBuf,
        live_docs: PathBuf,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn fixture(name: &str, values: &[f32], terms: &[&str]) -> Fixture {
        let root = std::env::temp_dir().join(format!(
            "protomolt-segments-{name}-{}-{}",
            std::process::id(),
            crate::wal::crc32(name.as_bytes())
        ));
        let _ = std::fs::remove_dir_all(&root);
        let source = root.join("source");
        std::fs::create_dir_all(&source).unwrap();
        let dim = 8;
        let config = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, dim, 4, values).unwrap();
        let mut vector = VectorIndex::from_backend_config(dim, &config).unwrap();
        vector.add(values, dim).unwrap();
        vector.prepare().unwrap();
        vector.write(&source.join("vector.index")).unwrap();
        ExactVectorStore::from_values(dim, values.to_vec())
            .unwrap()
            .write(&source.join("vectors.f32"))
            .unwrap();
        let mut bm25 = Bm25Store::new();
        for (row, term) in terms.iter().enumerate() {
            bm25.add_document(
                row as u32,
                format!("{term} document"),
                AnalyzedDoc::body(
                    vec![
                        ((*term).to_string(), 1, vec![(0, term.len() as u32)]),
                        (
                            "document".into(),
                            1,
                            vec![(term.len() as u32 + 1, term.len() as u32 + 9)],
                        ),
                    ],
                    2,
                ),
            );
        }
        bm25.save(&source.join("documents.bm25")).unwrap();
        LiveDocs::default()
            .write(&source.join("live-docs.bin"), terms.len() as u64)
            .unwrap();
        Fixture {
            root,
            vector: source.join("vector.index"),
            exact: source.join("vectors.f32"),
            bm25: source.join("documents.bm25"),
            live_docs: source.join("live-docs.bin"),
        }
    }

    fn source<'a>(fixture: &'a Fixture, id: &'a str, base: u64) -> SegmentSource<'a> {
        source_generation(fixture, id, base, 1)
    }

    fn source_generation<'a>(
        fixture: &'a Fixture,
        id: &'a str,
        base: u64,
        generation: u64,
    ) -> SegmentSource<'a> {
        SegmentSource {
            segment_id: id,
            generation,
            base_label: base,
            backend_kind: EMBEDDED_TURBOVEC,
            vector_path: Some(&fixture.vector),
            exact_vector_path: Some(&fixture.exact),
            bm25_path: &fixture.bm25,
            live_docs_path: &fixture.live_docs,
            partition_column: None,
        }
    }

    #[test]
    fn compaction_publication_preserves_previously_pinned_epochs() {
        let fixture = fixture(
            "pinned-epoch",
            &[
                1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ],
            &["one", "two"],
        );
        let catalog = SegmentCatalog::open(fixture.root.join("catalog")).unwrap();
        let before = catalog.snapshot();
        let after = catalog.commit_current(7).unwrap();
        assert_eq!(before.epoch(), 0, "a held snapshot is immutable");
        assert_eq!(after.epoch(), 7);
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(catalog.snapshot().epoch(), 7);
        assert_eq!(
            SegmentCatalog::open(fixture.root.join("catalog"))
                .unwrap()
                .snapshot()
                .epoch(),
            7
        );
    }

    #[test]
    fn failed_compaction_publication_does_not_consume_the_requested_epoch() {
        let fixture = fixture(
            "failed-epoch",
            &[
                1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ],
            &["one", "two"],
        );
        let root = fixture.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        catalog.commit_current(1).unwrap();
        let before = catalog.snapshot();
        let path = SegmentCatalog::manifest_path(&root);
        let backup = root.join("saved.json");
        std::fs::rename(&path, &backup).unwrap();
        std::fs::create_dir(&path).unwrap(); // Force manifest rename to fail.
        assert!(catalog.commit_current(2).is_err());
        assert_eq!(before.epoch(), 1);
        assert_eq!(catalog.snapshot().epoch(), 1);
        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(&backup, &path).unwrap();
        assert_eq!(catalog.commit_current(2).unwrap().epoch(), 2);
    }

    fn publication_fixture(name: &str, terms: &[&str]) -> Fixture {
        fixture(
            name,
            &[
                1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ],
            terms,
        )
    }

    #[test]
    fn uncertain_append_keeps_published_artifacts_and_retries_without_duplicate_rows() {
        let data = publication_fixture("uncertain-append", &["one", "two"]);
        let root = data.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let before = catalog.snapshot();
        FAIL_SET_SYNC.with(|fail| fail.set(true));
        assert!(catalog
            .append(source(&data, "s0", 0))
            .unwrap_err()
            .contains("after manifest rename"));
        assert_eq!(before.epoch(), 0);
        assert_eq!(catalog.snapshot().epoch(), 0);
        // The disk manifest already names s0: cleanup must retain it.
        assert!(SegmentCatalog::segment_dir(&root, "s0").exists());
        assert_eq!(
            SegmentCatalog::open(&root)
                .unwrap()
                .snapshot()
                .manifest()
                .segments
                .len(),
            1
        );
        // A different write cannot overwrite the disk manifest from a stale view.
        assert!(catalog
            .append(source(&data, "s1", 2))
            .unwrap_err()
            .contains("uncertain"));
        assert!(catalog
            .publish_partition_key(Some("year".into()))
            .unwrap_err()
            .contains("uncertain"));
        assert_eq!(
            SegmentCatalog::read_manifest(&root)
                .unwrap()
                .unwrap()
                .segments
                .len(),
            1
        );
        assert!(!SegmentCatalog::segment_dir(&root, "s1").exists());
        let recovered = catalog.append(source(&data, "s0", 0)).unwrap();
        assert_eq!(recovered.epoch(), 1);
        assert_eq!(recovered.manifest().segments.len(), 1);
        assert_eq!(recovered.manifest().segments[0].rows, 2);
        assert_eq!(catalog.append(source(&data, "s0", 0)).unwrap().epoch(), 1);
        assert_eq!(before.epoch(), 0);
    }

    #[test]
    fn segment_retry_refuses_changed_content_generation_range_and_damaged_artifacts() {
        let data = publication_fixture("retry-source", &["one", "two"]);
        let changed = publication_fixture("retry-changed", &["other", "two"]);
        let root = data.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        catalog.append(source(&data, "s0", 0)).unwrap();
        assert!(catalog.append(source(&changed, "s0", 0)).is_err());
        assert!(catalog
            .append(source_generation(&data, "s0", 0, 2))
            .is_err());
        assert!(catalog.append(source(&data, "s0", 2)).is_err());
        assert_eq!(catalog.snapshot().epoch(), 1);
        let vector = SegmentCatalog::segment_dir(&root, "s0").join("vector.index");
        let original = std::fs::read(&vector).unwrap();
        let mut damaged = original.clone();
        let last = damaged.len() - 1;
        damaged[last] ^= 1;
        std::fs::write(&vector, &damaged).unwrap();
        assert!(catalog.append(source(&data, "s0", 0)).is_err());
        std::fs::write(&vector, original).unwrap();
        assert_eq!(catalog.append(source(&data, "s0", 0)).unwrap().epoch(), 1);
    }

    #[test]
    fn uncertain_compaction_keeps_outputs_needed_to_reopen_the_published_set() {
        let data = publication_fixture("uncertain-compact", &["one", "two"]);
        let root = data.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let before = catalog.append(source(&data, "input", 0)).unwrap();
        FAIL_SET_SYNC.with(|fail| fail.set(true));
        assert!(catalog
            .replace_for_compaction(&["input".into()], source_generation(&data, "output", 0, 2))
            .is_err());
        assert_eq!(before.epoch(), 1);
        assert_eq!(
            catalog.snapshot().manifest().segments[0].segment_id,
            "input"
        );
        let reopened = SegmentCatalog::open(&root).unwrap();
        assert_eq!(reopened.snapshot().epoch(), 2);
        assert_eq!(
            reopened.snapshot().manifest().segments[0].segment_id,
            "output"
        );
        assert!(SegmentCatalog::segment_dir(&root, "input").exists());
        assert!(SegmentCatalog::segment_dir(&root, "output").exists());
    }

    #[test]
    fn cleanup_retains_artifacts_when_it_cannot_establish_the_committed_manifest() {
        let data = publication_fixture("uncertain-cleanup", &["one", "two"]);
        let root = data.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let metadata = stage_segment(&root, source(&data, "orphan", 0)).unwrap();
        std::fs::write(SegmentCatalog::manifest_path(&root), b"broken JSON").unwrap();
        cleanup_staged(&root, std::slice::from_ref(&metadata));
        assert!(SegmentCatalog::segment_dir(&root, "orphan").exists());
        assert!(catalog.append(source(&data, "orphan", 0)).is_err());
        std::fs::remove_file(SegmentCatalog::manifest_path(&root)).unwrap();
        cleanup_staged(&root, &[metadata]);
        assert!(!SegmentCatalog::segment_dir(&root, "orphan").exists());
    }

    fn retire(id: &str, rows: &[u64]) -> SegmentRowRetirement {
        SegmentRowRetirement {
            segment_id: id.into(),
            rows: rows.to_vec(),
        }
    }

    fn ids(snapshot: &OpenedSegmentSet) -> Vec<u64> {
        let mut ids: Vec<_> = snapshot
            .search_bm25(&["document".into()], Bm25Params::default(), 20)
            .unwrap()
            .into_iter()
            .map(|hit| hit.doc_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn row_replacement_publishes_both_legs_and_reuses_immutable_images() {
        let input = publication_fixture("replace-old", &["old", "kept"]);
        let output = publication_fixture("replace-new", &["new", "newer"]);
        let root = input.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let before = catalog.append(source(&input, "old", 0)).unwrap();
        let after = catalog
            .commit_rows(
                before.epoch(),
                &[retire("old", &[0])],
                vec![source(&output, "new", 2)],
            )
            .unwrap();
        assert_eq!(ids(&before), vec![0, 1]);
        assert_eq!(ids(&after), vec![1, 2, 3]);
        assert_eq!(after.epoch(), before.epoch() + 1);
        assert!(std::ptr::eq(before.bm25(0), after.bm25(0)));
        assert!(std::ptr::eq(
            before.vector(0).unwrap(),
            after.vector(0).unwrap()
        ));
        assert!(!before.live_docs(0).is_deleted(0));
        assert!(after.live_docs(0).is_deleted(0));
        assert_ne!(
            before.metadata(0).live_docs.file,
            after.metadata(0).live_docs.file
        );
        let mut vector_ids: Vec<_> = after
            .search_vector(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 20)
            .unwrap()
            .into_iter()
            .map(|hit| hit.doc_id)
            .collect();
        vector_ids.sort_unstable();
        assert_eq!(vector_ids, ids(&after));
        assert_eq!(
            after
                .global_bm25_stats(&["document".into()])
                .unwrap()
                .doc_count,
            3
        );
        assert_eq!(
            ids(&SegmentCatalog::open(&root).unwrap().snapshot()),
            ids(&after)
        );
    }

    #[test]
    fn zero_row_replacement_retires_the_complete_old_set() {
        let input = publication_fixture("retire-all", &["one", "two"]);
        let root = input.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let before = catalog.append(source(&input, "old", 0)).unwrap();
        let after = catalog
            .commit_rows(before.epoch(), &[retire("old", &[0, 1])], vec![])
            .unwrap();
        assert!(ids(&after).is_empty());
        assert!(after
            .search_vector(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 20)
            .unwrap()
            .is_empty());
        assert_eq!(after.metadata(0).live_rows, 0);
        assert_eq!(ids(&before), vec![0, 1]);
        assert!(ids(&SegmentCatalog::open(&root).unwrap().snapshot()).is_empty());
    }

    #[test]
    fn row_update_refusals_leave_manifest_and_old_bitmaps_unchanged() {
        let input = publication_fixture("retire-refuse", &["one", "two"]);
        let root = input.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let before = catalog.append(source(&input, "old", 0)).unwrap();
        let directory = SegmentCatalog::segment_dir(&root, "old");
        let count = std::fs::read_dir(&directory).unwrap().count();
        let manifest = std::fs::read(root.join(SET_FILE)).unwrap();
        for (epoch, retirements) in [
            (0, vec![retire("old", &[0])]),
            (1, vec![retire("missing", &[0])]),
            (1, vec![retire("old", &[2])]),
            (1, vec![retire("old", &[0, 0])]),
            (1, vec![retire("old", &[])]),
        ] {
            assert!(catalog.commit_rows(epoch, &retirements, vec![]).is_err());
        }
        let mut invalid_counts = before.published_manifest();
        invalid_counts.epoch += 1;
        invalid_counts.segments[0].live_rows = 0;
        assert!(catalog
            .publish(invalid_counts)
            .unwrap_err()
            .contains("row counts"));
        let missing = input.root.join("missing.bm25");
        let mut invalid = source(&input, "new", 2);
        invalid.bm25_path = &missing;
        assert!(catalog
            .commit_rows(1, &[retire("old", &[0])], vec![invalid])
            .is_err());
        assert_eq!(std::fs::read(root.join(SET_FILE)).unwrap(), manifest);
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), count);
        assert_eq!(catalog.snapshot().epoch(), 1);
        assert_eq!(ids(&before), ids(&catalog.snapshot()));
    }

    #[test]
    fn uncertain_row_update_retains_every_artifact_for_recovery() {
        let input = publication_fixture("retire-uncertain", &["one", "two"]);
        let root = input.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        let before = catalog.append(source(&input, "old", 0)).unwrap();
        FAIL_SET_SYNC.with(|fail| fail.set(true));
        assert!(catalog
            .commit_rows(1, &[retire("old", &[0, 1])], vec![source(&input, "new", 2)])
            .unwrap_err()
            .contains("after manifest rename"));
        assert_eq!(ids(&catalog.snapshot()), vec![0, 1]);
        assert_eq!(ids(&before), vec![0, 1]);
        assert!(catalog
            .commit_rows(1, &[retire("old", &[0])], vec![])
            .unwrap_err()
            .contains("uncertain"));
        let reopened = SegmentCatalog::open(&root).unwrap();
        assert_eq!(reopened.snapshot().epoch(), 2);
        assert_eq!(ids(&reopened.snapshot()), vec![2, 3]);
        assert!(directory_bitmap_exists(
            &root,
            &reopened.snapshot().metadata(0).live_docs.file
        ));
    }

    fn directory_bitmap_exists(root: &Path, name: &str) -> bool {
        SegmentCatalog::segment_dir(root, "old").join(name).exists()
    }

    #[test]
    fn exact_key_retirement_spans_segments_and_honors_snapshot_and_budget() {
        let first = publication_fixture("key-retire-first", &["one", "two"]);
        let second = publication_fixture("key-retire-second", &["three", "four"]);
        let original = crate::pb::ProtobufSource {
            descriptor_set: include_bytes!("../tests/fixtures/protobuf-semantics/descriptor.bin")
                .to_vec(),
            message_type: "semantics.Doc".into(),
            payload: vec![8, 1],
        };
        let key = vec![0, 255, 0];
        for (fixture, version) in [(&first, 1), (&second, 2)] {
            let mut store = Bm25Store::load(&fixture.bm25).unwrap();
            for row in 0..2 {
                let identity = crate::pb::DocumentIdentity {
                    document_key: key.clone(),
                    version,
                    chunk_ordinal: Some(row),
                };
                store
                    .source_archive_mut()
                    .attach_source_with_identity(row, &original, Some(row), Some(&identity))
                    .unwrap();
            }
            store.save(&fixture.bm25).unwrap();
        }
        let root = first.root.join("catalog");
        let catalog = SegmentCatalog::open(&root).unwrap();
        catalog.append(source(&first, "first", 0)).unwrap();
        let before = catalog.append(source(&second, "second", 2)).unwrap();
        assert!(before
            .document_retirements(&key, 3)
            .unwrap_err()
            .contains("row budget"));
        assert!(before.document_retirements(&key, 0).is_err());
        assert!(before
            .document_retirements(&[0, 255], 4)
            .unwrap()
            .is_empty());
        let all = before.document_retirements(&key, 4).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].rows, [0, 1]);
        assert_eq!(all[1].rows, [0, 1]);
        let partial = catalog
            .commit_rows(before.epoch(), &[retire("first", &[0])], vec![])
            .unwrap();
        let remaining = partial.document_retirements(&key, 3).unwrap();
        assert_eq!(remaining[0].rows, [1]);
        assert_eq!(remaining[1].rows, [0, 1]);
        assert_eq!(
            before.document_retirements(&key, 4).unwrap()[0].rows,
            [0, 1]
        );
        let gone = catalog
            .commit_rows(partial.epoch(), &remaining, vec![])
            .unwrap();
        assert!(gone.document_retirements(&key, 1).unwrap().is_empty());
        assert!(ids(&gone).is_empty());
        assert!(SegmentCatalog::open(&root)
            .unwrap()
            .snapshot()
            .document_retirements(&key, 1)
            .unwrap()
            .is_empty());
        assert_eq!(gone.bm25(0).document_identity(1).unwrap().document_key, key);
    }

    #[test]
    fn concurrent_row_updates_have_one_epoch_winner() {
        let input = publication_fixture("retire-race", &["one", "two"]);
        let catalog = SegmentCatalog::open(input.root.join("catalog")).unwrap();
        catalog.append(source(&input, "old", 0)).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = [0, 1]
            .into_iter()
            .map(|row| {
                let catalog = catalog.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    catalog
                        .commit_rows(1, &[retire("old", &[row])], vec![])
                        .is_ok()
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| usize::from(handle.join().unwrap()))
                .sum::<usize>(),
            1
        );
        assert_eq!(catalog.snapshot().epoch(), 2);
        assert_eq!(ids(&catalog.snapshot()).len(), 1);
    }

    #[test]
    fn append_publishes_aligned_segments_and_queries_one_global_order() {
        let a = fixture(
            "a",
            &[
                1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ],
            &["alpha", "shared"],
        );
        let b = fixture(
            "b",
            &[
                1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.8, 0.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ],
            &["shared", "beta"],
        );
        let catalog = SegmentCatalog::open(a.root.join("catalog")).unwrap();
        let old = catalog.snapshot();
        let after_first = catalog.append(source(&a, "s0", 0)).unwrap();
        let current = catalog.append(source(&b, "s1", 2)).unwrap();
        assert!(old.manifest().segments.is_empty());
        assert_eq!(current.manifest().epoch, 2);
        assert!(
            current.shares_segment(0, &after_first, 0),
            "a publish keeps the segments it already had open"
        );
        let vector = current
            .search_vector(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 3)
            .unwrap();
        assert_eq!(
            vector.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
            vec![0, 2, 1]
        );
        let lexical = current
            .search_bm25(&["shared".into()], Bm25Params::default(), 10)
            .unwrap();
        assert_eq!(
            lexical.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn overlap_and_corruption_fail_before_publication() {
        let values = [
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ];
        let a = fixture("overlap-a", &values, &["a", "b"]);
        let b = fixture("overlap-b", &values, &["c", "d"]);
        let catalog = SegmentCatalog::open(a.root.join("catalog")).unwrap();
        catalog.append(source(&a, "s0", 0)).unwrap();
        let error = catalog.append(source(&b, "s1", 1)).unwrap_err();
        assert!(error.contains("overlaps"), "{error}");
        assert_eq!(catalog.snapshot().manifest().segments.len(), 1);
        let vector_path = a.root.join("catalog/segments/s0/vector.index");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(vector_path)
            .unwrap();
        file.write_all(b"corrupt").unwrap();
        let error = OpenedSegmentSet::open(a.root.join("catalog")).unwrap_err();
        assert!(error.contains("integrity mismatch"), "{error}");
    }

    #[test]
    fn tombstones_are_removed_from_global_stats() {
        let values = [
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ];
        let fixture = fixture("live-stats", &values, &["gone", "live"]);
        let mut live = LiveDocs::default();
        assert!(live.delete(0));
        live.write(&fixture.live_docs, 2).unwrap();
        let catalog = SegmentCatalog::open(fixture.root.join("catalog")).unwrap();
        let snapshot = catalog.append(source(&fixture, "s0", 0)).unwrap();
        let stats = snapshot
            .global_bm25_stats(&["gone".into(), "live".into()])
            .unwrap();
        assert_eq!(stats.doc_count, 1);
        assert_eq!(stats.total_doc_length, 2);
        assert_eq!(stats.dfs, vec![0, 1]);
        assert!(snapshot
            .search_bm25(&["gone".into()], Bm25Params::default(), 10)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn compaction_can_publish_multiple_dense_outputs_atomically() {
        let values = [
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ];
        let input_a = fixture("compact-input-a", &values, &["a", "b"]);
        let input_b = fixture("compact-input-b", &values, &["c", "d"]);
        let output_a = fixture("compact-output-a", &values, &["a", "b"]);
        let output_b = fixture("compact-output-b", &values, &["c", "d"]);
        let catalog = SegmentCatalog::open(input_a.root.join("catalog")).unwrap();
        catalog.append(source(&input_a, "in-a", 0)).unwrap();
        catalog.append(source(&input_b, "in-b", 2)).unwrap();
        let before = catalog.snapshot();
        let after = catalog
            .replace_many_for_compaction(
                &["in-a".into(), "in-b".into()],
                vec![
                    source_generation(&output_a, "out-a", 100, 2),
                    source_generation(&output_b, "out-b", 102, 2),
                ],
                None,
            )
            .unwrap();
        assert_eq!(before.manifest().segments.len(), 2);
        assert_eq!(after.manifest().epoch, 3);
        assert_eq!(
            after
                .manifest()
                .segments
                .iter()
                .map(|segment| segment.segment_id.as_str())
                .collect::<Vec<_>>(),
            vec!["out-a", "out-b"]
        );
    }
}
