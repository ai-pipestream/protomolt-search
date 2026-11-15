//! The first two passes of a compaction from segments: the key count
//! that plans the cuts, and the spill of every live row into its cut.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::live_docs::LiveDocs;
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors};
use crate::segments::OpenedSegmentSet;
use crate::wal::{self, WalManifest};

use super::super::analysis_sidecar::AnalysisSidecars;
use super::super::cuts::{partition_key_of, ChildCutPlan};
use super::super::partitioned::flush_spills;
use super::super::reconstruct::{reconstruct_document, transpose_segment, transposed_fields};
use super::shape::CatalogShape;
use super::PartitionSpec;

/// The cutoff view of the catalog: its sealed set, the shard-wide overlay,
/// and the slot offset the rows' global ids start at.
#[derive(Clone, Copy)]
pub(super) struct CatalogRows<'a> {
    pub(super) set: &'a OpenedSegmentSet,
    pub(super) overlay: &'a LiveDocs,
    pub(super) slot_offset: u64,
}

impl CatalogRows<'_> {
    /// A row is live unless its segment or the overlay deletes it.
    pub(super) fn live_at(&self, segment: usize, row: u32) -> bool {
        let label = self.set.metadata(segment).base_label + u64::from(row);
        !self.set.live_docs(segment).is_deleted(row as usize)
            && !self
                .overlay
                .is_deleted(usize::try_from(label).unwrap_or(usize::MAX))
    }
}

/// What the key count found: the cut plan, and the rows seen.
pub(super) struct KeyCount {
    pub(super) plan: ChildCutPlan,
    pub(super) rows_before: u64,
    pub(super) tombstones: u64,
}

/// Pass 1: count the live rows per key over the column tables (no
/// transpose is needed) and plan the cuts.
pub(super) fn count_keys(
    rows: CatalogRows<'_>,
    shape: &CatalogShape,
    spec: PartitionSpec<'_>,
) -> Result<KeyCount, String> {
    let column = spec.column;
    let integers = &shape.tables.columns.integers;
    let column_index = integers
        .iter()
        .position(|name| name == column)
        .ok_or_else(|| {
            format!(
                "partition column {column:?} is not an integer column of the catalog \
                 ({integers:?})"
            )
        })?;
    let mut histogram: BTreeMap<i64, u64> = BTreeMap::new();
    let (mut unkeyed, mut rows_before, mut tombstones) = (0u64, 0u64, 0u64);
    for i in 0..rows.set.len() {
        let bm25 = rows.set.bm25(i);
        for row in 0..bm25.next_doc_id() {
            rows_before += 1;
            if !rows.live_at(i, row) {
                tombstones += 1;
                continue;
            }
            match bm25.integer_value(column_index, row) {
                Some(key) => *histogram.entry(key).or_insert(0) += 1,
                None => unkeyed += 1,
            }
        }
    }
    if histogram.is_empty() {
        return Err(format!(
            "partition column {column:?}: no live document of this catalog carries it"
        ));
    }
    let plan = ChildCutPlan::plan(&histogram, unkeyed, spec.bound as u64);
    if plan.buckets > 65_536 {
        return Err(format!(
            "the cut needs {} partitions, above 65,536; raise the row bound",
            plan.buckets
        ));
    }
    Ok(KeyCount {
        plan,
        rows_before,
        tombstones,
    })
}

/// The spill's manifest: one bucket, the catalog's provider state, and
/// the declaration its rows are derived under, as the split's spill
/// records it.
pub(super) fn spill_manifest(
    rows: CatalogRows<'_>,
    shape: &CatalogShape,
) -> Result<WalManifest, String> {
    let mut manifest = WalManifest {
        dim: shape.dim as u32,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: Vec::new(),
        collection: String::new(),
        slot_offset: rows.slot_offset,
        generation: 0,
        bucket_bits: 0,
        bucket_count: 1,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: wal::FORMAT_VERSION,
        derived_fingerprint: String::new(),
        derived: Vec::new(),
        columns: None,
    };
    manifest.set_backend_config(shape.backend_config.clone());
    if let Some(derived) = &shape.derived {
        manifest.derived_fingerprint = derived.fingerprint.clone();
        manifest.derived = crate::derived::config_from_spec(&crate::derived::decode_canonical(
            &derived.declaration,
        )?);
    }
    Ok(manifest)
}

/// Pass 2: one single-bucket log per cut, with the documents' analyzed
/// fields in an analysis sidecar beside it, exactly the split's spill.
pub(super) struct CutSpill<'a> {
    rows: CatalogRows<'a>,
    shape: &'a CatalogShape,
    spec: PartitionSpec<'a>,
    spills: Vec<wal::WalWriter>,
    sidecars: AnalysisSidecars,
    /// Rows filed per cut.
    pub(super) bucket_rows: Vec<u64>,
}

impl<'a> CutSpill<'a> {
    pub(super) fn create(
        rows: CatalogRows<'a>,
        shape: &'a CatalogShape,
        spec: PartitionSpec<'a>,
        spill_root: &Path,
        buckets: u32,
    ) -> Result<Self, String> {
        let manifest = spill_manifest(rows, shape)?;
        let mut spills = Vec::with_capacity(buckets as usize);
        for bucket in 0..buckets {
            let dir = spill_root.join(format!("p{bucket:05}"));
            let writer = wal::WalWriter::create(&dir, manifest.clone())
                .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
            spills.push(writer);
        }
        Ok(Self {
            rows,
            shape,
            spec,
            spills,
            sidecars: AnalysisSidecars::new(buckets),
            bucket_rows: vec![0; buckets as usize],
        })
    }

    /// Spill every live row of segment `i`, then drop its pages from the
    /// resident set and the page cache, so the run's footprint is one
    /// segment's.
    pub(super) fn spill_segment(
        &mut self,
        i: usize,
        plan: &mut ChildCutPlan,
    ) -> Result<(), String> {
        let set = self.rows.set;
        let meta = set.metadata(i);
        let context = || format!("{} segment {}", set.root().display(), meta.segment_id);
        let bm25 = set.bm25(i);
        let transposes = transpose_segment(bm25, &context)?;
        for row in 0..bm25.next_doc_id() {
            if !self.rows.live_at(i, row) {
                continue;
            }
            let id = self
                .rows
                .slot_offset
                .checked_add(meta.base_label)
                .and_then(|id| id.checked_add(u64::from(row)))
                .ok_or_else(|| "compaction from segments: id overflow".to_string())?;
            let fields = transposed_fields(&transposes, row, &context)?;
            self.spill_row(i, row, id, plan, &fields)?;
        }
        drop(transposes);
        if let Some(store) = set.exact_vectors(i) {
            store
                .release_pages()
                .map_err(|e| format!("{}: {e}", context()))?;
        }
        bm25.release_pages()
            .map_err(|e| format!("{}: {e}", context()))
    }

    fn spill_row(
        &mut self,
        i: usize,
        row: u32,
        id: u64,
        plan: &mut ChildCutPlan,
        fields: &[crate::postings::AnalyzedField],
    ) -> Result<(), String> {
        let set = self.rows.set;
        let document =
            reconstruct_document(set.bm25(i), row, &self.shape.tables, true).map_err(|e| {
                format!(
                    "{} segment {}: {e}",
                    set.root().display(),
                    set.metadata(i).segment_id
                )
            })?;
        let key = partition_key_of(&document, self.spec.column)?;
        let bucket = plan.bucket(key, self.spec.bound as u64);
        self.bucket_rows[bucket as usize] += 1;
        let spill = &mut self.spills[bucket as usize];
        spill
            .append(wal_record::Op::AddDocuments(LoggedAddDocuments {
                source_references: Vec::new(),
                first_id: id,
                documents: vec![document],
                stable_routing_keys: Vec::new(),
            }))
            .map_err(|error| format!("spill document {id}: {error}"))?;
        let store = set
            .exact_vectors(i)
            .expect("checked above: every segment has exact rows");
        let vector = store
            .row_values(row as usize, row as usize + 1)
            .map_err(|e| format!("read exact row {row}: {e}"))?;
        let retained = set
            .retained_segment(i)
            .retained(row)
            .map(crate::multi_res::encode)
            .transpose()
            .map_err(|error| format!("retain segment row {row}: {error}"))?;
        spill
            .append(wal_record::Op::AddVectors(LoggedAddVectors {
                first_id: id,
                batch: Some(crate::pb::AddVectorsRequest {
                    materialized_rows: retained.into_iter().collect(),
                    vectors: vector,
                    dim: self.shape.dim as u32,
                }),
                stable_routing_keys: Vec::new(),
            }))
            .map_err(|error| format!("spill vector {id}: {error}"))?;
        let dir = spill.dir().to_path_buf();
        self.sidecars.write(&dir, bucket, id, fields)
    }

    /// Flush every sidecar and log; the spill directories by cut.
    pub(super) fn finish(self) -> Result<(Vec<PathBuf>, Vec<u64>), String> {
        self.sidecars.finish()?;
        Ok((flush_spills(self.spills)?, self.bucket_rows))
    }
}
