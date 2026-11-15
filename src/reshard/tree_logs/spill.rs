//! Pass 1 of a re-placement split: route each live row under the new
//! tree into its child's spill log, one WAL bucket (or one source
//! segment) in memory at a time (`docs/replay-from-segments.md`).

use std::collections::BTreeSet;
use std::path::PathBuf;

use crate::exact_vectors::ExactVectorStore;
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors};
use crate::pb::AddDocumentsRequest;
use crate::postings::{AnalyzedField, FieldTranspose};
use crate::wal::{self, WalManifest};

use super::super::analysis_sidecar::AnalysisSidecars;
use super::super::bucket_of;
use super::super::cuts::{partition_key_of, ChildCutPlan};
use super::super::partitioned::flush_spills;
use super::super::tree_split::SpillCut;
use super::plan::TreePlan;

/// A vector row as the spill logs it: the values, the dimension, and the
/// retained payload.
pub(super) type SpilledVector = (Vec<f32>, u32, Option<Vec<u8>>);

/// One routed row on its way into a child's spill.
pub(super) struct RoutedRow<'a> {
    pub(super) index: usize,
    pub(super) id: u64,
    pub(super) document: AddDocumentsRequest,
    pub(super) vector: Option<SpilledVector>,
    pub(super) keys: Vec<Vec<u8>>,
    /// Transplanted fields, under `--from-segments`.
    pub(super) fields: Option<&'a [AnalyzedField]>,
}

/// What routing counted.
#[derive(Default)]
pub(super) struct SpillStats {
    /// Live documents whose code changed.
    pub(super) moved: u64,
    /// Live documents per child.
    pub(super) placed: Vec<u64>,
    /// Documents filed per (child, bucket): the threaded build takes each
    /// bucket's slot range from these before any bucket is replayed.
    pub(super) bucket_docs: Vec<Vec<u64>>,
    pub(super) transplanted_rows: u64,
    pub(super) peak_transpose_bytes: u64,
}

/// The spill logs and sidecars of every child while routing runs.
pub(super) struct TreeSpill<'a> {
    pub(super) plan: &'a TreePlan<'a>,
    spills: Vec<wal::WalWriter>,
    sidecars: Vec<AnalysisSidecars>,
    cut_plans: Option<Vec<ChildCutPlan>>,
    pub(super) spill_bucket_count: u32,
    pub(super) stats: SpillStats,
    seen: BTreeSet<u64>,
}

/// The spill once routing is done.
pub(super) struct SpillOutcome {
    pub(super) dirs: Vec<PathBuf>,
    pub(super) stats: SpillStats,
    pub(super) spill_bucket_count: u32,
    /// A hash-cut spill files by id hash, which the replay checks; a
    /// column cut files by value.
    pub(super) check_routing: bool,
}

impl<'a> TreeSpill<'a> {
    pub(super) fn create(
        plan: &'a TreePlan<'a>,
        cut_plans: Option<Vec<ChildCutPlan>>,
        spill_bucket_bits: u32,
    ) -> Result<Self, String> {
        let manifest = spill_manifest(plan, spill_bucket_bits);
        let spill_bucket_count = manifest.bucket_count;
        let n = plan.children.len();
        let spill_root = plan.out_dir.join("spill");
        let mut spills = Vec::with_capacity(n);
        for index in 0..n {
            let dir = spill_root.join(format!("c{index:05}"));
            let writer = wal::WalWriter::create(&dir, manifest.clone())
                .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
            spills.push(writer);
        }
        Ok(Self {
            plan,
            spills,
            sidecars: (0..n)
                .map(|_| AnalysisSidecars::new(spill_bucket_count))
                .collect(),
            cut_plans,
            spill_bucket_count,
            stats: SpillStats {
                placed: vec![0; n],
                bucket_docs: (0..n)
                    .map(|_| vec![0; spill_bucket_count as usize])
                    .collect(),
                ..SpillStats::default()
            },
            seen: BTreeSet::new(),
        })
    }

    /// Refuse a source id seen before: the inputs overlap.
    pub(super) fn first_sight(&mut self, id: u64, inputs: &str) -> Result<(), String> {
        if self.seen.insert(id) {
            return Ok(());
        }
        Err(format!(
            "live source id {id} appears in more than one {inputs}; the inputs overlap"
        ))
    }

    /// Route one live row and file it: rederived, its code rewritten to
    /// the leaf it lands in, counted.
    pub(super) fn route_row(
        &mut self,
        id: u64,
        document: AddDocumentsRequest,
        key: Option<&Vec<u8>>,
    ) -> Result<(usize, AddDocumentsRequest), String> {
        let document = self.plan.rederive(id, document, key.map(Vec::as_slice))?;
        let (index, code, changed) = self.plan.route(id, &document, key)?;
        if changed {
            self.stats.moved += 1;
        }
        let mut rewritten = document;
        let column = &self.plan.column;
        rewritten.integers.retain(|value| &value.field != column);
        rewritten.integers.push(crate::pb::IntegerValue {
            field: column.clone(),
            value: code,
        });
        Ok((index, rewritten))
    }

    /// File one routed row: its records into the child's spill, in the
    /// bucket the cut names (or the one the id hashes to), and its
    /// transplanted fields into the bucket's sidecar.
    pub(super) fn file_row(&mut self, row: RoutedRow<'_>) -> Result<(), String> {
        let (index, id) = (row.index, row.id);
        let bucket = self.cut_bucket(index, &row.document)?;
        let document = wal_record::Op::AddDocuments(LoggedAddDocuments {
            source_references: Vec::new(),
            first_id: id,
            documents: vec![row.document],
            stable_routing_keys: row.keys.clone(),
        });
        self.append(index, bucket, document)
            .map_err(|error| format!("spill document {id}: {error}"))?;
        let filed = bucket.unwrap_or(bucket_of(id, self.spill_bucket_count as usize) as u32);
        self.stats.bucket_docs[index][filed as usize] += 1;
        if let Some((vector, dim, retained)) = row.vector {
            let record = wal_record::Op::AddVectors(LoggedAddVectors {
                first_id: id,
                batch: Some(crate::pb::AddVectorsRequest {
                    materialized_rows: retained.into_iter().collect(),
                    vectors: vector,
                    dim,
                }),
                stable_routing_keys: row.keys,
            });
            self.append(index, bucket, record)
                .map_err(|error| format!("spill vector {id}: {error}"))?;
        }
        if let Some(fields) = row.fields {
            let dir = self.spills[index].dir().to_path_buf();
            self.sidecars[index].write(&dir, filed, id, fields)?;
        }
        self.stats.placed[index] += 1;
        Ok(())
    }

    /// The bucket a column cut files the row in; `None` under a hash cut.
    fn cut_bucket(
        &mut self,
        index: usize,
        document: &AddDocumentsRequest,
    ) -> Result<Option<u32>, String> {
        let (
            Some(plans),
            SpillCut::Column {
                column,
                rows_per_cut,
            },
        ) = (self.cut_plans.as_mut(), &self.plan.options.cut)
        else {
            return Ok(None);
        };
        let key = partition_key_of(document, column)?;
        Ok(Some(plans[index].bucket(key, *rows_per_cut)))
    }

    fn append(
        &mut self,
        index: usize,
        bucket: Option<u32>,
        record: wal_record::Op,
    ) -> std::io::Result<()> {
        match bucket {
            Some(bucket) => self.spills[index].append_to_bucket(bucket, record),
            None => self.spills[index].append(record),
        }
    }

    /// Flush every sidecar and spill log.
    pub(super) fn finish(self) -> Result<SpillOutcome, String> {
        drop(self.seen);
        for sidecars in self.sidecars {
            sidecars.finish()?;
        }
        Ok(SpillOutcome {
            dirs: flush_spills(self.spills)?,
            stats: self.stats,
            spill_bucket_count: self.spill_bucket_count,
            check_routing: self.cut_plans.is_none(),
        })
    }
}

/// The spill logs' manifest: the first input's, at the spill's bucket
/// count, recording what their rows are derived under.
fn spill_manifest(plan: &TreePlan<'_>, spill_bucket_bits: u32) -> WalManifest {
    let mut manifest = plan.manifest.clone();
    manifest.bucket_bits = spill_bucket_bits;
    manifest.bucket_count = 1u32 << spill_bucket_bits;
    match plan.declaration.as_deref() {
        Some(declaration) => {
            manifest.derived_fingerprint = declaration.fingerprint().to_string();
            manifest.derived = declaration.config();
        }
        None => {
            manifest.derived_fingerprint.clear();
            manifest.derived.clear();
        }
    }
    manifest
}

/// One source segment being routed: its transposed fields and exact rows.
pub(super) struct SegmentRows<'a> {
    pub(super) shard: &'a super::super::segment_sources::SegmentSourceShard,
    pub(super) segment: usize,
    pub(super) exact: Option<&'a ExactVectorStore>,
    pub(super) transposes: &'a [FieldTranspose],
    pub(super) dim: u32,
}

impl SegmentRows<'_> {
    pub(super) fn context(&self) -> String {
        format!(
            "{} segment {}",
            self.shard.root.display(),
            self.shard.set.metadata(self.segment).segment_id
        )
    }

    /// Row `row`'s exact vector with its retained payload, when the
    /// segment has vectors.
    pub(super) fn vector(&self, row: u32) -> Result<Option<SpilledVector>, String> {
        let Some(store) = self.exact else {
            return Ok(None);
        };
        let values = store
            .row_values(row as usize, row as usize + 1)
            .map_err(|e| format!("read exact row {row}: {e}"))?;
        let retained = self
            .shard
            .set
            .retained_segment(self.segment)
            .retained(row)
            .map(crate::multi_res::encode)
            .transpose()
            .map_err(|error| format!("retain segment row {row}: {error}"))?;
        Ok(Some((values, self.dim, retained)))
    }
}
