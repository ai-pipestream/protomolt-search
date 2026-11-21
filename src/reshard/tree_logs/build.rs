//! Pass 2 of a re-placement split: what every child is built under, and
//! the build-memory budget checked before any child is replayed.

use std::path::Path;

use crate::postings::StoredDerived;

use super::super::child_build::{finish_child, ChildSchema, ChildSlot};
use super::super::replay::{replay_buckets_routed, Replay, ReplayScope};
use super::super::tree_split::{TreeChild, BUILD_BYTES_PER_ROW};
use super::super::{Analyzer, ChildImage, ColumnTables};
use super::plan::TreePlan;
use super::spill::SpillOutcome;

/// The tables a child pins: the sources' (or the manifest's, in table
/// order), with the derived names of each kind appended after the source
/// columns, present or not, the way a node declares them.
pub(super) struct PinnedTables {
    pub(super) fingerprints: Option<Vec<u64>>,
    pub(super) columns: Option<ColumnTables>,
    pub(super) fields: Option<Vec<String>>,
    pub(super) derived: Option<StoredDerived>,
}

impl PinnedTables {
    pub(super) fn of(plan: &TreePlan<'_>) -> Self {
        let (fingerprints, mut columns, fields) = match &plan.sources {
            Some((_, tables)) => (
                Some(tables.fingerprints.clone()),
                Some(tables.columns.clone()),
                Some(tables.fields.clone()),
            ),
            None => (None, plan.source_columns.clone(), None),
        };
        let declaration = plan.declaration.as_deref();
        if let (Some(declaration), Some(columns)) = (declaration, columns.as_mut()) {
            for (kind, table) in [
                (crate::pb::MaterializeKind::F64, &mut columns.numerics),
                (crate::pb::MaterializeKind::I64, &mut columns.integers),
                (
                    crate::pb::MaterializeKind::U64,
                    &mut columns.unsigned_integers,
                ),
            ] {
                for name in declaration.names_of(kind) {
                    if !table.contains(&name) {
                        table.push(name);
                    }
                }
            }
        }
        Self {
            fingerprints,
            columns,
            fields,
            derived: declaration.map(StoredDerived::of),
        }
    }
}

/// Everything the child builds share once routing is done.
pub(super) struct TreeBuild<'a> {
    pub(super) plan: &'a TreePlan<'a>,
    pub(super) spill: &'a SpillOutcome,
    pub(super) schema: ChildSchema<'a>,
    pub(super) generation: u64,
}

impl<'a> TreeBuild<'a> {
    pub(super) fn new(
        plan: &'a TreePlan<'a>,
        spill: &'a SpillOutcome,
        pinned: &'a PinnedTables,
        bm25_fields: Option<&'a [String]>,
    ) -> Result<Self, String> {
        let schema = ChildSchema {
            bm25_fields: pinned.fields.as_deref().or(bm25_fields),
            pinned_fingerprints: pinned.fingerprints.as_deref(),
            binding: plan.binding.as_ref(),
            derived: pinned.derived.as_ref(),
            columns: pinned.columns.as_ref(),
            hnsw_sources: None,
        };
        let generation = plan
            .top_generation
            .checked_add(1)
            .ok_or_else(|| "re-placement split generation overflow".to_string())?;
        Ok(Self {
            plan,
            spill,
            schema,
            generation,
        })
    }

    /// Replay spill buckets `buckets` of child `index`, with the
    /// transplanted fields beside them under `--from-segments`.
    pub(super) fn replay(
        &self,
        index: usize,
        buckets: std::ops::Range<u32>,
    ) -> Result<Replay, String> {
        let spill_dir = &self.spill.dirs[index];
        let scope = ReplayScope {
            bucket_count: self.spill.spill_bucket_count as usize,
            dim: self.plan.manifest.dim as usize,
            vectors_only: false,
            documents_only: false,
            upto_clock: u64::MAX,
            check_routing: self.spill.check_routing,
        };
        let mut replay = Replay::default();
        replay_buckets_routed(spill_dir, buckets.clone(), &scope, &mut replay)?;
        if self.plan.sources.is_some() {
            for bucket in buckets {
                let path = super::super::analysis_sidecar::analysis_sidecar_path(spill_dir, bucket);
                replay
                    .analyzed
                    .extend(super::super::analysis_sidecar::read_analysis_file(&path)?);
            }
        }
        replay.compact();
        Ok(replay)
    }

    /// One image of child `index`: `replay` sealed under `out_dir` as
    /// `ordinal` at `slot`.
    pub(super) fn image(
        &self,
        index: usize,
        replay: Replay,
        out_dir: &Path,
        ordinal: usize,
        slot: u64,
        analyze: &mut Analyzer,
    ) -> Result<ChildImage, String> {
        let child = &self.plan.children[index];
        let slot = ChildSlot {
            ordinal,
            out_dir,
            slot_offset: slot,
            hash_lo: child.hash_lo,
            hash_hi: child.hash_hi,
        };
        finish_child(&self.plan.manifest, replay, slot, &self.schema, analyze)
    }

    /// A child entry with no rows of its own in the output list: a
    /// segmented child's catalog path, or one `--only-child` skipped.
    pub(super) fn catalog_image(&self, index: usize, vectors: u64, documents: u64) -> ChildImage {
        let child = &self.plan.children[index];
        let index_path = self.plan.out_dir.join(format!("shard-{index}.tv"));
        ChildImage {
            multi_res_path: None,
            exact_vector_path: crate::node::segments_root(&index_path),
            vector_path: index_path,
            bm25_path: None,
            slot_offset: self.plan.slot_offsets[index],
            hash_lo: child.hash_lo,
            hash_hi: child.hash_hi,
            num_vectors: vectors,
            num_documents: documents,
            parent_ids: Vec::new(),
            row_parent_ids: Vec::new(),
            parents_colocated: false,
        }
    }
}

/// The build-memory budget, against the spill's own per-bucket row counts
/// now that routing is done: the largest bucket of each child to be built
/// must fit alone (one thread still replays one bucket), and the thread
/// count times it must fit together. A plan that cannot fit is refused by
/// name; nothing is lowered to fit.
pub(super) fn check_build_memory(plan: &TreePlan<'_>, spill: &SpillOutcome) -> Result<(), String> {
    let options = plan.options;
    let Some(budget) = options.build_memory else {
        return Ok(());
    };
    let threads = options.build_threads.max(1) as u64;
    for (index, child) in plan.children.iter().enumerate() {
        if options.only_child.is_some_and(|only| only != index) {
            continue;
        }
        let Some((bucket, &rows)) = spill.stats.bucket_docs[index]
            .iter()
            .enumerate()
            .filter(|(_, &n)| n > 0)
            .max_by_key(|(_, &n)| n)
        else {
            continue;
        };
        check_bucket_budget(index, child, bucket, rows, budget, threads)?;
    }
    Ok(())
}

fn check_bucket_budget(
    index: usize,
    child: &TreeChild,
    bucket: usize,
    rows: u64,
    budget: u64,
    threads: u64,
) -> Result<(), String> {
    let estimate = rows
        .checked_mul(BUILD_BYTES_PER_ROW)
        .ok_or_else(|| "--build-memory arithmetic overflow estimating a bucket".to_string())?;
    if estimate > budget {
        return Err(format!(
            "child {index} ({}) bucket {bucket}: {rows} documents at {} KiB a row (the \
             conservative end of the 40-70 KB band) is about {} MiB, over --build-memory={} MiB \
             for a single bucket; raise the budget or cut finer (--cut-rows)",
            child.leaf,
            BUILD_BYTES_PER_ROW / 1024,
            estimate.div_ceil(1 << 20),
            budget / (1 << 20)
        ));
    }
    let need = estimate.checked_mul(threads).ok_or_else(|| {
        "--build-memory arithmetic overflow multiplying the bucket estimate by build threads"
            .to_string()
    })?;
    if need > budget {
        return Err(format!(
            "child {index} ({}) bucket {bucket}: {rows} documents at {} KiB a row is about {} \
             MiB, and {threads} build threads need {} MiB at once, over --build-memory={} MiB; \
             lower --build-threads, raise the budget, or cut finer (--cut-rows) -- the thread \
             count is not lowered for you",
            child.leaf,
            BUILD_BYTES_PER_ROW / 1024,
            estimate.div_ceil(1 << 20),
            need.div_ceil(1 << 20),
            budget / (1 << 20)
        ));
    }
    Ok(())
}
