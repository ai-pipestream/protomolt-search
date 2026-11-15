//! How a re-placement split's spill is cut: the column-cut plans, the
//! spill bucket count, and the open-files budget of the spill pass.

use std::collections::BTreeMap;

use super::super::cuts::{partition_key_of, plan_cuts, ChildCutPlan};
use super::super::open_files_soft_limit;
use super::super::reconstruct::reconstruct_document;
use super::super::replay::{read_gen_manifest, replay_buckets_through, Replay};
use super::super::tree_split::SpillCut;
use super::plan::TreePlan;

/// Per-child key histograms of a column cut, and the unkeyed counts.
struct Histograms<'a> {
    plan: &'a TreePlan<'a>,
    column: &'a str,
    keyed: Vec<BTreeMap<i64, u64>>,
    unkeyed: Vec<u64>,
}

impl Histograms<'_> {
    fn count(
        &mut self,
        id: u64,
        document: crate::pb::AddDocumentsRequest,
        key: Option<&Vec<u8>>,
    ) -> Result<(), String> {
        let document = self.plan.rederive(id, document, key.map(Vec::as_slice))?;
        let (index, _, _) = self.plan.route(id, &document, key)?;
        match partition_key_of(&document, self.column)? {
            Some(key) => *self.keyed[index].entry(key).or_insert(0) += 1,
            None => self.unkeyed[index] += 1,
        }
        Ok(())
    }

    fn count_segments(&mut self) -> Result<(), String> {
        let (shards, tables) = self.plan.sources.as_ref().expect("segment sources");
        if !tables
            .columns
            .integers
            .iter()
            .any(|name| name == self.column)
        {
            return Err(format!(
                "the cut column {:?} is not an integer column of the sources ({:?})",
                self.column, tables.columns.integers
            ));
        }
        for shard in shards {
            for i in 0..shard.set.len() {
                let bm25 = shard.set.bm25(i);
                for row in 0..bm25.next_doc_id() {
                    let label = shard.set.metadata(i).base_label + u64::from(row);
                    if !shard.is_live(i, row, label) {
                        continue;
                    }
                    let document = reconstruct_document(bm25, row, tables, false)?;
                    self.count(shard.slot_offset + label, document, None)?;
                }
            }
        }
        Ok(())
    }

    fn count_logs(&mut self) -> Result<(), String> {
        for gen in self.plan.gens {
            let current = read_gen_manifest(gen)?;
            for bucket in 0..current.bucket_count {
                let mut replay = Replay::default();
                let (count, dim) = (current.bucket_count as usize, current.dim as usize);
                let buckets = bucket..bucket + 1;
                replay_buckets_through(gen, buckets, count, dim, false, u64::MAX, &mut replay)?;
                replay.compact();
                for (id, document) in &replay.documents {
                    self.count(*id, document.clone(), replay.stable_keys.get(id))?;
                }
            }
        }
        Ok(())
    }
}

/// A column cut plans each child's buckets from a first pass over the
/// columns; a hash cut needs no plan.
pub(super) fn cut_plans(plan: &TreePlan<'_>) -> Result<Option<Vec<ChildCutPlan>>, String> {
    let SpillCut::Column {
        column: cut_column,
        rows_per_cut,
    } = &plan.options.cut
    else {
        return Ok(None);
    };
    if *rows_per_cut == 0 {
        return Err("a column cut needs a positive row bound".to_string());
    }
    if cut_column == &plan.column {
        return Err(format!(
            "the cut column {cut_column:?} is the placement column; every row of a child \
             carries one code there, cut by another column"
        ));
    }
    let n = plan.children.len();
    let mut histograms = Histograms {
        plan,
        column: cut_column,
        keyed: (0..n).map(|_| BTreeMap::new()).collect(),
        unkeyed: vec![0; n],
    };
    if plan.sources.is_some() {
        histograms.count_segments()?;
    } else {
        histograms.count_logs()?;
    }
    Ok(Some(plan_cuts(
        &histograms.keyed,
        &histograms.unkeyed,
        *rows_per_cut,
    )))
}

/// log2 of the spill logs' bucket count: what the column cut needs, the
/// bits asked for, or the widest input's bucket count.
pub(super) fn spill_bucket_bits(
    plan: &TreePlan<'_>,
    cut_plans: Option<&[ChildCutPlan]>,
) -> Result<u32, String> {
    if let Some(plans) = cut_plans {
        let widest = plans
            .iter()
            .map(|plan| plan.buckets)
            .max()
            .unwrap_or(1)
            .max(1);
        let bits = widest.next_power_of_two().trailing_zeros();
        if bits > 16 {
            return Err(format!(
                "the column cut needs {widest} buckets in one child, above 65,536; raise the row \
                 bound"
            ));
        }
        return Ok(bits);
    }
    if let Some(bits) = plan.options.spill_bucket_bits {
        if bits > 16 {
            return Err(format!(
                "spill bucket bits {bits} exceed 16 (65,536 bucket files per child)"
            ));
        }
        return Ok(bits);
    }
    let mut widest = plan.manifest.bucket_count;
    for gen in plan.gens {
        widest = widest.max(read_gen_manifest(gen)?.bucket_count);
    }
    Ok(widest.trailing_zeros())
}

/// The spill pass holds a log file and an analysis sidecar open per
/// bucket per child, and the sources' segments beside them; a plan past
/// the process's open-files limit dies deep into the pass (the archive at
/// 150,000 rows per cut: "Too many open files" after 34 minutes), so it is
/// refused here by name.
pub(super) fn check_open_files(
    plan: &TreePlan<'_>,
    cut_plans: Option<&[ChildCutPlan]>,
    spill_bucket_count: u32,
) -> Result<(), String> {
    let spill_files = (0..plan.children.len())
        .map(|index| match cut_plans {
            Some(plans) => u64::from(plans[index].buckets),
            None => u64::from(spill_bucket_count),
        })
        .sum::<u64>()
        .saturating_mul(2);
    let source_files = plan.sources.as_ref().map_or(0, |(shards, _)| {
        shards
            .iter()
            .map(|shard| shard.set.len() as u64)
            .sum::<u64>()
            .saturating_mul(2)
    });
    let needed = spill_files.saturating_add(source_files).saturating_add(64);
    let limit = match plan.options.open_files_limit {
        Some(limit) => Some(limit),
        None => open_files_soft_limit(),
    };
    let Some(limit) = limit else {
        return Ok(());
    };
    if needed > limit {
        return Err(format!(
            "the split would hold about {needed} files open ({spill_files} spill logs and \
             analysis sidecars over {} children, {source_files} for the sources' segments) above \
             this process's open-files limit of {limit}; raise it (ulimit -n, or the reshard \
             raises its own to the hard limit at start) or use fewer, larger cuts (--cut-rows)",
            plan.children.len()
        ));
    }
    Ok(())
}
