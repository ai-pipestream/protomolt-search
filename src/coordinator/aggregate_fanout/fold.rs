//! The cross-shard fold of aggregation partials, groups, and histograms.

use std::collections::BTreeMap;

use tonic::Status;

use super::super::{AggMerge, CompiledAggregate, FilterKnown, PctMerge, RequestFilters};

/// The running merge of the shards' aggregation folds: partials fold in
/// shard order, groups join by value, and histograms sum by bucket.
pub(super) struct AggregateFold<'a> {
    compiled: &'a CompiledAggregate,
    known: FilterKnown,
    /// The same leaf enumeration the shards answer positionally:
    /// aggregations first, then histograms.
    leaves: Vec<crate::values::ValueLeaf>,
    leaves_known: Vec<bool>,
    group_column_known: bool,
    pub(super) matched: u64,
    pub(super) ungrouped: u64,
    merged: Vec<AggMerge>,
    /// Groups merge by VALUE: the BTreeMap is both the cross-shard join and
    /// the deterministic ascending order the response promises.
    groups: BTreeMap<String, (u64, Vec<AggMerge>)>,
    hist_buckets: Vec<BTreeMap<i64, u64>>,
    hist_present: Vec<u64>,
    hist_unbucketable: Vec<u64>,
    pub(super) pct_merged: Vec<PctMerge>,
}

impl<'a> AggregateFold<'a> {
    pub(super) fn new(compiled: &'a CompiledAggregate, known: FilterKnown) -> Self {
        let CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            group_by,
            ..
        } = compiled;
        let mut leaves = Vec::new();
        for expr in aggregations
            .iter()
            .filter_map(|a| a.expr.as_ref())
            .chain(histograms.iter().filter_map(|h| h.expr.as_ref()))
            .chain(percentiles.iter().filter_map(|p| p.expr.as_ref()))
        {
            crate::values::column_leaves(expr, &mut leaves);
        }
        Self {
            compiled,
            known,
            leaves_known: vec![false; leaves.len()],
            leaves,
            group_column_known: group_by.is_empty(),
            matched: 0,
            ungrouped: 0,
            merged: aggregations.iter().map(|_| AggMerge::new()).collect(),
            groups: BTreeMap::new(),
            hist_buckets: histograms.iter().map(|_| Default::default()).collect(),
            hist_present: vec![0u64; histograms.len()],
            hist_unbucketable: vec![0u64; histograms.len()],
            pct_merged: percentiles.iter().map(|_| PctMerge::new()).collect(),
        }
    }

    /// Fold one shard's answer, refusing any positional shape mismatch.
    pub(super) fn fold_shard(
        &mut self,
        shard: usize,
        response: &crate::pb::AggregateShardResponse,
    ) -> Result<(), Status> {
        let CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            ..
        } = self.compiled;
        self.known.merge_shard(
            shard,
            &response.geo_columns_known,
            &response.filter_columns_known,
        )?;
        if response.expr_leaves_known.len() != self.leaves_known.len() {
            return Err(Status::internal(format!(
                "shard answered {} expression-leaf flags for {} leaves",
                response.expr_leaves_known.len(),
                self.leaves_known.len()
            )));
        }
        for (acc, k) in self
            .leaves_known
            .iter_mut()
            .zip(&response.expr_leaves_known)
        {
            *acc |= *k;
        }
        if response.partials.len() != self.merged.len() {
            return Err(Status::internal(format!(
                "shard answered {} aggregation partials for {} aggregations",
                response.partials.len(),
                self.merged.len()
            )));
        }
        self.group_column_known |= response.group_column_known;
        self.matched += response.matched;
        self.ungrouped += response.ungrouped;
        for (m, (p, agg)) in self
            .merged
            .iter_mut()
            .zip(response.partials.iter().zip(aggregations))
        {
            m.fold(p, agg)?;
        }
        self.fold_groups(response)?;
        if response.histograms.len() != histograms.len() {
            return Err(Status::internal(format!(
                "shard answered {} histograms for {} requested",
                response.histograms.len(),
                histograms.len()
            )));
        }
        if response.percentile_partials.len() != percentiles.len() {
            return Err(Status::internal(format!(
                "shard answered {} percentile partials for {} requested",
                response.percentile_partials.len(),
                percentiles.len()
            )));
        }
        for (m, (p, spec)) in self
            .pct_merged
            .iter_mut()
            .zip(response.percentile_partials.iter().zip(percentiles))
        {
            m.fold(p, &spec.name)?;
        }
        self.fold_histograms(response)
    }

    fn fold_groups(&mut self, response: &crate::pb::AggregateShardResponse) -> Result<(), Status> {
        let CompiledAggregate {
            aggregations,
            group_by,
            max_groups,
            ..
        } = self.compiled;
        let group_cap = *max_groups as usize;
        for shard_group in &response.groups {
            if shard_group.partials.len() != aggregations.len() {
                return Err(Status::internal(format!(
                    "shard answered {} group partials for {} aggregations",
                    shard_group.partials.len(),
                    aggregations.len()
                )));
            }
            let entry = self
                .groups
                .entry(shard_group.value.clone())
                .or_insert_with(|| (0, aggregations.iter().map(|_| AggMerge::new()).collect()));
            entry.0 += shard_group.matched;
            for (m, (p, agg)) in entry
                .1
                .iter_mut()
                .zip(shard_group.partials.iter().zip(aggregations))
            {
                m.fold(p, agg)?;
            }
            if self.groups.len() > group_cap {
                return Err(Status::failed_precondition(format!(
                    "group_by {group_by:?} exceeds {group_cap} distinct values \
                     across the fleet; tighten the filter or raise max_groups"
                )));
            }
        }
        Ok(())
    }

    fn fold_histograms(
        &mut self,
        response: &crate::pb::AggregateShardResponse,
    ) -> Result<(), Status> {
        let histograms = &self.compiled.histograms;
        for (i, (shard_hist, spec)) in response.histograms.iter().zip(histograms).enumerate() {
            if shard_hist.bucket_index.len() != shard_hist.bucket_count.len() {
                return Err(Status::internal(
                    "shard answered a histogram with mismatched columns",
                ));
            }
            self.hist_present[i] += shard_hist.present;
            self.hist_unbucketable[i] += shard_hist.unbucketable;
            for (&idx, &count) in shard_hist.bucket_index.iter().zip(&shard_hist.bucket_count) {
                *self.hist_buckets[i].entry(idx).or_insert(0) += count;
            }
            if self.hist_buckets[i].len() > spec.max_buckets as usize {
                return Err(Status::failed_precondition(format!(
                    "histogram {:?} exceeds {} buckets across the fleet; use a \
                     coarser interval or a tighter filter",
                    spec.name, spec.max_buckets
                )));
            }
        }
        Ok(())
    }

    /// Refuse a filter leaf, expression leaf, or group column no shard knows.
    pub(super) fn refuse_unknown(&self, filters: &RequestFilters) -> Result<(), Status> {
        let group_by = &self.compiled.group_by;
        self.known.refuse_unknown(filters)?;
        for (leaf, k) in self.leaves.iter().zip(&self.leaves_known) {
            if !k {
                return Err(Status::invalid_argument(format!(
                    "aggregation: no shard has column {}",
                    leaf.describe()
                )));
            }
        }
        if !self.group_column_known {
            return Err(Status::invalid_argument(format!(
                "group_by column {group_by:?} is not a facet column on any shard"
            )));
        }
        Ok(())
    }

    pub(super) fn results(&self) -> Result<Vec<crate::pb::AggregateResult>, Status> {
        let aggregations = &self.compiled.aggregations;
        let mut results = Vec::with_capacity(aggregations.len());
        for (agg, m) in aggregations.iter().zip(&self.merged) {
            results.push(m.result(&agg.name, crate::node::agg_op_of(agg.op)?)?);
        }
        Ok(results)
    }

    pub(super) fn group_results(&self) -> Result<Vec<crate::pb::AggregateGroup>, Status> {
        let aggregations = &self.compiled.aggregations;
        let mut group_results = Vec::with_capacity(self.groups.len());
        for (value, (group_matched, ms)) in &self.groups {
            let mut rs = Vec::with_capacity(aggregations.len());
            for (agg, m) in aggregations.iter().zip(ms) {
                rs.push(m.result(&agg.name, crate::node::agg_op_of(agg.op)?)?);
            }
            group_results.push(crate::pb::AggregateGroup {
                value: value.clone(),
                matched: *group_matched,
                results: rs,
            });
        }
        Ok(group_results)
    }

    pub(super) fn histogram_results(&self) -> Vec<crate::pb::HistogramResult> {
        let histograms = &self.compiled.histograms;
        histograms
            .iter()
            .enumerate()
            .map(|(i, spec)| crate::pb::HistogramResult {
                name: spec.name.clone(),
                buckets: self.hist_buckets[i]
                    .iter()
                    .map(|(&idx, &count)| {
                        if spec.calendar != 0 {
                            // A calendar bucket's key IS its start
                            // instant, in the expression's micros.
                            crate::pb::HistogramBucket {
                                lower: idx as f64,
                                lower_int: idx,
                                count,
                            }
                        } else {
                            crate::pb::HistogramBucket {
                                lower: idx as f64 * spec.interval,
                                lower_int: 0,
                                count,
                            }
                        }
                    })
                    .collect(),
                present: self.hist_present[i],
                unbucketable: self.hist_unbucketable[i],
            })
            .collect()
    }
}
