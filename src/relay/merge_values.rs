//! Exact merges of value answers: aggregate folds and browse pages.

use std::collections::BTreeMap;

use tonic::Status;

use crate::coordinator::{AggMerge, PctMerge};
use crate::pb::{AggregateShardResponse, CompiledAggregation};

use super::merge_util::{add_count, merge_known, SegmentCounts};

/// What an aggregate fold was asked for.
struct AggSpec<'a> {
    aggregations: &'a [CompiledAggregation],
    group_by: &'a str,
    max_groups: u32,
    histograms: &'a [crate::pb::CompiledHistogram],
    percentiles: &'a [crate::pb::CompiledPercentile],
}

/// The running fold of the children's aggregate partials.
struct AggFold {
    geo_known: Option<Vec<bool>>,
    filter_known: Option<Vec<bool>>,
    leaves_known: Option<Vec<bool>>,
    matched: u64,
    ungrouped: u64,
    group_column_known: bool,
    merged: Vec<AggMerge>,
    groups: BTreeMap<String, (u64, Vec<AggMerge>)>,
    buckets: Vec<BTreeMap<i64, u64>>,
    present: Vec<u64>,
    unbucketable: Vec<u64>,
    pct: Vec<PctMerge>,
    segments: SegmentCounts,
}

fn fresh_merges(aggregations: &[CompiledAggregation]) -> Vec<AggMerge> {
    aggregations.iter().map(|_| AggMerge::new()).collect()
}

/// The children's `AggregateShard` answers folded into one shard's
/// answer, in child order: each partial folds through the root's own
/// fold (`crate::coordinator::AggMerge`) and comes back out as a
/// partial, groups join by value, histograms sum by bucket, percentile
/// partials fold, the match counts add with a check, and the known
/// flags OR as every child sizes them. The caps a root applies
/// (`max_groups`, `max_buckets`, `max_distinct`) apply here, so a relay
/// refuses where the root would. The receipt fields are the caller's to
/// fill. A relay whose children are a prefix of the root's shard order
/// (one relay over all of them, or a chain) folds bit for bit as the
/// root folds the shards; relays side by side fold their doubles
/// compensated at each level, which can differ from the flat fold in
/// the last bit (ints, counts, extrema, histograms, and percentile
/// partials are exact in any grouping).
pub fn merge_aggregate_shares(
    aggregations: &[CompiledAggregation],
    group_by: &str,
    max_groups: u32,
    histograms: &[crate::pb::CompiledHistogram],
    percentiles: &[crate::pb::CompiledPercentile],
    shares: &[AggregateShardResponse],
) -> Result<AggregateShardResponse, Status> {
    let spec = AggSpec {
        aggregations,
        group_by,
        max_groups,
        histograms,
        percentiles,
    };
    let mut fold = AggFold::new(&spec);
    for (child, share) in shares.iter().enumerate() {
        fold.check_shape(&spec, child, share)?;
        fold.add_counters(child, share)?;
        for (m, (p, agg)) in fold
            .merged
            .iter_mut()
            .zip(share.partials.iter().zip(aggregations))
        {
            m.fold(p, agg)?;
        }
        fold.fold_groups(&spec, child, share)?;
        fold.fold_histograms(&spec, child, share)?;
        for (m, (p, spec)) in fold
            .pct
            .iter_mut()
            .zip(share.percentile_partials.iter().zip(percentiles))
        {
            m.fold(p, &spec.name)?;
        }
    }
    Ok(fold.finish())
}

impl AggFold {
    fn new(spec: &AggSpec) -> Self {
        AggFold {
            geo_known: None,
            filter_known: None,
            leaves_known: None,
            matched: 0,
            ungrouped: 0,
            group_column_known: false,
            merged: fresh_merges(spec.aggregations),
            groups: BTreeMap::new(),
            buckets: spec.histograms.iter().map(|_| BTreeMap::new()).collect(),
            present: vec![0; spec.histograms.len()],
            unbucketable: vec![0; spec.histograms.len()],
            pct: spec.percentiles.iter().map(|_| PctMerge::new()).collect(),
            segments: SegmentCounts::default(),
        }
    }

    /// The known flags joined and the partial lists sized as requested.
    fn check_shape(
        &mut self,
        spec: &AggSpec,
        child: usize,
        share: &AggregateShardResponse,
    ) -> Result<(), Status> {
        merge_known("geo", child, &mut self.geo_known, &share.geo_columns_known)?;
        merge_known(
            "filter",
            child,
            &mut self.filter_known,
            &share.filter_columns_known,
        )?;
        merge_known(
            "expression-leaf",
            child,
            &mut self.leaves_known,
            &share.expr_leaves_known,
        )?;
        if share.partials.len() != spec.aggregations.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} aggregation partials for {} aggregations",
                share.partials.len(),
                spec.aggregations.len()
            )));
        }
        if share.histograms.len() != spec.histograms.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} histograms for {} requested",
                share.histograms.len(),
                spec.histograms.len()
            )));
        }
        if share.percentile_partials.len() != spec.percentiles.len() {
            return Err(Status::internal(format!(
                "relay: child {child} answered {} percentile partials for {} requested",
                share.percentile_partials.len(),
                spec.percentiles.len()
            )));
        }
        Ok(())
    }

    fn add_counters(&mut self, child: usize, share: &AggregateShardResponse) -> Result<(), Status> {
        self.group_column_known |= share.group_column_known;
        add_count(
            &mut self.matched,
            share.matched,
            child,
            "aggregate match count",
        )?;
        add_count(
            &mut self.ungrouped,
            share.ungrouped,
            child,
            "ungrouped count",
        )?;
        self.segments
            .add(child, share.segments_total, share.segments_skipped)
    }

    /// Groups join by value; past `max_groups` distinct values refuses.
    fn fold_groups(
        &mut self,
        spec: &AggSpec,
        child: usize,
        share: &AggregateShardResponse,
    ) -> Result<(), Status> {
        for group in &share.groups {
            if group.partials.len() != spec.aggregations.len() {
                return Err(Status::internal(format!(
                    "relay: child {child} answered {} group partials for {} aggregations",
                    group.partials.len(),
                    spec.aggregations.len()
                )));
            }
            let entry = self
                .groups
                .entry(group.value.clone())
                .or_insert_with(|| (0, fresh_merges(spec.aggregations)));
            add_count(&mut entry.0, group.matched, child, "group match count")?;
            for (m, (p, agg)) in entry
                .1
                .iter_mut()
                .zip(group.partials.iter().zip(spec.aggregations))
            {
                m.fold(p, agg)?;
            }
            if self.groups.len() > spec.max_groups as usize {
                return Err(Status::failed_precondition(format!(
                    "group_by {:?} exceeds {} distinct values across the \
                     relay's children; tighten the filter or raise max_groups",
                    spec.group_by, spec.max_groups
                )));
            }
        }
        Ok(())
    }

    /// Histograms sum by bucket; past a histogram's `max_buckets` refuses.
    fn fold_histograms(
        &mut self,
        spec: &AggSpec,
        child: usize,
        share: &AggregateShardResponse,
    ) -> Result<(), Status> {
        for (i, (hist, hspec)) in share.histograms.iter().zip(spec.histograms).enumerate() {
            if hist.bucket_index.len() != hist.bucket_count.len() {
                return Err(Status::internal(format!(
                    "relay: child {child} answered a histogram with mismatched columns"
                )));
            }
            add_count(
                &mut self.present[i],
                hist.present,
                child,
                "histogram present count",
            )?;
            add_count(
                &mut self.unbucketable[i],
                hist.unbucketable,
                child,
                "histogram unbucketable count",
            )?;
            for (&index, &count) in hist.bucket_index.iter().zip(&hist.bucket_count) {
                let bucket = self.buckets[i].entry(index).or_insert(0);
                add_count(bucket, count, child, "histogram bucket count")?;
            }
            if self.buckets[i].len() > hspec.max_buckets as usize {
                return Err(Status::failed_precondition(format!(
                    "histogram {:?} exceeds {} buckets across the relay's children; use a \
                     coarser interval or a tighter filter",
                    hspec.name, hspec.max_buckets
                )));
            }
        }
        Ok(())
    }

    fn finish(self) -> AggregateShardResponse {
        let histograms = self
            .buckets
            .iter()
            .enumerate()
            .map(|(i, b)| crate::pb::ShardHistogram {
                bucket_index: b.keys().copied().collect(),
                bucket_count: b.values().copied().collect(),
                present: self.present[i],
                unbucketable: self.unbucketable[i],
            })
            .collect();
        AggregateShardResponse {
            partials: self.merged.iter().map(AggMerge::partial).collect(),
            matched: self.matched,
            geo_columns_known: self.geo_known.unwrap_or_default(),
            filter_columns_known: self.filter_known.unwrap_or_default(),
            expr_leaves_known: self.leaves_known.unwrap_or_default(),
            groups: self
                .groups
                .into_iter()
                .map(|(value, (matched, ms))| crate::pb::AggregateShardGroup {
                    value,
                    matched,
                    partials: ms.iter().map(AggMerge::partial).collect(),
                })
                .collect(),
            ungrouped: self.ungrouped,
            group_column_known: self.group_column_known,
            histograms,
            percentile_partials: self.pct.iter().map(PctMerge::partial).collect(),
            segments_total: self.segments.total,
            segments_skipped: self.segments.skipped,
            stats_epoch: 0,
            stats_incarnation: Vec::new(),
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
        }
    }
}
