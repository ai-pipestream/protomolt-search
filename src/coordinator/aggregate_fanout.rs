//! Distributed aggregation and percentile reduction.

use super::*;

impl CoordinatorServiceImpl {
    /// Aggregation fan-out (docs/aggregations.md): every shard folds
    /// exact partials over its admitted documents; the coordinator
    /// merges them IN SHARD ORDER so the folds are deterministic
    /// bit-for-bit across runs. The typo rules hold as on every
    /// filtered route, and expression column leaves follow the
    /// projection contract: a leaf NO shard knows refuses by name.
    pub(crate) async fn fanout_aggregate(
        &self,
        filters: &RequestFilters,
        compiled: &CompiledAggregate,
        doc_ids: Option<&[u64]>,
    ) -> Result<crate::pb::AggregateResponse, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.aggregate(filters, compiled)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_aggregate(filters, compiled, doc_ids)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            group_by,
            max_groups,
            ..
        } = compiled;
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let claim = self.query_read_versions.as_ref().expect("pinned read set")[shard];
            let request = crate::pb::AggregateShardRequest {
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                geo_filters: filters.geo.clone(),
                aggregations: aggregations.to_vec(),
                group_by: group_by.to_string(),
                max_groups: *max_groups,
                histograms: histograms.to_vec(),
                percentiles: percentiles.to_vec(),
                doc_ids: doc_ids.unwrap_or_default().to_vec(),
                restrict_doc_ids: doc_ids.is_some(),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .aggregate_shard(request)
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        let mut responses = Vec::with_capacity(tasks.len());
        for (shard, task) in tasks {
            responses.push((
                shard,
                task.await
                    .map_err(|e| Status::internal(format!("aggregate task failed: {e}")))??,
            ));
        }
        self.merge_aggregate_responses(
            filters,
            compiled,
            mask.as_ref(),
            responses,
            PercentileScope::Ids(doc_ids),
        )
        .await
    }

    /// The merge of the shards' folds (`AggregateShard` answers, or the
    /// folds a Boolean evaluation carried) into one response: partials
    /// fold in shard order, groups join by value, histograms sum by
    /// bucket, and the percentiles converge over count-below rounds
    /// under `scope`.
    pub(crate) async fn merge_aggregate_responses(
        &self,
        filters: &RequestFilters,
        compiled: &CompiledAggregate,
        mask: Option<&crate::placement::ShardMask>,
        responses: Vec<(usize, crate::pb::AggregateShardResponse)>,
        scope: PercentileScope<'_>,
    ) -> Result<crate::pb::AggregateResponse, Status> {
        let visibility_scope =
            crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; visibility_scope.column_count()];
        let CompiledAggregate {
            aggregations,
            histograms,
            percentiles,
            percentile_specs,
            group_by,
            max_groups,
        } = compiled;
        let grouping = !group_by.is_empty();
        let group_cap = *max_groups as usize;
        // The same leaf enumeration the shards answer positionally:
        // aggregations first, then histograms.
        let mut leaves = Vec::new();
        for expr in aggregations
            .iter()
            .filter_map(|a| a.expr.as_ref())
            .chain(histograms.iter().filter_map(|h| h.expr.as_ref()))
            .chain(percentiles.iter().filter_map(|p| p.expr.as_ref()))
        {
            crate::values::column_leaves(expr, &mut leaves);
        }
        let mut known = Self::filter_known(filters, mask);
        let mut leaves_known = vec![false; leaves.len()];
        let mut group_column_known = !grouping;
        let mut matched = 0u64;
        let mut ungrouped = 0u64;
        let mut merged: Vec<AggMerge> = aggregations.iter().map(|_| AggMerge::new()).collect();
        // Groups merge by VALUE: the BTreeMap is both the cross-shard
        // join and the deterministic ascending order the response
        // promises.
        let mut groups: std::collections::BTreeMap<String, (u64, Vec<AggMerge>)> =
            std::collections::BTreeMap::new();
        let mut hist_buckets: Vec<std::collections::BTreeMap<i64, u64>> =
            histograms.iter().map(|_| Default::default()).collect();
        let mut hist_present = vec![0u64; histograms.len()];
        let mut hist_unbucketable = vec![0u64; histograms.len()];
        let mut pct_merged: Vec<PctMerge> = percentiles.iter().map(|_| PctMerge::new()).collect();
        for (shard, response) in responses {
            self.check_read_view(shard, &visibility_scope, &response, &mut visibility_known)?;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            if response.expr_leaves_known.len() != leaves_known.len() {
                return Err(Status::internal(format!(
                    "shard answered {} expression-leaf flags for {} leaves",
                    response.expr_leaves_known.len(),
                    leaves_known.len()
                )));
            }
            for (acc, k) in leaves_known.iter_mut().zip(&response.expr_leaves_known) {
                *acc |= *k;
            }
            if response.partials.len() != merged.len() {
                return Err(Status::internal(format!(
                    "shard answered {} aggregation partials for {} aggregations",
                    response.partials.len(),
                    merged.len()
                )));
            }
            group_column_known |= response.group_column_known;
            matched += response.matched;
            ungrouped += response.ungrouped;
            for (m, (p, agg)) in merged
                .iter_mut()
                .zip(response.partials.iter().zip(aggregations))
            {
                m.fold(p, agg)?;
            }
            for shard_group in &response.groups {
                if shard_group.partials.len() != aggregations.len() {
                    return Err(Status::internal(format!(
                        "shard answered {} group partials for {} aggregations",
                        shard_group.partials.len(),
                        aggregations.len()
                    )));
                }
                let entry = groups
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
                if groups.len() > group_cap {
                    return Err(Status::failed_precondition(format!(
                        "group_by {group_by:?} exceeds {group_cap} distinct values \
                         across the fleet; tighten the filter or raise max_groups"
                    )));
                }
            }
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
            for (m, (p, spec)) in pct_merged
                .iter_mut()
                .zip(response.percentile_partials.iter().zip(percentiles))
            {
                m.fold(p, &spec.name)?;
            }
            for (i, (shard_hist, spec)) in response.histograms.iter().zip(histograms).enumerate() {
                if shard_hist.bucket_index.len() != shard_hist.bucket_count.len() {
                    return Err(Status::internal(
                        "shard answered a histogram with mismatched columns",
                    ));
                }
                hist_present[i] += shard_hist.present;
                hist_unbucketable[i] += shard_hist.unbucketable;
                for (&idx, &count) in shard_hist.bucket_index.iter().zip(&shard_hist.bucket_count) {
                    *hist_buckets[i].entry(idx).or_insert(0) += count;
                }
                if hist_buckets[i].len() > spec.max_buckets as usize {
                    return Err(Status::failed_precondition(format!(
                        "histogram {:?} exceeds {} buckets across the fleet; use a \
                         coarser interval or a tighter filter",
                        spec.name, spec.max_buckets
                    )));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        for (leaf, k) in leaves.iter().zip(&leaves_known) {
            if !k {
                return Err(Status::invalid_argument(format!(
                    "aggregation: no shard has column {}",
                    leaf.describe()
                )));
            }
        }
        if !group_column_known {
            return Err(Status::invalid_argument(format!(
                "group_by column {group_by:?} is not a facet column on any shard"
            )));
        }
        let mut results = Vec::with_capacity(aggregations.len());
        for (agg, m) in aggregations.iter().zip(&merged) {
            results.push(m.result(&agg.name, crate::node::agg_op_of(agg.op)?)?);
        }
        let mut group_results = Vec::with_capacity(groups.len());
        for (value, (group_matched, ms)) in &groups {
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
        let hist_results = histograms
            .iter()
            .enumerate()
            .map(|(i, spec)| crate::pb::HistogramResult {
                name: spec.name.clone(),
                buckets: hist_buckets[i]
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
                present: hist_present[i],
                unbucketable: hist_unbucketable[i],
            })
            .collect();
        let pct_results = self
            .solve_percentiles(filters, percentile_specs, percentiles, &pct_merged, scope)
            .await?;
        Ok(crate::pb::AggregateResponse {
            results,
            matched,
            groups: group_results,
            ungrouped: if grouping { ungrouped } else { 0 },
            histograms: hist_results,
            percentiles: pct_results,
        })
    }

    /// The exact-percentile binary search (docs/aggregations.md): every
    /// requested (spec, percentile) target converges simultaneously
    /// over at most 64 count-below rounds in the order-bits domain, and
    /// each answer is the nearest-rank order statistic — a value some
    /// admitted document actually holds, never an interpolation.
    pub(super) async fn solve_percentiles(
        &self,
        filters: &RequestFilters,
        specs: &[crate::pb::PercentileSpec],
        compiled: &[crate::pb::CompiledPercentile],
        merged: &[PctMerge],
        scope: PercentileScope<'_>,
    ) -> Result<Vec<crate::pb::PercentileResult>, Status> {
        struct Target {
            spec: usize,
            pct_index: usize,
            k: u64,
            lo: u64,
            hi: u64,
        }
        let mut targets = Vec::new();
        for (si, (spec, m)) in specs.iter().zip(merged).enumerate() {
            if m.present == 0 {
                continue;
            }
            for (pi, &p) in spec.percentiles.iter().enumerate() {
                // Nearest rank: the k-th smallest, k = ceil(p/100 * n)
                // clamped into [1, n].
                let k = nearest_percentile_rank(p, m.present);
                targets.push(Target {
                    spec: si,
                    pct_index: pi,
                    k,
                    lo: m.min_bits,
                    hi: m.max_bits,
                });
            }
        }
        // Invariant per target: the answer (the smallest bits value b
        // with count(<= b) >= k) lies in [lo, hi]. Bit space is 64
        // wide, so at most 64 rounds close every window.
        while targets.iter().any(|t| t.lo < t.hi) {
            let active: Vec<usize> = (0..targets.len())
                .filter(|&i| targets[i].lo < targets[i].hi)
                .collect();
            let probes: Vec<crate::pb::QuantileTarget> = active
                .iter()
                .map(|&i| {
                    let t = &targets[i];
                    crate::pb::QuantileTarget {
                        expr_index: t.spec as u32,
                        threshold_bits: t.lo + (t.hi - t.lo) / 2,
                    }
                })
                .collect();
            let counts = self
                .quantile_round(filters, compiled, &probes, scope)
                .await?;
            for (&i, (probe, count)) in active.iter().zip(probes.iter().zip(counts)) {
                let t = &mut targets[i];
                if count >= t.k {
                    t.hi = probe.threshold_bits;
                } else {
                    t.lo = probe.threshold_bits + 1;
                }
            }
        }
        let mut answers: Vec<Vec<Option<(u64, u64)>>> = specs
            .iter()
            .map(|s| vec![None; s.percentiles.len()])
            .collect();
        for t in &targets {
            answers[t.spec][t.pct_index] = Some((t.k, t.lo));
        }
        let mut results = Vec::with_capacity(specs.len());
        for (si, (spec, m)) in specs.iter().zip(merged).enumerate() {
            use crate::pb::percentile_value::Value as W;
            let int_typed = m.vt == Some(crate::pb::AggregateValueType::Int);
            let values = spec
                .percentiles
                .iter()
                .enumerate()
                .map(|(pi, &p)| {
                    let (rank, value) = match answers[si][pi] {
                        None => (0, None),
                        Some((k, bits)) => (
                            k,
                            Some(if int_typed {
                                W::IntValue(crate::node::i64_from_order_bits(bits))
                            } else if m.vt == Some(crate::pb::AggregateValueType::Uint) {
                                W::UintValue(bits)
                            } else {
                                W::DoubleValue(crate::node::f64_from_order_bits(bits))
                            }),
                        ),
                    };
                    crate::pb::PercentileValue {
                        percentile: p,
                        rank,
                        value,
                    }
                })
                .collect();
            results.push(crate::pb::PercentileResult {
                name: spec.name.clone(),
                present: m.present,
                unrankable: m.unrankable,
                values,
            });
        }
        Ok(results)
    }

    /// One count-below round against every shard, counts summed per
    /// target.
    pub(super) async fn quantile_round(
        &self,
        filters: &RequestFilters,
        exprs: &[crate::pb::CompiledPercentile],
        targets: &[crate::pb::QuantileTarget],
        selection_scope: PercentileScope<'_>,
    ) -> Result<Vec<u64>, Status> {
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let claims = self
            .query_read_versions
            .as_ref()
            .ok_or_else(|| Status::internal("quantile rounds require a pinned read set"))?;
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = match selection_scope {
                PercentileScope::Ids(doc_ids) => crate::pb::QuantileCountsRequest {
                    visibility: self.document_visibility.clone(),
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                    geo_filters: filters.geo.clone(),
                    exprs: exprs.to_vec(),
                    targets: targets.to_vec(),
                    doc_ids: doc_ids.unwrap_or_default().to_vec(),
                    restrict_doc_ids: doc_ids.is_some(),
                    boolean: None,
                },
                PercentileScope::Boolean(plans) => {
                    // A shard the Boolean plan skipped counts nothing.
                    let Some(plan) = plans.get(shard).and_then(|p| p.as_ref()) else {
                        continue;
                    };
                    crate::pb::QuantileCountsRequest {
                        visibility: self.document_visibility.clone(),
                        expected_stats_epoch: claims[shard].epoch,
                        expected_stats_incarnation: claims[shard].incarnation(),
                        filter: None,
                        geo_filters: Vec::new(),
                        exprs: exprs.to_vec(),
                        targets: targets.to_vec(),
                        doc_ids: Vec::new(),
                        restrict_doc_ids: false,
                        boolean: Some(plan.clone()),
                    }
                }
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .quantile_counts(request)
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        let mut totals = vec![0u64; targets.len()];
        for (shard, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("quantile task failed: {e}")))??;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            if response.counts.len() != totals.len() {
                return Err(Status::internal(format!(
                    "shard answered {} quantile counts for {} targets",
                    response.counts.len(),
                    totals.len()
                )));
            }
            for (total, c) in totals.iter_mut().zip(&response.counts) {
                *total += *c;
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok(totals)
    }
}
