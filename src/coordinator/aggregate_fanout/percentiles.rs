//! Exact nearest-rank percentiles over count-below rounds.

use super::*;

/// One requested percentile's search window in the order-bits domain.
struct Target {
    spec: usize,
    pct_index: usize,
    k: u64,
    lo: u64,
    hi: u64,
}

/// Nearest rank: the k-th smallest, k = ceil(p/100 * n) clamped into [1, n].
fn percentile_targets(specs: &[crate::pb::PercentileSpec], merged: &[PctMerge]) -> Vec<Target> {
    let mut targets = Vec::new();
    for (si, (spec, m)) in specs.iter().zip(merged).enumerate() {
        if m.present == 0 {
            continue;
        }
        for (pi, &p) in spec.percentiles.iter().enumerate() {
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
    targets
}

fn percentile_results(
    specs: &[crate::pb::PercentileSpec],
    merged: &[PctMerge],
    targets: &[Target],
) -> Vec<crate::pb::PercentileResult> {
    let mut answers: Vec<Vec<Option<(u64, u64)>>> = specs
        .iter()
        .map(|s| vec![None; s.percentiles.len()])
        .collect();
    for t in targets {
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
    results
}

type QuantileTask = tokio::task::JoinHandle<Result<crate::pb::QuantileCountsResponse, Status>>;

impl CoordinatorServiceImpl {
    /// Solve exact nearest-rank percentiles simultaneously across at most 64
    /// count-below binary-search rounds in the order-bits domain.
    pub(super) async fn solve_percentiles(
        &self,
        filters: &RequestFilters,
        specs: &[crate::pb::PercentileSpec],
        compiled: &[crate::pb::CompiledPercentile],
        merged: &[PctMerge],
        scope: PercentileScope<'_>,
    ) -> Result<Vec<crate::pb::PercentileResult>, Status> {
        let mut targets = percentile_targets(specs, merged);
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
        Ok(percentile_results(specs, merged, &targets))
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
        self.sum_quantile_counts(&scope, tasks, targets.len()).await
    }

    /// Sum the shards' counts per target under the read-view checks.
    async fn sum_quantile_counts(
        &self,
        scope: &crate::visibility::VisibilityScope,
        tasks: Vec<(usize, QuantileTask)>,
        targets: usize,
    ) -> Result<Vec<u64>, Status> {
        let mut visibility_known = vec![false; scope.column_count()];
        let mut totals = vec![0u64; targets];
        for (shard, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("quantile task failed: {e}")))??;
            self.check_read_view(shard, scope, &response, &mut visibility_known)?;
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
