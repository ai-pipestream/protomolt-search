//! Distributed aggregation and percentile reduction.

mod fold;
mod percentiles;

use self::fold::AggregateFold;
use super::*;

impl CoordinatorServiceImpl {
    /// Fan out an aggregation across shards and merge exact partials in shard
    /// order for bit-deterministic results, refusing unknown columns by name.
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
        let mut fold = AggregateFold::new(compiled, Self::filter_known(filters, mask));
        for (shard, response) in responses {
            self.check_read_view(shard, &visibility_scope, &response, &mut visibility_known)?;
            fold.fold_shard(shard, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        fold.refuse_unknown(filters)?;
        let results = fold.results()?;
        let group_results = fold.group_results()?;
        let hist_results = fold.histogram_results();
        let pct_results = self
            .solve_percentiles(
                filters,
                &compiled.percentile_specs,
                &compiled.percentiles,
                &fold.pct_merged,
                scope,
            )
            .await?;
        Ok(crate::pb::AggregateResponse {
            results,
            matched: fold.matched,
            groups: group_results,
            ungrouped: if compiled.group_by.is_empty() {
                0
            } else {
                fold.ungrouped
            },
            histograms: hist_results,
            percentiles: pct_results,
        })
    }
}
