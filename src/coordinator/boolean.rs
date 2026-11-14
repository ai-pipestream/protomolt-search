//! Boolean query execution and membership composition.

use super::*;

impl CoordinatorServiceImpl {
    /// One shard-side Boolean evaluation over the fleet
    /// (`docs/query-api.md`, "Recursive boolean execution"). The root's
    /// MUST filter leaves are the AND spine the placement tree prunes
    /// shards by: a shard the spine excludes is not asked, and a leaf a
    /// shard's placement implies is dropped from that shard's copy of
    /// the leaf. Each consulted shard answers its best `depth` members;
    /// the merge is the ranked union cut to `depth`. `claims[shard]` is
    /// that shard's stats-epoch claim for the lexical leaves.
    pub(crate) async fn evaluate_boolean_fanout(
        &self,
        plan: &BooleanFanoutPlan,
        claims: &[StatsClaim],
    ) -> Result<BooleanFanout, Status> {
        use crate::pb::boolean_plan_leaf::Leaf as L;
        if claims.len() != self.node_addrs.len() {
            return Err(Status::internal(format!(
                "Boolean fan-out has {} epoch claims for {} shards",
                claims.len(),
                self.node_addrs.len()
            )));
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut binding = None;
        let dense_fields: Vec<&str> = plan
            .leaves
            .iter()
            .filter_map(|leaf| match leaf.leaf.as_ref() {
                Some(L::Dense(dense)) => Some(dense.field.as_str()),
                _ => None,
            })
            .collect();
        if let Some(fields) = &self.field_permissions {
            for leaf in &plan.leaves {
                fields.boolean_leaf(leaf)?;
            }
            if let Some((_, aggregate)) = &plan.aggregate {
                fields.aggregate(&RequestFilters::default(), aggregate)?;
            }
        }
        // The spine: the root MUST filter leaves' trees conjoined, with
        // each leaf's flags at a known offset of the conjunction's walk.
        let mut spine_trees = Vec::new();
        let mut spine_offsets: Vec<(usize, usize, usize)> = Vec::new();
        let mut offset = 0usize;
        for &leaf in &plan.root_must_filters {
            let Some(tree) = plan
                .filters
                .get(leaf)
                .and_then(|f| f.as_ref()?.tree.as_ref())
            else {
                continue;
            };
            let count = crate::filter::leaf_count(tree);
            spine_offsets.push((leaf, offset, count));
            spine_trees.push(tree.clone());
            offset += count;
        }
        let spine = match spine_trees.len() {
            0 => None,
            1 => spine_trees.pop(),
            _ => Some(crate::pb::FilterExpr {
                expr: Some(crate::pb::filter_expr::Expr::And(crate::pb::FilterList {
                    exprs: spine_trees,
                })),
            }),
        };
        let mask = (self.document_visibility.is_none()
            && dense_fields.iter().all(|field| field.is_empty()))
        .then(|| self.shard_mask(spine.as_ref()))
        .flatten();
        let shards_total = self.node_addrs.len() as u32;
        let shards_skipped = mask.as_ref().map_or(0, |m| m.skipped_count());
        // One known-column accumulator per filter leaf; the spine leaves
        // learn what the mask resolved, at their own offsets.
        let mut known: Vec<Option<FilterKnown>> = plan
            .filters
            .iter()
            .map(|filters| filters.as_ref().map(FilterKnown::new))
            .collect();
        if let Some(mask) = mask.as_ref() {
            for &(leaf, base, count) in &spine_offsets {
                let Some(acc) = known[leaf].as_mut() else {
                    continue;
                };
                let local: Vec<usize> = mask
                    .known
                    .iter()
                    .filter(|&&index| index >= base && index < base + count)
                    .map(|&index| index - base)
                    .collect();
                mark_known(&mut acc.tree, &local);
                acc.kept = mask
                    .implied
                    .iter()
                    .map(|dropped| {
                        let local: Vec<usize> = dropped
                            .iter()
                            .filter(|&&index| index >= base && index < base + count)
                            .map(|&index| index - base)
                            .collect();
                        (!local.is_empty())
                            .then(|| (0..count).filter(|index| !local.contains(index)).collect())
                    })
                    .collect();
            }
        }
        // Per shard: the plan with the spine leaves pruned of what the
        // shard's placement implies.
        let mut shard_requests: Vec<Option<crate::pb::BooleanShardRequest>> =
            Vec::with_capacity(self.node_addrs.len());
        for (shard, claim) in claims.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                shard_requests.push(None);
                continue;
            }
            let mut leaves = plan.leaves.clone();
            if let Some(mask) = mask.as_ref() {
                for &(leaf, base, count) in &spine_offsets {
                    let local: Vec<usize> = mask.implied[shard]
                        .iter()
                        .filter(|&&index| index >= base && index < base + count)
                        .map(|&index| index - base)
                        .collect();
                    if local.is_empty() {
                        continue;
                    }
                    if let Some(L::Filter(filter)) = leaves[leaf].leaf.as_mut() {
                        filter.filter = filter
                            .filter
                            .as_ref()
                            .and_then(|tree| crate::placement::without_leaves(tree, &local));
                    }
                }
            }
            shard_requests.push(Some(crate::pb::BooleanShardRequest {
                root: Some(plan.root.clone()),
                leaves,
                depth: plan.depth,
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                visibility: self.document_visibility.clone(),
                max_logical_bytes: self.max_rerank_bytes,
                exact_batch: self.signal_batch() as u32,
                aggregate: plan.aggregate.as_ref().map(|(spec, _)| spec.clone()),
            }));
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, request) in shard_requests.iter().enumerate() {
            let Some(request) = request.clone() else {
                continue;
            };
            let mut client = self.node_client(&self.node_addrs[shard])?;
            let deadline = self.limits.shard_deadline;
            tasks.push(tokio::spawn(async move {
                let call = client.evaluate_boolean(request);
                let response = match deadline {
                    Some(limit) => tokio::time::timeout(limit, call)
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded("boolean evaluation shard deadline exceeded")
                        })?
                        .map(tonic::Response::into_inner),
                    None => call.await.map(tonic::Response::into_inner),
                }?;
                Ok::<_, Status>((shard, response))
            }));
        }
        let mut candidates = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        let mut stage_known: Vec<Option<Vec<bool>>> = plan.leaves.iter().map(|_| None).collect();
        let mut folds = Vec::new();
        for task in tasks {
            let (shard, response) = task
                .await
                .map_err(|e| Status::internal(format!("boolean evaluation task failed: {e}")))??;
            let receipt = response.read_receipt.as_ref().ok_or_else(|| {
                Status::failed_precondition("Boolean response omitted its read receipt")
            })?;
            let actual = self.check_read_view(shard, &scope, receipt, &mut visibility_known)?;
            if actual != claims[shard] || response.stats_epoch != actual.epoch {
                return Err(Status::failed_precondition(
                    "Boolean response changed its admitted read version",
                ));
            }
            for field in &dense_fields {
                Self::check_vector_binding(field, receipt.vector_binding.as_ref(), &mut binding)?;
            }
            if response.filters_known.len() != plan.leaves.len()
                || response.stages_known.len() != plan.leaves.len()
            {
                return Err(Status::internal(format!(
                    "shard {shard} answered {} filter and {} stage flag lists for {} leaves",
                    response.filters_known.len(),
                    response.stages_known.len(),
                    plan.leaves.len()
                )));
            }
            for (index, flags) in response.filters_known.iter().enumerate() {
                if let Some(acc) = known[index].as_mut() {
                    acc.merge_shard(shard, &flags.geo_columns_known, &flags.filter_columns_known)?;
                }
            }
            for &index in &plan.positive_lexical {
                let Some(L::Lexical(lexical)) = plan.leaves[index].leaf.as_ref() else {
                    continue;
                };
                let flags = &response.stages_known[index].stage_columns_known;
                if flags.len() != lexical.score_stages.len() {
                    return Err(Status::failed_precondition(format!(
                        "shard {shard} returned {} stage-known flags for {} stages",
                        flags.len(),
                        lexical.score_stages.len()
                    )));
                }
                let acc = stage_known[index].get_or_insert_with(|| vec![false; flags.len()]);
                for (a, k) in acc.iter_mut().zip(flags) {
                    *a |= *k;
                }
            }
            candidates.extend(response.candidates);
            prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            if let Some(fold) = response.aggregate {
                folds.push((shard, fold));
            }
        }
        for (index, acc) in known.iter().enumerate() {
            if let (Some(acc), Some(filters)) = (acc, plan.filters[index].as_ref()) {
                acc.refuse_unknown(filters)?;
            }
        }
        for &index in &plan.positive_lexical {
            let Some(L::Lexical(lexical)) = plan.leaves[index].leaf.as_ref() else {
                continue;
            };
            let flags = stage_known[index]
                .clone()
                .unwrap_or_else(|| vec![false; lexical.score_stages.len()]);
            for (stage, known) in lexical.score_stages.iter().zip(flags) {
                if !known {
                    return Err(Status::invalid_argument(format!(
                        "no shard has numeric column {}: the score stage would be a silent no-op",
                        stage.column
                    )));
                }
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        candidates.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        candidates.truncate(plan.depth as usize);
        let aggregate = match plan.aggregate.as_ref() {
            Some((_, compiled)) => {
                let empty = RequestFilters::compile(&[], "")?;
                Some(
                    self.merge_aggregate_responses(
                        &empty,
                        compiled,
                        None,
                        folds,
                        PercentileScope::Boolean(&shard_requests),
                    )
                    .await?,
                )
            }
            None => None,
        };
        Ok(BooleanFanout {
            candidates,
            prune,
            shards_total,
            shards_skipped,
            aggregate,
        })
    }
}
