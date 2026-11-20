//! Boolean query execution and membership composition.

use super::*;

use crate::pb::boolean_plan_leaf::Leaf as L;

/// `(leaf, offset, count)`: where one root MUST filter leaf's flags sit in the
/// walk of the conjoined spine.
type SpineOffsets = [(usize, usize, usize)];

type BooleanTask =
    tokio::task::JoinHandle<Result<(usize, crate::pb::BooleanShardResponse), Status>>;

fn boolean_dense_fields(plan: &BooleanFanoutPlan) -> Vec<&str> {
    plan.leaves
        .iter()
        .filter_map(|leaf| match leaf.leaf.as_ref() {
            Some(L::Dense(dense)) => Some(dense.field.as_str()),
            _ => None,
        })
        .collect()
}

/// The spine: the root MUST filter leaves' trees conjoined, with each leaf's
/// flags at a known offset of the conjunction's walk.
fn boolean_spine(
    plan: &BooleanFanoutPlan,
) -> (Option<crate::pb::FilterExpr>, Vec<(usize, usize, usize)>) {
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
    (spine, spine_offsets)
}

/// One known-column accumulator per filter leaf; the spine leaves learn what
/// the mask resolved, at their own offsets.
fn spine_known(
    plan: &BooleanFanoutPlan,
    mask: Option<&crate::placement::ShardMask>,
    spine_offsets: &SpineOffsets,
) -> Vec<Option<FilterKnown>> {
    let mut known: Vec<Option<FilterKnown>> = plan
        .filters
        .iter()
        .map(|filters| filters.as_ref().map(FilterKnown::new))
        .collect();
    if let Some(mask) = mask {
        for &(leaf, base, count) in spine_offsets {
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
    known
}

/// The running merge of the consulted shards' Boolean answers.
struct BooleanFold<'a> {
    plan: &'a BooleanFanoutPlan,
    claims: &'a [StatsClaim],
    scope: crate::visibility::VisibilityScope,
    dense_fields: Vec<&'a str>,
    visibility_known: Vec<bool>,
    binding: Option<Option<crate::pb::MappedVectorBinding>>,
    known: Vec<Option<FilterKnown>>,
    stage_known: Vec<Option<Vec<bool>>>,
    candidates: Vec<crate::pb::BooleanCandidate>,
    prune: crate::segment_prune::PruneStats,
    folds: Vec<(usize, crate::pb::AggregateShardResponse)>,
}

impl BooleanFold<'_> {
    /// Refuse a filter column or score-stage column no consulted shard knows.
    fn refuse_unknown(&self) -> Result<(), Status> {
        for (index, acc) in self.known.iter().enumerate() {
            if let (Some(acc), Some(filters)) = (acc, self.plan.filters[index].as_ref()) {
                acc.refuse_unknown(filters)?;
            }
        }
        for &index in &self.plan.positive_lexical {
            let Some(L::Lexical(lexical)) = self.plan.leaves[index].leaf.as_ref() else {
                continue;
            };
            let flags = self.stage_known[index]
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
        Ok(())
    }
}

impl CoordinatorServiceImpl {
    /// Evaluate a Boolean plan across the fleet, pruning shards against the
    /// root's MUST filter spine, stripping placement-implied leaves per shard,
    /// and merging consulted shards' top-`depth` candidates.
    pub(crate) async fn evaluate_boolean_fanout(
        &self,
        plan: &BooleanFanoutPlan,
        claims: &[StatsClaim],
    ) -> Result<BooleanFanout, Status> {
        if claims.len() != self.node_addrs.len() {
            return Err(Status::internal(format!(
                "Boolean fan-out has {} epoch claims for {} shards",
                claims.len(),
                self.node_addrs.len()
            )));
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let dense_fields = boolean_dense_fields(plan);
        self.check_boolean_fields(plan)?;
        let (spine, spine_offsets) = boolean_spine(plan);
        let mask = (self.document_visibility.is_none()
            && dense_fields.iter().all(|field| field.is_empty()))
        .then(|| self.shard_mask(spine.as_ref()))
        .flatten();
        let shards_total = self.node_addrs.len() as u32;
        let shards_skipped = mask.as_ref().map_or(0, |m| m.skipped_count());
        let known = spine_known(plan, mask.as_ref(), &spine_offsets);
        let shard_requests =
            self.boolean_shard_requests(plan, claims, mask.as_ref(), &spine_offsets);
        let tasks = self.spawn_boolean_shards(&shard_requests)?;
        let mut fold = BooleanFold {
            plan,
            claims,
            visibility_known: vec![false; scope.column_count()],
            scope,
            dense_fields,
            binding: None,
            known,
            stage_known: plan.leaves.iter().map(|_| None).collect(),
            candidates: Vec::new(),
            prune: crate::segment_prune::PruneStats::default(),
            folds: Vec::new(),
        };
        for task in tasks {
            let (shard, response) = task
                .await
                .map_err(|e| Status::internal(format!("boolean evaluation task failed: {e}")))??;
            self.fold_boolean_response(&mut fold, shard, response)?;
        }
        fold.refuse_unknown()?;
        self.check_visibility_columns(&fold.visibility_known)?;
        let BooleanFold {
            mut candidates,
            prune,
            folds,
            ..
        } = fold;
        candidates.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        candidates.truncate(plan.depth as usize);
        let aggregate = self
            .merge_boolean_aggregate(plan, folds, &shard_requests)
            .await?;
        Ok(BooleanFanout {
            candidates,
            prune,
            shards_total,
            shards_skipped,
            aggregate,
        })
    }

    fn check_boolean_fields(&self, plan: &BooleanFanoutPlan) -> Result<(), Status> {
        if let Some(fields) = &self.field_permissions {
            for leaf in &plan.leaves {
                fields.boolean_leaf(leaf)?;
            }
            if let Some((_, aggregate)) = &plan.aggregate {
                fields.aggregate(&RequestFilters::default(), aggregate)?;
            }
        }
        Ok(())
    }

    /// Merge the folds the Boolean evaluation carried, when the plan
    /// aggregates.
    async fn merge_boolean_aggregate(
        &self,
        plan: &BooleanFanoutPlan,
        folds: Vec<(usize, crate::pb::AggregateShardResponse)>,
        shard_requests: &[Option<crate::pb::BooleanShardRequest>],
    ) -> Result<Option<crate::pb::AggregateResponse>, Status> {
        match plan.aggregate.as_ref() {
            Some((_, compiled)) => {
                let empty = RequestFilters::compile(&[], "")?;
                Ok(Some(
                    self.merge_aggregate_responses(
                        &empty,
                        compiled,
                        None,
                        folds,
                        PercentileScope::Boolean(shard_requests),
                    )
                    .await?,
                ))
            }
            None => Ok(None),
        }
    }

    /// Per shard: the plan with the spine leaves pruned of what the shard's
    /// placement implies; `None` for a shard the mask skips.
    fn boolean_shard_requests(
        &self,
        plan: &BooleanFanoutPlan,
        claims: &[StatsClaim],
        mask: Option<&crate::placement::ShardMask>,
        spine_offsets: &SpineOffsets,
    ) -> Vec<Option<crate::pb::BooleanShardRequest>> {
        let mut shard_requests: Vec<Option<crate::pb::BooleanShardRequest>> =
            Vec::with_capacity(self.node_addrs.len());
        for (shard, claim) in claims.iter().enumerate() {
            if mask.is_some_and(|m| m.skipped[shard]) {
                shard_requests.push(None);
                continue;
            }
            let mut leaves = plan.leaves.clone();
            if let Some(mask) = mask {
                for &(leaf, base, count) in spine_offsets {
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
        shard_requests
    }

    fn spawn_boolean_shards(
        &self,
        shard_requests: &[Option<crate::pb::BooleanShardRequest>],
    ) -> Result<Vec<BooleanTask>, Status> {
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
        Ok(tasks)
    }

    /// Fold one shard's answer under its admitted read version.
    fn fold_boolean_response(
        &self,
        fold: &mut BooleanFold<'_>,
        shard: usize,
        response: crate::pb::BooleanShardResponse,
    ) -> Result<(), Status> {
        let plan = fold.plan;
        let receipt = response.read_receipt.as_ref().ok_or_else(|| {
            Status::failed_precondition("Boolean response omitted its read receipt")
        })?;
        let actual =
            self.check_read_view(shard, &fold.scope, receipt, &mut fold.visibility_known)?;
        if actual != fold.claims[shard] || response.stats_epoch != actual.epoch {
            return Err(Status::failed_precondition(
                "Boolean response changed its admitted read version",
            ));
        }
        for field in &fold.dense_fields {
            Self::check_vector_binding(field, receipt.vector_binding.as_ref(), &mut fold.binding)?;
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
            if let Some(acc) = fold.known[index].as_mut() {
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
            let acc = fold.stage_known[index].get_or_insert_with(|| vec![false; flags.len()]);
            for (a, k) in acc.iter_mut().zip(flags) {
                *a |= *k;
            }
        }
        fold.candidates.extend(response.candidates);
        fold.prune.add(crate::segment_prune::PruneStats {
            segments_total: response.segments_total,
            segments_skipped: response.segments_skipped,
        });
        if let Some(shard_fold) = response.aggregate {
            fold.folds.push((shard, shard_fold));
        }
        Ok(())
    }
}
