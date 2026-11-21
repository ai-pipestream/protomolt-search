//! Filter/lexical/vector bitmap, boolean evaluation, and aggregation RPCs.

use crate::node::*;

impl NodeServiceImpl {
    pub(super) async fn handle_resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        crate::metrics::timed(Route::ResolveFilterBitmap, request, |request| async move {
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let filter =
                crate::visibility::intersect_filter(req.visibility.as_ref(), req.filter.clone())?;
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = read_shard(&self.state);
            let (geo_columns_known, filter_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
            let (_, visibility_columns_known) = filter_known_flags(
                guard.bm25.as_ref(),
                &[],
                req.visibility
                    .as_ref()
                    .and_then(|view| view.filter.as_ref()),
            );
            let label_count = guard
                .bm25
                .as_ref()
                .map_or(0, |store| store.next_doc_id() as usize);
            let (_, allow, prune) = resolve_shard_filters(
                guard.bm25.as_ref(),
                guard.live_docs.words(),
                label_count,
                ShardFilterInputs {
                    geo: &req.geo_filters,
                    regions: &geo_regions,
                    tree: filter.as_ref(),
                },
                self.knobs.segment_pruning(),
                None,
            )?;
            let allow = allow.unwrap_or_else(|| crate::boolean_bits::Bits::full(label_count));
            let mut bits = vec![0u8; label_count.div_ceil(8)];
            for position in allow.iter() {
                bits[position / 8] |= 1 << (position % 8);
            }
            Ok(Response::new(crate::pb::FilterBitmapResponse {
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known,
                base_label: self.config.slot_offset,
                label_count: label_count as u64,
                bits,
                geo_columns_known,
                filter_columns_known,
                segments_total: prune.segments_total,
                segments_skipped: prune.segments_skipped,
            }))
        })
        .await
    }

    fn resolve_visibility_bits(
        &self,
        guard: &ShardState,
        label_count: usize,
        view_filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<Option<crate::boolean_bits::Bits>, Status> {
        let Some(filter) = view_filter else {
            return Ok(None);
        };
        Ok(resolve_shard_filters(
            guard.bm25.as_ref(),
            guard.live_docs.words(),
            label_count,
            ShardFilterInputs {
                geo: &[],
                regions: &[],
                tree: Some(filter),
            },
            self.knobs.segment_pruning(),
            None,
        )?
        .1)
    }

    fn collect_lexical_bitmap(
        &self,
        shard: &Bm25Shard,
        index: &dyn Bm25Index,
        guard: &ShardState,
        visible: Option<&crate::boolean_bits::Bits>,
        label_count: usize,
        terms: &[String],
    ) -> (Vec<u8>, crate::segment_prune::PruneStats) {
        let pruning = self.knobs.segment_pruning();
        let (masked, prune) = match shard {
            Bm25Shard::Segmented(segmented) => {
                let lacking = segmented.parts_lacking_terms(0, terms);
                let skipped = lacking.iter().filter(|&&lacks| lacks).count() as u32;
                let stats = crate::segment_prune::PruneStats {
                    segments_total: lacking.len() as u32,
                    segments_skipped: if pruning { skipped } else { 0 },
                };
                let masked = (pruning && skipped > 0).then(|| {
                    segmented.masked(lacking.iter().map(|&lacks| !lacks).collect::<Arc<[bool]>>())
                });
                (masked, stats)
            }
            _ => (None, crate::segment_prune::PruneStats::default()),
        };
        let index: &dyn Bm25Index = match masked.as_ref() {
            Some(masked) => masked,
            None => index,
        };
        let mut bits = vec![0u8; label_count.div_ceil(8)];
        for term in terms {
            index.for_each_doc_tf(term, &mut |doc_id, _tf| {
                let position = doc_id as usize;
                if position < label_count
                    && !guard.live_docs.is_deleted(position)
                    && visible.is_none_or(|allowed| allowed.test(position))
                {
                    bits[position / 8] |= 1 << (position % 8);
                }
            });
        }
        (bits, prune)
    }

    pub(super) async fn handle_resolve_lexical_bitmap(
        &self,
        request: Request<LexicalBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        let req = request.into_inner();
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let guard = read_shard(&self.state);
        let view_filter = req
            .visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref());
        let (_, visibility_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], view_filter);
        guard.check_query_analysis("body", req.analysis_fingerprint)?;
        let Some(shard) = guard.bm25.as_ref() else {
            return Ok(Response::new(MembershipBitmapResponse {
                vector_binding: None,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known,
                segments_total: 0,
                segments_skipped: 0,
                base_label: self.config.slot_offset,
                label_count: 0,
                bits: Vec::new(),
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
            }));
        };
        let index = shard.as_index();
        let label_count = usize::try_from(shard.next_doc_id())
            .map_err(|_| Status::resource_exhausted("lexical row count does not fit usize"))?;
        let visible = self.resolve_visibility_bits(&guard, label_count, view_filter)?;
        let (bits, prune) = self.collect_lexical_bitmap(
            shard,
            index,
            &guard,
            visible.as_ref(),
            label_count,
            &req.terms,
        );
        Ok(Response::new(MembershipBitmapResponse {
            vector_binding: None,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
            base_label: self.config.slot_offset,
            label_count: label_count as u64,
            bits,
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            segments_total: prune.segments_total,
            segments_skipped: prune.segments_skipped,
        }))
    }

    pub(super) async fn handle_resolve_vector_bitmap(
        &self,
        request: Request<VectorBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        let req = request.into_inner();
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let guard = read_shard(&self.state);
        let vector_binding = guard.vector_binding(&req.field)?;
        let view_filter = req
            .visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref());
        let (_, visibility_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], view_filter);
        let label_count = guard.index.as_ref().map_or(0, VectorIndex::len);
        let visible = self.resolve_visibility_bits(&guard, label_count, view_filter)?;
        let documents = guard
            .bm25
            .as_ref()
            .map_or(0, |store| store.next_doc_id() as usize);
        let mut bits = vec![0u8; label_count.div_ceil(8)];
        for (base, rows) in guard
            .index
            .as_ref()
            .map_or_else(Vec::new, VectorIndex::vector_rows)
        {
            for position in base..base.saturating_add(rows).min(label_count) {
                if !guard.live_docs.is_deleted(position)
                    && (req.visibility.is_none() || position < documents)
                    && visible
                        .as_ref()
                        .is_none_or(|allowed| allowed.test(position))
                {
                    bits[position / 8] |= 1 << (position % 8);
                }
            }
        }
        Ok(Response::new(MembershipBitmapResponse {
            vector_binding,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
            segments_total: 0,
            segments_skipped: 0,
            base_label: self.config.slot_offset,
            label_count: label_count as u64,
            bits,
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
        }))
    }

    pub(super) async fn handle_evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        crate::metrics::timed(Route::EvaluateBoolean, request, |request| async move {
            let req = request.into_inner();
            // An FP32 leaf takes the rerank lanes ExactVectorRescore
            // takes for one piece, held across the evaluation.
            let exact = req.leaves.iter().any(|leaf| {
                matches!(
                    leaf.leaf.as_ref(),
                    Some(crate::pb::boolean_plan_leaf::Leaf::Dense(dense)) if dense.exact_fp32
                )
            });
            let permits = if exact {
                let parallel = resolved_rerank_parallel(self.config.rerank_parallel);
                let batch = if req.exact_batch == 0 {
                    10_000
                } else {
                    req.exact_batch as usize
                };
                let lanes = crate::exact_vectors::rerank_task_count(batch, parallel);
                Some(
                    self.rerank_slots
                        .clone()
                        .acquire_many_owned(lanes as u32)
                        .await
                        .map_err(|_| Status::unavailable("exact rerank worker budget closed"))?,
                )
            } else {
                None
            };
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                let _permits = permits;
                service.run_evaluate_boolean(req)
            })
            .await
            .map_err(|e| Status::internal(format!("boolean evaluation task failed: {e}")))?
            .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_aggregate_shard(
        &self,
        request: Request<crate::pb::AggregateShardRequest>,
    ) -> Result<Response<crate::pb::AggregateShardResponse>, Status> {
        crate::metrics::timed(Route::AggregateShard, request, |request| async move {
            let req = request.into_inner();
            let guard = read_shard(&self.state);
            self.run_aggregate_shard(&guard, req, None)
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_quantile_counts(
        &self,
        request: Request<crate::pb::QuantileCountsRequest>,
    ) -> Result<Response<crate::pb::QuantileCountsResponse>, Status> {
        crate::metrics::timed(Route::QuantileCounts, request, |request| async move {
            let req = request.into_inner();
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let filter =
                crate::visibility::intersect_filter(req.visibility.as_ref(), req.filter.clone())?;
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            let guard = read_shard(&self.state);
            crate::stats_identity::StatsClaim::required(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
            )?;
            guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
            let (_, visibility_columns_known) = filter_known_flags(
                guard.bm25.as_ref(),
                &[],
                req.visibility
                    .as_ref()
                    .and_then(|view| view.filter.as_ref()),
            );
            // A Boolean round: the tree owns membership, resolved again
            // on this shard under the same guard the counts read through.
            let membership = match req.boolean.as_ref() {
                Some(plan) => {
                    if req.filter.is_some() || !req.geo_filters.is_empty() || req.restrict_doc_ids {
                        return Err(Status::invalid_argument(
                            "quantile round: a Boolean membership excludes filters and an id \
                             allowlist",
                        ));
                    }
                    Some(
                        evaluate_boolean_membership(&guard, plan, self.knobs.segment_pruning())?
                            .members,
                    )
                }
                None => None,
            };
            let Some(store) = guard.bm25.as_ref() else {
                return Ok(Response::new(crate::pb::QuantileCountsResponse {
                    stats_epoch: guard.stats_epoch,
                    stats_incarnation: guard.stats_incarnation.bytes()?,
                    visibility_fingerprint: scope.fingerprint().to_vec(),
                    visibility_columns_known: visibility_columns_known.clone(),
                    counts: vec![0; req.targets.len()],
                }));
            };
            let exprs = resolve_quantile_exprs(store, &req.exprs, &req.targets)?;
            let pass = self.admitted_doc_pass(
                &guard,
                store,
                DocPassSpec {
                    geo_filters: &req.geo_filters,
                    geo_regions: &geo_regions,
                    filter: filter.as_ref(),
                    doc_ids: req.restrict_doc_ids.then_some(req.doc_ids.as_slice()),
                },
            )?;
            let counts = count_quantile_targets(&pass, membership.as_ref(), &exprs, &req.targets);
            Ok(Response::new(crate::pb::QuantileCountsResponse {
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known,
                counts,
            }))
        })
        .await
    }
}

type ResolvedQuantileExpr = Option<(crate::values::ResolvedValue, crate::pb::AggregateValueType)>;

fn resolve_quantile_exprs(
    store: &Bm25Shard,
    specs: &[crate::pb::CompiledPercentile],
    targets: &[crate::pb::QuantileTarget],
) -> Result<Vec<ResolvedQuantileExpr>, Status> {
    let mut exprs = Vec::with_capacity(specs.len());
    for spec in specs {
        exprs.push(resolve_rankable(
            store,
            spec.expr.as_ref(),
            &spec.name,
            "percentile",
        )?);
    }
    for target in targets {
        if target.expr_index as usize >= exprs.len() {
            return Err(Status::internal(
                "quantile target refers past the expression list",
            ));
        }
    }
    Ok(exprs)
}

fn count_quantile_targets(
    pass: &AdmittedDocPass<'_>,
    membership: Option<&crate::boolean_bits::Bits>,
    exprs: &[ResolvedQuantileExpr],
    targets: &[crate::pb::QuantileTarget],
) -> Vec<u64> {
    let mut counts = vec![0u64; targets.len()];
    let mut bits_of = vec![None; exprs.len()];
    for doc in pass.prune.admitted_slots(pass.rows) {
        if !pass.admits(doc, membership) {
            continue;
        }
        for (slot, resolved) in bits_of.iter_mut().zip(exprs) {
            *slot = match resolved {
                Some((rv, vtype)) => {
                    crate::values::eval(rv, doc, &pass.cols).and_then(|v| rankable_bits(v, *vtype))
                }
                None => None,
            };
        }
        for (count, t) in counts.iter_mut().zip(targets) {
            if bits_of[t.expr_index as usize].is_some_and(|bits| bits <= t.threshold_bits) {
                *count += 1;
            }
        }
    }
    counts
}
