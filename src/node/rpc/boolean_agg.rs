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
                &req.geo_filters,
                &geo_regions,
                filter.as_ref(),
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
        let index = shard.as_index().ok_or_else(|| {
            Status::failed_precondition("bm25 bulk build in progress; Flush first")
        })?;
        let label_count = usize::try_from(shard.next_doc_id())
            .map_err(|_| Status::resource_exhausted("lexical row count does not fit usize"))?;
        let visible = if view_filter.is_some() {
            resolve_shard_filters(
                guard.bm25.as_ref(),
                guard.live_docs.words(),
                label_count,
                &[],
                &[],
                view_filter,
                self.knobs.segment_pruning(),
                None,
            )?
            .1
        } else {
            None
        };
        // A sealed segment none of the terms occur in contributes no
        // member: the walk skips it and the count says so
        // (docs/segment-pruning.md). One dictionary lookup per term per
        // part decides; with pruning off every part is walked.
        let (masked, prune) = match shard {
            Bm25Shard::Segmented(segmented) => {
                let lacking = segmented.parts_lacking_terms(0, &req.terms);
                let skipped = lacking.iter().filter(|&&lacks| lacks).count() as u32;
                let stats = crate::segment_prune::PruneStats {
                    segments_total: lacking.len() as u32,
                    segments_skipped: if self.knobs.segment_pruning() {
                        skipped
                    } else {
                        0
                    },
                };
                let masked = (self.knobs.segment_pruning() && skipped > 0).then(|| {
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
        for term in req.terms {
            index.for_each_doc_tf(&term, &mut |doc_id, _tf| {
                let position = doc_id as usize;
                if position < label_count
                    && !guard.live_docs.is_deleted(position)
                    && visible
                        .as_ref()
                        .is_none_or(|allowed| allowed.test(position))
                {
                    bits[position / 8] |= 1 << (position % 8);
                }
            });
        }
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
        let visible = if view_filter.is_some() {
            resolve_shard_filters(
                guard.bm25.as_ref(),
                guard.live_docs.words(),
                label_count,
                &[],
                &[],
                view_filter,
                self.knobs.segment_pruning(),
                None,
            )?
            .1
        } else {
            None
        };
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
            if store.as_index().is_none() {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush before aggregating",
                ));
            }
            let mut exprs = Vec::with_capacity(req.exprs.len());
            for spec in &req.exprs {
                exprs.push(resolve_rankable(
                    store,
                    spec.expr.as_ref(),
                    &spec.name,
                    "percentile",
                )?);
            }
            for t in &req.targets {
                if t.expr_index as usize >= exprs.len() {
                    return Err(Status::internal(
                        "quantile target refers past the expression list",
                    ));
                }
            }
            let doc_filter = crate::filter::DocFilter {
                deleted: guard.live_docs.words(),
                geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                pred: filter
                    .as_ref()
                    .map(|f| store.resolve_filter(f))
                    .transpose()?,
                phrase: Vec::new(),
            };
            let cols = ShardNumericRead(store);
            let n = u64::from(store.next_doc_id());
            let prune = prune_segments(
                store,
                doc_filter.pred.as_ref(),
                self.knobs.segment_pruning(),
            );
            let id_allowlist = req.restrict_doc_ids.then(|| {
                req.doc_ids
                    .iter()
                    .filter_map(|id| id.checked_sub(self.config.slot_offset))
                    .filter_map(|local| u32::try_from(local).ok())
                    .collect::<std::collections::HashSet<_>>()
            });
            let mut counts = vec![0u64; req.targets.len()];
            let mut bits_of = vec![None; exprs.len()];
            // One admitted-set pass per round: each expression evaluates
            // once per document, every target reads the cached bits.
            for doc in prune.admitted_slots(n as u32) {
                if id_allowlist
                    .as_ref()
                    .is_some_and(|allowlist| !allowlist.contains(&doc))
                {
                    continue;
                }
                if membership.as_ref().is_some_and(|m| !m.test(doc as usize)) {
                    continue;
                }
                if !doc_filter.passes(doc, &cols) {
                    continue;
                }
                for (slot, resolved) in bits_of.iter_mut().zip(&exprs) {
                    *slot = match resolved {
                        Some((rv, vtype)) => crate::values::eval(rv, doc, &cols)
                            .and_then(|v| rankable_bits(v, *vtype)),
                        None => None,
                    };
                }
                for (count, t) in counts.iter_mut().zip(&req.targets) {
                    if let Some(bits) = bits_of[t.expr_index as usize] {
                        if bits <= t.threshold_bits {
                            *count += 1;
                        }
                    }
                }
            }
            Ok(Response::new(crate::pb::QuantileCountsResponse {
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known: visibility_columns_known.clone(),
                counts,
            }))
        })
        .await
    }
}
