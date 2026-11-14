//! Composed bitmap routes: filter, lexical, and vector bitmaps, boolean evaluation.

use tonic::{Request, Response, Status};

use crate::coordinator::{CoordinatorServiceImpl, RequestFilters};
use crate::metrics::Route;
use crate::pb::TermStatsRequest;

use super::merge_lexical::{add_count32, child_ranges, merge_boolean_responses, KnownFlags};
use super::stream_search::child_error;
use super::values::merge_aggregate_shares;
use super::{grpc_timeout, read_metadata, RelayService};

/// One child's packed bitmap as it came back, or nothing for a child the
/// placement mask skipped (it contributes no member).
pub struct ChildBitmap {
    pub base_label: u64,
    pub label_count: u64,
    pub bits: Vec<u8>,
}

/// The children's bitmaps laid over the relay's one slot range: child
/// `i`'s bits go at `offset_i - base` (its slot offset, which its
/// `base_label` must equal), every range checked for its declared
/// length and for zero padding, the relay's `label_count` running to
/// the end of the last labelled child. Between a child's last label and
/// the next child's first slot the bits are zero, which is what those
/// unfilled slots hold on a node too. Returns `(base_label, label_count,
/// bits)`.
pub fn concat_bitmaps(
    children: &[String],
    ranges: &[(u64, u64)],
    shares: &[Option<ChildBitmap>],
) -> Result<(u64, u64, Vec<u8>), Status> {
    let base = ranges
        .iter()
        .map(|&(offset, _)| offset)
        .min()
        .ok_or_else(|| Status::failed_precondition("relay: a relay coordinator has no children"))?;
    let name = |child: usize| children.get(child).map_or("", String::as_str);
    let mut total: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        let Some(share) = share else {
            continue;
        };
        let (offset, span) = ranges[child];
        if share.base_label != offset {
            return Err(Status::failed_precondition(format!(
                "relay: child {child} ({}) answered a bitmap based at label {} while its slot \
                 range starts at {offset}",
                name(child),
                share.base_label
            )));
        }
        if share.label_count > span {
            return Err(Status::failed_precondition(format!(
                "relay: child {child} ({}) answered {} labels over a slot range of {span}",
                name(child),
                share.label_count
            )));
        }
        let expected = usize::try_from(share.label_count.div_ceil(8)).map_err(|_| {
            Status::resource_exhausted("relay: membership bitmap does not fit usize")
        })?;
        if share.bits.len() != expected {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) answered {} bitmap bytes for {} labels; expected \
                 {expected}",
                name(child),
                share.bits.len(),
                share.label_count
            )));
        }
        if !share.label_count.is_multiple_of(8)
            && share.bits.last().is_some_and(|last| {
                let used = (share.label_count % 8) as u8;
                *last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(Status::internal(format!(
                "relay: child {child} ({}) sets padding bits beyond its label count",
                name(child)
            )));
        }
        let end = (offset - base)
            .checked_add(share.label_count)
            .ok_or_else(|| Status::internal("relay: membership label range overflows u64"))?;
        total = total.max(end);
    }
    let bytes = usize::try_from(total.div_ceil(8)).map_err(|_| {
        Status::resource_exhausted("relay: merged membership bitmap does not fit usize")
    })?;
    let mut bits = vec![0u8; bytes];
    for (child, share) in shares.iter().enumerate() {
        let Some(share) = share else {
            continue;
        };
        let at = usize::try_from(ranges[child].0 - base)
            .map_err(|_| Status::resource_exhausted("relay: bitmap offset does not fit usize"))?;
        let count = usize::try_from(share.label_count).map_err(|_| {
            Status::resource_exhausted("relay: bitmap label count does not fit usize")
        })?;
        or_bits(&mut bits, at, &share.bits, count);
    }
    Ok((base, total, bits))
}

/// OR `count` bits of `src` (LSB-first per byte, the wire packing) into
/// `dst` starting at bit `at`. Byte-aligned placements copy whole bytes;
/// the rest walk the set bits.
fn or_bits(dst: &mut [u8], at: usize, src: &[u8], count: usize) {
    if at.is_multiple_of(8) {
        let first = at / 8;
        for (i, byte) in src.iter().enumerate() {
            dst[first + i] |= byte;
        }
        return;
    }
    for (byte_index, byte) in src.iter().copied().enumerate() {
        let mut held = byte;
        while held != 0 {
            let bit = held.trailing_zeros() as usize;
            let position = byte_index * 8 + bit;
            if position < count {
                let target = at + position;
                dst[target / 8] |= 1 << (target % 8);
            }
            held &= held - 1;
        }
    }
}

impl RelayService {
    pub(super) async fn handle_resolve_filter_bitmap(
        &self,
        request: Request<crate::pb::FilterBitmapRequest>,
    ) -> Result<Response<crate::pb::FilterBitmapResponse>, Status> {
        crate::metrics::timed(Route::ResolveFilterBitmap, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            crate::node::validate_geo_filters(&req.geo_filters)?;
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let filters = RequestFilters {
                geo: req.geo_filters.clone(),
                tree: req.filter.clone(),
                enumerate_max_rows: None,
                patience_saturation_ppm: None,
                patience_hops: None,
                finish_settle_us: None,
                finish_deadline_us: None,
                floor_prune: None,
                floor_k: None,
                parent_floor_share_ppm: None,
                floor_greediness_ppm: None,
                floor_clamp_min: None,
                include_rerank_candidate: false,
            };
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let reports = self.children_health(&frozen, timeout).await?;
            let ranges = child_ranges(&self.collection, &children, &reports)?;
            let mask = req
                .visibility
                .is_none()
                .then(|| frozen.shard_mask(filters.tree.as_ref()))
                .flatten();
            let mut known = KnownFlags::new(&filters, mask.as_ref());
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    tasks.push(None);
                    continue;
                }
                let mut link = frozen.node_client(addr)?;
                let child_req = crate::pb::FilterBitmapRequest {
                    visibility: req.visibility.clone(),
                    geo_filters: filters.geo.clone(),
                    filter: CoordinatorServiceImpl::shard_filter_tree(
                        &filters,
                        mask.as_ref(),
                        shard,
                    ),
                };
                let addr = addr.clone();
                tasks.push(Some(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.resolve_filter_bitmap(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "filter bitmap", status))
                })));
            }
            let mut shares = Vec::with_capacity(children.len());
            let mut receipts = Vec::with_capacity(children.len());
            let mut segments_total: u32 = 0;
            let mut segments_skipped: u32 = 0;
            for (shard, task) in tasks.into_iter().enumerate() {
                let Some(task) = task else {
                    let mut request = Request::new(TermStatsRequest {
                        version_only: true,
                        ..Default::default()
                    });
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    let share = frozen
                        .node_client(&children[shard])?
                        .term_stats(request)
                        .await?
                        .into_inner();
                    crate::visibility::validate_stats_mode(true, &share)?;
                    receipts.push(crate::pb::VectorReadReceipt {
                        stats_epoch: share.stats_epoch,
                        stats_incarnation: share.stats_incarnation,
                        visibility_fingerprint: share.visibility_fingerprint,
                        visibility_columns_known: share.visibility_columns_known,
                        ..Default::default()
                    });
                    shares.push(None);
                    continue;
                };
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay filter bitmap task: {e}")))??;
                receipts.push(read_metadata(&share));
                known.merge(
                    shard,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                add_count32(
                    &mut segments_total,
                    share.segments_total,
                    shard,
                    "segment count",
                )?;
                add_count32(
                    &mut segments_skipped,
                    share.segments_skipped,
                    shard,
                    "skipped segment count",
                )?;
                shares.push(Some(ChildBitmap {
                    base_label: share.base_label,
                    label_count: share.label_count,
                    bits: share.bits,
                }));
            }
            let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
            self.still_current(&pinned, "ResolveFilterBitmap")?;
            let receipt =
                self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
            Ok(Response::new(crate::pb::FilterBitmapResponse {
                stats_epoch: receipt.stats_epoch,
                stats_incarnation: receipt.stats_incarnation,
                visibility_fingerprint: receipt.visibility_fingerprint,
                visibility_columns_known: receipt.visibility_columns_known,
                base_label,
                label_count,
                bits,
                geo_columns_known: known.geo,
                filter_columns_known: known.tree,
                segments_total,
                segments_skipped,
            }))
        })
        .await
    }

    pub(super) async fn handle_resolve_lexical_bitmap(
        &self,
        request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        let reports = self.children_health(&frozen, timeout).await?;
        let ranges = child_ranges(&self.collection, &children, &reports)?;
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let child_req = req.clone();
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(child_req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.resolve_lexical_bitmap(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, "lexical bitmap", status))
            }));
        }
        let mut shares = Vec::with_capacity(children.len());
        let mut receipts = Vec::with_capacity(children.len());
        let mut binding = None;
        let mut segments_total: u32 = 0;
        let mut segments_skipped: u32 = 0;
        for (shard, task) in tasks.into_iter().enumerate() {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("relay lexical bitmap task: {e}")))??;
            add_count32(
                &mut segments_total,
                share.segments_total,
                shard,
                "segment count",
            )?;
            add_count32(
                &mut segments_skipped,
                share.segments_skipped,
                shard,
                "skipped segment count",
            )?;
            crate::vector_read::check_binding("", None, &mut binding)?;
            receipts.push(read_metadata(&share));
            shares.push(Some(ChildBitmap {
                base_label: share.base_label,
                label_count: share.label_count,
                bits: share.bits,
            }));
        }
        let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
        self.still_current(&pinned, "ResolveLexicalBitmap")?;
        let receipt =
            self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
        Ok(Response::new(crate::pb::MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total,
            segments_skipped,
        }))
    }

    pub(super) async fn handle_resolve_vector_bitmap(
        &self,
        request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<crate::pb::MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let (pinned, frozen) = self.pin();
        let children = frozen.node_addresses().to_vec();
        if children.is_empty() {
            return Err(Status::failed_precondition(
                "relay: a relay coordinator has no children",
            ));
        }
        let reports = self.children_health(&frozen, timeout).await?;
        let ranges = child_ranges(&self.collection, &children, &reports)?;
        let mut tasks = Vec::with_capacity(children.len());
        for (shard, addr) in children.iter().enumerate() {
            let mut link = frozen.node_client(addr)?;
            let addr = addr.clone();
            let req = req.clone();
            tasks.push(tokio::spawn(async move {
                let mut request = Request::new(req);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                link.resolve_vector_bitmap(request)
                    .await
                    .map(|r| r.into_inner())
                    .map_err(|status| child_error(shard, &addr, "vector bitmap", status))
            }));
        }
        let mut shares = Vec::with_capacity(children.len());
        let mut receipts = Vec::with_capacity(children.len());
        let mut binding = None;
        for task in tasks {
            let share = task
                .await
                .map_err(|e| Status::internal(format!("relay vector bitmap task: {e}")))??;
            crate::vector_read::check_binding(
                &req.field,
                share.vector_binding.as_ref(),
                &mut binding,
            )?;
            receipts.push(read_metadata(&share));
            shares.push(Some(ChildBitmap {
                base_label: share.base_label,
                label_count: share.label_count,
                bits: share.bits,
            }));
        }
        let (base_label, label_count, bits) = concat_bitmaps(&children, &ranges, &shares)?;
        self.still_current(&pinned, "ResolveVectorBitmap")?;
        let receipt =
            self.read_receipt(&pinned, children, req.visibility.as_ref(), None, &receipts)?;
        Ok(Response::new(crate::pb::MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total: 0,
            segments_skipped: 0,
        }))
    }

    /// One shard-side Boolean evaluation over the children: the parent's
    /// stats claim translates per child, each child answers its best
    /// `depth`, and the merge is the ranked union cut to `depth` with the
    /// counts summed and the known flags joined. An aggregate is not
    /// composed here (the fold order is the root's) and refuses by name.
    pub(super) async fn handle_evaluate_boolean(
        &self,
        request: Request<crate::pb::BooleanShardRequest>,
    ) -> Result<Response<crate::pb::BooleanShardResponse>, Status> {
        crate::metrics::timed(Route::EvaluateBoolean, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            if req.depth == 0 {
                return Err(Status::invalid_argument(
                    "EvaluateBoolean: depth 0 asks for no candidates",
                ));
            }
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.evaluate_boolean(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "boolean evaluation", status))
                }));
            }
            let mut shares = Vec::with_capacity(children.len());
            for task in tasks {
                shares.push(
                    task.await
                        .map_err(|e| Status::internal(format!("relay boolean task: {e}")))??,
                );
            }
            let mut merged = merge_boolean_responses(&req, &shares)?;
            self.still_current(&pinned, "EvaluateBoolean")?;
            let receipts = shares
                .iter()
                .map(|share| {
                    let receipt = share.read_receipt.clone().ok_or_else(|| {
                        Status::failed_precondition("relay child omitted Boolean read receipt")
                    })?;
                    if receipt.stats_epoch != share.stats_epoch {
                        return Err(Status::failed_precondition(
                            "Boolean receipt epoch mismatch",
                        ));
                    }
                    Ok(receipt)
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let mut receipt = self.read_receipt(
                &pinned,
                children,
                req.visibility.as_ref(),
                Some(&claims),
                &receipts,
            )?;
            let mut binding = None;
            for leaf in &req.leaves {
                if let Some(crate::pb::boolean_plan_leaf::Leaf::Dense(dense)) = leaf.leaf.as_ref() {
                    for child in &receipts {
                        crate::vector_read::check_binding(
                            &dense.field,
                            child.vector_binding.as_ref(),
                            &mut binding,
                        )?;
                    }
                }
            }
            receipt.vector_binding = binding.flatten();
            merged.stats_epoch = receipt.stats_epoch;
            merged.read_receipt = Some(receipt);
            // The root aggregate each child folded over its match set,
            // folded in child order, with a receipt of its own the root
            // checks as it checks a shard's.
            if let Some(spec) = req.aggregate.as_ref() {
                let folds = shares
                    .iter()
                    .enumerate()
                    .map(|(shard, share)| {
                        share.aggregate.clone().ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "relay: child {shard} answered no aggregate fold for a Boolean \
                                 plan that asked for one"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, Status>>()?;
                let mut fold = merge_aggregate_shares(
                    &spec.aggregations,
                    &spec.group_by,
                    spec.max_groups,
                    &spec.histograms,
                    &spec.percentiles,
                    &folds,
                )?;
                let fold_receipt = self.read_receipt(
                    &pinned,
                    self.children(),
                    req.visibility.as_ref(),
                    Some(&claims),
                    &folds,
                )?;
                fold.stats_epoch = fold_receipt.stats_epoch;
                fold.stats_incarnation = fold_receipt.stats_incarnation;
                fold.visibility_fingerprint = fold_receipt.visibility_fingerprint;
                fold.visibility_columns_known = fold_receipt.visibility_columns_known;
                merged.aggregate = Some(fold);
            }
            Ok(Response::new(merged))
        })
        .await
    }
}
