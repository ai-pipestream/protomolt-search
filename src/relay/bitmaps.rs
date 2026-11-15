//! Composed bitmap routes: filter, lexical, and vector bitmaps.

use std::time::Duration;

use tonic::{Request, Response, Status};

use crate::coordinator::{CoordinatorServiceImpl, RequestFilters};
use crate::metrics::Route;
use crate::pb::{MembershipBitmapResponse, TermStatsRequest};

use super::fanout::{fan_out, read_metadata, same_requests, shares_only};
use super::map::MapSnapshot;
use super::merge_util::{KnownFlags, SegmentCounts};
use super::{grpc_timeout, RelayService};

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

impl From<&MembershipBitmapResponse> for ChildBitmap {
    fn from(share: &MembershipBitmapResponse) -> Self {
        ChildBitmap {
            base_label: share.base_label,
            label_count: share.label_count,
            bits: share.bits.clone(),
        }
    }
}

/// The filters a filter bitmap resolves: the request's geo filters and
/// tree, none of the scan knobs.
fn bitmap_filters(req: &crate::pb::FilterBitmapRequest) -> RequestFilters {
    RequestFilters {
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
    }
}

/// One filter-bitmap request per child the placement mask keeps, each
/// with that child's own tree; `None` for a child the mask skipped.
fn filter_bitmap_requests(
    req: &crate::pb::FilterBitmapRequest,
    filters: &RequestFilters,
    mask: Option<&crate::placement::ShardMask>,
    children: usize,
) -> Vec<Option<crate::pb::FilterBitmapRequest>> {
    (0..children)
        .map(|shard| {
            let skipped = mask.is_some_and(|m| m.skipped[shard]);
            (!skipped).then(|| crate::pb::FilterBitmapRequest {
                visibility: req.visibility.clone(),
                geo_filters: filters.geo.clone(),
                filter: CoordinatorServiceImpl::shard_filter_tree(filters, mask, shard),
            })
        })
        .collect()
}

/// The filter bitmap answer over the merged bitmap, flags, and receipt.
fn filter_response(
    receipt: crate::pb::VectorReadReceipt,
    bitmap: (u64, u64, Vec<u8>),
    known: KnownFlags,
    segments: SegmentCounts,
) -> crate::pb::FilterBitmapResponse {
    let (base_label, label_count, bits) = bitmap;
    crate::pb::FilterBitmapResponse {
        stats_epoch: receipt.stats_epoch,
        stats_incarnation: receipt.stats_incarnation,
        visibility_fingerprint: receipt.visibility_fingerprint,
        visibility_columns_known: receipt.visibility_columns_known,
        base_label,
        label_count,
        bits,
        geo_columns_known: known.geo,
        filter_columns_known: known.tree,
        segments_total: segments.total,
        segments_skipped: segments.skipped,
    }
}

/// The children's membership answers under one map reading.
struct Membership {
    pinned: MapSnapshot,
    children: Vec<String>,
    ranges: Vec<(u64, u64)>,
    shares: Vec<MembershipBitmapResponse>,
}

impl RelayService {
    /// A version-only read receipt from a child the placement mask
    /// skipped: it contributes no member but its version still binds.
    async fn skipped_child_receipt(
        &self,
        frozen: &CoordinatorServiceImpl,
        addr: &str,
        timeout: Option<Duration>,
    ) -> Result<crate::pb::VectorReadReceipt, Status> {
        let mut request = Request::new(TermStatsRequest {
            version_only: true,
            ..Default::default()
        });
        if let Some(timeout) = timeout {
            request.set_timeout(timeout);
        }
        let share = frozen
            .node_client(addr)?
            .term_stats(request)
            .await?
            .into_inner();
        crate::visibility::validate_stats_mode(true, &share)?;
        Ok(crate::pb::VectorReadReceipt {
            stats_epoch: share.stats_epoch,
            stats_incarnation: share.stats_incarnation,
            visibility_fingerprint: share.visibility_fingerprint,
            visibility_columns_known: share.visibility_columns_known,
            ..Default::default()
        })
    }

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
            let filters = bitmap_filters(&req);
            let (pinned, frozen, children) = self.pin_children()?;
            let ranges = self.ranges_of(&frozen, &children, timeout).await?;
            let mask = req
                .visibility
                .is_none()
                .then(|| frozen.shard_mask(filters.tree.as_ref()))
                .flatten();
            let requests = filter_bitmap_requests(&req, &filters, mask.as_ref(), children.len());
            let mut answered = fan_out(
                &frozen,
                &children,
                requests,
                timeout,
                "filter bitmap",
                |mut link, request| async move { link.resolve_filter_bitmap(request).await },
            )
            .await?
            .into_iter()
            .peekable();
            let mut known = KnownFlags::new(&filters, mask.as_ref());
            let mut shares = Vec::with_capacity(children.len());
            let mut receipts = Vec::with_capacity(children.len());
            let mut segments = SegmentCounts::default();
            for (shard, addr) in children.iter().enumerate() {
                let Some((_, share)) = answered.next_if(|(child, _)| *child == shard) else {
                    receipts.push(self.skipped_child_receipt(&frozen, addr, timeout).await?);
                    shares.push(None);
                    continue;
                };
                receipts.push(read_metadata(&share));
                known.merge(
                    shard,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                segments.add(shard, share.segments_total, share.segments_skipped)?;
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
            Ok(Response::new(filter_response(
                receipt,
                (base_label, label_count, bits),
                known,
                segments,
            )))
        })
        .await
    }

    /// One membership request per child, unchanged, under one map
    /// reading, with the children's slot ranges.
    async fn membership_shares<Req, F, Fut>(
        &self,
        req: &Req,
        timeout: Option<Duration>,
        label: &'static str,
        call: F,
    ) -> Result<Membership, Status>
    where
        Req: Clone + Send + 'static,
        F: Fn(crate::link::NodeLink, Request<Req>) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<Response<MembershipBitmapResponse>, Status>>
            + Send
            + 'static,
    {
        let (pinned, frozen, children) = self.pin_children()?;
        let ranges = self.ranges_of(&frozen, &children, timeout).await?;
        let requests = same_requests(req, children.len());
        let shares =
            shares_only(fan_out(&frozen, &children, requests, timeout, label, call).await?);
        Ok(Membership {
            pinned,
            children,
            ranges,
            shares,
        })
    }

    /// The children's membership bitmaps laid over the relay's range,
    /// their bindings checked against `field` (the lexical route has
    /// none), and the receipt over the children's.
    fn finish_membership(
        &self,
        m: Membership,
        route: &str,
        visibility: Option<&crate::pb::DocumentVisibility>,
        field: Option<&str>,
    ) -> Result<MembershipBitmapResponse, Status> {
        let mut binding = None;
        let mut segments = SegmentCounts::default();
        let mut receipts = Vec::with_capacity(m.shares.len());
        for (shard, share) in m.shares.iter().enumerate() {
            match field {
                Some(field) => crate::vector_read::check_binding(
                    field,
                    share.vector_binding.as_ref(),
                    &mut binding,
                )?,
                None => {
                    segments.add(shard, share.segments_total, share.segments_skipped)?;
                    crate::vector_read::check_binding("", None, &mut binding)?;
                }
            }
            receipts.push(read_metadata(share));
        }
        let bitmaps: Vec<Option<ChildBitmap>> = m.shares.iter().map(|s| Some(s.into())).collect();
        let (base_label, label_count, bits) = concat_bitmaps(&m.children, &m.ranges, &bitmaps)?;
        self.still_current(&m.pinned, route)?;
        let receipt = self.read_receipt(&m.pinned, m.children, visibility, None, &receipts)?;
        Ok(MembershipBitmapResponse {
            vector_binding: binding.flatten(),
            stats_incarnation: receipt.stats_incarnation,
            visibility_fingerprint: receipt.visibility_fingerprint,
            visibility_columns_known: receipt.visibility_columns_known,
            base_label,
            label_count,
            bits,
            stats_epoch: receipt.stats_epoch,
            segments_total: segments.total,
            segments_skipped: segments.skipped,
        })
    }

    pub(super) async fn handle_resolve_lexical_bitmap(
        &self,
        request: Request<crate::pb::LexicalBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let m = self
            .membership_shares(
                &req,
                timeout,
                "lexical bitmap",
                |mut link, request| async move { link.resolve_lexical_bitmap(request).await },
            )
            .await?;
        let visibility = req.visibility.as_ref();
        Ok(Response::new(self.finish_membership(
            m,
            "ResolveLexicalBitmap",
            visibility,
            None,
        )?))
    }

    pub(super) async fn handle_resolve_vector_bitmap(
        &self,
        request: Request<crate::pb::VectorBitmapRequest>,
    ) -> Result<Response<MembershipBitmapResponse>, Status> {
        let timeout = grpc_timeout(request.metadata());
        let req = request.into_inner();
        crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let m = self
            .membership_shares(
                &req,
                timeout,
                "vector bitmap",
                |mut link, request| async move { link.resolve_vector_bitmap(request).await },
            )
            .await?;
        let visibility = req.visibility.as_ref();
        let field = Some(req.field.as_str());
        Ok(Response::new(self.finish_membership(
            m,
            "ResolveVectorBitmap",
            visibility,
            field,
        )?))
    }
}
