//! Blocking scan execution for `StreamSearch`.

use crate::node::*;
use crate::pb::StartStreamSearch;

use super::{collect_rerank_candidates, validate_query_dim};

pub(super) type StreamScanResult = Result<
    (
        StreamSearchSummary,
        Option<crate::query_identity::ScanIdentities>,
    ),
    Status,
>;

pub(super) struct StreamScanCtx {
    pub state: Arc<RwLock<ShardState>>,
    pub start: StartStreamSearch,
    pub geo_regions: Vec<crate::geo::GeoRegion>,
    pub scan_tx: mpsc::Sender<Result<StreamSearchResponse, Status>>,
    pub scan_signals: Arc<StreamSignals>,
    pub slot_offset: u64,
    pub segment_pruning: bool,
    pub share_floors: bool,
    pub stream_poll_delay: Option<std::time::Duration>,
    pub identity_enabled: bool,
}

fn build_stream_options<'a>(
    index: &VectorIndex,
    start: &StartStreamSearch,
    allow: Option<&'a crate::boolean_bits::Bits>,
    share_floors: bool,
) -> Result<(VectorSearchOptions<'a>, f32), Status> {
    let mut options = VectorSearchOptions::new();
    if let Some(a) = allow {
        options = options.with_bits(a);
    }
    if let Some(depth) = start.candidate_depth {
        options = options.with_candidate_depth(depth as usize);
    }
    check_enumerate_threshold(index, start.enumerate_max_rows)?;
    if let Some(rows) = start.enumerate_max_rows {
        options = options.with_enumerate_max_rows(rows as usize);
    }
    if start.staged_selection {
        return Err(Status::failed_precondition(
            "staged top-k selection is served on the unary search route; a stream completes \
             as an exhaustive scan",
        ));
    }
    let controls = selection_controls(index, start.filter_strategy, start.enumerate_all, false)?;
    options.filter_routing = controls.filter_routing;
    if controls.enumerate_all {
        options.allow = Some(
            options
                .allow
                .unwrap_or(crate::vector::Mask::All(index.len())),
        );
        options.filter_regime = Some(crate::vector::FilterRegime::Enumerate);
        options.enumerate_max_rows = None;
    }
    if let Some(patience) =
        check_patience_options(index, start.patience_saturation_ppm, start.patience_hops)?
    {
        options = options.with_patience(patience);
    }
    if let Some(floor_prune) = check_floor_prune_options(
        index,
        start.floor_prune,
        start.floor_k,
        start.candidate_depth.unwrap_or(0),
        start.floor_share_ppm,
        start.floor_greediness_ppm,
        start.floor_clamp_min,
    )? {
        options = options.with_floor_prune(floor_prune);
    }
    let mut floor_now = f32::NEG_INFINITY;
    if share_floors {
        if let Some(f) = start.initial_floor {
            options = options.with_initial_threshold(f);
            floor_now = f;
        }
    }
    Ok((options, floor_now))
}

fn pack_stream_batch(
    batch: &crate::vector::VectorStreamBatch<'_>,
    slot_offset: u64,
    parents: Option<&ParentMap>,
    include_rerank_candidate: bool,
    emitted_slots: &mut Vec<u32>,
) -> Vec<u8> {
    let stride = if parents.is_some() { 20 } else { 12 };
    let mut hits = Vec::with_capacity(stride * batch.slots.len());
    for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
        if slot < 0 {
            continue;
        }
        if include_rerank_candidate {
            emitted_slots.push(slot as u32);
        }
        hits.extend_from_slice(&(slot_offset + slot as u64).to_le_bytes());
        hits.extend_from_slice(&score.to_le_bytes());
        if let Some(p) = parents {
            hits.extend_from_slice(&p.rows[slot as usize].to_le_bytes());
        }
    }
    hits
}

pub(super) fn poll_stream_control(
    signals: &StreamSignals,
    stream_poll_delay: Option<std::time::Duration>,
    allow_early_finish: bool,
    share_floors: bool,
    floor_now: &mut f32,
    raises: &mut u64,
) -> VectorStreamControl {
    if let Some(delay) = stream_poll_delay {
        std::thread::sleep(delay);
    }
    if signals.cancelled.load(AtomicOrdering::Acquire) {
        return VectorStreamControl::Stop;
    }
    if allow_early_finish {
        if let Some(deadline) = signals.finish_state() {
            return VectorStreamControl::Finish { deadline };
        }
    }
    if share_floors {
        let f = f32::from_bits(signals.floor.load(AtomicOrdering::Acquire));
        if f > *floor_now {
            *floor_now = f;
            *raises += 1;
            return VectorStreamControl::RaiseFloor(f);
        }
    }
    VectorStreamControl::Continue
}

pub(super) fn build_scan_identities(
    guard: &ShardState,
    slot_offset: u64,
    index_len: usize,
    allow: Option<crate::boolean_bits::Bits>,
    include_rerank_candidate: bool,
    emitted_slots: Vec<u32>,
) -> Result<crate::query_identity::ScanIdentities, Status> {
    let identities = guard
        .bm25
        .as_ref()
        .map(Bm25Shard::identity_snapshot)
        .transpose()
        .map_err(Status::internal)?
        .unwrap_or_default();
    let rerank_candidates =
        include_rerank_candidate.then(|| collect_rerank_candidates(guard, true, emitted_slots));
    crate::query_identity::ScanIdentities::new(
        slot_offset,
        index_len,
        identities,
        allow,
        rerank_candidates,
    )
}

pub(super) fn run_stream_scan_blocking(ctx: StreamScanCtx) -> StreamScanResult {
    let parents = if ctx.start.collapse_parents {
        let n = read_shard(&ctx.state)
            .index
            .as_ref()
            .map_or(0, VectorIndex::len);
        Some(NodeServiceImpl::parent_map(&ctx.state, ctx.slot_offset, n)?)
    } else {
        None
    };
    let guard = read_shard(&ctx.state);
    let index = guard.index.as_ref().ok_or_else(|| {
        Status::failed_precondition("shard has no index yet (set calibration or add vectors)")
    })?;
    let dim = index
        .dim_opt()
        .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
    validate_query_dim(&ctx.start.vector, dim)?;
    if let Some(p) = parents.as_ref() {
        guard.check_stats_epoch(p.claim.epoch, &p.claim.incarnation())?;
        if p.rows.len() != index.len() {
            return Err(Status::aborted("shard grew between setup and scan; retry"));
        }
    }
    let scoring_fingerprint = index.descriptor().scoring_fingerprint;
    if scoring_fingerprint.is_empty() {
        return Err(Status::failed_precondition(
            "vector backend has no scoring fingerprint",
        ));
    }
    let ((_, allow, prune), receipt) = guard.vector_scan_filters(
        index.len(),
        &ctx.start.geo_filters,
        &ctx.geo_regions,
        ctx.start.filter.as_ref(),
        ctx.start.read_context.as_ref(),
        ctx.segment_pruning,
    )?;
    if let Some(receipt) = receipt {
        ctx.scan_tx
            .blocking_send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::ReadReady(receipt)),
            }))
            .map_err(|_| Status::cancelled("scoped scan response closed"))?;
    }
    execute_stream_scan(
        &ctx,
        &guard,
        index,
        parents.as_ref(),
        allow,
        prune,
        scoring_fingerprint,
    )
}

fn build_stream_summary(
    summary: crate::vector::VectorStreamSummary,
    cancelled: bool,
    raises: u64,
    geo_columns_known: Vec<bool>,
    filter_columns_known: Vec<bool>,
    scoring_fingerprint: String,
    prune: crate::segment_prune::PruneStats,
) -> StreamSearchSummary {
    let completed = summary.completed && !cancelled;
    let end_reason = if !completed {
        crate::vector::VectorStreamEndReason::Abandoned.to_proto()
    } else {
        summary.end_reason.to_proto()
    };
    StreamSearchSummary {
        completed,
        emitted: summary.emitted as u64,
        blocks_scanned: summary.units_scanned as u64,
        floor_raises_applied: raises,
        geo_columns_known,
        filter_columns_known,
        scoring_fingerprint,
        segments_total: prune.segments_total,
        segments_skipped: prune.segments_skipped,
        filter_regimes: summary.regimes.to_proto(),
        tail_enumerated_rows: summary.tail_enumerated_rows,
        patience_stopped: summary.patience_stopped,
        end_reason,
        floor_pruned: summary.floor_pruned,
        parts_total: summary.parts.total,
        parts_skipped: summary.parts.skipped,
        parts_walked: summary.parts.walked,
        parts_enumerated: summary.parts.enumerated,
        parts_scanned: summary.parts.scanned,
        parts_staged: summary.parts.staged,
        fp32_rows_scored: 0,
        fp32_logical_bytes: 0,
        fp32_pages_touched: 0,
        fp32_tasks: 0,
    }
}

pub(super) fn emit_stream_batch(
    batch: &crate::vector::VectorStreamBatch<'_>,
    ctx: &StreamScanCtx,
    parents: Option<&ParentMap>,
    emitted_slots: &mut Vec<u32>,
) -> VectorStreamControl {
    let hits = pack_stream_batch(
        batch,
        ctx.slot_offset,
        parents,
        ctx.start.include_rerank_candidate,
        emitted_slots,
    );
    let sent = ctx.scan_tx.blocking_send(Ok(StreamSearchResponse {
        payload: Some(stream_search_response::Payload::Batch(StreamSearchBatch {
            hits,
        })),
    }));
    if sent.is_err() {
        VectorStreamControl::Stop
    } else {
        VectorStreamControl::Continue
    }
}

fn execute_stream_scan(
    ctx: &StreamScanCtx,
    guard: &ShardState,
    index: &VectorIndex,
    parents: Option<&ParentMap>,
    allow: Option<crate::boolean_bits::Bits>,
    prune: crate::segment_prune::PruneStats,
    scoring_fingerprint: String,
) -> StreamScanResult {
    let (geo_columns_known, filter_columns_known) = filter_known_flags(
        guard.bm25.as_ref(),
        &ctx.start.geo_filters,
        ctx.start.filter.as_ref(),
    );
    let (options, mut floor_now) =
        build_stream_options(index, &ctx.start, allow.as_ref(), ctx.share_floors)?;
    let allow_early_finish = matches!(
        index.descriptor().quality_contract,
        crate::vector::QualityContract::ConfiguredAnn
            | crate::vector::QualityContract::ProbabilisticBound
    );
    let mut raises = 0u64;
    let mut emitted_slots = Vec::new();
    let summary = index
        .try_search_streaming_controlled(
            &ctx.start.vector,
            options,
            |batch| emit_stream_batch(batch, ctx, parents, &mut emitted_slots),
            || {
                poll_stream_control(
                    &ctx.scan_signals,
                    ctx.stream_poll_delay,
                    allow_early_finish,
                    ctx.share_floors,
                    &mut floor_now,
                    &mut raises,
                )
            },
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    let identities = ctx
        .identity_enabled
        .then(|| {
            build_scan_identities(
                guard,
                ctx.slot_offset,
                index.len(),
                allow,
                ctx.start.include_rerank_candidate,
                emitted_slots,
            )
        })
        .transpose()?;
    let cancelled = ctx.scan_signals.cancelled.load(AtomicOrdering::Acquire);
    Ok((
        build_stream_summary(
            summary,
            cancelled,
            raises,
            geo_columns_known,
            filter_columns_known,
            scoring_fingerprint,
            prune,
        ),
        identities,
    ))
}
