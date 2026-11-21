//! Shard-local streaming FP32 sidecar scan (`StartStreamSearch.exact_fp32_scan`).

use crate::node::*;

use super::exact_scan::{
    admitted_vector_rows, collect_checked_chunk_slots, exact_part_counts, exact_row_span,
    vector_row_ranges, EXACT_SCAN_CHUNK_ROWS,
};
use super::stream_scan::{
    build_scan_identities, emit_stream_batch, poll_stream_control, StreamScanCtx, StreamScanResult,
};
use super::validate_query_dim;

struct ExactStreamLoop<'a> {
    ctx: &'a StreamScanCtx,
    exact: &'a ExactVectorStore,
    parents: Option<&'a ParentMap>,
    allow: Option<&'a crate::boolean_bits::Bits>,
    ranges: &'a [(usize, usize)],
    parallel: usize,
}

#[derive(Default)]
struct ExactStreamTotals {
    emitted: u64,
    blocks: u64,
    raises: u64,
    fp32_rows: u64,
    fp32_bytes: u64,
    fp32_pages: u64,
    fp32_tasks: u32,
    stopped: bool,
    emitted_slots: Vec<u32>,
}

struct ChunkScratch {
    slots: Vec<usize>,
    batch_slots: Vec<i64>,
    batch_scores: Vec<f32>,
}

fn score_and_emit_chunk(
    run: &ExactStreamLoop<'_>,
    start: usize,
    floor_now: f32,
    scratch: &mut ChunkScratch,
    out: &mut ExactStreamTotals,
) -> Result<bool, Status> {
    if scratch.slots.is_empty() {
        return Ok(false);
    }
    let scored = run
        .exact
        .score_slots_profiled(&run.ctx.start.vector, &scratch.slots, run.parallel)
        .map_err(|e| Status::internal(format!("exact FP32 stream scan failed: {e}")))?;
    out.blocks += 1;
    out.fp32_rows += scored.rows.len() as u64;
    out.fp32_bytes += scored.logical_bytes;
    out.fp32_pages += scored.pages_touched;
    out.fp32_tasks = out.fp32_tasks.max(scored.tasks);
    scratch.batch_slots.clear();
    scratch.batch_scores.clear();
    for (slot, score) in scored.rows {
        if score >= floor_now {
            scratch.batch_slots.push(slot as i64);
            scratch.batch_scores.push(score);
        }
    }
    if scratch.batch_slots.is_empty() {
        return Ok(false);
    }
    out.emitted += scratch.batch_slots.len() as u64;
    let batch = crate::vector::VectorStreamBatch {
        query_index: 0,
        block_base: start,
        scores: &scratch.batch_scores,
        slots: &scratch.batch_slots,
    };
    Ok(
        emit_stream_batch(&batch, run.ctx, run.parents, &mut out.emitted_slots)
            == VectorStreamControl::Stop,
    )
}

fn stream_exact_chunks(run: ExactStreamLoop<'_>) -> Result<ExactStreamTotals, Status> {
    let mut floor_now = if run.ctx.share_floors {
        run.ctx.start.initial_floor.unwrap_or(f32::NEG_INFINITY)
    } else {
        f32::NEG_INFINITY
    };
    let mut out = ExactStreamTotals::default();
    let mut scratch = ChunkScratch {
        slots: Vec::new(),
        batch_slots: Vec::new(),
        batch_scores: Vec::new(),
    };
    'ranges: for &(base, len) in run.ranges {
        let range_end = base + len;
        let mut start = base;
        while start < range_end {
            let ctl = poll_stream_control(
                &run.ctx.scan_signals,
                run.ctx.stream_poll_delay,
                false,
                run.ctx.share_floors,
                &mut floor_now,
                &mut out.raises,
            );
            if ctl == VectorStreamControl::Stop {
                out.stopped = true;
                break 'ranges;
            }
            let end = (start + EXACT_SCAN_CHUNK_ROWS).min(range_end);
            collect_checked_chunk_slots(
                run.exact,
                run.allow,
                start,
                end,
                run.ctx.slot_offset,
                &mut scratch.slots,
            )?;
            if score_and_emit_chunk(&run, start, floor_now, &mut scratch, &mut out)? {
                out.stopped = true;
                break 'ranges;
            }
            start = end;
        }
    }
    Ok(out)
}

fn build_exact_stream_summary(
    totals: &ExactStreamTotals,
    parts: crate::vector::PartCounts,
    prune: crate::segment_prune::PruneStats,
    known: (Vec<bool>, Vec<bool>),
    scoring_fingerprint: String,
    completed: bool,
) -> StreamSearchSummary {
    let end_reason = if completed {
        crate::pb::StreamEndReason::Exhausted as i32
    } else {
        crate::pb::StreamEndReason::Abandoned as i32
    };
    StreamSearchSummary {
        completed,
        emitted: totals.emitted,
        blocks_scanned: totals.blocks,
        floor_raises_applied: totals.raises,
        geo_columns_known: known.0,
        filter_columns_known: known.1,
        scoring_fingerprint,
        segments_total: prune.segments_total,
        segments_skipped: prune.segments_skipped,
        filter_regimes: None,
        tail_enumerated_rows: 0,
        patience_stopped: 0,
        end_reason,
        floor_pruned: 0,
        parts_total: parts.total,
        parts_skipped: parts.skipped,
        parts_walked: 0,
        parts_enumerated: if completed { parts.enumerated } else { 0 },
        parts_scanned: 0,
        parts_staged: 0,
        fp32_rows_scored: totals.fp32_rows,
        fp32_logical_bytes: totals.fp32_bytes,
        fp32_pages_touched: totals.fp32_pages,
        fp32_tasks: totals.fp32_tasks,
    }
}

fn scoring_fingerprint_for(guard: &ShardState, dim: usize) -> String {
    guard
        .index
        .as_ref()
        .map(|i| i.descriptor().scoring_fingerprint)
        .filter(|fp| !fp.is_empty())
        .unwrap_or_else(|| format!("exact-fp32:dim={dim}"))
}

pub(super) fn run_exact_fp32_stream_scan(ctx: StreamScanCtx, parallel: usize) -> StreamScanResult {
    let parents = if ctx.start.collapse_parents {
        let n = exact_row_span(&read_shard(&ctx.state));
        Some(NodeServiceImpl::parent_map(&ctx.state, ctx.slot_offset, n)?)
    } else {
        None
    };
    let guard = read_shard(&ctx.state);
    let rows = exact_row_span(&guard);
    if let Some(p) = parents.as_ref() {
        guard.check_stats_epoch(p.claim.epoch, &p.claim.incarnation())?;
        if p.rows.len() != rows {
            return Err(Status::aborted("shard grew between setup and scan; retry"));
        }
    }
    let (exact, dim) = NodeServiceImpl::prepare_exact_sidecar(&guard, ctx.slot_offset, Some(rows))?;
    validate_query_dim(&ctx.start.vector, dim)?;
    let scoring_fingerprint = scoring_fingerprint_for(&guard, dim);
    let ((_, allow, prune), receipt) = guard.vector_scan_filters(
        rows,
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
    let known = filter_known_flags(
        guard.bm25.as_ref(),
        &ctx.start.geo_filters,
        ctx.start.filter.as_ref(),
    );
    let ranges = vector_row_ranges(&guard, rows);
    let admitted = admitted_vector_rows(&ranges, allow.as_ref());
    let parts = exact_part_counts(&guard, rows, allow.as_ref());
    let totals = stream_exact_chunks(ExactStreamLoop {
        ctx: &ctx,
        exact,
        parents: parents.as_ref(),
        allow: allow.as_ref(),
        ranges: &ranges,
        parallel,
    })?;
    let completed = !totals.stopped && !ctx.scan_signals.cancelled.load(AtomicOrdering::Acquire);
    if completed && totals.fp32_rows != admitted {
        return Err(Status::internal(format!(
            "exact FP32 stream scan scored {} rows, expected {admitted} admitted live vector rows",
            totals.fp32_rows
        )));
    }
    let identities = ctx
        .identity_enabled
        .then(|| {
            build_scan_identities(
                &guard,
                ctx.slot_offset,
                rows,
                allow,
                ctx.start.include_rerank_candidate,
                totals.emitted_slots.clone(),
            )
        })
        .transpose()?;
    Ok((
        build_exact_stream_summary(&totals, parts, prune, known, scoring_fingerprint, completed),
        identities,
    ))
}
