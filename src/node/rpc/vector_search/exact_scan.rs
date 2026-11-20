//! Shard-local FP32 sidecar top-`k` and streaming scans (`exact_fp32_scan`).

use crate::node::*;

use super::search_shard::{prepare_exec_filters, SearchShardExec};
use super::stream_scan::{
    build_scan_identities, emit_stream_batch, poll_stream_control, StreamScanCtx, StreamScanResult,
};
use super::{collect_rerank_candidates, validate_query_dim, DonePayload};

const EXACT_SCAN_CHUNK_ROWS: usize = 4096;

fn exact_row_span(guard: &ShardState) -> usize {
    guard.index.as_ref().map_or_else(
        || {
            guard
                .exact_vectors
                .as_ref()
                .map_or(0, ExactVectorStore::len)
        },
        VectorIndex::len,
    )
}

fn exact_part_counts(
    guard: &ShardState,
    rows: usize,
    allow: Option<&crate::boolean_bits::Bits>,
) -> crate::vector::PartCounts {
    let (total, skipped) = match guard.index.as_ref() {
        Some(index) => index.part_counts(allow.map(crate::vector::Mask::Bits)),
        None if rows == 0 => (0, 0),
        None => (
            1,
            u32::from(allow.is_some_and(|bits| bits.count_range(0, rows) == 0)),
        ),
    };
    crate::vector::PartCounts {
        total,
        skipped,
        enumerated: total.saturating_sub(skipped),
        ..Default::default()
    }
}

fn collect_chunk_slots(
    exact: &ExactVectorStore,
    allow: Option<&crate::boolean_bits::Bits>,
    start: usize,
    end: usize,
    slots: &mut Vec<usize>,
) {
    slots.clear();
    match allow {
        Some(bits) => {
            slots.extend(
                bits.iter_range(start, end)
                    .filter(|&slot| exact.contains_row(slot)),
            );
        }
        None => {
            slots.extend((start..end).filter(|&slot| exact.contains_row(slot)));
        }
    }
}

struct ExactChunkAccum {
    heap: Vec<ChunkHit>,
    spare: Vec<ChunkHit>,
    chunk: Vec<ChunkHit>,
    slots: Vec<usize>,
    stats: ScanStats,
}

impl ExactChunkAccum {
    fn score_range(
        &mut self,
        exact: &ExactVectorStore,
        exec: &mut SearchShardExec,
        allow: Option<&crate::boolean_bits::Bits>,
        rows: usize,
        parallel: usize,
    ) -> Result<(), Status> {
        let k = exec.start.k as usize;
        let mut floor = f32::NEG_INFINITY;
        let mut start = 0;
        while start < rows {
            let end = (start + EXACT_SCAN_CHUNK_ROWS).min(rows);
            if let Some(ext) = (exec.external)() {
                if !ext.is_nan() && ext > floor {
                    floor = ext;
                    self.stats.floor_updates_applied += 1;
                }
            }
            collect_chunk_slots(exact, allow, start, end, &mut self.slots);
            if !self.slots.is_empty() && k > 0 {
                let t0 = std::time::Instant::now();
                let scored = exact
                    .score_slots_profiled(&exec.start.vector, &self.slots, parallel)
                    .map_err(|e| Status::internal(format!("exact FP32 scan failed: {e}")))?;
                self.stats.chunk_calls += 1;
                self.stats.scan_nanos += u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
                self.stats.fp32_rows_scored += scored.rows.len() as u64;
                self.stats.fp32_logical_bytes += scored.logical_bytes;
                self.stats.fp32_pages_touched += scored.pages_touched;
                self.stats.fp32_tasks = self.stats.fp32_tasks.max(scored.tasks);
                self.absorb_scored(scored.rows, k, &mut floor, &mut *exec.publish);
            }
            start = end;
        }
        Ok(())
    }

    fn absorb_scored(
        &mut self,
        rows: Vec<(usize, f32)>,
        k: usize,
        floor: &mut f32,
        publish: &mut dyn FnMut(f32) -> bool,
    ) {
        self.chunk.clear();
        for (slot, score) in rows {
            if score >= *floor {
                self.stats.candidates_collected += 1;
                self.chunk.push(ChunkHit {
                    slot: slot as u32,
                    score,
                });
            }
        }
        crate::chunked::merge_chunk_hits(&mut self.heap, &mut self.spare, &mut self.chunk, k, true);
        if self.heap.len() >= k {
            let kth = self.heap[k - 1].score;
            *floor = (*floor).max(kth);
            self.stats.floors_offered += 1;
            if publish(kth) {
                self.stats.floors_published += 1;
            }
        }
    }
}

pub(super) fn run_exact_fp32_shard_scan(
    mut exec: SearchShardExec,
    parallel: usize,
) -> Result<DonePayload, Status> {
    let state = Arc::clone(&exec.state);
    let guard = read_shard(&state);
    let rows = exact_row_span(&guard);
    let (exact, dim) =
        NodeServiceImpl::prepare_exact_sidecar(&guard, exec.slot_offset, Some(rows))?;
    validate_query_dim(&exec.start.vector, dim)?;
    let filters = prepare_exec_filters(&guard, rows, &exec)?;
    let mut acc = ExactChunkAccum {
        heap: Vec::with_capacity(exec.start.k as usize),
        spare: Vec::new(),
        chunk: Vec::new(),
        slots: Vec::new(),
        stats: ScanStats::default(),
    };
    acc.stats.parts = exact_part_counts(&guard, rows, filters.allow.as_ref());
    acc.score_range(exact, &mut exec, filters.allow.as_ref(), rows, parallel)?;
    acc.stats.segments_total = filters.prune.segments_total;
    acc.stats.segments_skipped = filters.prune.segments_skipped;
    crate::metrics::record_scan(&acc.stats);
    let identities = guard
        .bm25
        .as_ref()
        .map(Bm25Shard::identity_snapshot)
        .transpose()
        .map_err(Status::internal)?
        .unwrap_or_default();
    let rerank_candidates = collect_rerank_candidates(
        &guard,
        exec.start.include_rerank_candidate,
        acc.heap.iter().map(|h| h.slot),
    );
    Ok(DonePayload {
        hits: acc.heap.into_iter().map(|h| (h.slot, h.score, 0)).collect(),
        stats: acc.stats,
        identities,
        rerank_candidates,
        geo_columns_known: filters.geo_columns_known,
        filter_columns_known: filters.filter_columns_known,
    })
}

struct ExactStreamLoop<'a> {
    ctx: &'a StreamScanCtx,
    exact: &'a ExactVectorStore,
    parents: Option<&'a ParentMap>,
    allow: Option<&'a crate::boolean_bits::Bits>,
    rows: usize,
    parallel: usize,
}

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

fn stream_exact_chunks(run: ExactStreamLoop<'_>) -> Result<ExactStreamTotals, Status> {
    let mut floor_now = if run.ctx.share_floors {
        run.ctx.start.initial_floor.unwrap_or(f32::NEG_INFINITY)
    } else {
        f32::NEG_INFINITY
    };
    let mut out = ExactStreamTotals {
        emitted: 0,
        blocks: 0,
        raises: 0,
        fp32_rows: 0,
        fp32_bytes: 0,
        fp32_pages: 0,
        fp32_tasks: 0,
        stopped: false,
        emitted_slots: Vec::new(),
    };
    let mut slots = Vec::new();
    let mut batch_slots = Vec::new();
    let mut batch_scores = Vec::new();
    let mut start = 0;
    while start < run.rows {
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
            break;
        }
        let end = (start + EXACT_SCAN_CHUNK_ROWS).min(run.rows);
        collect_chunk_slots(run.exact, run.allow, start, end, &mut slots);
        if !slots.is_empty() {
            let scored = run
                .exact
                .score_slots_profiled(&run.ctx.start.vector, &slots, run.parallel)
                .map_err(|e| Status::internal(format!("exact FP32 stream scan failed: {e}")))?;
            out.blocks += 1;
            out.fp32_rows += scored.rows.len() as u64;
            out.fp32_bytes += scored.logical_bytes;
            out.fp32_pages += scored.pages_touched;
            out.fp32_tasks = out.fp32_tasks.max(scored.tasks);
            batch_slots.clear();
            batch_scores.clear();
            for (slot, score) in scored.rows {
                if score >= floor_now {
                    batch_slots.push(slot as i64);
                    batch_scores.push(score);
                }
            }
            if !batch_slots.is_empty() {
                out.emitted += batch_slots.len() as u64;
                let batch = crate::vector::VectorStreamBatch {
                    query_index: 0,
                    block_base: start,
                    scores: &batch_scores,
                    slots: &batch_slots,
                };
                if emit_stream_batch(&batch, run.ctx, run.parents, &mut out.emitted_slots)
                    == VectorStreamControl::Stop
                {
                    out.stopped = true;
                    break;
                }
            }
        }
        start = end;
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
    let scoring_fingerprint = guard
        .index
        .as_ref()
        .map(|i| i.descriptor().scoring_fingerprint)
        .filter(|fp| !fp.is_empty())
        .unwrap_or_else(|| format!("exact-fp32:dim={dim}"));
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
    let parts = exact_part_counts(&guard, rows, allow.as_ref());
    let totals = stream_exact_chunks(ExactStreamLoop {
        ctx: &ctx,
        exact,
        parents: parents.as_ref(),
        allow: allow.as_ref(),
        rows,
        parallel,
    })?;
    let completed = !totals.stopped && !ctx.scan_signals.cancelled.load(AtomicOrdering::Acquire);
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
