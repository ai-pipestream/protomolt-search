//! Shard-local unary FP32 sidecar top-`k` scan and shared row-range accounting.

use crate::node::*;

use super::search_shard::{prepare_exec_filters, SearchShardExec};
use super::{collect_rerank_candidates, validate_query_dim, DonePayload};

pub(super) const EXACT_SCAN_CHUNK_ROWS: usize = 4096;

pub(super) fn exact_row_span(guard: &ShardState) -> usize {
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

pub(super) fn exact_part_counts(
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

pub(super) fn vector_row_ranges(guard: &ShardState, rows: usize) -> Vec<(usize, usize)> {
    match guard.index.as_ref() {
        Some(index) => index
            .vector_rows()
            .into_iter()
            .filter(|&(_, len)| len > 0)
            .collect(),
        None if rows > 0 => vec![(0, rows)],
        None => Vec::new(),
    }
}

pub(super) fn admitted_vector_rows(
    ranges: &[(usize, usize)],
    allow: Option<&crate::boolean_bits::Bits>,
) -> u64 {
    ranges
        .iter()
        .map(|&(base, len)| match allow {
            Some(bits) => bits.count_range(base, base + len) as u64,
            None => len as u64,
        })
        .sum()
}

pub(super) fn collect_checked_chunk_slots(
    exact: &ExactVectorStore,
    allow: Option<&crate::boolean_bits::Bits>,
    start: usize,
    end: usize,
    slot_offset: u64,
    slots: &mut Vec<usize>,
) -> Result<(), Status> {
    slots.clear();
    match allow {
        Some(bits) => slots.extend(bits.iter_range(start, end)),
        None => slots.extend(start..end),
    }
    if let Some(&missing) = slots.iter().find(|&&slot| !exact.contains_row(slot)) {
        return Err(Status::failed_precondition(format!(
            "exact-vector sidecar is missing admitted live vector row {}",
            slot_offset + missing as u64
        )));
    }
    Ok(())
}

struct ExactChunkAccum {
    heap: Vec<ChunkHit>,
    spare: Vec<ChunkHit>,
    chunk: Vec<ChunkHit>,
    slots: Vec<usize>,
    stats: ScanStats,
}

impl ExactChunkAccum {
    fn score_ranges(
        &mut self,
        exact: &ExactVectorStore,
        exec: &mut SearchShardExec,
        allow: Option<&crate::boolean_bits::Bits>,
        ranges: &[(usize, usize)],
        parallel: usize,
    ) -> Result<(), Status> {
        let k = exec.start.k as usize;
        if k == 0 {
            return Ok(());
        }
        let mut floor = f32::NEG_INFINITY;
        for &(base, len) in ranges {
            let range_end = base + len;
            let mut start = base;
            while start < range_end {
                let end = (start + EXACT_SCAN_CHUNK_ROWS).min(range_end);
                if let Some(ext) = (exec.external)() {
                    if !ext.is_nan() && ext > floor {
                        floor = ext;
                        self.stats.floor_updates_applied += 1;
                    }
                }
                collect_checked_chunk_slots(
                    exact,
                    allow,
                    start,
                    end,
                    exec.slot_offset,
                    &mut self.slots,
                )?;
                if !self.slots.is_empty() {
                    let t0 = std::time::Instant::now();
                    let scored = exact
                        .score_slots_profiled(&exec.start.vector, &self.slots, parallel)
                        .map_err(|e| Status::internal(format!("exact FP32 scan failed: {e}")))?;
                    self.stats.chunk_calls += 1;
                    self.stats.scan_nanos +=
                        u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    self.stats.fp32_rows_scored += scored.rows.len() as u64;
                    self.stats.fp32_logical_bytes += scored.logical_bytes;
                    self.stats.fp32_pages_touched += scored.pages_touched;
                    self.stats.fp32_tasks = self.stats.fp32_tasks.max(scored.tasks);
                    self.absorb_scored(scored.rows, k, &mut floor, &mut *exec.publish);
                }
                start = end;
            }
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
    let ranges = vector_row_ranges(&guard, rows);
    let admitted = admitted_vector_rows(&ranges, filters.allow.as_ref());
    let mut acc = ExactChunkAccum {
        heap: Vec::with_capacity(exec.start.k as usize),
        spare: Vec::new(),
        chunk: Vec::new(),
        slots: Vec::new(),
        stats: ScanStats::default(),
    };
    if exec.start.k > 0 {
        acc.stats.parts = exact_part_counts(&guard, rows, filters.allow.as_ref());
        acc.score_ranges(exact, &mut exec, filters.allow.as_ref(), &ranges, parallel)?;
        if acc.stats.fp32_rows_scored != admitted {
            return Err(Status::internal(format!(
                "exact FP32 scan scored {} rows, expected {admitted} admitted live vector rows",
                acc.stats.fp32_rows_scored
            )));
        }
    }
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
