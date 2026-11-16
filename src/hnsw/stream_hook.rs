//! Batching, exact scalar rescoring, and mid-stream control hook for HNSW streaming search.

use crate::vector::{VectorStreamBatch, VectorStreamControl, VectorStreamEndReason};

use super::graph::Hook;

/// Streaming emission batch: rows scored before the sink is called.
const STREAM_BATCH_ROWS: usize = 64;
/// Hops before `StreamHook` emits its early first-flush batch so the
/// coordinator establishes a global floor without waiting for 64 rows.
const STREAM_FIRST_FLUSH_HOPS: usize = 2;
/// Minimum buffered rows required for the early first-flush after
/// [`STREAM_FIRST_FLUSH_HOPS`] layer-0 hops.
const STREAM_FIRST_FLUSH_ROWS: usize = 8;

/// Streaming walk hook: rescores every allowed row with the scalar scorer,
/// buffers hits at or above the floor, emits batches ordered score descending
/// then row ascending, polls `control` after each expansion, and applies
/// raised floors, hop-boundary early finish, or stop decisions.
pub(super) struct StreamHook<'a> {
    pub(super) query_index: usize,
    pub(super) query: &'a [f32],
    pub(super) node_to_row: Option<&'a [u32]>,
    pub(super) floor: &'a mut f32,
    pub(super) allow_early_finish: bool,
    pub(super) hops: usize,
    pub(super) flushed_once: bool,
    pub(super) scores: Vec<f32>,
    pub(super) slots: Vec<i64>,
    pub(super) sink: &'a mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
    pub(super) control: &'a mut dyn FnMut() -> VectorStreamControl,
    pub(super) stopped: bool,
    pub(super) finish_reason: Option<VectorStreamEndReason>,
    pub(super) emitted: usize,
}

impl<'a> StreamHook<'a> {
    /// The hook of query `query_index` of a stream, with nothing buffered.
    pub(super) fn new(
        query_index: usize,
        query: &'a [f32],
        node_to_row: Option<&'a [u32]>,
        floor: &'a mut f32,
        allow_early_finish: bool,
        sink: &'a mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &'a mut dyn FnMut() -> VectorStreamControl,
    ) -> Self {
        Self {
            query_index,
            query,
            node_to_row,
            floor,
            allow_early_finish,
            hops: 0,
            flushed_once: false,
            scores: Vec::with_capacity(STREAM_BATCH_ROWS),
            slots: Vec::with_capacity(STREAM_BATCH_ROWS),
            sink,
            control,
            stopped: false,
            finish_reason: None,
            emitted: 0,
        }
    }

    pub(super) fn apply(&mut self, decision: VectorStreamControl) {
        match decision {
            VectorStreamControl::Continue => {}
            VectorStreamControl::RaiseFloor(raised) => {
                if raised > *self.floor {
                    *self.floor = raised;
                }
            }
            VectorStreamControl::Finish { deadline } => {
                if self.allow_early_finish && self.finish_reason.is_none() {
                    self.finish_reason = Some(if deadline {
                        VectorStreamEndReason::Deadline
                    } else {
                        VectorStreamEndReason::FinishRequested
                    });
                }
            }
            VectorStreamControl::Stop => self.stopped = true,
        }
    }

    pub(super) fn flush(&mut self) {
        if self.scores.is_empty() || self.stopped {
            self.scores.clear();
            self.slots.clear();
            return;
        }
        let floor = *self.floor;
        let mut kept = 0;
        for i in 0..self.scores.len() {
            if self.scores[i] >= floor {
                self.scores[kept] = self.scores[i];
                self.slots[kept] = self.slots[i];
                kept += 1;
            }
        }
        self.scores.truncate(kept);
        self.slots.truncate(kept);
        if kept == 0 {
            return;
        }
        let mut order: Vec<usize> = (0..kept).collect();
        order.sort_by(|&a, &b| {
            self.scores[b]
                .total_cmp(&self.scores[a])
                .then(self.slots[a].cmp(&self.slots[b]))
        });
        self.scores = order.iter().map(|&i| self.scores[i]).collect();
        self.slots = order.iter().map(|&i| self.slots[i]).collect();
        self.flushed_once = true;
        self.emitted += kept;
        let decision = (self.sink)(&VectorStreamBatch {
            query_index: self.query_index,
            block_base: 0,
            scores: &self.scores,
            slots: &self.slots,
        });
        self.scores.clear();
        self.slots.clear();
        self.apply(decision);
    }
}

impl Hook for StreamHook<'_> {
    #[inline]
    fn scored(&mut self, node: u32, _traversal_score: f32, row: &[f32]) {
        if self.stopped {
            return;
        }
        let score = crate::exact_vectors::dot(row, self.query);
        if score >= *self.floor {
            let ext_row = self.node_to_row.map_or(node, |m| m[node as usize]);
            self.scores.push(score);
            self.slots.push(i64::from(ext_row));
            if self.scores.len() >= STREAM_BATCH_ROWS {
                self.flush();
            }
        }
    }

    fn hop(&mut self) -> bool {
        self.hops += 1;
        if !self.stopped && self.finish_reason.is_none() {
            let decision = (self.control)();
            self.apply(decision);
        }
        if !self.stopped
            && self.finish_reason.is_none()
            && !self.flushed_once
            && self.hops >= STREAM_FIRST_FLUSH_HOPS
            && self.scores.len() >= STREAM_FIRST_FLUSH_ROWS
        {
            self.flush();
        }
        !self.stopped && self.finish_reason.is_none()
    }

    #[inline]
    fn current_floor(&self) -> f32 {
        *self.floor
    }
}
