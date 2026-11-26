//! Per-part option projection and result/stream aggregation for [`super::SegmentedProvider`].

use std::cell::Cell;

use crate::vector::{
    FilterRegime, FilterRegimeCounts, Mask, PartCounts, QualityContract, VectorError, VectorIndex,
    VectorSearchOptions, VectorSearchResults, VectorStreamBatch, VectorStreamControl,
    VectorStreamEndReason, VectorStreamSummary,
};

/// Build per-segment [`VectorSearchOptions`] by exhaustively destructuring the
/// caller's options so every query knob (including `filter_routing`) is
/// forwarded without silent default fallback.
pub(super) fn segment_options<'a>(
    options: &VectorSearchOptions<'_>,
    part_allow: Option<Mask<'a>>,
    minimum_score: Option<f32>,
    part_rows: usize,
    shard_eligible: usize,
) -> VectorSearchOptions<'a> {
    let VectorSearchOptions {
        allow: _,
        minimum_score: _,
        candidate_depth,
        filter_regime,
        enumerate_max_rows,
        filter_routing,
        patience,
        floor_prune,
    } = *options;
    let floor_prune = floor_prune.map(|fp| {
        let seg_eligible = part_allow.map_or(part_rows, |m| m.count());
        fp.for_segment(seg_eligible, shard_eligible)
    });
    VectorSearchOptions {
        allow: part_allow,
        minimum_score,
        candidate_depth,
        filter_regime,
        enumerate_max_rows,
        filter_routing,
        patience,
        floor_prune,
    }
}

#[derive(Default)]
pub(super) struct UnaryAccum {
    parts: PartCounts,
    regimes: FilterRegimeCounts,
    tail_enumerated_rows: u64,
    patience_stopped: u32,
    floor_pruned: u32,
}

impl UnaryAccum {
    pub(super) fn note_skipped(&mut self) {
        self.parts.total = self.parts.total.saturating_add(1);
        self.parts.skipped = self.parts.skipped.saturating_add(1);
    }

    pub(super) fn absorb(&mut self, results: &VectorSearchResults) {
        self.parts.add(results.parts);
        self.regimes.add(results.regimes);
        self.tail_enumerated_rows = self
            .tail_enumerated_rows
            .saturating_add(results.tail_enumerated_rows);
        self.patience_stopped = self
            .patience_stopped
            .saturating_add(results.patience_stopped);
        self.floor_pruned = self.floor_pruned.saturating_add(results.floor_pruned);
    }

    pub(super) fn finish(
        self,
        mut per_query: Vec<Vec<(f32, i64)>>,
        k: usize,
    ) -> VectorSearchResults {
        let nq = per_query.len();
        let mut scores = Vec::with_capacity(nq * k);
        let mut slots = Vec::with_capacity(nq * k);
        for hits in &mut per_query {
            hits.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            hits.truncate(k);
            for &(score, slot) in hits.iter() {
                scores.push(score);
                slots.push(slot);
            }
            for _ in hits.len()..k {
                scores.push(f32::NEG_INFINITY);
                slots.push(-1);
            }
        }
        VectorSearchResults {
            scores,
            slots,
            query_count: nq,
            result_count: k,
            parts: self.parts,
            regimes: self.regimes,
            tail_enumerated_rows: self.tail_enumerated_rows,
            patience_stopped: self.patience_stopped,
            floor_pruned: self.floor_pruned,
        }
    }
}

pub(super) fn collect_part_hits(
    results: &VectorSearchResults,
    base: usize,
    k: usize,
    running_floor: &mut Option<f32>,
    per_query: &mut [Vec<(f32, i64)>],
) {
    for (q, hits) in per_query.iter_mut().enumerate() {
        for (&slot, &score) in results
            .slots_for_query(q)
            .iter()
            .zip(results.scores_for_query(q))
        {
            if slot >= 0 && score.is_finite() {
                hits.push((score, slot + base as i64));
            }
        }
    }
    if per_query.len() == 1 && k > 0 && per_query[0].len() >= k {
        per_query[0].sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
        per_query[0].truncate(k);
        let kth = per_query[0][k - 1].0;
        *running_floor = Some(running_floor.map_or(kth, |cur| cur.max(kth)));
    }
}

pub(super) struct StreamState {
    floor: Cell<Option<f32>>,
    stopped: Cell<bool>,
    finished: Cell<bool>,
    end_reason: Cell<VectorStreamEndReason>,
    allow_early_finish: bool,
    emitted: usize,
    units: usize,
    parts: PartCounts,
    regimes: FilterRegimeCounts,
    tail_enumerated_rows: u64,
    patience_stopped: u32,
    floor_pruned: u32,
}

impl StreamState {
    pub(super) fn new(
        quality_contract: QualityContract,
        options: &VectorSearchOptions<'_>,
        parts_total: u32,
    ) -> Self {
        let allow_early_finish = matches!(
            quality_contract,
            QualityContract::ConfiguredAnn | QualityContract::ProbabilisticBound
        ) && options.filter_regime != Some(FilterRegime::Enumerate);
        Self {
            floor: Cell::new(options.minimum_score),
            stopped: Cell::new(false),
            finished: Cell::new(false),
            end_reason: Cell::new(VectorStreamEndReason::Unspecified),
            allow_early_finish,
            emitted: 0,
            units: 0,
            parts: PartCounts {
                total: parts_total,
                ..Default::default()
            },
            regimes: FilterRegimeCounts::default(),
            tail_enumerated_rows: 0,
            patience_stopped: 0,
            floor_pruned: 0,
        }
    }

    pub(super) fn note_skipped(&mut self) {
        self.parts.skipped = self.parts.skipped.saturating_add(1);
    }

    pub(super) fn is_done(&self) -> bool {
        self.stopped.get() || self.finished.get()
    }

    pub(super) fn current_floor(&self) -> Option<f32> {
        self.floor.get()
    }

    fn note(&self, verdict: VectorStreamControl) {
        match verdict {
            VectorStreamControl::RaiseFloor(f) => {
                self.floor
                    .set(Some(self.floor.get().map_or(f, |cur| cur.max(f))));
            }
            VectorStreamControl::Finish { deadline } if self.allow_early_finish => {
                self.finished.set(true);
                let reason = if deadline {
                    VectorStreamEndReason::Deadline
                } else {
                    VectorStreamEndReason::FinishRequested
                };
                self.end_reason.set(self.end_reason.get().merge(reason));
            }
            VectorStreamControl::Finish { .. } | VectorStreamControl::Continue => {}
            VectorStreamControl::Stop => {
                self.stopped.set(true);
                self.end_reason.set(VectorStreamEndReason::Abandoned);
            }
        }
    }

    pub(super) fn run_part(
        &mut self,
        (base, image): (usize, &VectorIndex),
        queries: &[f32],
        part_options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<(), VectorError> {
        let summary = {
            let state = &*self;
            let mut remapped: Vec<i64> = Vec::new();
            let mut part_sink = |batch: &VectorStreamBatch<'_>| -> VectorStreamControl {
                remapped.clear();
                remapped.extend(
                    batch
                        .slots
                        .iter()
                        .map(|&s| if s < 0 { s } else { s + base as i64 }),
                );
                let outward = VectorStreamBatch {
                    query_index: batch.query_index,
                    block_base: batch.block_base,
                    scores: batch.scores,
                    slots: &remapped,
                };
                let verdict = sink(&outward);
                state.note(verdict);
                verdict
            };
            let mut part_control = || -> VectorStreamControl {
                let verdict = control();
                state.note(verdict);
                verdict
            };
            image.try_search_streaming_controlled(
                queries,
                part_options,
                &mut part_sink,
                &mut part_control,
            )?
        };
        self.emitted += summary.emitted;
        self.units += summary.units_scanned;
        self.parts.skipped = self.parts.skipped.saturating_add(summary.parts.skipped);
        self.parts.walked = self.parts.walked.saturating_add(summary.parts.walked);
        self.parts.enumerated = self
            .parts
            .enumerated
            .saturating_add(summary.parts.enumerated);
        self.parts.scanned = self.parts.scanned.saturating_add(summary.parts.scanned);
        self.parts.staged = self.parts.staged.saturating_add(summary.parts.staged);
        self.regimes.add(summary.regimes);
        self.tail_enumerated_rows = self
            .tail_enumerated_rows
            .saturating_add(summary.tail_enumerated_rows);
        self.patience_stopped = self
            .patience_stopped
            .saturating_add(summary.patience_stopped);
        self.floor_pruned = self.floor_pruned.saturating_add(summary.floor_pruned);
        self.end_reason
            .set(self.end_reason.get().merge(summary.end_reason));
        if summary.end_reason.is_early_finish() {
            self.finished.set(true);
        }
        if !summary.completed {
            self.stopped.set(true);
            self.end_reason.set(VectorStreamEndReason::Abandoned);
        }
        Ok(())
    }

    pub(super) fn into_summary(self, query_count: usize) -> VectorStreamSummary {
        let end_reason = if self.stopped.get() {
            VectorStreamEndReason::Abandoned
        } else if self.end_reason.get() == VectorStreamEndReason::Unspecified {
            VectorStreamEndReason::Exhausted
        } else {
            self.end_reason.get()
        };
        VectorStreamSummary {
            query_count,
            emitted: self.emitted,
            units_scanned: self.units,
            completed: !self.stopped.get(),
            end_reason,
            parts: if query_count == 0 {
                PartCounts::default()
            } else {
                self.parts
            },
            regimes: self.regimes,
            tail_enumerated_rows: self.tail_enumerated_rows,
            patience_stopped: self.patience_stopped,
            floor_pruned: self.floor_pruned,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::boolean_bits::Bits;
    use crate::vector::{
        FilterRegime, FilterRouting, FloorPruneSpec, Mask, PatienceSpec, VectorSearchOptions,
    };

    use super::segment_options;

    #[test]
    fn segment_options_preserves_every_field_and_rebinds_part_allow() {
        let mut global_bits = Bits::empty(16);
        global_bits.set(1);
        global_bits.set(9);
        let mut part_bits = Bits::empty(8);
        part_bits.set(2);
        part_bits.set(5);

        let patience = PatienceSpec::from_ppm(975_000, 4);
        let floor_prune = FloorPruneSpec {
            enabled: true,
            floor_k: 8,
            share_ppm: 120_000,
            greediness_ppm: 850_000,
            clamp_min: 4,
        };

        let base = VectorSearchOptions::new()
            .with_bits(&global_bits)
            .with_filter_routing(FilterRouting::Exact)
            .with_filter_regime(FilterRegime::FilterAware)
            .with_enumerate_max_rows(2048)
            .with_candidate_depth(96)
            .with_minimum_score(0.10)
            .with_patience(patience)
            .with_floor_prune(floor_prune);

        let scoped = segment_options(&base, Some(Mask::Bits(&part_bits)), Some(0.42), 8, 4);
        assert!(matches!(scoped.allow, Some(Mask::Bits(bm)) if bm.count() == 2 && bm.test(5)));
        assert_eq!(scoped.minimum_score, Some(0.42));
        assert_eq!(scoped.filter_routing, FilterRouting::Exact);
        assert_eq!(scoped.filter_regime, Some(FilterRegime::FilterAware));
        assert_eq!(scoped.enumerate_max_rows, Some(2048));
        assert_eq!(scoped.candidate_depth, Some(96));
        assert_eq!(scoped.patience, Some(patience));
        assert_eq!(scoped.floor_prune, Some(floor_prune.for_segment(2, 4)));

        let unscoped = segment_options(&base, None, None, 8, 8);
        assert!(unscoped.allow.is_none());
        assert_eq!(unscoped.minimum_score, None);
        assert_eq!(unscoped.filter_routing, FilterRouting::Exact);
    }
}
