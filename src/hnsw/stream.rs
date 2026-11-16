//! Streaming search: every allowed row a walk or an enumerate pass scores
//! exactly at or above the floor, emitted in batches under a control.

use std::ops::ControlFlow;

use crate::vector::{
    FilterRegime, FilterRegimeCounts, Mask, VectorError, VectorProvider, VectorSearchOptions,
    VectorStreamBatch, VectorStreamControl, VectorStreamEndReason, VectorStreamSummary,
};

use super::distance::{self, prefetch};
use super::graph::{self, with_scratch, Hook, Walk};
use super::plan::Plan;
use super::stream_hook::StreamHook;
use super::{HnswIndex, EF_FLOOR, HNSW};

/// Rows the enumerate regime streams between polls of the control.
const STREAM_POLL_ROWS: usize = 16;

/// How one query of a stream was answered.
struct QueryOutcome {
    /// Work units the query spent: rows scored.
    scored: usize,
    /// False when a stop ended the query early.
    completed: bool,
    reason: VectorStreamEndReason,
    answered: Option<FilterRegime>,
    budget_handoff: bool,
}

impl QueryOutcome {
    /// An enumerate pass that ends the query, alone or after a walk;
    /// `scored` counts every row the query scored.
    fn enumerated(answered: Option<FilterRegime>, scored: usize, completed: bool) -> Self {
        Self {
            scored,
            completed,
            reason: VectorStreamEndReason::Exhausted,
            answered,
            budget_handoff: false,
        }
    }
}

/// The running totals of a stream across its queries.
#[derive(Default)]
struct StreamTotals {
    emitted: usize,
    units: usize,
    regimes: FilterRegimeCounts,
    tail_enumerated_rows: u64,
    patience_stopped: u32,
    floor_pruned: u32,
}

impl HnswIndex {
    /// Score one chunk of at most `STREAM_POLL_ROWS` rows into `hook`,
    /// prefetching the rows `ahead`, and poll the control after a full
    /// chunk. With a finite floor the chunk is scored by the SIMD kernel
    /// first and only the rows its error bound cannot rule out reach the
    /// scalar scorer; without one every row goes to the scalar scorer
    /// directly. Returns whether the pass continues.
    fn stream_rows_chunk<L: graph::LinkCells + ?Sized>(
        view: &graph::View<'_, L>,
        slack_per_norm: f64,
        chunk: &[u32],
        ahead: &[u32],
        hook: &mut StreamHook<'_>,
        total: &mut usize,
    ) -> bool {
        let lines = view.kernel.prefetch_lines(view.dim);
        let batch = view.kernel.batch_width();
        for &node in &ahead[..ahead.len().min(batch)] {
            prefetch(view.row(node), lines);
        }
        if (*hook.floor).is_finite() {
            let mut simd_scores = [0.0f32; STREAM_POLL_ROWS];
            view.kernel.score_nodes(
                hook.query,
                view.rows,
                view.dim,
                chunk,
                lines,
                &mut simd_scores[..chunk.len()],
            );
            for (&node, &simd_score) in chunk.iter().zip(&simd_scores[..chunk.len()]) {
                if hook.stopped {
                    return false;
                }
                *total += 1;
                let inv = view.inv_norm[node as usize];
                let keep = !(inv > 0.0 && inv.is_finite())
                    || !(*hook.floor).is_finite()
                    || f64::from(simd_score)
                        + distance::kernel_row_slack(view.dim, slack_per_norm, inv)
                        >= f64::from(*hook.floor);
                if keep {
                    hook.scored(node, simd_score, view.row(node));
                }
            }
        } else {
            for &node in chunk {
                if hook.stopped {
                    return false;
                }
                *total += 1;
                hook.scored(node, 0.0, view.row(node));
            }
        }
        if hook.stopped || hook.finish_reason.is_some() {
            return false;
        }
        chunk.len() < STREAM_POLL_ROWS || hook.hop()
    }

    /// The enumerate regime of a stream: every allowed row `skip` does not
    /// name, in row order. When a finite floor is active (from a prior
    /// segment's `minimum_score` or a mid-stream `RaiseFloor`), chunks of
    /// `STREAM_POLL_ROWS` rows are pre-filtered with the SIMD kernel using
    /// the exact error bound before invoking the scalar scorer; when no
    /// floor is active yet, rows go directly to the scalar scorer to avoid
    /// double-scoring. The control is polled every `STREAM_POLL_ROWS` rows.
    /// Returns the rows scored and whether the pass finished.
    fn stream_rows(
        &self,
        allow: Mask<'_>,
        skip: impl Fn(u32) -> bool,
        hook: &mut StreamHook<'_>,
    ) -> (usize, bool) {
        let slack_per_norm = self.slack_per_norm(hook.query);
        with_view!(self, |view| {
            let mut cur = [0u32; STREAM_POLL_ROWS];
            let mut cur_len = 0usize;
            let mut next = [0u32; STREAM_POLL_ROWS];
            let mut next_len = 0usize;
            let mut total = 0usize;
            let flow = allow.try_for_each(|row| {
                let node = row as u32;
                if skip(node) {
                    return ControlFlow::Continue(());
                }
                if cur_len < STREAM_POLL_ROWS {
                    cur[cur_len] = node;
                    cur_len += 1;
                } else {
                    next[next_len] = node;
                    next_len += 1;
                    if next_len == STREAM_POLL_ROWS {
                        let ok = Self::stream_rows_chunk(
                            &view,
                            slack_per_norm,
                            &cur[..cur_len],
                            &next[..next_len],
                            hook,
                            &mut total,
                        );
                        if !ok {
                            return ControlFlow::Break(());
                        }
                        cur = next;
                        cur_len = next_len;
                        next_len = 0;
                    }
                }
                ControlFlow::Continue(())
            });
            if flow.is_continue()
                && (cur_len == 0
                    || Self::stream_rows_chunk(
                        &view,
                        slack_per_norm,
                        &cur[..cur_len],
                        &next[..next_len],
                        hook,
                        &mut total,
                    ))
                && next_len > 0
            {
                let _ = Self::stream_rows_chunk(
                    &view,
                    slack_per_norm,
                    &next[..next_len],
                    &[],
                    hook,
                    &mut total,
                );
            }
            (total, !hook.stopped)
        })
    }

    /// One query of a stream under `plan`, reported through `hook`: the
    /// enumerate regime, or a walk that an enumerate pass over the allowed
    /// rows it did not reach finishes when it gives the query up.
    fn stream_query(
        &self,
        ef: usize,
        plan: &Plan<'_>,
        hook: &mut StreamHook<'_>,
        scratch: &mut graph::Scratch,
        totals: &mut StreamTotals,
    ) -> QueryOutcome {
        if !self.is_graph_linked() {
            let allow = plan.allow.unwrap_or(Mask::All(self.len()));
            let (rows, done) = self.stream_rows(allow, |_| false, hook);
            totals.tail_enumerated_rows = totals.tail_enumerated_rows.saturating_add(rows as u64);
            let answered = plan.regime.map(|_| FilterRegime::Enumerate);
            return QueryOutcome::enumerated(answered, rows, done);
        }
        if let (Some(FilterRegime::Enumerate), Some(allow)) = (plan.regime, plan.allow) {
            let (rows, done) = self.stream_rows(allow, |_| false, hook);
            return QueryOutcome::enumerated(plan.regime, rows, done);
        }
        let patience = plan.patience(ef, *hook.floor);
        let (_, stats, walk) = self.walk_plan(hook.query, ef, patience, plan, scratch, hook);
        let walked = stats.scored as usize;
        match (walk, plan.allow) {
            (Walk::Far, Some(allow)) => {
                let (rows, done) = self.stream_rows(allow, |_| false, hook);
                QueryOutcome::enumerated(Some(FilterRegime::Enumerate), walked + rows, done)
            }
            // The walk already emitted the rows it reached.
            (Walk::Short | Walk::Budget, Some(allow)) => {
                let (rows, done) = self.stream_rows(allow, |row| scratch.seen(row), hook);
                QueryOutcome {
                    budget_handoff: walk == Walk::Budget,
                    ..QueryOutcome::enumerated(Some(FilterRegime::Enumerate), walked + rows, done)
                }
            }
            (walk, _) => {
                if walk == Walk::Patience {
                    totals.patience_stopped = totals.patience_stopped.saturating_add(1);
                }
                if walk == Walk::FloorPruned {
                    totals.floor_pruned = totals.floor_pruned.saturating_add(1);
                }
                QueryOutcome {
                    scored: walked,
                    completed: walk != Walk::Stopped || hook.finish_reason.is_some(),
                    reason: if walk == Walk::FloorPruned {
                        VectorStreamEndReason::FloorPruned
                    } else {
                        VectorStreamEndReason::WalkConverged
                    },
                    answered: if scratch.walked_plain() {
                        Some(FilterRegime::Plain)
                    } else {
                        plan.regime
                    },
                    budget_handoff: false,
                }
            }
        }
    }

    /// The streaming search of [`VectorProvider::search_streaming_controlled`].
    pub(super) fn stream_search(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let query_count = self.check_queries(queries)?;
        let depth = options.candidate_depth.ok_or_else(|| {
            VectorError::new(format!(
                "{HNSW} streaming search needs the request's candidate depth; the beam is not \
                 part of the backend config"
            ))
        })?;
        if depth == 0 {
            return Err(VectorError::new(format!(
                "{HNSW} streaming search was given candidate depth 0"
            )));
        }
        let ef = depth.max(EF_FLOOR);
        let early_finish = options.filter_regime != Some(FilterRegime::Enumerate);
        let node_bits = self.node_allow_bits(options.allow)?;
        let effective_options = match &node_bits {
            Some(bits) => VectorSearchOptions {
                allow: Some(Mask::Bits(bits)),
                ..options
            },
            None => options,
        };
        let plan = self.plan(ef, &effective_options)?;
        let n2r = self.node_to_row_slice();
        let mut floor = effective_options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        let mut totals = StreamTotals::default();
        let mut stopped = false;
        let mut end_reason = VectorStreamEndReason::Unspecified;
        with_scratch(|scratch| {
            for (index, query) in queries.chunks_exact(self.dim).enumerate() {
                let mut hook =
                    StreamHook::new(index, query, n2r, &mut floor, early_finish, sink, control);
                let outcome = self.stream_query(ef, &plan, &mut hook, scratch, &mut totals);
                hook.flush();
                if outcome.budget_handoff {
                    totals.regimes.count_budget_handoff();
                } else if let Some(regime) = outcome.answered {
                    totals.regimes.count(regime);
                }
                totals.units += outcome.scored;
                totals.emitted += hook.emitted;
                if !outcome.completed || hook.stopped {
                    stopped = true;
                    end_reason = VectorStreamEndReason::Abandoned;
                    break;
                }
                if let Some(reason) = hook.finish_reason {
                    end_reason = end_reason.merge(reason);
                    break;
                }
                end_reason = end_reason.merge(outcome.reason);
            }
        });
        if !stopped && end_reason == VectorStreamEndReason::Unspecified {
            end_reason = VectorStreamEndReason::Exhausted;
        }
        Ok(VectorStreamSummary {
            query_count,
            emitted: totals.emitted,
            units_scanned: totals.units,
            completed: !stopped,
            end_reason,
            regimes: totals.regimes,
            tail_enumerated_rows: totals.tail_enumerated_rows,
            patience_stopped: totals.patience_stopped,
            floor_pruned: totals.floor_pruned,
        })
    }
}
