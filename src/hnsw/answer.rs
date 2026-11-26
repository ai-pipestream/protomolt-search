//! Answering one query under a plan: the walk, the enumerate regime, the
//! handoffs between them, and the scalar rescoring of everything reported.

use crate::vector::{
    first_invalid_coordinate, FilterRegime, Mask, VectorError, VectorProvider, VectorSearchOptions,
};

use super::graph::{self, with_scratch, FloorHook, Hook, Patience, Scored, Walk};
use super::plan::Plan;
use super::{distance, Answer, HnswIndex, RowSource, SearchStats, EF_FLOOR};

impl HnswIndex {
    /// Rescore the walk's results with the scalar scorer, drop those below
    /// the floor, map internal nodes back to external row IDs when a seal-time
    /// node map is present, and order them score descending then row ascending.
    /// This is the only path a unary result takes out of the provider.
    pub(super) fn report(
        hits: Vec<Scored>,
        query: &[f32],
        rows: &dyn RowSource,
        floor: f32,
        k: usize,
        node_to_row: Option<&[u32]>,
    ) -> Vec<(u32, f32)> {
        let mut exact: Vec<(u32, f32)> = hits
            .into_iter()
            .map(|h| {
                let ext_row = node_to_row.map_or(h.node, |m| m[h.node as usize]);
                (ext_row, crate::exact_vectors::dot(rows.row(h.node), query))
            })
            .filter(|(_, score)| *score >= floor)
            .collect();
        exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        exact.truncate(k);
        exact
    }

    pub(super) fn check_queries(&self, queries: &[f32]) -> Result<usize, VectorError> {
        if !queries.len().is_multiple_of(self.dim) {
            return Err(VectorError::new(format!(
                "{} query values do not form rows of dimension {}",
                queries.len(),
                self.dim
            )));
        }
        if let Some((row, coord, value)) = first_invalid_coordinate(queries, self.dim) {
            return Err(VectorError::new(format!(
                "query {row} coordinate {coord} is invalid: {value}"
            )));
        }
        Ok(queries.len() / self.dim)
    }

    pub(super) fn slack_per_norm(&self, query: &[f32]) -> f64 {
        distance::kernel_dot_slack_per_norm(self.dim, query)
    }

    /// The enumerate regime: the exact top `k` of the allowed rows at or
    /// above the floor. Every allowed row is scored with the traversal
    /// kernel, and the scalar scorer settles the rows the kernel cannot
    /// rule out (`graph::View::enumerate`).
    pub(super) fn enumerate(
        &self,
        query: &[f32],
        k: usize,
        allow: Mask<'_>,
        floor: f32,
    ) -> (Vec<(u32, f32)>, SearchStats) {
        self.enumerate_skipping(query, k, allow, floor, &[], |_| false)
    }

    pub(super) fn enumerate_skipping(
        &self,
        query: &[f32],
        k: usize,
        allow: Mask<'_>,
        floor: f32,
        already_scored: &[Scored],
        skip: impl Fn(u32) -> bool,
    ) -> (Vec<(u32, f32)>, SearchStats) {
        let slack_per_norm = self.slack_per_norm(query);
        let mut survivors = Vec::new();
        let scored = with_view!(self, |view| view.enumerate_skipping(
            graph::EnumerateQuery {
                q: query,
                allow,
                k,
                floor,
                slack_per_norm,
                already_scored,
            },
            skip,
            &mut survivors
        ));
        let stats = SearchStats {
            scored,
            ..SearchStats::default()
        };
        let n2r = self.node_to_row_slice();
        let hits = self.with_row_source(|rows| Self::report(survivors, query, rows, floor, k, n2r));
        (hits, stats)
    }

    /// Walk the graph for `query` at beam `ef` as `plan` steers it,
    /// reporting to `hook`.
    pub(super) fn walk_plan<H: Hook>(
        &self,
        query: &[f32],
        ef: usize,
        patience: Patience,
        plan: &Plan<'_>,
        scratch: &mut graph::Scratch,
        hook: &mut H,
    ) -> (Vec<Scored>, SearchStats, Walk) {
        let floor_prune = plan.floor_prune(if plan.floor_prune_gate.is_some() {
            self.slack_per_norm(query)
        } else {
            0.0
        });
        let steering = graph::Steering {
            filter: plan.filter(),
            patience,
            floor_prune,
        };
        with_view!(self, |view| view.walk(query, ef, steering, scratch, hook))
    }

    /// One query under a plan: up to `k` `(row, score)` best first, and
    /// how it was answered.
    pub(super) fn answer(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        floor: f32,
        plan: &Plan<'_>,
        scratch: &mut graph::Scratch,
    ) -> (Vec<(u32, f32)>, Answer) {
        if !self.is_graph_linked() {
            let allow = plan.allow.unwrap_or(Mask::All(self.len()));
            let (hits, stats) = self.enumerate(query, k, allow, floor);
            let answer = Answer {
                tail_enumerated_rows: stats.scored,
                enumerated_part: true,
                ..Answer::of(plan.regime.map(|_| FilterRegime::Enumerate), stats)
            };
            return (hits, answer);
        }
        if let (Some(FilterRegime::Enumerate), Some(allow)) = (plan.regime, plan.allow) {
            let (hits, stats) = self.enumerate(query, k, allow, floor);
            return (hits, Answer::of(plan.regime, stats));
        }
        let patience = plan.patience(k, floor);
        let mut hook = FloorHook(floor);
        let (hits, mut stats, walk) = self.walk_plan(query, ef, patience, plan, scratch, &mut hook);
        match (walk, plan.allow) {
            (Walk::Far, Some(allow)) => {
                // The graph cannot serve this mask from where the query lands:
                // the answer is the exact top k of the mask.
                let (hits, exact) = self.enumerate(query, k, allow, floor);
                stats.scored += exact.scored;
                return (hits, Answer::of(Some(FilterRegime::Enumerate), stats));
            }
            (Walk::Short | Walk::Budget, Some(allow)) => {
                // Keep the allowed rows the walk already scored and score
                // only the remaining allowed rows in mask order.
                let already_scored = scratch.scored_allowed();
                let (hits, exact) =
                    self.enumerate_skipping(query, k, allow, floor, already_scored, |row| {
                        scratch.seen(row)
                    });
                stats.scored += exact.scored;
                let answer = Answer {
                    budget_handoff: walk == Walk::Budget,
                    ..Answer::of(Some(FilterRegime::Enumerate), stats)
                };
                return (hits, answer);
            }
            _ => {}
        }
        let n2r = self.node_to_row_slice();
        let hits = self.with_row_source(|rows| Self::report(hits, query, rows, floor, k, n2r));
        let regime = if scratch.walked_plain() {
            Some(FilterRegime::Plain)
        } else {
            plan.regime
        };
        let answer = Answer {
            patience_stopped: walk == Walk::Patience,
            floor_pruned: walk == Walk::FloorPruned,
            ..Answer::of(regime, stats)
        };
        (hits, answer)
    }

    /// One query at an explicit beam: up to `k` `(row, score)` best first
    /// among the allowed rows at or above the floor, with the regime that
    /// answered and its work. The beam is `max(beam, k, EF_FLOOR)`.
    pub fn search_with_stats(
        &self,
        query: &[f32],
        k: usize,
        beam: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<(Vec<(u32, f32)>, Answer), VectorError> {
        if self.check_queries(query)? != 1 {
            return Err(VectorError::new(
                "search_with_stats takes exactly one query",
            ));
        }
        let ef = beam.max(k).max(EF_FLOOR);
        let node_bits = self.node_allow_bits(options.allow)?;
        let effective_options = match &node_bits {
            Some(bits) => VectorSearchOptions {
                allow: Some(Mask::Bits(bits)),
                ..options
            },
            None => options,
        };
        let plan = self.plan(ef, &effective_options)?;
        let floor = effective_options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        Ok(with_scratch(|scratch| {
            self.answer(query, k, ef, floor, &plan, scratch)
        }))
    }
}
