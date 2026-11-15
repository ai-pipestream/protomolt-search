//! One prepared request: everything validation fixed before a shard is
//! asked, handed to the route that serves the selection's shape.

use std::time::Instant;

use super::boosts::BoostPlan;
use super::cursor::Cursor;
use super::prepare_dense::DensePlan;
use super::selection::Plan;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{LexicalQuery, QueryRequest};
use crate::turbo_bridge::query_plan::BoundOnePassQuery;

/// What every phase reads: the coordinator the request runs on (scoped
/// to the selection's vector field when it has a dense leaf), the
/// request, its validated selection, and the clocks.
#[derive(Clone, Copy)]
pub(super) struct Base<'a> {
    pub(super) coordinator: &'a CoordinatorServiceImpl,
    pub(super) req: &'a QueryRequest,
    pub(super) plan: &'a Plan<'a>,
    /// The Stage-2 deadline in epoch microseconds; `0` is unbounded.
    pub(super) finish_deadline_us: u64,
    pub(super) t_total: Instant,
}

/// The depths a route runs at.
pub(super) struct Depths {
    /// The candidate-set depth the final page is drawn from.
    pub(super) selection_k: u32,
    /// How deep the underlying route is asked to answer.
    pub(super) fetch_k: u32,
    /// Whether a collapse that finds too few groups may re-run deeper.
    pub(super) collapse_may_deepen: bool,
}

pub(super) struct Run<'a> {
    pub(super) base: Base<'a>,
    /// Purely observational: the same request with profile off returns
    /// identical hits, bitwise.
    pub(super) prof: Option<crate::pb::QueryProfile>,
    /// The selection's CEL filters as one predicate, with the one-pass
    /// analyzer's synthesized filter ANDed on when it found one.
    pub(super) filter: String,
    pub(super) one_pass: Option<BoundOnePassQuery>,
    pub(super) dense: DensePlan,
    pub(super) scorer: Option<crate::ltr::Scorer>,
    pub(super) boosts: BoostPlan<'a>,
    /// The projections fetched post-selection by id; empty when the
    /// route carries them natively.
    pub(super) projections: Vec<crate::pb::CompiledProjection>,
    pub(super) pool_aggregate: Option<crate::coordinator::CompiledAggregate>,
    pub(super) cursor: Option<Cursor>,
    pub(super) depths: Depths,
}

impl Run<'_> {
    /// The text the lexical route searches: the one-pass residual when
    /// the analyzer moved part of the query into a filter, else the
    /// leaf's own text.
    pub(super) fn effective_text(&self, query: &LexicalQuery) -> String {
        self.one_pass
            .as_ref()
            .filter(|b| !b.synthesized_cel.is_empty() && !b.residual_text.trim().is_empty())
            .map_or_else(|| query.text.clone(), |b| b.residual_text.clone())
    }

    /// The one-pass analyzer's residual terms, when they are the terms
    /// the leaf's own analysis would produce: a plain leaf (no phrase,
    /// no prefixes) under the body analysis.
    pub(super) fn residual_terms(&self, query: &LexicalQuery) -> Vec<String> {
        self.one_pass
            .as_ref()
            .filter(|b| {
                !b.residual_terms.is_empty()
                    && query.phrase.is_none()
                    && query.prefixes.is_empty()
                    && (query.analysis.is_none()
                        || query.analysis.as_ref() == Some(&crate::analyzer::body_spec()))
            })
            .map(|b| b.residual_terms.iter().map(|(t, _)| t.clone()).collect())
            .unwrap_or_default()
    }
}
