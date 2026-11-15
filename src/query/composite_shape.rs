//! The two-leaf strategy composite: its leaves, its strategy, and the
//! hybrid route's leg options the strategy maps onto.

use tonic::Status;

use super::refuse;
use super::selection::{no_query, Shape, Strategy};
use crate::pb::{
    search_query, selection_query, selection_score_strategy, CompositeSearchStrategy, DenseQuery,
    FusionMode, HybridLegOptions, LexicalQuery, SelectionOperator,
};

/// The composite's scoring leaves as found: each with its request id.
#[derive(Default)]
struct Leaves<'a> {
    dense: Option<(&'a str, &'a DenseQuery)>,
    lexical: Option<(&'a str, &'a LexicalQuery)>,
}

/// The two-leaf strategy composite: exactly one dense and one lexical
/// scoring leaf, an operator the strategy can certify, and a strategy
/// that maps onto a fusion mode.
pub(super) fn composite_shape(composite: &CompositeSearchStrategy) -> Result<Shape<'_>, Status> {
    let leaves = composite_leaves(composite)?;
    let Some((dense_id, dense)) = leaves.dense else {
        return Err(refuse(
            "the composite needs a dense leaf; a lexical-only query is the \
             single-lexical-leaf selection",
        ));
    };
    let Some((lexical_id, lexical)) = leaves.lexical else {
        return Err(refuse(
            "the composite needs a lexical leaf; a dense-only query is the \
             single-dense-leaf selection",
        ));
    };
    let strategy = composite_strategy(composite, dense_id)?;
    check_operator(&strategy, composite.operator())?;
    Ok(Shape::Composite {
        dense_id,
        dense,
        lexical_id,
        lexical,
        strategy,
    })
}

fn composite_leaves(composite: &CompositeSearchStrategy) -> Result<Leaves<'_>, Status> {
    let mut leaves = Leaves::default();
    for clause in &composite.clauses {
        match clause.node.as_ref() {
            Some(selection_query::Node::Search(leaf)) => match leaf.query.as_ref() {
                Some(search_query::Query::Dense(q)) => {
                    if leaves.dense.replace((&leaf.id, q)).is_some() {
                        return Err(refuse(
                            "two dense leaves in one composite have no engine path yet; \
                             increment 1 fuses exactly one dense and one lexical leaf",
                        ));
                    }
                }
                Some(search_query::Query::Lexical(q)) => {
                    check_lexical_leg(q)?;
                    if leaves.lexical.replace((&leaf.id, q)).is_some() {
                        return Err(refuse(
                            "two lexical leaves in one composite have no engine path yet; \
                             increment 1 fuses exactly one dense and one lexical leaf",
                        ));
                    }
                }
                None => return Err(no_query(leaf)),
            },
            Some(selection_query::Node::Filter(f)) => {
                return Err(refuse(format!(
                    "filter {:?} sits inside the scoring composite; a filter belongs to an \
                     AND wrapper AROUND the composite (under OR it would admit documents \
                     with no relevance signal, which needs the match-all order that does \
                     not exist yet)",
                    f.id
                )));
            }
            Some(selection_query::Node::Composite(_)) => {
                return Err(refuse(
                    "nested scoring composites are not served yet; increment 1 supports \
                     one level: filters AND (one leaf | OR(dense, lexical))",
                ));
            }
            Some(selection_query::Node::Boolean(_)) => {
                return Err(refuse(
                    "BooleanQuery is not nested inside legacy CompositeSearchStrategy; use it as the root selection shape",
                ));
            }
            None => return Err(refuse("empty selection node")),
        }
    }
    Ok(leaves)
}

/// What the hybrid legs cannot carry for the composite's lexical leaf.
fn check_lexical_leg(q: &LexicalQuery) -> Result<(), Status> {
    if q.phrase.is_some() || !q.prefixes.is_empty() {
        return Err(refuse(
            "a phrase or prefix constraint on a composite's lexical leaf is not served: \
             the hybrid legs carry no phrase gate yet. Use a \
             single-lexical-leaf selection for a phrase",
        ));
    }
    if !q.score_stages.is_empty() {
        return Err(refuse(
            "score_stages on a composite's lexical leaf are not served: \
             stages do not ride the hybrid legs today. Use a \
             single-lexical-leaf selection for a staged query",
        ));
    }
    Ok(())
}

fn composite_strategy<'a>(
    composite: &'a CompositeSearchStrategy,
    dense_id: &str,
) -> Result<Strategy<'a>, Status> {
    match composite.scoring.as_ref().and_then(|s| s.strategy.as_ref()) {
        Some(selection_score_strategy::Strategy::Rrf(s)) => Ok(Strategy::Rrf(s)),
        Some(selection_score_strategy::Strategy::ScoreBlend(s)) => Ok(Strategy::Blend(s)),
        Some(selection_score_strategy::Strategy::Decomposed(s)) => Ok(Strategy::Decomposed(s)),
        Some(selection_score_strategy::Strategy::Cascade(s)) => {
            if s.gate_id != dense_id {
                return Err(refuse(format!(
                    "cascade gate {:?} is not the dense leaf {dense_id:?}: the engine's \
                     cascade is vector-gate, BM25-rerank; a lexical gate has no engine \
                     path yet",
                    s.gate_id
                )));
            }
            Ok(Strategy::Cascade)
        }
        Some(selection_score_strategy::Strategy::Single(_)) => Err(refuse(
            "SingleScore names exactly one scoring leaf; this composite holds two. \
             Pick rrf, score_blend, decomposed, or cascade",
        )),
        None => Err(refuse(
            "a composite with two scoring leaves needs an explicit strategy (rrf, \
             score_blend, decomposed, or cascade); defaulting one silently would \
             misreport what ran",
        )),
    }
}

/// The operator and the strategy must agree on membership.
fn check_operator(strategy: &Strategy<'_>, operator: SelectionOperator) -> Result<(), Status> {
    match (strategy, operator) {
        (Strategy::Cascade, SelectionOperator::Unspecified) => Ok(()),
        (Strategy::Cascade, _) => Err(refuse(
            "cascade defines membership itself (the gate's admissions), so the \
             boolean operator must be left unspecified; naming AND or OR would \
             contradict the strategy",
        )),
        (_, SelectionOperator::Or) => Ok(()),
        (_, SelectionOperator::And) => Err(refuse(
            "AND over the two scoring leaves has no certifiable engine path yet (the \
             fusion modes are union-shaped); use OR, or cascade for gate semantics",
        )),
        (_, SelectionOperator::Unspecified) => Err(refuse(
            "the composite needs an explicit operator: OR for the fusion strategies \
             (their membership is the union of the retained legs)",
        )),
    }
}

/// Map a strategy onto the hybrid route's leg options.
pub(super) fn leg_options(
    strategy: &Strategy<'_>,
    selection_k: u32,
) -> (FusionMode, HybridLegOptions) {
    let mut legs = HybridLegOptions {
        leg_k: selection_k,
        ..Default::default()
    };
    let mode = match strategy {
        Strategy::Rrf(s) => {
            legs.rrf_k = s.rrf_k;
            legs.vector_weight = s.dense_weight;
            legs.bm25_weight = s.lexical_weight;
            FusionMode::GlobalRank
        }
        Strategy::Blend(s) => {
            legs.normalization = s.normalization;
            legs.combination = s.combination;
            legs.vector_weight = s.dense_weight;
            legs.bm25_weight = s.lexical_weight;
            FusionMode::ScoreBlend
        }
        Strategy::Decomposed(s) => {
            legs.vector_weight = s.dense_weight;
            legs.bm25_weight = s.lexical_weight;
            FusionMode::Decomposed
        }
        Strategy::Cascade => FusionMode::Cascade,
    };
    legs.set_fusion_mode(mode);
    (mode, legs)
}
