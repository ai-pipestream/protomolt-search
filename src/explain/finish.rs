//! The finish pass over each hit's tree after the boost and scorer
//! phases.

use tonic::Status;

use super::{leaf, node};
use crate::pb::{DimensionScore, Explanation, QueryHit};

/// A request-level boost that reorders its window without changing
/// the served score (`BoostQuery` without a composite scorer).
#[derive(Debug, Clone)]
pub struct WindowBoost {
    pub id: String,
    pub base_weight: f64,
    pub boost_weight: f64,
}

/// Finish each hit's tree after the boost and scorer phases. With a
/// composite scorer the root becomes the scorer's operation over its
/// dimension nodes, the selection tree kept under it as the provenance
/// of the selection signal; with window boosts and no scorer the root
/// keeps the selection's value and gains one leaf per boost signal
/// that says how the window was ordered.
pub fn finish(
    hits: &mut [QueryHit],
    boosts: &[WindowBoost],
    scorer: Option<&str>,
) -> Result<(), Status> {
    for hit in hits.iter_mut() {
        let selection = hit.explain.take().ok_or_else(|| {
            Status::internal(format!(
                "explain: document {} reached the finish pass without a selection tree",
                hit.doc_id
            ))
        })?;
        let tree = match scorer {
            Some(operation) => scored_root(hit, selection, operation),
            None => boosted_root(hit, selection, boosts),
        };
        hit.explain = Some(tree);
    }
    Ok(())
}

/// The composite scorer's operation over its dimension nodes, the
/// selection tree kept under it as provenance.
fn scored_root(hit: &QueryHit, selection: Explanation, operation: &str) -> Explanation {
    let mut details: Vec<Explanation> = hit.dimensions.iter().map(dimension_node).collect();
    details.push(node(
        selection.value,
        "the selection score this document entered the scorer with (provenance \
             of the selection signal, not a term of the operation)",
        vec![selection],
    ));
    node(
        f64::from(hit.score),
        format!(
            "composite scorer {operation} over the dimension nodes \
                 (docs/query-api.md \"Composite scorer\")"
        ),
        details,
    )
}

fn dimension_node(dim: &DimensionScore) -> Explanation {
    let mut inputs = Vec::new();
    if let Some(raw) = dim.raw {
        inputs.push(leaf(raw, "the dimension's raw signal"));
    }
    inputs.push(leaf(
        dim.normalized,
        "the raw signal after the dimension's normalization over the pool",
    ));
    let description = if dim.skipped {
        format!(
            "dimension {:?}: skipped (disabled, or missing under the SKIP \
             policy); contributes no term",
            dim.id
        )
    } else if dim.raw.is_none() {
        format!(
            "dimension {:?}: missing signal scored as 0 under the ZERO \
             policy, times weight",
            dim.id
        )
    } else {
        format!("dimension {:?}: normalized signal times weight", dim.id)
    };
    node(dim.contribution, description, inputs)
}

/// The selection tree, with one leaf per window boost signal that says
/// how the window was ordered.
fn boosted_root(hit: &QueryHit, selection: Explanation, boosts: &[WindowBoost]) -> Explanation {
    let mut details = vec![selection];
    for boost in boosts {
        if let Some(signal) = hit.signals.iter().find(|s| s.id == boost.id) {
            let key = boost.base_weight * f64::from(hit.score)
                + boost.boost_weight * f64::from(signal.score);
            details.push(leaf(
                f64::from(signal.score),
                format!(
                    "boost {:?}: the boost query's relevance; orders the window by \
                     base_weight {} * score + boost_weight {} * boost = {key}, and is \
                     not part of the served score",
                    boost.id, boost.base_weight, boost.boost_weight
                ),
            ));
        }
    }
    if details.len() == 1 {
        details.pop().expect("the selection tree")
    } else {
        node(
            f64::from(hit.score),
            "the selection score, unchanged by the boost; the boost leaf says how \
             the window was ordered",
            details,
        )
    }
}
