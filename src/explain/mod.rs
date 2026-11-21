//! The per-document explain tree.
//!
//! An [`Explanation`] is a node with a value, a description of what
//! that value is, and the nodes it was computed from. The rule for
//! every node this module builds: the description states the function
//! of the children, and the root's value is the hit's served score
//! (its f32) so a reader can check the tree against the response
//! without a calculator. The tree is assembled from numbers the
//! engine already computed on the path that produced the hit; it
//! never re-scores anything, so a request with `explain` on returns
//! the same hits in the same order, bitwise, as one with it off.

mod composite;
mod finish;
mod lexical;
#[cfg(test)]
mod tests;

use crate::pb::{Explanation, QueryHit};

pub use composite::{cascade, composite, Fusion, LegacyBoost};
pub use finish::{finish, WindowBoost};
pub use lexical::lexical;

/// A node with children.
pub fn node(value: f64, description: impl Into<String>, details: Vec<Explanation>) -> Explanation {
    Explanation {
        value,
        description: description.into(),
        details,
    }
}

/// A node without children.
pub fn leaf(value: f64, description: impl Into<String>) -> Explanation {
    node(value, description, Vec::new())
}

/// The dense leaf's tree. `estimate` is the candidate score the
/// provider produced before an FP32 score from the original row replaced it.
pub fn dense(id: &str, score: f32, estimate: Option<f32>) -> Explanation {
    match estimate {
        Some(estimate) => node(
            f64::from(score),
            format!(
                "dense leaf {id:?}: exact FP32 dot product of the query with the stored \
                 vector (docs/query-api.md \"Dense execution contract\")"
            ),
            vec![leaf(
                f64::from(estimate),
                "the provider's quantized score that selected this document for FP32 \
                 scoring (not part of the served score)",
            )],
        ),
        None => leaf(
            f64::from(score),
            format!(
                "dense leaf {id:?}: the provider's native similarity score for the stored \
                 vector (docs/query-api.md \"Dense execution contract\")"
            ),
        ),
    }
}

/// A boolean root's tree: the sum of its positive scoring clauses'
/// relevance, each clause a leaf under its id.
pub fn boolean(hit: &QueryHit) -> Explanation {
    let details = hit
        .signals
        .iter()
        .map(|signal| {
            leaf(
                f64::from(signal.score),
                format!(
                    "clause {:?}: the leaf's relevance for this document",
                    signal.id
                ),
            )
        })
        .collect();
    node(
        f64::from(hit.score),
        "boolean root: sum of the positive scoring clauses (docs/query-api.md \"Recursive \
         boolean execution\")",
        details,
    )
}
