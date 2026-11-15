//! Request ids: every search leaf, filter and boost is named, and the
//! names are what score provenance and the composite scorer refer to.

use std::collections::HashSet;

use tonic::Status;

use super::refuse;
use super::selection::{Plan, Shape};
use crate::ltr::SignalKind;

/// Every request id and what it names, for scorer source validation.
pub(super) fn signal_ids<'a>(
    plan: &'a Plan<'a>,
    boosts: &'a [crate::pb::BoostQuery],
) -> Vec<(&'a str, SignalKind)> {
    let mut ids: Vec<(&str, SignalKind)> = Vec::new();
    match &plan.shape {
        Shape::Browse => {}
        Shape::Lexical { id, .. } | Shape::Dense { id, .. } => ids.push((id, SignalKind::Search)),
        Shape::Composite {
            dense_id,
            lexical_id,
            ..
        } => {
            ids.push((dense_id, SignalKind::Search));
            ids.push((lexical_id, SignalKind::Search));
        }
    }
    for f in &plan.filter_ids {
        ids.push((f, SignalKind::Filter));
    }
    for b in boosts {
        if let Some(q) = &b.query {
            ids.push((&q.id, SignalKind::Boost));
        }
    }
    ids
}

/// Refuse the first id that is empty (with `empty`, the caller's wording
/// of the rule) or that repeats an earlier one.
pub(super) fn check_unique<'a>(
    ids: impl IntoIterator<Item = &'a str>,
    empty: &'static str,
) -> Result<(), Status> {
    let mut seen = HashSet::new();
    for id in ids {
        if id.is_empty() {
            return Err(refuse(empty));
        }
        if !seen.insert(id) {
            return Err(refuse(format!("duplicate query id {id:?}")));
        }
    }
    Ok(())
}

/// Every id in the request — search leaves, filters, boosts — must be
/// non-empty and request-unique, which is what makes score provenance
/// unambiguous.
pub(super) fn check_ids(plan: &Plan<'_>, boosts: &[crate::pb::BoostQuery]) -> Result<(), Status> {
    check_unique(
        signal_ids(plan, boosts).into_iter().map(|(id, _)| id),
        "every search, filter, and boost query needs a non-empty id; ids are what \
         make score provenance unambiguous",
    )
}
