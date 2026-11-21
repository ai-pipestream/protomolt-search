//! The fan-out pruner: whether any row under a leaf's bounds can satisfy a
//! filter.

use super::bounds::{separated, ColumnBounds};
use super::doc_columns::string_in_range;
use crate::filter::edge_of;
use crate::pb;

/// `Some(leaves)` when NO row under `bounds` can satisfy `filter`, with
/// the indices (in [`crate::filter::walk_leaves`] order) of the filter
/// leaves that established it; `None` means the shard must be asked.
/// The rules are the segment pruner's: an AND is impossible when a
/// child is, an OR when every child is, a NOT never, and only number
/// and facet-shaped leaves say anything.
pub fn impossible_under(filter: &pb::FilterExpr, bounds: &ColumnBounds) -> Option<Vec<usize>> {
    let mut next = 0usize;
    impossible_walk(filter, bounds, &mut next)
}

fn impossible_walk(
    expr: &pb::FilterExpr,
    bounds: &ColumnBounds,
    next: &mut usize,
) -> Option<Vec<usize>> {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) => {
            let mut found = None;
            for child in &list.exprs {
                let verdict = impossible_walk(child, bounds, next);
                if found.is_none() {
                    found = verdict;
                }
            }
            found
        }
        Some(Expr::Or(list)) => {
            let mut all = Vec::new();
            let mut every = !list.exprs.is_empty();
            for child in &list.exprs {
                match impossible_walk(child, bounds, next) {
                    Some(leaves) => all.extend(leaves),
                    None => every = false,
                }
            }
            every.then_some(all)
        }
        Some(Expr::Not(child)) => {
            let _ = impossible_walk(child, bounds, next);
            None
        }

        Some(Expr::Number(p)) => {
            let index = *next;
            *next += 1;
            number_impossible(p, bounds, index)
        }

        Some(Expr::Facet(p)) => {
            let index = *next;
            *next += 1;
            let admitted = bounds.facet(&p.column)?;
            (!p.values.iter().any(|v| admitted.contains(v))).then_some(vec![index])
        }

        Some(Expr::StringRange(p)) => {
            let index = *next;
            *next += 1;
            none_admitted(
                bounds,
                p.map_key.is_none().then_some(&p.column),
                index,
                |v| string_in_range(v, p.min.as_ref(), p.max.as_ref()),
            )
        }
        Some(Expr::StringPrefix(p)) => {
            let index = *next;
            *next += 1;
            none_admitted(
                bounds,
                p.map_key.is_none().then_some(&p.column),
                index,
                |v| v.as_bytes().starts_with(p.prefix.as_bytes()),
            )
        }

        Some(Expr::MapFacet(_))
        | Some(Expr::MapNumber(_))
        | Some(Expr::MapHasKey(_))
        | Some(Expr::Has(_))
        | Some(Expr::Geo(_)) => {
            *next += 1;
            None
        }
        None => None,
    }
}

/// A number clause is impossible when its own interval is empty, or when it
/// shares no value with the interval the bounds put on its column.
fn number_impossible(
    p: &pb::NumberPredicate,
    bounds: &ColumnBounds,
    index: usize,
) -> Option<Vec<usize>> {
    let min = p.min.as_ref().and_then(edge_of);
    let max = p.max.as_ref().and_then(edge_of);
    if let (Some(lo), Some(hi)) = (min.as_ref(), max.as_ref()) {
        if separated(hi, lo) {
            return Some(vec![index]);
        }
    }
    let (blo, bhi) = bounds.number(&p.column)?;
    let below = match (max.as_ref(), blo.as_ref()) {
        (Some(fmax), Some(blo)) => separated(fmax, blo),
        _ => false,
    };
    let above = match (bhi.as_ref(), min.as_ref()) {
        (Some(bhi), Some(fmin)) => separated(bhi, fmin),
        _ => false,
    };
    (below || above).then_some(vec![index])
}

/// A string clause on a facet column (`None` for a map entry, which the
/// bounds say nothing about) is impossible when no admitted value of the
/// column satisfies it.
fn none_admitted(
    bounds: &ColumnBounds,
    column: Option<&String>,
    index: usize,
    satisfies: impl Fn(&str) -> bool,
) -> Option<Vec<usize>> {
    let admitted = bounds.facet(column?)?;
    (!admitted.iter().any(|v| satisfies(v))).then_some(vec![index])
}
