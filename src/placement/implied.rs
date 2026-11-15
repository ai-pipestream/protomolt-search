//! Filter clauses a leaf's bounds imply, and the filter with them removed.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use super::bounds::{cmp_bound, ColumnBounds};
use crate::filter::{edge_of, Edge};
use crate::pb;

/// Leaf indices (walk order, the order `impossible_under` and the
/// known handshake use) of the request's filter leaves that hold for
/// every row a leaf with `bounds` serves, counting only leaves reachable
/// from the root through `AND` nodes. A clause under `OR` or `NOT` is
/// never listed: removing it there would change the tree's value.
///
/// Soundness: a row in the leaf made every predicate on the leaf's path
/// true, so its columns lie inside `bounds`. A number clause whose
/// interval contains the leaf's interval, a facet clause whose value set
/// contains the leaf's admitted set, and a presence test on a column the
/// leaf constrains are therefore true for every row the shard holds, and
/// an `AND` child that is always true can be dropped from that shard's
/// tree without changing which rows pass.
pub fn implied_under(filter: &pb::FilterExpr, bounds: &ColumnBounds) -> Vec<usize> {
    let mut next = 0usize;
    let mut out = Vec::new();
    implied_walk(filter, bounds, &mut next, true, &mut out);
    out
}

fn implied_walk(
    expr: &pb::FilterExpr,
    bounds: &ColumnBounds,
    next: &mut usize,
    spine: bool,
    out: &mut Vec<usize>,
) {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) => {
            for child in &list.exprs {
                implied_walk(child, bounds, next, spine, out);
            }
        }
        Some(Expr::Or(list)) => {
            for child in &list.exprs {
                implied_walk(child, bounds, next, false, out);
            }
        }
        Some(Expr::Not(child)) => implied_walk(child, bounds, next, false, out),
        Some(Expr::Number(p)) => {
            let index = *next;
            *next += 1;
            if spine && number_implied(p, bounds) {
                out.push(index);
            }
        }
        Some(Expr::Facet(p)) => {
            let index = *next;
            *next += 1;
            if spine && facet_implied(p, bounds) {
                out.push(index);
            }
        }
        Some(Expr::Has(p)) => {
            let index = *next;
            *next += 1;
            if spine && (bounds.number(&p.column).is_some() || bounds.facet(&p.column).is_some()) {
                out.push(index);
            }
        }
        Some(Expr::StringRange(_))
        | Some(Expr::StringPrefix(_))
        | Some(Expr::MapStringRange(_))
        | Some(Expr::MapStringPrefix(_))
        | Some(Expr::MapFacet(_))
        | Some(Expr::MapNumber(_))
        | Some(Expr::TypedMapNumber(_))
        | Some(Expr::MapHasKey(_))
        | Some(Expr::TypedMapHasKey(_))
        | Some(Expr::Geo(_)) => {
            *next += 1;
        }
        None => {}
    }
}

/// Whether every value inside the leaf's interval on the clause's
/// column satisfies the clause. A clause bound the leaf does not bound
/// on that side is not implied; a clause with no bound at all is left
/// as it is.
fn number_implied(p: &pb::NumberPredicate, bounds: &ColumnBounds) -> bool {
    let Some((blo, bhi)) = bounds.number(&p.column) else {
        return false;
    };
    let cmin = p.min.as_ref().and_then(edge_of);
    let cmax = p.max.as_ref().and_then(edge_of);
    if cmin.is_none() && cmax.is_none() {
        return false;
    }
    let lower = match (cmin.as_ref(), blo.as_ref()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(c), Some(l)) => lower_dominates(l, c),
    };
    let upper = match (cmax.as_ref(), bhi.as_ref()) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(c), Some(h)) => upper_dominates(h, c),
    };
    lower && upper
}

/// Every x with `x >= l` (or `> l`) also has `x >= c` (or `> c`).
fn lower_dominates(l: &Edge, c: &Edge) -> bool {
    match cmp_bound(&l.value, &c.value) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => !(c.exclusive && !l.exclusive),
    }
}

/// Every x with `x <= h` (or `< h`) also has `x <= c` (or `< c`).
fn upper_dominates(h: &Edge, c: &Edge) -> bool {
    match cmp_bound(&h.value, &c.value) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => !(c.exclusive && !h.exclusive),
    }
}

/// Every value the leaf admits on the column is one the clause lists.
fn facet_implied(p: &pb::FacetPredicate, bounds: &ColumnBounds) -> bool {
    match bounds.facet(&p.column) {
        Some(admitted) => {
            !admitted.is_empty() && admitted.iter().all(|v| p.values.iter().any(|w| w == v))
        }
        None => false,
    }
}

/// `filter` with the leaves at `dropped` (walk-order indices, as
/// [`implied_under`] lists them) removed. An `AND` left with one child
/// becomes that child; an `AND` left with none is removed, and a removed
/// root is `None`, no filter. Indices under `OR` or `NOT` are never
/// listed by `implied_under`, and this walk leaves those subtrees intact.
pub fn without_leaves(filter: &pb::FilterExpr, dropped: &[usize]) -> Option<pb::FilterExpr> {
    if dropped.is_empty() {
        return Some(filter.clone());
    }
    let dropped: BTreeSet<usize> = dropped.iter().copied().collect();
    let mut next = 0usize;
    strip_walk(filter, &dropped, &mut next)
}

fn strip_walk(
    expr: &pb::FilterExpr,
    dropped: &BTreeSet<usize>,
    next: &mut usize,
) -> Option<pb::FilterExpr> {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) => {
            let mut kept = Vec::with_capacity(list.exprs.len());
            for child in &list.exprs {
                if let Some(child) = strip_walk(child, dropped, next) {
                    kept.push(child);
                }
            }
            match kept.len() {
                0 => None,
                1 => kept.pop(),
                _ => Some(pb::FilterExpr {
                    expr: Some(Expr::And(pb::FilterList { exprs: kept })),
                }),
            }
        }
        Some(Expr::Or(_)) | Some(Expr::Not(_)) => {
            count_leaves(expr, next);
            Some(expr.clone())
        }
        Some(_) => {
            let index = *next;
            *next += 1;
            (!dropped.contains(&index)).then(|| expr.clone())
        }
        None => Some(expr.clone()),
    }
}

/// Advance the walk-order index past every leaf under `expr`.
fn count_leaves(expr: &pb::FilterExpr, next: &mut usize) {
    use pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) | Some(Expr::Or(list)) => {
            for child in &list.exprs {
                count_leaves(child, next);
            }
        }
        Some(Expr::Not(child)) => count_leaves(child, next),
        Some(_) => *next += 1,
        None => {}
    }
}
