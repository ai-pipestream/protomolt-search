//! The bounds a leaf's predicates put on its rows.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use crate::filter::{cmp_f64_i64, edge_of, Edge, NumBound};
use crate::pb;

/// Exact order of two bounds across the integer and float domains.
pub(super) fn cmp_bound(a: &NumBound, b: &NumBound) -> Ordering {
    match (a, b) {
        (NumBound::I(x), NumBound::I(y)) => x.cmp(y),
        (NumBound::U(x), NumBound::U(y)) => x.cmp(y),
        (NumBound::U(x), NumBound::I(y)) => i128::from(*x).cmp(&i128::from(*y)),
        (NumBound::I(x), NumBound::U(y)) => i128::from(*x).cmp(&i128::from(*y)),
        (NumBound::F(x), NumBound::U(y)) => crate::filter::cmp_f64_u64(*x, *y),
        (NumBound::U(x), NumBound::F(y)) => crate::filter::cmp_f64_u64(*y, *x).reverse(),
        (NumBound::F(x), NumBound::F(y)) => x.total_cmp(y),
        (NumBound::F(x), NumBound::I(y)) => cmp_f64_i64(*x, *y),
        (NumBound::I(x), NumBound::F(y)) => cmp_f64_i64(*y, *x).reverse(),
    }
}

/// Whether the interval ending at `hi` lies entirely below the one
/// starting at `lo`: the two share no real number.
pub(super) fn separated(hi: &Edge, lo: &Edge) -> bool {
    match cmp_bound(&hi.value, &lo.value) {
        Ordering::Less => true,
        Ordering::Equal => hi.exclusive || lo.exclusive,
        Ordering::Greater => false,
    }
}

/// The tighter of two lower edges.
fn tighter_lower(a: Edge, b: Edge) -> Edge {
    match cmp_bound(&a.value, &b.value) {
        Ordering::Greater => a,
        Ordering::Less => b,
        Ordering::Equal => {
            if a.exclusive {
                a
            } else {
                b
            }
        }
    }
}

/// The tighter of two upper edges.
fn tighter_upper(a: Edge, b: Edge) -> Edge {
    match cmp_bound(&a.value, &b.value) {
        Ordering::Less => a,
        Ordering::Greater => b,
        Ordering::Equal => {
            if a.exclusive {
                a
            } else {
                b
            }
        }
    }
}

fn merge_lower(current: &mut Option<Edge>, edge: Option<Edge>) {
    if let Some(edge) = edge {
        *current = Some(match current.take() {
            Some(have) => tighter_lower(have, edge),
            None => edge,
        });
    }
}

fn merge_upper(current: &mut Option<Edge>, edge: Option<Edge>) {
    if let Some(edge) = edge {
        *current = Some(match current.take() {
            Some(have) => tighter_upper(have, edge),
            None => edge,
        });
    }
}

/// What a conjunction of filter-dialect predicates guarantees about the
/// rows that satisfy it, per column: a numeric interval and a set of
/// admitted facet values. Only the sound part is kept: predicates under
/// `!` or `||`, map, geo, string, and presence predicates contribute
/// nothing, so the bounds describe a superset of the rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnBounds {
    numbers: HashMap<String, (Option<Edge>, Option<Edge>)>,
    facets: HashMap<String, BTreeSet<String>>,
}

impl ColumnBounds {
    /// The bounds of `exprs` taken together.
    pub fn of_conjunction(exprs: &[pb::FilterExpr]) -> Self {
        let mut bounds = ColumnBounds::default();
        for expr in exprs {
            bounds.absorb(expr);
        }
        bounds
    }

    fn absorb(&mut self, expr: &pb::FilterExpr) {
        use pb::filter_expr::Expr;
        match &expr.expr {
            Some(Expr::And(list)) => {
                for child in &list.exprs {
                    self.absorb(child);
                }
            }
            Some(Expr::Number(p)) => {
                let entry = self.numbers.entry(p.column.clone()).or_default();
                merge_lower(&mut entry.0, p.min.as_ref().and_then(edge_of));
                merge_upper(&mut entry.1, p.max.as_ref().and_then(edge_of));
            }
            Some(Expr::Facet(p)) => {
                let values: BTreeSet<String> = p.values.iter().cloned().collect();
                match self.facets.get_mut(&p.column) {
                    Some(have) => have.retain(|v| values.contains(v)),
                    None => {
                        self.facets.insert(p.column.clone(), values);
                    }
                }
            }
            _ => {}
        }
    }

    /// Pin an integer column to one value, the placement column to the
    /// leaf's code.
    pub fn pin_int(&mut self, column: &str, value: i64) {
        let edge = Edge {
            value: NumBound::I(value),
            exclusive: false,
        };
        let entry = self.numbers.entry(column.to_string()).or_default();
        merge_lower(&mut entry.0, Some(edge));
        merge_upper(&mut entry.1, Some(edge));
    }

    /// The numeric interval the bounds put on `column`, if any.
    pub fn number(&self, column: &str) -> Option<(Option<Edge>, Option<Edge>)> {
        self.numbers.get(column).copied()
    }

    /// The facet values the bounds admit on `column`, if any.
    pub fn facet(&self, column: &str) -> Option<&BTreeSet<String>> {
        self.facets.get(column)
    }
}
