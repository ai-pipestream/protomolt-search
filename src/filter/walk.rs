//! The wire tree's leaves in the one depth-first order both sides of the
//! `filter_columns_known` handshake use.

use super::{Edge, NumBound};

/// One leaf of the WIRE tree, borrowed. [`walk_leaves`] hands these
/// out in depth-first order — the ONE definition of the order the
/// positional `filter_columns_known` handshake uses, shared by the
/// node (answering) and the coordinator (refusing), so the two sides
/// cannot disagree about which flag is whose.
#[derive(Debug, Clone, Copy)]
pub enum LeafRef<'a> {
    /// A facet predicate.
    Facet(&'a crate::pb::FacetPredicate),
    /// A number predicate.
    Number(&'a crate::pb::NumberPredicate),
    /// A map-facet predicate.
    MapFacet(&'a crate::pb::MapFacetPredicate),
    /// Numeric map comparison across f64, i64 and u64 map columns.
    MapNumber(&'a crate::pb::MapNumberPredicate),
    /// Key presence across all string and numeric map columns.
    MapHasKey(&'a crate::pb::MapKeyPredicate),
    /// A column presence test.
    Has(&'a crate::pb::HasPredicate),
    /// A geo leaf.
    Geo(&'a crate::pb::GeoFilter),
    /// A string range on a facet column or map-facet value.
    StringRange(&'a crate::pb::StringRangePredicate),
    /// A string prefix on a facet column or map-facet value.
    StringPrefix(&'a crate::pb::StringPrefixPredicate),
}

impl LeafRef<'_> {
    /// `kind column[key]` for refusal messages: which table the caller
    /// should be checking for the spelling.
    pub fn describe(&self) -> String {
        match self {
            LeafRef::Facet(p) => format!("facet column {:?}", p.column),
            LeafRef::Number(p) => format!("numeric column {:?}", p.column),
            LeafRef::MapFacet(p) => format!("map-facet column {:?} key {:?}", p.column, p.key),
            LeafRef::MapNumber(p) => {
                format!("numeric map column {:?} key {:?}", p.column, p.key)
            }
            LeafRef::MapHasKey(p) => format!("map column {:?}", p.column),
            LeafRef::Has(p) => format!("column {:?}", p.column),
            LeafRef::Geo(g) => format!("geo column {:?}", g.column),
            LeafRef::StringRange(p) => describe_string_target(&p.column, p.map_key.as_deref()),
            LeafRef::StringPrefix(p) => describe_string_target(&p.column, p.map_key.as_deref()),
        }
    }
}

fn describe_string_target(column: &str, key: Option<&str>) -> String {
    match key {
        None => format!("facet column {column:?}"),
        Some(key) => format!("map-facet column {column:?} key {key:?}"),
    }
}

/// Visit every leaf of `expr` in depth-first, field-order sequence
/// (and/or children left to right, then a not's child). Assumes a
/// validated tree; an unset oneof contributes nothing.
pub fn walk_leaves<'a>(expr: &'a crate::pb::FilterExpr, visit: &mut dyn FnMut(LeafRef<'a>)) {
    use crate::pb::filter_expr::Expr;
    match &expr.expr {
        Some(Expr::And(list)) | Some(Expr::Or(list)) => {
            for child in &list.exprs {
                walk_leaves(child, visit);
            }
        }
        Some(Expr::Not(child)) => walk_leaves(child, visit),
        Some(Expr::Facet(p)) => visit(LeafRef::Facet(p)),
        Some(Expr::Number(p)) => visit(LeafRef::Number(p)),
        Some(Expr::MapFacet(p)) => visit(LeafRef::MapFacet(p)),
        Some(Expr::MapNumber(p)) => visit(LeafRef::MapNumber(p)),
        Some(Expr::MapHasKey(p)) => visit(LeafRef::MapHasKey(p)),
        Some(Expr::Has(p)) => visit(LeafRef::Has(p)),
        Some(Expr::Geo(g)) => visit(LeafRef::Geo(g)),
        Some(Expr::StringRange(p)) => visit(LeafRef::StringRange(p)),
        Some(Expr::StringPrefix(p)) => visit(LeafRef::StringPrefix(p)),
        None => {}
    }
}

/// Number of leaves a validated tree holds — the length both sides
/// expect of `filter_columns_known`.
pub fn leaf_count(expr: &crate::pb::FilterExpr) -> usize {
    let mut n = 0;
    walk_leaves(expr, &mut |_| n += 1);
    n
}

/// Resolve one wire bound into an [`Edge`], normalizing `-0.0` so
/// [`f64::total_cmp`] cannot disagree with IEEE equality. Assumes a
/// validated tree (the oneof is set and floats are finite).
pub fn edge_of(b: &crate::pb::FilterBound) -> Option<Edge> {
    use crate::pb::filter_bound::Value;
    let value = match b.value.as_ref()? {
        Value::Int(n) => NumBound::I(*n),
        Value::Uint(n) => NumBound::U(*n),
        Value::Num(x) => NumBound::F(if *x == 0.0 { 0.0 } else { *x }),
    };
    Some(Edge {
        value,
        exclusive: b.exclusive,
    })
}
