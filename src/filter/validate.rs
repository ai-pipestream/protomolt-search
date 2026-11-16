//! Validation of the wire filter tree: shape, caps, and each leaf's own
//! contract, every refusal by name.

use super::FilterError;

use super::{MAX_DEPTH, MAX_LEAVES};

/// Validate a wire filter tree: shape (every oneof set, connectives
/// non-empty, depth and leaf caps) and every leaf's own contract, each
/// refusal by name. Runs on the coordinator BEFORE fan-out — a
/// malformed filter refuses even when the query has no match set — and
/// again on every node, which trusts no caller.
pub fn validate_filter(expr: &crate::pb::FilterExpr) -> Result<(), FilterError> {
    let mut leaves = 0usize;
    validate_node(expr, 1, &mut leaves)?;
    if leaves > MAX_LEAVES {
        return Err(FilterError::invalid_argument(format!(
            "filter has {leaves} leaves; the limit is {MAX_LEAVES} — a filter is a \
             predicate, not a program"
        )));
    }
    Ok(())
}

fn validate_node(
    expr: &crate::pb::FilterExpr,
    depth: usize,
    leaves: &mut usize,
) -> Result<(), FilterError> {
    use crate::pb::filter_expr::Expr;
    if depth > MAX_DEPTH {
        return Err(FilterError::invalid_argument(format!(
            "filter tree is deeper than {MAX_DEPTH} levels"
        )));
    }
    match &expr.expr {
        None => Err(FilterError::invalid_argument(
            "filter: a node with no expression set; an empty filter is the field left \
             absent, never an empty node",
        )),
        Some(Expr::And(list)) | Some(Expr::Or(list)) => {
            if list.exprs.is_empty() {
                return Err(FilterError::invalid_argument(
                    "filter: and/or with no children; a connective with nothing to \
                     connect is refused rather than resolved to a vacuous truth value",
                ));
            }
            for child in &list.exprs {
                validate_node(child, depth + 1, leaves)?;
            }
            Ok(())
        }
        Some(Expr::Not(child)) => validate_node(child, depth + 1, leaves),
        Some(leaf) => {
            *leaves += 1;
            validate_leaf(leaf)
        }
    }
}

/// A predicate names the column it reads.
fn named(column: &str, kind: &str) -> Result<(), FilterError> {
    if column.is_empty() {
        return Err(FilterError::invalid_argument(format!(
            "filter {kind} predicate: a predicate names the column it reads"
        )));
    }
    Ok(())
}

/// One leaf's own contract, refused by name.
fn validate_leaf(leaf: &crate::pb::filter_expr::Expr) -> Result<(), FilterError> {
    use crate::pb::filter_expr::Expr;
    match leaf {
        Expr::And(_) | Expr::Or(_) | Expr::Not(_) => Ok(()),
        Expr::Facet(p) => {
            named(&p.column, "facet")?;
            if p.values.is_empty() {
                return Err(FilterError::invalid_argument(format!(
                    "filter facet predicate on {:?}: at least one value; membership in \
                     the empty set is a predicate nobody meant to send",
                    p.column
                )));
            }
            Ok(())
        }
        Expr::Number(p) => validate_number(p),
        Expr::MapFacet(p) => {
            named(&p.column, "map-facet")?;
            if p.values.is_empty() {
                return Err(FilterError::invalid_argument(format!(
                    "filter map-facet predicate on {:?}[{:?}]: at least one value",
                    p.column, p.key
                )));
            }
            Ok(())
        }
        Expr::MapNumber(p) | Expr::TypedMapNumber(p) => validate_map_number(p),
        Expr::MapHasKey(p) | Expr::TypedMapHasKey(p) => named(&p.column, "map-key"),
        Expr::Has(p) => named(&p.column, "has"),
        Expr::Geo(g) => crate::node::validate_geo_filter(g)
            .map(|_| ())
            .map_err(FilterError::foreign),
        Expr::StringRange(p) | Expr::MapStringRange(p) => validate_string_range(p),
        Expr::StringPrefix(p) | Expr::MapStringPrefix(p) => validate_string_prefix(p),
    }
}

fn validate_number(p: &crate::pb::NumberPredicate) -> Result<(), FilterError> {
    named(&p.column, "number")?;
    validate_bound(&p.column, p.min.as_ref())?;
    validate_bound(&p.column, p.max.as_ref())?;
    if p.min.is_none() && p.max.is_none() {
        return Err(FilterError::invalid_argument(format!(
            "filter number predicate on {:?}: at least one bound; bare presence \
             is has({})",
            p.column, p.column
        )));
    }
    Ok(())
}

fn validate_map_number(p: &crate::pb::MapNumberPredicate) -> Result<(), FilterError> {
    named(&p.column, "map-number")?;
    validate_bound(&p.column, p.min.as_ref())?;
    validate_bound(&p.column, p.max.as_ref())?;
    if p.min.is_none() && p.max.is_none() {
        return Err(FilterError::invalid_argument(format!(
            "filter map-number predicate on {:?}[{:?}]: at least one bound; key \
             presence is `{:?} in {}`",
            p.column, p.key, p.key, p.column
        )));
    }
    Ok(())
}

fn validate_string_range(p: &crate::pb::StringRangePredicate) -> Result<(), FilterError> {
    named(&p.column, "string-range")?;
    if p.min.is_none() && p.max.is_none() {
        return Err(FilterError::invalid_argument(format!(
            "filter string-range predicate on {:?}: at least one bound; bare \
             presence is has({})",
            p.column, p.column
        )));
    }
    Ok(())
}

fn validate_string_prefix(p: &crate::pb::StringPrefixPredicate) -> Result<(), FilterError> {
    named(&p.column, "string-prefix")?;
    if p.prefix.is_empty() {
        return Err(FilterError::invalid_argument(format!(
            "filter string-prefix predicate on {:?}: an empty prefix matches every \
             value, which is has({})",
            p.column, p.column
        )));
    }
    Ok(())
}

/// One bound's own contract: the oneof is set and a float bound is
/// finite. (An integer bound has nothing to check — every i64 is a
/// legal bound.)
fn validate_bound(column: &str, b: Option<&crate::pb::FilterBound>) -> Result<(), FilterError> {
    use crate::pb::filter_bound::Value;
    match b {
        None => Ok(()),
        Some(bound) => match bound.value.as_ref() {
            None => Err(FilterError::invalid_argument(format!(
                "filter bound on {column:?}: no value set; a bound says the number it \
                 bounds by"
            ))),
            Some(Value::Num(x)) if !x.is_finite() => Err(FilterError::invalid_argument(format!(
                "filter bound on {column:?}: {x} is not finite; NaN and infinity bound \
                 nothing a column can hold"
            ))),
            Some(_) => Ok(()),
        },
    }
}
