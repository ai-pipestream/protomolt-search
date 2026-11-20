//! Resolution of comparisons: numbers and booleans against each other, and
//! a facet read against a string literal.

use super::ValuesError;

use super::eval::string_ordering;
use super::lookup::ColumnLookup;
use super::refuse;
use super::resolve::resolve;
use super::types::{cmp_op_of, CmpOp, ResolvedValue, ValueType};
use crate::pb;

pub(super) fn resolve_compare(
    cmp: &pb::ValueCompare,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    use pb::value_expr::Expr as V;

    let op = cmp_op_of(cmp.op)?;
    let ordering = !matches!(op, CmpOp::Eq | CmpOp::Ne);
    let left = cmp
        .left
        .as_ref()
        .ok_or_else(|| refuse("comparison node without a left operand"))?;
    let right = cmp
        .right
        .as_ref()
        .ok_or_else(|| refuse("comparison node without a right operand"))?;
    let lit_of = |e: &pb::ValueExpr| match e.expr.as_ref() {
        Some(V::StringLiteral(v)) => Some(v.clone()),
        _ => None,
    };
    match (lit_of(left), lit_of(right)) {
        (Some(a), Some(b)) => {
            if ordering {
                return Err(string_ordering(op));
            }
            let eq = a == b;
            Ok((
                ResolvedValue::ConstBool(if op == CmpOp::Ne { !eq } else { eq }),
                ValueType::Bool,
            ))
        }

        (Some(lit), None) | (None, Some(lit)) => {
            if ordering {
                return Err(string_ordering(op));
            }
            let read_pb = if lit_of(left).is_some() { right } else { left };
            resolve_literal_compare(op, lit, read_pb, cols)
        }
        (None, None) => resolve_operand_compare(op, left, right, cols),
    }
}

/// A string literal against a direct facet or map-facet read, with the
/// literal pre-resolved to the read column's dictionary.
fn resolve_literal_compare(
    op: CmpOp,
    lit: String,
    read_pb: &pb::ValueExpr,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let (rv, vt) = resolve(read_pb, cols)?;
    let lit_ord = match &rv {
        ResolvedValue::FacetCol(fi) => cols.facet_value_ord_of(*fi, &lit),
        ResolvedValue::MapFacet { column, .. } => cols.map_facet_value_ord_of(*column, &lit),
        ResolvedValue::Absent => None,
        _ => {
            return Err(refuse(format!(
                "a string literal compares only against a direct facet \
                 or map-facet read, not a {} expression",
                vt.name()
            )));
        }
    };
    Ok((
        ResolvedValue::CompareStr {
            ne: op == CmpOp::Ne,
            read: Box::new(rv),
            lit_ord,
        },
        ValueType::Bool,
    ))
}

/// Two computed operands: numbers of one type, or booleans under `==` and
/// `!=`.
fn resolve_operand_compare(
    op: CmpOp,
    left: &pb::ValueExpr,
    right: &pb::ValueExpr,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let ordering = !matches!(op, CmpOp::Eq | CmpOp::Ne);

    let (lrv, lt) = resolve(left, cols)?;
    let (rrv, rt) = resolve(right, cols)?;
    if lt == ValueType::Unknown || rt == ValueType::Unknown {
        // One side can never hold a value, so the
        // comparison is absent for every document.
        return Ok((ResolvedValue::Absent, ValueType::Unknown));
    }
    if lt == ValueType::Str || rt == ValueType::Str {
        if lt == rt {
            return Err(refuse(
                "comparing two string columns; compare each against \
                 a string literal",
            ));
        }
        return Err(refuse(
            "a string column compares only against a string literal",
        ));
    }
    if ordering && (lt == ValueType::Bool || rt == ValueType::Bool) {
        return Err(refuse(
            "booleans do not order; only `==` and `!=` compare them",
        ));
    }
    if lt != rt {
        return Err(refuse(
            "a comparison mixes numeric types; stock CEL does \
             not coerce — convert explicitly with double()",
        ));
    }
    Ok((
        ResolvedValue::Compare {
            op,
            left: Box::new(lrv),
            right: Box::new(rrv),
        },
        ValueType::Bool,
    ))
}
