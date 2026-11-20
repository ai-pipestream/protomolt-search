//! Ingest-time comparisons, Kleene logic, and the conditional.

use super::ValuesError;

use super::eval::{cmp_double, cmp_int, string_ordering};
use super::ingest::eval_ingest_inner;
use super::ingest_env::{IngestEnv, IngestVal};
use super::refuse;
use super::types::{cmp_op_of, CmpOp};
use crate::pb;

pub(super) fn ingest_compare(
    cmp: &pb::ValueCompare,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
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
    let is_lit = |e: &pb::ValueExpr| matches!(e.expr.as_ref(), Some(V::StringLiteral(_)));
    if is_lit(left) || is_lit(right) {
        if ordering {
            return Err(string_ordering(op));
        }
        if let (Some(V::StringLiteral(a)), Some(V::StringLiteral(b))) =
            (left.expr.as_ref(), right.expr.as_ref())
        {
            let eq = a == b;
            return Ok(Some(IngestVal::Bool(if op == CmpOp::Ne {
                !eq
            } else {
                eq
            })));
        }
        let (other, lit) = if is_lit(left) {
            (right, left)
        } else {
            (left, right)
        };
        let Some(V::StringLiteral(lit)) = lit.expr.as_ref() else {
            unreachable!("is_lit matched a string literal")
        };
        // The read side is a facet of this document (resolution
        // admits only a direct facet or map-facet read here); a
        // map facet is not an ingest input, so it is absent.
        return Ok(match other.expr.as_ref() {
            Some(V::Column(name)) => env.facets.get(name).map(|value| {
                let eq = value == lit;
                IngestVal::Bool(if op == CmpOp::Ne { !eq } else { eq })
            }),
            _ => None,
        });
    }
    let (l, r) = (
        eval_ingest_inner(left, env, strict)?,
        eval_ingest_inner(right, env, strict)?,
    );
    match (l, r) {
        (None, _) | (_, None) => Ok(None),
        (Some(IngestVal::Int(a)), Some(IngestVal::Int(b))) => {
            Ok(Some(IngestVal::Bool(cmp_int(op, a, b))))
        }
        (Some(IngestVal::Uint(a)), Some(IngestVal::Uint(b))) => {
            Ok(Some(IngestVal::Bool(cmp_int(op, a, b))))
        }
        (Some(IngestVal::Double(a)), Some(IngestVal::Double(b))) => {
            Ok(Some(IngestVal::Bool(cmp_double(op, a, b))))
        }
        (Some(IngestVal::Bool(a)), Some(IngestVal::Bool(b))) => match op {
            CmpOp::Eq => Ok(Some(IngestVal::Bool(a == b))),
            CmpOp::Ne => Ok(Some(IngestVal::Bool(a != b))),
            _ => Err(refuse(
                "booleans do not order; only `==` and `!=` compare them",
            )),
        },
        _ => Err(refuse(
            "a materialization comparison mixes numeric types on this document; stock CEL does not coerce — convert \
             explicitly with double()",
        )),
    }
}

pub(super) fn ingest_logic(
    logic: &pb::ValueLogic,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    let and = match pb::ValueLogicOp::try_from(logic.op) {
        Ok(pb::ValueLogicOp::And) => true,
        Ok(pb::ValueLogicOp::Or) => false,
        _ => return Err(refuse("logic node with an unknown operation")),
    };
    if logic.children.len() < 2 {
        return Err(refuse("logic node with fewer than two operands"));
    }
    let sym = if and { "&&" } else { "||" };
    // Every operand evaluates (no short-circuit): a type
    // conflict stays loud no matter where it sits.
    let mut absent = false;
    let mut determined = false;
    for child in &logic.children {
        match eval_ingest_inner(child, env, strict)? {
            None => absent = true,
            Some(IngestVal::Bool(b)) => {
                if b != and {
                    determined = true;
                }
            }
            Some(_) => {
                return Err(refuse(format!(
                    "`{sym}` over a number; Kleene logic joins booleans"
                )));
            }
        }
    }
    Ok(if determined {
        // The determinate answer absorbs absence, exactly
        // stock CEL's error-absorbing commutative logic.
        Some(IngestVal::Bool(!and))
    } else if absent {
        None
    } else {
        Some(IngestVal::Bool(and))
    })
}

pub(super) fn ingest_ternary(
    t: &pb::ValueTernary,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    let cond = t
        .cond
        .as_ref()
        .ok_or_else(|| refuse("ternary node without a condition"))?;
    let then = t
        .then_value
        .as_ref()
        .ok_or_else(|| refuse("ternary node without a then-branch"))?;
    let otherwise = t
        .else_value
        .as_ref()
        .ok_or_else(|| refuse("ternary node without an else-branch"))?;
    match eval_ingest_inner(cond, env, strict)? {
        None => Ok(None),
        Some(IngestVal::Bool(true)) => eval_ingest_inner(then, env, strict),
        Some(IngestVal::Bool(false)) => eval_ingest_inner(otherwise, env, strict),
        Some(_) => Err(refuse(
            "the ternary's condition is a number; a condition is a boolean",
        )),
    }
}
