//! Query-time evaluation of a resolved expression for one document.

use tonic::Status;

use super::functions::eval_fn;
use super::refuse;
use super::types::{CmpOp, Op, ResolvedValue, Val};
use crate::pb;
use crate::scorefn::NumericRead;

/// The string-ordering refusal, shared by both evaluation paths.
pub(super) fn string_ordering(op: CmpOp) -> Status {
    refuse(format!(
        "`{}` orders strings, which is not in the vocabulary; only `==` and \
         `!=` compare strings",
        op.name()
    ))
}

/// Integer comparison.
pub(super) fn cmp_int<T: Ord>(op: CmpOp, a: T, b: T) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Le => a <= b,
        CmpOp::Gt => a > b,
        CmpOp::Ge => a >= b,
    }
}

/// IEEE f64 comparison, exactly stock CEL: every comparison with NaN
/// is false except `!=`, which is true.
pub(super) fn cmp_double(op: CmpOp, a: f64, b: f64) -> bool {
    match op {
        CmpOp::Eq => a == b,
        CmpOp::Ne => a != b,
        CmpOp::Lt => a < b,
        CmpOp::Le => a <= b,
        CmpOp::Gt => a > b,
        CmpOp::Ge => a >= b,
    }
}

/// Evaluate one resolved expression for one document. `None` is
/// absence: a missing input, or integer arithmetic stock CEL would
/// call an error.
pub fn eval(rv: &ResolvedValue, doc_id: u32, cols: &dyn NumericRead) -> Option<Val> {
    match rv {
        ResolvedValue::NumCol(ni) => cols.value(*ni, doc_id).map(Val::Double),
        ResolvedValue::IntCol(ii) => cols.int_value(*ii, doc_id).map(Val::Int),
        ResolvedValue::UintCol(ui) => cols.uint_value(*ui, doc_id).map(Val::Uint),
        ResolvedValue::FacetCol(fi) => cols
            .facet_ord(*fi, doc_id)
            .map(|ord| Val::FacetOrd { column: *fi, ord }),
        ResolvedValue::MapNum { column, key_ord } => {
            cols.map_value(*column, *key_ord, doc_id).map(Val::Double)
        }
        ResolvedValue::MapInt { column, key_ord } => key_ord
            .and_then(|key| cols.map_int_value(*column, key, doc_id))
            .map(Val::Int),
        ResolvedValue::MapUint { column, key_ord } => key_ord
            .and_then(|key| cols.map_uint_value(*column, key, doc_id))
            .map(Val::Uint),
        ResolvedValue::MapFacet { column, key_ord } => cols
            .map_facet_value_ord(*column, *key_ord, doc_id)
            .map(|ord| Val::MapFacetOrd {
                column: *column,
                ord,
            }),
        ResolvedValue::Absent => None,
        ResolvedValue::StableKeyHash => {
            unreachable!("resolution refused the stable-key hash at query time")
        }
        ResolvedValue::ConstInt(v) => Some(Val::Int(*v)),
        ResolvedValue::ConstUint(v) => Some(Val::Uint(*v)),
        ResolvedValue::ConstDouble(v) => Some(Val::Double(*v)),

        ResolvedValue::ToDouble(inner) => to_double(eval(inner, doc_id, cols)?),
        ResolvedValue::Negate(inner) => negate(eval(inner, doc_id, cols)?),
        ResolvedValue::Arith { op, left, right } => {
            let l = eval(left, doc_id, cols)?;
            let r = eval(right, doc_id, cols)?;
            arith(op, l, r)
        }

        ResolvedValue::ConstBool(v) => Some(Val::Bool(*v)),

        ResolvedValue::Compare { op, left, right } => {
            let l = eval(left, doc_id, cols)?;
            let r = eval(right, doc_id, cols)?;
            Some(Val::Bool(compare(op, l, r)))
        }

        ResolvedValue::CompareStr { ne, read, lit_ord } => {
            let ord = match eval(read, doc_id, cols)? {
                Val::FacetOrd { ord, .. } | Val::MapFacetOrd { ord, .. } => ord,
                _ => unreachable!("resolution shaped the read side"),
            };
            let eq = *lit_ord == Some(ord);
            Some(Val::Bool(eq != *ne))
        }

        ResolvedValue::Logic { and, children } => eval_logic(*and, children, doc_id, cols),

        ResolvedValue::Not(inner) => match eval(inner, doc_id, cols)? {
            Val::Bool(b) => Some(Val::Bool(!b)),
            _ => unreachable!("resolution type-checked the operand"),
        },
        ResolvedValue::Func { f, args } => eval_call(*f, args, doc_id, cols),
        ResolvedValue::Ternary {
            cond,
            then,
            otherwise,
        } => match eval(cond, doc_id, cols)? {
            Val::Bool(true) => eval(then, doc_id, cols),
            Val::Bool(false) => eval(otherwise, doc_id, cols),
            _ => unreachable!("resolution type-checked the condition"),
        },
    }
}

fn to_double(v: Val) -> Option<Val> {
    match v {
        Val::Int(v) => Some(Val::Double(v as f64)),
        Val::Uint(v) => Some(Val::Double(v as f64)),
        Val::Double(v) => Some(Val::Double(v)),
        Val::FacetOrd { .. } | Val::MapFacetOrd { .. } | Val::Bool(_) => {
            unreachable!("resolution refused double() over a non-number")
        }
    }
}

fn negate(v: Val) -> Option<Val> {
    match v {
        Val::Int(v) => v.checked_neg().map(Val::Int),
        Val::Uint(_) => unreachable!("resolution refused unary minus over a uint"),
        Val::Double(v) => Some(Val::Double(-v)),
        Val::FacetOrd { .. } | Val::MapFacetOrd { .. } | Val::Bool(_) => {
            unreachable!("resolution refused unary minus over a non-number")
        }
    }
}

fn arith(op: &Op, l: Val, r: Val) -> Option<Val> {
    match (l, r) {
        (Val::Int(a), Val::Int(b)) => int_arith(*op, a, b).map(Val::Int),
        (Val::Uint(a), Val::Uint(b)) => uint_arith(*op, a, b).map(Val::Uint),
        (Val::Double(a), Val::Double(b)) => Some(Val::Double(double_arith(*op, a, b))),
        _ => unreachable!("resolution type-checked the operands"),
    }
}

fn compare(op: &CmpOp, l: Val, r: Val) -> bool {
    match (l, r) {
        (Val::Int(a), Val::Int(b)) => cmp_int(*op, a, b),
        (Val::Uint(a), Val::Uint(b)) => cmp_int(*op, a, b),
        (Val::Double(a), Val::Double(b)) => cmp_double(*op, a, b),
        (Val::Bool(a), Val::Bool(b)) => match op {
            CmpOp::Eq => a == b,
            CmpOp::Ne => a != b,
            _ => unreachable!("resolution refused ordering booleans"),
        },
        _ => unreachable!("resolution type-checked the operands"),
    }
}

/// A function call. Kleene: every argument must be present.
fn eval_call(
    f: pb::ValueFn,
    args: &[ResolvedValue],
    doc_id: u32,
    cols: &dyn NumericRead,
) -> Option<Val> {
    let mut vals = Vec::with_capacity(args.len());
    for a in args {
        vals.push(eval(a, doc_id, cols)?);
    }
    eval_fn(f, &vals)
}

/// Kleene `&&` (`and` true) or `||` over `children`.
fn eval_logic(
    and: bool,
    children: &[ResolvedValue],
    doc_id: u32,
    cols: &dyn NumericRead,
) -> Option<Val> {
    let mut absent = false;
    for child in children {
        match eval(child, doc_id, cols) {
            None => absent = true,
            Some(Val::Bool(b)) => {
                if b != and {
                    // The determinate answer absorbs absence, exactly
                    // stock CEL's error-absorbing commutative logic.
                    return Some(Val::Bool(!and));
                }
            }
            Some(_) => unreachable!("resolution type-checked the operands"),
        }
    }
    if absent {
        None
    } else {
        Some(Val::Bool(and))
    }
}

/// Checked u64 arithmetic: overflow, underflow and zero divisors yield absence.
pub(super) fn uint_arith(op: Op, a: u64, b: u64) -> Option<u64> {
    match op {
        Op::Add => a.checked_add(b),
        Op::Sub => a.checked_sub(b),
        Op::Mul => a.checked_mul(b),
        Op::Div => a.checked_div(b),
        Op::Mod => a.checked_rem(b),
    }
}

/// Checked i64 arithmetic: where stock CEL errors (overflow, division
/// by zero, `i64::MIN / -1`, `i64::MIN % -1`), the engine answers
/// ABSENT. Division truncates toward zero and `%` takes the dividend's
/// sign, exactly CEL's (and Rust's) rule.
pub(super) fn int_arith(op: Op, a: i64, b: i64) -> Option<i64> {
    match op {
        Op::Add => a.checked_add(b),
        Op::Sub => a.checked_sub(b),
        Op::Mul => a.checked_mul(b),
        Op::Div => a.checked_div(b),
        Op::Mod => a.checked_rem(b),
    }
}

/// IEEE f64 arithmetic, exactly what stock CEL does with doubles.
/// Division by zero is a signed infinity and 0/0 is NaN — values, not
/// errors, on both sides of the oracle.
pub(super) fn double_arith(op: Op, a: f64, b: f64) -> f64 {
    match op {
        Op::Add => a + b,
        Op::Sub => a - b,
        Op::Mul => a * b,
        Op::Div => a / b,
        Op::Mod => unreachable!("resolution refused a double remainder"),
    }
}
