//! Ingest-time evaluation of a compiled expression against one document's
//! own values, for materialized columns.

use super::ValuesError;

use super::eval::{double_arith, int_arith, uint_arith};
use super::functions::fnv1a64_bytes;
use super::ingest_call::ingest_func;
use super::ingest_env::{IngestEnv, IngestVal};
use super::ingest_logic::{ingest_compare, ingest_logic, ingest_ternary};
use super::refuse;
use super::resolve::resolve;
use super::types::Op;
use crate::pb;

/// Evaluate one compiled expression against one document's own values.
/// `Ok(None)` is absence (a name this document does not carry, or
/// checked integer arithmetic); `Err` is a REAL conflict the document
/// exposes — int mixed with double, an ambiguous name — refused loudly
/// per the loud-failure rule, never stored as a guess.
pub fn eval_ingest(
    expr: &pb::ValueExpr,
    env: &IngestEnv,
) -> Result<Option<IngestVal>, ValuesError> {
    // Check all branches without evaluating them. A type error in an untaken
    // branch is still an error; arithmetic absence there must not propagate.
    resolve(expr, env)?;
    eval_ingest_inner(expr, env, false)
}

/// [`eval_ingest`] for one of the index's DECLARED derived columns: the integer operations stock CEL calls
/// errors — overflow, a zero divisor, the `i64::MIN` edges — refuse
/// loudly naming the operation, because a declared column must not
/// turn an impossible value into the same absence a missing input
/// stores. A genuinely missing input still evaluates `Ok(None)`:
/// absence and impossibility are different outcomes.
pub fn eval_ingest_derived(
    expr: &pb::ValueExpr,
    env: &IngestEnv,
) -> Result<Option<IngestVal>, ValuesError> {
    resolve(expr, env)?;
    eval_ingest_inner(expr, env, true)
}

/// The strict derived-column refusal for an impossible integer
/// operation: the operands, the operation, and the cause.
fn arith_overflow(
    op: Op,
    a: impl std::fmt::Display,
    b: impl std::fmt::Display,
    zero_divisor: bool,
) -> ValuesError {
    let cause = if zero_divisor {
        "division by zero"
    } else {
        "overflow"
    };
    refuse(format!(
        "integer arithmetic {cause}: {a} {} {b} has no exact answer; a declared derived \
         column refuses the document rather than storing a wrapped value or a fake absence",
        op.name()
    ))
}

pub(super) fn eval_ingest_inner(
    expr: &pb::ValueExpr,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    use pb::value_expr::Expr as V;
    match expr
        .expr
        .as_ref()
        .ok_or_else(|| refuse("empty ValueExpr node"))?
    {
        V::Column(name) => ingest_column(name, env),

        V::StableKeyHash(_) => Ok(env
            .stable_key
            .as_deref()
            .map(|key| IngestVal::Uint(fnv1a64_bytes(key)))),
        V::Map(read) => Ok(env
            .map_numerics
            .get(&(read.column.clone(), read.key.clone()))
            .map(|v| IngestVal::Double(*v))),
        V::TypedMap(read) => {
            let key = (read.column.clone(), read.key.clone());
            Ok(env
                .map_integers
                .get(&key)
                .map(|v| IngestVal::Int(*v))
                .or_else(|| {
                    env.map_unsigned_integers
                        .get(&key)
                        .map(|v| IngestVal::Uint(*v))
                })
                .or_else(|| env.map_numerics.get(&key).map(|v| IngestVal::Double(*v))))
        }
        V::IntLiteral(v) => Ok(Some(IngestVal::Int(*v))),
        V::UintLiteral(v) => Ok(Some(IngestVal::Uint(*v))),
        V::FloatLiteral(v) => Ok(Some(IngestVal::Double(*v))),
        V::ToDouble(inner) => Ok(match eval_ingest_inner(inner, env, strict)? {
            None => None,
            Some(IngestVal::Int(i)) => Some(IngestVal::Double(i as f64)),
            Some(IngestVal::Uint(i)) => Some(IngestVal::Double(i as f64)),
            Some(IngestVal::Double(d)) => Some(IngestVal::Double(d)),
            Some(IngestVal::Bool(_)) => {
                return Err(refuse("double() over a bool; only numbers convert"));
            }
        }),

        V::Negate(inner) => ingest_negate(inner, env, strict),
        V::Arith(arith) => ingest_arith(arith, env, strict),

        V::BoolLiteral(v) => Ok(Some(IngestVal::Bool(*v))),
        V::StringLiteral(_) => Err(refuse(
            "a string literal is not a value outside a `==`/`!=` comparison",
        )),

        V::Compare(cmp) => ingest_compare(cmp, env, strict),
        V::Func(func) => ingest_func(func, env, strict),
        V::Logic(logic) => ingest_logic(logic, env, strict),

        V::Not(inner) => Ok(match eval_ingest_inner(inner, env, strict)? {
            None => None,
            Some(IngestVal::Bool(b)) => Some(IngestVal::Bool(!b)),
            Some(_) => {
                return Err(refuse("`!` over a number; Kleene logic joins booleans"));
            }
        }),

        V::Ternary(t) => ingest_ternary(t, env, strict),
    }
}

/// A plain column read from the document's own numeric families.
fn ingest_column(name: &str, env: &IngestEnv) -> Result<Option<IngestVal>, ValuesError> {
    let num = env.numerics.get(name);
    let int = env.integers.get(name);
    let uint = env.unsigned_integers.get(name);
    let facet = env.facets.contains_key(name);
    if usize::from(num.is_some())
        + usize::from(int.is_some())
        + usize::from(uint.is_some())
        + usize::from(facet)
        > 1
    {
        return Err(refuse(format!(
            "materialization input {name:?} arrives in more than one family on this document"
        )));
    }
    if facet {
        // Resolution admits a facet read only under hash.fnv64()
        // or against a string literal, both handled by their
        // parent nodes below.
        return Err(refuse(format!(
            "facet {name:?} is a string; it feeds hash.fnv64() or a `==`/`!=` \
             comparison, and is not itself a stored number"
        )));
    }
    Ok(num
        .map(|v| IngestVal::Double(*v))
        .or(int.map(|v| IngestVal::Int(*v)))
        .or(uint.map(|v| IngestVal::Uint(*v))))
}

fn ingest_negate(
    inner: &pb::ValueExpr,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    Ok(match eval_ingest_inner(inner, env, strict)? {
        None => None,
        Some(IngestVal::Int(v)) => match v.checked_neg() {
            Some(v) => Some(IngestVal::Int(v)),
            None if strict => {
                return Err(refuse(format!(
                    "integer overflow: -({v}) has no exact answer; a declared derived \
                     column refuses the document rather than storing a wrapped value \
                     or a fake absence"
                )));
            }
            None => None,
        },
        Some(IngestVal::Uint(_)) => return Err(refuse("unary minus over a uint")),
        Some(IngestVal::Double(v)) => Some(IngestVal::Double(-v)),
        Some(IngestVal::Bool(_)) => {
            return Err(refuse(
                "unary minus over a boolean; booleans negate with `!`",
            ));
        }
    })
}

fn ingest_arith(
    arith: &pb::ValueArith,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    let left = arith
        .left
        .as_ref()
        .ok_or_else(|| refuse("arithmetic node without a left operand"))?;
    let right = arith
        .right
        .as_ref()
        .ok_or_else(|| refuse("arithmetic node without a right operand"))?;
    let (l, r) = (
        eval_ingest_inner(left, env, strict)?,
        eval_ingest_inner(right, env, strict)?,
    );
    let op = match pb::ArithOp::try_from(arith.op) {
        Ok(pb::ArithOp::Add) => Op::Add,
        Ok(pb::ArithOp::Sub) => Op::Sub,
        Ok(pb::ArithOp::Mul) => Op::Mul,
        Ok(pb::ArithOp::Div) => Op::Div,
        Ok(pb::ArithOp::Mod) => Op::Mod,
        _ => return Err(refuse("arithmetic node with an unknown operation")),
    };
    match (l, r) {
        (None, _) | (_, None) => Ok(None),
        (Some(IngestVal::Bool(_)), _) | (_, Some(IngestVal::Bool(_))) => Err(refuse(
            "a boolean joins no arithmetic; wrap it in a ternary that \
             yields a number",
        )),
        (Some(IngestVal::Int(a)), Some(IngestVal::Int(b))) => match int_arith(op, a, b) {
            Some(v) => Ok(Some(IngestVal::Int(v))),
            None if strict => Err(arith_overflow(op, a, b, b == 0)),
            None => Ok(None),
        },
        (Some(IngestVal::Uint(a)), Some(IngestVal::Uint(b))) => match uint_arith(op, a, b) {
            Some(v) => Ok(Some(IngestVal::Uint(v))),
            None if strict => Err(arith_overflow(op, a, b, b == 0)),
            None => Ok(None),
        },
        (Some(IngestVal::Double(a)), Some(IngestVal::Double(b))) => {
            if op == Op::Mod {
                return Err(refuse(
                    "`%` is integer-only in CEL; there is no double remainder",
                ));
            }
            Ok(Some(IngestVal::Double(double_arith(op, a, b))))
        }
        _ => Err(refuse(
            "materialization arithmetic mixes numeric types \
             on this document; stock CEL does not coerce — convert explicitly \
             with double()",
        )),
    }
}
