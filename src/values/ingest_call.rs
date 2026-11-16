//! Ingest-time function calls.

use super::ValuesError;

use super::functions::{
    check_fn_arity, civil_of_micros, eval_fn, fn_display, fn_signature, fnv1a64_bytes, FnSignature,
};
use super::ingest::eval_ingest_inner;
use super::ingest_env::{IngestEnv, IngestVal};
use super::refuse;
use super::types::{Val, ValueType};
use crate::pb;

pub(super) fn ingest_func(
    func: &pb::ValueFunc,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    use pb::value_expr::Expr as V;

    let f = match pb::ValueFn::try_from(func.function) {
        Ok(pb::ValueFn::Unspecified) | Err(_) => {
            return Err(refuse("function node with an unknown function"));
        }
        Ok(f) => f,
    };
    let display = fn_display(f);
    check_fn_arity(f, func.args.len())?;
    match fn_signature(f) {
        FnSignature::StringToUint => {
            // Resolution typed the argument as a facet read.
            return Ok(match func.args[0].expr.as_ref() {
                Some(V::Column(name)) => env
                    .facets
                    .get(name)
                    .map(|value| IngestVal::Uint(fnv1a64_bytes(value.as_bytes()))),
                _ => None,
            });
        }
        FnSignature::IntToInt => {
            return Ok(match eval_ingest_inner(&func.args[0], env, strict)? {
                None => None,
                Some(IngestVal::Int(micros)) => {
                    let (y, m, d) = civil_of_micros(micros);
                    Some(IngestVal::Int(match f {
                        pb::ValueFn::CalendarYear => y,
                        pb::ValueFn::CalendarMonth => i64::from(m),
                        _ => i64::from(d),
                    }))
                }
                Some(_) => {
                    return Err(refuse(format!(
                        "{display} takes an int of epoch microseconds (a timestamp \
                         column) on this document"
                    )));
                }
            });
        }
        FnSignature::Numeric { .. } => {}
    }

    ingest_numeric_func(f, display, func, env, strict)
}

/// A numeric function over the document's values: one numeric type across
/// the present arguments, absent when any argument is.
fn ingest_numeric_func(
    f: pb::ValueFn,
    display: &str,
    func: &pb::ValueFunc,
    env: &IngestEnv,
    strict: bool,
) -> Result<Option<IngestVal>, ValuesError> {
    let type_preserving = matches!(
        f,
        pb::ValueFn::Abs | pb::ValueFn::Sign | pb::ValueFn::Greatest | pb::ValueFn::Least
    );
    let mut vals: Vec<Option<Val>> = Vec::with_capacity(func.args.len());
    let mut known: Option<ValueType> = None;
    for a in &func.args {
        let v = match eval_ingest_inner(a, env, strict)? {
            None => None,
            Some(IngestVal::Bool(_)) => {
                return Err(refuse(format!("{display} over a bool; it takes numbers")));
            }
            Some(IngestVal::Int(v)) => {
                if known.is_some_and(|t| t != ValueType::Int) {
                    return Err(refuse(format!(
                        "{display} mixes numeric types on this \
                         document; stock CEL does not coerce — convert \
                         explicitly with double()"
                    )));
                }
                known = Some(ValueType::Int);
                Some(Val::Int(v))
            }
            Some(IngestVal::Uint(v)) => {
                if known.is_some_and(|t| t != ValueType::Uint) {
                    return Err(refuse(format!(
                        "{display} mixes numeric types on this \
                         document; stock CEL does not coerce — convert \
                         explicitly with double()"
                    )));
                }
                known = Some(ValueType::Uint);
                Some(Val::Uint(v))
            }
            Some(IngestVal::Double(v)) => {
                if known.is_some_and(|t| t != ValueType::Double) {
                    return Err(refuse(format!(
                        "{display} mixes numeric types on this \
                         document; stock CEL does not coerce — convert \
                         explicitly with double()"
                    )));
                }
                known = Some(ValueType::Double);
                Some(Val::Double(v))
            }
        };
        vals.push(v);
    }
    if !type_preserving && matches!(known, Some(ValueType::Int | ValueType::Uint)) {
        return Err(refuse(format!(
            "{display} takes a double; convert explicitly with double()"
        )));
    }
    let Some(vals) = vals.into_iter().collect::<Option<Vec<Val>>>() else {
        return Ok(None);
    };
    if strict && f == pb::ValueFn::Abs && matches!(vals[0], Val::Int(i64::MIN)) {
        return Err(refuse(
            "integer overflow: math.abs(i64::MIN) has no exact i64 result",
        ));
    }
    Ok(eval_fn(f, &vals).map(|v| match v {
        Val::Int(i) => IngestVal::Int(i),
        Val::Uint(i) => IngestVal::Uint(i),
        Val::Double(d) => IngestVal::Double(d),
        Val::Bool(b) => IngestVal::Bool(b),
        Val::FacetOrd { .. } | Val::MapFacetOrd { .. } => {
            unreachable!("functions compute numbers and bools")
        }
    }))
}
