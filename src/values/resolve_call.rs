//! Resolution of function calls, Kleene logic, and the conditional.

use super::ValuesError;

use super::functions::{check_fn_arity, fn_display, fn_signature, FnSignature};
use super::lookup::ColumnLookup;
use super::refuse;
use super::resolve::resolve;
use super::types::{ResolvedValue, ValueType};
use crate::pb;

pub(super) fn resolve_func(
    func: &pb::ValueFunc,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let f = match pb::ValueFn::try_from(func.function) {
        Ok(pb::ValueFn::Unspecified) | Err(_) => {
            return Err(refuse("function node with an unknown function"));
        }
        Ok(f) => f,
    };
    let display = fn_display(f);
    check_fn_arity(f, func.args.len())?;

    match fn_signature(f) {
        FnSignature::StringToUint => resolve_string_hash(f, display, func, cols),
        FnSignature::IntToInt => resolve_calendar_part(f, display, func, cols),
        FnSignature::Numeric { .. } => resolve_numeric_func(f, display, func, cols),
    }
}

/// A function from a facet string to a uint; it computes at ingest only.
fn resolve_string_hash(
    f: pb::ValueFn,
    display: &str,
    func: &pb::ValueFunc,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    if !cols.at_ingest() {
        return Err(refuse(format!(
            "{display} computes at ingest; at query time read the derived \
             column that stores it (docs/derived-columns.md)"
        )));
    }
    let (rv, vt) = resolve(&func.args[0], cols)?;

    match vt {
        ValueType::Str => Ok((ResolvedValue::Func { f, args: vec![rv] }, ValueType::Uint)),
        ValueType::Unknown => Ok((ResolvedValue::Absent, ValueType::Unknown)),
        other => Err(refuse(format!(
            "{display} takes a facet string, not a {}",
            other.name()
        ))),
    }
}

/// A function from an int of epoch microseconds to an int.
fn resolve_calendar_part(
    f: pb::ValueFn,
    display: &str,
    func: &pb::ValueFunc,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let (rv, vt) = resolve(&func.args[0], cols)?;

    match vt {
        ValueType::Int => Ok((ResolvedValue::Func { f, args: vec![rv] }, ValueType::Int)),
        ValueType::Unknown => Ok((ResolvedValue::Absent, ValueType::Unknown)),
        other => Err(refuse(format!(
            "{display} takes an int of epoch microseconds (a timestamp \
             column), not a {}",
            other.name()
        ))),
    }
}

/// A numeric function: one numeric type across its arguments, and absent
/// when any argument can never hold a value.
fn resolve_numeric_func(
    f: pb::ValueFn,
    display: &str,
    func: &pb::ValueFunc,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let type_preserving = matches!(
        f,
        pb::ValueFn::Abs | pb::ValueFn::Sign | pb::ValueFn::Greatest | pb::ValueFn::Least
    );
    let mut args = Vec::with_capacity(func.args.len());
    let mut known: Option<ValueType> = None;
    let mut any_absent = false;
    for a in &func.args {
        let (rv, vt) = resolve(a, cols)?;
        match vt {
            ValueType::Bool | ValueType::Str => {
                return Err(refuse(format!(
                    "{display} over a {}; it takes numbers",
                    vt.name()
                )));
            }
            ValueType::Unknown => any_absent = true,
            t => {
                if let Some(k) = known {
                    if k != t {
                        return Err(refuse(format!(
                            "{display} mixes numeric types; stock CEL \
                             does not coerce — convert explicitly with double()"
                        )));
                    }
                }
                known = Some(t);
            }
        }
        args.push(rv);
    }
    if !type_preserving && matches!(known, Some(ValueType::Int | ValueType::Uint)) {
        return Err(refuse(format!(
            "{display} takes a double; convert explicitly with double()"
        )));
    }
    if any_absent {
        // Kleene: an argument that can never hold a value makes
        // the call absent for every document here.
        return Ok((ResolvedValue::Absent, ValueType::Unknown));
    }
    let vt = if type_preserving {
        known.unwrap_or(ValueType::Unknown)
    } else {
        match f {
            pb::ValueFn::IsNan | pb::ValueFn::IsInf | pb::ValueFn::IsFinite => ValueType::Bool,
            _ => ValueType::Double,
        }
    };
    Ok((ResolvedValue::Func { f, args }, vt))
}

pub(super) fn resolve_logic(
    logic: &pb::ValueLogic,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let and = match pb::ValueLogicOp::try_from(logic.op) {
        Ok(pb::ValueLogicOp::And) => true,
        Ok(pb::ValueLogicOp::Or) => false,
        _ => return Err(refuse("logic node with an unknown operation")),
    };
    if logic.children.len() < 2 {
        return Err(refuse("logic node with fewer than two operands"));
    }
    let sym = if and { "&&" } else { "||" };
    let mut children = Vec::with_capacity(logic.children.len());
    for child in &logic.children {
        let (rv, vt) = resolve(child, cols)?;
        if !matches!(vt, ValueType::Bool | ValueType::Unknown) {
            return Err(refuse(format!(
                "`{sym}` over a {}; Kleene logic joins booleans",
                vt.name()
            )));
        }
        children.push(rv);
    }
    Ok((ResolvedValue::Logic { and, children }, ValueType::Bool))
}

pub(super) fn resolve_ternary(
    t: &pb::ValueTernary,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
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
    let (crv, ct) = resolve(cond, cols)?;
    if !matches!(ct, ValueType::Bool | ValueType::Unknown) {
        return Err(refuse(format!(
            "the ternary's condition is a {}; a condition is a boolean",
            ct.name()
        )));
    }
    let (trv, tt) = resolve(then, cols)?;
    let (erv, et) = resolve(otherwise, cols)?;
    let vt = match (tt, et) {
        (a, b) if a == b => a,
        (ValueType::Unknown, other) | (other, ValueType::Unknown) => other,
        (ValueType::Int, ValueType::Double) | (ValueType::Double, ValueType::Int) => {
            return Err(refuse(
                "the ternary's branches disagree: int versus double; stock \
                 CEL does not coerce — convert explicitly with double()",
            ));
        }
        (a, b) => {
            return Err(refuse(format!(
                "the ternary's branches disagree: {} versus {}",
                a.name(),
                b.name()
            )));
        }
    };
    Ok((
        ResolvedValue::Ternary {
            cond: Box::new(crv),
            then: Box::new(trv),
            otherwise: Box::new(erv),
        },
        vt,
    ))
}
