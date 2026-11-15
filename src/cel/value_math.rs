//! The namespaced function vocabulary of the value compiler: `math.*`,
//! `engine.*`, `hash.*`, and `calendar.*`.

use tonic::Status;

use super::ast::Ast;
use super::refuse;
use super::value::{compile_value_ast, value_of, VType};
use crate::pb;

/// One namespaced call: arity, then typing as far as literals pin it down
/// (finished per shard like everything else).
pub(super) fn compile_math_call(
    ns: &str,
    name: &str,
    args: &[Ast],
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), Status> {
    let Some(f) = math_fn_of(ns, name) else {
        return Err(refuse(format!(
            "unknown function {ns}.{name}(); the function vocabulary is documented \
             in docs/cel-values.md"
        )));
    };
    let display = format!("{ns}.{name}()");

    match crate::values::fn_signature(f) {
        crate::values::FnSignature::StringToUint => compile_string_hash(f, &display, args, depth),
        crate::values::FnSignature::IntToInt => compile_calendar_part(f, &display, args, depth),
        crate::values::FnSignature::Numeric { .. } => {
            compile_numeric_call(f, &display, args, depth)
        }
    }
}

/// A function from a facet string to a uint.
fn compile_string_hash(
    f: pb::ValueFn,
    display: &str,
    args: &[Ast],
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), Status> {
    use pb::value_expr::Expr as V;

    let [arg] = args else {
        return Err(refuse(format!("{display} takes exactly one argument")));
    };
    // `hash.fnv64(stable_key())` is its own leaf: the key is not
    // a column, and this is the only place stable_key() reads.
    if let Ast::Call(inner, inner_args) = arg {
        if inner == "stable_key" {
            if !inner_args.is_empty() {
                return Err(refuse("stable_key() takes no arguments"));
            }
            return Ok((
                value_of(V::StableKeyHash(pb::StableKeyHash {})),
                Some(VType::Uint),
            ));
        }
    }
    let (e, vt) = compile_value_ast(arg, depth + 1)?;
    match vt {
        Some(VType::Str) => {
            return Err(refuse(format!(
                "{display} over a string literal is a constant; hash a facet column"
            )));
        }
        Some(t) => {
            return Err(refuse(format!(
                "{display} takes a facet string, not a {}",
                t.name()
            )));
        }
        None => {}
    }

    Ok((
        value_of(V::Func(pb::ValueFunc {
            function: f as i32,
            args: vec![e],
        })),
        Some(VType::Uint),
    ))
}

/// A function from an int of epoch microseconds to an int.
fn compile_calendar_part(
    f: pb::ValueFn,
    display: &str,
    args: &[Ast],
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), Status> {
    use pb::value_expr::Expr as V;

    let [arg] = args else {
        return Err(refuse(format!("{display} takes exactly one argument")));
    };
    let (e, vt) = compile_value_ast(arg, depth + 1)?;
    match vt {
        Some(VType::Int) | None => {}
        Some(t) => {
            return Err(refuse(format!(
                "{display} takes an int of epoch microseconds (a timestamp column), \
                 not a {}",
                t.name()
            )));
        }
    }

    Ok((
        value_of(V::Func(pb::ValueFunc {
            function: f as i32,
            args: vec![e],
        })),
        Some(VType::Int),
    ))
}

/// A numeric function: its arity, then one numeric type across the
/// arguments literals pin.
fn compile_numeric_call(
    f: pb::ValueFn,
    display: &str,
    args: &[Ast],
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), Status> {
    use pb::value_expr::Expr as V;

    let arity_ok = match f {
        pb::ValueFn::Greatest | pb::ValueFn::Least => !args.is_empty(),
        pb::ValueFn::Pow => args.len() == 2,
        _ => args.len() == 1,
    };
    if !arity_ok {
        let want = match f {
            pb::ValueFn::Greatest | pb::ValueFn::Least => "at least one argument",
            pb::ValueFn::Pow => "exactly two arguments",
            _ => "exactly one argument",
        };
        return Err(refuse(format!("{display} takes {want}")));
    }
    let type_preserving = matches!(
        f,
        pb::ValueFn::Abs | pb::ValueFn::Sign | pb::ValueFn::Greatest | pb::ValueFn::Least
    );
    let mut compiled = Vec::with_capacity(args.len());
    let mut known: Option<VType> = None;
    for a in args {
        let (e, vt) = compile_value_ast(a, depth + 1)?;
        match vt {
            Some(VType::Bool) | Some(VType::Str) => {
                return Err(refuse(format!(
                    "{display} over a {}; it takes numbers",
                    vt.unwrap().name()
                )));
            }
            Some(t) => match known {
                Some(k) if k != t => {
                    return Err(refuse(format!(
                        "{display} mixes numeric types; stock CEL does not \
                         coerce — convert explicitly with double()"
                    )));
                }
                _ => known = Some(t),
            },
            None => {}
        }
        compiled.push(e);
    }
    let vt = if type_preserving {
        known
    } else {
        if matches!(known, Some(VType::Int | VType::Uint)) {
            return Err(refuse(format!(
                "{display} takes a double; convert explicitly with double()"
            )));
        }
        match f {
            pb::ValueFn::IsNan | pb::ValueFn::IsInf | pb::ValueFn::IsFinite => Some(VType::Bool),
            _ => Some(VType::Double),
        }
    };
    Ok((
        value_of(V::Func(pb::ValueFunc {
            function: f as i32,
            args: compiled,
        })),
        vt,
    ))
}

/// The (namespace, name) to wire-function table.
fn math_fn_of(ns: &str, name: &str) -> Option<pb::ValueFn> {
    match (ns, name) {
        ("math", "abs") => Some(pb::ValueFn::Abs),
        ("math", "sign") => Some(pb::ValueFn::Sign),
        ("math", "ceil") => Some(pb::ValueFn::Ceil),
        ("math", "floor") => Some(pb::ValueFn::Floor),
        ("math", "round") => Some(pb::ValueFn::Round),
        ("math", "trunc") => Some(pb::ValueFn::Trunc),
        ("math", "sqrt") => Some(pb::ValueFn::Sqrt),
        ("math", "isNaN") => Some(pb::ValueFn::IsNan),
        ("math", "isInf") => Some(pb::ValueFn::IsInf),
        ("math", "isFinite") => Some(pb::ValueFn::IsFinite),
        ("math", "greatest") => Some(pb::ValueFn::Greatest),
        ("math", "least") => Some(pb::ValueFn::Least),
        ("engine", "ln") => Some(pb::ValueFn::Ln),
        ("engine", "exp") => Some(pb::ValueFn::Exp),
        ("engine", "log10") => Some(pb::ValueFn::Log10),
        ("engine", "pow") => Some(pb::ValueFn::Pow),
        ("hash", "fnv64") => Some(pb::ValueFn::Fnv64),
        ("calendar", "year") => Some(pb::ValueFn::CalendarYear),
        ("calendar", "month") => Some(pb::ValueFn::CalendarMonth),
        ("calendar", "day") => Some(pb::ValueFn::CalendarDay),
        _ => None,
    }
}
