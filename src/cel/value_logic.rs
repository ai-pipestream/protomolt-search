//! The boolean layer of the value compiler: Kleene logic, comparisons, and
//! the conditional.

use super::CelError;

use super::ast::{Ast, RelOp};
use super::refuse;
use super::value::{compile_value_ast, value_of, VType};
use crate::pb;

/// The string, list, logic, comparison, and conditional arms of the value
/// compiler; [`compile_value_ast`] handles the rest before delegating here.
pub(super) fn compile_value_tail(
    ast: &Ast,
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;
    match ast {
        Ast::Str(v) => Ok((value_of(V::StringLiteral(v.clone())), Some(VType::Str))),
        Ast::List(_) => Err(refuse("a list is not a value")),

        Ast::Or(children) | Ast::And(children) => {
            compile_logic(matches!(ast, Ast::And(_)), children, depth)
        }

        Ast::Not(inner) => {
            let (expr, vt) = compile_value_ast(inner, depth + 1)?;
            if let Some(t) = vt {
                if t != VType::Bool {
                    return Err(refuse(format!(
                        "`!` over a {}; Kleene logic joins booleans",
                        t.name()
                    )));
                }
            }
            Ok((value_of(V::Not(Box::new(expr))), Some(VType::Bool)))
        }

        Ast::Rel(l, op, r) => compile_compare(l, op, r, depth),
        Ast::Ternary(c, t, e) => compile_ternary(c, t, e, depth),

        Ast::Ident(_)
        | Ast::MapAccess(..)
        | Ast::Int(_)
        | Ast::Uint(_)
        | Ast::Float(_)
        | Ast::Neg(_)
        | Ast::Arith(..)
        | Ast::Call(..) => {
            unreachable!("compile_value_ast handles these before delegating")
        }
    }
}

/// `&&` or `||` over boolean children.
fn compile_logic(
    and: bool,
    children: &[Ast],
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;

    let sym = if and { "&&" } else { "||" };
    let mut compiled = Vec::with_capacity(children.len());
    for child in children {
        let (expr, vt) = compile_value_ast(child, depth + 1)?;
        if let Some(t) = vt {
            if t != VType::Bool {
                return Err(refuse(format!(
                    "`{sym}` over a {}; Kleene logic joins booleans",
                    t.name()
                )));
            }
        }
        compiled.push(expr);
    }
    let op = if and {
        pb::ValueLogicOp::And
    } else {
        pb::ValueLogicOp::Or
    };
    Ok((
        value_of(V::Logic(pb::ValueLogic {
            children: compiled,
            op: op as i32,
        })),
        Some(VType::Bool),
    ))
}

/// A comparison: strings and booleans compare for equality only, and
/// operands whose types literals pin must agree.
fn compile_compare(
    l: &Ast,
    op: &RelOp,
    r: &Ast,
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;

    let (le, lt) = compile_value_ast(l, depth + 1)?;
    let (re, rt) = compile_value_ast(r, depth + 1)?;
    let ordering = !matches!(op, RelOp::Eq | RelOp::Ne);
    for t in [lt, rt].into_iter().flatten() {
        match t {
            VType::Str if ordering => {
                return Err(refuse(format!(
                    "`{}` orders strings, which is not in the vocabulary; only \
                     `==` and `!=` compare strings",
                    op.name()
                )));
            }
            VType::Bool if ordering => {
                return Err(refuse(format!(
                    "`{}` over a boolean; booleans do not order",
                    op.name()
                )));
            }
            _ => {}
        }
    }
    if let (Some(a), Some(b)) = (lt, rt) {
        if a != b {
            let fix = if matches!(
                (a, b),
                (VType::Int, VType::Double) | (VType::Double, VType::Int)
            ) {
                "; stock CEL does not coerce — convert explicitly with double()"
            } else {
                ""
            };
            return Err(refuse(format!(
                "`{}` compares a {} with a {}{fix}",
                op.name(),
                a.name(),
                b.name()
            )));
        }
    }
    let wire_op = match op {
        RelOp::Eq => pb::ValueCmpOp::Eq,
        RelOp::Ne => pb::ValueCmpOp::Ne,
        RelOp::Lt => pb::ValueCmpOp::Lt,
        RelOp::Le => pb::ValueCmpOp::Le,
        RelOp::Gt => pb::ValueCmpOp::Gt,
        RelOp::Ge => pb::ValueCmpOp::Ge,
        RelOp::In => unreachable!("the value parser refuses `in` before building it"),
    };
    Ok((
        value_of(V::Compare(Box::new(pb::ValueCompare {
            left: Some(Box::new(le)),
            right: Some(Box::new(re)),
            op: wire_op as i32,
        }))),
        Some(VType::Bool),
    ))
}

/// The conditional: a boolean condition over two branches of one type.
fn compile_ternary(
    c: &Ast,
    t: &Ast,
    e: &Ast,
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;

    let (ce, ct) = compile_value_ast(c, depth + 1)?;
    if let Some(vt) = ct {
        if vt != VType::Bool {
            return Err(refuse(format!(
                "the ternary's condition is a {}; a condition is a boolean",
                vt.name()
            )));
        }
    }
    let (te, tt) = compile_value_ast(t, depth + 1)?;
    let (ee, et) = compile_value_ast(e, depth + 1)?;
    for bt in [tt, et].into_iter().flatten() {
        if bt == VType::Str {
            return Err(refuse(
                "a string literal is not a computable value; a string-valued \
                 ternary reads facet columns in both branches",
            ));
        }
    }
    let vt = match (tt, et) {
        (Some(a), Some(b)) if a != b => {
            let fix = if matches!(
                (a, b),
                (VType::Int, VType::Double) | (VType::Double, VType::Int)
            ) {
                "; stock CEL does not coerce — convert explicitly with double()"
            } else {
                ""
            };
            return Err(refuse(format!(
                "the ternary's branches disagree: {} versus {}{fix}",
                a.name(),
                b.name()
            )));
        }
        (Some(a), _) | (_, Some(a)) => Some(a),
        (None, None) => None,
    };
    Ok((
        value_of(V::Ternary(Box::new(pb::ValueTernary {
            cond: Some(Box::new(ce)),
            then_value: Some(Box::new(te)),
            else_value: Some(Box::new(ee)),
        }))),
        vt,
    ))
}
