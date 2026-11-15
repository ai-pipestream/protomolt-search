//! The predicate compiler: [`Ast`] to [`pb::FilterExpr`].

use tonic::Status;

use super::ast::{Ast, RelOp};
use super::calls::{compile_call, compile_in};
use super::refuse;
use super::timestamp::timestamp_argument;
use crate::pb;

/// What one side of a relation is, after literal folding.
pub(super) enum Side {
    Column(String),
    MapAccess(String, String),
    Str(String),
    Int(i64),
    Uint(u64),
    Float(f64),
    List(Vec<Ast>),
}

pub(super) fn wrap(expr: pb::filter_expr::Expr) -> pb::FilterExpr {
    pb::FilterExpr { expr: Some(expr) }
}

pub(super) fn not_of(inner: pb::FilterExpr) -> pb::FilterExpr {
    wrap(pb::filter_expr::Expr::Not(Box::new(inner)))
}

fn bound(value: pb::filter_bound::Value, exclusive: bool) -> pb::FilterBound {
    pb::FilterBound {
        value: Some(value),
        exclusive,
    }
}

pub(super) fn compile(ast: &Ast) -> Result<pb::FilterExpr, Status> {
    match ast {
        Ast::Or(children) => Ok(wrap(pb::filter_expr::Expr::Or(pb::FilterList {
            exprs: children.iter().map(compile).collect::<Result<_, _>>()?,
        }))),
        Ast::And(children) => Ok(wrap(pb::filter_expr::Expr::And(pb::FilterList {
            exprs: children.iter().map(compile).collect::<Result<_, _>>()?,
        }))),
        Ast::Not(inner) => Ok(not_of(compile(inner)?)),
        Ast::Rel(l, op, r) => compile_relation(l, *op, r),
        Ast::Call(name, args) => compile_call(name, args),
        Ast::Ident(name) => match name.as_str() {
            "true" | "false" => Err(refuse(
                "boolean literals do not compile; the engine has no boolean columns — \
                 a facet holding \"true\" compares as the string",
            )),
            "null" => Err(refuse(
                "null does not compile; absence is tested with has(column)",
            )),
            _ => Err(refuse(format!(
                "bare column {name:?} is not a predicate; write has({name}) or compare \
                 it against a value"
            ))),
        },
        Ast::MapAccess(col, key) => Err(refuse(format!(
            "bare map access {col}[{key:?}] is not a predicate; compare it against a \
             value, or test the key with '{key}' in {col}"
        ))),
        Ast::Int(_) | Ast::Uint(_) | Ast::Float(_) | Ast::Str(_) => {
            Err(refuse("a bare literal is not a predicate"))
        }
        Ast::List(_) => Err(refuse(
            "a bare list is not a predicate; lists appear on the right of `in`",
        )),
        Ast::Arith(..) | Ast::Neg(_) => {
            unreachable!("the predicate parser refuses arithmetic before building it")
        }
        Ast::Ternary(..) => {
            unreachable!("the predicate parser refuses the ternary before building it")
        }
    }
}

/// Fold one relation side into a [`Side`], resolving `timestamp()`
/// calls into their epoch-micros integer.
pub(super) fn side_of(ast: &Ast) -> Result<Side, Status> {
    match ast {
        Ast::Ident(name) => match name.as_str() {
            "true" | "false" => Err(refuse(
                "boolean literals do not compile; the engine has no boolean columns — \
                 a facet holding \"true\" compares as the string",
            )),
            "null" => Err(refuse(
                "null comparisons do not compile; absence is tested with has(column) \
                 and its negation",
            )),
            _ => Ok(Side::Column(name.clone())),
        },
        Ast::MapAccess(col, key) => Ok(Side::MapAccess(col.clone(), key.clone())),
        Ast::Str(v) => Ok(Side::Str(v.clone())),
        Ast::Int(v) => Ok(Side::Int(int_literal(*v)?)),
        Ast::Uint(v) => Ok(Side::Uint(*v)),
        Ast::Float(v) => Ok(Side::Float(*v)),
        Ast::List(items) => Ok(Side::List(items.clone())),
        Ast::Call(name, args) if name == "timestamp" => Ok(Side::Int(timestamp_argument(args)?)),
        // Any other call in value position: let the vocabulary compiler
        // name the refusal (int() conversion, matches(), ...); a call
        // it WOULD accept is a predicate sitting where a value belongs.
        Ast::Call(name, args) => match compile_call(name, args) {
            Err(e) => Err(e),
            Ok(_) => Err(refuse(format!(
                "{name}() is a predicate, not a comparable value"
            ))),
        },
        Ast::Or(_) | Ast::And(_) | Ast::Not(_) | Ast::Rel(..) => Err(refuse(
            "a boolean expression cannot be a comparison operand; relations do not nest",
        )),
        Ast::Arith(..) | Ast::Neg(_) => {
            unreachable!("the predicate parser refuses arithmetic before building it")
        }
        Ast::Ternary(..) => {
            unreachable!("the predicate parser refuses the ternary before building it")
        }
    }
}

pub(super) fn int_literal(v: i128) -> Result<i64, Status> {
    i64::try_from(v).map_err(|_| refuse(format!("integer literal {v} is out of i64 range")))
}

fn compile_relation(l: &Ast, op: RelOp, r: &Ast) -> Result<pb::FilterExpr, Status> {
    let (lhs, rhs) = (side_of(l)?, side_of(r)?);
    match op {
        RelOp::In => compile_in(lhs, rhs),
        _ => {
            // Orient column-vs-literal, mirroring the operator when the
            // literal is on the left.
            let (col, op, lit) = match (&lhs, &rhs) {
                (Side::Column(_) | Side::MapAccess(..), Side::Column(_) | Side::MapAccess(..)) => {
                    return Err(refuse(
                        "cross-column comparison does not compile to a dictionary \
                         predicate; each side of a relation is one column and one value",
                    ));
                }
                (Side::Column(_) | Side::MapAccess(..), _) => (&lhs, op, &rhs),
                (_, Side::Column(_) | Side::MapAccess(..)) => (&rhs, op.mirrored(), &lhs),
                _ => {
                    return Err(refuse(
                        "constant comparison (neither side is a column) does not \
                         compile; a predicate reads a column",
                    ));
                }
            };
            match lit {
                Side::Str(v) => compile_string_relation(col, op, v),
                Side::Int(v) => compile_number_relation(col, op, pb::filter_bound::Value::Int(*v)),
                Side::Uint(v) => {
                    compile_number_relation(col, op, pb::filter_bound::Value::Uint(*v))
                }
                Side::Float(v) => {
                    if !v.is_finite() {
                        return Err(refuse(format!(
                            "float literal {v} is not finite; NaN and infinity bound \
                             nothing a column can hold"
                        )));
                    }
                    compile_number_relation(col, op, pb::filter_bound::Value::Num(*v))
                }
                Side::List(_) => Err(refuse(format!(
                    "`{}` against a list does not compile; membership is `in`",
                    op.name()
                ))),
                Side::Column(_) | Side::MapAccess(..) => unreachable!("matched above"),
            }
        }
    }
}

fn compile_string_relation(col: &Side, op: RelOp, value: &str) -> Result<pb::FilterExpr, Status> {
    let eq = match col {
        Side::Column(name) => wrap(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
            column: name.clone(),
            values: vec![value.to_string()],
        })),
        Side::MapAccess(name, key) => {
            wrap(pb::filter_expr::Expr::MapFacet(pb::MapFacetPredicate {
                column: name.clone(),
                key: key.clone(),
                values: vec![value.to_string()],
            }))
        }
        _ => unreachable!("callers pass a column side"),
    };
    let (column, key) = match col {
        Side::Column(name) => (name.clone(), String::new()),
        Side::MapAccess(name, key) => (name.clone(), key.clone()),
        _ => unreachable!("callers pass a column side"),
    };
    // String ordering compiles to a dictionary ordinal range:
    // dictionaries are written in byte order,
    // which is the order stock CEL compares strings in.
    let bound = |exclusive: bool| {
        Some(pb::StringBound {
            value: value.to_string(),
            exclusive,
        })
    };
    let range = |min: Option<pb::StringBound>, max: Option<pb::StringBound>| {
        let predicate = pb::StringRangePredicate {
            column: column.clone(),
            key: key.clone(),
            min,
            max,
        };
        // Preserve the legacy encoding where it is unambiguous. An empty
        // map key needs its own oneof variant so older readers refuse it.
        wrap(
            if matches!(col, Side::MapAccess(_, key) if key.is_empty()) {
                pb::filter_expr::Expr::MapStringRange(predicate)
            } else {
                pb::filter_expr::Expr::StringRange(predicate)
            },
        )
    };
    match op {
        RelOp::Eq => Ok(eq),
        RelOp::Ne => Ok(not_of(eq)),
        RelOp::Lt => Ok(range(None, bound(true))),
        RelOp::Le => Ok(range(None, bound(false))),
        RelOp::Gt => Ok(range(bound(true), None)),
        RelOp::Ge => Ok(range(bound(false), None)),
        RelOp::In => unreachable!("`in` is compiled by compile_in"),
    }
}

pub(super) fn compile_number_relation(
    col: &Side,
    op: RelOp,
    value: pb::filter_bound::Value,
) -> Result<pb::FilterExpr, Status> {
    let (min, max) = match op {
        RelOp::Eq | RelOp::Ne => (Some(bound(value, false)), Some(bound(value, false))),
        RelOp::Gt => (Some(bound(value, true)), None),
        RelOp::Ge => (Some(bound(value, false)), None),
        RelOp::Lt => (None, Some(bound(value, true))),
        RelOp::Le => (None, Some(bound(value, false))),
        RelOp::In => unreachable!("in is compiled separately"),
    };
    let leaf = match col {
        Side::Column(name) => wrap(pb::filter_expr::Expr::Number(pb::NumberPredicate {
            column: name.clone(),
            min,
            max,
        })),
        Side::MapAccess(name, key) => wrap(pb::filter_expr::Expr::TypedMapNumber(
            pb::MapNumberPredicate {
                column: name.clone(),
                key: key.clone(),
                min,
                max,
            },
        )),
        _ => unreachable!("callers pass a column side"),
    };
    Ok(if op == RelOp::Ne { not_of(leaf) } else { leaf })
}
