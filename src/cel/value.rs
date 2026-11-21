//! The value compiler: [`Ast`] to [`pb::ValueExpr`], typed as far as
//! literals determine.

use super::CelError;

use super::ast::{Ast, VOp};
use super::predicate::int_literal;
use super::refuse;
use super::value_logic::compile_value_tail;
use super::value_math::compile_math_call;
use crate::pb;

/// Static type of a value expression as far as literals determine it.
/// `None` means "depends on a column", resolved (and type-checked)
/// per shard against its own tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VType {
    Int,
    Uint,
    Double,
    /// A comparison, Kleene logic, `!`, or a bool literal.
    Bool,
    /// A string literal: legal only as a `==`/`!=` operand.
    Str,
}

impl VType {
    pub(super) fn name(self) -> &'static str {
        match self {
            VType::Int => "int",
            VType::Uint => "uint",
            VType::Double => "double",
            VType::Bool => "bool",
            VType::Str => "string",
        }
    }
}

/// Depth cap for value expressions, same defensive posture as the
/// filter tree caps.
const VALUE_MAX_DEPTH: usize = 32;

pub(super) fn value_of(expr: pb::value_expr::Expr) -> pb::ValueExpr {
    pb::ValueExpr { expr: Some(expr) }
}

pub(super) fn compile_value_ast(
    ast: &Ast,
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;
    if depth > VALUE_MAX_DEPTH {
        return Err(refuse(format!(
            "value expression nests deeper than {VALUE_MAX_DEPTH} levels"
        )));
    }
    match ast {
        Ast::Ident(name) => match name.as_str() {
            "true" => Ok((value_of(V::BoolLiteral(true)), Some(VType::Bool))),
            "false" => Ok((value_of(V::BoolLiteral(false)), Some(VType::Bool))),
            "null" => Err(refuse(
                "null is not a value; an absent input already propagates to an absent result",
            )),
            _ => Ok((value_of(V::Column(name.clone())), None)),
        },
        Ast::MapAccess(col, key) => Ok((
            value_of(V::Map(pb::MapRead {
                column: col.clone(),
                key: key.clone(),
            })),
            None,
        )),
        Ast::Int(v) => Ok((value_of(V::IntLiteral(int_literal(*v)?)), Some(VType::Int))),
        Ast::Uint(v) => Ok((value_of(V::UintLiteral(*v)), Some(VType::Uint))),
        Ast::Float(v) => {
            if !v.is_finite() {
                return Err(refuse(format!(
                    "float literal {v} is not finite; no column holds NaN or infinity"
                )));
            }
            Ok((value_of(V::FloatLiteral(*v)), Some(VType::Double)))
        }

        Ast::Neg(inner) => compile_negate(inner, depth),
        Ast::Arith(l, op, r) => compile_arith(l, op, r, depth),

        Ast::Call(name, args) => {
            // Namespaced math calls parse as method syntax: the
            // receiver is the namespace identifier (`math.abs(x)` is
            // Call("abs", [Ident("math"), x])). The namespaces are
            // reserved: a column named "math" or "engine" still READS
            // (no parentheses), it just cannot be a call receiver.
            if let Some(Ast::Ident(ns)) = args.first() {
                if matches!(ns.as_str(), "math" | "engine" | "hash" | "calendar") {
                    return compile_math_call(ns, name, &args[1..], depth);
                }
            }
            compile_plain_call(name, args, depth)
        }
        other => compile_value_tail(other, depth),
    }
}

fn compile_negate(inner: &Ast, depth: usize) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;

    let (expr, vt) = compile_value_ast(inner, depth + 1)?;
    match vt {
        Some(VType::Uint) => return Err(refuse("unary minus over a uint")),
        Some(VType::Bool) => {
            return Err(refuse(
                "unary minus over a boolean; booleans negate with `!`",
            ));
        }
        Some(VType::Str) => {
            return Err(refuse("unary minus over a string literal"));
        }
        _ => {}
    }
    Ok((value_of(V::Negate(Box::new(expr))), vt))
}

/// Binary arithmetic: both operands numeric and, where literals pin a
/// type, the same type.
fn compile_arith(
    l: &Ast,
    op: &VOp,
    r: &Ast,
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;

    let (le, lt) = compile_value_ast(l, depth + 1)?;
    let (re, rt) = compile_value_ast(r, depth + 1)?;
    for t in [lt, rt].into_iter().flatten() {
        match t {
            VType::Bool => {
                return Err(refuse(format!(
                    "`{}` over a boolean; a boolean joins no arithmetic — wrap \
                     it in a ternary that yields a number",
                    op.name()
                )));
            }
            VType::Str => {
                return Err(refuse(
                    "a string joins no arithmetic; string literals appear only \
                     as `==`/`!=` comparison operands",
                ));
            }
            VType::Int | VType::Uint | VType::Double => {}
        }
    }
    let vt = match (lt, rt) {
        (Some(a), Some(b)) if a != b => {
            return Err(refuse(format!(
                "`{}` mixes numeric types; stock CEL does not coerce — \
                 convert explicitly with double()",
                op.name()
            )));
        }
        (Some(a), _) | (_, Some(a)) => Some(a),
        (None, None) => None,
    };
    if *op == VOp::Mod && vt == Some(VType::Double) {
        return Err(refuse(
            "`%` is integer-only in CEL; there is no double remainder",
        ));
    }
    let wire_op = match op {
        VOp::Add => pb::ArithOp::Add,
        VOp::Sub => pb::ArithOp::Sub,
        VOp::Mul => pb::ArithOp::Mul,
        VOp::Div => pb::ArithOp::Div,
        VOp::Mod => pb::ArithOp::Mod,
    };
    Ok((
        value_of(V::Arith(Box::new(pb::ValueArith {
            left: Some(Box::new(le)),
            right: Some(Box::new(re)),
            op: wire_op as i32,
        }))),
        vt,
    ))
}

/// The non-namespaced calls: `double()` compiles, the rest refuse by
/// name.
fn compile_plain_call(
    name: &str,
    args: &[Ast],
    depth: usize,
) -> Result<(pb::ValueExpr, Option<VType>), CelError> {
    use pb::value_expr::Expr as V;
    match name {
        "double" => {
            if args.len() != 1 {
                return Err(refuse("double() takes exactly one argument"));
            }
            let (inner, vt) = compile_value_ast(&args[0], depth + 1)?;
            if matches!(vt, Some(VType::Bool) | Some(VType::Str)) {
                return Err(refuse(format!(
                    "double() over a {}; only numbers convert",
                    vt.unwrap().name()
                )));
            }
            Ok((value_of(V::ToDouble(Box::new(inner))), Some(VType::Double)))
        }
        "int" | "uint" => Err(refuse(format!(
            "{name}() conversion does not compile; compute in double, or store \
                 the value as an integer column"
        ))),
        "timestamp" => Err(refuse(
            "timestamp() is a filter literal; a projection over an i64 date column \
                 already reads epoch micros",
        )),
        "has" => Err(refuse(
            "has() is a predicate; an absent input already propagates to an \
                 absent result",
        )),
        "stable_key" => Err(refuse(
            "stable_key() is the document's opaque routing key, readable only as \
             hash.fnv64(stable_key()) in a derived column (docs/derived-columns.md)",
        )),
        "matches" | "contains" | "startsWith" | "endsWith" | "size" => Err(refuse(format!(
            "{name}() does not compile to a value; string functions are not in \
                 the engine's vocabulary"
        ))),
        other => Err(refuse(format!(
            "unknown function {other}() in a value expression; the vocabulary is \
                 arithmetic, the conditional layer, double(), math.*, engine.*, \
                 calendar.*, and hash.fnv64()"
        ))),
    }
}
