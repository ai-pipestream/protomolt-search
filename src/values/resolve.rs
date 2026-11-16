//! Resolution of a compiled expression against one shard's tables, which
//! finishes the type check.

use super::ValuesError;

use super::lookup::ColumnLookup;
use super::refuse;
use super::resolve_call::{resolve_func, resolve_logic, resolve_ternary};
use super::resolve_compare::resolve_compare;
use super::types::{Op, ResolvedValue, ValueType};
use crate::pb;

/// Resolve one compiled expression against a shard's tables, finishing
/// the type check. Errors here are the caller's (a type conflict the
/// literals could not pin down at compile time, an ambiguous name, a
/// string inside arithmetic), so they refuse the request by name.
pub fn resolve(
    expr: &pb::ValueExpr,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    use pb::value_expr::Expr as V;
    match expr
        .expr
        .as_ref()
        .ok_or_else(|| refuse("empty ValueExpr node"))?
    {
        V::Column(name) => resolve_column(name, cols),
        V::Map(read) | V::TypedMap(read) => {
            resolve_map(read, matches!(expr.expr, Some(V::TypedMap(_))), cols)
        }

        V::IntLiteral(v) => Ok((ResolvedValue::ConstInt(*v), ValueType::Int)),
        V::UintLiteral(v) => Ok((ResolvedValue::ConstUint(*v), ValueType::Uint)),
        V::FloatLiteral(v) => Ok((ResolvedValue::ConstDouble(*v), ValueType::Double)),
        V::StableKeyHash(_) => {
            if !cols.at_ingest() {
                return Err(refuse(
                    "hash.fnv64(stable_key()) computes at ingest, where the document's \
                     stable routing key is known; at query time read the derived column \
                     that stores it (docs/derived-columns.md)",
                ));
            }
            Ok((ResolvedValue::StableKeyHash, ValueType::Uint))
        }

        V::ToDouble(inner) => resolve_to_double(inner, cols),
        V::Negate(inner) => resolve_negate(inner, cols),
        V::Arith(arith) => resolve_arith(arith, cols),

        V::BoolLiteral(v) => Ok((ResolvedValue::ConstBool(*v), ValueType::Bool)),
        V::StringLiteral(_) => Err(refuse(
            "a string literal is not a value outside a `==`/`!=` comparison",
        )),

        V::Compare(cmp) => resolve_compare(cmp, cols),
        V::Func(func) => resolve_func(func, cols),
        V::Logic(logic) => resolve_logic(logic, cols),

        V::Not(inner) => {
            let (rv, vt) = resolve(inner, cols)?;
            if !matches!(vt, ValueType::Bool | ValueType::Unknown) {
                return Err(refuse(format!(
                    "`!` over a {}; Kleene logic joins booleans",
                    vt.name()
                )));
            }
            Ok((ResolvedValue::Not(Box::new(rv)), ValueType::Bool))
        }

        V::Ternary(t) => resolve_ternary(t, cols),
    }
}

/// A plain column read, in whichever single family holds the name.
fn resolve_column(
    name: &str,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let num = cols.numeric_index(name);
    let int = cols.integer_index(name);
    let uint = cols.unsigned_integer_index(name);
    let facet = cols.facet_index(name);
    let families = usize::from(num.is_some())
        + usize::from(int.is_some())
        + usize::from(uint.is_some())
        + usize::from(facet.is_some());
    if families > 1 {
        return Err(refuse(format!(
            "column {name:?} exists in more than one family on this shard; \
             a value read cannot pick one"
        )));
    }
    if let Some(ni) = num {
        Ok((ResolvedValue::NumCol(ni), ValueType::Double))
    } else if let Some(ii) = int {
        Ok((ResolvedValue::IntCol(ii), ValueType::Int))
    } else if let Some(ui) = uint {
        Ok((ResolvedValue::UintCol(ui), ValueType::Uint))
    } else if let Some(fi) = facet {
        Ok((ResolvedValue::FacetCol(fi), ValueType::Str))
    } else {
        Ok((ResolvedValue::Absent, ValueType::Unknown))
    }
}

/// A map entry read. A typed read also sees the exact integer planes, and
/// keeps the column's static type when the key is missing.
fn resolve_map(
    read: &pb::MapRead,
    typed: bool,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let num = cols.map_numeric_index(&read.column);
    let facet = cols.map_facet_index(&read.column);
    let int = cols.map_integer_index(&read.column).filter(|_| typed);
    let uint = cols
        .map_unsigned_integer_index(&read.column)
        .filter(|_| typed);
    if [num, facet, int, uint]
        .iter()
        .filter(|c| c.is_some())
        .count()
        > 1
    {
        return Err(refuse(format!(
            "map column {:?} exists in multiple map families on this shard; \
             a value read cannot pick one",
            read.column
        )));
    }
    if let Some(ci) = int {
        Ok((
            ResolvedValue::MapInt {
                column: ci,
                key_ord: cols.map_integer_key_ord(ci, &read.key),
            },
            ValueType::Int,
        ))
    } else if let Some(ci) = uint {
        Ok((
            ResolvedValue::MapUint {
                column: ci,
                key_ord: cols.map_unsigned_integer_key_ord(ci, &read.key),
            },
            ValueType::Uint,
        ))
    } else if let Some(ci) = num {
        match cols.map_numeric_key_ord(ci, &read.key) {
            Some(key_ord) => Ok((
                ResolvedValue::MapNum {
                    column: ci,
                    key_ord,
                },
                ValueType::Double,
            )),
            None => Ok((
                ResolvedValue::Absent,
                if typed {
                    ValueType::Double
                } else {
                    ValueType::Unknown
                },
            )),
        }
    } else if let Some(ci) = facet {
        match cols.map_facet_key_ord(ci, &read.key) {
            Some(key_ord) => Ok((
                ResolvedValue::MapFacet {
                    column: ci,
                    key_ord,
                },
                ValueType::Str,
            )),
            None => Ok((
                ResolvedValue::Absent,
                if typed {
                    ValueType::Str
                } else {
                    ValueType::Unknown
                },
            )),
        }
    } else {
        Ok((ResolvedValue::Absent, ValueType::Unknown))
    }
}

fn resolve_to_double(
    inner: &pb::ValueExpr,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let (rv, vt) = resolve(inner, cols)?;
    match vt {
        ValueType::Str => {
            return Err(refuse(
                "double() over a string column; strings do not convert",
            ));
        }
        ValueType::Bool => {
            return Err(refuse("double() over a bool; only numbers convert"));
        }
        _ => {}
    }
    Ok((ResolvedValue::ToDouble(Box::new(rv)), ValueType::Double))
}

fn resolve_negate(
    inner: &pb::ValueExpr,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let (rv, vt) = resolve(inner, cols)?;
    match vt {
        ValueType::Uint => return Err(refuse("unary minus over a uint")),
        ValueType::Str => return Err(refuse("unary minus over a string column")),
        ValueType::Bool => {
            return Err(refuse(
                "unary minus over a boolean; booleans negate with `!`",
            ));
        }
        _ => {}
    }
    Ok((ResolvedValue::Negate(Box::new(rv)), vt))
}

fn resolve_arith(
    arith: &pb::ValueArith,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), ValuesError> {
    let left = arith
        .left
        .as_ref()
        .ok_or_else(|| refuse("arithmetic node without a left operand"))?;
    let right = arith
        .right
        .as_ref()
        .ok_or_else(|| refuse("arithmetic node without a right operand"))?;
    let (lrv, lt) = resolve(left, cols)?;
    let (rrv, rt) = resolve(right, cols)?;
    if lt == ValueType::Str || rt == ValueType::Str {
        return Err(refuse(
            "a string column joins no arithmetic; string projections are \
             bare column reads",
        ));
    }
    if lt == ValueType::Bool || rt == ValueType::Bool {
        return Err(refuse(
            "a boolean joins no arithmetic; wrap it in a ternary that \
             yields a number",
        ));
    }
    let op = match pb::ArithOp::try_from(arith.op) {
        Ok(pb::ArithOp::Add) => Op::Add,
        Ok(pb::ArithOp::Sub) => Op::Sub,
        Ok(pb::ArithOp::Mul) => Op::Mul,
        Ok(pb::ArithOp::Div) => Op::Div,
        Ok(pb::ArithOp::Mod) => Op::Mod,
        _ => return Err(refuse("arithmetic node with an unknown operation")),
    };
    let vt = match (lt, rt) {
        (ValueType::Int, ValueType::Int) => ValueType::Int,
        (ValueType::Uint, ValueType::Uint) => ValueType::Uint,
        (ValueType::Double, ValueType::Double) => ValueType::Double,
        (ValueType::Unknown, other) | (other, ValueType::Unknown) => {
            // One side can never hold a value, so the result
            // is always absent; the other side's type is moot.
            let _ = other;
            ValueType::Unknown
        }
        _ => {
            return Err(refuse(
                "arithmetic mixes numeric types; stock CEL \
                 does not coerce — convert explicitly with double()",
            ));
        }
    };
    if op == Op::Mod && vt == ValueType::Double {
        return Err(refuse(
            "`%` is integer-only in CEL; there is no double remainder",
        ));
    }
    Ok((
        ResolvedValue::Arith {
            op,
            left: Box::new(lrv),
            right: Box::new(rrv),
        },
        vt,
    ))
}
