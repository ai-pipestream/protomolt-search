//! Compiled value expressions on the serving path
//! (`docs/cel-values.md`): the resolution and evaluation half of the
//! CEL value dialect, the sibling of `src/filter.rs`.
//!
//! The coordinator compiles expression text once (`cel::compile_value`)
//! into the [`crate::pb::ValueExpr`] IR. Each shard resolves column
//! names against its OWN dictionaries — a name this shard's tables lack
//! is ABSENT for every document here, exactly the filter rule — and
//! finishes the type check stock CEL's rules demand: int with int,
//! double with double, mixed refused naming `double()`, strings joining
//! no arithmetic. Evaluation is then array reads and register
//! arithmetic per RETURNED hit; nothing is interpreted, and nothing
//! runs per candidate.
//!
//! Absence semantics are the engine's Kleene rule extended to values:
//! an absent input makes the result absent, and the integer operations
//! stock CEL calls ERRORS — overflow, division by zero, `i64::MIN`
//! edge cases — also evaluate to absent rather than failing the query.
//! That deviation is documented and pinned in tests; everywhere stock
//! CEL yields a value, this module yields the same value (the
//! differential oracle in `tests/cel_values.rs` holds it there).

use std::collections::HashMap;

use tonic::Status;

use crate::pb;
use crate::scorefn::NumericRead;

/// One refusal, uniformly shaped like the compiler's.
fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(format!("projection: {}", msg.into()))
}

/// Column lookup surface a shard provides to resolution — name to
/// table index per family, plus map key dictionaries. Implemented by
/// the node's shard wrapper over both the heap store and the mmap
/// reader.
pub trait ColumnLookup {
    /// f64 column index for `name`.
    fn numeric_index(&self, name: &str) -> Option<usize>;
    /// i64 column index for `name`.
    fn integer_index(&self, name: &str) -> Option<usize>;
    /// Facet column index for `name`.
    fn facet_index(&self, name: &str) -> Option<usize>;
    /// Map-numeric column index for `name`.
    fn map_numeric_index(&self, name: &str) -> Option<usize>;
    /// Key ordinal in map-numeric column `ci`'s key dictionary.
    fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32>;
    /// Map-facet column index for `name`.
    fn map_facet_index(&self, name: &str) -> Option<usize>;
    /// Key ordinal in map-facet column `ci`'s key dictionary.
    fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32>;
    /// Value ordinal of `value` in facet column `ci`'s dictionary.
    fn facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32>;
    /// Value ordinal of `value` in map-facet column `ci`'s value
    /// dictionary.
    fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32>;
}

/// Static type of a resolved value expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    /// CEL int (i64 columns, int literals).
    Int,
    /// CEL double (f64 and map-numeric columns, double literals,
    /// `double()`).
    Double,
    /// A facet or map-facet read: legal as the WHOLE expression, as a
    /// `==`/`!=` operand against a string literal, and as a ternary
    /// branch.
    Str,
    /// A comparison, Kleene logic, `!`, or a bool literal.
    Bool,
    /// Every column reference is missing from this shard: absent for
    /// every document, joins anything, never evaluates.
    Unknown,
}

impl ValueType {
    fn name(self) -> &'static str {
        match self {
            ValueType::Int => "int",
            ValueType::Double => "double",
            ValueType::Str => "string",
            ValueType::Bool => "bool",
            ValueType::Unknown => "unknown",
        }
    }
}

/// Comparison op, decoded once at resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    /// `==`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

/// Arithmetic op, decoded once at resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%` (integers only, like CEL)
    Mod,
}

/// A value expression resolved against ONE shard's tables.
#[derive(Debug)]
pub enum ResolvedValue {
    /// f64 column read.
    NumCol(usize),
    /// i64 column read.
    IntCol(usize),
    /// Facet column read (whole-expression string).
    FacetCol(usize),
    /// Map-numeric entry read.
    MapNum {
        /// Map-numeric column index.
        column: usize,
        /// Key ordinal in that column's key dictionary.
        key_ord: u32,
    },
    /// Map-facet entry read (whole-expression string).
    MapFacet {
        /// Map-facet column index.
        column: usize,
        /// Key ordinal in that column's key dictionary.
        key_ord: u32,
    },
    /// A reference this shard cannot resolve: absent everywhere.
    Absent,
    /// Int literal.
    ConstInt(i64),
    /// Double literal.
    ConstDouble(f64),
    /// Binary arithmetic; operand types agree by construction.
    Arith {
        /// The operation.
        op: Op,
        /// Left operand.
        left: Box<ResolvedValue>,
        /// Right operand.
        right: Box<ResolvedValue>,
    },
    /// CEL double(x).
    ToDouble(Box<ResolvedValue>),
    /// Unary minus.
    Negate(Box<ResolvedValue>),
    /// Bool literal.
    ConstBool(bool),
    /// Numeric or bool comparison; operand types agree by
    /// construction.
    Compare {
        /// The operation.
        op: CmpOp,
        /// Left operand.
        left: Box<ResolvedValue>,
        /// Right operand.
        right: Box<ResolvedValue>,
    },
    /// A facet or map-facet read compared against a string literal,
    /// pre-resolved to this shard's value dictionary. `lit_ord` is
    /// `None` when the dictionary lacks the literal, so a PRESENT
    /// value can never equal it (an absent value stays absent).
    CompareStr {
        /// True for `!=`.
        ne: bool,
        /// The facet or map-facet read (or Absent on this shard).
        read: Box<ResolvedValue>,
        /// The literal's ordinal in the read column's dictionary.
        lit_ord: Option<u32>,
    },
    /// Kleene conjunction (`and` true) or disjunction.
    Logic {
        /// `&&` when true, `||` when false.
        and: bool,
        /// Bool operands in source order.
        children: Vec<ResolvedValue>,
    },
    /// Logical not.
    Not(Box<ResolvedValue>),
    /// The conditional; only the taken branch evaluates.
    Ternary {
        /// Bool condition.
        cond: Box<ResolvedValue>,
        /// Value when true.
        then: Box<ResolvedValue>,
        /// Value when false.
        otherwise: Box<ResolvedValue>,
    },
}

/// One evaluated value. Strings stay as (column, ordinal) so the
/// caller renders them against the dictionary it owns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Val {
    /// CEL int.
    Int(i64),
    /// CEL double.
    Double(f64),
    /// A facet value ordinal in facet column `column`.
    FacetOrd {
        /// Facet column index.
        column: usize,
        /// Value ordinal in that column's dictionary.
        ord: u32,
    },
    /// A map-facet value ordinal in map-facet column `column`.
    MapFacetOrd {
        /// Map-facet column index.
        column: usize,
        /// Value ordinal in that column's value dictionary.
        ord: u32,
    },
    /// CEL bool.
    Bool(bool),
}

/// Resolve one compiled expression against a shard's tables, finishing
/// the type check. Errors here are the caller's (a type conflict the
/// literals could not pin down at compile time, an ambiguous name, a
/// string inside arithmetic), so they refuse the request by name.
pub fn resolve(
    expr: &pb::ValueExpr,
    cols: &dyn ColumnLookup,
) -> Result<(ResolvedValue, ValueType), Status> {
    use pb::value_expr::Expr as V;
    match expr.expr.as_ref().ok_or_else(|| refuse("empty ValueExpr node"))? {
        V::Column(name) => {
            let num = cols.numeric_index(name);
            let int = cols.integer_index(name);
            let facet = cols.facet_index(name);
            let families = usize::from(num.is_some())
                + usize::from(int.is_some())
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
            } else if let Some(fi) = facet {
                Ok((ResolvedValue::FacetCol(fi), ValueType::Str))
            } else {
                Ok((ResolvedValue::Absent, ValueType::Unknown))
            }
        }
        V::Map(read) => {
            let num = cols.map_numeric_index(&read.column);
            let facet = cols.map_facet_index(&read.column);
            if num.is_some() && facet.is_some() {
                return Err(refuse(format!(
                    "map column {:?} exists in both map families on this shard; \
                     a value read cannot pick one",
                    read.column
                )));
            }
            if let Some(ci) = num {
                match cols.map_numeric_key_ord(ci, &read.key) {
                    Some(key_ord) => {
                        Ok((ResolvedValue::MapNum { column: ci, key_ord }, ValueType::Double))
                    }
                    None => Ok((ResolvedValue::Absent, ValueType::Unknown)),
                }
            } else if let Some(ci) = facet {
                match cols.map_facet_key_ord(ci, &read.key) {
                    Some(key_ord) => {
                        Ok((ResolvedValue::MapFacet { column: ci, key_ord }, ValueType::Str))
                    }
                    None => Ok((ResolvedValue::Absent, ValueType::Unknown)),
                }
            } else {
                Ok((ResolvedValue::Absent, ValueType::Unknown))
            }
        }
        V::IntLiteral(v) => Ok((ResolvedValue::ConstInt(*v), ValueType::Int)),
        V::FloatLiteral(v) => Ok((ResolvedValue::ConstDouble(*v), ValueType::Double)),
        V::ToDouble(inner) => {
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
        V::Negate(inner) => {
            let (rv, vt) = resolve(inner, cols)?;
            match vt {
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
        V::Arith(arith) => {
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
                (ValueType::Double, ValueType::Double) => ValueType::Double,
                (ValueType::Unknown, other) | (other, ValueType::Unknown) => {
                    // One side can never hold a value, so the result
                    // is always absent; the other side's type is moot.
                    let _ = other;
                    ValueType::Unknown
                }
                _ => {
                    return Err(refuse(
                        "arithmetic mixes an int column and a double column; stock CEL \
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
        V::BoolLiteral(v) => Ok((ResolvedValue::ConstBool(*v), ValueType::Bool)),
        V::StringLiteral(_) => Err(refuse(
            "a string literal is not a value outside a `==`/`!=` comparison",
        )),
        V::Compare(cmp) => {
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
            let lit_of = |e: &pb::ValueExpr| match e.expr.as_ref() {
                Some(V::StringLiteral(v)) => Some(v.clone()),
                _ => None,
            };
            match (lit_of(left), lit_of(right)) {
                (Some(a), Some(b)) => {
                    if ordering {
                        return Err(string_ordering(op));
                    }
                    let eq = a == b;
                    Ok((
                        ResolvedValue::ConstBool(if op == CmpOp::Ne { !eq } else { eq }),
                        ValueType::Bool,
                    ))
                }
                (Some(lit), None) | (None, Some(lit)) => {
                    if ordering {
                        return Err(string_ordering(op));
                    }
                    let read_pb = if lit_of(left).is_some() { right } else { left };
                    let (rv, vt) = resolve(read_pb, cols)?;
                    let lit_ord = match &rv {
                        ResolvedValue::FacetCol(fi) => cols.facet_value_ord_of(*fi, &lit),
                        ResolvedValue::MapFacet { column, .. } => {
                            cols.map_facet_value_ord_of(*column, &lit)
                        }
                        ResolvedValue::Absent => None,
                        _ => {
                            return Err(refuse(format!(
                                "a string literal compares only against a direct facet \
                                 or map-facet read, not a {} expression",
                                vt.name()
                            )));
                        }
                    };
                    Ok((
                        ResolvedValue::CompareStr {
                            ne: op == CmpOp::Ne,
                            read: Box::new(rv),
                            lit_ord,
                        },
                        ValueType::Bool,
                    ))
                }
                (None, None) => {
                    let (lrv, lt) = resolve(left, cols)?;
                    let (rrv, rt) = resolve(right, cols)?;
                    if lt == ValueType::Unknown || rt == ValueType::Unknown {
                        // One side can never hold a value, so the
                        // comparison is absent for every document.
                        return Ok((ResolvedValue::Absent, ValueType::Unknown));
                    }
                    if lt == ValueType::Str || rt == ValueType::Str {
                        if lt == rt {
                            return Err(refuse(
                                "comparing two string columns; compare each against \
                                 a string literal",
                            ));
                        }
                        return Err(refuse(
                            "a string column compares only against a string literal",
                        ));
                    }
                    if ordering && (lt == ValueType::Bool || rt == ValueType::Bool) {
                        return Err(refuse(
                            "booleans do not order; only `==` and `!=` compare them",
                        ));
                    }
                    if lt != rt {
                        return Err(refuse(
                            "a comparison mixes an int and a double; stock CEL does \
                             not coerce — convert explicitly with double()",
                        ));
                    }
                    Ok((
                        ResolvedValue::Compare {
                            op,
                            left: Box::new(lrv),
                            right: Box::new(rrv),
                        },
                        ValueType::Bool,
                    ))
                }
            }
        }
        V::Logic(logic) => {
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
        V::Ternary(t) => {
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
    }
}

/// Decode one wire comparison op.
fn cmp_op_of(op: i32) -> Result<CmpOp, Status> {
    match pb::ValueCmpOp::try_from(op) {
        Ok(pb::ValueCmpOp::Eq) => Ok(CmpOp::Eq),
        Ok(pb::ValueCmpOp::Ne) => Ok(CmpOp::Ne),
        Ok(pb::ValueCmpOp::Lt) => Ok(CmpOp::Lt),
        Ok(pb::ValueCmpOp::Le) => Ok(CmpOp::Le),
        Ok(pb::ValueCmpOp::Gt) => Ok(CmpOp::Gt),
        Ok(pb::ValueCmpOp::Ge) => Ok(CmpOp::Ge),
        _ => Err(refuse("comparison node with an unknown operation")),
    }
}

impl CmpOp {
    fn name(self) -> &'static str {
        match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

/// The string-ordering refusal, shared by both evaluation paths.
fn string_ordering(op: CmpOp) -> Status {
    refuse(format!(
        "`{}` orders strings, which is not in the vocabulary; only `==` and \
         `!=` compare strings",
        op.name()
    ))
}

/// Integer comparison.
fn cmp_int(op: CmpOp, a: i64, b: i64) -> bool {
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
fn cmp_double(op: CmpOp, a: f64, b: f64) -> bool {
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
        ResolvedValue::FacetCol(fi) => cols
            .facet_ord(*fi, doc_id)
            .map(|ord| Val::FacetOrd { column: *fi, ord }),
        ResolvedValue::MapNum { column, key_ord } => {
            cols.map_value(*column, *key_ord, doc_id).map(Val::Double)
        }
        ResolvedValue::MapFacet { column, key_ord } => cols
            .map_facet_value_ord(*column, *key_ord, doc_id)
            .map(|ord| Val::MapFacetOrd {
                column: *column,
                ord,
            }),
        ResolvedValue::Absent => None,
        ResolvedValue::ConstInt(v) => Some(Val::Int(*v)),
        ResolvedValue::ConstDouble(v) => Some(Val::Double(*v)),
        ResolvedValue::ToDouble(inner) => match eval(inner, doc_id, cols)? {
            Val::Int(v) => Some(Val::Double(v as f64)),
            Val::Double(v) => Some(Val::Double(v)),
            Val::FacetOrd { .. } | Val::MapFacetOrd { .. } | Val::Bool(_) => {
                unreachable!("resolution refused double() over a non-number")
            }
        },
        ResolvedValue::Negate(inner) => match eval(inner, doc_id, cols)? {
            Val::Int(v) => v.checked_neg().map(Val::Int),
            Val::Double(v) => Some(Val::Double(-v)),
            Val::FacetOrd { .. } | Val::MapFacetOrd { .. } | Val::Bool(_) => {
                unreachable!("resolution refused unary minus over a non-number")
            }
        },
        ResolvedValue::Arith { op, left, right } => {
            let l = eval(left, doc_id, cols)?;
            let r = eval(right, doc_id, cols)?;
            match (l, r) {
                (Val::Int(a), Val::Int(b)) => int_arith(*op, a, b).map(Val::Int),
                (Val::Double(a), Val::Double(b)) => Some(Val::Double(double_arith(*op, a, b))),
                _ => unreachable!("resolution type-checked the operands"),
            }
        }
        ResolvedValue::ConstBool(v) => Some(Val::Bool(*v)),
        ResolvedValue::Compare { op, left, right } => {
            let l = eval(left, doc_id, cols)?;
            let r = eval(right, doc_id, cols)?;
            let b = match (l, r) {
                (Val::Int(a), Val::Int(b)) => cmp_int(*op, a, b),
                (Val::Double(a), Val::Double(b)) => cmp_double(*op, a, b),
                (Val::Bool(a), Val::Bool(b)) => match op {
                    CmpOp::Eq => a == b,
                    CmpOp::Ne => a != b,
                    _ => unreachable!("resolution refused ordering booleans"),
                },
                _ => unreachable!("resolution type-checked the operands"),
            };
            Some(Val::Bool(b))
        }
        ResolvedValue::CompareStr { ne, read, lit_ord } => {
            let ord = match eval(read, doc_id, cols)? {
                Val::FacetOrd { ord, .. } | Val::MapFacetOrd { ord, .. } => ord,
                _ => unreachable!("resolution shaped the read side"),
            };
            let eq = *lit_ord == Some(ord);
            Some(Val::Bool(eq != *ne))
        }
        ResolvedValue::Logic { and, children } => {
            let mut absent = false;
            for child in children {
                match eval(child, doc_id, cols) {
                    None => absent = true,
                    Some(Val::Bool(b)) => {
                        if b != *and {
                            // The determinate answer absorbs absence,
                            // exactly stock CEL's error-absorbing
                            // commutative logic.
                            return Some(Val::Bool(!*and));
                        }
                    }
                    Some(_) => unreachable!("resolution type-checked the operands"),
                }
            }
            if absent {
                None
            } else {
                Some(Val::Bool(*and))
            }
        }
        ResolvedValue::Not(inner) => match eval(inner, doc_id, cols)? {
            Val::Bool(b) => Some(Val::Bool(!b)),
            _ => unreachable!("resolution type-checked the operand"),
        },
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

/// Checked i64 arithmetic: where stock CEL errors (overflow, division
/// by zero, `i64::MIN / -1`, `i64::MIN % -1`), the engine answers
/// ABSENT. Division truncates toward zero and `%` takes the dividend's
/// sign, exactly CEL's (and Rust's) rule.
fn int_arith(op: Op, a: i64, b: i64) -> Option<i64> {
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
fn double_arith(op: Op, a: f64, b: f64) -> f64 {
    match op {
        Op::Add => a + b,
        Op::Sub => a - b,
        Op::Mul => a * b,
        Op::Div => a / b,
        Op::Mod => unreachable!("resolution refused a double remainder"),
    }
}

// ---------------------------------------------------------------------
// Leaf enumeration for the coordinator's typo refusal
// ---------------------------------------------------------------------

/// One column-read leaf of a value expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueLeaf {
    /// A plain column read.
    Column(String),
    /// A map entry read.
    Map {
        /// Map column name.
        column: String,
        /// Entry key.
        key: String,
    },
}

impl ValueLeaf {
    /// Human-readable form for refusal messages.
    pub fn describe(&self) -> String {
        match self {
            ValueLeaf::Column(name) => name.clone(),
            ValueLeaf::Map { column, key } => format!("{column}[{key:?}]"),
        }
    }
}

/// Collect the column-read leaves of one expression, depth-first, the
/// order `projection_leaves_known` flags follow.
pub fn column_leaves(expr: &pb::ValueExpr, out: &mut Vec<ValueLeaf>) {
    use pb::value_expr::Expr as V;
    match expr.expr.as_ref() {
        Some(V::Column(name)) => out.push(ValueLeaf::Column(name.clone())),
        Some(V::Map(read)) => out.push(ValueLeaf::Map {
            column: read.column.clone(),
            key: read.key.clone(),
        }),
        Some(V::Arith(arith)) => {
            if let Some(left) = arith.left.as_ref() {
                column_leaves(left, out);
            }
            if let Some(right) = arith.right.as_ref() {
                column_leaves(right, out);
            }
        }
        Some(V::ToDouble(inner)) | Some(V::Negate(inner)) | Some(V::Not(inner)) => {
            column_leaves(inner, out)
        }
        Some(V::Compare(cmp)) => {
            if let Some(left) = cmp.left.as_ref() {
                column_leaves(left, out);
            }
            if let Some(right) = cmp.right.as_ref() {
                column_leaves(right, out);
            }
        }
        Some(V::Logic(logic)) => {
            for child in &logic.children {
                column_leaves(child, out);
            }
        }
        Some(V::Ternary(t)) => {
            for part in [t.cond.as_ref(), t.then_value.as_ref(), t.else_value.as_ref()]
                .into_iter()
                .flatten()
            {
                column_leaves(part, out);
            }
        }
        Some(V::IntLiteral(_))
        | Some(V::FloatLiteral(_))
        | Some(V::BoolLiteral(_))
        | Some(V::StringLiteral(_))
        | None => {}
    }
}

/// Whether THIS shard resolves one leaf: the column in some family,
/// and for a map read the key in the column's key dictionary — the
/// same rule filter leaves use.
pub fn leaf_known(leaf: &ValueLeaf, cols: &dyn ColumnLookup) -> bool {
    match leaf {
        ValueLeaf::Column(name) => {
            cols.numeric_index(name).is_some()
                || cols.integer_index(name).is_some()
                || cols.facet_index(name).is_some()
        }
        ValueLeaf::Map { column, key } => {
            let num = cols
                .map_numeric_index(column)
                .and_then(|ci| cols.map_numeric_key_ord(ci, key))
                .is_some();
            let facet = cols
                .map_facet_index(column)
                .and_then(|ci| cols.map_facet_key_ord(ci, key))
                .is_some();
            num || facet
        }
    }
}

// ---------------------------------------------------------------------
// Ingest-time evaluation (materialized columns)
// ---------------------------------------------------------------------

/// The value environment one AddDocumentsRequest provides: its own
/// numeric families, by name. Facet strings are deliberately not here —
/// materialization computes NUMBERS, and a string expression stores
/// nothing a copy would not.
#[derive(Debug, Default)]
pub struct IngestEnv {
    /// f64 values by column name.
    pub numerics: HashMap<String, f64>,
    /// i64 values by column name.
    pub integers: HashMap<String, i64>,
    /// Map-numeric values by (column, key).
    pub map_numerics: HashMap<(String, String), f64>,
}

/// One evaluated ingest value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IngestVal {
    /// Lands in the i64 family.
    Int(i64),
    /// Lands in the f64 family.
    Double(f64),
    /// A comparison or logic result. Never stored: the kind check
    /// refuses it naming the ternary as the fix.
    Bool(bool),
}

/// Evaluate one compiled expression against one document's own values.
/// `Ok(None)` is absence (a name this document does not carry, or
/// checked integer arithmetic); `Err` is a REAL conflict the document
/// exposes — int mixed with double, an ambiguous name — refused loudly
/// per the loud-failure rule, never stored as a guess.
pub fn eval_ingest(expr: &pb::ValueExpr, env: &IngestEnv) -> Result<Option<IngestVal>, Status> {
    use pb::value_expr::Expr as V;
    match expr.expr.as_ref().ok_or_else(|| refuse("empty ValueExpr node"))? {
        V::Column(name) => {
            let num = env.numerics.get(name);
            let int = env.integers.get(name);
            if num.is_some() && int.is_some() {
                return Err(refuse(format!(
                    "materialization input {name:?} arrives as both a numeric and an \
                     integer on this document"
                )));
            }
            Ok(num
                .map(|v| IngestVal::Double(*v))
                .or(int.map(|v| IngestVal::Int(*v))))
        }
        V::Map(read) => Ok(env
            .map_numerics
            .get(&(read.column.clone(), read.key.clone()))
            .map(|v| IngestVal::Double(*v))),
        V::IntLiteral(v) => Ok(Some(IngestVal::Int(*v))),
        V::FloatLiteral(v) => Ok(Some(IngestVal::Double(*v))),
        V::ToDouble(inner) => Ok(match eval_ingest(inner, env)? {
            None => None,
            Some(IngestVal::Int(i)) => Some(IngestVal::Double(i as f64)),
            Some(IngestVal::Double(d)) => Some(IngestVal::Double(d)),
            Some(IngestVal::Bool(_)) => {
                return Err(refuse("double() over a bool; only numbers convert"));
            }
        }),
        V::Negate(inner) => Ok(match eval_ingest(inner, env)? {
            None => None,
            Some(IngestVal::Int(v)) => v.checked_neg().map(IngestVal::Int),
            Some(IngestVal::Double(v)) => Some(IngestVal::Double(-v)),
            Some(IngestVal::Bool(_)) => {
                return Err(refuse(
                    "unary minus over a boolean; booleans negate with `!`",
                ));
            }
        }),
        V::Arith(arith) => {
            let left = arith
                .left
                .as_ref()
                .ok_or_else(|| refuse("arithmetic node without a left operand"))?;
            let right = arith
                .right
                .as_ref()
                .ok_or_else(|| refuse("arithmetic node without a right operand"))?;
            let (l, r) = (eval_ingest(left, env)?, eval_ingest(right, env)?);
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
                (Some(IngestVal::Int(a)), Some(IngestVal::Int(b))) => {
                    Ok(int_arith(op, a, b).map(IngestVal::Int))
                }
                (Some(IngestVal::Double(a)), Some(IngestVal::Double(b))) => {
                    if op == Op::Mod {
                        return Err(refuse(
                            "`%` is integer-only in CEL; there is no double remainder",
                        ));
                    }
                    Ok(Some(IngestVal::Double(double_arith(op, a, b))))
                }
                _ => Err(refuse(
                    "materialization arithmetic mixes an int input and a double input \
                     on this document; stock CEL does not coerce — convert explicitly \
                     with double()",
                )),
            }
        }
        V::BoolLiteral(v) => Ok(Some(IngestVal::Bool(*v))),
        V::StringLiteral(_) => Err(refuse(
            "a string literal is not a value outside a `==`/`!=` comparison",
        )),
        V::Compare(cmp) => {
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
                    return Ok(Some(IngestVal::Bool(if op == CmpOp::Ne { !eq } else { eq })));
                }
                let other = if is_lit(left) { right } else { left };
                return match eval_ingest(other, env)? {
                    // Materialization reads no facets, so the read
                    // side is absent on every document — Kleene.
                    None => Ok(None),
                    Some(_) => Err(refuse(
                        "a string literal compares only against a facet read, and \
                         materialization reads no facets",
                    )),
                };
            }
            let (l, r) = (eval_ingest(left, env)?, eval_ingest(right, env)?);
            match (l, r) {
                (None, _) | (_, None) => Ok(None),
                (Some(IngestVal::Int(a)), Some(IngestVal::Int(b))) => {
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
                    "a materialization comparison mixes an int input and a double \
                     input on this document; stock CEL does not coerce — convert \
                     explicitly with double()",
                )),
            }
        }
        V::Logic(logic) => {
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
                match eval_ingest(child, env)? {
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
        V::Not(inner) => Ok(match eval_ingest(inner, env)? {
            None => None,
            Some(IngestVal::Bool(b)) => Some(IngestVal::Bool(!b)),
            Some(_) => {
                return Err(refuse("`!` over a number; Kleene logic joins booleans"));
            }
        }),
        V::Ternary(t) => {
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
            match eval_ingest(cond, env)? {
                None => Ok(None),
                Some(IngestVal::Bool(true)) => eval_ingest(then, env),
                Some(IngestVal::Bool(false)) => eval_ingest(otherwise, env),
                Some(_) => Err(refuse(
                    "the ternary's condition is a number; a condition is a boolean",
                )),
            }
        }
    }
}
