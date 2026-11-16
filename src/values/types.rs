//! The resolved expression, its static types, and evaluated values.

use super::ValuesError;

use super::refuse;
use crate::pb;

/// Static type of a resolved value expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    /// CEL int (i64 columns, int literals).
    Int,
    /// CEL uint (u64 columns and unsigned literals).
    Uint,
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

/// Wire type used to validate projection results across shards.
impl From<ValueType> for pb::ScalarValueType {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Unknown => Self::Unspecified,
            ValueType::Int => Self::Integer,
            ValueType::Uint => Self::UnsignedInteger,
            ValueType::Double => Self::Number,
            ValueType::Str => Self::Text,
            ValueType::Bool => Self::Boolean,
        }
    }
}

impl ValueType {
    pub fn name(self) -> &'static str {
        match self {
            ValueType::Int => "int",
            ValueType::Uint => "uint",
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
    /// u64 column read.
    UintCol(usize),
    /// Facet column read (whole-expression string).
    FacetCol(usize),
    /// Map-numeric entry read.
    MapNum {
        /// Map-numeric column index.
        column: usize,
        /// Key ordinal in that column's key dictionary.
        key_ord: u32,
    },
    /// Exact signed map entry. Missing keys retain the column's static type.
    MapInt { column: usize, key_ord: Option<u32> },
    /// Exact unsigned map entry. Missing keys retain the column's static type.
    MapUint { column: usize, key_ord: Option<u32> },
    /// Map-facet entry read (whole-expression string).
    MapFacet {
        /// Map-facet column index.
        column: usize,
        /// Key ordinal in that column's key dictionary.
        key_ord: u32,
    },
    /// A reference this shard cannot resolve: absent everywhere.
    Absent,
    /// `hash.fnv64(stable_key())`: resolves at ingest only.
    StableKeyHash,
    /// Int literal.
    ConstInt(i64),
    /// Unsigned integer literal.
    ConstUint(u64),
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
    /// A math function call over present arguments.
    Func {
        /// The function.
        f: pb::ValueFn,
        /// Resolved arguments in source order.
        args: Vec<ResolvedValue>,
    },
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
    /// CEL uint, preserved without conversion.
    Uint(u64),
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

/// Decode one wire comparison op.
pub(super) fn cmp_op_of(op: i32) -> Result<CmpOp, ValuesError> {
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
    pub(super) fn name(self) -> &'static str {
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

impl Op {
    /// The operator's spelling, for refusals.
    pub(super) fn name(self) -> &'static str {
        match self {
            Op::Add => "+",
            Op::Sub => "-",
            Op::Mul => "*",
            Op::Div => "/",
            Op::Mod => "%",
        }
    }
}
