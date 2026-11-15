//! The parsed shape both compilers work from, and its operators.

/// The parsed shape this compiler works from. Deliberately smaller
/// than CEL's: everything CEL can say that this cannot hold was
/// refused by name during parsing.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Ast {
    Or(Vec<Ast>),
    And(Vec<Ast>),
    Not(Box<Ast>),
    Rel(Box<Ast>, RelOp, Box<Ast>),
    /// A bare or dotted identifier chain (`court`, `meta.court`).
    Ident(String),
    /// `column["key"]` map access.
    MapAccess(String, String),
    /// A function call on bare identifiers/literals.
    Call(String, Vec<Ast>),
    Int(i128),
    Uint(u64),
    Float(f64),
    Str(String),
    List(Vec<Ast>),
    /// Value mode only: binary arithmetic.
    Arith(Box<Ast>, VOp, Box<Ast>),
    /// Value mode only: unary minus on a non-literal.
    Neg(Box<Ast>),
    /// Value mode only: the conditional `cond ? then : else`.
    Ternary(Box<Ast>, Box<Ast>, Box<Ast>),
}

/// Arithmetic operators, value mode only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl VOp {
    pub(super) fn name(self) -> &'static str {
        match self {
            VOp::Add => "+",
            VOp::Sub => "-",
            VOp::Mul => "*",
            VOp::Div => "/",
            VOp::Mod => "%",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
}

impl RelOp {
    /// The op with its sides swapped (`5 < year` is `year > 5`).
    pub(super) fn mirrored(self) -> RelOp {
        match self {
            RelOp::Lt => RelOp::Gt,
            RelOp::Le => RelOp::Ge,
            RelOp::Gt => RelOp::Lt,
            RelOp::Ge => RelOp::Le,
            other => other,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            RelOp::Eq => "==",
            RelOp::Ne => "!=",
            RelOp::Lt => "<",
            RelOp::Le => "<=",
            RelOp::Gt => ">",
            RelOp::Ge => ">=",
            RelOp::In => "in",
        }
    }
}
