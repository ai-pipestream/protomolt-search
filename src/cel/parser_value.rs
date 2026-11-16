//! The value grammar of the parser.

use super::CelError;

use super::ast::{Ast, RelOp, VOp};
use super::lex::Tok;
use super::parser::Parser;
use super::refuse;

impl Parser {
    /// Top level of a VALUE expression, CEL's conditional grammar:
    /// `?:` (right-associative, lowest) over `||` over `&&` over
    /// relations over arithmetic. `in` is the one relation that stays
    /// refused: a list is not a value.
    pub(super) fn value_expr(&mut self) -> Result<Ast, CelError> {
        let cond = self.value_or()?;
        if !self.eat(&Tok::Question) {
            return Ok(cond);
        }
        // CEL: Expr = ConditionalOr ["?" ConditionalOr ":" Expr].
        let then = self.value_or()?;
        self.expect(Tok::Colon, "`:` after the ternary's then-branch")?;
        let otherwise = self.value_expr()?;
        Ok(Ast::Ternary(
            Box::new(cond),
            Box::new(then),
            Box::new(otherwise),
        ))
    }

    fn value_or(&mut self) -> Result<Ast, CelError> {
        let first = self.value_and()?;
        if self.peek() != Some(&Tok::OrOr) {
            return Ok(first);
        }
        let mut children = vec![first];
        while self.eat(&Tok::OrOr) {
            children.push(self.value_and()?);
        }
        Ok(Ast::Or(children))
    }

    fn value_and(&mut self) -> Result<Ast, CelError> {
        let first = self.value_rel()?;
        if self.peek() != Some(&Tok::AndAnd) {
            return Ok(first);
        }
        let mut children = vec![first];
        while self.eat(&Tok::AndAnd) {
            children.push(self.value_rel()?);
        }
        Ok(Ast::And(children))
    }

    /// One relation over additive arithmetic. Left-associative like
    /// CEL's grammar; a chained relation compiles here and the type
    /// check refuses the nonsense (`a < b < c` compares a bool with a
    /// number).
    fn value_rel(&mut self) -> Result<Ast, CelError> {
        let mut e = self.additive()?;
        loop {
            let op = match self.peek() {
                Some(Tok::EqEq) => RelOp::Eq,
                Some(Tok::NotEq) => RelOp::Ne,
                Some(Tok::Lt) => RelOp::Lt,
                Some(Tok::Le) => RelOp::Le,
                Some(Tok::Gt) => RelOp::Gt,
                Some(Tok::Ge) => RelOp::Ge,
                Some(Tok::In) => {
                    return Err(refuse(
                        "`in` needs a list, and a list is not a value; write the \
                         alternatives as `==` comparisons joined with `||`",
                    ));
                }
                _ => return Ok(e),
            };
            self.pos += 1;
            e = Ast::Rel(Box::new(e), op, Box::new(self.additive()?));
        }
    }

    fn additive(&mut self) -> Result<Ast, CelError> {
        let mut e = self.multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Plus) => VOp::Add,
                Some(Tok::Minus) => VOp::Sub,
                _ => return Ok(e),
            };
            self.pos += 1;
            e = Ast::Arith(Box::new(e), op, Box::new(self.multiplicative()?));
        }
    }

    fn multiplicative(&mut self) -> Result<Ast, CelError> {
        let mut e = self.value_unary()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Star) => VOp::Mul,
                Some(Tok::Slash) => VOp::Div,
                Some(Tok::Percent) => VOp::Mod,
                _ => return Ok(e),
            };
            self.pos += 1;
            e = Ast::Arith(Box::new(e), op, Box::new(self.value_unary()?));
        }
    }

    fn value_unary(&mut self) -> Result<Ast, CelError> {
        if self.eat(&Tok::Bang) {
            return Ok(Ast::Not(Box::new(self.value_unary()?)));
        }
        if self.eat(&Tok::Minus) {
            // Literals fold; anything else keeps an explicit negate
            // node, because stock CEL negates (and -0.0 is not 0-x).
            return match self.value_unary()? {
                Ast::Int(v) => Ok(Ast::Int(-v)),
                Ast::Uint(_) => Err(refuse("unary minus is not defined for uint literals")),
                Ast::Float(v) => Ok(Ast::Float(-v)),
                other => Ok(Ast::Neg(Box::new(other))),
            };
        }
        self.member()
    }
}
