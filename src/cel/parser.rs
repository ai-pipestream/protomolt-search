//! The recursive-descent parser, CEL precedence: `||` < `&&` < relation <
//! unary < member. This file is the shared machinery and the predicate
//! grammar.

use super::CelError;

use super::ast::{Ast, RelOp};
use super::lex::Tok;
use super::refuse;

pub(super) struct Parser {
    pub(super) toks: Vec<Tok>,
    pub(super) pos: usize,
    /// Value mode (`compile_value`): arithmetic is grammar, not a
    /// refusal, and parenthesized subterms re-enter the VALUE grammar.
    pub(super) value_mode: bool,
}

impl Parser {
    pub(super) fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    pub(super) fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    pub(super) fn expect(&mut self, t: Tok, what: &str) -> Result<(), CelError> {
        if self.eat(&t) {
            Ok(())
        } else {
            Err(refuse(format!(
                "expected {what}, found {}",
                self.describe_here()
            )))
        }
    }

    pub(super) fn expect_end(&self) -> Result<(), CelError> {
        if self.pos == self.toks.len() {
            Ok(())
        } else {
            Err(refuse(format!(
                "trailing input after the expression: {}",
                self.describe_here()
            )))
        }
    }

    fn describe_here(&self) -> String {
        match self.peek() {
            None => "end of input".to_string(),
            Some(t) => format!("{t:?}"),
        }
    }

    /// Subexpression entry for parentheses and call arguments: the
    /// grammar re-entered matches the front-end that is parsing.
    fn expr_any(&mut self) -> Result<Ast, CelError> {
        if self.value_mode {
            self.value_expr()
        } else {
            self.expr()
        }
    }

    /// Top level: an `or`, then the refusals for what CEL would allow
    /// next but predicates cannot hold.
    pub(super) fn expr(&mut self) -> Result<Ast, CelError> {
        let e = self.or()?;
        if self.peek() == Some(&Tok::Question) {
            return Err(refuse(
                "the ternary (`_ ? _ : _`) does not compile to an index predicate; \
                 write the two branches as `(cond && a) || (!cond && b)`",
            ));
        }
        Ok(e)
    }

    fn or(&mut self) -> Result<Ast, CelError> {
        let first = self.and()?;
        if self.peek() != Some(&Tok::OrOr) {
            return Ok(first);
        }
        let mut children = vec![first];
        while self.eat(&Tok::OrOr) {
            children.push(self.and()?);
        }
        Ok(Ast::Or(children))
    }

    fn and(&mut self) -> Result<Ast, CelError> {
        let first = self.relation()?;
        if self.peek() != Some(&Tok::AndAnd) {
            return Ok(first);
        }
        let mut children = vec![first];
        while self.eat(&Tok::AndAnd) {
            children.push(self.relation()?);
        }
        Ok(Ast::And(children))
    }

    /// One (non-associative) relation, or a bare unary. Arithmetic
    /// operators are recognized here purely to refuse them by name.
    fn relation(&mut self) -> Result<Ast, CelError> {
        let lhs = self.unary()?;
        self.refuse_arithmetic()?;
        let op = match self.peek() {
            Some(Tok::EqEq) => RelOp::Eq,
            Some(Tok::NotEq) => RelOp::Ne,
            Some(Tok::Lt) => RelOp::Lt,
            Some(Tok::Le) => RelOp::Le,
            Some(Tok::Gt) => RelOp::Gt,
            Some(Tok::Ge) => RelOp::Ge,
            Some(Tok::In) => RelOp::In,
            _ => return Ok(lhs),
        };
        self.pos += 1;
        let rhs = self.unary()?;
        self.refuse_arithmetic()?;
        Ok(Ast::Rel(Box::new(lhs), op, Box::new(rhs)))
    }

    fn refuse_arithmetic(&self) -> Result<(), CelError> {
        let sym = match self.peek() {
            Some(Tok::Plus) => "+",
            Some(Tok::Minus) => "-",
            Some(Tok::Star) => "*",
            Some(Tok::Slash) => "/",
            Some(Tok::Percent) => "%",
            _ => return Ok(()),
        };
        Err(refuse(format!(
            "arithmetic (`{sym}`) does not compile to an index predicate; precompute \
             the value and compare the column against the constant"
        )))
    }

    fn unary(&mut self) -> Result<Ast, CelError> {
        if self.eat(&Tok::Bang) {
            return Ok(Ast::Not(Box::new(self.unary()?)));
        }
        if self.eat(&Tok::Minus) {
            // Unary minus folds into a numeric literal and nothing
            // else — negating a column is arithmetic.
            return match self.unary()? {
                Ast::Int(v) => Ok(Ast::Int(-v)),
                Ast::Uint(_) => Err(refuse("unary minus is not defined for uint literals")),
                Ast::Float(v) => Ok(Ast::Float(-v)),
                _ => Err(refuse(
                    "unary minus on a non-literal; negating a column is arithmetic, \
                     which does not compile",
                )),
            };
        }
        self.member()
    }

    /// Member access: dotted selection folds into the column name
    /// (dots are just characters in this engine's flat column
    /// namespace); bracket indexing is map access and needs a string
    /// literal key.
    pub(super) fn member(&mut self) -> Result<Ast, CelError> {
        let mut e = self.primary()?;
        loop {
            if self.eat(&Tok::Dot) {
                let field = match self.next() {
                    Some(Tok::Ident(name)) => name,
                    _ => return Err(refuse("expected an identifier after `.`")),
                };
                // CEL method-call syntax (`col.matches(...)`): parse it
                // so the refusal can name the METHOD, not the syntax.
                if self.peek() == Some(&Tok::LParen) {
                    self.pos += 1;
                    let mut args = vec![e];
                    if self.peek() != Some(&Tok::RParen) {
                        loop {
                            args.push(self.expr_any()?);
                            if !self.eat(&Tok::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Tok::RParen, "`)`")?;
                    e = Ast::Call(field, args);
                    continue;
                }
                match e {
                    Ast::Ident(base) => e = Ast::Ident(format!("{base}.{field}")),
                    _ => {
                        return Err(refuse(
                            "field selection on a non-column; map values are strings \
                             or numbers, not structures",
                        ));
                    }
                }
            } else if self.eat(&Tok::LBracket) {
                let key = self.expr_any()?;
                self.expect(Tok::RBracket, "`]`")?;
                match (&e, key) {
                    (Ast::Ident(col), Ast::Str(k)) => e = Ast::MapAccess(col.clone(), k),
                    (Ast::Ident(col), _) => {
                        return Err(refuse(format!(
                            "map access on {col:?} needs a string literal key; computed \
                             keys do not compile"
                        )));
                    }
                    _ => {
                        return Err(refuse(
                            "indexing a non-column; only `column[\"key\"]` compiles",
                        ));
                    }
                }
            } else {
                return Ok(e);
            }
        }
    }

    fn primary(&mut self) -> Result<Ast, CelError> {
        match self.next() {
            Some(Tok::LParen) => {
                let e = self.expr_any()?;
                self.expect(Tok::RParen, "`)`")?;
                Ok(e)
            }
            Some(Tok::LBracket) => {
                let mut items = Vec::new();
                if self.peek() != Some(&Tok::RBracket) {
                    loop {
                        items.push(self.unary()?);
                        if !self.eat(&Tok::Comma) {
                            break;
                        }
                    }
                }
                self.expect(Tok::RBracket, "`]`")?;
                Ok(Ast::List(items))
            }
            Some(Tok::Ident(name)) => {
                if self.eat(&Tok::LParen) {
                    let mut args = Vec::new();
                    if self.peek() != Some(&Tok::RParen) {
                        loop {
                            args.push(self.expr_any()?);
                            if !self.eat(&Tok::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Tok::RParen, "`)`")?;
                    Ok(Ast::Call(name, args))
                } else {
                    Ok(Ast::Ident(name))
                }
            }
            Some(Tok::Int(v)) => Ok(Ast::Int(v)),
            Some(Tok::Uint(v)) => Ok(Ast::Uint(v)),
            Some(Tok::Float(v)) => Ok(Ast::Float(v)),
            Some(Tok::Str(v)) => Ok(Ast::Str(v)),
            Some(other) => Err(refuse(format!(
                "unexpected token {other:?} where a value was expected"
            ))),
            None => Err(refuse("unexpected end of input")),
        }
    }
}
