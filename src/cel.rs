//! The CEL filter front-end (`docs/cel-filters.md`): compiles the
//! public `Bm25SearchRequest.filter` string into the
//! [`crate::pb::FilterExpr`] predicate IR, once, on the coordinator.
//!
//! **CEL selects; function chains score.** This compiler accepts
//! exactly the CEL subset that compiles to dictionary-resolved
//! predicates plus boolean algebra — comparisons, `in`, `has()`,
//! `!`/`&&`/`||`, `timestamp()`, and the two geo containment functions
//! — and REFUSES everything else BY NAME: arithmetic, the ternary,
//! string functions, macros, `matches()` (a regex engine is a CVE
//! class this codebase deliberately does not link), cross-column
//! comparisons. Nothing is ever interpreted per document, here or
//! anywhere: what does not compile does not run slowly, it does not
//! run.
//!
//! The lexer and parser are hand-rolled recursive descent — no regex,
//! no parser generator, no new dependency in the serving binary. The
//! grammar is CEL's (operator precedence `||` < `&&` < relations <
//! unary < member access), so an expression this module accepts means
//! what stock CEL says it means; the differential oracle in
//! `tests/cel_filters.rs` holds the two to agreement wherever both are
//! defined. Where stock CEL is UNDEFINED — a missing field is an
//! error there — the engine's three-valued semantics take over
//! (`src/filter.rs`), and that deviation is pinned in its own tests,
//! never hidden in the oracle.
//!
//! Timestamps: `timestamp("2015-01-01T00:00:00Z")` parses RFC 3339 by
//! exact integer math (days-from-civil, no floats, no patterns) into
//! the epoch MICROSECONDS an i64 column stores
//! (`docs/range-facets.md`), so `decided >= timestamp(...)` is an
//! ordinary integer bound by the time a shard sees it.

use tonic::Status;

use crate::pb;

/// Compile a request's CEL filter text: empty (or all-whitespace) is
/// "no filter", anything else must compile and validate whole. The
/// returned tree has already passed [`crate::filter::validate_filter`]
/// — depth and leaf caps, bound finiteness, geo region rules — so
/// callers hold a tree every shard will accept.
pub fn compile_filter(text: &str) -> Result<Option<pb::FilterExpr>, Status> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let toks = lex(trimmed)?;
    let mut p = Parser { toks, pos: 0 };
    let ast = p.expr()?;
    p.expect_end()?;
    let expr = compile(&ast)?;
    crate::filter::validate_filter(&expr)?;
    Ok(Some(expr))
}

/// One refusal, uniformly shaped: every message begins `filter: ` and
/// names the construct it refuses.
fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(format!("filter: {}", msg.into()))
}

// ---------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------

/// One token. Integer literals carry i128 so `-9223372036854775808`
/// can lex as (minus, 2^63) and fold back into range at compile.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Int(i128),
    Float(f64),
    Str(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    OrOr,
    AndAnd,
    Bang,
    EqEq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Question,
    Colon,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
}

fn lex(s: &str) -> Result<Vec<Tok>, Status> {
    let b = s.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            b')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            b'[' => {
                toks.push(Tok::LBracket);
                i += 1;
            }
            b']' => {
                toks.push(Tok::RBracket);
                i += 1;
            }
            b',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            b'.' => {
                // A leading-dot float (.5) is CEL-legal; refuse it by
                // name rather than misreading it as member access.
                if b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    return Err(refuse(format!(
                        "float literal starting with a bare dot at byte {i}; write 0.5, \
                         not .5"
                    )));
                }
                toks.push(Tok::Dot);
                i += 1;
            }
            b'?' => {
                toks.push(Tok::Question);
                i += 1;
            }
            b':' => {
                toks.push(Tok::Colon);
                i += 1;
            }
            b'+' => {
                toks.push(Tok::Plus);
                i += 1;
            }
            b'-' => {
                toks.push(Tok::Minus);
                i += 1;
            }
            b'*' => {
                toks.push(Tok::Star);
                i += 1;
            }
            b'/' => {
                // CEL comments are //; a division that looks like one
                // still refuses as arithmetic, so lexing / alone is
                // enough.
                toks.push(Tok::Slash);
                i += 1;
            }
            b'%' => {
                toks.push(Tok::Percent);
                i += 1;
            }
            b'!' => {
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::NotEq);
                    i += 2;
                } else {
                    toks.push(Tok::Bang);
                    i += 1;
                }
            }
            b'=' => {
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::EqEq);
                    i += 2;
                } else {
                    return Err(refuse(format!("single `=` at byte {i}; equality is `==`")));
                }
            }
            b'<' => {
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::Le);
                    i += 2;
                } else {
                    toks.push(Tok::Lt);
                    i += 1;
                }
            }
            b'>' => {
                if b.get(i + 1) == Some(&b'=') {
                    toks.push(Tok::Ge);
                    i += 2;
                } else {
                    toks.push(Tok::Gt);
                    i += 1;
                }
            }
            b'&' => {
                if b.get(i + 1) == Some(&b'&') {
                    toks.push(Tok::AndAnd);
                    i += 2;
                } else {
                    return Err(refuse(format!(
                        "single `&` at byte {i}; conjunction is `&&`"
                    )));
                }
            }
            b'|' => {
                if b.get(i + 1) == Some(&b'|') {
                    toks.push(Tok::OrOr);
                    i += 2;
                } else {
                    return Err(refuse(format!(
                        "single `|` at byte {i}; disjunction is `||`"
                    )));
                }
            }
            b'"' | b'\'' => {
                let (t, next) = lex_string(b, i)?;
                toks.push(t);
                i = next;
            }
            b'0'..=b'9' => {
                let (t, next) = lex_number(b, i)?;
                toks.push(t);
                i = next;
            }
            c if c == b'_' || c.is_ascii_alphabetic() => {
                let start = i;
                while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric()) {
                    i += 1;
                }
                let word = &s[start..i];
                // CEL raw and bytes string prefixes, refused by name
                // before they misread as an identifier + a string.
                if (word == "r" || word == "R" || word == "b" || word == "B")
                    && matches!(b.get(i), Some(b'"') | Some(b'\''))
                {
                    return Err(refuse(format!(
                        "{} string literal at byte {start}; only plain quoted strings \
                         compile",
                        if word.eq_ignore_ascii_case("r") {
                            "raw"
                        } else {
                            "bytes"
                        }
                    )));
                }
                toks.push(if word == "in" {
                    Tok::In
                } else {
                    Tok::Ident(word.to_string())
                });
            }
            other => {
                return Err(refuse(format!(
                    "unexpected character {:?} at byte {i}",
                    other as char
                )));
            }
        }
    }
    Ok(toks)
}

/// A quoted string with the common escapes. Unknown escapes refuse by
/// name — a silently-passed-through backslash is how a query means one
/// thing and matches another.
fn lex_string(b: &[u8], start: usize) -> Result<(Tok, usize), Status> {
    let quote = b[start];
    let mut out = String::new();
    let mut i = start + 1;
    while i < b.len() {
        match b[i] {
            c if c == quote => return Ok((Tok::Str(out), i + 1)),
            b'\\' => {
                let esc = *b
                    .get(i + 1)
                    .ok_or_else(|| refuse(format!("unterminated escape at byte {i}")))?;
                match esc {
                    b'\\' => out.push('\\'),
                    b'"' => out.push('"'),
                    b'\'' => out.push('\''),
                    b'n' => out.push('\n'),
                    b't' => out.push('\t'),
                    b'r' => out.push('\r'),
                    b'0' => out.push('\0'),
                    b'u' => {
                        let hex = b
                            .get(i + 2..i + 6)
                            .ok_or_else(|| refuse("truncated \\u escape"))?;
                        let hex = std::str::from_utf8(hex)
                            .ok()
                            .filter(|h| h.bytes().all(|c| c.is_ascii_hexdigit()))
                            .ok_or_else(|| refuse("\\u escape needs 4 hex digits"))?;
                        let cp = u32::from_str_radix(hex, 16).expect("4 hex digits");
                        let ch = char::from_u32(cp)
                            .ok_or_else(|| refuse(format!("\\u{hex} is not a scalar value")))?;
                        out.push(ch);
                        i += 4;
                    }
                    other => {
                        return Err(refuse(format!(
                            "unsupported string escape \\{}; the compiled escapes are \
                             \\\\ \\\" \\' \\n \\t \\r \\0 \\uXXXX",
                            other as char
                        )));
                    }
                }
                i += 2;
            }
            // Strings are UTF-8 in, UTF-8 out; copy whole code points.
            _ => {
                let s = std::str::from_utf8(&b[i..])
                    .map_err(|_| refuse(format!("invalid UTF-8 in string at byte {i}")))?;
                let ch = s.chars().next().expect("non-empty by loop condition");
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    Err(refuse(format!(
        "unterminated string starting at byte {start}"
    )))
}

/// An integer or float literal. Hex ints compile; uint suffixes
/// refuse (the engine's integer domain is i64).
fn lex_number(b: &[u8], start: usize) -> Result<(Tok, usize), Status> {
    let mut i = start;
    if b[i] == b'0' && matches!(b.get(i + 1), Some(b'x') | Some(b'X')) {
        i += 2;
        let hex_start = i;
        while i < b.len() && b[i].is_ascii_hexdigit() {
            i += 1;
        }
        if i == hex_start {
            return Err(refuse(format!("empty hex literal at byte {start}")));
        }
        let text = std::str::from_utf8(&b[hex_start..i]).expect("ascii hex");
        let v = i128::from_str_radix(text, 16)
            .ok()
            .filter(|v| *v <= i128::from(u64::MAX))
            .ok_or_else(|| refuse(format!("hex literal 0x{text} is out of range")))?;
        if matches!(b.get(i), Some(b'u') | Some(b'U')) {
            return Err(refuse(
                "uint literal (u suffix); the engine's integer domain is i64",
            ));
        }
        return Ok((Tok::Int(v), i));
    }
    let mut is_float = false;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i < b.len() && b[i] == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
        is_float = true;
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        if j < b.len() && b[j].is_ascii_digit() {
            is_float = true;
            i = j;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
        }
    }
    if matches!(b.get(i), Some(b'u') | Some(b'U')) {
        return Err(refuse(
            "uint literal (u suffix); the engine's integer domain is i64",
        ));
    }
    let text = std::str::from_utf8(&b[start..i]).expect("ascii digits");
    if is_float {
        let v: f64 = text
            .parse()
            .map_err(|_| refuse(format!("malformed float literal {text}")))?;
        Ok((Tok::Float(v), i))
    } else {
        let v: i128 = text
            .parse()
            .map_err(|_| refuse(format!("integer literal {text} is out of range")))?;
        // 2^63 itself stays lexable so `-9223372036854775808` can fold
        // back into i64::MIN under the unary minus; the bare positive
        // form refuses at compile.
        if v > i128::from(i64::MAX) + 1 {
            return Err(refuse(format!(
                "integer literal {text} is out of i64 range"
            )));
        }
        Ok((Tok::Int(v), i))
    }
}

// ---------------------------------------------------------------------
// Parser (CEL precedence: || < && < relation < unary < member)
// ---------------------------------------------------------------------

/// The parsed shape this compiler works from. Deliberately smaller
/// than CEL's: everything CEL can say that this cannot hold was
/// refused by name during parsing.
#[derive(Debug, Clone, PartialEq)]
enum Ast {
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
    Float(f64),
    Str(String),
    List(Vec<Ast>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelOp {
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
    fn mirrored(self) -> RelOp {
        match self {
            RelOp::Lt => RelOp::Gt,
            RelOp::Le => RelOp::Ge,
            RelOp::Gt => RelOp::Lt,
            RelOp::Ge => RelOp::Le,
            other => other,
        }
    }

    fn name(self) -> &'static str {
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

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, t: Tok, what: &str) -> Result<(), Status> {
        if self.eat(&t) {
            Ok(())
        } else {
            Err(refuse(format!(
                "expected {what}, found {}",
                self.describe_here()
            )))
        }
    }

    fn expect_end(&self) -> Result<(), Status> {
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

    /// Top level: an `or`, then the refusals for what CEL would allow
    /// next but predicates cannot hold.
    fn expr(&mut self) -> Result<Ast, Status> {
        let e = self.or()?;
        if self.peek() == Some(&Tok::Question) {
            return Err(refuse(
                "the ternary (`_ ? _ : _`) does not compile to an index predicate; \
                 write the two branches as `(cond && a) || (!cond && b)`",
            ));
        }
        Ok(e)
    }

    fn or(&mut self) -> Result<Ast, Status> {
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

    fn and(&mut self) -> Result<Ast, Status> {
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
    fn relation(&mut self) -> Result<Ast, Status> {
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

    fn refuse_arithmetic(&self) -> Result<(), Status> {
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

    fn unary(&mut self) -> Result<Ast, Status> {
        if self.eat(&Tok::Bang) {
            return Ok(Ast::Not(Box::new(self.unary()?)));
        }
        if self.eat(&Tok::Minus) {
            // Unary minus folds into a numeric literal and nothing
            // else — negating a column is arithmetic.
            return match self.unary()? {
                Ast::Int(v) => Ok(Ast::Int(-v)),
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
    fn member(&mut self) -> Result<Ast, Status> {
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
                            args.push(self.expr()?);
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
                let key = self.expr()?;
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

    fn primary(&mut self) -> Result<Ast, Status> {
        match self.next() {
            Some(Tok::LParen) => {
                let e = self.expr()?;
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
                            args.push(self.expr()?);
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
            Some(Tok::Float(v)) => Ok(Ast::Float(v)),
            Some(Tok::Str(v)) => Ok(Ast::Str(v)),
            Some(other) => Err(refuse(format!(
                "unexpected token {other:?} where a value was expected"
            ))),
            None => Err(refuse("unexpected end of input")),
        }
    }
}

// ---------------------------------------------------------------------
// Compiler (Ast -> pb::FilterExpr)
// ---------------------------------------------------------------------

/// What one side of a relation is, after literal folding.
enum Side {
    Column(String),
    MapAccess(String, String),
    Str(String),
    Int(i64),
    Float(f64),
    List(Vec<Ast>),
}

fn wrap(expr: pb::filter_expr::Expr) -> pb::FilterExpr {
    pb::FilterExpr { expr: Some(expr) }
}

fn not_of(inner: pb::FilterExpr) -> pb::FilterExpr {
    wrap(pb::filter_expr::Expr::Not(Box::new(inner)))
}

fn bound(value: pb::filter_bound::Value, exclusive: bool) -> pb::FilterBound {
    pb::FilterBound {
        value: Some(value),
        exclusive,
    }
}

fn compile(ast: &Ast) -> Result<pb::FilterExpr, Status> {
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
        Ast::Int(_) | Ast::Float(_) | Ast::Str(_) => {
            Err(refuse("a bare literal is not a predicate"))
        }
        Ast::List(_) => Err(refuse(
            "a bare list is not a predicate; lists appear on the right of `in`",
        )),
    }
}

/// Fold one relation side into a [`Side`], resolving `timestamp()`
/// calls into their epoch-micros integer.
fn side_of(ast: &Ast) -> Result<Side, Status> {
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
    }
}

fn int_literal(v: i128) -> Result<i64, Status> {
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
    match op {
        RelOp::Eq => Ok(eq),
        RelOp::Ne => Ok(not_of(eq)),
        other => Err(refuse(format!(
            "string ordering (`{}`) does not compile; facet dictionaries are unordered \
             (equality and `in` are the string predicates)",
            other.name()
        ))),
    }
}

fn compile_number_relation(
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
        Side::MapAccess(name, key) => {
            wrap(pb::filter_expr::Expr::MapNumber(pb::MapNumberPredicate {
                column: name.clone(),
                key: key.clone(),
                min,
                max,
            }))
        }
        _ => unreachable!("callers pass a column side"),
    };
    Ok(if op == RelOp::Ne { not_of(leaf) } else { leaf })
}

/// `in` has two compiled shapes: `"key" in map_column` (key presence)
/// and `column in [v, ...]` (membership).
fn compile_in(lhs: Side, rhs: Side) -> Result<pb::FilterExpr, Status> {
    match (lhs, rhs) {
        (Side::Str(key), Side::Column(column)) => Ok(wrap(pb::filter_expr::Expr::MapHasKey(
            pb::MapKeyPredicate { column, key },
        ))),
        (Side::Column(_) | Side::MapAccess(..), Side::Column(name)) => Err(refuse(format!(
            "`in {name}` needs a string literal key on the left (`\"k\" in {name}`) or \
             a list on the right"
        ))),
        (col @ (Side::Column(_) | Side::MapAccess(..)), Side::List(items)) => {
            if items.is_empty() {
                return Err(refuse(
                    "membership in an empty list is a predicate nobody meant to send",
                ));
            }
            let mut strings = Vec::new();
            let mut numbers = Vec::new();
            for item in &items {
                match side_of(item)? {
                    Side::Str(v) => strings.push(v),
                    Side::Int(v) => numbers.push(pb::filter_bound::Value::Int(v)),
                    Side::Float(v) if v.is_finite() => {
                        numbers.push(pb::filter_bound::Value::Num(v))
                    }
                    Side::Float(v) => {
                        return Err(refuse(format!(
                            "float literal {v} is not finite; NaN and infinity bound \
                             nothing a column can hold"
                        )));
                    }
                    _ => {
                        return Err(refuse(
                            "a membership list holds literals only (strings or numbers)",
                        ));
                    }
                }
            }
            match (strings.is_empty(), numbers.is_empty()) {
                (false, true) => Ok(match &col {
                    Side::Column(name) => wrap(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
                        column: name.clone(),
                        values: strings,
                    })),
                    Side::MapAccess(name, key) => {
                        wrap(pb::filter_expr::Expr::MapFacet(pb::MapFacetPredicate {
                            column: name.clone(),
                            key: key.clone(),
                            values: strings,
                        }))
                    }
                    _ => unreachable!("matched above"),
                }),
                (true, false) => {
                    // Numeric membership is a disjunction of point
                    // ranges: exact per element, and the leaf cap
                    // bounds the fan-out.
                    let eqs = numbers
                        .into_iter()
                        .map(|v| compile_number_relation(&col, RelOp::Eq, v))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(if eqs.len() == 1 {
                        eqs.into_iter().next().expect("len checked")
                    } else {
                        wrap(pb::filter_expr::Expr::Or(pb::FilterList { exprs: eqs }))
                    })
                }
                (false, false) => Err(refuse(
                    "a membership list mixes strings and numbers; one column holds one \
                     kind",
                )),
                (true, true) => unreachable!("non-empty list produced no literals"),
            }
        }
        (Side::Str(_), Side::List(_)) | (_, Side::List(_)) => Err(refuse(
            "`in` needs a column on the left of a list (`column in [..]`)",
        )),
        _ => Err(refuse(
            "`in` compiles as `column in [values]` or `\"key\" in map_column`",
        )),
    }
}

/// The compiled function vocabulary, everything else refused by name
/// with the reason.
fn compile_call(name: &str, args: &[Ast]) -> Result<pb::FilterExpr, Status> {
    match name {
        "has" => {
            if args.len() != 1 {
                return Err(refuse("has() takes exactly one column"));
            }
            match &args[0] {
                Ast::Ident(column) => Ok(wrap(pb::filter_expr::Expr::Has(pb::HasPredicate {
                    column: column.clone(),
                }))),
                Ast::MapAccess(col, key) => Err(refuse(format!(
                    "has() on a map entry; key presence is '{key}' in {col}"
                ))),
                _ => Err(refuse("has() takes a column name")),
            }
        }
        "timestamp" => Err(refuse(
            "timestamp() is a value, not a predicate; compare a column against it \
             (`decided >= timestamp(\"2015-01-01T00:00:00Z\")`)",
        )),
        "within_bbox" => {
            let (column, rest) = geo_args(name, args, 4)?;
            Ok(wrap(pb::filter_expr::Expr::Geo(pb::GeoFilter {
                column,
                region: Some(pb::geo_filter::Region::Bbox(pb::GeoBbox {
                    min_lat: rest[0],
                    max_lat: rest[1],
                    min_lon: rest[2],
                    max_lon: rest[3],
                })),
            })))
        }
        "within_radius" | "within_radius_manhattan" => {
            let (column, rest) = geo_args(name, args, 3)?;
            let metric = if name == "within_radius" {
                pb::GeoMetric::Haversine
            } else {
                pb::GeoMetric::Manhattan
            };
            Ok(wrap(pb::filter_expr::Expr::Geo(pb::GeoFilter {
                column,
                region: Some(pb::geo_filter::Region::Radius(pb::GeoRadius {
                    lat: rest[0],
                    lon: rest[1],
                    meters: rest[2],
                    metric: metric.into(),
                })),
            })))
        }
        "matches" => Err(refuse(
            "matches() (regex) is not supported: patterns do not compile to dictionary \
             predicates, and a regex engine is a dependency this engine deliberately \
             does not link",
        )),
        "startsWith" | "endsWith" | "contains" => Err(refuse(format!(
            "{name}() does not compile: facet dictionaries resolve whole values \
             (equality and `in`), not substrings"
        ))),
        "all" | "exists" | "exists_one" | "filter" | "map" => Err(refuse(format!(
            "the {name}() macro (a comprehension) does not compile to an index predicate"
        ))),
        "size" => Err(refuse("size() does not compile to an index predicate")),
        "int" | "uint" | "double" | "string" | "bool" | "bytes" | "dyn" | "type" => {
            Err(refuse(format!(
                "the {name}() conversion does not compile; compare the column in its \
                 own type (the literal's type picks the table)"
            )))
        }
        "duration" => Err(refuse(
            "duration() does not compile; timestamps are compared directly \
             (`decided >= timestamp(...)`)",
        )),
        other => Err(refuse(format!(
            "function {other}() is not in the compiled vocabulary: comparisons, `in`, \
             has(), timestamp(), within_bbox(), within_radius(), \
             within_radius_manhattan()"
        ))),
    }
}

/// Geo function arguments: a column identifier plus `n` numeric
/// literals.
fn geo_args(name: &str, args: &[Ast], n: usize) -> Result<(String, Vec<f64>), Status> {
    if args.len() != n + 1 {
        return Err(refuse(format!(
            "{name}() takes a geo column and {n} numbers, got {} arguments",
            args.len()
        )));
    }
    let column = match &args[0] {
        Ast::Ident(c) => c.clone(),
        _ => {
            return Err(refuse(format!(
                "{name}(): the first argument is the geo column name"
            )));
        }
    };
    let mut rest = Vec::with_capacity(n);
    for a in &args[1..] {
        rest.push(match a {
            Ast::Int(v) => int_literal(*v)? as f64,
            Ast::Float(v) => *v,
            _ => {
                return Err(refuse(format!(
                    "{name}(): coordinates and distances are numeric literals"
                )));
            }
        });
    }
    Ok((column, rest))
}

// ---------------------------------------------------------------------
// RFC 3339 -> epoch microseconds, by exact integer math
// ---------------------------------------------------------------------

fn timestamp_argument(args: &[Ast]) -> Result<i64, Status> {
    match args {
        [Ast::Str(s)] => parse_rfc3339_micros(s),
        _ => Err(refuse(
            "timestamp() takes one RFC 3339 string, e.g. \
             timestamp(\"2015-01-01T00:00:00Z\")",
        )),
    }
}

/// Days from 1970-01-01 for a civil date (Howard Hinnant's
/// days_from_civil): pure integer arithmetic, exact over the whole i64
/// range this can produce.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (i64::from(if m > 2 { m - 3 } else { m + 9 })) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Parse an RFC 3339 instant (`YYYY-MM-DDTHH:MM:SS[.frac][Z|+HH:MM]`)
/// into epoch microseconds, refusing malformed input by the part that
/// is wrong. Fractional digits beyond microseconds floor away, the
/// same drop (and the same direction) as
/// [`crate::node::timestamp_to_epoch_micros`] at ingest, so a filter
/// bound and a stored value truncate identically.
fn parse_rfc3339_micros(s: &str) -> Result<i64, Status> {
    let b = s.as_bytes();
    let bad = |what: &str| refuse(format!("timestamp {s:?}: {what}"));
    let digits = |range: std::ops::Range<usize>| -> Result<i64, Status> {
        let part = b.get(range.clone()).ok_or_else(|| bad("truncated"))?;
        if !part.iter().all(u8::is_ascii_digit) {
            return Err(bad("expected digits"));
        }
        Ok(std::str::from_utf8(part)
            .expect("ascii digits")
            .parse::<i64>()
            .expect("bounded digit run"))
    };
    let sep = |i: usize, c: u8| -> Result<(), Status> {
        if b.get(i) == Some(&c) {
            Ok(())
        } else {
            Err(bad(&format!("expected {:?} at byte {i}", c as char)))
        }
    };
    let year = digits(0..4)?;
    sep(4, b'-')?;
    let month = digits(5..7)? as u32;
    sep(7, b'-')?;
    let day = digits(8..10)? as u32;
    if !matches!(b.get(10), Some(b'T') | Some(b't')) {
        return Err(bad("expected 'T' between date and time"));
    }
    let hour = digits(11..13)?;
    sep(13, b':')?;
    let minute = digits(14..16)?;
    sep(16, b':')?;
    let second = digits(17..19)?;
    if !(1..=12).contains(&month) {
        return Err(bad("month is outside 1..=12"));
    }
    if day < 1 || day > days_in_month(year, month) {
        return Err(bad("day is outside the month"));
    }
    if hour > 23 || minute > 59 {
        return Err(bad("time of day is outside 23:59"));
    }
    if second > 59 {
        // RFC 3339 admits :60 leap seconds; epoch math cannot place
        // one honestly, so it refuses rather than aliasing it onto a
        // neighbor.
        return Err(bad(
            "second is outside 0..=59 (leap seconds do not compile)",
        ));
    }
    let mut i = 19;
    let mut micros_frac: i64 = 0;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return Err(bad("empty fractional second"));
        }
        // First six digits are microseconds; the remainder floors away
        // (the ingest contract).
        for (n, c) in b[start..i].iter().take(6).enumerate() {
            micros_frac += i64::from(c - b'0') * 10_i64.pow(5 - n as u32);
        }
    }
    let offset_seconds: i64 = match b.get(i) {
        Some(b'Z') | Some(b'z') => {
            i += 1;
            0
        }
        Some(&sign @ (b'+' | b'-')) => {
            let oh = digits(i + 1..i + 3)?;
            sep(i + 3, b':')?;
            let om = digits(i + 4..i + 6)?;
            if oh > 23 || om > 59 {
                return Err(bad("offset is outside 23:59"));
            }
            i += 6;
            let magnitude = oh * 3600 + om * 60;
            if sign == b'+' {
                magnitude
            } else {
                -magnitude
            }
        }
        _ => return Err(bad("expected 'Z' or a +HH:MM offset")),
    };
    if i != b.len() {
        return Err(bad("trailing input after the offset"));
    }
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    seconds
        .checked_mul(1_000_000)
        .and_then(|us| us.checked_add(micros_frac))
        .ok_or_else(|| bad("does not fit epoch microseconds"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compiled(text: &str) -> pb::FilterExpr {
        compile_filter(text)
            .unwrap_or_else(|e| panic!("{text:?} refused: {}", e.message()))
            .expect("non-empty filter")
    }

    fn refused(text: &str, needle: &str) {
        let err = compile_filter(text).expect_err(needle);
        assert!(
            err.message().contains(needle),
            "{text:?}: {needle:?} not in {:?}",
            err.message()
        );
    }

    fn facet(column: &str, values: &[&str]) -> pb::FilterExpr {
        pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Facet(pb::FacetPredicate {
                column: column.into(),
                values: values.iter().map(|v| v.to_string()).collect(),
            })),
        }
    }

    /// Every compiled construct, checked structurally against the
    /// hand-built tree.
    #[test]
    fn compiles_the_pinned_vocabulary() {
        assert_eq!(compile_filter("").expect("empty is no filter"), None);
        assert_eq!(compile_filter("  \n ").expect("blank is no filter"), None);

        assert_eq!(compiled("court == \"scotus\""), facet("court", &["scotus"]));
        assert_eq!(compiled("'scotus' == court"), facet("court", &["scotus"]));
        assert_eq!(
            compiled("court != \"scotus\""),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Not(Box::new(facet(
                    "court",
                    &["scotus"]
                )))),
            }
        );
        assert_eq!(
            compiled("court in [\"scotus\", \"ca9\"]"),
            facet("court", &["scotus", "ca9"])
        );
        // Dotted identifiers are flat column names here.
        assert_eq!(
            compiled("meta.court == \"scotus\""),
            facet("meta.court", &["scotus"])
        );

        let year_ge = pb::FilterExpr {
            expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                column: "year".into(),
                min: Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Int(1990)),
                    exclusive: false,
                }),
                max: None,
            })),
        };
        assert_eq!(compiled("year >= 1990"), year_ge);
        assert_eq!(compiled("1990 <= year"), year_ge, "mirrored literal-first");
        assert_eq!(
            compiled("score < 2.5"),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                    column: "score".into(),
                    min: None,
                    max: Some(pb::FilterBound {
                        value: Some(pb::filter_bound::Value::Num(2.5)),
                        exclusive: true,
                    }),
                })),
            }
        );
        // Equality is the two-sided inclusive point range.
        assert_eq!(
            compiled("year == 1990"),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Number(pb::NumberPredicate {
                    column: "year".into(),
                    min: Some(pb::FilterBound {
                        value: Some(pb::filter_bound::Value::Int(1990)),
                        exclusive: false,
                    }),
                    max: Some(pb::FilterBound {
                        value: Some(pb::filter_bound::Value::Int(1990)),
                        exclusive: false,
                    }),
                })),
            }
        );
        // i64::MIN folds through the unary minus.
        assert!(matches!(
            compiled("year == -9223372036854775808").expr,
            Some(pb::filter_expr::Expr::Number(p))
                if p.min == Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Int(i64::MIN)),
                    exclusive: false,
                })
        ));

        assert_eq!(
            compiled("tags[\"color\"] == \"red\""),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::MapFacet(pb::MapFacetPredicate {
                    column: "tags".into(),
                    key: "color".into(),
                    values: vec!["red".into()],
                })),
            }
        );
        assert_eq!(
            compiled("cites[\"410 U.S. 113\"] >= 3"),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::MapNumber(pb::MapNumberPredicate {
                    column: "cites".into(),
                    key: "410 U.S. 113".into(),
                    min: Some(pb::FilterBound {
                        value: Some(pb::filter_bound::Value::Int(3)),
                        exclusive: false,
                    }),
                    max: None,
                })),
            }
        );
        assert_eq!(
            compiled("\"color\" in tags"),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::MapHasKey(pb::MapKeyPredicate {
                    column: "tags".into(),
                    key: "color".into(),
                })),
            }
        );
        assert_eq!(
            compiled("has(court)"),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Has(pb::HasPredicate {
                    column: "court".into(),
                })),
            }
        );

        // Numeric membership is a disjunction of point ranges.
        let year_in = compiled("year in [1990, 1995]");
        match &year_in.expr {
            Some(pb::filter_expr::Expr::Or(list)) => assert_eq!(list.exprs.len(), 2),
            other => panic!("expected Or of point ranges, got {other:?}"),
        }

        // Geo sugar compiles to the existing GeoFilter leaf.
        assert_eq!(
            compiled("within_radius(courthouse, 38.9, -77.0, 25000)"),
            pb::FilterExpr {
                expr: Some(pb::filter_expr::Expr::Geo(pb::GeoFilter {
                    column: "courthouse".into(),
                    region: Some(pb::geo_filter::Region::Radius(pb::GeoRadius {
                        lat: 38.9,
                        lon: -77.0,
                        meters: 25000.0,
                        metric: pb::GeoMetric::Haversine.into(),
                    })),
                })),
            }
        );
        assert!(matches!(
            compiled("within_bbox(courthouse, 30.0, 40.0, -80.0, -70.0)").expr,
            Some(pb::filter_expr::Expr::Geo(_))
        ));

        // timestamp() becomes an integer bound in epoch micros.
        assert!(matches!(
            compiled("decided >= timestamp(\"2015-01-01T00:00:00Z\")").expr,
            Some(pb::filter_expr::Expr::Number(p))
                if p.min == Some(pb::FilterBound {
                    value: Some(pb::filter_bound::Value::Int(1_420_070_400_000_000)),
                    exclusive: false,
                })
        ));
    }

    /// CEL precedence: `||` binds loosest, then `&&`, then relations,
    /// then `!`; parentheses override.
    #[test]
    fn precedence_matches_cel() {
        let e = compiled("a == \"x\" || b == \"y\" && c == \"z\"");
        match &e.expr {
            Some(pb::filter_expr::Expr::Or(or)) => {
                assert_eq!(or.exprs.len(), 2);
                assert_eq!(or.exprs[0], facet("a", &["x"]));
                match &or.exprs[1].expr {
                    Some(pb::filter_expr::Expr::And(and)) => {
                        assert_eq!(and.exprs[0], facet("b", &["y"]));
                        assert_eq!(and.exprs[1], facet("c", &["z"]));
                    }
                    other => panic!("expected And on the right, got {other:?}"),
                }
            }
            other => panic!("expected Or at the top, got {other:?}"),
        }
        let e = compiled("(a == \"x\" || b == \"y\") && c == \"z\"");
        assert!(
            matches!(&e.expr, Some(pb::filter_expr::Expr::And(_))),
            "parentheses hoist the Or under the And"
        );
        let e = compiled("!(a == \"x\") && b == \"y\"");
        match &e.expr {
            Some(pb::filter_expr::Expr::And(and)) => {
                assert!(matches!(
                    &and.exprs[0].expr,
                    Some(pb::filter_expr::Expr::Not(_))
                ));
            }
            other => panic!("expected And, got {other:?}"),
        }
        // A chain of three folds into one connective, not a skewed
        // tree — depth budget is spent on the user's nesting only.
        let e = compiled("a == \"x\" && b == \"y\" && c == \"z\"");
        match &e.expr {
            Some(pb::filter_expr::Expr::And(and)) => assert_eq!(and.exprs.len(), 3),
            other => panic!("expected flat And, got {other:?}"),
        }
    }

    /// Everything outside the vocabulary refuses BY NAME.
    #[test]
    fn refusals_name_the_construct() {
        refused("year + 1 > 1990", "arithmetic (`+`)");
        refused("year - 1 > 1990", "arithmetic (`-`)");
        refused("year * 2 > 1990", "arithmetic (`*`)");
        refused("court == \"a\" ? has(x) : has(y)", "ternary");
        refused("court.matches(\"sco.*\")", "matches() (regex)");
        refused("court.startsWith(\"sco\")", "startsWith()");
        refused("tags.all(t, t == \"x\")", "all() macro");
        refused("size(court) > 2", "size()");
        refused("int(year) == 1990", "int() conversion");
        refused("court == name", "cross-column");
        refused("1 == 1", "constant comparison");
        refused("court", "bare column");
        refused("true", "boolean literal");
        refused("court == true", "boolean literal");
        refused("court == null", "null");
        refused("court < \"b\"", "string ordering");
        refused("year in []", "empty list");
        refused("year in [1990, \"x\"]", "mixes strings and numbers");
        refused("has(tags[\"color\"])", "'color' in tags");
        refused("-court == \"x\"", "unary minus on a non-literal");
        refused("court == 'a' 'b'", "trailing input");
        refused("r\"raw\" == court", "raw string literal");
        refused("b'bytes' == court", "bytes string literal");
        refused("5u == year", "uint literal");
        refused("court = \"x\"", "single `=`");
        refused("a && (b == \"x\"", "expected `)`");
        refused("m[court] == \"x\"", "string literal key");
        refused("duration(\"1h\") == x", "duration()");
        refused("frobnicate(court)", "not in the compiled vocabulary");
        refused("year == 9223372036854775808", "out of i64 range");
        refused("court == \"\\q\"", "unsupported string escape");
        refused("year >= timestamp(\"2015-13-01T00:00:00Z\")", "month");
        refused(
            "year >= timestamp(\"2015-02-29T00:00:00Z\")",
            "day is outside",
        );
        refused("year >= timestamp(\"2015-06-30T23:59:60Z\")", "leap second");
        refused("year >= timestamp(\"2015-06-30\")", "expected 'T'");
        // A refusal inside a nested expression still surfaces.
        refused("a == \"x\" && (year + 1 > 2)", "arithmetic");
    }

    /// The exact instants the integer math must produce: the epoch,
    /// a leap day, a negative-offset instant, fractional flooring, and
    /// a pre-epoch date.
    #[test]
    fn rfc3339_micros_are_exact() {
        let us = |s: &str| parse_rfc3339_micros(s).expect(s);
        assert_eq!(us("1970-01-01T00:00:00Z"), 0);
        assert_eq!(us("2015-01-01T00:00:00Z"), 1_420_070_400_000_000);
        assert_eq!(us("2016-02-29T12:00:00Z"), 1_456_747_200_000_000);
        // -05:00 means five hours LATER in UTC.
        assert_eq!(
            us("2015-01-01T00:00:00-05:00"),
            1_420_070_400_000_000 + 5 * 3600 * 1_000_000
        );
        assert_eq!(
            us("2015-01-01T00:00:00+05:30"),
            1_420_070_400_000_000 - (5 * 3600 + 30 * 60) * 1_000_000
        );
        // Micros survive; the seventh digit floors away.
        assert_eq!(us("1970-01-01T00:00:00.123456Z"), 123_456);
        assert_eq!(us("1970-01-01T00:00:00.1234569Z"), 123_456);
        assert_eq!(us("1970-01-01T00:00:00.5Z"), 500_000);
        // Pre-epoch: 1969-12-31T23:59:59Z is minus one second.
        assert_eq!(us("1969-12-31T23:59:59Z"), -1_000_000);
        // Case-insensitive T and Z, per RFC 3339.
        assert_eq!(us("1970-01-01t00:00:00z"), 0);
    }
}
