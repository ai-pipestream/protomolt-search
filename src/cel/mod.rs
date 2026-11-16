//! The CEL filter front-end: compiles the
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
//! ([`crate::filter`]), and that deviation is pinned in its own tests,
//! never hidden in the oracle.
//!
//! Timestamps: `timestamp("2015-01-01T00:00:00Z")` parses RFC 3339 by
//! exact integer math (days-from-civil, no floats, no patterns) into
//! the epoch MICROSECONDS an i64 column stores, so `decided >= timestamp(...)` is an
//! ordinary integer bound by the time a shard sees it.

#![deny(clippy::result_large_err)]

mod ast;
mod calls;
mod lex;
mod parser;
mod parser_value;
mod predicate;
mod timestamp;
mod value;
mod value_logic;
mod value_math;

#[cfg(test)]
mod filter_tests;
#[cfg(test)]
mod value_tests;

use crate::pb;
use lex::lex;
use parser::Parser;
use predicate::compile;
use value::{compile_value_ast, VType};

/// Compile a request's CEL filter text: empty (or all-whitespace) is
/// "no filter", anything else must compile and validate whole. The
/// returned tree has already passed [`crate::filter::validate_filter`]
/// — depth and leaf caps, bound finiteness, geo region rules — so
/// callers hold a tree every shard will accept.
pub fn compile_filter(text: &str) -> Result<Option<pb::FilterExpr>, CelError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let toks = lex(trimmed)?;
    let mut p = Parser {
        toks,
        pos: 0,
        value_mode: false,
    };
    let ast = p.expr()?;
    p.expect_end()?;
    let expr = compile(&ast)?;
    crate::filter::validate_filter(&expr)?;
    Ok(Some(expr))
}

crate::domain_error::domain_error! {
    /// Why a CEL filter or value expression was refused, converted to
    /// transport status only at the RPC edge.
    #[derive(Debug, Clone)]
    pub enum CelError {
        InvalidArgument => invalid_argument,
    }
    foreign: Geo
}

impl From<crate::filter::FilterError> for CelError {
    fn from(error: crate::filter::FilterError) -> Self {
        use crate::filter::FilterError;
        match error {
            FilterError::InvalidArgument(message) => Self::InvalidArgument(message),
            FilterError::Geo(status) => Self::Geo(status),
        }
    }
}

/// One refusal, uniformly shaped: every message begins `filter: ` and
/// names the construct it refuses.
fn refuse(msg: impl Into<String>) -> CelError {
    CelError::invalid_argument(format!("filter: {}", msg.into()))
}

/// Compile a CEL VALUE expression (projections, materialized columns):
/// column reads, int, double, and bool literals, `+ - * / %`, unary
/// minus, `double()`, comparisons, Kleene `&& || !`, and the ternary.
/// Everything else refuses by name, exactly like the
/// filter front-end. Typing is stock CEL's — int with int, double with
/// double — checked here as far as literals pin it down and finished
/// per shard where column kinds are known.
pub fn compile_value(text: &str) -> Result<pb::ValueExpr, CelError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(refuse("empty value expression"));
    }
    let toks = lex(trimmed)?;
    let mut p = Parser {
        toks,
        pos: 0,
        value_mode: true,
    };
    let ast = p.value_expr()?;
    p.expect_end()?;
    let (expr, vt) = compile_value_ast(&ast, 0)?;
    if vt == Some(VType::Str) {
        return Err(refuse(
            "a string literal is not a computable value; string projections are \
             bare facet or map-facet column reads",
        ));
    }
    Ok(expr)
}
