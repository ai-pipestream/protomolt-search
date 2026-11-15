//! Structural Rust token analyzer for `code_hygiene`.
//!
//! Operates on the token stream produced by [`super::lexer::lex`] to measure
//! function line spans (`signature..=closing_brace`), parameter counts,
//! `#[cfg(test)]` scopes, unconditional `#[allow(...)]` attributes, and
//! comment hygiene findings without false positives from string literals.

use std::ops::Range;

use crate::attrs::inspect_attr;
use crate::lexer::{lex, Delim, Token, TokenKind};
use crate::sig::{count_params, parse_fn_name, skip_generics, update_angle_depth};
use crate::slop::{scan_comment, CommentFindings};

#[derive(Clone, Debug)]
pub(crate) struct FnRecord {
    pub(crate) name: String,
    pub(crate) line: usize,
    pub(crate) lines: Option<usize>,
    pub(crate) params: usize,
    pub(crate) is_test: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct FileReport {
    pub(crate) functions: Vec<FnRecord>,
    pub(crate) too_many_args_allows: Vec<usize>,
    pub(crate) dead_code_allows: Vec<usize>,
    pub(crate) comments: CommentFindings,
}

pub(crate) struct CodeView<'a> {
    pub(crate) tokens: Vec<Token<'a>>,
    pub(crate) matching_close: Vec<Option<usize>>,
}

#[derive(Default)]
struct Pending {
    cfg_test: bool,
    test_attr: bool,
    qual_line: Option<usize>,
}

pub(crate) fn is_test_file(rel_path: &str) -> bool {
    rel_path.ends_with("tests.rs") || rel_path.contains("/tests/")
}

pub(crate) fn analyze_source(rel_path: &str, src: &str) -> FileReport {
    let raw_tokens = lex(src);
    let mut report = FileReport::default();
    let mut code_tokens = Vec::with_capacity(raw_tokens.len());
    for tok in raw_tokens {
        if let TokenKind::Comment(text) = tok.kind {
            scan_comment(text, tok.line, &mut report.comments);
        } else {
            code_tokens.push(tok);
        }
    }
    let matching_close = build_matching_close(&code_tokens);
    let view = CodeView {
        tokens: code_tokens,
        matching_close,
    };
    let file_is_test = is_test_file(rel_path);
    scan_range(&view, 0..view.tokens.len(), file_is_test, &mut report);
    report
}

fn build_matching_close(tokens: &[Token<'_>]) -> Vec<Option<usize>> {
    let mut out = vec![None; tokens.len()];
    let mut stack: Vec<(Delim, usize)> = Vec::new();
    for (idx, tok) in tokens.iter().enumerate() {
        match tok.kind {
            TokenKind::Open(d) => stack.push((d, idx)),
            TokenKind::Close(d) => {
                if let Some(pos) = stack.iter().rposition(|(open_d, _)| *open_d == d) {
                    let (_, open_idx) = stack[pos];
                    stack.truncate(pos);
                    out[open_idx] = Some(idx);
                }
            }
            _ => {}
        }
    }
    out
}

fn scan_range(
    view: &CodeView<'_>,
    range: Range<usize>,
    mut scope_is_test: bool,
    out: &mut FileReport,
) {
    let mut pending = Pending::default();
    let mut i = range.start;
    while i < range.end {
        if let Some(next) = try_scan_attribute(view, i, &mut scope_is_test, &mut pending, out) {
            i = next;
        } else if let Some(next) = try_scan_qualifier(view, i, &mut pending) {
            i = next;
        } else if view.tokens[i].kind == TokenKind::Ident("fn") {
            let fn_is_test = scope_is_test || pending.cfg_test || pending.test_attr;
            let sig_line = pending.qual_line.unwrap_or(view.tokens[i].line);
            pending = Pending::default();
            i = parse_fn_item(view, i..range.end, fn_is_test, sig_line, out).unwrap_or(i + 1);
        } else if matches!(
            view.tokens[i].kind,
            TokenKind::Ident("mod" | "impl" | "trait")
        ) {
            let item_is_test = scope_is_test || pending.cfg_test;
            pending = Pending::default();
            i = scan_scoped_item(view, i + 1..range.end, item_is_test, out);
        } else {
            i = scan_other_token(view, i, scope_is_test, &mut pending, out);
        }
    }
}

fn try_scan_attribute(
    view: &CodeView<'_>,
    i: usize,
    scope_is_test: &mut bool,
    pending: &mut Pending,
    out: &mut FileReport,
) -> Option<usize> {
    if view.tokens[i].kind != TokenKind::Punct(b'#') {
        return None;
    }
    if view.tokens.get(i + 1).map(|t| t.kind) == Some(TokenKind::Open(Delim::Bracket)) {
        let close = view.matching_close[i + 1]?;
        pending.qual_line = None;
        let summary = inspect_attr(view, i + 2..close, view.tokens[i].line, out);
        pending.cfg_test |= summary.is_cfg_test;
        pending.test_attr |= summary.is_test_fn;
        return Some(close + 1);
    }
    if view.tokens.get(i + 1).map(|t| t.kind) == Some(TokenKind::Punct(b'!'))
        && view.tokens.get(i + 2).map(|t| t.kind) == Some(TokenKind::Open(Delim::Bracket))
    {
        let close = view.matching_close[i + 2]?;
        let summary = inspect_attr(view, i + 3..close, view.tokens[i].line, out);
        if summary.is_cfg_test {
            *scope_is_test = true;
        }
        return Some(close + 1);
    }
    None
}

fn try_scan_qualifier(view: &CodeView<'_>, i: usize, pending: &mut Pending) -> Option<usize> {
    let line = view.tokens[i].line;
    match view.tokens[i].kind {
        TokenKind::Ident("pub") => {
            pending.qual_line.get_or_insert(line);
            if view.tokens.get(i + 1).map(|t| t.kind) == Some(TokenKind::Open(Delim::Paren)) {
                if let Some(close) = view.matching_close[i + 1] {
                    return Some(close + 1);
                }
            }
            Some(i + 1)
        }
        TokenKind::Ident("default" | "const" | "async" | "unsafe") => {
            pending.qual_line.get_or_insert(line);
            Some(i + 1)
        }
        TokenKind::Ident("extern") => {
            pending.qual_line.get_or_insert(line);
            if view.tokens.get(i + 1).map(|t| t.kind) == Some(TokenKind::Literal) {
                Some(i + 2)
            } else {
                Some(i + 1)
            }
        }
        _ => None,
    }
}

fn scan_scoped_item(
    view: &CodeView<'_>,
    range: Range<usize>,
    item_is_test: bool,
    out: &mut FileReport,
) -> usize {
    let mut m = range.start;
    let mut angle_depth = 0usize;
    while m < range.end {
        match view.tokens[m].kind {
            TokenKind::Open(Delim::Paren | Delim::Bracket) => {
                m = view.matching_close[m].map_or(m + 1, |c| c + 1);
            }
            TokenKind::Open(Delim::Brace) if angle_depth > 0 => {
                m = view.matching_close[m].map_or(m + 1, |c| c + 1);
            }
            TokenKind::Open(Delim::Brace) => {
                let Some(close) = view.matching_close[m] else {
                    return m + 1;
                };
                scan_range(view, m + 1..close, item_is_test, out);
                return close + 1;
            }
            TokenKind::Punct(b';') if angle_depth == 0 => return m + 1,
            other => {
                update_angle_depth(other, &mut angle_depth);
                m += 1;
            }
        }
    }
    range.end
}

fn scan_other_token(
    view: &CodeView<'_>,
    i: usize,
    scope_is_test: bool,
    pending: &mut Pending,
    out: &mut FileReport,
) -> usize {
    match view.tokens[i].kind {
        TokenKind::Punct(b';') => {
            *pending = Pending::default();
            i + 1
        }
        TokenKind::Open(Delim::Brace) => {
            let sub_is_test = scope_is_test || pending.cfg_test;
            *pending = Pending::default();
            let Some(close) = view.matching_close[i] else {
                return i + 1;
            };
            scan_range(view, i + 1..close, sub_is_test, out);
            close + 1
        }
        TokenKind::Open(Delim::Paren | Delim::Bracket) => {
            pending.qual_line = None;
            let Some(close) = view.matching_close[i] else {
                return i + 1;
            };
            scan_range(view, i + 1..close, scope_is_test, out);
            close + 1
        }
        _ => {
            pending.qual_line = None;
            i + 1
        }
    }
}

fn parse_fn_item(
    view: &CodeView<'_>,
    range: Range<usize>,
    fn_is_test: bool,
    sig_line: usize,
    out: &mut FileReport,
) -> Option<usize> {
    let (name, mut k) = parse_fn_name(view, range.start + 1, range.end)?;
    k = skip_generics(view, k, range.end);
    if view.tokens.get(k)?.kind != TokenKind::Open(Delim::Paren) {
        return None;
    }
    let p_close = view.matching_close[k].filter(|&c| c < range.end)?;
    let params = count_params(view, k + 1..p_close);
    let mut m = p_close + 1;
    let mut angle_depth = 0usize;
    while m < range.end {
        match view.tokens[m].kind {
            TokenKind::Open(Delim::Paren | Delim::Bracket) => {
                m = view.matching_close[m]? + 1;
            }
            TokenKind::Open(Delim::Brace) if angle_depth > 0 => {
                m = view.matching_close[m]? + 1;
            }
            TokenKind::Open(Delim::Brace) => {
                let b_close = view.matching_close[m].filter(|&c| c < range.end)?;
                let lines = Some(view.tokens[b_close].line.saturating_sub(sig_line) + 1);
                out.functions.push(FnRecord {
                    name,
                    line: sig_line,
                    lines,
                    params,
                    is_test: fn_is_test,
                });
                scan_range(view, m + 1..b_close, fn_is_test, out);
                return Some(b_close + 1);
            }
            TokenKind::Punct(b';') if angle_depth == 0 => {
                out.functions.push(FnRecord {
                    name,
                    line: sig_line,
                    lines: None,
                    params,
                    is_test: fn_is_test,
                });
                return Some(m + 1);
            }
            other => {
                update_angle_depth(other, &mut angle_depth);
                m += 1;
            }
        }
    }
    Some(p_close + 1)
}
