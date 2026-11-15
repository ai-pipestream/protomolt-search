//! Attribute inspection helpers for `code_hygiene`.
//!
//! Distinguishes `#[cfg(test)]` and `#[cfg(all(..., test))]` test gates from
//! production `#[cfg(not(test))]`, detects `#[test]` / `#[tokio::test]`
//! markers, and counts unconditional `#[allow(clippy::too_many_arguments)]`
//! and `#[allow(dead_code)]` suppressions while permitting legitimate
//! feature-conditional `#[cfg_attr(..., allow(dead_code))]` gates.

use std::ops::Range;

use crate::analyzer::{CodeView, FileReport};
use crate::lexer::{Delim, TokenKind};

#[derive(Default)]
pub(crate) struct AttrSummary {
    pub(crate) is_cfg_test: bool,
    pub(crate) is_test_fn: bool,
}

pub(crate) fn inspect_attr(
    view: &CodeView<'_>,
    bracket_inner: Range<usize>,
    attr_line: usize,
    out: &mut FileReport,
) -> AttrSummary {
    let mut summary = AttrSummary::default();
    let first = bracket_inner.start;
    if first >= bracket_inner.end {
        return summary;
    }
    let first_kind = view.tokens[first].kind;
    let second_open = (first + 1 < bracket_inner.end)
        .then(|| view.tokens[first + 1].kind)
        .filter(|k| *k == TokenKind::Open(Delim::Paren));

    if first_kind == TokenKind::Ident("allow") && second_open.is_some() {
        if let Some(p_close) = view.matching_close[first + 1] {
            record_allow_tokens(view, first + 2..p_close, attr_line, true, out);
        }
        return summary;
    }
    if first_kind == TokenKind::Ident("cfg_attr") && second_open.is_some() {
        if let Some(p_close) = view.matching_close[first + 1] {
            record_allow_tokens(view, first + 2..p_close, attr_line, false, out);
        }
        return summary;
    }
    if first_kind == TokenKind::Ident("cfg") && second_open.is_some() {
        if let Some(p_close) = view.matching_close[first + 1] {
            summary.is_cfg_test = is_cfg_test_expr(view, first + 2..p_close);
        }
        return summary;
    }
    summary.is_test_fn = is_test_fn_path(view, bracket_inner);
    summary
}

fn record_allow_tokens(
    view: &CodeView<'_>,
    range: Range<usize>,
    attr_line: usize,
    include_dead_code: bool,
    out: &mut FileReport,
) {
    for tok in &view.tokens[range] {
        match tok.kind {
            TokenKind::Ident("too_many_arguments") => out.too_many_args_allows.push(attr_line),
            TokenKind::Ident("dead_code") if include_dead_code => {
                out.dead_code_allows.push(attr_line);
            }
            _ => {}
        }
    }
}

fn is_cfg_test_expr(view: &CodeView<'_>, range: Range<usize>) -> bool {
    if range.is_empty() {
        return false;
    }
    if range.len() == 1 {
        return view.tokens[range.start].kind == TokenKind::Ident("test");
    }
    let first = view.tokens[range.start].kind;
    if matches!(first, TokenKind::Ident("all" | "any"))
        && view.tokens.get(range.start + 1).map(|t| t.kind) == Some(TokenKind::Open(Delim::Paren))
        && view.matching_close[range.start + 1] == Some(range.end - 1)
    {
        let close = range.end - 1;
        let args = split_top_level_commas(view, range.start + 2..close);
        if args.is_empty() {
            return false;
        }
        return if first == TokenKind::Ident("all") {
            args.into_iter().any(|sub| is_cfg_test_expr(view, sub))
        } else {
            args.into_iter().all(|sub| is_cfg_test_expr(view, sub))
        };
    }
    false
}

fn split_top_level_commas(view: &CodeView<'_>, range: Range<usize>) -> Vec<Range<usize>> {
    let mut parts = Vec::new();
    let mut seg_start = range.start;
    let mut k = range.start;
    while k < range.end {
        match view.tokens[k].kind {
            TokenKind::Open(_) => {
                k = view.matching_close[k].map_or(k + 1, |c| c + 1);
            }
            TokenKind::Punct(b',') => {
                if seg_start < k {
                    parts.push(seg_start..k);
                }
                k += 1;
                seg_start = k;
            }
            _ => k += 1,
        }
    }
    if seg_start < range.end {
        parts.push(seg_start..range.end);
    }
    parts
}

fn is_test_fn_path(view: &CodeView<'_>, range: Range<usize>) -> bool {
    let mut last_ident = None;
    let mut k = range.start;
    while k < range.end {
        match view.tokens[k].kind {
            TokenKind::Ident(name) => {
                last_ident = Some(name);
                k += 1;
            }
            TokenKind::PathSep => k += 1,
            TokenKind::Open(Delim::Paren) => break,
            _ => return false,
        }
    }
    matches!(last_ident, Some("test" | "rstest"))
}
