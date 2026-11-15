//! Function signature and generic/parameter token helpers for `code_hygiene`.

use std::ops::Range;

use crate::analyzer::CodeView;
use crate::lexer::TokenKind;

pub(crate) fn parse_fn_name(
    view: &CodeView<'_>,
    pos: usize,
    end: usize,
) -> Option<(String, usize)> {
    if pos >= end {
        return None;
    }
    match view.tokens[pos].kind {
        TokenKind::Ident(name) => Some((name.to_owned(), pos + 1)),
        TokenKind::Punct(b'$') if pos + 1 < end => {
            if let TokenKind::Ident(mname) = view.tokens[pos + 1].kind {
                Some((format!("${mname}"), pos + 2))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(crate) fn skip_generics(view: &CodeView<'_>, start: usize, end: usize) -> usize {
    if start >= end
        || !matches!(
            view.tokens[start].kind,
            TokenKind::Punct(b'<') | TokenKind::Shl
        )
    {
        return start;
    }
    let mut k = start;
    let mut depth = 0usize;
    while k < end {
        if matches!(view.tokens[k].kind, TokenKind::Open(_)) {
            if let Some(close) = view.matching_close[k] {
                k = close + 1;
                continue;
            }
        }
        update_angle_depth(view.tokens[k].kind, &mut depth);
        k += 1;
        if depth == 0 {
            break;
        }
    }
    k
}

pub(crate) fn count_params(view: &CodeView<'_>, range: Range<usize>) -> usize {
    let mut count = 0usize;
    let mut has_tokens = false;
    let mut angle_depth = 0usize;
    let mut k = range.start;
    while k < range.end {
        match view.tokens[k].kind {
            TokenKind::Open(_) => {
                has_tokens = true;
                k = view.matching_close[k].map_or(k + 1, |c| c + 1);
            }
            TokenKind::Punct(b',') if angle_depth == 0 => {
                if has_tokens {
                    count += 1;
                    has_tokens = false;
                }
                k += 1;
            }
            other => {
                has_tokens = true;
                update_angle_depth(other, &mut angle_depth);
                k += 1;
            }
        }
    }
    if has_tokens {
        count + 1
    } else {
        count
    }
}

pub(crate) fn update_angle_depth(kind: TokenKind<'_>, depth: &mut usize) {
    match kind {
        TokenKind::Punct(b'<') => *depth += 1,
        TokenKind::Shl => *depth += 2,
        TokenKind::Punct(b'>') => *depth = depth.saturating_sub(1),
        TokenKind::Shr => *depth = depth.saturating_sub(2),
        _ => {}
    }
}
