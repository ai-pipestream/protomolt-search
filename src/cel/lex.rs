//! The lexer: CEL source text to tokens.

use super::CelError;

use super::refuse;

/// One token. Integer literals carry i128 so `-9223372036854775808`
/// can lex as (minus, 2^63) and fold back into range at compile.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Tok {
    Ident(String),
    Int(i128),
    Uint(u64),
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

pub(super) fn lex(s: &str) -> Result<Vec<Tok>, CelError> {
    let b = s.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if matches!(c, b' ' | b'\t' | b'\r' | b'\n') {
            i += 1;
            continue;
        }
        if let Some(tok) = single_char_token(c) {
            toks.push(tok);
            i += 1;
            continue;
        }
        let (tok, next) = match c {
            b'.' => {
                // A leading-dot float (.5) is CEL-legal; refuse it by
                // name rather than misreading it as member access.
                if b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    return Err(refuse(format!(
                        "float literal starting with a bare dot at byte {i}; write 0.5, \
                         not .5"
                    )));
                }
                (Tok::Dot, i + 1)
            }
            b'!' | b'=' | b'<' | b'>' | b'&' | b'|' => lex_operator(b, i)?,
            b'"' | b'\'' => lex_string(b, i)?,
            b'0'..=b'9' => lex_number(b, i)?,
            c if c == b'_' || c.is_ascii_alphabetic() => lex_word(s, i)?,
            other => {
                return Err(refuse(format!(
                    "unexpected character {:?} at byte {i}",
                    other as char
                )));
            }
        };
        toks.push(tok);
        i = next;
    }
    Ok(toks)
}

/// The token a punctuation byte is on its own, whatever follows it.
fn single_char_token(c: u8) -> Option<Tok> {
    Some(match c {
        b'(' => Tok::LParen,
        b')' => Tok::RParen,
        b'[' => Tok::LBracket,
        b']' => Tok::RBracket,
        b',' => Tok::Comma,
        b'?' => Tok::Question,
        b':' => Tok::Colon,
        b'+' => Tok::Plus,
        b'-' => Tok::Minus,
        b'*' => Tok::Star,
        // CEL comments are //; a division that looks like one still
        // refuses as arithmetic, so lexing / alone is enough.
        b'/' => Tok::Slash,
        b'%' => Tok::Percent,
        _ => return None,
    })
}

/// An operator that is one or two bytes long: `!` `!=` `==` `<` `<=` `>`
/// `>=` `&&` `||`. The lone halves of the two-byte-only ones refuse.
fn lex_operator(b: &[u8], i: usize) -> Result<(Tok, usize), CelError> {
    let doubled = |second: u8| b.get(i + 1) == Some(&second);
    Ok(match b[i] {
        b'!' if doubled(b'=') => (Tok::NotEq, i + 2),
        b'!' => (Tok::Bang, i + 1),
        b'=' if doubled(b'=') => (Tok::EqEq, i + 2),
        b'=' => return Err(refuse(format!("single `=` at byte {i}; equality is `==`"))),
        b'<' if doubled(b'=') => (Tok::Le, i + 2),
        b'<' => (Tok::Lt, i + 1),
        b'>' if doubled(b'=') => (Tok::Ge, i + 2),
        b'>' => (Tok::Gt, i + 1),
        b'&' if doubled(b'&') => (Tok::AndAnd, i + 2),
        b'&' => {
            return Err(refuse(format!(
                "single `&` at byte {i}; conjunction is `&&`"
            )))
        }
        b'|' if doubled(b'|') => (Tok::OrOr, i + 2),
        b'|' => {
            return Err(refuse(format!(
                "single `|` at byte {i}; disjunction is `||`"
            )))
        }
        other => unreachable!("lex dispatches only operator bytes here, not {other}"),
    })
}

/// An identifier or the `in` keyword starting at `start`.
fn lex_word(s: &str, start: usize) -> Result<(Tok, usize), CelError> {
    let b = s.as_bytes();
    let mut i = start;
    while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric()) {
        i += 1;
    }
    let word = &s[start..i];
    // CEL raw and bytes string prefixes, refused by name before they
    // misread as an identifier + a string.
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
    let tok = if word == "in" {
        Tok::In
    } else {
        Tok::Ident(word.to_string())
    };
    Ok((tok, i))
}

/// A quoted string with the common escapes. Unknown escapes refuse by
/// name — a silently-passed-through backslash is how a query means one
/// thing and matches another.
fn lex_string(b: &[u8], start: usize) -> Result<(Tok, usize), CelError> {
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

/// A signed integer, unsigned integer, or float literal. Decimal and
/// hexadecimal integers accept a `u`/`U` suffix for the full u64 domain.
fn lex_number(b: &[u8], start: usize) -> Result<(Tok, usize), CelError> {
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
            return Ok((Tok::Uint(v as u64), i + 1));
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
    let text = std::str::from_utf8(&b[start..i]).expect("ascii digits");
    if matches!(b.get(i), Some(b'u') | Some(b'U')) {
        if is_float {
            return Err(refuse("uint suffix on a floating-point literal"));
        }
        let value = text
            .parse::<u64>()
            .map_err(|_| refuse(format!("uint literal {text} is out of u64 range")))?;
        return Ok((Tok::Uint(value), i + 1));
    }
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
