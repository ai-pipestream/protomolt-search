//! Zero-false-positive Rust token and comment lexer for `code_hygiene`.
//!
//! Tracks strings (`"..."`, `b"..."`, `c"..."`), raw strings (`r#"..."#`,
//! `br#"..."#`, `cr#"..."#`), char and byte-char literals (`'a'`, `'\''`,
//! `'\u{10ffff}'`, `b'x'`) distinct from lifetimes (`'a`, `'static`, `'_`),
//! raw identifiers (`r#type`), line/doc comments (`//`, `///`, `//!`), and
//! nested block comments (`/* /* ... */ */`).

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Delim {
    Paren,
    Bracket,
    Brace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TokenKind<'a> {
    Ident(&'a str),
    Lifetime,
    Literal,
    Comment(&'a str),
    Open(Delim),
    Close(Delim),
    Arrow,
    FatArrow,
    PathSep,
    Shl,
    Shr,
    Le,
    Ge,
    EqEq,
    Ne,
    Punct(u8),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Token<'a> {
    pub(crate) kind: TokenKind<'a>,
    pub(crate) line: usize,
}

struct Cursor<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
    line: usize,
}

impl<'a> Cursor<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
            line: 1,
        }
    }

    fn advance_byte(&mut self) -> u8 {
        let b = self.bytes[self.pos];
        if b == b'\n' {
            self.line += 1;
        }
        self.pos += 1;
        b
    }

    fn advance_char(&mut self) -> char {
        let ch = self.src[self.pos..].chars().next().expect("non-empty tail");
        if ch == '\n' {
            self.line += 1;
        }
        self.pos += ch.len_utf8();
        ch
    }
}

pub(crate) fn lex(src: &str) -> Vec<Token<'_>> {
    let mut cur = Cursor::new(src);
    let mut out = Vec::with_capacity(src.len() / 4);
    while cur.pos < cur.bytes.len() {
        let b = cur.bytes[cur.pos];
        if b.is_ascii_whitespace() {
            cur.advance_byte();
            continue;
        }
        let line = cur.line;
        let kind = next_token(&mut cur);
        out.push(Token { kind, line });
    }
    out
}

fn next_token<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    let b = cur.bytes[cur.pos];
    let next = cur.bytes.get(cur.pos + 1).copied();
    if b == b'/' && next == Some(b'/') {
        return lex_line_comment(cur);
    }
    if b == b'/' && next == Some(b'*') {
        return lex_block_comment(cur);
    }
    if let Some((quote_pos, hashes)) = raw_string_prefix(cur.bytes, cur.pos) {
        return lex_raw_string(cur, quote_pos, hashes);
    }
    if b == b'"' || ((b == b'b' || b == b'c') && next == Some(b'"')) {
        return lex_cooked_string(cur);
    }
    if b == b'b' && next == Some(b'\'') {
        return lex_byte_char(cur);
    }
    if b == b'\'' {
        return lex_char_or_lifetime(cur);
    }
    if b.is_ascii_digit() {
        return lex_number(cur);
    }
    if let Some(ident) = try_lex_ident(cur) {
        return TokenKind::Ident(ident);
    }
    lex_punct_or_delim(cur)
}

fn lex_line_comment<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    let start = cur.pos;
    while cur.pos < cur.bytes.len() && cur.bytes[cur.pos] != b'\n' {
        cur.pos += 1;
    }
    TokenKind::Comment(&cur.src[start..cur.pos])
}

fn lex_block_comment<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    let start = cur.pos;
    cur.pos += 2;
    let mut depth = 1usize;
    while cur.pos < cur.bytes.len() && depth > 0 {
        let next = cur.bytes.get(cur.pos + 1).copied();
        if cur.bytes[cur.pos] == b'/' && next == Some(b'*') {
            depth += 1;
            cur.pos += 2;
        } else if cur.bytes[cur.pos] == b'*' && next == Some(b'/') {
            depth -= 1;
            cur.pos += 2;
        } else {
            cur.advance_byte();
        }
    }
    TokenKind::Comment(&cur.src[start..cur.pos])
}

fn raw_string_prefix(bytes: &[u8], pos: usize) -> Option<(usize, usize)> {
    let offset = match (bytes.get(pos).copied(), bytes.get(pos + 1).copied()) {
        (Some(b'r'), _) => 1,
        (Some(b'b' | b'c'), Some(b'r')) => 2,
        _ => return None,
    };
    let mut hashes = 0usize;
    while bytes.get(pos + offset + hashes) == Some(&b'#') {
        hashes += 1;
    }
    if bytes.get(pos + offset + hashes) == Some(&b'"') {
        Some((pos + offset + hashes, hashes))
    } else {
        None
    }
}

fn lex_raw_string<'a>(cur: &mut Cursor<'a>, quote_pos: usize, hashes: usize) -> TokenKind<'a> {
    cur.pos = quote_pos + 1;
    while cur.pos < cur.bytes.len() {
        let b = cur.advance_byte();
        if b == b'"' && (0..hashes).all(|i| cur.bytes.get(cur.pos + i) == Some(&b'#')) {
            cur.pos += hashes;
            break;
        }
    }
    TokenKind::Literal
}

fn lex_cooked_string<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    if cur.bytes[cur.pos] != b'"' {
        cur.pos += 1;
    }
    cur.pos += 1;
    while cur.pos < cur.bytes.len() {
        let b = cur.advance_byte();
        if b == b'\\' && cur.pos < cur.bytes.len() {
            cur.advance_byte();
        } else if b == b'"' {
            break;
        }
    }
    TokenKind::Literal
}

fn lex_byte_char<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    cur.pos += 2;
    consume_char_body_to_tick(cur);
    TokenKind::Literal
}

fn lex_char_or_lifetime<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    if cur.bytes.get(cur.pos + 1) == Some(&b'\\') {
        cur.pos += 1;
        consume_char_body_to_tick(cur);
        return TokenKind::Literal;
    }
    if let Some(ch) = cur.src[cur.pos + 1..].chars().next() {
        let close_pos = cur.pos + 1 + ch.len_utf8();
        if ch != '\'' && ch != '\n' && ch != '\r' && cur.bytes.get(close_pos) == Some(&b'\'') {
            cur.pos = close_pos + 1;
            return TokenKind::Literal;
        }
    }
    cur.pos += 1;
    if cur.src[cur.pos..].starts_with("r#") {
        cur.pos += 2;
    }
    consume_ident_continue(cur);
    TokenKind::Lifetime
}

fn consume_char_body_to_tick(cur: &mut Cursor<'_>) {
    while cur.pos < cur.bytes.len() {
        let b = cur.bytes[cur.pos];
        if b == b'\n' {
            break;
        }
        cur.advance_byte();
        if b == b'\\' && cur.pos < cur.bytes.len() && cur.bytes[cur.pos] != b'\n' {
            cur.advance_byte();
        } else if b == b'\'' {
            break;
        }
    }
}

fn lex_number<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    while cur.pos < cur.bytes.len() {
        let b = cur.bytes[cur.pos];
        if b.is_ascii_alphanumeric() || b == b'_' {
            cur.pos += 1;
        } else {
            break;
        }
    }
    TokenKind::Literal
}

fn try_lex_ident<'a>(cur: &mut Cursor<'a>) -> Option<&'a str> {
    let start = cur.pos;
    if cur.src[cur.pos..].starts_with("r#") {
        let after = cur.src[cur.pos + 2..].chars().next()?;
        if is_ident_start(after) {
            cur.pos += 2;
            cur.advance_char();
            consume_ident_continue(cur);
            return Some(&cur.src[start..cur.pos]);
        }
    }
    let first = cur.src[cur.pos..].chars().next()?;
    if !is_ident_start(first) {
        return None;
    }
    cur.advance_char();
    consume_ident_continue(cur);
    Some(&cur.src[start..cur.pos])
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_alphabetic()
}

fn consume_ident_continue(cur: &mut Cursor<'_>) {
    while let Some(ch) = cur.src[cur.pos..].chars().next() {
        if ch == '_' || ch.is_alphanumeric() {
            cur.pos += ch.len_utf8();
        } else {
            break;
        }
    }
}

fn lex_punct_or_delim<'a>(cur: &mut Cursor<'a>) -> TokenKind<'a> {
    let b = cur.bytes[cur.pos];
    let next = cur.bytes.get(cur.pos + 1).copied();
    if let Some(two) = match_two_byte_punct(b, next) {
        cur.pos += 2;
        return two;
    }
    cur.advance_char();
    match b {
        b'(' => TokenKind::Open(Delim::Paren),
        b')' => TokenKind::Close(Delim::Paren),
        b'[' => TokenKind::Open(Delim::Bracket),
        b']' => TokenKind::Close(Delim::Bracket),
        b'{' => TokenKind::Open(Delim::Brace),
        b'}' => TokenKind::Close(Delim::Brace),
        _ => TokenKind::Punct(b),
    }
}

fn match_two_byte_punct<'a>(b: u8, next: Option<u8>) -> Option<TokenKind<'a>> {
    match (b, next) {
        (b'-', Some(b'>')) => Some(TokenKind::Arrow),
        (b'=', Some(b'>')) => Some(TokenKind::FatArrow),
        (b':', Some(b':')) => Some(TokenKind::PathSep),
        (b'<', Some(b'<')) => Some(TokenKind::Shl),
        (b'>', Some(b'>')) => Some(TokenKind::Shr),
        (b'<', Some(b'=')) => Some(TokenKind::Le),
        (b'>', Some(b'=')) => Some(TokenKind::Ge),
        (b'=', Some(b'=')) => Some(TokenKind::EqEq),
        (b'!', Some(b'=')) => Some(TokenKind::Ne),
        _ => None,
    }
}
