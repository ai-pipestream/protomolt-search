//! Sentence and token boundaries in the requested offset unit.

use super::*;

/// Newline sentence detection: one span per line that holds any
/// non-whitespace character, trimmed at both ends, in `offset_unit`.
/// `\r`, `\n`, and `\r\n` all end a line.
pub(crate) fn split_sentences(text: &str, offset_unit: OffsetUnit) -> Vec<Span> {
    let mut sentences = Vec::new();
    // (utf16, byte) of the first non-whitespace character on this line.
    let mut start: Option<(u32, usize)> = None;
    // Just past the last non-whitespace character seen on this line.
    let mut last_end = (0u32, 0usize);
    let mut utf16 = 0u32;
    for (byte, ch) in text.char_indices() {
        if ch == '\n' || ch == '\r' {
            if let Some((start_utf16, start_byte)) = start.take() {
                sentences.push(Span {
                    start: offset_unit.position(start_utf16, start_byte),
                    end: offset_unit.position(last_end.0, last_end.1),
                });
            }
        } else if !is_unicode_whitespace(ch) {
            if start.is_none() {
                start = Some((utf16, byte));
            }
            last_end = (utf16 + ch.len_utf16() as u32, byte + ch.len_utf8());
        }
        utf16 += ch.len_utf16() as u32;
    }
    if let Some((start_utf16, start_byte)) = start {
        sentences.push(Span {
            start: offset_unit.position(start_utf16, start_byte),
            end: offset_unit.position(last_end.0, last_end.1),
        });
    }
    sentences
}

pub(crate) fn tokenize_whitespace(text: &str, offset_unit: OffsetUnit) -> Vec<Token<'_>> {
    let mut tokens = Vec::new();
    let mut start_byte = None;
    let mut start_utf16 = 0u32;
    let mut utf16 = 0u32;

    for (byte, ch) in text.char_indices() {
        if is_unicode_whitespace(ch) {
            if let Some(start) = start_byte.take() {
                tokens.push(Token {
                    surface: &text[start..byte],
                    span: Span {
                        start: offset_unit.position(start_utf16, start),
                        end: offset_unit.position(utf16, byte),
                    },
                });
            }
        } else if start_byte.is_none() {
            start_byte = Some(byte);
            start_utf16 = utf16;
        }
        utf16 += ch.len_utf16() as u32;
    }
    if let Some(start) = start_byte {
        tokens.push(Token {
            surface: &text[start..],
            span: Span {
                start: offset_unit.position(start_utf16, start),
                end: offset_unit.position(utf16, text.len()),
            },
        });
    }
    tokens
}

pub(crate) fn tokenize_uax29(text: &str, offset_unit: OffsetUnit) -> Vec<Token<'_>> {
    const MAX_TOKEN_UTF16: u32 = 255;
    let pictographic = CodePointSetData::new::<ExtendedPictographic>();
    let mut tokens = Vec::new();
    let mut utf16 = 0u32;
    let mut last_byte = 0usize;

    for (byte, segment) in text.split_word_bound_indices() {
        utf16 += text[last_byte..byte].encode_utf16().count() as u32;
        let segment_utf16 = segment.encode_utf16().count() as u32;
        if segment.chars().any(|ch| {
            ch.is_alphanumeric()
                || pictographic.contains(ch)
                || matches!(ch as u32, 0x1F1E6..=0x1F1FF)
        }) {
            let mut chunk_byte = byte;
            let mut chunk_utf16 = utf16;
            let mut used = 0u32;
            for (relative, ch) in segment.char_indices() {
                let units = ch.len_utf16() as u32;
                if used != 0 && used + units > MAX_TOKEN_UTF16 {
                    let end = byte + relative;
                    tokens.push(Token {
                        surface: &text[chunk_byte..end],
                        span: Span {
                            start: offset_unit.position(chunk_utf16, chunk_byte),
                            end: offset_unit.position(chunk_utf16 + used, end),
                        },
                    });
                    chunk_byte = end;
                    chunk_utf16 += used;
                    used = 0;
                }
                used += units;
            }
            if used != 0 {
                tokens.push(Token {
                    surface: &text[chunk_byte..byte + segment.len()],
                    span: Span {
                        start: offset_unit.position(chunk_utf16, chunk_byte),
                        end: offset_unit.position(chunk_utf16 + used, byte + segment.len()),
                    },
                });
            }
        }
        utf16 += segment_utf16;
        last_byte = byte + segment.len();
    }
    tokens
}
