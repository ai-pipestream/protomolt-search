//! Sentence-aware chunk assembly and its contiguity invariant.

use super::{tokens_in, Span};

/// Assemble sentence-aware chunks of ~`target_tokens` whitespace tokens
/// from one opinion's analysis.
///
/// Boundaries are sentence starts; whole sentences are packed while they
/// fit the target. A single sentence LONGER than the target is split at
/// its own token boundaries (truncation of the text is never involved, so
/// the contiguity invariant below cannot be broken by oversize input).
///
/// CONTIGUITY INVARIANT: the returned spans are contiguous and gap-free —
/// `spans[0].start == 0`, `spans.last().end == text.chars().count()`, and
/// `spans[i+1].start == spans[i].end` — so concatenating the chunk texts
/// in ordinal order reproduces the original text byte-for-byte.
pub fn assemble_chunks(
    text: &str,
    sentences: &[Span],
    tokens: &[Span],
    target_tokens: usize,
) -> Vec<Span> {
    let text_len = text.chars().count() as u32;
    if text_len == 0 {
        return Vec::new();
    }
    let target = target_tokens.max(1) as u32;
    let mut boundaries: Vec<u32> = vec![0];

    for sentence in sentences {
        let s_start = sentence.start;
        let s_end = sentence.end.min(text_len);
        if s_end <= s_start {
            continue;
        }
        let chunk_start = *boundaries.last().unwrap();
        let sentence_tokens = tokens_in(tokens, s_start, s_end);
        let current_tokens = tokens_in(tokens, chunk_start, s_end);

        if sentence_tokens as u32 > target {
            // Oversize sentence: close the current chunk at the sentence
            // start, then split the sentence at token boundaries.
            if chunk_start < s_start {
                boundaries.push(s_start);
            }
            let mut split_start = s_start;
            let sentence_token_spans: Vec<Span> = tokens
                .iter()
                .filter(|t| t.start >= s_start && t.start < s_end)
                .copied()
                .collect();
            for window in sentence_token_spans.chunks(target as usize) {
                let split_end = if window.len() as u32 == target {
                    // Next chunk starts at the token after this window.
                    let next = sentence_token_spans
                        .iter()
                        .find(|t| t.start >= window[window.len() - 1].end);
                    next.map(|t| t.start).unwrap_or(s_end)
                } else {
                    s_end
                };
                if split_end > split_start {
                    boundaries.push(split_end);
                }
                split_start = split_end;
            }
        } else if current_tokens as u32 > target && chunk_start < s_start {
            // Adding this sentence would exceed the target: close the
            // current chunk at the sentence start.
            boundaries.push(s_start);
        }
    }
    if *boundaries.last().unwrap() < text_len {
        boundaries.push(text_len);
    }

    boundaries
        .windows(2)
        .map(|w| Span {
            start: w[0],
            end: w[1],
        })
        .filter(|s| s.end > s.start)
        .collect()
}

/// Verify the contiguity invariant on assembled spans (used by the
/// pipeline's per-opinion assertion and by tests).
pub fn is_contiguous(text: &str, spans: &[Span]) -> bool {
    if spans.is_empty() {
        return text.is_empty();
    }
    let text_len = text.chars().count() as u32;
    spans[0].start == 0
        && spans.last().unwrap().end == text_len
        && spans.windows(2).all(|w| w[0].end == w[1].start)
}
