//! The tokenizer half of the model: `BertNormalizer`, `BertPreTokenizer`,
//! and greedy WordPiece, oracle-exact against HF tokenizers.

use std::collections::HashMap;

use icu_normalizer::DecomposingNormalizerBorrowed;
use icu_properties::props::GeneralCategory;
use icu_properties::CodePointMapData;

pub(crate) fn general_category(ch: char) -> GeneralCategory {
    CodePointMapData::<GeneralCategory>::new().get(ch)
}

pub(crate) fn nfd(text: &str) -> String {
    DecomposingNormalizerBorrowed::new_nfd()
        .normalize(text)
        .into_owned()
}

pub(crate) fn is_control(c: char) -> bool {
    if c == '\t' || c == '\n' || c == '\r' {
        return false;
    }
    matches!(
        general_category(c),
        GeneralCategory::Control
            | GeneralCategory::Format
            | GeneralCategory::PrivateUse
            | GeneralCategory::Surrogate
            | GeneralCategory::Unassigned
    )
}

pub(crate) fn is_ws(c: char) -> bool {
    c == ' '
        || c == '\t'
        || c == '\n'
        || c == '\r'
        || general_category(c) == GeneralCategory::SpaceSeparator
}

pub(crate) fn is_punct(c: char) -> bool {
    c.is_ascii_punctuation()
        || matches!(
            general_category(c),
            GeneralCategory::ConnectorPunctuation
                | GeneralCategory::DashPunctuation
                | GeneralCategory::OpenPunctuation
                | GeneralCategory::ClosePunctuation
                | GeneralCategory::InitialPunctuation
                | GeneralCategory::FinalPunctuation
                | GeneralCategory::OtherPunctuation
        )
}

/// The BERT "Chinese character" ranges: CJK ideographs and compatibility
/// ideographs, NOT kana or Hangul — those stay inside their words, which is
/// why 日本語テスト splits the ideographs but keeps テスト whole.
pub(crate) fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x4E00..=0x9FFF | 0x3400..=0x4DBF | 0x20000..=0x2A6DF | 0x2A700..=0x2B73F
        | 0x2B740..=0x2B81F | 0x2B820..=0x2CEAF | 0xF900..=0xFAFF | 0x2F800..=0x2FA1F)
}

/// `BertNormalizer { clean_text, handle_chinese_chars, strip_accents: null,
/// lowercase: true }` — the configuration saved in potion-retrieval-32M's
/// tokenizer.json. A null strip_accents resolves to the lowercase flag,
/// matching HF's `unwrap_or(self.lowercase)`.
pub fn normalize(text: &str) -> String {
    let mut cleaned = String::with_capacity(text.len());
    for c in text.chars() {
        if c == '\0' || c == '\u{fffd}' || is_control(c) {
            continue;
        }
        if is_ws(c) {
            cleaned.push(' ');
        } else if is_cjk(c) {
            cleaned.push(' ');
            cleaned.push(c);
            cleaned.push(' ');
        } else {
            cleaned.push(c);
        }
    }
    // Strip accents: NFD, then drop nonspacing marks.
    let decomposed = nfd(&cleaned);
    decomposed
        .chars()
        .filter(|c| general_category(*c) != GeneralCategory::NonspacingMark)
        .flat_map(char::to_lowercase)
        .collect()
}

/// `BertPreTokenizer`: split on whitespace, then isolate every punctuation
/// character as its own pre-token.
pub fn pre_tokenize(normalized: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for c in normalized.chars() {
        if c == ' ' {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
        } else if is_punct(c) {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            words.push(c.to_string());
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// Greedy longest-match WordPiece over one pre-token. A word longer than
/// `max_word_chars`, or any position with no vocabulary match, collapses the
/// WHOLE word to a single `[UNK]` — partial matches are discarded, per the
/// reference algorithm.
pub fn wordpiece(
    vocab: &HashMap<String, u32>,
    unk_id: u32,
    max_word_chars: usize,
    word: &str,
    out: &mut Vec<u32>,
) {
    let chars: Vec<char> = word.chars().collect();
    if chars.len() > max_word_chars {
        out.push(unk_id);
        return;
    }
    let mut ids = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let mut end = chars.len();
        let mut found = None;
        while end > start {
            let piece: String = chars[start..end].iter().collect();
            let key = if start > 0 {
                format!("##{piece}")
            } else {
                piece
            };
            if let Some(&id) = vocab.get(&key) {
                found = Some(id);
                break;
            }
            end -= 1;
        }
        match found {
            Some(id) => {
                ids.push(id);
                start = end;
            }
            None => {
                out.push(unk_id);
                return;
            }
        }
    }
    out.extend(ids);
}
