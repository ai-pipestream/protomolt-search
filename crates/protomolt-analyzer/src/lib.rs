//! Cross-platform lexical analysis with OpenNLP-compatible term identity.
//!
//! The crate is intentionally independent of gRPC, Tokio, TurboVec, and the
//! search server. It is the embeddable analysis core for native clients and
//! for the in-process search backend.

mod porter;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use icu_casemap::CaseMapper;
use icu_normalizer::{ComposingNormalizerBorrowed, DecomposingNormalizerBorrowed};
use icu_properties::props::{ExtendedPictographic, GeneralCategory, Script};
use icu_properties::{CodePointMapData, CodePointSetData};
use unicode_segmentation::UnicodeSegmentation;

/// Maximum accepted UTF-8 input size, matching the OpenNLP analysis service.
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

/// Token boundary algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tokenizer {
    /// Split on the Unicode `White_Space` property.
    Whitespace,
    /// Unicode Standard Annex #29 word boundaries with OpenNLP-compatible
    /// filtering of punctuation-only segments.
    Uax29,
}

/// Token stemming algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stemmer {
    /// Preserve the selected token identity without stemming.
    None,
    /// Classic Porter English stemming, not Porter2/Snowball.
    Porter,
}

/// Amount of occurrence data retained for each term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermVectorMode {
    /// Frequencies and original-text UTF-16 occurrence spans.
    Full,
    /// Frequencies only.
    ScoringOnly,
}

/// Stage that defines a term's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermVectorSource {
    /// Normalized token text.
    Tokens,
    /// Stem of the original token surface form. Normalizers are ignored.
    Stems,
    /// Stem of the normalized token text.
    NormalizedStems,
}

/// OpenNLP-compatible normalizer stages supported by the native analyzer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NormalizerStep {
    /// Remove the explicit OpenNLP invisible/control set.
    StripInvisible,
    /// Collapse and trim Unicode whitespace.
    Whitespace,
    /// Remove diacritics from Latin, Greek, and Cyrillic base characters.
    AccentFold,
    /// Apply Unicode full, non-Turkic case folding.
    FullCaseFold,
}

/// Complete term-identity contract for one analysis pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisSpec {
    pub tokenizer: Tokenizer,
    pub stemmer: Stemmer,
    pub term_vector_mode: TermVectorMode,
    pub term_vector_source: TermVectorSource,
    pub normalizers: Vec<NormalizerStep>,
}

/// Half-open UTF-16 span in the original input text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

/// One distinct term, preserving first-occurrence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermVector {
    pub term: String,
    pub frequency: u32,
    pub occurrences: Vec<Span>,
}

/// Native analysis result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzedDocument {
    pub term_vectors: Vec<TermVector>,
    /// Token surface forms in input order, used only by ingest vocabulary
    /// accounting. Query callers may ignore this field.
    pub tokens: Vec<String>,
    /// Sum of retained term frequencies.
    pub length: u32,
}

/// One registered glossary surface form. Multiple entries may share an id to
/// represent aliases of the same concept.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GlossaryEntry {
    pub id: String,
    pub term: String,
}

/// One glossary occurrence in original-text UTF-16 coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlossaryMatch {
    pub id: String,
    pub registered_term: String,
    pub span: Span,
    /// Number of UAX #29 tokens in the registered surface form.
    pub token_count: u32,
}

/// Compiled, cross-platform phrase vocabulary.
///
/// Matching is Unicode case-folded when requested, requires Unicode word
/// boundaries, and can expose either leftmost-longest non-overlapping matches
/// or every explicitly registered overlapping concept for phrase indexing.
#[derive(Clone)]
pub struct Glossary {
    entries: Vec<GlossaryEntry>,
    token_counts: Vec<u32>,
    matcher: AhoCorasick,
    /// Entry indexes for each distinct matcher pattern. More than one concept
    /// may intentionally register the same normalized surface form.
    pattern_entries: Vec<Vec<usize>>,
    ignore_case: bool,
    fingerprint: u64,
}

impl fmt::Debug for Glossary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Glossary")
            .field("entries", &self.entries)
            .field("ignore_case", &self.ignore_case)
            .field("fingerprint", &format_args!("{:016x}", self.fingerprint))
            .finish_non_exhaustive()
    }
}

impl Glossary {
    /// Compile a vocabulary. Exact duplicate `(id, term)` pairs, empty values,
    /// and punctuation-only terms are rejected so the fingerprint and posting
    /// identity remain unambiguous.
    pub fn new(entries: Vec<GlossaryEntry>, ignore_case: bool) -> Result<Self, AnalysisError> {
        if entries.is_empty() {
            return Err(AnalysisError::new("glossary contains no entries"));
        }
        let mut seen = HashSet::new();
        let mut patterns = Vec::<String>::with_capacity(entries.len());
        let mut pattern_indexes = HashMap::<String, usize>::new();
        let mut pattern_entries = Vec::<Vec<usize>>::new();
        let mut token_counts = Vec::with_capacity(entries.len());
        for (entry_index, entry) in entries.iter().enumerate() {
            if entry.id.is_empty() || entry.term.is_empty() {
                return Err(AnalysisError::new(
                    "glossary ids and registered terms must not be empty",
                ));
            }
            if !seen.insert((entry.id.clone(), entry.term.clone())) {
                return Err(AnalysisError::new(format!(
                    "duplicate glossary entry ({:?}, {:?})",
                    entry.id, entry.term
                )));
            }
            let pattern = if ignore_case {
                full_case_fold(&entry.term)
            } else {
                entry.term.clone()
            };
            if pattern.is_empty() {
                return Err(AnalysisError::new(format!(
                    "glossary term {:?} is empty after normalization",
                    entry.term
                )));
            }
            let count = tokenize_uax29(&entry.term).len() as u32;
            if count == 0 {
                return Err(AnalysisError::new(format!(
                    "glossary term {:?} contains no indexable UAX #29 token",
                    entry.term
                )));
            }
            if let Some(&pattern_index) = pattern_indexes.get(&pattern) {
                pattern_entries[pattern_index].push(entry_index);
            } else {
                let pattern_index = patterns.len();
                pattern_indexes.insert(pattern.clone(), pattern_index);
                patterns.push(pattern);
                pattern_entries.push(vec![entry_index]);
            }
            token_counts.push(count);
        }
        let matcher = AhoCorasickBuilder::new()
            .match_kind(MatchKind::Standard)
            .build(&patterns)
            .map_err(|error| AnalysisError::new(format!("invalid glossary: {error}")))?;
        let fingerprint = glossary_fingerprint(&entries, ignore_case);
        Ok(Self {
            entries,
            token_counts,
            matcher,
            pattern_entries,
            ignore_case,
            fingerprint,
        })
    }

    /// Stable, entry-order-independent vocabulary identity.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Leftmost-longest non-overlapping matches for annotation consumers.
    pub fn matches(&self, text: &str) -> Vec<GlossaryMatch> {
        let mut matches = self.all_valid_matches(text);
        matches.sort_by(|a, b| {
            a.span
                .start
                .cmp(&b.span.start)
                .then_with(|| b.span.end.cmp(&a.span.end))
                .then_with(|| a.id.cmp(&b.id))
                .then_with(|| a.registered_term.cmp(&b.registered_term))
        });
        let mut end = 0;
        matches
            .into_iter()
            .filter(|candidate| {
                if candidate.span.start < end {
                    false
                } else {
                    end = candidate.span.end;
                    true
                }
            })
            .collect()
    }

    /// Every explicitly registered occurrence, including nested concepts such
    /// as both `new york` and `new york city`. No unregistered shingle is
    /// synthesized.
    pub fn index_matches(&self, text: &str) -> Vec<GlossaryMatch> {
        let mut matches = self.all_valid_matches(text);
        matches.sort_by(|a, b| {
            a.span
                .start
                .cmp(&b.span.start)
                .then_with(|| b.span.end.cmp(&a.span.end))
                .then_with(|| a.id.cmp(&b.id))
                .then_with(|| a.registered_term.cmp(&b.registered_term))
        });
        matches
    }

    fn all_valid_matches(&self, text: &str) -> Vec<GlossaryMatch> {
        let folded = FoldedText::new(text, self.ignore_case);
        let mut matches = Vec::new();
        for matched in self.matcher.find_overlapping_iter(&folded.text) {
            let Some((start_utf16, start_byte)) = folded.original_boundary(matched.start()) else {
                continue;
            };
            let Some((end_utf16, end_byte)) = folded.original_boundary(matched.end()) else {
                continue;
            };
            if !is_word_boundary(text, start_byte) || !is_word_boundary(text, end_byte) {
                continue;
            }
            for &entry_index in &self.pattern_entries[matched.pattern().as_usize()] {
                let entry = &self.entries[entry_index];
                matches.push(GlossaryMatch {
                    id: entry.id.clone(),
                    registered_term: entry.term.clone(),
                    span: Span {
                        start: start_utf16,
                        end: end_utf16,
                    },
                    token_count: self.token_counts[entry_index],
                });
            }
        }
        matches
    }
}

/// Collision-free posting identity for a glossary concept id.
pub fn phrase_posting_term(id: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(8 + id.len() * 2);
    out.push_str("$phrase:");
    for byte in id.bytes() {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// A request the native implementation cannot analyze without changing the
/// OpenNLP contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisError(String);

impl AnalysisError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for AnalysisError {}

#[derive(Debug)]
struct Token<'a> {
    surface: &'a str,
    span: Span,
}

/// Analyze one document. Terms are emitted in first-occurrence order and
/// occurrence spans use Java/OpenNLP UTF-16 coordinates.
pub fn analyze(text: &str, spec: &AnalysisSpec) -> Result<AnalyzedDocument, AnalysisError> {
    validate(text, spec)?;
    let tokens = match spec.tokenizer {
        Tokenizer::Whitespace => tokenize_whitespace(text),
        Tokenizer::Uax29 => tokenize_uax29(text),
    };
    let mut vectors = Vec::<TermVector>::new();
    let mut positions = HashMap::<String, usize>::new();

    for token in &tokens {
        let term = term_identity(token.surface, spec);
        if term.is_empty() {
            continue;
        }
        if let Some(&index) = positions.get(&term) {
            let vector = &mut vectors[index];
            vector.frequency += 1;
            if spec.term_vector_mode == TermVectorMode::Full {
                vector.occurrences.push(token.span);
            }
        } else {
            let index = vectors.len();
            positions.insert(term.clone(), index);
            vectors.push(TermVector {
                term,
                frequency: 1,
                occurrences: if spec.term_vector_mode == TermVectorMode::Full {
                    vec![token.span]
                } else {
                    Vec::new()
                },
            });
        }
    }

    let length = vectors.iter().map(|vector| vector.frequency).sum();
    Ok(AnalyzedDocument {
        term_vectors: vectors,
        tokens: tokens
            .into_iter()
            .map(|token| token.surface.to_string())
            .collect(),
        length,
    })
}

fn validate(text: &str, spec: &AnalysisSpec) -> Result<(), AnalysisError> {
    if text.is_empty() {
        return Err(AnalysisError::new("empty document text"));
    }
    if text.len() > MAX_TEXT_BYTES {
        return Err(AnalysisError::new(format!(
            "document of {} bytes exceeds the {}-byte cap",
            text.len(),
            MAX_TEXT_BYTES
        )));
    }
    if matches!(
        spec.term_vector_source,
        TermVectorSource::Stems | TermVectorSource::NormalizedStems
    ) && spec.stemmer == Stemmer::None
    {
        return Err(AnalysisError::new(
            "term vector source STEMS requires a stemmer other than STEMMER_NONE",
        ));
    }
    Ok(())
}

fn term_identity(surface: &str, spec: &AnalysisSpec) -> String {
    match spec.term_vector_source {
        TermVectorSource::Tokens => normalize(surface, &spec.normalizers),
        TermVectorSource::Stems => stem(surface, spec.stemmer),
        TermVectorSource::NormalizedStems => {
            stem(&normalize(surface, &spec.normalizers), spec.stemmer)
        }
    }
}

fn stem(token: &str, stemmer: Stemmer) -> String {
    match stemmer {
        Stemmer::None => token.to_string(),
        Stemmer::Porter => porter::stem(token),
    }
}

/// OpenNLP's builder applies a fixed semantic order independent of the order
/// in which enum values appear on the wire. Duplicate values are idempotent.
fn normalize(token: &str, steps: &[NormalizerStep]) -> String {
    let has = |step| steps.contains(&step);
    let mut value = token.to_string();
    if has(NormalizerStep::StripInvisible) {
        value = strip_invisible(&value);
    }
    if has(NormalizerStep::Whitespace) {
        value = normalize_whitespace(&value);
    }
    if has(NormalizerStep::AccentFold) {
        value = accent_fold(&value);
    }
    if has(NormalizerStep::FullCaseFold) {
        value = full_case_fold(&value);
    }
    value
}

fn full_case_fold(text: &str) -> String {
    // JDK 25's normalization and character-property tables are Unicode 16,
    // while OpenNLP deliberately bundles Unicode 17's CaseFolding.txt. ICU4X
    // 2.0 supplies the former. These are the latter table's new common folds.
    CaseMapper::new()
        .fold_string(text)
        .chars()
        .map(|ch| match ch {
            '\u{A7CE}' => '\u{A7CF}',
            '\u{A7D2}' => '\u{A7D3}',
            '\u{A7D4}' => '\u{A7D5}',
            '\u{16EA0}'..='\u{16EB8}' => {
                char::from_u32(ch as u32 + 0x1B).expect("Unicode 17 Beria Erfe fold is a scalar")
            }
            _ => ch,
        })
        .collect()
}

/// Unicode full, non-Turkic case folding used by persisted product identities.
pub fn unicode_case_fold(text: &str) -> String {
    full_case_fold(text)
}

fn tokenize_whitespace(text: &str) -> Vec<Token<'_>> {
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
                        start: start_utf16,
                        end: utf16,
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
                start: start_utf16,
                end: utf16,
            },
        });
    }
    tokens
}

fn tokenize_uax29(text: &str) -> Vec<Token<'_>> {
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
                            start: chunk_utf16,
                            end: chunk_utf16 + used,
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
                        start: chunk_utf16,
                        end: chunk_utf16 + used,
                    },
                });
            }
        }
        utf16 += segment_utf16;
        last_byte = byte + segment.len();
    }
    tokens
}

struct FoldedText {
    text: String,
    /// A folded UTF-8 byte boundary is mapped only when it is also an original
    /// scalar boundary. This prevents a one-character pattern from matching
    /// half of an expanding fold such as `ß -> ss`.
    boundaries: Vec<Option<(u32, usize)>>,
}

impl FoldedText {
    fn new(text: &str, fold: bool) -> Self {
        if !fold {
            let mut boundaries = vec![None; text.len() + 1];
            let mut utf16 = 0u32;
            boundaries[0] = Some((0, 0));
            for (byte, ch) in text.char_indices() {
                boundaries[byte] = Some((utf16, byte));
                utf16 += ch.len_utf16() as u32;
                let end = byte + ch.len_utf8();
                boundaries[end] = Some((utf16, end));
            }
            return Self {
                text: text.to_string(),
                boundaries,
            };
        }

        let mut folded = String::with_capacity(text.len());
        let mut boundaries = vec![Some((0, 0))];
        let mut utf16 = 0u32;
        for (original_byte, ch) in text.char_indices() {
            let start = folded.len();
            let value = full_case_fold(ch.encode_utf8(&mut [0; 4]));
            folded.push_str(&value);
            boundaries.resize(folded.len() + 1, None);
            boundaries[start] = Some((utf16, original_byte));
            utf16 += ch.len_utf16() as u32;
            boundaries[folded.len()] = Some((utf16, original_byte + ch.len_utf8()));
        }
        Self {
            text: folded,
            boundaries,
        }
    }

    fn original_boundary(&self, folded_byte: usize) -> Option<(u32, usize)> {
        self.boundaries.get(folded_byte).copied().flatten()
    }
}

fn is_word_boundary(text: &str, byte: usize) -> bool {
    let left = text[..byte].chars().next_back();
    let right = text[byte..].chars().next();
    !left.is_some_and(is_word_char) || !right.is_some_and(is_word_char)
}

fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

fn glossary_fingerprint(entries: &[GlossaryEntry], ignore_case: bool) -> u64 {
    let mut canonical = entries
        .iter()
        .map(|entry| (entry.id.as_bytes(), entry.term.as_bytes()))
        .collect::<Vec<_>>();
    canonical.sort_unstable();
    let mut hash = 0xcbf29ce484222325u64;
    for byte in b"protomolt-glossary-v1" {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
    hash = (hash ^ u64::from(ignore_case)).wrapping_mul(0x100000001b3);
    for (id, term) in canonical {
        for bytes in [id, term] {
            for byte in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
                hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        }
    }
    hash
}

fn is_unicode_whitespace(ch: char) -> bool {
    matches!(
        ch as u32,
        0x0009..=0x000D
            | 0x0020
            | 0x0085
            | 0x00A0
            | 0x1680
            | 0x2000..=0x200A
            | 0x2028
            | 0x2029
            | 0x202F
            | 0x205F
            | 0x3000
    )
}

fn strip_invisible(text: &str) -> String {
    text.chars()
        .filter(|ch| {
            !matches!(
                *ch as u32,
                0x00AD
                    | 0x061C
                    | 0x200B
                    | 0x200E
                    | 0x200F
                    | 0x202A..=0x202E
                    | 0x2060..=0x2064
                    | 0x2066..=0x2069
                    | 0xFEFF
            )
        })
        .collect()
}

fn normalize_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        if is_unicode_whitespace(ch) {
            pending_space = !out.is_empty();
        } else {
            if pending_space {
                out.push(' ');
                pending_space = false;
            }
            out.push(ch);
        }
    }
    out
}

fn accent_fold(text: &str) -> String {
    let decomposed = DecomposingNormalizerBorrowed::new_nfd().normalize(text);
    let categories = CodePointMapData::<GeneralCategory>::new();
    let scripts = CodePointMapData::<Script>::new();
    let mut out = String::with_capacity(decomposed.len());
    let mut base_script = None;

    for ch in decomposed.chars() {
        if categories.get(ch) == GeneralCategory::NonspacingMark {
            if !base_script.is_some_and(is_folded_script) {
                out.push(ch);
            }
            continue;
        }
        if let Some(mapped) = stroke_letter(ch) {
            out.push_str(mapped);
            base_script = Some(Script::Latin);
        } else {
            out.push(ch);
            base_script = Some(scripts.get(ch));
        }
    }
    ComposingNormalizerBorrowed::new_nfc()
        .normalize(&out)
        .into_owned()
}

fn is_folded_script(script: Script) -> bool {
    matches!(script, Script::Latin | Script::Greek | Script::Cyrillic)
}

fn stroke_letter(ch: char) -> Option<&'static str> {
    Some(match ch {
        '\u{00F8}' => "o",
        '\u{00D8}' => "O",
        '\u{00E6}' => "ae",
        '\u{00C6}' => "AE",
        '\u{0153}' => "oe",
        '\u{0152}' => "OE",
        '\u{00DF}' => "ss",
        '\u{1E9E}' => "SS",
        '\u{00FE}' => "th",
        '\u{00DE}' => "TH",
        '\u{00F0}' => "d",
        '\u{00D0}' => "D",
        '\u{0111}' => "d",
        '\u{0110}' => "D",
        '\u{0142}' => "l",
        '\u{0141}' => "L",
        '\u{0127}' => "h",
        '\u{0126}' => "H",
        '\u{0131}' => "i",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn folded() -> AnalysisSpec {
        AnalysisSpec {
            tokenizer: Tokenizer::Whitespace,
            stemmer: Stemmer::Porter,
            term_vector_mode: TermVectorMode::Full,
            term_vector_source: TermVectorSource::NormalizedStems,
            normalizers: vec![
                NormalizerStep::StripInvisible,
                NormalizerStep::Whitespace,
                NormalizerStep::AccentFold,
                NormalizerStep::FullCaseFold,
            ],
        }
    }

    #[test]
    fn folded_terms_group_and_keep_utf16_offsets() {
        let doc = analyze("😀 Running Rodríguez running", &folded()).unwrap();
        assert_eq!(
            doc.term_vectors,
            vec![
                TermVector {
                    term: "😀".into(),
                    frequency: 1,
                    occurrences: vec![Span { start: 0, end: 2 }],
                },
                TermVector {
                    term: "run".into(),
                    frequency: 2,
                    occurrences: vec![Span { start: 3, end: 10 }, Span { start: 21, end: 28 }],
                },
                TermVector {
                    term: "rodriguez".into(),
                    frequency: 1,
                    occurrences: vec![Span { start: 11, end: 20 }],
                },
            ]
        );
        assert_eq!(doc.length, 4);
    }

    #[test]
    fn full_case_fold_is_non_turkic_and_expanding() {
        assert_eq!(
            normalize("Maße İ", &[NormalizerStep::FullCaseFold]),
            "masse i\u{307}"
        );
        assert_eq!(
            full_case_fold("\u{A7CE}\u{A7D2}\u{A7D4}\u{16EA0}\u{16EB8}"),
            "\u{A7CF}\u{A7D3}\u{A7D5}\u{16EBB}\u{16ED3}"
        );
    }

    #[test]
    fn accent_fold_keeps_marks_on_non_folded_scripts() {
        assert_eq!(accent_fold("café ά й"), "cafe α и");
        assert_eq!(accent_fold("بَ אָ"), "بَ אָ");
    }

    #[test]
    fn whitespace_matches_the_complete_unicode_property() {
        let separators: String = [
            '\u{0009}', '\u{000A}', '\u{000B}', '\u{000C}', '\u{000D}', '\u{0020}', '\u{0085}',
            '\u{00A0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}',
            '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200A}', '\u{2028}',
            '\u{2029}', '\u{202F}', '\u{205F}', '\u{3000}',
        ]
        .into_iter()
        .collect();
        let text = format!("a{}b", separators);
        let doc = analyze(&text, &folded()).unwrap();
        assert_eq!(doc.tokens, ["a", "b"]);
    }

    #[test]
    fn scoring_only_omits_offsets() {
        let mut spec = folded();
        spec.term_vector_mode = TermVectorMode::ScoringOnly;
        let doc = analyze("running runs run", &spec).unwrap();
        assert_eq!(doc.term_vectors[0].term, "run");
        assert_eq!(doc.term_vectors[0].frequency, 3);
        assert!(doc.term_vectors[0].occurrences.is_empty());
    }

    #[test]
    fn uax29_matches_words_ideographs_emoji_and_utf16_offsets() {
        let mut spec = folded();
        spec.tokenizer = Tokenizer::Uax29;
        spec.stemmer = Stemmer::None;
        spec.term_vector_source = TermVectorSource::Tokens;
        let doc = analyze("😀 U.S. 東京 カタカナ hot-dog!", &spec).unwrap();
        assert_eq!(
            doc.tokens,
            ["😀", "U.S", "東", "京", "カタカナ", "hot", "dog"]
        );
        assert_eq!(doc.term_vectors[0].occurrences, [Span { start: 0, end: 2 }]);
        assert_eq!(doc.term_vectors[1].term, "u.s");
    }

    #[test]
    fn uax29_splits_long_tokens_without_splitting_surrogate_pairs() {
        let mut spec = folded();
        spec.tokenizer = Tokenizer::Uax29;
        spec.stemmer = Stemmer::None;
        spec.term_vector_source = TermVectorSource::Tokens;
        let text = format!("{}𐐀b", "a".repeat(254));
        let doc = analyze(&text, &spec).unwrap();
        assert_eq!(doc.tokens[0].encode_utf16().count(), 254);
        assert_eq!(doc.tokens[1], "𐐀b");
    }

    fn glossary() -> Glossary {
        Glossary::new(
            vec![
                GlossaryEntry {
                    id: "nyc".into(),
                    term: "New York City".into(),
                },
                GlossaryEntry {
                    id: "new-york".into(),
                    term: "New York".into(),
                },
                GlossaryEntry {
                    id: "nyc".into(),
                    term: "NYC".into(),
                },
                GlossaryEntry {
                    id: "hot-dog".into(),
                    term: "hot dog".into(),
                },
            ],
            true,
        )
        .unwrap()
    }

    #[test]
    fn glossary_indexes_only_registered_overlapping_concepts() {
        let matches = glossary().index_matches("A NEW YORK CITY hot dog stand");
        assert_eq!(
            matches
                .iter()
                .map(|item| (item.id.as_str(), item.token_count))
                .collect::<Vec<_>>(),
            [("nyc", 3), ("new-york", 2), ("hot-dog", 2)]
        );
        assert_eq!(glossary().matches("New York City").len(), 1);
        assert_eq!(glossary().matches("New York City")[0].id, "nyc");
    }

    #[test]
    fn glossary_preserves_original_utf16_spans_across_folding() {
        let glossary = Glossary::new(
            vec![GlossaryEntry {
                id: "street".into(),
                term: "STRASSE".into(),
            }],
            true,
        )
        .unwrap();
        let found = glossary.matches("😀 Straße!");
        assert_eq!(found[0].span, Span { start: 3, end: 9 });
        assert!(glossary.matches("Grosses").is_empty());
    }

    #[test]
    fn glossary_requires_word_boundaries() {
        let glossary = Glossary::new(
            vec![GlossaryEntry {
                id: "york".into(),
                term: "York".into(),
            }],
            true,
        )
        .unwrap();
        assert!(glossary.matches("New Yorkshire").is_empty());
        assert_eq!(glossary.matches("New York").len(), 1);
    }

    #[test]
    fn glossary_keeps_every_concept_for_a_shared_normalized_surface() {
        let glossary = Glossary::new(
            vec![
                GlossaryEntry {
                    id: "city".into(),
                    term: "New York City".into(),
                },
                GlossaryEntry {
                    id: "metro-area".into(),
                    term: "NEW YORK CITY".into(),
                },
            ],
            true,
        )
        .unwrap();
        assert_eq!(
            glossary
                .index_matches("New York City")
                .into_iter()
                .map(|found| found.id)
                .collect::<Vec<_>>(),
            ["city", "metro-area"]
        );
    }

    #[test]
    fn glossary_fingerprint_is_order_independent_and_alias_sensitive() {
        let mut entries = glossary().entries.clone();
        let forward = Glossary::new(entries.clone(), true).unwrap().fingerprint();
        entries.reverse();
        assert_eq!(
            forward,
            Glossary::new(entries.clone(), true).unwrap().fingerprint()
        );
        entries[0].term.push('!');
        assert_ne!(forward, Glossary::new(entries, true).unwrap().fingerprint());
        assert_ne!(phrase_posting_term("a:b"), phrase_posting_term("a") + ":b");
    }
}
