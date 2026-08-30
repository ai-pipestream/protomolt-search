//! Cross-platform lexical analysis with OpenNLP-compatible term identity.
//!
//! The crate is intentionally independent of gRPC, Tokio, TurboVec, and the
//! search server. It is the embeddable analysis core for native clients and
//! for the in-process search backend.

mod porter;

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

use icu_casemap::CaseMapper;
use icu_normalizer::{ComposingNormalizerBorrowed, DecomposingNormalizerBorrowed};
use icu_properties::props::{GeneralCategory, Script};
use icu_properties::CodePointMapData;

/// Maximum accepted UTF-8 input size, matching the OpenNLP analysis service.
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

/// Token boundary algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tokenizer {
    /// Split on the Unicode `White_Space` property.
    Whitespace,
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
    let tokens = tokenize_whitespace(text);
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
}
