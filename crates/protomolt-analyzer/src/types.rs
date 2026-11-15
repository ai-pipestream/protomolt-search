//! The analysis contract: specs, tokens, term vectors, and errors.

use std::error::Error;
use std::fmt;

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
    /// Frequencies and original-text UTF-8 occurrence spans.
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

/// Coordinate system used by every returned original-text span.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum OffsetUnit {
    /// Java/OpenNLP string coordinates. Supplementary scalars occupy two
    /// units.
    Utf16CodeUnits,
    /// Byte boundaries in the original text's UTF-8 encoding, directly usable
    /// to slice a Rust string. This is the default.
    #[default]
    Utf8Bytes,
}

impl OffsetUnit {
    pub(crate) fn position(self, utf16: u32, utf8: usize) -> u32 {
        match self {
            Self::Utf16CodeUnits => utf16,
            Self::Utf8Bytes => utf8 as u32,
        }
    }
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

/// Half-open span in the original input text, using the result's selected
/// [`OffsetUnit`].
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
    /// Token ordinal of each occurrence, parallel to `occurrences`: the
    /// index of the token in the tokenizer's complete output, counting
    /// every token the tokenizer produced, including ones whose term
    /// identity normalized to nothing and were therefore never emitted.
    /// Two occurrences are adjacent exactly when their ordinals differ by
    /// one, which is what a phrase or proximity query asks. Character
    /// spans cannot answer that: a dropped token and a run of whitespace
    /// look identical between two spans. Empty in [`TermVectorMode::ScoringOnly`].
    pub positions: Vec<u32>,
}

/// Native analysis result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzedDocument {
    pub term_vectors: Vec<TermVector>,
    /// Sentence spans in `offset_unit`, in document order, produced by
    /// the newline sentence detector (the same model-free rule the
    /// sidecar's default detector applies): each maximal run of text
    /// between line breaks, trimmed of whitespace at both ends; blank
    /// lines yield none. Every token lies inside exactly one sentence,
    /// because a whitespace token never contains a line break.
    pub sentences: Vec<Span>,
    /// Token surface forms in input order, used only by ingest vocabulary
    /// accounting. Query callers may ignore this field.
    pub tokens: Vec<String>,
    /// Sum of retained term frequencies.
    pub length: u32,
    /// Coordinate system shared by every term occurrence.
    pub offset_unit: OffsetUnit,
}

/// One registered glossary surface form. Multiple entries may share an id to
/// represent aliases of the same concept.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GlossaryEntry {
    pub id: String,
    pub term: String,
}

/// One glossary occurrence in original-text coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlossaryMatch {
    pub id: String,
    pub registered_term: String,
    pub span: Span,
    /// Number of UAX #29 tokens in the registered surface form.
    pub token_count: u32,
}

/// A request the native implementation cannot analyze without changing the
/// OpenNLP contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisError(String);

impl AnalysisError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for AnalysisError {}
