//! Document analysis: tokens to term vectors under one spec.

use super::*;

#[derive(Debug)]
pub(crate) struct Token<'a> {
    pub(crate) surface: &'a str,
    pub(crate) span: Span,
}

/// Analyze one document. Terms are emitted in first-occurrence order and
/// occurrence spans use UTF-8 byte coordinates.
pub fn analyze(text: &str, spec: &AnalysisSpec) -> Result<AnalyzedDocument, AnalysisError> {
    analyze_with_offset_unit(text, spec, OffsetUnit::Utf8Bytes)
}

/// Analyze one document and return every occurrence in `offset_unit`.
pub fn analyze_with_offset_unit(
    text: &str,
    spec: &AnalysisSpec,
    offset_unit: OffsetUnit,
) -> Result<AnalyzedDocument, AnalysisError> {
    validate(text, spec)?;
    let tokens = match spec.tokenizer {
        Tokenizer::Whitespace => tokenize_whitespace(text, offset_unit),
        Tokenizer::Uax29 => tokenize_uax29(text, offset_unit),
    };
    let mut vectors = TermAccumulator::new(spec.term_vector_mode == TermVectorMode::Full);
    for (ordinal, token) in tokens.iter().enumerate() {
        vectors.push(term_identity(token.surface, spec), token.span, ordinal);
    }
    let sentences = split_sentences(text, offset_unit);
    let tokens = tokens
        .into_iter()
        .map(|token| token.surface.to_string())
        .collect();
    Ok(vectors.finish(sentences, tokens, offset_unit))
}

/// Both term identities of one text from ONE tokenization
/// (`docs/dual-cased.md`): `folded` under `spec`, `cased` under the same
/// chain without case folding ([`cased_twin`]). Vector for vector the
/// occurrence spans and token ordinals coincide, because the tokens are
/// the same tokens; only the identity each token maps to differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DualAnalyzedDocument {
    pub folded: AnalyzedDocument,
    pub cased: AnalyzedDocument,
}

/// The cased twin of a spec: the same tokenizer, stemmer, mode, and
/// source, and the same normalizer chain minus case folding.
pub fn cased_twin(spec: &AnalysisSpec) -> AnalysisSpec {
    AnalysisSpec {
        normalizers: spec
            .normalizers
            .iter()
            .filter(|step| **step != NormalizerStep::FullCaseFold)
            .cloned()
            .collect(),
        ..spec.clone()
    }
}

pub fn analyze_dual(
    text: &str,
    spec: &AnalysisSpec,
) -> Result<DualAnalyzedDocument, AnalysisError> {
    analyze_dual_with_offset_unit(text, spec, OffsetUnit::Utf8Bytes)
}

pub fn analyze_dual_with_offset_unit(
    text: &str,
    spec: &AnalysisSpec,
    offset_unit: OffsetUnit,
) -> Result<DualAnalyzedDocument, AnalysisError> {
    validate(text, spec)?;
    if spec.term_vector_source == TermVectorSource::Stems {
        return Err(AnalysisError::new(
            "dual term identity needs a step-chain term vector source (TOKENS or \
             NORMALIZED_STEMS); STEMS ignores the chain, so it has no folded form to contrast \
             with",
        ));
    }
    let cased_spec = cased_twin(spec);
    let tokens = match spec.tokenizer {
        Tokenizer::Whitespace => tokenize_whitespace(text, offset_unit),
        Tokenizer::Uax29 => tokenize_uax29(text, offset_unit),
    };
    let full = spec.term_vector_mode == TermVectorMode::Full;
    let mut folded = TermAccumulator::new(full);
    let mut cased = TermAccumulator::new(full);
    for (ordinal, token) in tokens.iter().enumerate() {
        folded.push(term_identity(token.surface, spec), token.span, ordinal);
        cased.push(
            term_identity(token.surface, &cased_spec),
            token.span,
            ordinal,
        );
    }
    let sentences = split_sentences(text, offset_unit);
    let tokens: Vec<String> = tokens
        .into_iter()
        .map(|token| token.surface.to_string())
        .collect();
    Ok(DualAnalyzedDocument {
        folded: folded.finish(sentences.clone(), tokens.clone(), offset_unit),
        cased: cased.finish(sentences, tokens, offset_unit),
    })
}

/// Term vectors under construction: one identity stream over the
/// tokenizer's output. The ordinal counts every token, emitted or not: a
/// token whose identity normalizes to nothing still occupies a position,
/// so the terms on either side of it are not adjacent.
pub(crate) struct TermAccumulator {
    full: bool,
    vectors: Vec<TermVector>,
    index: HashMap<String, usize>,
}

impl TermAccumulator {
    fn new(full: bool) -> Self {
        TermAccumulator {
            full,
            vectors: Vec::new(),
            index: HashMap::new(),
        }
    }

    fn push(&mut self, term: String, span: Span, ordinal: usize) {
        if term.is_empty() {
            return;
        }
        let ordinal = u32::try_from(ordinal).expect("token count fits u32 under the 1 MiB cap");
        if let Some(&index) = self.index.get(&term) {
            let vector = &mut self.vectors[index];
            vector.frequency += 1;
            if self.full {
                vector.occurrences.push(span);
                vector.positions.push(ordinal);
            }
        } else {
            let index = self.vectors.len();
            self.index.insert(term.clone(), index);
            self.vectors.push(TermVector {
                term,
                frequency: 1,
                occurrences: if self.full { vec![span] } else { Vec::new() },
                positions: if self.full { vec![ordinal] } else { Vec::new() },
            });
        }
    }

    fn finish(
        self,
        sentences: Vec<Span>,
        tokens: Vec<String>,
        offset_unit: OffsetUnit,
    ) -> AnalyzedDocument {
        let length = self.vectors.iter().map(|vector| vector.frequency).sum();
        AnalyzedDocument {
            term_vectors: self.vectors,
            sentences,
            tokens,
            length,
            offset_unit,
        }
    }
}

pub(crate) fn validate(text: &str, spec: &AnalysisSpec) -> Result<(), AnalysisError> {
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

pub(crate) fn term_identity(surface: &str, spec: &AnalysisSpec) -> String {
    match spec.term_vector_source {
        TermVectorSource::Tokens => normalize(surface, &spec.normalizers),
        TermVectorSource::Stems => stem(surface, spec.stemmer),
        TermVectorSource::NormalizedStems => {
            stem(&normalize(surface, &spec.normalizers), spec.stemmer)
        }
    }
}

pub(crate) fn stem(token: &str, stemmer: Stemmer) -> String {
    match stemmer {
        Stemmer::None => token.to_string(),
        Stemmer::Porter => porter::stem(token),
    }
}
