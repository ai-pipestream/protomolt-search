//! Translation of an [`AnalysisSpec`] into each in-process backend's own
//! configuration, refusing what that backend does not implement.

use tonic::Status;

use super::spec::{
    body_spec, CHAR_FILTER_ACCENT_FOLD, CHAR_FILTER_CASE_FOLD, CHAR_FILTER_DEHYPHENATE,
    CHAR_FILTER_FULL_CASE_FOLD, CHAR_FILTER_HTML_ENTITY, CHAR_FILTER_HTML_STRIP,
    CHAR_FILTER_MOJIBAKE, CHAR_FILTER_NFC, CHAR_FILTER_NFKC, CHAR_FILTER_STRIP_INVISIBLE,
    CHAR_FILTER_WHITESPACE, SOURCE_NORMALIZED_STEMS, SOURCE_STEMS, SOURCE_TOKENS, STEMMER_NONE,
    STEMMER_PORTER, TERM_VECTOR_MODE_FULL, TERM_VECTOR_MODE_SCORING_ONLY, TOKENIZER_SIMPLE,
    TOKENIZER_UAX29, TOKENIZER_WHITESPACE,
};
use crate::pb::AnalysisSpec;

/// Translate a `protomolt-search` [`AnalysisSpec`] into a `turbo-client`
/// [`turbo_client::FieldAnalysisConfig`].
pub fn turbo_field_config(
    spec: Option<&AnalysisSpec>,
) -> Result<turbo_client::FieldAnalysisConfig, Status> {
    use turbo_client::{NormalizerKind, StemmerKind, TermSourceKind, TokenizerKind};

    let effective = spec.cloned().unwrap_or_else(body_spec);
    let tokenizer = match effective.tokenizer {
        0 | TOKENIZER_WHITESPACE => TokenizerKind::Whitespace,
        TOKENIZER_SIMPLE => TokenizerKind::Simple,
        TOKENIZER_UAX29 => TokenizerKind::Uax29,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported tokenizer {other}; expected WHITESPACE (1), SIMPLE (2), or UAX29 (3)"
            )))
        }
    };
    let stemmer = match effective.stemmer {
        0 | STEMMER_NONE => StemmerKind::None,
        STEMMER_PORTER => StemmerKind::Porter,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported stemmer {other}"
            )))
        }
    };
    let term_source = match effective.term_vector_source {
        0 | SOURCE_TOKENS => TermSourceKind::Tokens,
        SOURCE_STEMS => TermSourceKind::Stems,
        SOURCE_NORMALIZED_STEMS => TermSourceKind::NormalizedStems,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported term_vector_source {other}"
            )))
        }
    };
    if matches!(
        term_source,
        TermSourceKind::Stems | TermSourceKind::NormalizedStems
    ) && stemmer == StemmerKind::None
    {
        return Err(Status::invalid_argument(
            "term vector source STEMS requires a stemmer other than STEMMER_NONE",
        ));
    }
    let mut normalizers = Vec::new();
    if term_source != TermSourceKind::Stems {
        for &step in &effective.char_filters {
            let kind = match step {
                0 => continue,
                CHAR_FILTER_STRIP_INVISIBLE => NormalizerKind::StripInvisible,
                CHAR_FILTER_WHITESPACE => NormalizerKind::Whitespace,
                CHAR_FILTER_FULL_CASE_FOLD => NormalizerKind::FullCaseFold,
                CHAR_FILTER_NFKC => NormalizerKind::Nfkc,
                CHAR_FILTER_NFC => NormalizerKind::Nfc,
                CHAR_FILTER_ACCENT_FOLD => NormalizerKind::AccentFold,
                CHAR_FILTER_CASE_FOLD => NormalizerKind::SimpleCaseFold,
                CHAR_FILTER_HTML_STRIP => NormalizerKind::HtmlTagStrip,
                CHAR_FILTER_HTML_ENTITY => NormalizerKind::HtmlEntityDecode,
                CHAR_FILTER_MOJIBAKE => NormalizerKind::Mojibake,
                CHAR_FILTER_DEHYPHENATE => NormalizerKind::Dehyphenate,
                unsupported => {
                    return Err(Status::invalid_argument(format!(
                        "unsupported normalizer step {unsupported}"
                    )))
                }
            };
            normalizers.push(kind);
        }
    }
    Ok(turbo_client::FieldAnalysisConfig {
        tokenizer,
        normalizers,
        stemmer,
        term_source,
        emit_lattice: true,
        sentence_language: "eng".to_string(),
    })
}

pub(crate) fn validate_native_spec(spec: &AnalysisSpec) -> Result<(), Status> {
    native_spec(Some(spec)).map(|_| ())
}

pub(super) fn native_spec(
    spec: Option<&AnalysisSpec>,
) -> Result<super::native::NativeSpec, Status> {
    use super::native::{NativeSpec, Stemmer, TermVectorMode, TermVectorSource, Tokenizer};

    let spec = spec.ok_or_else(|| {
        Status::failed_precondition(
            "native analysis requires an explicit AnalysisSpec; analyzer 'server' is sidecar-only",
        )
    })?;

    let tokenizer = match spec.tokenizer {
        0 | TOKENIZER_WHITESPACE => Tokenizer::Whitespace,
        TOKENIZER_UAX29 => Tokenizer::Uax29,
        value => {
            return Err(Status::invalid_argument(format!(
                "native analysis supports TOKENIZER_WHITESPACE (1) and TOKENIZER_UAX29 (3), got {value}"
            )))
        }
    };
    let stemmer = match spec.stemmer {
        0 | STEMMER_NONE => Stemmer::None,
        STEMMER_PORTER => Stemmer::Porter,
        value => {
            return Err(Status::invalid_argument(format!(
                "native analysis supports only STEMMER_NONE (1) and STEMMER_PORTER (2), got {value}"
            )))
        }
    };
    let term_vector_mode = match spec.term_vector_mode {
        0 | TERM_VECTOR_MODE_FULL => TermVectorMode::Full,
        TERM_VECTOR_MODE_SCORING_ONLY => TermVectorMode::ScoringOnly,
        value => {
            return Err(Status::invalid_argument(format!(
            "native analysis supports term vector modes FULL (1) and SCORING_ONLY (2), got {value}"
        )))
        }
    };
    let term_vector_source = match spec.term_vector_source {
        0 | SOURCE_TOKENS => TermVectorSource::Tokens,
        SOURCE_STEMS => TermVectorSource::Stems,
        SOURCE_NORMALIZED_STEMS => TermVectorSource::NormalizedStems,
        value => {
            return Err(Status::invalid_argument(format!(
                "native analysis supports term vector sources TOKENS (1), STEMS (2), and NORMALIZED_STEMS (3), got {value}"
            )))
        }
    };

    let native = NativeSpec {
        tokenizer,
        stemmer,
        term_vector_mode,
        term_vector_source,
        normalizers: native_normalizers(&spec.char_filters)?,
    };

    if matches!(
        native.term_vector_source,
        TermVectorSource::Stems | TermVectorSource::NormalizedStems
    ) && native.stemmer == Stemmer::None
    {
        return Err(Status::invalid_argument(
            "term vector source STEMS requires a stemmer other than STEMMER_NONE",
        ));
    }
    Ok(native)
}

fn native_normalizers(char_filters: &[i32]) -> Result<Vec<super::native::NormalizerStep>, Status> {
    use super::native::NormalizerStep;

    let mut normalizers = Vec::with_capacity(char_filters.len());
    for &value in char_filters {
        let step = match value {
            0 => continue,
            CHAR_FILTER_STRIP_INVISIBLE => NormalizerStep::StripInvisible,
            CHAR_FILTER_WHITESPACE => NormalizerStep::Whitespace,
            CHAR_FILTER_ACCENT_FOLD => NormalizerStep::AccentFold,
            CHAR_FILTER_FULL_CASE_FOLD => NormalizerStep::FullCaseFold,
            unsupported => {
                return Err(Status::invalid_argument(format!(
                    "native analysis does not implement normalizer step {unsupported}; supported values are STRIP_INVISIBLE (1), WHITESPACE (2), FULL_CASE_FOLD (6), and ACCENT_FOLD (15)"
                )))
            }
        };
        normalizers.push(step);
    }

    Ok(normalizers)
}
