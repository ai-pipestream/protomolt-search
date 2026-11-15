//! The analysis specs the product names, their persisted fingerprint, and
//! the wire enum values an [`AnalysisSpec`] carries.

use tonic::Status;

use super::spec_convert::{native_spec, turbo_field_config};
use crate::pb::AnalysisSpec;

/// The analysis a body-text corpus is built with, and the ONLY spec that
/// may query one.
pub fn body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: TOKENIZER_WHITESPACE,
        stemmer: STEMMER_PORTER,
        term_vector_mode: TERM_VECTOR_MODE_FULL,
        term_vector_source: SOURCE_NORMALIZED_STEMS,
        char_filters: vec![
            CHAR_FILTER_STRIP_INVISIBLE,
            CHAR_FILTER_WHITESPACE,
            CHAR_FILTER_ACCENT_FOLD,
            CHAR_FILTER_FULL_CASE_FOLD,
        ],
    }
}

/// Unicode word-boundary variant of [`body_spec`].
pub fn uax29_body_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: TOKENIZER_UAX29,
        ..body_spec()
    }
}

/// The cased twin of [`body_spec`]: identical tokenizer and stemmer,
/// but term identity taken from the stem of the SURFACE form.
pub fn cased_body_spec() -> AnalysisSpec {
    AnalysisSpec {
        term_vector_source: SOURCE_STEMS,
        char_filters: Vec::new(),
        ..body_spec()
    }
}

/// Named corpus analyzers for CLIs and configuration.
pub fn analyzer_by_name(name: &str) -> Result<Option<AnalysisSpec>, String> {
    Ok(match name {
        "ingest" | "folded" => Some(body_spec()),
        "cased" => Some(cased_body_spec()),
        "server" => None,
        other => {
            return Err(format!(
                "unknown analyzer {other:?}; expected one of {}",
                ANALYZER_NAMES.join(", ")
            ))
        }
    })
}

/// Every name [`analyzer_by_name`] accepts.
pub const ANALYZER_NAMES: &[&str] = &["ingest", "folded", "cased", "server"];

/// Stable 64-bit FNV-1a identity of an [`AnalysisSpec`] persisted in `.bm25` headers.
pub fn analysis_fingerprint(spec: Option<&AnalysisSpec>) -> u64 {
    fn eat(hash: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    let Some(spec) = spec else { return 0 };
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    eat(&mut hash, b"turbovec.analysis.v1");
    for value in [
        spec.tokenizer,
        spec.stemmer,
        spec.term_vector_mode,
        spec.term_vector_source,
    ] {
        eat(&mut hash, &value.to_le_bytes());
    }
    eat(&mut hash, &(spec.char_filters.len() as u32).to_le_bytes());
    for filter in &spec.char_filters {
        eat(&mut hash, &filter.to_le_bytes());
    }
    if hash == 0 {
        1
    } else {
        hash
    }
}

/// Cryptographic SHA-256 [`turbo_client::IndexGenerationHandshake`] combining
/// a field's [`turbo_client::AnalysisFingerprint`] with an optional neural
/// [`turbo_client::BundleScoringFingerprint`].
pub fn turbo_handshake(
    spec: Option<&AnalysisSpec>,
    bundle: Option<turbo_client::BundleScoringFingerprint>,
) -> Result<turbo_client::IndexGenerationHandshake, Status> {
    let cfg = turbo_field_config(spec)?;
    Ok(turbo_client::IndexGenerationHandshake {
        analysis: Some(cfg.fingerprint()),
        bundle,
    })
}

/// `Tokenizer.TOKENIZER_WHITESPACE`.
pub const TOKENIZER_WHITESPACE: i32 = 1;
/// `Tokenizer.TOKENIZER_SIMPLE`.
pub const TOKENIZER_SIMPLE: i32 = 2;
/// `Tokenizer.TOKENIZER_UAX29`.
pub const TOKENIZER_UAX29: i32 = 3;
/// `Stemmer.STEMMER_NONE`.
pub const STEMMER_NONE: i32 = 1;
/// `Stemmer.STEMMER_PORTER`.
pub const STEMMER_PORTER: i32 = 2;
/// `TermVectorMode.MODE_FULL` (occurrence offsets included).
pub const TERM_VECTOR_MODE_FULL: i32 = 1;
/// `TermVectorMode.MODE_SCORING_ONLY` (frequency only).
pub const TERM_VECTOR_MODE_SCORING_ONLY: i32 = 2;
/// `TermVectorSource.SOURCE_TOKENS` (char filters define identity).
pub const SOURCE_TOKENS: i32 = 1;
/// `TermVectorSource.SOURCE_STEMS` (char filters ignored).
pub const SOURCE_STEMS: i32 = 2;
/// `TermVectorSource.SOURCE_NORMALIZED_STEMS` (char filters, then stem).
pub const SOURCE_NORMALIZED_STEMS: i32 = 3;
/// Strip invisible controls.
pub const CHAR_FILTER_STRIP_INVISIBLE: i32 = 1;
/// Collapse whitespace.
pub const CHAR_FILTER_WHITESPACE: i32 = 2;
/// Full Unicode case fold.
pub const CHAR_FILTER_FULL_CASE_FOLD: i32 = 6;
/// Unicode NFKC compatibility composition.
pub const CHAR_FILTER_NFKC: i32 = 12;
/// Unicode NFC canonical composition.
pub const CHAR_FILTER_NFC: i32 = 13;
/// Strip diacritics (`Rodríguez` -> `Rodriguez`).
pub const CHAR_FILTER_ACCENT_FOLD: i32 = 15;
/// Simple (per-character) case folding.
pub const CHAR_FILTER_CASE_FOLD: i32 = 16;
/// Strip HTML/XML markup tags while preserving source offsets.
pub const CHAR_FILTER_HTML_STRIP: i32 = 20;
/// Decode HTML character entities while preserving source offsets.
pub const CHAR_FILTER_HTML_ENTITY: i32 = 21;
/// Repair UTF-8-as-Latin-1 mojibake sequences while preserving source offsets.
pub const CHAR_FILTER_MOJIBAKE: i32 = 23;
/// Rejoin words split across line-break hyphens while emitting split+joined lattice arcs.
pub const CHAR_FILTER_DEHYPHENATE: i32 = 24;

/// Normalize a term prefix under `spec`'s normalizer chain (never stemming).
pub fn normalize_prefix(prefix: &str, spec: Option<&AnalysisSpec>) -> Result<String, Status> {
    if prefix.is_empty() {
        return Err(Status::invalid_argument(
            "a term prefix must be non-empty; an empty prefix is every term",
        ));
    }
    let Some(spec) = spec else {
        return Err(Status::invalid_argument(
            "term prefixes need an explicit AnalysisSpec: the prefix is normalized under \
             the field's char filters before it is compared against the dictionary, and \
             an undeclared chain is not known to the coordinator",
        ));
    };
    if spec.term_vector_source == SOURCE_STEMS {
        return Ok(prefix.to_string());
    }
    let normalized = match native_spec(Some(spec)) {
        Ok(native) => protomolt_analyzer::normalize_term(prefix, &native.normalizers),
        Err(_) => {
            let cfg = turbo_field_config(Some(spec))?;
            turbo_client::LexicalAnalyzer::new(cfg).normalize_prefix(prefix)
        }
    };
    if normalized.is_empty() {
        return Err(Status::invalid_argument(format!(
            "term prefix {prefix:?} normalizes to nothing under the field's char filters"
        )));
    }
    Ok(normalized)
}

/// Cased twin of an [`AnalysisSpec`]: identical tokenizer, stemmer, mode, and
/// source with case-folding filters removed.
pub fn cased_twin_spec(spec: &AnalysisSpec) -> AnalysisSpec {
    AnalysisSpec {
        char_filters: spec
            .char_filters
            .iter()
            .copied()
            .filter(|step| *step != CHAR_FILTER_FULL_CASE_FOLD && *step != CHAR_FILTER_CASE_FOLD)
            .collect(),
        ..spec.clone()
    }
}

/// Validate that `spec` can produce a distinct folded/cased twin pair.
pub fn validate_dual_cased_spec(spec: Option<&AnalysisSpec>) -> Result<(), Status> {
    let Some(spec) = spec else {
        return Err(Status::invalid_argument(
            "cased_field needs an explicit body AnalysisSpec: the cased field's fingerprint is \
             the twin of the body's, and analyzer 'server' describes none",
        ));
    };
    if spec.term_vector_source == SOURCE_STEMS {
        return Err(Status::invalid_argument(
            "cased_field needs a step-chain term vector source on the body (SOURCE_TOKENS or \
             SOURCE_NORMALIZED_STEMS); SOURCE_STEMS ignores the step chain, so it has no folded \
             form to contrast with",
        ));
    }
    Ok(())
}
