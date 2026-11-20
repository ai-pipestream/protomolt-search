//! The minimal in-process `protomolt-analyzer` lexical provider.

use tonic::Status;

pub use protomolt_analyzer::{phrase_posting_term, GlossaryEntry};

/// Every `protomolt-analyzer` item the crate uses. Other modules reach the
/// native stack only through these names, so retiring it in favor of
/// `turbo-client` replaces this list and nothing else outside this module.
pub(crate) use protomolt_analyzer::{
    analyze, analyze_dual, normalize_term, unicode_case_fold, AnalysisSpec as NativeSpec,
    AnalyzedDocument, Glossary, NormalizerStep, Stemmer, TermVectorMode, TermVectorSource,
    Tokenizer,
};

use super::spec::validate_dual_cased_spec;
use super::spec_convert::native_spec;
use super::SessionLayers;
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

/// Analyze one document with the minimal in-process `protomolt-analyzer` provider.
pub fn analyze_document_native(
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    Ok(native_analysis(text, spec)?.doc)
}

/// Both the folded and the cased term identity from one native pass.
pub fn analyze_document_native_dual(
    text: &str,
    spec: Option<&AnalysisSpec>,
) -> Result<AnalyzedDoc, Status> {
    validate_dual_cased_spec(spec)?;
    let spec = native_spec(spec)?;
    Ok(native_dual_with_spec(text, &spec)?.doc)
}

pub(super) fn analyze_document_native_layers(
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    let unsupported = SessionLayers {
        sentences: false,
        dual_cased: false,
        ..layers
    };
    if unsupported != SessionLayers::default() {
        return Err(Status::failed_precondition(
            "native lexical analysis does not provide quality, geography, or entities; use --analysis-addr=turbo or turbo-kserve",
        ));
    }
    if layers.dual_cased {
        analyze_document_native_dual(text, spec)
    } else {
        analyze_document_native(text, spec)
    }
}

#[derive(Debug)]
pub(super) struct NativeAnalysis {
    pub(super) doc: AnalyzedDoc,
    pub(super) tokens: Vec<String>,
}

fn native_analysis(text: &str, spec: Option<&AnalysisSpec>) -> Result<NativeAnalysis, Status> {
    let spec = native_spec(spec)?;
    native_analysis_with_spec(text, &spec)
}

pub(super) fn native_analysis_with_spec(
    text: &str,
    spec: &NativeSpec,
) -> Result<NativeAnalysis, Status> {
    let analyzed =
        analyze(text, spec).map_err(|error| Status::invalid_argument(error.to_string()))?;
    let tokens = analyzed.tokens.clone();
    Ok(NativeAnalysis {
        doc: AnalyzedDoc {
            fields: vec![native_field(analyzed)],
            ..Default::default()
        },
        tokens,
    })
}

pub(super) fn native_dual_with_spec(
    text: &str,
    spec: &NativeSpec,
) -> Result<NativeAnalysis, Status> {
    let dual =
        analyze_dual(text, spec).map_err(|error| Status::invalid_argument(error.to_string()))?;
    let tokens = dual.folded.tokens.clone();
    Ok(NativeAnalysis {
        doc: AnalyzedDoc {
            fields: vec![native_field(dual.folded)],
            cased: Some(native_field(dual.cased)),
            ..Default::default()
        },
        tokens,
    })
}

fn native_field(analyzed: AnalyzedDocument) -> crate::postings::AnalyzedField {
    let mut terms = crate::postings::DocTerms::with_capacity(analyzed.term_vectors.len());
    let mut positions = crate::postings::DocPositions::with_capacity(analyzed.term_vectors.len());
    for vector in analyzed.term_vectors {
        terms.push((
            vector.term,
            vector.frequency,
            vector
                .occurrences
                .into_iter()
                .map(|span| (span.start, span.end))
                .collect(),
        ));
        positions.push(vector.positions);
    }
    crate::postings::AnalyzedField {
        terms,
        length: analyzed.length,
        positions: Some(positions),
        sentences: Some(
            analyzed
                .sentences
                .iter()
                .map(|span| (span.start, span.end))
                .collect(),
        ),
        ..Default::default()
    }
}
