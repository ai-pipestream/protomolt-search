//! The in-process `turbo-client` provider: the lexical lattice plus the
//! optional one-pass NLP layers.

use tonic::Status;

use super::lattice::LatticeFold;
use super::spec::{body_spec, validate_dual_cased_spec, TERM_VECTOR_MODE_SCORING_ONLY};
use super::spec_convert::turbo_field_config;
use super::status::client_err_to_status;
use super::unary::check_document_text;
use super::SessionLayers;
use crate::pb::AnalysisSpec;
use crate::postings::AnalyzedDoc;

/// Analyze one document in-process using `turbo-client` (`LexicalAnalyzer` +
/// optional `OnePassDocumentNlp` layers) with zero network calls and zero JVM.
pub fn analyze_document_turbo(
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    Ok(turbo_analysis_pass(text, spec, layers)?.doc)
}

pub(super) struct TurboPassOutput {
    pub(super) doc: AnalyzedDoc,
    pub(super) surface_tokens: Vec<String>,
}

static TURBO_NLP_SINGLETON: std::sync::OnceLock<Result<turbo_client::OnePassDocumentNlp, String>> =
    std::sync::OnceLock::new();

pub(super) fn shared_turbo_nlp() -> Result<&'static turbo_client::OnePassDocumentNlp, Status> {
    TURBO_NLP_SINGLETON
        .get_or_init(|| {
            turbo_client::OnePassDocumentNlp::builder()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|msg| Status::internal(format!("failed to build OnePassDocumentNlp: {msg}")))
}

pub(super) fn turbo_analysis_pass(
    text: &str,
    spec: Option<&AnalysisSpec>,
    layers: SessionLayers,
) -> Result<TurboPassOutput, Status> {
    check_document_text(text)?;

    if layers.dual_cased {
        validate_dual_cased_spec(spec)?;
    }
    let effective_spec = spec.cloned().unwrap_or_else(body_spec);
    let scoring_only = effective_spec.term_vector_mode == TERM_VECTOR_MODE_SCORING_ONLY;
    let cfg = turbo_field_config(Some(&effective_spec))?;
    let lex = turbo_client::LexicalAnalyzer::new(cfg);
    let surface_tokens: Vec<String> = text.split_whitespace().map(str::to_owned).collect();

    let (primary_field, cased_field) = if layers.dual_cased {
        let dual = lex.analyze_dual(text).map_err(client_err_to_status)?;
        (
            turbo_field_to_postings(dual.primary, scoring_only),
            Some(turbo_field_to_postings(dual.cased, scoring_only)),
        )
    } else {
        let primary = lex.analyze(text).map_err(client_err_to_status)?;
        (turbo_field_to_postings(primary, scoring_only), None)
    };

    let (quality, geography, entities, nlp_proj) =
        if layers.quality || layers.geography || layers.entities || layers.nlp_columns {
            let nlp = shared_turbo_nlp()?
                .analyze(text)
                .map_err(client_err_to_status)?;
            let (q, g, e) = reduce_nlp_layers(&nlp, layers);
            (q, g, e, layers.nlp_columns.then(|| Box::new(nlp)))
        } else {
            (None, None, Vec::new(), None)
        };

    Ok(TurboPassOutput {
        doc: AnalyzedDoc {
            fields: vec![primary_field],
            cased: cased_field,
            quality,
            geography,
            entities,
            nlp: nlp_proj,
        },
        surface_tokens,
    })
}

pub(super) fn reduce_nlp_layers(
    nlp: &turbo_client::DocumentNlpProjection,
    layers: SessionLayers,
) -> (
    Option<crate::postings::DocQuality>,
    Option<crate::postings::DocGeography>,
    Vec<crate::phrases::DocEntity>,
) {
    let quality = layers.quality.then(|| crate::postings::DocQuality {
        noise: if nlp.quality.worst_noise_score.is_finite() {
            nlp.quality.worst_noise_score
        } else {
            0.0
        },
        noise_chars: nlp.quality.union_noise_chars,
        artifacts: nlp.quality.artifact_count,
    });
    let geography = layers.geography.then(|| {
        let mut best: Option<&turbo_client::GeoHit> = None;
        let mut votes: Vec<(String, usize)> = Vec::new();
        for hit in &nlp.geo {
            if !hit.confidence.is_finite() {
                continue;
            }
            if best.is_none_or(|b| hit.confidence > b.confidence) {
                best = Some(hit);
            }
            if !hit.country_code.is_empty() {
                match votes.iter_mut().find(|(c, _)| *c == hit.country_code) {
                    Some((_, n)) => *n += 1,
                    None => votes.push((hit.country_code.clone(), 1)),
                }
            }
        }
        votes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        crate::postings::DocGeography {
            point: best.map(|b| (b.latitude, b.longitude)),
            confidence: best.map_or(0.0, |b| b.confidence),
            country: votes.first().map(|(c, _)| c.clone()).unwrap_or_default(),
        }
    });
    let entities = if layers.entities {
        nlp.affirmed_entities
            .iter()
            .map(|e| crate::phrases::DocEntity {
                kind: e.kind.clone(),
                text: e.surface.clone(),
                start: e.spans.utf8.0,
                end: e.spans.utf8.1,
            })
            .collect()
    } else {
        Vec::new()
    };
    (quality, geography, entities)
}

pub(super) fn turbo_field_to_postings(
    analyzed: turbo_client::AnalyzedField,
    scoring_only: bool,
) -> crate::postings::AnalyzedField {
    let mut fold = LatticeFold::new(scoring_only);
    for arc in analyzed.arcs {
        if arc.term.is_empty() {
            continue;
        }
        fold.push(arc.term, arc.pos, arc.pos_len, arc.spans.utf8);
    }
    let sentences = analyzed
        .sentences
        .iter()
        .map(|s| s.spans.utf8)
        .filter(|&(s, e)| s < e)
        .collect();
    fold.finish(sentences)
}
