//! Conversion of a remote `SearchDocumentProjection` into postings and the
//! one-pass NLP projection, without re-running analysis locally.

use tonic::Status;
use turbo_client::proto::result::{
    OffsetMode, SearchAssessmentState, SearchDocumentProjection, SearchLexicalToken,
    SearchLinkedEntity, SearchNumericTemporalColumns, SearchSentenceRow,
};

use super::lattice::LatticeFold;
use super::turbo::reduce_nlp_layers;
use super::{SessionLayers, EMPTY_TERMS_DROPPED};
use crate::postings::AnalyzedDoc;

/// Converts a remote `SearchDocumentProjection` directly into an
/// [`AnalyzedDoc`] and surface token list without re-running local lexical or
/// NLP analysis.
pub fn analyzed_from_search_projection(
    mut proj: SearchDocumentProjection,
    scoring_only: bool,
    layers: SessionLayers,
) -> Result<(AnalyzedDoc, Vec<String>), Status> {
    if proj.offset_mode != 0 && proj.offset_mode != OffsetMode::Utf8 as i32 {
        return Err(Status::failed_precondition(
            "ProjectSearchDocument returned non-UTF-8 offsets for UTF-8 storage request",
        ));
    }
    let mut primary_tokens: Vec<SearchLexicalToken> = Vec::new();
    let mut cased_tokens: Vec<SearchLexicalToken> = Vec::new();
    let first_instance = proj.tokens.first().map(|t| t.instance.clone());
    for tok in std::mem::take(&mut proj.tokens) {
        if layers.dual_cased
            && (tok.instance.contains("cased")
                || first_instance
                    .as_deref()
                    .is_some_and(|first| !tok.instance.is_empty() && tok.instance != first))
        {
            cased_tokens.push(tok);
        } else {
            primary_tokens.push(tok);
        }
    }
    let surface_tokens: Vec<String> = primary_tokens
        .iter()
        .filter(|t| t.pos_len <= 1)
        .map(|t| t.term.clone())
        .collect();
    let primary_field = search_tokens_to_postings(&primary_tokens, &proj.sentences, scoring_only);
    let cased_field = if layers.dual_cased && !cased_tokens.is_empty() {
        Some(search_tokens_to_postings(
            &cased_tokens,
            &proj.sentences,
            scoring_only,
        ))
    } else {
        None
    };

    let (quality, geography, entities, nlp_proj) =
        if layers.quality || layers.geography || layers.entities || layers.nlp_columns {
            let nlp = search_projection_to_nlp(&proj);
            let (q, g, e) = reduce_nlp_layers(&nlp, layers);
            (q, g, e, layers.nlp_columns.then(|| Box::new(nlp)))
        } else {
            (None, None, Vec::new(), None)
        };

    Ok((
        AnalyzedDoc {
            fields: vec![primary_field],
            cased: cased_field,
            quality,
            geography,
            entities,
            nlp: nlp_proj,
        },
        surface_tokens,
    ))
}

fn search_tokens_to_postings(
    tokens: &[SearchLexicalToken],
    sentences: &[SearchSentenceRow],
    scoring_only: bool,
) -> crate::postings::AnalyzedField {
    let mut fold = LatticeFold::new(scoring_only);
    for arc in tokens {
        if arc.term.is_empty() {
            EMPTY_TERMS_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }
        fold.push(
            arc.term.as_str(),
            arc.pos,
            arc.pos_len,
            (arc.span_start, arc.span_end),
        );
    }
    let sentences = sentences
        .iter()
        .map(|s| (s.span_start, s.span_end))
        .filter(|&(s, e)| s < e)
        .collect();
    fold.finish(sentences)
}

fn utf8_only_spans(start: u32, end: u32) -> turbo_client::ProjectedSpanSet {
    turbo_client::ProjectedSpanSet {
        utf8: (start, end),
        utf16: (start, end),
        code_point: (start, end),
    }
}

fn search_projection_to_nlp(
    proj: &SearchDocumentProjection,
) -> turbo_client::DocumentNlpProjection {
    let nt = proj.numeric_temporal.as_ref();
    let entities = proj.entities.as_ref();
    turbo_client::DocumentNlpProjection {
        money: nt.map(projected_money).unwrap_or_default(),
        quantities: nt.map(projected_quantities).unwrap_or_default(),
        document_date: nt.and_then(|n| n.document_date.as_ref()).map(|d| {
            turbo_client::UtcDateSummary {
                iso: d.iso.clone(),
                epoch_seconds: d.epoch_seconds,
                year: d.year,
                month: d.month,
                day: d.day,
            }
        }),
        temporals: nt.map(projected_temporals).unwrap_or_default(),
        geo: projected_geo(proj),
        sentiment: projected_sentiment(proj),
        quality: projected_quality(proj),
        affirmed_entities: entities
            .map(|ep| ep.affirmed.iter().map(projected_entity).collect())
            .unwrap_or_default(),
        negated_entities: entities
            .map(|ep| ep.negated.iter().map(projected_entity).collect())
            .unwrap_or_default(),
        sketch: projected_sketch(proj),
    }
}

fn projected_money(n: &SearchNumericTemporalColumns) -> Vec<turbo_client::MoneyHit> {
    n.money
        .iter()
        .map(|m| turbo_client::MoneyHit {
            spans: utf8_only_spans(m.span_start, m.span_end),
            currency: m.currency.clone(),
            amount_f64: m.amount_f64,
            plain_amount: m.plain_amount.clone(),
            per: m.per.clone(),
            ecb_converted: m.ecb_converted,
        })
        .collect()
}

fn projected_quantities(n: &SearchNumericTemporalColumns) -> Vec<turbo_client::QuantityHit> {
    n.quantities
        .iter()
        .map(|q| turbo_client::QuantityHit {
            spans: utf8_only_spans(q.span_start, q.span_end),
            unit: q.unit.clone(),
            value_f64: q.value_f64,
            plain_value: q.plain_value.clone(),
            is_percent: q.is_percent,
        })
        .collect()
}

fn projected_temporals(n: &SearchNumericTemporalColumns) -> Vec<turbo_client::TemporalHit> {
    n.temporals
        .iter()
        .map(|t| turbo_client::TemporalHit {
            spans: utf8_only_spans(t.span_start, t.span_end),
            granularity: t.granularity.clone(),
            origin: t.origin.clone(),
            iso: t.iso.clone(),
            start_epoch_seconds: t.start_epoch_seconds,
            end_epoch_seconds: t.end_epoch_seconds,
        })
        .collect()
}

fn projected_geo(proj: &SearchDocumentProjection) -> Vec<turbo_client::GeoHit> {
    proj.geo_locations
        .iter()
        .map(|g| turbo_client::GeoHit {
            spans: utf8_only_spans(g.span_start, g.span_end),
            record_id: g.record_id.clone(),
            canonical_name: g.canonical_name.clone(),
            country_code: g.country_code.clone(),
            latitude: g.latitude,
            longitude: g.longitude,
            spine_path: g.spine_path.clone(),
            confidence: g.confidence,
        })
        .collect()
}

fn projected_sentiment(proj: &SearchDocumentProjection) -> turbo_client::SentimentSummary {
    proj.categories_sentiment
        .as_ref()
        .map(|cs| turbo_client::SentimentSummary {
            label: cs.sentiment_label.clone(),
            probability: cs.sentiment_score,
        })
        .unwrap_or(turbo_client::SentimentSummary {
            label: None,
            probability: None,
        })
}

fn projected_quality(proj: &SearchDocumentProjection) -> turbo_client::QualitySummary {
    proj.quality
        .as_ref()
        .map(|q| turbo_client::QualitySummary {
            assessed: q.noise_state == SearchAssessmentState::Assessed as i32
                || q.artifacts_state == SearchAssessmentState::Assessed as i32
                || q.text_quality_state == SearchAssessmentState::Assessed as i32,
            worst_noise_score: q.worst_noise_score,
            union_noise_chars: q.union_noise_chars,
            artifact_count: q.artifact_count,
            composite_score: q
                .metrics
                .as_ref()
                .and_then(|m| m.composite_score.map(f64::from)),
        })
        .unwrap_or(turbo_client::QualitySummary {
            assessed: false,
            worst_noise_score: 0.0,
            union_noise_chars: 0,
            artifact_count: 0,
            composite_score: None,
        })
}

fn projected_entity(e: &SearchLinkedEntity) -> turbo_client::EntityPolarityHit {
    turbo_client::EntityPolarityHit {
        spans: utf8_only_spans(e.span_start, e.span_end),
        surface: if e.matched_alias.is_empty() {
            e.canonical_name.clone()
        } else {
            e.matched_alias.clone()
        },
        kind: e.entity_kind.clone(),
        kb_id: e.kb_id.clone(),
        canonical_name: (!e.canonical_name.is_empty()).then(|| e.canonical_name.clone()),
        score: e.score,
        epistemic: e.epistemic.clone(),
        cue: e.cue.clone(),
    }
}

fn projected_sketch(proj: &SearchDocumentProjection) -> turbo_client::DocumentSketch {
    proj.document_sketch
        .as_ref()
        .map(|sk| turbo_client::DocumentSketch {
            word_count: sk.words,
            exact: (u128::from(sk.exact_hi) << 64) | u128::from(sk.exact_lo),
            shape: sk.shape_hash,
            simhash: sk.simhash,
            minhash: sk.minhash.clone(),
        })
        .unwrap_or(turbo_client::DocumentSketch {
            word_count: 0,
            exact: 0,
            shape: 0,
            simhash: 0,
            minhash: Vec::new(),
        })
}
