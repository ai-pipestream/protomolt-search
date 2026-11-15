//! The mock's analysis: whitespace tokens with UTF-8 spans, term vectors
//! in first-occurrence order, and the optional quality, sentence, and
//! geography layers.

use std::sync::atomic::{AtomicU64, Ordering};

use tonic::Status;

use crate::analyzer::wire::{
    MockAnalyzeResponse, WireGeoLocation, WireNoiseSpan, WireRegionVote, WireSessionOptions,
    WireSpan, WireTermVector, WireTextArtifact, WireToken, WIRE_OFFSET_UTF8,
};

use super::toy_stem;

/// The layers a mock serves, whatever the request asks for.
#[derive(Clone, Copy)]
pub(super) struct MockLayers {
    pub(super) ner: bool,
    pub(super) sentence_layer: bool,
    pub(super) dual_identity: bool,
}

const GAZETTEER: [(&str, &str, &str, f64, f64, f64); 3] = [
    ("paris", "Paris", "FR", 48.8566, 2.3522, 0.9),
    ("berlin", "Berlin", "DE", 52.52, 13.405, 0.9),
    ("springfield", "Springfield", "US", 39.7817, -89.6501, 0.4),
];

const SOURCE_STEMS: i32 = 2;
const MODE_SCORING_ONLY: i32 = 2;
const FULL_CASE_FOLD: i32 = 6;
const CASE_FOLD: i32 = 16;

fn newline_sentences(text: &str) -> Vec<WireSpan> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    for line in text.split_inclusive(['\n', '\r']) {
        let body = line.trim_end_matches(['\n', '\r']);
        let lead = body.len() - body.trim_start().len();
        let trimmed = body.trim();
        if !trimmed.is_empty() {
            out.push(WireSpan {
                start: (cursor + lead) as i32,
                end: (cursor + lead + trimmed.len()) as i32,
            });
        }
        cursor += line.len();
    }
    out
}

fn whitespace_tokens(text: &str) -> Vec<WireToken> {
    let mut tokens = Vec::new();
    let mut offset = 0usize;
    for word in text.split_whitespace() {
        let start = text[offset..].find(word).map(|i| offset + i).unwrap();
        let end = start + word.len();
        tokens.push(WireToken {
            span: Some(WireSpan {
                start: start as i32,
                end: end as i32,
            }),
            text: word.to_string(),
        });
        offset = end;
    }
    tokens
}

/// Count one occurrence of `identity`, keeping its span unless the mode
/// asks for scoring statistics only.
fn push_occurrence(vectors: &mut Vec<WireTermVector>, identity: String, span: WireSpan, mode: i32) {
    match vectors.iter_mut().find(|t| t.term == identity) {
        Some(entry) => {
            entry.frequency += 1;
            if mode != MODE_SCORING_ONLY {
                entry.occurrences.push(span);
            }
        }
        None => vectors.push(WireTermVector {
            term: identity,
            frequency: 1,
            occurrences: if mode == MODE_SCORING_ONLY {
                Vec::new()
            } else {
                vec![span]
            },
        }),
    }
}

type TermVectors = (Vec<WireTermVector>, Vec<WireTermVector>);

/// The folded (or stemmed) term vectors and, when the session asks for
/// dual identity and the mock serves it, the cased ones.
fn term_vectors(
    tokens: &[WireToken],
    session: &WireSessionOptions,
    dual_identity: bool,
) -> Result<TermVectors, Status> {
    let (mode, source, steps, stemmer) = match &session.spec {
        Some(s) => (
            s.term_vector_mode,
            s.term_vector_source,
            s.char_filters.as_slice(),
            s.stemmer,
        ),
        None => (0, 0, &[][..], 0),
    };
    let stemming_on = stemmer > 1;
    if source == SOURCE_STEMS && !stemming_on {
        return Err(Status::invalid_argument("SOURCE_STEMS requires a stemmer"));
    }
    if session.layers.dual_cased && source == SOURCE_STEMS {
        return Err(Status::invalid_argument(
            "dual_cased requires a step-chain term vector source; SOURCE_STEMS has no \
             folded form to contrast with",
        ));
    }
    let folds = steps.is_empty()
        || steps
            .iter()
            .any(|step| *step == FULL_CASE_FOLD || *step == CASE_FOLD);
    let mut folded = Vec::new();
    let mut cased = Vec::new();
    for token in tokens {
        let identity = if source == SOURCE_STEMS {
            toy_stem(&token.text)
        } else if folds {
            token.text.to_lowercase()
        } else {
            token.text.clone()
        };
        let span = token.span.unwrap();
        push_occurrence(&mut folded, identity, span, mode);
        if session.layers.dual_cased && dual_identity {
            push_occurrence(&mut cased, token.text.clone(), span, mode);
        }
    }
    Ok((folded, cased))
}

/// Runs of `#` as gibberish noise, U+FFFD as replacement artifacts.
fn quality_layers(text: &str, tokens: &[WireToken]) -> (Vec<WireNoiseSpan>, Vec<WireTextArtifact>) {
    let noise = tokens
        .iter()
        .filter(|t| !t.text.is_empty() && t.text.bytes().all(|b| b == b'#'))
        .map(|t| WireNoiseSpan {
            span: t.span,
            severity: "gibberish".to_string(),
            score: (t.text.len() as f64 / 10.0).min(1.0),
        })
        .collect();
    let artifacts = text
        .char_indices()
        .filter(|&(_, c)| c == '\u{FFFD}')
        .map(|(i, c)| WireTextArtifact {
            span: Some(WireSpan {
                start: i as i32,
                end: (i + c.len_utf8()) as i32,
            }),
            kind: "replacement".to_string(),
        })
        .collect();
    (noise, artifacts)
}

fn gazetteer_locations(tokens: &[WireToken]) -> Vec<WireGeoLocation> {
    tokens
        .iter()
        .filter_map(|t| {
            let lower = t.text.to_lowercase();
            GAZETTEER.iter().find(|(key, ..)| *key == lower).map(
                |&(_, name, country, lat, lon, confidence)| WireGeoLocation {
                    span: t.span,
                    name: name.to_string(),
                    country_code: country.to_string(),
                    latitude: lat,
                    longitude: lon,
                    confidence,
                },
            )
        })
        .collect()
}

/// Each country's share of the located mentions, largest first.
fn region_votes(locations: &[WireGeoLocation]) -> Vec<WireRegionVote> {
    let mut votes: Vec<WireRegionVote> = Vec::new();
    for location in locations {
        match votes
            .iter_mut()
            .find(|v| v.country_code == location.country_code)
        {
            Some(vote) => vote.share += 1.0,
            None => votes.push(WireRegionVote {
                country_code: location.country_code.clone(),
                share: 1.0,
            }),
        }
    }
    let total = locations.len() as f64;
    for vote in &mut votes {
        vote.share /= total;
    }
    votes.sort_by(|a, b| {
        b.share
            .total_cmp(&a.share)
            .then_with(|| a.country_code.cmp(&b.country_code))
    });
    votes
}

/// Analyze one document under `session`, counting the call.
pub(super) fn analyze_text(
    text: &str,
    session: &WireSessionOptions,
    layers: MockLayers,
    calls: &AtomicU64,
) -> Result<MockAnalyzeResponse, Status> {
    calls.fetch_add(1, Ordering::SeqCst);
    if text.is_empty() {
        return Err(Status::invalid_argument("empty text"));
    }
    let tokens = whitespace_tokens(text);
    let (term_vectors, cased_term_vectors) = term_vectors(&tokens, session, layers.dual_identity)?;
    let (noise, artifacts) = if session.layers.quality {
        quality_layers(text, &tokens)
    } else {
        (Vec::new(), Vec::new())
    };
    let locations = if session.layers.geography && layers.ner {
        gazetteer_locations(&tokens)
    } else {
        Vec::new()
    };
    let regions = region_votes(&locations);
    let sentences = if session.layers.sentences && layers.sentence_layer {
        newline_sentences(text)
    } else {
        Vec::new()
    };
    Ok(MockAnalyzeResponse {
        offset_unit: WIRE_OFFSET_UTF8,
        sentences,
        tokens,
        term_vectors,
        cased_term_vectors,
        noise,
        artifacts,
        locations,
        regions,
        entities: Vec::new(),
    })
}
