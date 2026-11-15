//! Conversion of a decoded analysis payload into an [`AnalyzedDoc`].

use tonic::Status;

use super::{
    MockAnalyzeResponse, WireGeoLocation, WireTermVector, WireToken, WIRE_OFFSET_UNSPECIFIED,
    WIRE_OFFSET_UTF16, WIRE_OFFSET_UTF8,
};
use crate::analyzer::{SessionLayers, EMPTY_TERMS_DROPPED};
use crate::postings::{
    AnalyzedDoc, AnalyzedField, DocGeography, DocPositions, DocQuality, DocTerms,
};

fn terms_of(vectors: Vec<WireTermVector>) -> (DocTerms, u32) {
    let mut terms = DocTerms::new();
    let mut length = 0u32;
    for tv in vectors {
        if tv.frequency <= 0 {
            continue;
        }
        if tv.term.is_empty() {
            let n = EMPTY_TERMS_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if n <= 20 || n.is_multiple_of(1_000_000) {
                eprintln!(
                    "analysis: dropped zero-length term (occurrence {n} this process): a \
                     token of stripped characters; unqueryable, and refused at index open \
                     if it were kept"
                );
            }
            continue;
        }
        let offsets = tv
            .occurrences
            .iter()
            .map(|s| (s.start.max(0) as u32, s.end.max(0) as u32))
            .collect();
        length += tv.frequency as u32;
        terms.push((tv.term, tv.frequency as u32, offsets));
    }
    (terms, length)
}

pub(crate) fn analyzed_from_mock(
    mut response: MockAnalyzeResponse,
    layers: SessionLayers,
) -> Result<AnalyzedDoc, Status> {
    match response.offset_unit {
        WIRE_OFFSET_UNSPECIFIED | WIRE_OFFSET_UTF8 => {}
        WIRE_OFFSET_UTF16 => {
            return Err(Status::failed_precondition(
                "analysis service returned UTF-16 offsets after search requested UTF-8; refusing ambiguous persisted spans",
            ));
        }
        value => {
            return Err(Status::failed_precondition(format!(
                "analysis service returned unknown offset unit {value}"
            )));
        }
    }
    let quality = layers.quality.then(|| doc_quality(&response));
    let geography = layers.geography.then(|| doc_geography(&response));
    let entities = if layers.entities {
        response
            .entities
            .iter()
            .map(|entity| {
                let span = entity.span.as_ref();
                crate::phrases::DocEntity {
                    kind: entity.kind.clone(),
                    text: entity.text.clone(),
                    start: span.map_or(0, |s| s.start.max(0) as u32),
                    end: span.map_or(0, |s| s.end.max(0) as u32),
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let (terms, length) = terms_of(std::mem::take(&mut response.term_vectors));
    let positions = token_positions(&response.tokens, &terms)?;
    let sentences = layers.sentences.then(|| {
        response
            .sentences
            .iter()
            .map(|s| (s.start.max(0) as u32, s.end.max(0) as u32))
            .collect::<Vec<(u32, u32)>>()
    });
    let mut doc = AnalyzedDoc::body(terms, length);
    doc.fields[0].positions = positions;
    doc.fields[0].sentences = sentences.clone();
    if layers.dual_cased {
        let (cased_terms, cased_length) =
            terms_of(std::mem::take(&mut response.cased_term_vectors));
        if cased_terms.is_empty() && !doc.fields[0].terms.is_empty() {
            return Err(Status::failed_precondition(
                "dual_cased was requested but the analysis service returned no cased term \
                 identities; upgrade turbo-kserve or disable cased_field",
            ));
        }
        let cased_positions = token_positions(&response.tokens, &cased_terms)?;
        doc.cased = Some(AnalyzedField {
            terms: cased_terms,
            length: cased_length,
            positions: cased_positions,
            sentences,
            ..Default::default()
        });
    }
    doc.quality = quality;
    doc.geography = geography;
    doc.entities = entities;
    Ok(doc)
}

fn token_positions(tokens: &[WireToken], terms: &DocTerms) -> Result<Option<DocPositions>, Status> {
    let occurrences: usize = terms.iter().map(|(_, _, offsets)| offsets.len()).sum();
    if occurrences == 0 {
        return Ok(Some(terms.iter().map(|_| Vec::new()).collect()));
    }
    if tokens.is_empty() {
        return Ok(None);
    }
    let mut wanted: Vec<(u32, u32, usize, usize)> = Vec::with_capacity(occurrences);
    for (ti, (_, _, offsets)) in terms.iter().enumerate() {
        for (oi, &(start, end)) in offsets.iter().enumerate() {
            wanted.push((start, end, ti, oi));
        }
    }
    wanted.sort_unstable();
    let mut positions: DocPositions = terms
        .iter()
        .map(|(_, _, offsets)| vec![0u32; offsets.len()])
        .collect();
    let mut cursor = 0usize;
    for (start, end, ti, oi) in wanted {
        while cursor < tokens.len() {
            let span = tokens[cursor].span.as_ref();
            let token_start = span.map_or(0, |s| s.start.max(0) as u32);
            if token_start >= start {
                break;
            }
            cursor += 1;
        }
        let matched = tokens
            .get(cursor)
            .and_then(|t| t.span.as_ref())
            .is_some_and(|s| s.start.max(0) as u32 == start && s.end.max(0) as u32 == end);
        if !matched {
            return Err(Status::failed_precondition(format!(
                "analysis service reported occurrence [{start}, {end}) of {:?} that is not a \
                 token span; token positions cannot be derived from this response",
                terms[ti].0
            )));
        }
        positions[ti][oi] = u32::try_from(cursor).expect("token count fits u32 under the text cap");
        cursor += 1;
    }
    Ok(Some(positions))
}

pub(crate) fn doc_quality(response: &MockAnalyzeResponse) -> DocQuality {
    let mut worst = 0.0f64;
    let mut spans: Vec<(i64, i64)> = Vec::with_capacity(response.noise.len());
    for finding in &response.noise {
        if finding.score.is_finite() && finding.score > worst {
            worst = finding.score;
        }
        if let Some(span) = finding.span.as_ref() {
            let (start, end) = (i64::from(span.start), i64::from(span.end));
            if end > start {
                spans.push((start, end));
            }
        }
    }
    spans.sort_unstable();
    let mut covered = 0i64;
    let mut reach = i64::MIN;
    for (start, end) in spans {
        let start = start.max(reach);
        if end > start {
            covered += end - start;
        }
        reach = reach.max(end);
    }
    DocQuality {
        noise: worst,
        noise_chars: covered,
        artifacts: response.artifacts.len() as i64,
    }
}

pub(crate) fn doc_geography(response: &MockAnalyzeResponse) -> DocGeography {
    let mut best: Option<&WireGeoLocation> = None;
    for location in &response.locations {
        if !location.confidence.is_finite() {
            continue;
        }
        if best.is_none_or(|b| location.confidence > b.confidence) {
            best = Some(location);
        }
    }
    DocGeography {
        point: best.map(|b| (b.latitude, b.longitude)),
        confidence: best.map_or(0.0, |b| b.confidence),
        country: response
            .regions
            .first()
            .map(|r| r.country_code.clone())
            .unwrap_or_default(),
    }
}
