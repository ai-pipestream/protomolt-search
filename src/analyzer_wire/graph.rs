//! The deterministic mock analysis payload and its binary graph codec.

use tonic::Status;

use super::codec::{
    decode_term_vectors, encode_term_vectors, put_f64, put_i32, put_span, put_str, put_u32, Reader,
};
use super::MOCK_GRAPH_MAGIC;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireSpan {
    pub(crate) start: i32,
    pub(crate) end: i32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireToken {
    pub(crate) span: Option<WireSpan>,
    pub(crate) text: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireTermVector {
    pub(crate) term: String,
    pub(crate) frequency: i32,
    pub(crate) occurrences: Vec<WireSpan>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WireNoiseSpan {
    pub(crate) span: Option<WireSpan>,
    pub(crate) severity: String,
    pub(crate) score: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireTextArtifact {
    pub(crate) span: Option<WireSpan>,
    pub(crate) kind: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WireGeoLocation {
    pub(crate) span: Option<WireSpan>,
    pub(crate) name: String,
    pub(crate) country_code: String,
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
    pub(crate) confidence: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WireRegionVote {
    pub(crate) country_code: String,
    pub(crate) share: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireEntity {
    pub(crate) span: Option<WireSpan>,
    pub(crate) kind: String,
    pub(crate) text: String,
}

/// Deterministic analysis payload used by `MockAnalysis` and unit tests.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MockAnalyzeResponse {
    pub(crate) offset_unit: i32,
    pub(crate) sentences: Vec<WireSpan>,
    pub(crate) tokens: Vec<WireToken>,
    pub(crate) term_vectors: Vec<WireTermVector>,
    pub(crate) cased_term_vectors: Vec<WireTermVector>,
    pub(crate) noise: Vec<WireNoiseSpan>,
    pub(crate) artifacts: Vec<WireTextArtifact>,
    pub(crate) locations: Vec<WireGeoLocation>,
    pub(crate) regions: Vec<WireRegionVote>,
    pub(crate) entities: Vec<WireEntity>,
}

impl MockAnalyzeResponse {
    pub(crate) fn encode_graph(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(MOCK_GRAPH_MAGIC);
        put_i32(&mut out, self.offset_unit);
        put_u32(&mut out, self.sentences.len() as u32);
        for s in &self.sentences {
            put_span(&mut out, Some(*s));
        }
        put_u32(&mut out, self.tokens.len() as u32);
        for t in &self.tokens {
            put_span(&mut out, t.span);
            put_str(&mut out, &t.text);
        }
        encode_term_vectors(&mut out, &self.term_vectors);
        encode_term_vectors(&mut out, &self.cased_term_vectors);
        put_u32(&mut out, self.noise.len() as u32);
        for n in &self.noise {
            put_span(&mut out, n.span);
            put_str(&mut out, &n.severity);
            put_f64(&mut out, n.score);
        }
        put_u32(&mut out, self.artifacts.len() as u32);
        for a in &self.artifacts {
            put_span(&mut out, a.span);
            put_str(&mut out, &a.kind);
        }
        put_u32(&mut out, self.locations.len() as u32);
        for loc in &self.locations {
            put_span(&mut out, loc.span);
            put_str(&mut out, &loc.name);
            put_str(&mut out, &loc.country_code);
            put_f64(&mut out, loc.latitude);
            put_f64(&mut out, loc.longitude);
            put_f64(&mut out, loc.confidence);
        }
        put_u32(&mut out, self.regions.len() as u32);
        for r in &self.regions {
            put_str(&mut out, &r.country_code);
            put_f64(&mut out, r.share);
        }
        put_u32(&mut out, self.entities.len() as u32);
        for e in &self.entities {
            put_span(&mut out, e.span);
            put_str(&mut out, &e.kind);
            put_str(&mut out, &e.text);
        }
        out
    }

    pub(crate) fn decode_graph(bytes: &[u8]) -> Result<Self, Status> {
        let Some(body) = bytes.strip_prefix(MOCK_GRAPH_MAGIC) else {
            return Err(Status::internal("invalid mock graph magic"));
        };
        let mut r = Reader::new(body);
        let offset_unit = r.i32()?;
        let sentences = decode_sentences(&mut r)?;
        let tokens = decode_tokens(&mut r)?;
        let term_vectors = decode_term_vectors(&mut r)?;
        let cased_term_vectors = decode_term_vectors(&mut r)?;
        let noise = decode_noise(&mut r)?;
        let artifacts = decode_artifacts(&mut r)?;
        let locations = decode_locations(&mut r)?;
        let regions = decode_regions(&mut r)?;
        let entities = decode_entities(&mut r)?;
        Ok(Self {
            offset_unit,
            sentences,
            tokens,
            term_vectors,
            cased_term_vectors,
            noise,
            artifacts,
            locations,
            regions,
            entities,
        })
    }
}

fn decode_sentences(r: &mut Reader<'_>) -> Result<Vec<WireSpan>, Status> {
    let n_sent = r.u32()? as usize;
    let mut sentences = Vec::with_capacity(n_sent);
    for _ in 0..n_sent {
        if let Some(s) = r.span()? {
            sentences.push(s);
        }
    }
    Ok(sentences)
}

fn decode_tokens(r: &mut Reader<'_>) -> Result<Vec<WireToken>, Status> {
    let n_tok = r.u32()? as usize;
    let mut tokens = Vec::with_capacity(n_tok);
    for _ in 0..n_tok {
        let span = r.span()?;
        let text = r.string()?;
        tokens.push(WireToken { span, text });
    }
    Ok(tokens)
}

fn decode_noise(r: &mut Reader<'_>) -> Result<Vec<WireNoiseSpan>, Status> {
    let n_noise = r.u32()? as usize;
    let mut noise = Vec::with_capacity(n_noise);
    for _ in 0..n_noise {
        let span = r.span()?;
        let severity = r.string()?;
        let score = r.f64()?;
        noise.push(WireNoiseSpan {
            span,
            severity,
            score,
        });
    }
    Ok(noise)
}

fn decode_artifacts(r: &mut Reader<'_>) -> Result<Vec<WireTextArtifact>, Status> {
    let n_art = r.u32()? as usize;
    let mut artifacts = Vec::with_capacity(n_art);
    for _ in 0..n_art {
        let span = r.span()?;
        let kind = r.string()?;
        artifacts.push(WireTextArtifact { span, kind });
    }
    Ok(artifacts)
}

fn decode_locations(r: &mut Reader<'_>) -> Result<Vec<WireGeoLocation>, Status> {
    let n_loc = r.u32()? as usize;
    let mut locations = Vec::with_capacity(n_loc);
    for _ in 0..n_loc {
        let span = r.span()?;
        let name = r.string()?;
        let country_code = r.string()?;
        let latitude = r.f64()?;
        let longitude = r.f64()?;
        let confidence = r.f64()?;
        locations.push(WireGeoLocation {
            span,
            name,
            country_code,
            latitude,
            longitude,
            confidence,
        });
    }
    Ok(locations)
}

fn decode_regions(r: &mut Reader<'_>) -> Result<Vec<WireRegionVote>, Status> {
    let n_reg = r.u32()? as usize;
    let mut regions = Vec::with_capacity(n_reg);
    for _ in 0..n_reg {
        let country_code = r.string()?;
        let share = r.f64()?;
        regions.push(WireRegionVote {
            country_code,
            share,
        });
    }
    Ok(regions)
}

fn decode_entities(r: &mut Reader<'_>) -> Result<Vec<WireEntity>, Status> {
    let n_ent = r.u32()? as usize;
    let mut entities = Vec::with_capacity(n_ent);
    for _ in 0..n_ent {
        let span = r.span()?;
        let kind = r.string()?;
        let text = r.string()?;
        entities.push(WireEntity { span, kind, text });
    }
    Ok(entities)
}
